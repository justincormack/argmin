// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::node_client::{RetainedShardAckNodeClient, ShardAckNodeClient};
use crate::node_runtime::client_facade::LocalStorageNodeClient;
use crate::node_runtime::engine::pg_lock_test_hook::with_contention;
use std::sync::mpsc;

const WAIT: Duration = Duration::from_secs(10);

fn with_ack_pause<T>(
    remote: bool,
    server: Arc<StorageNodeServer>,
    hook: impl FnOnce(bool) + Send + 'static,
    action: impl FnOnce() -> T,
) -> T {
    if remote {
        let serving = thread::spawn(move || {
            let _time = crate::clock::test_time_override_guard(1_000);
            PgStore::test_with_before_shard_ack_effect_hook(hook, || server.accept_one().unwrap());
        });
        let result = action();
        serving.join().unwrap();
        result
    } else {
        PgStore::test_with_before_shard_ack_effect_hook(hook, action)
    }
}

fn with_pg_pause<T>(
    remote: bool,
    server: Arc<StorageNodeServer>,
    before: impl FnOnce(u32) + Send + 'static,
    after: impl FnOnce(u32, &PgStore) + Send + 'static,
    expire: bool,
    action: impl FnOnce() -> T,
) -> T {
    // The clock controller is created and used on the executing thread, whether
    // that is the embedded caller or the installed Unix server worker.
    let install = move || {
        let clock = crate::clock::test_time_override_guard(1_000);
        let control = clock.control();
        (clock, move |pg_id| {
            if expire {
                control.set(4_500);
            }
            before(pg_id);
        })
    };
    if remote {
        let serving = thread::spawn(move || {
            let (_clock, before) = install();
            with_contention(before, after, || server.accept_one().unwrap());
        });
        let result = action();
        serving.join().unwrap();
        result
    } else {
        let (_clock, before) = install();
        with_contention(before, after, action)
    }
}

#[test]
fn embedded_ack_cleanup_waits_for_admitted_transaction() {
    admitted_batch_then_cleanup(false);
}

#[test]
fn unix_ack_cleanup_waits_for_admitted_transaction() {
    admitted_batch_then_cleanup(true);
}

fn admitted_batch_then_cleanup(remote: bool) {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let node = server.test_storage_node();
    let first = test_shard_key(0);
    let second = ShardKey::new(&[0x81; 16], 1, 0);
    let canary = ShardKey::new(&[0x82; 16], 1, 0);
    let first_ack = node.write_shard_file(0, &first, b"first").unwrap();
    let second_ack = node.write_shard_file(0, &second, b"second").unwrap();
    let canary_ack = node
        .get_pg(0)
        .unwrap()
        .write_shard(&canary, b"canary")
        .unwrap();
    let local = LocalStorageNodeClient::new(config.node_id, Arc::clone(&node));
    let unix = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );
    let pg_id = DataPgId::new_for_test(PgId::new(0));
    // Mint both routes BEFORE the race. Contention during route construction
    // must not stand in for contention at the actual mutation boundary.
    let register = if remote {
        unix.open_shard_ack_route(config.cluster_epoch, pg_id)
    } else {
        local.open_shard_ack_route(config.cluster_epoch, pg_id)
    }
    .unwrap();
    let delete = if remote {
        unix.open_retained_shard_ack_route(config.cluster_epoch, pg_id, &first)
    } else {
        local.open_retained_shard_ack_route(config.cluster_epoch, pg_id, &first)
    }
    .unwrap();
    let (admitted_tx, admitted_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let (blocked_tx, blocked_rx) = mpsc::channel();
    let (deleted_tx, deleted_rx) = mpsc::channel();
    let observed = Arc::new(AtomicBool::new(false));
    thread::scope(|scope| {
        let writer_server = Arc::clone(&server);
        let writer_first = first.clone();
        let writer_second = second.clone();
        let writer = scope.spawn(move || {
            let _time = crate::clock::test_time_override_guard(1_000);
            with_ack_pause(
                remote,
                writer_server,
                move |in_transaction| {
                    assert!(in_transaction);
                    admitted_tx.send(()).unwrap();
                    resume_rx.recv_timeout(WAIT).unwrap();
                },
                || {
                    register.register_shard_acks_with_effect_fence(
                        &[(&writer_first, first_ack), (&writer_second, second_ack)],
                        AdmittedRouteEffectFence::bounded(config.cluster_epoch, 10_000, 9_000),
                    )
                },
            )
            .unwrap();
        });
        admitted_rx.recv_timeout(WAIT).unwrap();
        let deleting_server = Arc::clone(&server);
        let checked = Arc::clone(&observed);
        let expected = [
            (first.clone(), first_ack),
            (second.clone(), second_ack),
            (canary.clone(), canary_ack),
        ];
        let deleter = scope.spawn(move || {
            let _time = crate::clock::test_time_override_guard(1_000);
            with_pg_pause(
                remote,
                deleting_server,
                move |pg| {
                    assert_eq!(pg, 0);
                    blocked_tx.send(()).unwrap();
                },
                move |pg_id, pg| {
                    assert_eq!(pg_id, 0);
                    // Positive canary at the exact delete boundary: the full batch
                    // must exist before deleting its target, not just its sibling.
                    for (key, ack) in expected {
                        pg.validate_written_shard_ack(&key, ack).unwrap();
                    }
                    checked.store(true, Ordering::SeqCst);
                },
                false,
                || delete.delete_retained_shard_ack(),
            )
            .unwrap();
            deleted_tx.send(()).unwrap();
        });
        blocked_rx.recv_timeout(WAIT).unwrap();
        assert!(matches!(
            deleted_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        resume_tx.send(()).unwrap();
        writer.join().unwrap();
        deleted_rx.recv_timeout(WAIT).unwrap();
        deleter.join().unwrap();
    });
    assert!(observed.load(Ordering::SeqCst));
    drop(local);
    drop(server);
    drop(node);
    // One runtime at a time. Reopen pins the committed batch/delete result.
    let reopened = SharedStorageNode::open_with_default_ec_shape(
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
    )
    .unwrap();
    let pg = reopened.get_pg(0).unwrap();
    assert!(matches!(pg.stat_shard(&first), Err(StoreError::NotFound)));
    pg.validate_written_shard_ack(&second, second_ack).unwrap();
    pg.validate_written_shard_ack(&canary, canary_ack).unwrap();
    drop(pg);
    assert_eq!(reopened.read_shard_file(0, &first).unwrap(), b"first");
    assert_eq!(reopened.read_shard_file(0, &second).unwrap(), b"second");
}

#[test]
fn embedded_delayed_ack_expires_behind_cleanup_without_resurrection() {
    cleanup_then_expired_batch(false);
}

#[test]
fn unix_delayed_ack_expires_behind_cleanup_without_resurrection() {
    cleanup_then_expired_batch(true);
}

fn cleanup_then_expired_batch(remote: bool) {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    let _time = crate::clock::test_time_override_guard(1_000);
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let node = server.test_storage_node();
    let first = test_shard_key(0);
    let second = ShardKey::new(&[0x83; 16], 1, 0);
    let canary = ShardKey::new(&[0x84; 16], 1, 0);
    let first_ack = node
        .get_pg(0)
        .unwrap()
        .write_shard(&first, b"first")
        .unwrap();
    let second_ack = node.write_shard_file(0, &second, b"second").unwrap();
    let canary_ack = node
        .get_pg(0)
        .unwrap()
        .write_shard(&canary, b"canary")
        .unwrap();
    let local = LocalStorageNodeClient::new(config.node_id, Arc::clone(&node));
    let unix = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );
    let pg_id = DataPgId::new_for_test(PgId::new(0));
    let register = if remote {
        unix.open_shard_ack_route(config.cluster_epoch, pg_id)
    } else {
        local.open_shard_ack_route(config.cluster_epoch, pg_id)
    }
    .unwrap();
    let fresh = if remote {
        unix.open_shard_ack_route(config.cluster_epoch, pg_id)
    } else {
        local.open_shard_ack_route(config.cluster_epoch, pg_id)
    }
    .unwrap();
    let pg = node.get_pg(0).unwrap();
    pg.validate_written_shard_ack(&first, first_ack).unwrap();
    pg.delete_shard_record(&first).unwrap();
    let (blocked_tx, blocked_rx) = mpsc::channel();
    let observed = Arc::new(AtomicBool::new(false));
    thread::scope(|scope| {
        let writer_server = Arc::clone(&server);
        let checked = Arc::clone(&observed);
        let first = first.clone();
        let second = second.clone();
        let canary = canary.clone();
        let writer = scope.spawn(move || {
            let _time = crate::clock::test_time_override_guard(1_000);
            let expected_first = first.clone();
            with_pg_pause(
                remote,
                writer_server,
                move |pg| {
                    assert_eq!(pg, 0);
                    blocked_tx.send(()).unwrap();
                },
                move |pg_id, pg| {
                    assert_eq!(pg_id, 0);
                    assert!(matches!(
                        pg.stat_shard(&expected_first),
                        Err(StoreError::NotFound)
                    ));
                    pg.validate_written_shard_ack(&canary, canary_ack).unwrap();
                    checked.store(true, Ordering::SeqCst);
                },
                true,
                || {
                    register.register_shard_acks_with_effect_fence(
                        &[(&first, first_ack), (&second, second_ack)],
                        AdmittedRouteEffectFence::bounded(config.cluster_epoch, 5_000, 4_000),
                    )
                },
            )
        });
        blocked_rx.recv_timeout(WAIT).unwrap();
        drop(pg);
        let error = writer.join().unwrap().unwrap_err();
        assert!(
            if remote {
                matches!(
                    error,
                    StoreError::StorageRpc {
                        failure: StorageRpcErrorCode::StaleShardLocation,
                        ..
                    }
                )
            } else {
                matches!(error, StoreError::RouteMapExpired { now_ms: 4_500, .. })
            },
            "remote={remote}: {error:?}"
        );
    });
    assert!(observed.load(Ordering::SeqCst));
    {
        let pg = node.get_pg(0).unwrap();
        assert!(matches!(pg.stat_shard(&first), Err(StoreError::NotFound)));
        assert!(matches!(pg.stat_shard(&second), Err(StoreError::NotFound)));
        pg.validate_written_shard_ack(&canary, canary_ack).unwrap();
    }
    assert_eq!(node.read_shard_file(0, &first).unwrap(), b"first");
    assert_eq!(node.read_shard_file(0, &second).unwrap(), b"second");
    let serving = remote.then(|| {
        let server = Arc::clone(&server);
        thread::spawn(move || {
            let _time = crate::clock::test_time_override_guard(1_000);
            server.accept_one().unwrap();
        })
    });
    // This is only an effect-boundary canary, not permission for a cancelled
    // production attempt to obtain new authority. Owner/window rules remain open.
    fresh
        .register_shard_acks_with_effect_fence(
            &[(&first, first_ack), (&second, second_ack)],
            AdmittedRouteEffectFence::bounded(config.cluster_epoch, 10_000, 9_000),
        )
        .unwrap();
    if let Some(serving) = serving {
        serving.join().unwrap();
    }
    let pg = node.get_pg(0).unwrap();
    pg.validate_written_shard_ack(&first, first_ack).unwrap();
    pg.validate_written_shard_ack(&second, second_ack).unwrap();
}
