// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::node_runtime::pg_store::shard_file_mutation_test_hook::{self, Point};

const WAIT: Duration = Duration::from_secs(10);
const PAYLOAD: &[u8] = b"published shard bytes";

fn location(config: &StorageNodeProcessConfig, key: &ShardKey) -> crate::cluster::ShardLocation {
    crate::cluster::ShardLocation::new(
        config.cluster_epoch,
        DataPgId::new_for_test(PgId::new(0)),
        key.shard_index(),
        config.node_id,
    )
}

#[test]
fn unix_file_cleanup_waits_through_admitted_publication() {
    for after_publication in [false, true] {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let node = server.test_storage_node();
        let key = test_shard_key(0);
        let canary = ShardKey::new(&[0x91; 16], 99, 0);
        let canary_ack = node
            .get_pg(0)
            .unwrap()
            .write_shard(&canary, b"canary")
            .unwrap();
        let unix = UnixStorageNodeClient::new(
            config.node_id,
            config.cluster_epoch,
            config.socket_path.clone(),
        );
        let write = unix
            .open_placed_shard_route(location(&config, &key), &key)
            .unwrap();
        let delete = unix
            .open_retained_placed_shard_route(location(&config, &key), &key)
            .unwrap();
        let (paused_tx, paused_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let writing_server = Arc::clone(&server);
        let serving_write = thread::spawn(move || {
            let _clock = crate::clock::test_time_override_guard(1_000);
            let pause = move || {
                paused_tx.send(()).unwrap();
                resume_rx.recv_timeout(WAIT).unwrap();
            };
            if after_publication {
                PgStore::test_with_after_shard_publication_hook(pause, || {
                    writing_server.accept_one().unwrap()
                });
            } else {
                PgStore::test_with_before_shard_publication_hook(pause, || {
                    writing_server.accept_one().unwrap()
                });
            }
        });
        let (blocked_tx, blocked_rx) = mpsc::channel();
        let (deleted_tx, deleted_rx) = mpsc::channel();
        let observed = Arc::new(AtomicBool::new(false));
        thread::scope(|scope| {
            let writer = scope.spawn(move || {
                let _clock = crate::clock::test_time_override_guard(1_000);
                write
                    .write_placed_shard_with_effect_fence(
                        PAYLOAD,
                        AdmittedRouteEffectFence::bounded(config.cluster_epoch, 10_000, 9_000),
                    )
                    .unwrap()
            });
            paused_rx.recv_timeout(WAIT).unwrap();
            if after_publication {
                assert_eq!(node.read_shard_file(0, &key).unwrap(), PAYLOAD);
            } else {
                assert!(matches!(
                    node.read_shard_file(0, &key),
                    Err(StoreError::NotFound)
                ));
            }
            let deleting_server = Arc::clone(&server);
            let inspecting_node = Arc::clone(&node);
            let expected_key = key.clone();
            let checked = Arc::clone(&observed);
            let serving_delete = scope.spawn(move || {
                let _clock = crate::clock::test_time_override_guard(1_000);
                shard_file_mutation_test_hook::with_hook(
                    move |point, key| {
                        assert_eq!(key, &expected_key);
                        match point {
                            Point::DeleteWaiting => blocked_tx.send(()).unwrap(),
                            Point::DeleteAcquired => {
                                // Inspect under exclusive file access, immediately before
                                // the real unlink: absence cannot satisfy this canary.
                                assert_eq!(
                                    inspecting_node.read_shard_file(0, key).unwrap(),
                                    PAYLOAD
                                );
                                checked.store(true, Ordering::SeqCst);
                            }
                            Point::PublishWaiting => panic!("delete worker attempted publication"),
                        }
                    },
                    || deleting_server.accept_one().unwrap(),
                );
            });
            let deleter = scope.spawn(move || {
                delete.delete_placed_shard_for_historical_cleanup().unwrap();
                deleted_tx.send(()).unwrap();
            });
            // This is actual failed try_write, not merely thread/RPC start.
            blocked_rx.recv_timeout(WAIT).unwrap();
            assert!(matches!(
                deleted_rx.try_recv(),
                Err(mpsc::TryRecvError::Empty)
            ));
            // Another key on the same PG remains writable during both requests.
            node.write_shard_file(0, &canary, b"canary").unwrap();
            resume_tx.send(()).unwrap();
            let ack = writer.join().unwrap();
            assert_eq!(ack.stored_size, PAYLOAD.len() as u64);
            assert_eq!(ack.crc64, checksum::crc64::checksum(PAYLOAD));
            deleted_rx.recv_timeout(WAIT).unwrap();
            deleter.join().unwrap();
            serving_delete.join().unwrap();
        });
        serving_write.join().unwrap();
        assert!(observed.load(Ordering::SeqCst));
        drop(node);
        drop(server);
        let reopened = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        assert!(matches!(
            reopened.read_shard_file(0, &key),
            Err(StoreError::NotFound)
        ));
        assert_eq!(reopened.read_shard_file(0, &canary).unwrap(), b"canary");
        let pg = reopened.get_pg(0).unwrap();
        pg.validate_written_shard_ack(&canary, canary_ack).unwrap();
        // File publication returns an ack; it does not itself insert the
        // separate acknowledgement row.
        assert!(matches!(pg.stat_shard(&key), Err(StoreError::NotFound)));
        assert!(std::fs::read_dir(config.data_dir.join("pg-0000/tmp"))
            .unwrap()
            .next()
            .is_none());
    }
}

#[test]
fn unix_delayed_file_expires_behind_cleanup_without_resurrection() {
    let _clock = crate::clock::test_time_override_guard(1_000);
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let node = server.test_storage_node();
    let key = test_shard_key(0);
    let canary = ShardKey::new(&[0x92; 16], 99, 0);
    let old_ack = node.get_pg(0).unwrap().write_shard(&key, PAYLOAD).unwrap();
    let canary_ack = node
        .get_pg(0)
        .unwrap()
        .write_shard(&canary, b"canary")
        .unwrap();
    assert_eq!(node.read_shard_file(0, &key).unwrap(), PAYLOAD);
    let unix = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );
    let write = unix
        .open_placed_shard_route(location(&config, &key), &key)
        .unwrap();
    let fresh = unix
        .open_placed_shard_route(location(&config, &key), &key)
        .unwrap();
    let delete = unix
        .open_retained_placed_shard_route(location(&config, &key), &key)
        .unwrap();
    let (deleting_tx, deleting_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let deleting_server = Arc::clone(&server);
    let expected_key = key.clone();
    let serving_delete = thread::spawn(move || {
        let _clock = crate::clock::test_time_override_guard(1_000);
        shard_file_mutation_test_hook::with_hook(
            move |point, key| {
                assert_eq!(point, Point::DeleteAcquired);
                assert_eq!(key, &expected_key);
                deleting_tx.send(()).unwrap();
                resume_rx.recv_timeout(WAIT).unwrap();
            },
            || deleting_server.accept_one().unwrap(),
        );
    });
    thread::scope(|scope| {
        let deleter =
            scope.spawn(move || delete.delete_placed_shard_for_historical_cleanup().unwrap());
        deleting_rx.recv_timeout(WAIT).unwrap();
        let (blocked_tx, blocked_rx) = mpsc::channel();
        let writing_server = Arc::clone(&server);
        let expected_key = key.clone();
        let serving_write = scope.spawn(move || {
            let clock = crate::clock::test_time_override_guard(1_000);
            let control = clock.control();
            shard_file_mutation_test_hook::with_hook(
                move |point, key| {
                    assert_eq!(point, Point::PublishWaiting);
                    assert_eq!(key, &expected_key);
                    // The installed frame and portable client deadline have already
                    // been validated. Expire only after reaching file exclusion.
                    control.set(4_500);
                    blocked_tx.send(()).unwrap();
                },
                || {
                    PgStore::test_with_before_shard_publication_hook(
                        || panic!("expired waiting writer reached temporary-file publication"),
                        || writing_server.accept_one().unwrap(),
                    );
                },
            );
        });
        let writer = scope.spawn(move || {
            let _clock = crate::clock::test_time_override_guard(1_000);
            write.write_placed_shard_with_effect_fence(
                PAYLOAD,
                AdmittedRouteEffectFence::bounded(config.cluster_epoch, 5_000, 4_000),
            )
        });
        blocked_rx.recv_timeout(WAIT).unwrap();
        resume_tx.send(()).unwrap();
        deleter.join().unwrap();
        let error = writer.join().unwrap().unwrap_err();
        assert!(
            matches!(
                error,
                StoreError::StorageRpc {
                    failure: StorageRpcErrorCode::StaleShardLocation,
                    ..
                }
            ),
            "{error:?}"
        );
        serving_write.join().unwrap();
    });
    serving_delete.join().unwrap();
    assert!(matches!(
        node.read_shard_file(0, &key),
        Err(StoreError::NotFound)
    ));
    assert!(std::fs::read_dir(config.data_dir.join("pg-0000/tmp"))
        .unwrap()
        .next()
        .is_none());
    {
        let pg = node.get_pg(0).unwrap();
        // Retained file deletion must not impersonate independent ack cleanup.
        pg.validate_written_shard_ack(&key, old_ack).unwrap();
        pg.validate_written_shard_ack(&canary, canary_ack).unwrap();
    }
    assert_eq!(node.read_shard_file(0, &canary).unwrap(), b"canary");
    let serving_fresh = thread::spawn(move || {
        let _clock = crate::clock::test_time_override_guard(1_000);
        server.accept_one().unwrap();
    });
    // Positive effect-boundary canary only: this does not give an abandoned
    // production attempt permission to mint new cleanup-window authority.
    assert_eq!(
        fresh
            .write_placed_shard_with_effect_fence(
                PAYLOAD,
                AdmittedRouteEffectFence::bounded(config.cluster_epoch, 10_000, 9_000),
            )
            .unwrap(),
        old_ack
    );
    serving_fresh.join().unwrap();
    assert_eq!(node.read_shard_file(0, &key).unwrap(), PAYLOAD);
}
