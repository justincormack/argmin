// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Independent receiver-owned lookup against installed authenticated Unix/TLS
//! primary and witness endpoints. No physical grant or live dispatch activation.

use super::super::fanout_mixed::Mixed;
use super::*;
use crate::storage_rpc::{
    self as rpc, complete_settled_prefix as witness, complete_window_primary as wire,
    StorageRpcOperationDeadline,
};
use std::sync::atomic::Ordering;

fn inspect(
    f: &Mixed,
    cutoff: Instant,
    hook: impl FnMut(&'static str),
) -> Result<
    crate::node_runtime::pg_store::StagingWindowConfirmationObservation,
    rpc::StorageRpcErrorResponse,
> {
    let field = f.context.encode_field().unwrap();
    inspect_bytes(
        f,
        &field[7..],
        StorageRpcOperationDeadline::from_instant(cutoff),
        cutoff,
        hook,
    )
}

fn inspect_bytes(
    f: &Mixed,
    descriptor: &[u8],
    portable: StorageRpcOperationDeadline,
    cutoff: Instant,
    hook: impl FnMut(&'static str),
) -> Result<
    crate::node_runtime::pg_store::StagingWindowConfirmationObservation,
    rpc::StorageRpcErrorResponse,
> {
    let id = f.context.id();
    let receiver = f.remote(NodeId::new(4)).unwrap();
    receiver.server.test_inspect_receiver_window(
        f.remote
            .iter()
            .map(|peer| (peer.node, peer.client.test_execution_endpoint().clone()))
            .collect(),
        super::super::super::complete_log_rpc::window_receiver_auth(id.cluster_epoch()),
        wire::Request {
            route: witness::Request {
                node: NodeId::new(4),
                epoch: id.cluster_epoch(),
                pg: id.pg_id(),
                deadline: portable,
                topology: witness::topology_digest(&topology()),
            },
            descriptor,
        },
        cutoff,
        hook,
    )
}

#[test]
fn staging_receiver_window_unix_tls_all_targets_are_read_only_and_release_primary() {
    for tls in [false, true] {
        for (target, append) in candidate::cases() {
            let (_dir, f) = primary::mixed(tls, true, target, append);
            let before = f.observe();
            let shadows = f.shadows();
            let primary_calls = primary::count(&f, 4);
            let witness_calls = primary::count(&f, 0);
            let _servers: Vec<_> = f.remote.iter().map(|peer| peer.run()).collect();
            let mut points = Vec::new();
            let observed = inspect(&f, deadline(), |point| {
                points.push(point);
                let peer = f.remote(NodeId::new(4)).unwrap();
                if point == "after section close" {
                    // Inspect immediately: waiting for a fresh section would
                    // prove only eventual asynchronous teardown.
                    peer.server
                        .test_require_window_primary_released(f.context.id().pg_id());
                } else {
                    peer.server
                        .test_require_window_primary_held(f.context.id().pg_id());
                }
            })
            .unwrap();
            assert_eq!(
                points,
                [
                    "before witness",
                    "after witness",
                    "after primary",
                    "after section close"
                ]
            );
            assert_eq!(observed.witness, Some(NodeId::new(0)));
            assert_eq!(
                observed.prefix.applied_log_index,
                f.context.id().log_index().get()
            );
            assert_eq!(primary_calls.load(Ordering::SeqCst), 1);
            assert_eq!(witness_calls.load(Ordering::SeqCst), 1);
            assert_eq!(f.observe(), before);
            assert_eq!(f.shadows(), shadows);
        }
    }
}

#[test]
fn staging_receiver_window_waits_for_remote_release_and_fails_on_release_timeout() {
    use std::sync::mpsc::{sync_channel, RecvTimeoutError};
    for tls in [false, true] {
        for expire in [false, true] {
            let (_dir, f) = primary::mixed(tls, true, StagingTarget::DirectPut, false);
            let before = f.observe();
            let primary = f.remote(NodeId::new(4)).unwrap();
            let (entered_tx, entered) = sync_channel(1);
            let (resume_tx, resume) = sync_channel(1);
            let resume = std::sync::Mutex::new(resume);
            primary.server.test_before_window_release(Arc::new(move || {
                entered_tx.send(()).unwrap();
                resume
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(10))
                    .unwrap();
            }));
            let (released_tx, released) = sync_channel(1);
            primary
                .server
                .set_response_frame_test_hook(Arc::new(move |kind, _| {
                    if kind == rpc::StorageRpcMessageKind::MetadataCommandPgLockRelease {
                        released_tx.send(()).unwrap();
                    }
                }));
            // Expired clients disconnect before a delayed release reply, which
            // the normal concurrent server handles as a connection-local error.
            let _servers: Vec<_> = f
                .remote
                .iter()
                .map(|peer| {
                    super::super::application_sender::RunningReplica::start_concurrent(
                        Arc::clone(&peer.server),
                        peer.tls,
                    )
                })
                .collect();
            std::thread::scope(|scope| {
                let (done_tx, done) = sync_channel(1);
                let f = &f;
                scope.spawn(move || {
                    done_tx.send(inspect(f, deadline(), |_| {})).unwrap();
                });
                entered.recv_timeout(Duration::from_secs(5)).unwrap();
                primary
                    .server
                    .test_require_window_primary_held(f.context.id().pg_id());
                assert!(matches!(
                    done.recv_timeout(Duration::from_millis(50)),
                    Err(RecvTimeoutError::Timeout)
                ));
                if expire {
                    let error = done
                        .recv_timeout(Duration::from_secs(2))
                        .unwrap()
                        .unwrap_err();
                    assert_eq!(
                        error.code,
                        rpc::StorageRpcErrorCode::TransportTimeout,
                        "{error:?}"
                    );
                    primary
                        .server
                        .test_require_window_primary_held(f.context.id().pg_id());
                }
                resume_tx.send(()).unwrap();
                if !expire {
                    done.recv_timeout(Duration::from_secs(2)).unwrap().unwrap();
                }
                released.recv_timeout(Duration::from_secs(2)).unwrap();
                primary
                    .server
                    .test_require_window_primary_released(f.context.id().pg_id());
            });
            assert_eq!(f.observe(), before);
        }
    }
}

#[test]
fn staging_receiver_window_requires_exact_successful_release_acknowledgement() {
    for tls in [false, true] {
        for malformed in [false, true] {
            let (_dir, f) = primary::mixed(tls, true, StagingTarget::DirectPut, false);
            let before = f.observe();
            let primary = f.remote(NodeId::new(4)).unwrap();
            let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let count = Arc::clone(&calls);
            primary
                .server
                .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                    if kind == rpc::StorageRpcMessageKind::MetadataCommandPgLockRelease {
                        count.fetch_add(1, Ordering::SeqCst);
                        frame.payload = if malformed {
                            rpc::encode_storage_rpc_success_response(
                                b"not an empty acknowledgement",
                            )
                        } else {
                            rpc::encode_storage_rpc_error_response(&rpc::StorageRpcErrorResponse {
                                code: rpc::StorageRpcErrorCode::MetadataCommandContention,
                                message: "release rejected".into(),
                            })
                            .unwrap()
                        };
                    }
                }));
            let _servers: Vec<_> = f.remote.iter().map(|peer| peer.run()).collect();
            let error = inspect(&f, deadline(), |_| {}).unwrap_err();
            assert_eq!(
                error.code,
                if malformed {
                    rpc::StorageRpcErrorCode::PayloadDecode
                } else {
                    rpc::StorageRpcErrorCode::MetadataCommandContention
                }
            );
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            primary
                .server
                .test_require_window_primary_released(f.context.id().pg_id());
            assert_eq!(f.observe(), before);
        }
    }
}

#[test]
fn staging_receiver_window_rejects_invalid_input_and_expiry_before_transport() {
    let (_dir, f) = primary::mixed(false, true, StagingTarget::DirectPut, false);
    let before = f.observe();
    // No accepting server loop: these must reject before attempting transport.
    let field = f.context.encode_field().unwrap();
    for bad in [
        Vec::new(),
        vec![0; wire::MAX_DESCRIPTOR_BYTES + 1],
        vec![0; 20],
    ] {
        assert_eq!(
            inspect_bytes(
                &f,
                &bad,
                StorageRpcOperationDeadline::from_instant(deadline()),
                deadline(),
                |_| panic!("unexpected IO")
            )
            .unwrap_err()
            .code,
            rpc::StorageRpcErrorCode::PayloadDecode
        );
    }
    let error = inspect_bytes(
        &f,
        &field[7..],
        StorageRpcOperationDeadline {
            portable_wall_valid_until_ms: 1,
        },
        deadline(),
        |_| panic!("expired lookup reached IO"),
    )
    .unwrap_err();
    assert_eq!(error.code, rpc::StorageRpcErrorCode::TransportTimeout);
    assert_eq!(f.observe(), before);
}

#[test]
fn staging_receiver_window_forwards_one_cutoff_without_holding_local_pg_lock() {
    for tls in [false, true] {
        let (_dir, f) = primary::mixed(tls, true, StagingTarget::DirectPut, false);
        let before = f.observe();
        let cutoffs = Arc::new(std::sync::Mutex::new(Vec::new()));
        for peer in &f.remote {
            let seen = Arc::clone(&cutoffs);
            let primary = peer.node == NodeId::new(4);
            peer.server
                .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                    if kind != rpc::StorageRpcMessageKind::MetadataCommandReplicaState {
                        return;
                    }
                    let payload = rpc::decode_storage_rpc_response_payload(&frame.payload)
                        .unwrap()
                        .unwrap();
                    let offset = if primary { 4 } else { 0 };
                    let echoed =
                        witness::decode_request(&payload[offset..offset + witness::REQUEST_BYTES])
                            .unwrap();
                    seen.lock()
                        .unwrap()
                        .push(echoed.deadline.portable_wall_valid_until_ms);
                }));
        }
        let _servers: Vec<_> = f.remote.iter().map(|peer| peer.run()).collect();
        let input = StorageRpcOperationDeadline {
            portable_wall_valid_until_ms: crate::clock::current_time_millis() + 10_000,
        };
        let field = f.context.encode_field().unwrap();
        inspect_bytes(&f, &field[7..], input, deadline(), |_| {
            // The receiver is also the primary in this fixture. Its local PG
            // mutex must remain independently obtainable during peer IO.
            drop(
                f.node(NodeId::new(4))
                    .get_pg_until(
                        f.context.id().pg_id().get(),
                        Instant::now() + Duration::from_millis(100),
                    )
                    .unwrap(),
            );
        })
        .unwrap();
        let cutoffs = cutoffs.lock().unwrap();
        assert_eq!(cutoffs.len(), 2);
        assert_eq!(cutoffs[0], cutoffs[1]);
        assert!(cutoffs[0] <= input.portable_wall_valid_until_ms);
        assert_eq!(f.observe(), before);
    }
}

#[test]
fn staging_receiver_window_fails_closed_for_unsettled_or_disagreeing_witness() {
    for change in ["pending", "cold", "prefix"] {
        let (_dir, f) = primary::mixed(false, true, StagingTarget::StreamPut, true);
        let id = f.context.id();
        let node = f.node(NodeId::new(0));
        let pg = node.get_pg(id.pg_id().get()).unwrap();
        if change == "cold" {
            pg.complete_prefix_validation.invalidate();
        } else {
            let next = MetadataCommandId::new(
                id.cluster_epoch(),
                id.pg_id(),
                MetadataCommandLogIndex::new(id.log_index().get() + 1).unwrap(),
            );
            let context =
                ContextualCommand::decode_field(ordinary(next), &[1, 0, 0], &topology()).unwrap();
            if change == "pending" {
                install(&pg, &context);
            } else {
                apply(&pg, &context);
            }
        }
        drop(pg);
        let before = f.observe();
        let pc = primary::count(&f, 4);
        let wc = primary::count(&f, 0);
        let _servers: Vec<_> = f.remote.iter().map(|peer| peer.run()).collect();
        let error = inspect(&f, deadline(), |_| {}).unwrap_err();
        assert_eq!(
            error.code,
            rpc::StorageRpcErrorCode::MetadataCommandContention,
            "{change}: {error:?}"
        );
        assert_eq!(pc.load(Ordering::SeqCst), usize::from(change == "prefix"));
        assert_eq!(wc.load(Ordering::SeqCst), 1);
        assert_eq!(f.observe(), before);
    }
}

#[test]
fn staging_receiver_window_rechecks_after_each_observation_and_teardown() {
    for point in [
        "before witness",
        "after witness",
        "after primary",
        "after section close",
    ] {
        let (_dir, f) = primary::mixed(false, true, StagingTarget::StreamPut, false);
        let before = f.observe();
        let pc = primary::count(&f, 4);
        let wc = primary::count(&f, 0);
        let _servers: Vec<_> = f.remote.iter().map(|peer| peer.run()).collect();
        let cutoff = Instant::now() + Duration::from_secs(5);
        let mut fired = false;
        let error = inspect(&f, cutoff, |seen| {
            if seen == point {
                fired = true;
                // Deterministic expiry of the captured monotonic work budget.
                std::thread::sleep(
                    cutoff.saturating_duration_since(Instant::now()) + Duration::from_millis(1),
                );
            }
        })
        .unwrap_err();
        assert!(fired);
        assert_eq!(
            error.code,
            rpc::StorageRpcErrorCode::TransportTimeout,
            "{point}: {error:?}"
        );
        assert_eq!(
            wc.load(Ordering::SeqCst),
            usize::from(point != "before witness")
        );
        assert_eq!(
            pc.load(Ordering::SeqCst),
            usize::from(matches!(point, "after primary" | "after section close"))
        );
        assert_eq!(f.observe(), before);
    }
}

#[test]
fn staging_receiver_window_rejects_changed_primary_window_and_same_epoch_refresh() {
    for change in ["window", "renewal"] {
        let (_dir, f) = primary::mixed(true, true, StagingTarget::StreamPut, true);
        let receiver = f.remote(NodeId::new(4)).unwrap();
        if change == "renewal" {
            receiver
                .server
                .test_receiver_route_validity(crate::clock::current_time_millis() + 30_000);
        }
        let _servers: Vec<_> = f.remote.iter().map(|peer| peer.run()).collect();
        let pc = primary::count(&f, 4);
        let mut changed = None;
        let error = inspect(&f, deadline(), |point| {
            if point != "after witness" {
                return;
            }
            if change == "window" {
                f.node(NodeId::new(4))
                    .get_pg(f.context.id().pg_id().get())
                    .unwrap()
                    .conn
                    .execute_batch(
                        "UPDATE payload_staging_owners SET reserved_end = reserved_end + 1",
                    )
                    .unwrap();
            } else {
                // Volatile same-route renewal while the receiver retains its
                // original admission. It cannot substitute new lookup authority.
                receiver
                    .server
                    .test_receiver_route_validity(crate::clock::current_time_millis() + 60_000);
            }
            changed = Some(f.observe());
        })
        .unwrap_err();
        assert!(changed.is_some());
        assert_eq!(pc.load(Ordering::SeqCst), usize::from(change == "window"));
        assert_eq!(
            error.code,
            if change == "window" {
                rpc::StorageRpcErrorCode::MetadataCommandContention
            } else {
                rpc::StorageRpcErrorCode::Internal
            },
            "{change}: {error:?}"
        );
        assert_eq!(f.observe(), changed.unwrap());
    }
}

#[test]
fn staging_receiver_window_malformed_authenticated_replies_fail_once() {
    for node in [0, 4] {
        for tls in [false, true] {
            let (_dir, f) = primary::mixed(tls, true, StagingTarget::DirectPut, false);
            let before = f.observe();
            let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let observed = Arc::clone(&calls);
            f.remote(NodeId::new(node))
                .unwrap()
                .server
                .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                    if kind == rpc::StorageRpcMessageKind::MetadataCommandReplicaState {
                        observed.fetch_add(1, Ordering::SeqCst);
                        frame.payload =
                            rpc::encode_storage_rpc_success_response(b"crossed observation");
                    }
                }));
            let _servers: Vec<_> = f.remote.iter().map(|peer| peer.run()).collect();
            assert!(inspect(&f, deadline(), |_| {}).is_err());
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(f.observe(), before);
        }
    }
}

#[test]
fn staging_receiver_window_primary_contention_obeys_captured_budget() {
    let (_dir, f) = primary::mixed(false, true, StagingTarget::DirectPut, false);
    let before = f.observe();
    let receiver = f.remote(NodeId::new(4)).unwrap();
    let wc = primary::count(&f, 0);
    // Two independent primary connections must be served concurrently; a
    // serial accept loop would test listener starvation, not PG contention.
    let _servers: Vec<_> = f
        .remote
        .iter()
        .map(|peer| {
            super::super::application_sender::RunningReplica::start_concurrent(
                Arc::clone(&peer.server),
                peer.tls,
            )
        })
        .collect();
    let held = receiver
        .client
        .open_metadata_command_critical_section_until(f.context.id().pg_id(), deadline())
        .unwrap();
    let field = f.context.encode_field().unwrap();
    let started = Instant::now();
    let error = inspect_bytes(
        &f,
        &field[7..],
        StorageRpcOperationDeadline::from_instant(deadline()),
        started + Duration::from_millis(30),
        |_| panic!("busy primary must not reach witness"),
    )
    .unwrap_err();
    assert!(started.elapsed() < Duration::from_secs(1), "{error:?}");
    assert_eq!(wc.load(Ordering::SeqCst), 0);
    drop(held);
    assert_eq!(f.observe(), before);
}
