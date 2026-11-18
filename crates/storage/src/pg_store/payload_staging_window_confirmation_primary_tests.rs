// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Real Unix/TLS primary sections, with embedded or remote designated witnesses.
//! No physical grants and no live publisher selection.

use super::super::fanout_mixed::Mixed;
use super::*;
use crate::storage_rpc::{self as rpc, StorageRpcMessageKind as Kind};
use std::sync::atomic::{AtomicUsize, Ordering};

const KIND: Kind = Kind::MetadataCommandReplicaState;

pub(super) fn mixed(
    tls: bool,
    remote_witness: bool,
    target: StagingTarget,
    append: bool,
) -> (test_util::TempDir, Mixed) {
    let Setup {
        _dir,
        map,
        handle,
        context,
    } = setup(target, append);
    (
        _dir,
        Mixed::from_settled_fixture(
            map,
            handle,
            context,
            [(4, tls), (if remote_witness { 0 } else { 2 }, !tls)],
        ),
    )
}

fn inspect(
    f: &Mixed,
    hook: impl FnMut(&'static str),
) -> Result<crate::node_runtime::pg_store::StagingWindowConfirmationObservation, E> {
    let id = f.context.id();
    let primary = f.remote(NodeId::new(4)).unwrap();
    let admission = f.handle.admit_current_route().unwrap();
    let route = admission
        .complete_fanout_route(id.pg_id(), id.cluster_epoch())
        .unwrap();
    let session = primary
        .client
        .open_metadata_command_critical_section_until(id.pg_id(), deadline())
        .unwrap();
    f.context.inspect_window_confirmation_remote(
        &route,
        &primary.client,
        &session,
        deadline(),
        hook,
    )
}

pub(super) fn count(f: &Mixed, node: u32) -> Arc<AtomicUsize> {
    let count = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&count);
    f.remote(NodeId::new(node))
        .unwrap()
        .server
        .set_response_frame_test_hook(Arc::new(move |kind, _| {
            if kind == KIND {
                observed.fetch_add(1, Ordering::SeqCst);
            }
        }));
    count
}

#[test]
fn staging_window_confirmation_primary_all_targets_and_witness_transports_read_only() {
    for tls in [false, true] {
        for remote_witness in [false, true] {
            for (target, append) in candidate::cases() {
                let (_dir, f) = mixed(tls, remote_witness, target, append);
                let before = f.observe();
                let shadows = f.shadows();
                let primary_calls = count(&f, 4);
                let other_calls = count(&f, if remote_witness { 0 } else { 2 });
                let _servers: Vec<_> = f.remote.iter().map(|peer| peer.run()).collect();
                let mut points = Vec::new();
                for _ in 0..2 {
                    let observation = inspect(&f, |point| {
                        f.remote(NodeId::new(4))
                            .unwrap()
                            .server
                            .test_require_window_primary_held(f.context.id().pg_id());
                        points.push(point);
                    })
                    .unwrap();
                    assert_eq!(observation.witness, Some(NodeId::new(0)));
                    assert_eq!(
                        observation.prefix.cluster_epoch,
                        f.context.id().cluster_epoch()
                    );
                    assert_eq!(
                        observation.prefix.applied_log_index,
                        f.context.id().log_index().get()
                    );
                }
                assert_eq!(
                    points,
                    ["before witness", "after witness", "after primary"].repeat(2)
                );
                assert_eq!(primary_calls.load(Ordering::SeqCst), 2);
                assert_eq!(
                    other_calls.load(Ordering::SeqCst),
                    if remote_witness { 2 } else { 0 }
                );
                assert_eq!(f.observe(), before);
                assert_eq!(f.shadows(), shadows);
            }
        }
    }
}

#[test]
fn staging_window_confirmation_primary_changed_window_pending_or_prefix_fails_closed() {
    for tls in [false, true] {
        for change in ["window", "pending", "cold", "prefix"] {
            let (_dir, f) = mixed(tls, true, StagingTarget::StreamPut, true);
            let primary = f.node(NodeId::new(4));
            let id = f.context.id();
            let _servers: Vec<_> = f.remote.iter().map(|peer| peer.run()).collect();
            let mut after_change = None;
            let mut fired = false;
            let error = inspect(&f, |point| {
                if point != "after witness" {
                    return;
                }
                fired = true;
                let pg = primary.get_pg(id.pg_id().get()).unwrap();
                match change {
                    "window" => {
                        pg.conn
                            .execute_batch(
                                "UPDATE payload_staging_owners SET reserved_end = reserved_end + 1",
                            )
                            .unwrap();
                    }
                    "cold" => pg.complete_prefix_validation.invalidate(),
                    "pending" | "prefix" => {
                        let next = MetadataCommandId::new(
                            id.cluster_epoch(),
                            id.pg_id(),
                            MetadataCommandLogIndex::new(id.log_index().get() + 1).unwrap(),
                        );
                        let next = ContextualCommand::decode_field(
                            ordinary(next),
                            &[1, 0, 0],
                            &topology(),
                        )
                        .unwrap();
                        if change == "pending" {
                            install(&pg, &next);
                        } else {
                            apply(&pg, &next);
                        }
                    }
                    _ => unreachable!(),
                }
                drop(pg);
                after_change = Some(f.observe());
            })
            .unwrap_err();
            assert!(fired);
            if change == "prefix" {
                assert!(matches!(error, E::PrefixDisagreement), "{error}");
            } else {
                assert!(
                    matches!(error, E::Store(StoreError::StorageRpc { failure, .. }) if failure == rpc::StorageRpcErrorCode::MetadataCommandContention),
                    "{change}: {error}"
                );
            }
            assert_eq!(f.observe(), after_change.unwrap());
        }
    }
}

#[test]
fn staging_window_confirmation_primary_authentic_crossed_replies_fail_without_retry() {
    for tls in [false, true] {
        let (_dir, f) = mixed(tls, false, StagingTarget::StreamPut, true);
        let before = f.observe();
        let primary = f.remote(NodeId::new(4)).unwrap();
        let _servers: Vec<_> = f.remote.iter().map(|peer| peer.run()).collect();
        // Purpose; echoed version/deadline/node/epoch/PG/topology; prefix epoch;
        // independently each valid prefix value; descriptor digest; peer tag/ID.
        for offset in [
            0,
            4,
            6,
            14,
            18,
            26,
            30,
            62,
            70,
            79,
            88,
            96,
            128,
            129,
            usize::MAX,
        ] {
            let calls = Arc::new(AtomicUsize::new(0));
            let observed = Arc::clone(&calls);
            primary
                .server
                .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                    if kind != KIND {
                        return;
                    }
                    observed.fetch_add(1, Ordering::SeqCst);
                    let decoded =
                        rpc::decode_storage_rpc_response_payload_with_connection_disposition(
                            &frame.payload,
                        )
                        .unwrap();
                    assert!(decoded.connection_reusable);
                    let mut bytes = decoded.response.unwrap();
                    if offset == usize::MAX {
                        bytes.pop();
                    } else {
                        bytes[offset] ^= 1;
                    }
                    frame.payload = rpc::encode_storage_rpc_success_response(&bytes);
                    rpc::set_storage_rpc_response_connection_reusable(
                        &mut frame.payload,
                        decoded.connection_reusable,
                    )
                    .unwrap();
                }));
            let error = inspect(&f, |_| {}).unwrap_err();
            if [70, 79, 88].contains(&offset) {
                assert!(matches!(error, E::PrefixDisagreement), "{offset}: {error}");
            } else {
                assert!(
                    matches!(error, E::Store(StoreError::StorageRpc { failure, .. }) if failure == rpc::StorageRpcErrorCode::PayloadDecode),
                    "{offset}: {error}"
                );
            }
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(f.observe(), before);
        }
    }
}

#[test]
fn staging_window_confirmation_primary_expiry_at_each_boundary_never_confirms() {
    for tls in [false, true] {
        for at in ["before witness", "after witness", "after primary"] {
            let (_dir, f) = mixed(tls, true, StagingTarget::DirectPut, false);
            let before = f.observe();
            let primary_calls = count(&f, 4);
            let _servers: Vec<_> = f.remote.iter().map(|peer| peer.run()).collect();
            let clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
            let mut fired = false;
            let error = inspect(&f, |point| {
                if point == at {
                    fired = true;
                    clock.set(crate::clock::current_time_millis() + 1_000_000);
                }
            })
            .unwrap_err();
            assert!(fired);
            assert!(matches!(error, E::Store(_)), "{error}");
            assert_eq!(
                primary_calls.load(Ordering::SeqCst),
                usize::from(at == "after primary")
            );
            assert_eq!(f.observe(), before);
        }
    }
}

#[test]
fn staging_window_confirmation_primary_lost_or_closed_reply_cannot_confirm() {
    for tls in [false, true] {
        for lost in [false, true] {
            let (_dir, f) = mixed(tls, false, StagingTarget::DirectPut, false);
            let before = f.observe();
            let primary = f.remote(NodeId::new(4)).unwrap();
            let calls = count(&f, 4);
            if lost {
                std::thread::scope(|scope| {
                    let server = scope.spawn(|| {
                        primary
                            .server
                            .test_accept_one_without_settled_reply()
                            .unwrap()
                    });
                    assert!(inspect(&f, |_| {}).is_err());
                    server.join().unwrap();
                });
                assert_eq!(calls.load(Ordering::SeqCst), 1);
            } else {
                primary
                    .server
                    .set_response_frame_test_hook(Arc::new(|kind, frame| {
                        if kind == KIND {
                            rpc::set_storage_rpc_response_connection_reusable(
                                &mut frame.payload,
                                false,
                            )
                            .unwrap();
                        }
                    }));
                let _server = primary.run();
                let error = inspect(&f, |_| {}).unwrap_err();
                assert!(
                    error.to_string().contains("lost its held section"),
                    "{error}"
                );
            }
            assert_eq!(f.observe(), before);
        }
    }
}

#[test]
fn staging_window_confirmation_primary_receiver_requires_exact_section_and_subject() {
    use rpc::{
        complete_settled_prefix as witness, complete_window_primary as wire,
        StorageRpcErrorCode as Code,
    };
    for tls in [false, true] {
        let (_dir, f) = mixed(tls, false, StagingTarget::StreamPut, true);
        let primary = f.remote(NodeId::new(4)).unwrap();
        let before = f.observe();
        let field = f.context.encode_field().unwrap();
        let descriptor = &field[7..];
        let id = f.context.id();
        let base = wire::Request {
            route: witness::Request {
                node: primary.node,
                epoch: id.cluster_epoch(),
                pg: id.pg_id(),
                deadline: rpc::StorageRpcOperationDeadline::from_instant(deadline()),
                topology: witness::topology_digest(&topology()),
            },
            descriptor,
        };
        primary
            .server
            .test_window_primary_admission(&wire::encode_request(base).unwrap());
        primary
            .server
            .test_window_primary_route_expiry(&wire::encode_request(base).unwrap());
        let _server = primary.run();
        let reply = primary
            .client
            .rpc_request_result(KIND, wire::encode_request(base).unwrap())
            .unwrap()
            .unwrap_err();
        assert_eq!(reply.code, Code::MetadataCommandContention);
        let session = primary
            .client
            .open_metadata_command_critical_section_until(id.pg_id(), deadline())
            .unwrap();
        for field in ["node", "epoch", "pg", "topology", "cutoff", "descriptor"] {
            let mut request = base;
            let mut bad_descriptor = descriptor.to_vec();
            match field {
                "node" => request.route.node = NodeId::new(0),
                "epoch" => {
                    request.route.epoch = ClusterEpoch::new(id.cluster_epoch().get() + 1).unwrap()
                }
                "pg" => request.route.pg = PgId::new((id.pg_id().get() + 1) % 8),
                "topology" => request.route.topology[0] ^= 1,
                "cutoff" => request.route.deadline.portable_wall_valid_until_ms = 1,
                "descriptor" => {
                    bad_descriptor[10] = 99;
                    request.descriptor = &bad_descriptor;
                }
                _ => unreachable!(),
            }
            let error = session
                .test_window_primary_request(wire::encode_request(request).unwrap())
                .unwrap()
                .unwrap_err();
            assert_eq!(
                error.code,
                match field {
                    "node" => Code::UnknownNode,
                    "epoch" => Code::StaleShardLocation,
                    "pg" => Code::MetadataCommandContention,
                    "topology" | "descriptor" => Code::PayloadDecode,
                    "cutoff" => Code::TransportTimeout,
                    _ => unreachable!(),
                },
                "{field}: {error:?}"
            );
        }
        assert_eq!(f.observe(), before);
    }
}

#[test]
fn staging_window_confirmation_primary_session_is_exclusive_and_wrong_pg_fails_before_io() {
    for tls in [false, true] {
        let (_dir, f) = mixed(tls, false, StagingTarget::DirectPut, false);
        let primary = f.remote(NodeId::new(4)).unwrap();
        let before = f.observe();
        let calls = count(&f, 4);
        let _server = primary.run();
        let id = f.context.id();
        let admission = f.handle.admit_current_route().unwrap();
        let route = admission
            .complete_fanout_route(id.pg_id(), id.cluster_epoch())
            .unwrap();
        let wrong = primary
            .client
            .open_metadata_command_critical_section_until(
                PgId::new((id.pg_id().get() + 1) % 8),
                deadline(),
            )
            .unwrap();
        let error = f
            .context
            .inspect_window_confirmation_remote(&route, &primary.client, &wrong, deadline(), |_| {
                panic!("must reject before witness")
            })
            .unwrap_err();
        assert!(
            error.to_string().contains("exact open primary session"),
            "{error}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        drop(wrong);
        let session = primary
            .client
            .open_metadata_command_critical_section_until(id.pg_id(), deadline())
            .unwrap();
        let mut count = 0;
        f.context
            .inspect_window_confirmation_remote(
                &route,
                &primary.client,
                &session,
                deadline(),
                |_| {
                    let started = Instant::now();
                    let result = session.borrow_complete_primary(
                        &primary.client,
                        id.pg_id(),
                        id.cluster_epoch(),
                        started + Duration::from_millis(5),
                    );
                    assert!(
                        result.is_err(),
                        "borrow must exclude interleaved RPC/release"
                    );
                    assert!(started.elapsed() < Duration::from_secs(2));
                    primary.server.test_require_window_primary_held(id.pg_id());
                    count += 1;
                },
            )
            .unwrap();
        assert_eq!(count, 3);
        assert_eq!(f.observe(), before);
    }
}

#[test]
fn staging_window_confirmation_primary_receiver_pg_wait_obeys_wire_cutoff() {
    use rpc::{complete_settled_prefix as witness, complete_window_primary as wire};
    for tls in [false, true] {
        let (_dir, f) = mixed(tls, false, StagingTarget::StreamPut, true);
        let primary = f.remote(NodeId::new(4)).unwrap();
        let _server = primary.run();
        let id = f.context.id();
        let session = primary
            .client
            .open_metadata_command_critical_section_until(id.pg_id(), deadline())
            .unwrap();
        let node = f.node(NodeId::new(4));
        let held = node.get_pg(id.pg_id().get()).unwrap();
        let before = snapshot(&held);
        let changes = held.conn.total_changes();
        let field = f.context.encode_field().unwrap();
        let request = wire::Request {
            route: witness::Request {
                node: primary.node,
                epoch: id.cluster_epoch(),
                pg: id.pg_id(),
                deadline: rpc::StorageRpcOperationDeadline {
                    portable_wall_valid_until_ms: crate::clock::current_time_millis() + 250,
                },
                topology: witness::topology_digest(&topology()),
            },
            descriptor: &field[7..],
        };
        let started = Instant::now();
        // Long client timeout: the receiver itself must stop waiting for the PG.
        let error = session
            .test_window_primary_request(wire::encode_request(request).unwrap())
            .unwrap()
            .unwrap_err();
        assert_eq!(
            error.code,
            rpc::StorageRpcErrorCode::TransportTimeout,
            "{error:?}"
        );
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(snapshot(&held), before);
        assert_eq!(held.conn.total_changes(), changes);
    }
}
