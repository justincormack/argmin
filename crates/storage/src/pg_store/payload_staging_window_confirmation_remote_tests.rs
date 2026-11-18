// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Authenticated, installed Unix/TLS witness reads under embedded-primary
//! serialization. These are scalar observations, not physical write grants.

use super::super::fanout_mixed::Mixed;
use super::*;
use crate::node_runtime::clients::MetadataCommandInspectionNodeClient;
use crate::storage_rpc::{
    self as rpc, complete_settled_prefix as wire, StorageRpcErrorCode as Code,
    StorageRpcMessageKind as Kind,
};
use std::sync::atomic::{AtomicUsize, Ordering};

const KIND: Kind = Kind::MetadataCommandReplicaState;

fn mixed(tls: bool, target: StagingTarget, append: bool) -> (test_util::TempDir, Mixed) {
    let Setup {
        _dir,
        map,
        handle,
        context,
    } = setup(target, append);
    (
        _dir,
        Mixed::from_fixture(map, handle, context, 1, [(0, tls), (2, !tls)]),
    )
}

fn inspect(
    f: &Mixed,
) -> Result<crate::node_runtime::pg_store::StagingWindowConfirmationObservation, E> {
    let id = f.context.id();
    let admission = f.handle.admit_current_route().unwrap();
    let route = admission
        .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    f.context
        .inspect_window_confirmation_embedded(&section, deadline(), |_| {
            assert!(f
                .map
                .runtime_state()
                .metadata_command_pg_lock(id.pg_id())
                .try_lock()
                .is_none());
        })
}

fn count(f: &Mixed, node: u32) -> Arc<AtomicUsize> {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    f.remote(NodeId::new(node))
        .unwrap()
        .server
        .set_response_frame_test_hook(Arc::new(move |kind, _| {
            if kind == KIND {
                observed.fetch_add(1, Ordering::SeqCst);
            }
        }));
    calls
}

#[test]
fn staging_window_confirmation_remote_all_targets_exact_read_only_and_designated() {
    for tls in [false, true] {
        for (target, append) in candidate::cases() {
            let (_dir, f) = mixed(tls, target, append);
            let before = f.observe();
            let shadows = f.shadows();
            let witness = count(&f, 0);
            let other = count(&f, 2);
            let _servers: Vec<_> = f.remote.iter().map(|peer| peer.run()).collect();
            for _ in 0..2 {
                let observation = inspect(&f).unwrap();
                assert_eq!(observation.witness, Some(NodeId::new(0)));
                assert_eq!(
                    observation.prefix.applied_log_index,
                    f.context.id().log_index().get()
                );
                assert_eq!(
                    observation.prefix.cluster_epoch,
                    f.context.id().cluster_epoch()
                );
            }
            assert_eq!(witness.load(Ordering::SeqCst), 2);
            assert_eq!(other.load(Ordering::SeqCst), 0);
            assert_eq!(f.observe(), before);
            assert_eq!(f.shadows(), shadows);
        }
    }
}

#[test]
fn staging_window_confirmation_remote_pending_cold_and_disagreeing_never_fall_back() {
    for tls in [false, true] {
        for state in ["pending", "cold", "advanced"] {
            let (_dir, f) = mixed(tls, StagingTarget::DirectPut, false);
            let id = f.context.id();
            let witness = f.node(NodeId::new(0));
            let pg = witness.get_pg(id.pg_id().get()).unwrap();
            let next = MetadataCommandId::new(
                id.cluster_epoch(),
                id.pg_id(),
                MetadataCommandLogIndex::new(id.log_index().get() + 1).unwrap(),
            );
            let next =
                ContextualCommand::decode_field(ordinary(next), &[1, 0, 0], &topology()).unwrap();
            match state {
                "pending" => install(&pg, &next),
                "cold" => pg.complete_prefix_validation.invalidate(),
                "advanced" => apply(&pg, &next),
                _ => unreachable!(),
            }
            drop(pg);
            // The shadow and every other member remain healthy and agreeing.
            let before = f.observe();
            let shadows = f.shadows();
            let calls = count(&f, 0);
            let other = count(&f, 2);
            let _servers: Vec<_> = f.remote.iter().map(|peer| peer.run()).collect();
            let error = inspect(&f).unwrap_err();
            match state {
                "pending" | "cold" => assert!(
                    matches!(error, E::Store(StoreError::StorageRpc { failure, .. }) if failure == Code::MetadataCommandContention),
                    "{error}"
                ),
                "advanced" => assert!(matches!(error, E::PrefixDisagreement), "{error}"),
                _ => unreachable!(),
            }
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(other.load(Ordering::SeqCst), 0);
            assert_eq!(f.observe(), before);
            assert_eq!(f.shadows(), shadows);
        }
    }
}

#[test]
fn staging_window_confirmation_remote_lost_reply_never_uses_shadow_or_other_peer() {
    for tls in [false, true] {
        let (_dir, f) = mixed(tls, StagingTarget::StreamPut, true);
        let before = f.observe();
        let shadows = f.shadows();
        let calls = count(&f, 0);
        let other = count(&f, 2);
        let peer = f.remote(NodeId::new(0)).unwrap();
        let _other = f.remote(NodeId::new(2)).unwrap().run();
        std::thread::scope(|scope| {
            let server =
                scope.spawn(|| peer.server.test_accept_one_without_settled_reply().unwrap());
            let error = inspect(&f).unwrap_err();
            assert!(matches!(error, E::Store(_)), "{error}");
            server.join().unwrap();
        });
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(other.load(Ordering::SeqCst), 0);
        assert_eq!(f.observe(), before);
        assert_eq!(f.shadows(), shadows);
    }
}

#[test]
fn staging_window_confirmation_remote_authentic_malformed_replies_fail_once() {
    for tls in [false, true] {
        let (_dir, f) = mixed(tls, StagingTarget::StreamPut, true);
        let before = f.observe();
        let shadows = f.shadows();
        let peer = f.remote(NodeId::new(0)).unwrap();
        let other = count(&f, 2);
        let _servers: Vec<_> = f.remote.iter().map(|peer| peer.run()).collect();
        // Request echo: version, cutoff, node, epoch, PG, complete topology;
        // prefix epoch; truncated envelope; retired raw-state grammar.
        for offset in [
            0,
            2,
            10,
            14,
            22,
            26,
            wire::REQUEST_BYTES,
            wire::REQUEST_BYTES + 8,
            wire::REQUEST_BYTES + 17,
            wire::REQUEST_BYTES + 26,
            usize::MAX - 1,
            usize::MAX,
        ] {
            let calls = Arc::new(AtomicUsize::new(0));
            let observed = Arc::clone(&calls);
            peer.server
                .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                    if kind != KIND {
                        return;
                    }
                    observed.fetch_add(1, Ordering::SeqCst);
                    let mut bytes = rpc::decode_storage_rpc_response_payload(&frame.payload)
                        .unwrap()
                        .unwrap()
                        .to_vec();
                    match offset {
                        usize::MAX => {
                            bytes.drain(..wire::REQUEST_BYTES);
                        }
                        value if value == usize::MAX - 1 => {
                            bytes.pop();
                        }
                        value => bytes[value] ^= 1,
                    }
                    frame.payload = rpc::encode_storage_rpc_success_response(&bytes);
                }));
            let error = inspect(&f).unwrap_err();
            if [
                wire::REQUEST_BYTES + 8,
                wire::REQUEST_BYTES + 17,
                wire::REQUEST_BYTES + 26,
            ]
            .contains(&offset)
            {
                assert!(matches!(error, E::PrefixDisagreement), "{offset}: {error}");
            } else {
                assert!(
                    matches!(error, E::Store(StoreError::StorageRpc { failure, .. }) if failure == Code::PayloadDecode),
                    "{offset}: {error}"
                );
            }
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(other.load(Ordering::SeqCst), 0);
            assert_eq!(f.observe(), before);
            assert_eq!(f.shadows(), shadows);
        }
    }
}

#[test]
fn staging_window_confirmation_remote_receiver_binds_role_topology_and_cutoff() {
    for tls in [false, true] {
        let (_dir, f) = mixed(tls, StagingTarget::StreamPut, true);
        let before = f.observe();
        let _servers: Vec<_> = f.remote.iter().map(|peer| peer.run()).collect();
        let id = f.context.id();
        for field in ["role", "node", "epoch", "topology", "cutoff"] {
            let node = NodeId::new(if field == "role" { 2 } else { 0 });
            let peer = f.remote(node).unwrap();
            let mut request = wire::Request {
                node,
                epoch: id.cluster_epoch(),
                pg: id.pg_id(),
                topology: wire::topology_digest(&topology()),
                deadline: rpc::StorageRpcOperationDeadline::from_instant(deadline()),
            };
            match field {
                "role" => {}
                "node" => request.node = NodeId::new(4),
                "epoch" => request.epoch = ClusterEpoch::new(id.cluster_epoch().get() + 1).unwrap(),
                "topology" => request.topology[0] ^= 1,
                "cutoff" => request.deadline.portable_wall_valid_until_ms = 1,
                _ => unreachable!(),
            }
            let error = peer
                .client
                .rpc_request_result(KIND, wire::encode_request(request))
                .unwrap()
                .unwrap_err();
            assert_eq!(
                error.code,
                match field {
                    "node" => Code::UnknownNode,
                    "topology" => Code::PayloadDecode,
                    "cutoff" => Code::TransportTimeout,
                    _ => Code::StaleShardLocation,
                },
                "{field}: {error:?}"
            );
            assert_eq!(f.observe(), before);
        }
        // The typed sender rejects wrong local subjects without transport.
        let calls = count(&f, 0);
        let peer = f.remote(NodeId::new(0)).unwrap();
        assert!(peer
            .client
            .complete_settled_witness_prefix_until(
                id.pg_id(),
                ClusterEpoch::new(id.cluster_epoch().get() + 1).unwrap(),
                deadline()
            )
            .is_err());
        assert!(peer
            .client
            .complete_settled_witness_prefix_until(PgId::new(999), id.cluster_epoch(), deadline())
            .is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn staging_window_confirmation_remote_receiver_pg_wait_obeys_wire_cutoff() {
    for tls in [false, true] {
        let (_dir, f) = mixed(tls, StagingTarget::StreamPut, true);
        let peer = f.remote(NodeId::new(0)).unwrap();
        let _server = peer.run();
        let id = f.context.id();
        let node = f.node(NodeId::new(0));
        let held = node.get_pg(id.pg_id().get()).unwrap();
        let before = snapshot(&held);
        let request = wire::Request {
            node: NodeId::new(0),
            epoch: id.cluster_epoch(),
            pg: id.pg_id(),
            topology: wire::topology_digest(&topology()),
            deadline: rpc::StorageRpcOperationDeadline {
                portable_wall_valid_until_ms: crate::clock::current_time_millis() + 250,
            },
        };
        let started = Instant::now();
        // Client IO timeout remains long: the receiver must release admission
        // itself rather than succeeding after its caller has already timed out.
        let error = peer
            .client
            .rpc_request_result(KIND, wire::encode_request(request))
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, Code::TransportTimeout, "{error:?}");
        assert!(error.message.contains("deadline"), "{error:?}");
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(snapshot(&held), before);
    }
}

#[test]
fn staging_window_confirmation_remote_rechecks_primary_after_successful_reply() {
    for tls in [false, true] {
        let (_dir, f) = mixed(tls, StagingTarget::DirectPut, false);
        let before = f.observe();
        let _servers: Vec<_> = f.remote.iter().map(|peer| peer.run()).collect();
        let id = f.context.id();
        let admission = f.handle.admit_current_route().unwrap();
        let route = admission
            .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        let clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
        let mut reached = false;
        let error = f
            .context
            .inspect_window_confirmation_embedded(&section, deadline(), |point| {
                if point == "after witness" {
                    reached = true;
                    clock.set(crate::clock::current_time_millis() + 1_000_000);
                }
            })
            .unwrap_err();
        assert!(reached);
        assert!(
            matches!(error, E::Store(StoreError::RouteMapExpired { .. })),
            "{error}"
        );
        assert_eq!(f.observe(), before);
    }
}

#[test]
fn staging_window_confirmation_remote_receiver_route_expiry_survives_renewal() {
    for tls in [false, true] {
        let (_dir, f) = mixed(tls, StagingTarget::StreamPut, true);
        let peer = f.remote(NodeId::new(0)).unwrap();
        let before = f.observe();
        peer.server
            .test_settled_witness_permits_and_live_dispatch(f.context.id().pg_id());
        let _server = peer.run();
        let id = f.context.id();
        let request = wire::Request {
            node: peer.node,
            epoch: id.cluster_epoch(),
            pg: id.pg_id(),
            topology: wire::topology_digest(&topology()),
            deadline: rpc::StorageRpcOperationDeadline::from_instant(deadline()),
        };
        let started = Instant::now();
        let response = peer.server.test_settled_witness_route_wait(id.pg_id(), || {
            peer.client
                .rpc_request_result(KIND, wire::encode_request(request))
        });
        let error = response.unwrap().unwrap_err();
        assert_eq!(error.code, Code::MetadataCommandContention, "{error:?}");
        assert!(error.message.contains("deadline"), "{error:?}");
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(f.observe(), before);
        // A separate request can use the renewed route, not the old request.
        inspect(&f).unwrap();
    }
}
