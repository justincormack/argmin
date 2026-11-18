// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Installed candidate receiver prerequisite. Replies observe durable authority;
//! they do not mint file writers or permit any remote physical mutation.
use super::*;
use crate::storage_rpc::{
    self as rpc, complete_settled_prefix as prefix, complete_window_primary as wire,
    StorageRpcErrorCode as Code, StorageRpcMessageKind as Kind,
};

#[test]
fn staging_direct_file_primary_requires_live_exact_reservation_without_mutation() {
    for tls in [false, true] {
        for change in ["none", "missing", "generation", "issuer", "expired"] {
            let (_dir, f, proof) = allocation_remote::setup(tls, StagingTarget::DirectPut, false);
            run_terminal(
                &f,
                proof.as_ref(),
                "installed",
                |f, primary, mut terminal| {
                    primary.retire(&mut terminal);
                    let confirmed = primary
                        .confirm(terminal, deadline(), |_| {})
                        .unwrap_or_else(|e| panic!("{}", e.error()));
                    let Primary::Remote(_, session) = primary else {
                        unreachable!()
                    };
                    let field = f.context.encode_field().unwrap();
                    let id = f.context.id();
                    let mut request = wire::Request {
                        route: prefix::Request {
                            node: NodeId::new(4),
                            epoch: id.cluster_epoch(),
                            pg: id.pg_id(),
                            deadline: rpc::StorageRpcOperationDeadline::from_instant(deadline()),
                            topology: prefix::topology_digest(&topology()),
                        },
                        descriptor: &field[7..],
                    };
                    let node = f.node(NodeId::new(4));
                    {
                        let pg = node.get_pg(id.pg_id().get()).unwrap();
                        match change {
                            "none" | "expired" => {}
                            "missing" => {
                                pg.conn
                                    .execute("DELETE FROM object_generation_reservations", [])
                                    .unwrap();
                            }
                            "generation" => {
                                pg.conn.execute("UPDATE object_generation_reservations SET generation_id = generation_id + 1", []).unwrap();
                            }
                            "issuer" => {
                                pg.begin_allocation_lifetime_replacement()
                                    .unwrap()
                                    .complete()
                                    .unwrap();
                            }
                            _ => unreachable!(),
                        }
                        if change == "issuer" {
                            // A new allocation lifetime leaves canonical state
                            // unchanged. Validate it explicitly so the rejection
                            // must come from the original-issuer comparison.
                            pg.recover_complete_command_state(
                                PgStoreRecoveryContext::for_node(NodeId::new(4)),
                                &topology(),
                            )
                            .unwrap();
                        }
                        if matches!(change, "missing" | "generation") {
                            // These are canonical-state corruptions, so startup
                            // must not bless the altered digest. Check the exact
                            // reservation predicate in a read snapshot as well
                            // as the installed receiver's fail-closed prefix.
                            let snapshot = pg.command_build_snapshot().unwrap();
                            let description = crate::node_runtime::pg_store::StagingWindowInspectionDescription::decode(request.descriptor).unwrap();
                            let topology = topology();
                            let error = description
                                .request(&topology)
                                .unwrap()
                                .require_direct_file_reservation(&pg, NodeId::new(4))
                                .unwrap_err();
                            assert!(
                                error.to_string().contains(if change == "generation" {
                                    "reservation generation"
                                } else {
                                    "reservation"
                                }),
                                "{error}"
                            );
                            snapshot.finish().unwrap();
                        }
                    }
                    // In particular, issuer replacement still permits ordinary
                    // inspection: only the new live-issuer check rejects it.
                    let ordinary = session
                        .test_window_primary_request(wire::encode_request(request).unwrap())
                        .unwrap();
                    if matches!(change, "missing" | "generation") {
                        let error = ordinary.unwrap_err();
                        assert_eq!(error.code, Code::MetadataCommandContention);
                        assert!(
                            error.message.contains("requires explicit local validation"),
                            "{error:?}"
                        );
                    } else {
                        wire::decode_response(&ordinary.unwrap(), request, Some(NodeId::new(0)))
                            .unwrap();
                    }
                    if change == "expired" {
                        request.route.deadline.portable_wall_valid_until_ms = 1;
                    }
                    let before = f.observe();
                    let shadows = f.shadows();
                    let reply = session
                        .test_window_primary_request(
                            wire::encode_request_for(wire::Purpose::DirectFileBinding, request)
                                .unwrap(),
                        )
                        .unwrap();
                    if change == "none" {
                        let reply = reply.unwrap();
                        let observed = wire::decode_response_for(
                            wire::Purpose::DirectFileBinding,
                            &reply,
                            request,
                            Some(NodeId::new(0)),
                        )
                        .unwrap();
                        assert_eq!(observed.applied_log_index, id.log_index().get());
                        assert!(
                            wire::decode_response(&reply, request, Some(NodeId::new(0))).is_err()
                        );
                        f.remote(NodeId::new(4))
                            .unwrap()
                            .server
                            .test_direct_file_primary_admission(
                                &wire::encode_request_for(
                                    wire::Purpose::DirectFileBinding,
                                    request,
                                )
                                .unwrap(),
                            );
                        for crossed in ["node", "epoch", "topology", "descriptor"] {
                            let mut bad = request;
                            let mut descriptor = request.descriptor.to_vec();
                            match crossed {
                                "node" => bad.route.node = NodeId::new(0),
                                "epoch" => {
                                    bad.route.epoch =
                                        ClusterEpoch::new(id.cluster_epoch().get() + 1).unwrap()
                                }
                                "topology" => bad.route.topology[0] ^= 1,
                                "descriptor" => {
                                    descriptor[10] = 99;
                                    bad.descriptor = &descriptor;
                                }
                                _ => unreachable!(),
                            }
                            let error = session
                                .test_window_primary_request(
                                    wire::encode_request_for(wire::Purpose::DirectFileBinding, bad)
                                        .unwrap(),
                                )
                                .unwrap()
                                .unwrap_err();
                            assert_eq!(
                                error.code,
                                match crossed {
                                    "node" => Code::UnknownNode,
                                    "epoch" => Code::StaleShardLocation,
                                    _ => Code::PayloadDecode,
                                },
                                "{crossed}: {error:?}"
                            );
                        }
                    } else {
                        let error = reply.unwrap_err();
                        assert_eq!(
                            error.code,
                            if change == "expired" {
                                Code::TransportTimeout
                            } else {
                                Code::MetadataCommandContention
                            },
                            "{change}: {error:?}"
                        );
                        if change == "issuer" {
                            assert!(error.message.contains("target/issuer"), "{error:?}");
                        }
                    }
                    assert_eq!(f.observe(), before);
                    assert_eq!(f.shadows(), shadows);
                    if change == "none" {
                        // Keep the object PG unavailable until the receiver
                        // itself returns; client IO timeout remains much longer.
                        let held = node.get_pg(id.pg_id().get()).unwrap();
                        let state = snapshot(&held);
                        let changes = held.conn.total_changes();
                        let mut waiting = request;
                        waiting.route.deadline.portable_wall_valid_until_ms =
                            crate::clock::current_time_millis() + 250;
                        let started = Instant::now();
                        let error = session
                            .test_window_primary_request(
                                wire::encode_request_for(wire::Purpose::DirectFileBinding, waiting)
                                    .unwrap(),
                            )
                            .unwrap()
                            .unwrap_err();
                        assert_eq!(error.code, Code::TransportTimeout, "{error:?}");
                        assert!(started.elapsed() < Duration::from_secs(2));
                        assert_eq!(snapshot(&held), state);
                        assert_eq!(held.conn.total_changes(), changes);
                    }
                    drop(confirmed);
                },
            );
        }
    }
}

#[test]
fn staging_direct_file_primary_rejects_stream_targets_before_effects() {
    for tls in [false, true] {
        for target in [StagingTarget::StreamPut, multipart_target()] {
            let (_dir, f, proof) = allocation_remote::setup(tls, target, false);
            run_terminal(
                &f,
                proof.as_ref(),
                "installed",
                |f, primary, mut terminal| {
                    primary.retire(&mut terminal);
                    let Primary::Remote(_, session) = primary else {
                        unreachable!()
                    };
                    let field = f.context.encode_field().unwrap();
                    let id = f.context.id();
                    let request = wire::Request {
                        route: prefix::Request {
                            node: NodeId::new(4),
                            epoch: id.cluster_epoch(),
                            pg: id.pg_id(),
                            deadline: rpc::StorageRpcOperationDeadline::from_instant(deadline()),
                            topology: prefix::topology_digest(&topology()),
                        },
                        descriptor: &field[7..],
                    };
                    session
                        .test_window_primary_request(wire::encode_request(request).unwrap())
                        .unwrap()
                        .unwrap();
                    let before = f.observe();
                    let error = session
                        .test_window_primary_request(
                            wire::encode_request_for(wire::Purpose::DirectFileBinding, request)
                                .unwrap(),
                        )
                        .unwrap()
                        .unwrap_err();
                    assert_eq!(error.code, Code::MetadataCommandContention);
                    assert!(error.message.contains("target/issuer"), "{error:?}");
                    assert_eq!(f.observe(), before);
                    let peer = f.remote(NodeId::new(4)).unwrap();
                    // A separate connection does not inherit the original section.
                    let error = peer
                        .client
                        .rpc_request_result(
                            Kind::MetadataCommandReplicaState,
                            wire::encode_request_for(wire::Purpose::DirectFileBinding, request)
                                .unwrap(),
                        )
                        .unwrap()
                        .unwrap_err();
                    assert_eq!(error.code, Code::MetadataCommandContention);
                    assert!(error.message.contains("already-held"), "{error:?}");
                    assert_eq!(f.observe(), before);
                },
            );
        }
    }
}
