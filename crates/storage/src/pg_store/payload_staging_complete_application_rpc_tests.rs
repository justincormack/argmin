// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Authenticated installed receiver; raw frame construction is owner-test-only.
//! This does not supply a production sender or authorize terminal cleanup.

use super::*;
use crate::node_runtime::clients::UnixStorageNodeClient;
use crate::node_runtime::pg_store::CompleteApplicationQuery;
use crate::storage_node_server::{StorageNodeProcessConfig, StorageNodeServer};
use crate::storage_rpc::{
    self as rpc, complete_application as wire, StorageRpcErrorCode as Code,
    StorageRpcMessageKind as Kind, StorageRpcOperationDeadline,
};

const KIND: Kind = Kind::MetadataCommandApplyAndRecord;

pub(super) fn encode_at(context: &ContextualCommand, node_id: NodeId, cutoff: u64) -> Vec<u8> {
    let command = context.encode_command().unwrap();
    wire::encode_request(wire::BorrowedRequest {
        command: rpc::complete_applied::BorrowedRequest {
            node_id,
            epoch: context.id().cluster_epoch(),
            pg_id: context.id().pg_id(),
            checksum: command.checksum_crc64(),
            command: command.bytes(),
        },
        deadline: StorageRpcOperationDeadline {
            portable_wall_valid_until_ms: cutoff,
        },
    })
    .unwrap()
}

pub(super) fn setup(
    tls: bool,
    case: usize,
    replica: bool,
    dir: &test_util::TempDir,
) -> (
    StorageNodeServer,
    UnixStorageNodeClient,
    StorageNodeProcessConfig,
    ContextualCommand,
) {
    let mut config = config(case);
    if replica {
        config.node_id = NodeId::new(2);
    }
    let (server, client, config) = if tls {
        config.data_dir = dir.path().join("node");
        config.socket_path = dir.path().join("unused.sock");
        let (server, client) =
            super::tls::tls_server_at(config.clone(), false, config.cluster_epoch);
        (server, client, config)
    } else {
        installed_config(dir, config, true)
    };
    let node = server.test_storage_node();
    let pg = node
        .get_pg(fixture(StagingTarget::DirectPut).subject.object_pg.get())
        .unwrap();
    if case == 1 {
        let digest = pg.metadata_command_replica_state().unwrap().state_digest;
        pg.initialize_metadata_transfer_empty_state(
            config.node_id.as_u32(),
            config.cluster_epoch,
            digest,
        )
        .unwrap();
    }
    let context = context(&pg, case);
    if !replica {
        install(&pg, &context);
    }
    drop(pg);
    if !replica {
        let evidence = pending(&context);
        let request = evidence
            .publication_request(config.node_id, &topology())
            .unwrap();
        server
            .test_with_complete_publication_authority(&request, deadline(), |pg, authority| {
                request.start(pg, authority)
            })
            .unwrap()
            .unwrap();
    }
    (server, client, config, context)
}

fn exchange(
    server: &StorageNodeServer,
    client: &UnixStorageNodeClient,
    payload: Vec<u8>,
    expire: bool,
    lose_reply: bool,
) -> Result<Result<Vec<u8>, rpc::StorageRpcErrorResponse>, StoreError> {
    std::thread::scope(|scope| {
        let worker = scope.spawn(|| {
            server
                .test_accept_one_complete_application_fault(expire, lose_reply)
                .unwrap()
        });
        let result = client.rpc_request_result(KIND, payload);
        worker.join().unwrap();
        result
    })
}

fn confirm(
    context: &ContextualCommand,
    node: NodeId,
    response: &[u8],
) -> crate::metadata_command::MetadataCommandReplicaState {
    wire::decode_confirmation_for(
        response,
        node,
        context.id(),
        context.encode_command().unwrap().command_sha256(),
    )
    .unwrap()
}

#[test]
fn staging_complete_application_rpc_unix_and_tls_all_contexts_lost_reply_retry_and_restart() {
    for tls in [false, true] {
        for replica in [false, true] {
            for case in 0..4 {
                let dir = test_util::tempdir();
                let (server, client, config, context) = setup(tls, case, replica, &dir);
                let node = server.test_storage_node();
                let pg_id = context.id().pg_id().get();
                let payload = encode_at(
                    &context,
                    config.node_id,
                    crate::clock::current_time_millis() + 60_000,
                );
                let before = snapshot(&node.get_pg(pg_id).unwrap());
                assert!(
                    exchange(&server, &client, payload.clone(), false, true).is_err(),
                    "committed reply must be lost"
                );
                let applied = snapshot(&node.get_pg(pg_id).unwrap());
                assert_ne!(before, applied, "case {case}, tls {tls}, replica {replica}");
                let writes = node.get_pg(pg_id).unwrap().conn.total_changes();
                let response = exchange(&server, &client, payload.clone(), false, false)
                    .unwrap()
                    .unwrap();
                let state = confirm(&context, config.node_id, &response);
                assert_eq!(state.applied_log_index, context.id().log_index().get());
                assert_eq!(snapshot(&node.get_pg(pg_id).unwrap()), applied);
                assert_eq!(node.get_pg(pg_id).unwrap().conn.total_changes(), writes);
                if !replica {
                    assert!(
                        started(&node.get_pg(pg_id).unwrap()),
                        "application cannot retire pending custody"
                    );
                }
                drop(client);
                drop(server);
                drop(node);
                let (server, client) = if tls {
                    super::tls::tls_server_at(config.clone(), true, config.cluster_epoch)
                } else {
                    let (prepared, client) = prepared(&config);
                    (prepared.bind_complete_for_test().unwrap(), client)
                };
                let node = server.test_storage_node();
                let writes = node.get_pg(pg_id).unwrap().conn.total_changes();
                let response = exchange(&server, &client, payload, false, false)
                    .unwrap()
                    .unwrap();
                assert_eq!(confirm(&context, config.node_id, &response), state);
                assert_eq!(snapshot(&node.get_pg(pg_id).unwrap()), applied);
                assert_eq!(node.get_pg(pg_id).unwrap().conn.total_changes(), writes);
            }
        }
    }
}

#[test]
fn staging_complete_application_rpc_unix_and_tls_expiry_before_arrival_and_after_decode() {
    for tls in [false, true] {
        for delayed in [false, true] {
            let _clock = crate::clock::test_time_override_guard(9_000);
            let dir = test_util::tempdir();
            let (server, client, config, context) = setup(tls, 0, true, &dir);
            let node = server.test_storage_node();
            let pg_id = context.id().pg_id().get();
            let payload = encode_at(
                &context,
                config.node_id,
                if delayed { 10_500 } else { 9_999 },
            );
            let before = snapshot(&node.get_pg(pg_id).unwrap());
            let writes = node.get_pg(pg_id).unwrap().conn.total_changes();
            std::thread::scope(|scope| {
                let worker = scope.spawn(|| {
                    let _clock = crate::clock::test_time_override_guard(10_000);
                    server
                        .test_accept_one_complete_application_fault(delayed, false)
                        .unwrap();
                });
                let error = client
                    .test_complete_application_request_until(payload, deadline())
                    .unwrap_or_else(|error| match &error {
                        StoreError::StorageRpc { detail, .. } => panic!(
                            "tls {tls}, delayed {delayed}: {error:?}: {}",
                            detail.as_str()
                        ),
                        _ => panic!("tls {tls}, delayed {delayed}: {error:?}"),
                    })
                    .unwrap_err();
                assert_eq!(error.code, Code::TransportTimeout, "{error:?}");
                worker.join().unwrap();
            });
            assert_eq!(snapshot(&node.get_pg(pg_id).unwrap()), before);
            assert_eq!(node.get_pg(pg_id).unwrap().conn.total_changes(), writes);
        }
    }
}

#[test]
fn staging_complete_application_rpc_rejects_malformed_and_crossed_inputs_without_effects() {
    let dir = test_util::tempdir();
    let (server, client, config, context) = setup(false, 0, true, &dir);
    let node = server.test_storage_node();
    let pg_id = context.id().pg_id().get();
    let payload = encode_at(
        &context,
        config.node_id,
        crate::clock::current_time_millis() + 60_000,
    );
    let before = snapshot(&node.get_pg(pg_id).unwrap());
    let writes = node.get_pg(pg_id).unwrap().conn.total_changes();
    let mut cases = vec![(payload[..payload.len() - 1].to_vec(), Code::PayloadDecode)];
    for (offset, code) in [
        (0, Code::PayloadDecode),
        (4, Code::PayloadDecode),
        (14, Code::UnknownNode),
        (18, Code::PayloadDecode),
        (26, Code::PayloadDecode),
        (30, Code::PayloadDecode),
    ] {
        let mut bytes = payload.clone();
        bytes[offset] ^= 1;
        cases.push((bytes, code));
    }
    for (offset, value) in [(69, 13), (payload.len() - 3, 99)] {
        let mut bytes = payload.clone();
        bytes[offset] = value;
        let checksum = checksum::crc64::checksum(&bytes[42..]);
        bytes[30..38].copy_from_slice(&checksum.to_le_bytes());
        cases.push((bytes, Code::PayloadDecode));
    }
    let mut trailing = payload.clone();
    trailing.push(0);
    cases.push((trailing, Code::PayloadDecode));
    cases.push((
        super::transport::encode_at(&context, crate::clock::current_time_millis() + 60_000),
        Code::PayloadDecode,
    ));
    for (bytes, code) in cases {
        let error = exchange(&server, &client, bytes, false, false)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, code, "{error:?}");
        assert_eq!(snapshot(&node.get_pg(pg_id).unwrap()), before);
        assert_eq!(node.get_pg(pg_id).unwrap().conn.total_changes(), writes);
    }
    let reply = exchange(&server, &client, payload, false, false)
        .unwrap()
        .unwrap();
    confirm(&context, config.node_id, &reply);
}

#[test]
fn staging_complete_application_rpc_unselected_nonactive_nonmember_and_historical_fail_closed() {
    for mode in ["unselected", "peering", "nonmember", "historical"] {
        let dir = test_util::tempdir();
        let mut config = config(0);
        config.node_id = NodeId::new(2);
        if mode == "historical" {
            config.cluster_epoch = ClusterEpoch::new(6).unwrap();
        }
        for route in &mut config.pg_routes {
            route.cluster_epoch = config.cluster_epoch;
            if mode == "peering" {
                route.state = PgState::Peering;
            }
            if mode == "nonmember" {
                route.acting_set.retain(|id| *id != config.node_id);
            }
        }
        let (server, client, config) = installed_config(&dir, config, mode != "unselected");
        let node = server.test_storage_node();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg.get();
        let context = context(&node.get_pg(pg_id).unwrap(), 0);
        let before = snapshot(&node.get_pg(pg_id).unwrap());
        let writes = node.get_pg(pg_id).unwrap().conn.total_changes();
        let payload = encode_at(
            &context,
            config.node_id,
            crate::clock::current_time_millis() + 60_000,
        );
        let error = exchange(&server, &client, payload, false, false)
            .unwrap()
            .unwrap_err();
        assert_eq!(
            error.code,
            match mode {
                "unselected" => Code::PayloadDecode,
                "peering" => Code::InactivePgRoute,
                "nonmember" => Code::NonActingSetAccess,
                _ => Code::StaleShardLocation,
            },
            "{mode}: {error:?}"
        );
        assert_eq!(snapshot(&node.get_pg(pg_id).unwrap()), before);
        assert_eq!(node.get_pg(pg_id).unwrap().conn.total_changes(), writes);
    }
}

#[test]
fn staging_complete_application_rpc_bad_authentication_and_crossed_confirmation_keep_applied_state()
{
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    for tls in [false, true] {
        for bad_auth in [false, true] {
            let dir = test_util::tempdir();
            let (server, client, config, context) = setup(tls, 1, true, &dir);
            let node = server.test_storage_node();
            let pg_id = context.id().pg_id().get();
            let payload = encode_at(
                &context,
                config.node_id,
                crate::clock::current_time_millis() + 60_000,
            );
            let replies = Arc::new(AtomicUsize::new(0));
            let observed = Arc::clone(&replies);
            server.set_response_frame_test_hook(Arc::new(move |kind, frame| {
                if kind == KIND && observed.fetch_add(1, Ordering::SeqCst) == 0 && !bad_auth {
                    let mut reply = rpc::decode_storage_rpc_response_payload(&frame.payload)
                        .unwrap()
                        .unwrap();
                    reply[30] ^= 1; // Authentication covers this crossed complete-command digest.
                    frame.payload = rpc::encode_storage_rpc_success_response(&reply);
                }
            }));
            if bad_auth {
                let observed = Arc::clone(&replies);
                server.set_response_envelope_test_hook(Arc::new(move |kind, envelope| {
                    if kind == KIND && observed.load(Ordering::SeqCst) == 1 {
                        *envelope.last_mut().unwrap() ^= 1;
                    }
                }));
            }
            let before = snapshot(&node.get_pg(pg_id).unwrap());
            let result = exchange(&server, &client, payload.clone(), false, false);
            if bad_auth {
                assert!(result.is_err(), "unauthenticated response accepted");
            } else {
                let response = result.unwrap().unwrap();
                wire::decode_confirmation(&response).unwrap();
                assert!(wire::decode_confirmation_for(
                    &response,
                    config.node_id,
                    context.id(),
                    context.encode_command().unwrap().command_sha256()
                )
                .is_err());
            }
            assert_eq!(
                replies.load(Ordering::SeqCst),
                1,
                "transport must not retry automatically"
            );
            let applied = snapshot(&node.get_pg(pg_id).unwrap());
            assert_ne!(applied, before);
            let writes = node.get_pg(pg_id).unwrap().conn.total_changes();
            let response = exchange(&server, &client, payload, false, false)
                .unwrap()
                .unwrap();
            confirm(&context, config.node_id, &response);
            assert_eq!(replies.load(Ordering::SeqCst), 2);
            assert_eq!(snapshot(&node.get_pg(pg_id).unwrap()), applied);
            assert_eq!(node.get_pg(pg_id).unwrap().conn.total_changes(), writes);
        }
    }
}

#[test]
fn staging_complete_application_query_projection_cannot_renew_received_deadline() {
    let clock = crate::clock::test_time_override_guard(1_000);
    let (_dir, _pg, context) = complete_application::setup(5);
    let bytes = encode_at(&context, NodeId::new(4), 50_000);
    assert_eq!(
        bytes,
        crate::tests::frozen_hex_fixture(include_str!(
            "testdata/complete_application_payload_v1_command_v14_context_v1.hex"
        ))
    );
    let query = CompleteApplicationQuery::decode(&bytes, &topology()).unwrap();
    let cutoff = query.deadline();
    let later = cutoff + Duration::from_secs(60);
    for now in [2_000, 1_000, 100_000] {
        clock.set(now);
        query
            .with_request(later, |request| {
                assert_eq!(request.deadline(), cutoff);
                assert_eq!(request.id(), context.id());
            })
            .unwrap();
    }
    let earlier = cutoff - Duration::from_millis(1);
    query
        .with_request(earlier, |request| assert_eq!(request.deadline(), earlier))
        .unwrap();
    assert_eq!(query.deadline(), cutoff);
}

#[test]
fn staging_complete_application_confirmation_rejects_independent_subject_substitution() {
    let (_dir, _pg, context) = complete_application::setup(5);
    let response = crate::tests::frozen_hex_fixture(include_str!(
        "testdata/complete_application_confirmation_v1_command_v14_context_v1_state_v10.hex"
    ));
    let digest = context.encode_command().unwrap().command_sha256();
    assert!(wire::decode_confirmation_for(&response, NodeId::new(4), context.id(), digest).is_ok());
    for offset in [6, 18, 22, 30] {
        let mut crossed = response.clone();
        crossed[offset] ^= 1;
        // Ensure framing remains valid: the failure is request binding, not decode.
        wire::decode_confirmation(&crossed).unwrap();
        assert!(
            wire::decode_confirmation_for(&crossed, NodeId::new(4), context.id(), digest).is_err(),
            "{offset}"
        );
    }
    let mut crossed_epoch = response.clone();
    crossed_epoch[10] ^= 1;
    crossed_epoch[62] ^= 1;
    wire::decode_confirmation(&crossed_epoch).unwrap();
    assert!(
        wire::decode_confirmation_for(&crossed_epoch, NodeId::new(4), context.id(), digest)
            .is_err()
    );
    let publication = crate::tests::frozen_hex_fixture(include_str!(
        "testdata/complete_publication_confirmation_v1_command_v14_context_v1.hex"
    ));
    assert!(
        wire::decode_confirmation_for(&publication, NodeId::new(4), context.id(), digest).is_err()
    );
}
