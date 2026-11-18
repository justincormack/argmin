// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Metadata-only complete pending observations through real embedded and
//! authenticated installed Unix interfaces. No recovery or cleanup is granted.

use super::complete_log_range::apply;
use super::complete_log_rpc::{installed_at_epoch, prepare_context, prepared, Running};
use super::complete_node_startup::{install, snapshot};
use super::*;
use crate::metadata_command::{
    MetadataCommandEnvelope, MetadataCommandPayload, ReserveObjectVersionCommand,
};
use crate::node_runtime::clients::{LocalStorageNodeClient, UnixStorageNodeClient};
use crate::node_runtime::pg_store::{
    CompletePendingEvidence, CompletePendingInspectionError, CompletePendingRequest,
};
use crate::storage_rpc::{
    self as rpc, complete_pending as wire, StorageRpcErrorCode, StorageRpcMessageKind,
};
use crate::{PgState, StoreError, VersionId};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

const KIND: StorageRpcMessageKind = StorageRpcMessageKind::MetadataCommandPendingEnvelope;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(30)
}

fn assert_context(evidence: CompletePendingEvidence, context: &ContextualCommand, started: bool) {
    assert_eq!(evidence.id(), Some(context.id()));
    assert_eq!(evidence.publication_started(), started);
    assert_eq!(
        evidence
            .context()
            .unwrap()
            .encode_command()
            .unwrap()
            .bytes(),
        context.encode_command().unwrap().bytes()
    );
}

#[test]
fn staging_complete_pending_rpc_contexts_and_terminal_custody_survive_restart() {
    for case in 0..3 {
        for terminal in ["unresolved", "applied", "abandoned"] {
            for historical in [false, true] {
                let dir = test_util::tempdir();
                let epoch = ClusterEpoch::new(if case == 2 { 6 } else { 5 }).unwrap();
                let route_epoch = ClusterEpoch::new(epoch.get() + u64::from(historical)).unwrap();
                let (server, client, config) = installed_at_epoch(
                    &dir,
                    if historical {
                        PgState::Peering
                    } else {
                        PgState::Active
                    },
                    route_epoch,
                    true,
                );
                let node = server.test_storage_node();
                let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
                let started = terminal != "unresolved";
                let (context, before, writes) = {
                    let pg = node.get_pg(pg_id.get()).unwrap();
                    if case == 1 {
                        // This introduction is the first command in epoch 5.
                        // Establish that empty durable prefix before installing
                        // it; startup must not trust a future-epoch pending slot.
                        let digest = pg.metadata_command_replica_state().unwrap().state_digest;
                        pg.initialize_metadata_transfer_empty_state(4, epoch, digest)
                            .unwrap();
                    }
                    let context = prepare_context(&pg, case);
                    install(&pg, &context);
                    pg.conn
                        .execute(
                            "UPDATE metadata_command_pending_slot SET publication_started = ?1",
                            [started],
                        )
                        .unwrap();
                    match terminal {
                        "unresolved" => {}
                        "applied" => apply(&pg, &context),
                        "abandoned" => {
                            pg.test_record_complete_command_abandoned(
                                4,
                                &context.encode_command().unwrap(),
                            )
                            .unwrap();
                        }
                        _ => unreachable!(),
                    }
                    (context, snapshot(&pg), pg.conn.total_changes())
                };
                let local = LocalStorageNodeClient::new(config.node_id, Arc::clone(&node));
                assert_context(
                    local
                        .complete_pending_command_until(pg_id, epoch, deadline())
                        .unwrap(),
                    &context,
                    started,
                );
                let running = Running::start(server, config.socket_path.clone());
                assert_context(
                    client
                        .complete_pending_command_until(pg_id, epoch, deadline())
                        .unwrap(),
                    &context,
                    started,
                );
                if historical {
                    let absent = client
                        .complete_pending_command_until(pg_id, route_epoch, deadline())
                        .unwrap();
                    assert!(absent.id().is_none());
                    assert!(!absent.publication_started());
                }
                let pg = node.get_pg(pg_id.get()).unwrap();
                let topology = topology();
                let strict = CompletePendingRequest::new(pg_id, route_epoch, &topology).unwrap();
                if historical {
                    let error = strict
                        .read_current_slot(&pg, NodeId::new(4))
                        .err()
                        .expect("historical slot is not clear");
                    assert!(
                        error.to_string().contains("inspect direct contender epoch"),
                        "{error}"
                    );
                } else {
                    assert_context(
                        strict.read_current_slot(&pg, NodeId::new(4)).unwrap(),
                        &context,
                        started,
                    );
                }
                assert_eq!(snapshot(&pg), before);
                assert_eq!(pg.conn.total_changes(), writes);
                drop(pg);
                drop(running);
                drop(local);
                drop(node);
                // No duplicate runtime: close the original before recovery and
                // validate the same custody through the restarted installed RPC.
                let (prepared, client) = prepared(&config);
                let server = prepared.bind_complete_for_test().unwrap_or_else(|error| {
                    if let crate::storage_node_server::StorageNodeServerError::Store(failure) =
                        &error
                    {
                        panic!(
                            "case {case}, {terminal}, historical={historical}: {:?}",
                            failure.retained_store_error()
                        );
                    }
                    panic!("case {case}, {terminal}, historical={historical}: {error:?}");
                });
                let node = server.test_storage_node();
                let _running = Running::start(server, config.socket_path);
                assert_context(
                    client
                        .complete_pending_command_until(pg_id, epoch, deadline())
                        .unwrap(),
                    &context,
                    started,
                );
                assert_eq!(snapshot(&node.get_pg(pg_id.get()).unwrap()), before);
            }
        }
    }
}

#[test]
fn staging_complete_pending_rpc_rejects_damaged_custody_even_outside_requested_epoch() {
    for damage in ["scope", "routes", "staging", "checksum", "terminal"] {
        let dir = test_util::tempdir();
        let (server, client, config) =
            installed_at_epoch(&dir, PgState::Peering, ClusterEpoch::new(6).unwrap(), true);
        let node = server.test_storage_node();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
        let (before, writes) = {
            let pg = node.get_pg(pg_id.get()).unwrap();
            let context = prepare_context(&pg, 1);
            install(&pg, &context);
            if damage == "terminal" {
                apply(&pg, &context);
            }
            pg.conn.execute_batch(match damage {
                "scope" => "UPDATE metadata_command_pending_slot SET scope_bucket = 'crossed'",
                "routes" => "DELETE FROM metadata_command_pending_route_dependencies",
                "staging" => "DELETE FROM metadata_command_pending_staging_protection",
                "checksum" => "UPDATE metadata_command_pending_slot SET command_checksum = command_checksum + 1",
                "terminal" => "UPDATE metadata_command_log SET command_checksum = command_checksum + 1 WHERE abandoned = 0",
                _ => unreachable!(),
            }).unwrap();
            (snapshot(&pg), pg.conn.total_changes())
        };
        let local = LocalStorageNodeClient::new(config.node_id, Arc::clone(&node));
        let _running = Running::start(server, config.socket_path);
        for epoch in [ClusterEpoch::new(5).unwrap(), config.cluster_epoch] {
            assert!(
                local
                    .complete_pending_command_until(pg_id, epoch, deadline())
                    .is_err(),
                "{damage}"
            );
            assert!(
                client
                    .complete_pending_command_until(pg_id, epoch, deadline())
                    .is_err(),
                "{damage}"
            );
        }
        let pg = node.get_pg(pg_id.get()).unwrap();
        assert_eq!(snapshot(&pg), before);
        assert_eq!(pg.conn.total_changes(), writes);
        assert!(pg.conn.is_autocommit());
    }
}

fn encoded(context: &ContextualCommand) -> Vec<u8> {
    let command = context.encode_command().unwrap();
    wire::encode(Some((command.checksum_crc64(), command.bytes())), true).unwrap()
}

#[test]
fn staging_complete_pending_rpc_rejects_malformed_authenticated_responses() {
    let dir = test_util::tempdir();
    let (server, client, config) =
        installed_at_epoch(&dir, PgState::Active, ClusterEpoch::new(5).unwrap(), true);
    let node = server.test_storage_node();
    let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
    let (context, before, writes) = {
        let pg = node.get_pg(pg_id.get()).unwrap();
        let context = prepare_context(&pg, 0);
        install(&pg, &context);
        (context, snapshot(&pg), pg.conn.total_changes())
    };
    let valid = encoded(&context);
    let mut cases = vec![
        vec![0, 1],
        vec![2, 0],
        vec![0, 0, 0],
        valid[..valid.len() - 1].to_vec(),
    ];
    let mut bad = valid.clone();
    *bad.last_mut().unwrap() = 2;
    cases.push(bad);
    let mut bad = valid.clone();
    bad[1] ^= 1;
    cases.push(bad);
    let mut bad = valid.clone();
    bad.push(0);
    cases.push(bad);
    // Valid, independently checksummed commands on a different PG or epoch
    // must fail request binding, not merely malformed command decoding.
    let owner = fixture(StagingTarget::DirectPut);
    let crossed_key = (0..100)
        .map(|i| crate::tests::object_key(format!("crossed-{i}")))
        .find(|key| topology().object_pg_for(&owner.subject.bucket, key) != pg_id.get())
        .unwrap();
    for (key, epoch) in [
        (crossed_key, config.cluster_epoch),
        (owner.subject.key.clone(), ClusterEpoch::new(4).unwrap()),
    ] {
        let target_pg = PgId::new(topology().object_pg_for(&owner.subject.bucket, &key));
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(epoch, target_pg, context.id().log_index()),
            MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
                owner.subject.bucket.clone(),
                key,
                VersionId::from_u64(17),
            )),
        );
        let context = ContextualCommand::decode_field(command, &[1, 0, 0], &topology()).unwrap();
        // Positive canary: this is a valid context for its own scope.
        CompletePendingRequest::new(target_pg, epoch, &topology())
            .unwrap()
            .decode_response(&encoded(&context))
            .unwrap();
        cases.push(encoded(&context));
    }
    let old = complete_log_range::ordinary(context.id());
    cases.push(wire::encode(Some((old.checksum_crc64(), &old.command_bytes())), true).unwrap());
    let sent = Arc::new(AtomicUsize::new(0));
    let response = Arc::new(std::sync::Mutex::new(valid.clone()));
    let hook_response = Arc::clone(&response);
    let count = Arc::clone(&sent);
    server.set_response_frame_test_hook(Arc::new(move |kind, frame| {
        if kind == KIND {
            count.fetch_add(1, Ordering::SeqCst);
            frame.payload =
                rpc::encode_storage_rpc_success_response(&hook_response.lock().unwrap());
        }
    }));
    let _running = Running::start(server, config.socket_path);
    assert_context(
        client
            .complete_pending_command_until(pg_id, config.cluster_epoch, deadline())
            .unwrap(),
        &context,
        true,
    );
    for (i, bytes) in cases.into_iter().enumerate() {
        *response.lock().unwrap() = bytes;
        let error =
            match client.complete_pending_command_until(pg_id, config.cluster_epoch, deadline()) {
                Ok(_) => panic!("case {i} accepted"),
                Err(error) => error,
            };
        assert!(
            matches!(error, StoreError::StorageRpc { failure, .. } if failure == StorageRpcErrorCode::PayloadDecode),
            "case {i}: {error:?}"
        );
        assert_eq!(sent.load(Ordering::SeqCst), i + 2, "no hidden retry");
    }
    let pg = node.get_pg(pg_id.get()).unwrap();
    assert_eq!(snapshot(&pg), before);
    assert_eq!(pg.conn.total_changes(), writes);
}

#[test]
fn staging_complete_pending_rpc_pretransport_scope_and_live_format_separation() {
    let dir = test_util::tempdir();
    let epoch = ClusterEpoch::new(5).unwrap();
    let client = UnixStorageNodeClient::new(
        placement::NodeId::new(4),
        epoch,
        dir.path().join("no-server"),
    );
    let error = client
        .complete_pending_command_until(PgId::new(4), epoch, deadline())
        .err()
        .unwrap();
    assert!(
        matches!(error, StoreError::StorageRpc { failure, .. } if failure == StorageRpcErrorCode::PayloadDecode)
    );
    let client = client.with_pg_topology(Arc::new(topology()));
    assert!(
        matches!(client.complete_pending_command_until(PgId::new(99), epoch, deadline()), Err(StoreError::StorageRpc { failure, .. }) if failure == StorageRpcErrorCode::PayloadDecode)
    );
    assert!(matches!(
        client.complete_pending_command_until(
            PgId::new(4),
            ClusterEpoch::new(6).unwrap(),
            deadline()
        ),
        Err(StoreError::StalePayloadOperation { .. })
    ));

    let (server, client, config) = installed_at_epoch(&dir, PgState::Active, epoch, false);
    let node = server.test_storage_node();
    let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
    let context = prepare_context(&node.get_pg(pg_id.get()).unwrap(), 0);
    install(&node.get_pg(pg_id.get()).unwrap(), &context);
    let local = LocalStorageNodeClient::new(config.node_id, Arc::clone(&node));
    assert!(matches!(
        local.complete_pending_command_until(pg_id, epoch, Instant::now()),
        Err(CompletePendingInspectionError::Store(
            StoreError::OperationDeadlineExceeded { .. }
        ))
    ));
    let _running = Running::start(server, config.socket_path);
    // Unselected live handler still rejects complete bytes; no format sniff.
    assert!(client
        .complete_pending_command_until(pg_id, epoch, deadline())
        .is_err());
}

#[test]
fn staging_complete_pending_rpc_frozen_v37_command_v14_context_v1_payload() {
    let (_dir, pg, context) = complete_application::setup(5);
    install(&pg, &context);
    let topology = topology();
    let scope = CompletePendingRequest::new(
        context.id().pg_id(),
        context.id().cluster_epoch(),
        &topology,
    )
    .unwrap();
    let payload = scope.read_response(&pg, 0).unwrap();
    let expected = include_str!("testdata/complete_pending_rpc_v37_command_v14_context_v1.hex")
        .lines()
        .find(|line| !line.starts_with('#') && !line.is_empty())
        .unwrap();
    let actual = payload
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert_eq!(actual, expected);
    assert_context(scope.decode_response(&payload).unwrap(), &context, true);
    assert!(rpc::decode_metadata_command_pending_envelope_response(
        &payload,
        &crate::node_runtime::MetadataCommandDecodeAuthority::new()
    )
    .is_err());
}
