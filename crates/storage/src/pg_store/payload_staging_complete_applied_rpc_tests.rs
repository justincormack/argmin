// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Read-only applied observations for complete identities. Metadata-only
//! fixtures; neither the query nor its result grants recovery/custody authority.

use super::complete_log_range::apply;
use super::complete_log_rpc::{installed_config, prepare_context, prepared, Running};
use super::complete_node_startup::snapshot;
use super::*;
use crate::node_runtime::clients::{LocalStorageNodeClient, UnixStorageNodeClient};
use crate::node_runtime::pg_store::{
    CompletePendingEvidence, CompletePendingInspectionError, CompletePendingRequest, PgStore,
};
use crate::storage_rpc::{
    self as rpc, complete_applied as wire, complete_pending, StorageRpcErrorCode as Code,
    StorageRpcMessageKind as Kind,
};
use crate::{PgState, StoreError};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};

pub(super) fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(30)
}

pub(super) fn pending(context: &ContextualCommand) -> CompletePendingEvidence {
    // A validated peer observation, not a new submission or fake applied row.
    let encoded = context.encode_command().unwrap();
    CompletePendingRequest::new(
        context.id().pg_id(),
        context.id().cluster_epoch(),
        &topology(),
    )
    .unwrap()
    .decode_response(
        &complete_pending::encode(Some((encoded.checksum_crc64(), encoded.bytes())), true).unwrap(),
    )
    .unwrap()
}

pub(super) fn context(pg: &PgStore, case: usize) -> ContextualCommand {
    if case != 3 {
        return prepare_context(pg, case);
    }
    let _clock = crate::clock::test_time_override_guard(2_000);
    let mut owner = fresh_capture::template(StagingTarget::StreamPut);
    let command = fresh_capture::candidate(pg, &mut owner);
    let intro = ContextualCommand::from_described(candidate::capture(pg, &owner, command, false));
    complete_application::anchor(pg, intro.id());
    apply(pg, &intro);
    let snapshot = pg.command_build_snapshot().unwrap();
    let owner = append::load(pg, &owner);
    snapshot.finish().unwrap();
    fresh_append_capture::prepare(pg, &owner);
    ContextualCommand::from_described(candidate::capture(
        pg,
        &owner,
        append::command(&owner),
        true,
    ))
}

pub(super) fn conflict(error: StoreError, id: MetadataCommandId) {
    assert!(
        matches!(error, StoreError::MetadataCommandLogConflict { node_id: 4, pg_id, cluster_epoch, log_index }
        if pg_id == id.pg_id().get() && cluster_epoch == id.cluster_epoch() && log_index == id.log_index().get()),
        "{error:?}"
    );
}

#[test]
fn staging_complete_applied_rpc_contexts_exact_receipts_and_restart() {
    for case in 0..4 {
        for abandoned in [false, true] {
            for historical in [false, true] {
                let dir = test_util::tempdir();
                let epoch = ClusterEpoch::new(if case == 2 { 6 } else { 5 }).unwrap();
                let mut config = capture::config(&fixture(StagingTarget::DirectPut));
                config.cluster_epoch = epoch;
                for route in &mut config.pg_routes {
                    route.cluster_epoch = epoch;
                }
                if historical {
                    config.historical_pg_routes = config.pg_routes.clone();
                    config.cluster_epoch = ClusterEpoch::new(epoch.get() + 1).unwrap();
                    for route in &mut config.pg_routes {
                        route.cluster_epoch = config.cluster_epoch;
                        route.state = PgState::Peering;
                    }
                }
                let (server, client, config) = installed_config(&dir, config, true);
                let node = server.test_storage_node();
                let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
                let context = context(&node.get_pg(pg_id.get()).unwrap(), case);
                let pending = pending(&context);
                let local = LocalStorageNodeClient::new(config.node_id, Arc::clone(&node));
                let running = Running::start(server, config.socket_path.clone());
                assert!(local
                    .complete_applied_command_until(&pending, deadline())
                    .unwrap()
                    .hashes()
                    .is_none());
                assert!(client
                    .complete_applied_command_until(&pending, deadline())
                    .unwrap()
                    .hashes()
                    .is_none());
                let expected = {
                    let pg = node.get_pg(pg_id.get()).unwrap();
                    if abandoned {
                        pg.test_record_complete_command_abandoned(
                            4,
                            &context.encode_command().unwrap(),
                        )
                        .unwrap();
                    } else {
                        apply(&pg, &context);
                    }
                    let result = pending
                        .applied_probe(config.node_id, &topology())
                        .unwrap()
                        .read(&pg);
                    if abandoned {
                        assert!(matches!(
                            result,
                            Err(CompletePendingInspectionError::Store(
                                StoreError::MetadataCommandLogConflict { .. }
                            ))
                        ));
                        None
                    } else {
                        Some(
                            result
                                .unwrap()
                                .hashes()
                                .expect("positive exact-application canary"),
                        )
                    }
                };
                for compacted in [false, true] {
                    if compacted {
                        let pg = node.get_pg(pg_id.get()).unwrap();
                        pg.record_complete_metadata_command_checkpoint(4, &topology())
                            .unwrap();
                        pg.compact_complete_metadata_command_log(&topology())
                            .unwrap();
                        assert!(complete_maintenance::log_state(&pg)[0].is_empty());
                    }
                    let pg = node.get_pg(pg_id.get()).unwrap();
                    let before = snapshot(&pg);
                    let writes = pg.conn.total_changes();
                    drop(pg);
                    if abandoned {
                        let error = client
                            .complete_applied_command_until(&pending, deadline())
                            .err()
                            .unwrap();
                        conflict(error, context.id());
                        assert!(matches!(
                            local.complete_applied_command_until(&pending, deadline()),
                            Err(CompletePendingInspectionError::Store(
                                StoreError::MetadataCommandLogConflict { .. }
                            ))
                        ));
                    } else {
                        assert_eq!(
                            client
                                .complete_applied_command_until(&pending, deadline())
                                .unwrap()
                                .hashes(),
                            expected
                        );
                        assert_eq!(
                            local
                                .complete_applied_command_until(&pending, deadline())
                                .unwrap()
                                .hashes(),
                            expected
                        );
                    }
                    let pg = node.get_pg(pg_id.get()).unwrap();
                    assert_eq!(snapshot(&pg), before);
                    assert_eq!(pg.conn.total_changes(), writes);
                }
                let before = snapshot(&node.get_pg(pg_id.get()).unwrap());
                drop(running);
                drop(local);
                drop(node);
                let (prepared, client) = prepared(&config);
                let server = prepared.bind_complete_for_test().unwrap();
                let node = server.test_storage_node();
                let _running = Running::start(server, config.socket_path);
                if abandoned {
                    conflict(
                        client
                            .complete_applied_command_until(&pending, deadline())
                            .err()
                            .unwrap(),
                        context.id(),
                    );
                } else {
                    assert_eq!(
                        client
                            .complete_applied_command_until(&pending, deadline())
                            .unwrap()
                            .hashes(),
                        expected
                    );
                }
                assert_eq!(snapshot(&node.get_pg(pg_id.get()).unwrap()), before);
            }
        }
    }
}

#[test]
fn staging_complete_applied_rpc_binds_context_not_only_payload_before_and_after_compaction() {
    let dir = test_util::tempdir();
    let mut config = capture::config(&fixture(StagingTarget::DirectPut));
    config.cluster_epoch = ClusterEpoch::new(6).unwrap();
    for route in &mut config.pg_routes {
        route.cluster_epoch = config.cluster_epoch;
    }
    let (server, client, config) = installed_config(&dir, config, true);
    let node = server.test_storage_node();
    let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
    let context = context(&node.get_pg(pg_id.get()).unwrap(), 2);
    let encoded = context.encode_command().unwrap();
    let mut crossed =
        encoded.bytes()[..encoded.bytes().len() - context.encode_field().unwrap().len()].to_vec();
    crossed.extend_from_slice(&[1, 0, 0]);
    let crossed = ContextualCommand::decode_command(
        &crossed,
        checksum::crc64::checksum(&crossed),
        &topology(),
    )
    .unwrap();
    assert_eq!(crossed.id(), context.id());
    let exact = pending(&context);
    let crossed = pending(&crossed);
    apply(&node.get_pg(pg_id.get()).unwrap(), &context);
    let _running = Running::start(server, config.socket_path);
    for compacted in [false, true] {
        let pg = node.get_pg(pg_id.get()).unwrap();
        if compacted {
            pg.record_complete_metadata_command_checkpoint(4, &topology())
                .unwrap();
            pg.compact_complete_metadata_command_log(&topology())
                .unwrap();
        }
        let before = snapshot(&pg);
        let writes = pg.conn.total_changes();
        drop(pg);
        assert!(client
            .complete_applied_command_until(&exact, deadline())
            .unwrap()
            .hashes()
            .is_some());
        conflict(
            client
                .complete_applied_command_until(&crossed, deadline())
                .err()
                .unwrap(),
            context.id(),
        );
        let pg = node.get_pg(pg_id.get()).unwrap();
        assert_eq!(snapshot(&pg), before);
        assert_eq!(pg.conn.total_changes(), writes);
    }
}

#[test]
fn staging_complete_applied_rpc_rejects_malformed_authenticated_responses_without_retry() {
    let dir = test_util::tempdir();
    let (server, client, config) = installed_config(
        &dir,
        capture::config(&fixture(StagingTarget::DirectPut)),
        true,
    );
    let node = server.test_storage_node();
    let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
    let context = context(&node.get_pg(pg_id.get()).unwrap(), 0);
    apply(&node.get_pg(pg_id.get()).unwrap(), &context);
    let pending = pending(&context);
    let probe = pending.applied_probe(config.node_id, &topology()).unwrap();
    let hashes = probe
        .read(&node.get_pg(pg_id.get()).unwrap())
        .unwrap()
        .hashes();
    let response = |outcome| {
        rpc::encode_metadata_command_applied_hashes_response(
            &rpc::StorageRpcMetadataCommandAppliedHashesResponse { outcome },
        )
    };
    use rpc::StorageRpcMetadataCommandAppliedHashesOutcome as O;
    let valid = response(O::Hashes(hashes));
    let mut cases = vec![
        vec![],
        vec![3],
        vec![0, 0],
        valid[..valid.len() - 1].to_vec(),
        response(O::Hashes(Some((hashes.unwrap().0, hashes.unwrap().1 ^ 1)))),
    ];
    let id = context.id();
    for field in 0..4 {
        cases.push(response(O::LogConflict {
            node_id: 4 + u32::from(field == 0),
            pg_id: pg_id.get() + u32::from(field == 1),
            cluster_epoch: ClusterEpoch::new(id.cluster_epoch().get() + u64::from(field == 2))
                .unwrap(),
            log_index: id.log_index().get() + u64::from(field == 3),
        }));
    }
    let selected = Arc::new(Mutex::new(valid));
    let selected_hook = Arc::clone(&selected);
    let count = Arc::new(AtomicUsize::new(0));
    let count_hook = Arc::clone(&count);
    server.set_response_frame_test_hook(Arc::new(move |kind, frame| {
        if kind == Kind::MetadataCommandAppliedLogHashes {
            count_hook.fetch_add(1, Ordering::SeqCst);
            frame.payload =
                rpc::encode_storage_rpc_success_response(&selected_hook.lock().unwrap());
        }
    }));
    let before = snapshot(&node.get_pg(pg_id.get()).unwrap());
    let _running = Running::start(server, config.socket_path);
    assert_eq!(
        client
            .complete_applied_command_until(&pending, deadline())
            .unwrap()
            .hashes(),
        hashes
    );
    for (i, bytes) in cases.into_iter().enumerate() {
        *selected.lock().unwrap() = bytes;
        let error = client
            .complete_applied_command_until(&pending, deadline())
            .err()
            .unwrap();
        assert!(
            matches!(
                error,
                StoreError::StorageRpc {
                    failure: Code::PayloadDecode,
                    ..
                }
            ),
            "{error:?}"
        );
        assert_eq!(count.load(Ordering::SeqCst), i + 2);
    }
    *selected.lock().unwrap() = response(O::LogConflict {
        node_id: 4,
        pg_id: pg_id.get(),
        cluster_epoch: id.cluster_epoch(),
        log_index: id.log_index().get(),
    });
    conflict(
        client
            .complete_applied_command_until(&pending, deadline())
            .err()
            .unwrap(),
        id,
    );
    assert_eq!(snapshot(&node.get_pg(pg_id.get()).unwrap()), before);
}

#[test]
fn staging_complete_applied_rpc_pretransport_scope_and_production_separation() {
    let (_dir, pg, context) = complete_application::setup(5);
    let pending = pending(&context);
    let dir = test_util::tempdir();
    let id = context.id();
    let client = UnixStorageNodeClient::new(
        placement::NodeId::new(4),
        id.cluster_epoch(),
        dir.path().join("no-server"),
    );
    assert!(matches!(
        client.complete_applied_command_until(&pending, deadline()),
        Err(StoreError::StorageRpc {
            failure: Code::PayloadDecode,
            ..
        })
    ));
    let crossed = crate::PgTopology::new(&[id.pg_id().get()]).unwrap();
    let client = client.with_pg_topology(Arc::new(crossed));
    assert!(matches!(
        client.complete_applied_command_until(&pending, deadline()),
        Err(StoreError::StorageRpc {
            failure: Code::PayloadDecode,
            ..
        })
    ));
    let absent = CompletePendingRequest::new(id.pg_id(), id.cluster_epoch(), &topology())
        .unwrap()
        .read(&pg, 4)
        .unwrap();
    assert!(absent
        .applied_probe(placement::NodeId::new(4), &topology())
        .is_err());
    let client = UnixStorageNodeClient::new(
        placement::NodeId::new(4),
        ClusterEpoch::INITIAL,
        dir.path().join("no-server"),
    )
    .with_pg_topology(Arc::new(topology()));
    assert!(matches!(
        client.complete_applied_command_until(&pending, deadline()),
        Err(StoreError::StalePayloadOperation { .. })
    ));
    let (server, client, config) = installed_config(
        &dir,
        capture::config(&fixture(StagingTarget::DirectPut)),
        false,
    );
    let node = server.test_storage_node();
    let local = LocalStorageNodeClient::new(config.node_id, Arc::clone(&node));
    assert!(matches!(
        local.complete_applied_command_until(&pending, Instant::now()),
        Err(CompletePendingInspectionError::Store(
            StoreError::OperationDeadlineExceeded { .. }
        ))
    ));
    let before = snapshot(&node.get_pg(id.pg_id().get()).unwrap());
    let _running = Running::start(server, config.socket_path);
    assert!(matches!(
        client.complete_applied_command_until(&pending, deadline()),
        Err(StoreError::StorageRpc {
            failure: Code::PayloadDecode,
            ..
        })
    ));
    assert_eq!(snapshot(&node.get_pg(id.pg_id().get()).unwrap()), before);
}

#[test]
fn staging_complete_applied_rpc_equal_crc_substitution_rejected_by_bytes_and_receipt_digest() {
    let dir = test_util::tempdir();
    let mut config = capture::config(&fixture(StagingTarget::DirectPut));
    config.cluster_epoch = ClusterEpoch::new(6).unwrap();
    for route in &mut config.pg_routes {
        route.cluster_epoch = config.cluster_epoch;
    }
    let (server, client, config) = installed_config(&dir, config, true);
    let node = server.test_storage_node();
    let pg_id = fixture(multipart_target()).subject.object_pg;
    let (context, crossed) = {
        let pg = node.get_pg(pg_id.get()).unwrap();
        let owner = part::initialize(&pg, 2);
        let source = part::command(&pg, &owner);
        let crossed = abandonment::source_carrier::crossed_proof_with_equal_crc(&source, "owner");
        let context = ContextualCommand::decode_field(source, &[1, 0, 0], &topology()).unwrap();
        let crossed = ContextualCommand::decode_field(crossed, &[1, 0, 0], &topology()).unwrap();
        assert_eq!(
            context.encode_command().unwrap().checksum_crc64(),
            crossed.encode_command().unwrap().checksum_crc64()
        );
        assert_ne!(
            context.encode_command().unwrap().command_sha256(),
            crossed.encode_command().unwrap().command_sha256()
        );
        complete_application::anchor(&pg, context.id());
        apply(&pg, &context);
        (context, crossed)
    };
    let crossed = pending(&crossed);
    let pending = pending(&context);
    let local = LocalStorageNodeClient::new(config.node_id, Arc::clone(&node));
    let _running = Running::start(server, config.socket_path);
    for compacted in [false, true] {
        let pg = node.get_pg(pg_id.get()).unwrap();
        if compacted {
            pg.record_complete_metadata_command_checkpoint(4, &topology())
                .unwrap();
            pg.compact_complete_metadata_command_log(&topology())
                .unwrap();
        }
        let before = snapshot(&pg);
        let writes = pg.conn.total_changes();
        drop(pg);
        assert!(client
            .complete_applied_command_until(&pending, deadline())
            .unwrap()
            .hashes()
            .is_some());
        conflict(
            client
                .complete_applied_command_until(&crossed, deadline())
                .err()
                .unwrap(),
            context.id(),
        );
        assert!(matches!(
            local.complete_applied_command_until(&crossed, deadline()),
            Err(CompletePendingInspectionError::Store(
                StoreError::MetadataCommandLogConflict { .. }
            ))
        ));
        let pg = node.get_pg(pg_id.get()).unwrap();
        assert_eq!(snapshot(&pg), before);
        assert_eq!(pg.conn.total_changes(), writes);
    }
}

#[test]
fn staging_complete_applied_rpc_frozen_query_and_malformed_request_rejection() {
    let (_dir, _pg, context) = complete_application::setup(5);
    let pending = pending(&context);
    let query = pending
        .applied_probe(placement::NodeId::new(4), &topology())
        .unwrap()
        .encode_request()
        .unwrap();
    let frozen = crate::tests::frozen_hex_fixture(include_str!(
        "testdata/complete_applied_rpc_v37_command_v14_context_v1.hex"
    ));
    assert_eq!(query, frozen);
    assert!(rpc::decode_metadata_command_request(
        &frozen,
        &crate::node_runtime::MetadataCommandDecodeAuthority::new()
    )
    .is_err());
    let dir = test_util::tempdir();
    let (server, client, config) = installed_config(
        &dir,
        capture::config(&fixture(StagingTarget::DirectPut)),
        true,
    );
    let node = server.test_storage_node();
    let before = snapshot(&node.get_pg(context.id().pg_id().get()).unwrap());
    let _running = Running::start(server, config.socket_path);
    let mut cases = vec![query[..query.len() - 1].to_vec()];
    for offset in [4, 12, 16] {
        let mut bytes = query.clone();
        bytes[offset] ^= 1;
        cases.push(bytes);
    }
    let mut bytes = query.clone();
    bytes.push(0);
    cases.push(bytes);
    let old = complete_log_range::ordinary(context.id());
    cases.push(
        wire::encode(wire::BorrowedRequest {
            node_id: placement::NodeId::new(4),
            epoch: context.id().cluster_epoch(),
            pg_id: context.id().pg_id(),
            checksum: old.checksum_crc64(),
            command: &old.command_bytes(),
        })
        .unwrap(),
    );
    // A valid complete query works; malformed/crossed and old-grammar queries
    // fail before touching the store and do not fall back to another decoder.
    let good = client
        .rpc_request(Kind::MetadataCommandAppliedLogHashes, query.clone())
        .unwrap();
    assert_eq!(good, [0]);
    for bytes in cases {
        let error = client
            .rpc_request(Kind::MetadataCommandAppliedLogHashes, bytes)
            .unwrap_err();
        assert!(
            matches!(
                error,
                StoreError::StorageRpc {
                    failure: Code::PayloadDecode,
                    ..
                }
            ),
            "{error:?}"
        );
    }
    let mut wrong_node = query;
    wrong_node[..4].copy_from_slice(&99u32.to_le_bytes());
    assert!(matches!(
        client.rpc_request(Kind::MetadataCommandAppliedLogHashes, wrong_node),
        Err(StoreError::StorageRpc {
            failure: Code::UnknownNode,
            ..
        })
    ));
    assert_eq!(
        snapshot(&node.get_pg(context.id().pg_id().get()).unwrap()),
        before
    );
}

#[test]
fn staging_complete_applied_rpc_rejects_damaged_rows_without_mutation() {
    for damage in [
        "entry-checksum",
        "entry-link",
        "receipt-digest",
        "receipt-link",
    ] {
        let dir = test_util::tempdir();
        let (server, client, config) = installed_config(
            &dir,
            capture::config(&fixture(StagingTarget::DirectPut)),
            true,
        );
        let node = server.test_storage_node();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
        let context = context(&node.get_pg(pg_id.get()).unwrap(), 0);
        let pending = pending(&context);
        let local = LocalStorageNodeClient::new(config.node_id, Arc::clone(&node));
        let _running = Running::start(server, config.socket_path);
        let pg = node.get_pg(pg_id.get()).unwrap();
        apply(&pg, &context);
        if damage.starts_with("receipt") {
            pg.record_complete_metadata_command_checkpoint(4, &topology())
                .unwrap();
            pg.compact_complete_metadata_command_log(&topology())
                .unwrap();
        }
        drop(pg);
        assert!(client
            .complete_applied_command_until(&pending, deadline())
            .unwrap()
            .hashes()
            .is_some());
        let pg = node.get_pg(pg_id.get()).unwrap();
        let sql = match damage {
            "entry-checksum" => "UPDATE metadata_command_log SET command_checksum = command_checksum + 1 WHERE log_index = ?1",
            "entry-link" => "UPDATE metadata_command_log SET log_hash = log_hash + 1 WHERE log_index = ?1",
            "receipt-digest" => "UPDATE metadata_command_terminal_receipts SET command_sha256 = zeroblob(32) WHERE log_index = ?1",
            "receipt-link" => "UPDATE metadata_command_terminal_receipts SET log_hash = log_hash + 1 WHERE log_index = ?1",
            _ => unreachable!(),
        };
        assert_eq!(
            pg.conn
                .execute(sql, [context.id().log_index().get() as i64])
                .unwrap(),
            1
        );
        let before = snapshot(&pg);
        let writes = pg.conn.total_changes();
        drop(pg);
        assert!(
            client
                .complete_applied_command_until(&pending, deadline())
                .is_err(),
            "{damage}"
        );
        assert!(
            local
                .complete_applied_command_until(&pending, deadline())
                .is_err(),
            "{damage}"
        );
        let pg = node.get_pg(pg_id.get()).unwrap();
        assert_eq!(snapshot(&pg), before);
        assert_eq!(pg.conn.total_changes(), writes);
        assert!(pg.conn.is_autocommit());
    }
}
