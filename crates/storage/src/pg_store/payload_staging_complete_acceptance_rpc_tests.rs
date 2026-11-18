// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Candidate acceptance observations: no publication, abandonment or cleanup
//! authority. Metadata-only fixtures use the real terminal recording paths.

use super::complete_applied_rpc::{conflict, context, deadline, pending};
use super::complete_log_range::{apply, ordinary};
use super::complete_log_rpc::{installed_config, prepared, Running};
use super::complete_node_startup::snapshot;
use super::*;
use crate::node_runtime::clients::{LocalStorageNodeClient, UnixStorageNodeClient};
use crate::node_runtime::pg_store::{
    CompleteAcceptanceDisposition as D, CompleteAcceptanceKind as K,
    CompletePendingInspectionError, CompletePendingRequest,
};
use crate::storage_rpc::{self as rpc, complete_applied as wire, StorageRpcErrorCode as Code};
use crate::{PgState, StoreError};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Instant;

#[test]
fn staging_complete_acceptance_rpc_context_matrix_terminal_receipts_and_restart() {
    for case in 0..4 {
        for terminal in [K::Apply, K::Abandon] {
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
                let before = snapshot(&node.get_pg(pg_id.get()).unwrap());
                let writes = node.get_pg(pg_id.get()).unwrap().conn.total_changes();
                for kind in [K::Apply, K::Abandon] {
                    let embedded = local
                        .complete_command_acceptance_until(&pending, kind, deadline())
                        .unwrap();
                    let remote = client
                        .complete_command_acceptance_until(&pending, kind, deadline())
                        .unwrap();
                    assert_eq!(embedded.kind(), kind);
                    assert_eq!(remote.kind(), kind);
                    assert_eq!(embedded.disposition(), D::NextPosition);
                    assert_eq!(remote.disposition(), D::NextPosition);
                }
                let pg = node.get_pg(pg_id.get()).unwrap();
                assert_eq!(snapshot(&pg), before);
                assert_eq!(pg.conn.total_changes(), writes);
                match terminal {
                    K::Apply => apply(&pg, &context),
                    K::Abandon => {
                        pg.test_record_complete_command_abandoned(
                            4,
                            &context.encode_command().unwrap(),
                        )
                        .unwrap();
                    }
                }
                drop(pg);
                for compacted in [false, true] {
                    let pg = node.get_pg(pg_id.get()).unwrap();
                    if compacted {
                        pg.record_complete_metadata_command_checkpoint(4, &topology())
                            .unwrap();
                        pg.compact_complete_metadata_command_log(&topology())
                            .unwrap();
                        assert!(complete_maintenance::log_state(&pg)[0].is_empty());
                    }
                    let before = snapshot(&pg);
                    let writes = pg.conn.total_changes();
                    drop(pg);
                    for kind in [K::Apply, K::Abandon] {
                        if kind == terminal {
                            let embedded = local
                                .complete_command_acceptance_until(&pending, kind, deadline())
                                .unwrap();
                            let remote = client
                                .complete_command_acceptance_until(&pending, kind, deadline())
                                .unwrap();
                            assert_eq!(embedded.kind(), kind);
                            assert_eq!(remote.kind(), kind);
                            assert_eq!(embedded.disposition(), D::ExactTerminal);
                            assert_eq!(remote.disposition(), D::ExactTerminal);
                        } else {
                            conflict(
                                client
                                    .complete_command_acceptance_until(&pending, kind, deadline())
                                    .err()
                                    .unwrap(),
                                context.id(),
                            );
                            assert!(matches!(
                                local.complete_command_acceptance_until(&pending, kind, deadline()),
                                Err(CompletePendingInspectionError::Store(
                                    StoreError::MetadataCommandLogConflict { .. }
                                ))
                            ));
                        }
                    }
                    let pg = node.get_pg(pg_id.get()).unwrap();
                    assert_eq!(snapshot(&pg), before);
                    assert_eq!(pg.conn.total_changes(), writes);
                    assert!(pg.conn.is_autocommit());
                }
                let before = snapshot(&node.get_pg(pg_id.get()).unwrap());
                drop(running);
                drop(local);
                drop(node);
                let (prepared, client) = prepared(&config);
                let server = prepared.bind_complete_for_test().unwrap();
                let node = server.test_storage_node();
                let local = LocalStorageNodeClient::new(config.node_id, Arc::clone(&node));
                let _running = Running::start(server, config.socket_path);
                assert_eq!(
                    client
                        .complete_command_acceptance_until(&pending, terminal, deadline())
                        .unwrap()
                        .disposition(),
                    D::ExactTerminal
                );
                assert_eq!(
                    local
                        .complete_command_acceptance_until(&pending, terminal, deadline())
                        .unwrap()
                        .disposition(),
                    D::ExactTerminal
                );
                assert_eq!(snapshot(&node.get_pg(pg_id.get()).unwrap()), before);
            }
        }
    }
}

#[test]
fn staging_complete_acceptance_rpc_context_and_equal_crc_substitutions_rejected() {
    for equal_crc in [false, true] {
        for kind in [K::Apply, K::Abandon] {
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
                let (context, crossed) = if equal_crc {
                    let owner = part::initialize(&pg, 2);
                    let source = part::command(&pg, &owner);
                    let crossed =
                        abandonment::source_carrier::crossed_proof_with_equal_crc(&source, "owner");
                    let context =
                        ContextualCommand::decode_field(source, &[1, 0, 0], &topology()).unwrap();
                    let crossed =
                        ContextualCommand::decode_field(crossed, &[1, 0, 0], &topology()).unwrap();
                    assert_eq!(
                        context.encode_command().unwrap().checksum_crc64(),
                        crossed.encode_command().unwrap().checksum_crc64()
                    );
                    complete_application::anchor(&pg, context.id());
                    (context, crossed)
                } else {
                    let context = context(&pg, 2);
                    let encoded = context.encode_command().unwrap();
                    let mut crossed = encoded.bytes()
                        [..encoded.bytes().len() - context.encode_field().unwrap().len()]
                        .to_vec();
                    crossed.extend_from_slice(&[1, 0, 0]);
                    let crossed = ContextualCommand::decode_command(
                        &crossed,
                        checksum::crc64::checksum(&crossed),
                        &topology(),
                    )
                    .unwrap();
                    (context, crossed)
                };
                assert_eq!(context.id(), crossed.id());
                assert_ne!(
                    context.encode_command().unwrap().command_sha256(),
                    crossed.encode_command().unwrap().command_sha256()
                );
                match kind {
                    K::Apply => apply(&pg, &context),
                    K::Abandon => {
                        pg.test_record_complete_command_abandoned(
                            4,
                            &context.encode_command().unwrap(),
                        )
                        .unwrap();
                    }
                }
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
                assert_eq!(
                    client
                        .complete_command_acceptance_until(&pending, kind, deadline())
                        .unwrap()
                        .disposition(),
                    D::ExactTerminal
                );
                conflict(
                    client
                        .complete_command_acceptance_until(&crossed, kind, deadline())
                        .err()
                        .unwrap(),
                    context.id(),
                );
                assert!(matches!(
                    local.complete_command_acceptance_until(&crossed, kind, deadline()),
                    Err(CompletePendingInspectionError::Store(
                        StoreError::MetadataCommandLogConflict { .. }
                    ))
                ));
                let pg = node.get_pg(pg_id.get()).unwrap();
                assert_eq!(snapshot(&pg), before);
                assert_eq!(pg.conn.total_changes(), writes);
            }
        }
    }
}

#[test]
fn staging_complete_acceptance_rpc_rejects_damaged_rows_and_digest_without_mutation() {
    for kind in [K::Apply, K::Abandon] {
        for damage in [
            "entry-checksum",
            "entry-link",
            "receipt-digest",
            "receipt-link",
            "state-digest",
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
            match kind {
                K::Apply => apply(&pg, &context),
                K::Abandon => {
                    pg.test_record_complete_command_abandoned(
                        4,
                        &context.encode_command().unwrap(),
                    )
                    .unwrap();
                }
            }
            if damage.starts_with("receipt") {
                pg.record_complete_metadata_command_checkpoint(4, &topology())
                    .unwrap();
                pg.compact_complete_metadata_command_log(&topology())
                    .unwrap();
            }
            drop(pg);
            assert_eq!(
                client
                    .complete_command_acceptance_until(&pending, kind, deadline())
                    .unwrap()
                    .disposition(),
                D::ExactTerminal
            );
            let pg = node.get_pg(pg_id.get()).unwrap();
            let sql = match damage {
                "entry-checksum" => "UPDATE metadata_command_log SET command_checksum = command_checksum + 1 WHERE log_index = ?1",
                "entry-link" => "UPDATE metadata_command_log SET log_hash = log_hash + 1 WHERE log_index = ?1",
                "receipt-digest" => "UPDATE metadata_command_terminal_receipts SET command_sha256 = zeroblob(32) WHERE log_index = ?1",
                "receipt-link" => "UPDATE metadata_command_terminal_receipts SET log_hash = log_hash + 1 WHERE log_index = ?1",
                "state-digest" => "UPDATE metadata_command_replica_state SET state_digest = state_digest + 1 WHERE applied_log_index = ?1",
                _ => unreachable!(),
            };
            assert_eq!(
                pg.conn
                    .execute(sql, [context.id().log_index().get() as i64])
                    .unwrap(),
                1
            );
            pg.invalidate_clean_metadata_digest_revision();
            let before = snapshot(&pg);
            let writes = pg.conn.total_changes();
            drop(pg);
            assert!(
                client
                    .complete_command_acceptance_until(&pending, kind, deadline())
                    .is_err(),
                "{kind:?}/{damage}"
            );
            let local_error = local
                .complete_command_acceptance_until(&pending, kind, deadline())
                .err()
                .unwrap();
            if damage == "state-digest" {
                assert!(
                    matches!(
                        local_error,
                        CompletePendingInspectionError::Store(
                            StoreError::MetadataStateDigestMismatch { .. }
                        )
                    ),
                    "{local_error:?}"
                );
            }
            let pg = node.get_pg(pg_id.get()).unwrap();
            assert_eq!(snapshot(&pg), before);
            assert_eq!(pg.conn.total_changes(), writes);
            assert!(pg.conn.is_autocommit());
        }
    }
}

#[test]
fn staging_complete_acceptance_rpc_position_rules_are_not_absence_or_historical_recovery() {
    for case in [
        "gap",
        "new-epoch-first",
        "new-epoch-gap",
        "stale",
        "missing-receipt",
    ] {
        let dir = test_util::tempdir();
        let (server, client, config) = installed_config(
            &dir,
            capture::config(&fixture(StagingTarget::DirectPut)),
            true,
        );
        let node = server.test_storage_node();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
        let pg = node.get_pg(pg_id.get()).unwrap();
        let original = if case.starts_with("new-epoch") {
            ContextualCommand::decode_field(
                ordinary(fixture(StagingTarget::DirectPut).subject.id.introduction),
                &[1, 0, 0],
                &topology(),
            )
            .unwrap()
        } else {
            context(&pg, 0)
        };
        let id = original.id();
        let query = match case {
            "gap" => ContextualCommand::decode_field(
                ordinary(MetadataCommandId::new(
                    id.cluster_epoch(),
                    pg_id,
                    MetadataCommandLogIndex::new(id.log_index().get() + 1).unwrap(),
                )),
                &[1, 0, 0],
                &topology(),
            )
            .unwrap(),
            "new-epoch-first" | "new-epoch-gap" => {
                // The captured route is epoch 5 while this replica is still at
                // epoch 4. Only index 1 of the route epoch can be next.
                let previous = ContextualCommand::decode_field(
                    ordinary(MetadataCommandId::new(
                        ClusterEpoch::new(4).unwrap(),
                        pg_id,
                        MetadataCommandLogIndex::new(1).unwrap(),
                    )),
                    &[1, 0, 0],
                    &topology(),
                )
                .unwrap();
                apply(&pg, &previous);
                ContextualCommand::decode_field(
                    ordinary(MetadataCommandId::new(
                        id.cluster_epoch(),
                        pg_id,
                        MetadataCommandLogIndex::new(if case == "new-epoch-first" { 1 } else { 2 })
                            .unwrap(),
                    )),
                    &[1, 0, 0],
                    &topology(),
                )
                .unwrap()
            }
            "stale" => {
                let next = ContextualCommand::decode_field(
                    ordinary(MetadataCommandId::new(
                        ClusterEpoch::new(6).unwrap(),
                        pg_id,
                        MetadataCommandLogIndex::new(1).unwrap(),
                    )),
                    &[1, 0, 0],
                    &topology(),
                )
                .unwrap();
                apply(&pg, &next);
                original
            }
            "missing-receipt" => {
                apply(&pg, &original);
                pg.record_complete_metadata_command_checkpoint(4, &topology())
                    .unwrap();
                pg.compact_complete_metadata_command_log(&topology())
                    .unwrap();
                pg.conn
                    .execute(
                        "DELETE FROM metadata_command_terminal_receipts WHERE log_index = ?1",
                        [id.log_index().get() as i64],
                    )
                    .unwrap();
                original
            }
            _ => unreachable!(),
        };
        let pending = pending(&query);
        let before = snapshot(&pg);
        let writes = pg.conn.total_changes();
        drop(pg);
        let local = LocalStorageNodeClient::new(config.node_id, Arc::clone(&node));
        let _running = Running::start(server, config.socket_path);
        for kind in [K::Apply, K::Abandon] {
            let embedded = local.complete_command_acceptance_until(&pending, kind, deadline());
            let remote = client.complete_command_acceptance_until(&pending, kind, deadline());
            if case == "new-epoch-first" {
                assert_eq!(embedded.unwrap().disposition(), D::NextPosition);
                assert_eq!(remote.unwrap().disposition(), D::NextPosition);
            } else {
                let error = embedded.err().unwrap();
                assert!(
                    match case {
                        "stale" => matches!(
                            error,
                            CompletePendingInspectionError::Store(
                                StoreError::StaleMetadataCommand { .. }
                            )
                        ),
                        "missing-receipt" => matches!(
                            error,
                            CompletePendingInspectionError::Store(
                                StoreError::MetadataCommandLogConflict { .. }
                            )
                        ),
                        _ => matches!(
                            error,
                            CompletePendingInspectionError::Store(
                                StoreError::MetadataCommandLogGap { .. }
                            )
                        ),
                    },
                    "{case}: {error:?}"
                );
                assert!(remote.is_err(), "{case}");
            }
        }
        let pg = node.get_pg(pg_id.get()).unwrap();
        assert_eq!(snapshot(&pg), before);
        assert_eq!(pg.conn.total_changes(), writes);
    }
}

#[test]
fn staging_complete_acceptance_rpc_rejects_malformed_authenticated_responses_without_retry() {
    for kind in [K::Apply, K::Abandon] {
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
        let id = context.id();
        use rpc::StorageRpcMetadataCommandAcceptanceOutcome as O;
        let response = |outcome| {
            rpc::encode_metadata_command_acceptance_response(
                &rpc::StorageRpcMetadataCommandAcceptanceResponse { outcome },
            )
        };
        assert_eq!(
            response(O::Acceptance(
                crate::metadata_command::MetadataCommandAcceptance::Apply
            )),
            [1]
        );
        assert_eq!(
            response(O::Acceptance(
                crate::metadata_command::MetadataCommandAcceptance::AlreadyApplied
            )),
            [2]
        );
        let mut cases = vec![vec![], vec![0], vec![4], vec![1, 0], vec![2, 0], vec![3, 0]];
        for field in 0..4 {
            cases.push(response(O::LogConflict {
                node_id: 4 + u32::from(field == 0),
                pg_id: pg_id.get() + u32::from(field == 1),
                cluster_epoch: ClusterEpoch::new(id.cluster_epoch().get() + u64::from(field == 2))
                    .unwrap(),
                log_index: id.log_index().get() + u64::from(field == 3),
            }));
        }
        let selected = Arc::new(Mutex::new(vec![1]));
        let selected_hook = Arc::clone(&selected);
        let count = Arc::new(AtomicUsize::new(0));
        let count_hook = Arc::clone(&count);
        server.set_response_frame_test_hook(Arc::new(move |message, frame| {
            if message == kind.message_kind() {
                count_hook.fetch_add(1, Ordering::SeqCst);
                frame.payload =
                    rpc::encode_storage_rpc_success_response(&selected_hook.lock().unwrap());
            }
        }));
        let before = snapshot(&node.get_pg(pg_id.get()).unwrap());
        let _running = Running::start(server, config.socket_path);
        for (tag, expected) in [(1, D::NextPosition), (2, D::ExactTerminal)] {
            *selected.lock().unwrap() = vec![tag];
            let observed = client
                .complete_command_acceptance_until(&pending, kind, deadline())
                .unwrap();
            assert_eq!(observed.kind(), kind);
            assert_eq!(observed.disposition(), expected);
        }
        for (i, bytes) in cases.into_iter().enumerate() {
            *selected.lock().unwrap() = bytes;
            let error = client
                .complete_command_acceptance_until(&pending, kind, deadline())
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
            assert_eq!(count.load(Ordering::SeqCst), i + 3);
        }
        *selected.lock().unwrap() = response(O::LogConflict {
            node_id: 4,
            pg_id: pg_id.get(),
            cluster_epoch: id.cluster_epoch(),
            log_index: id.log_index().get(),
        });
        let requests = count.load(Ordering::SeqCst);
        conflict(
            client
                .complete_command_acceptance_until(&pending, kind, deadline())
                .err()
                .unwrap(),
            id,
        );
        assert_eq!(count.load(Ordering::SeqCst), requests + 1);
        assert_eq!(snapshot(&node.get_pg(pg_id.get()).unwrap()), before);
    }
}

#[test]
fn staging_complete_acceptance_rpc_frozen_query_and_request_rejection() {
    let (_dir, _pg, context) = complete_application::setup(5);
    let pending = pending(&context);
    let frozen = crate::tests::frozen_hex_fixture(include_str!(
        "testdata/complete_applied_rpc_v37_command_v14_context_v1.hex"
    ));
    for kind in [K::Apply, K::Abandon] {
        let query = pending
            .acceptance_probe(placement::NodeId::new(4), &topology(), kind)
            .unwrap()
            .encode_request()
            .unwrap();
        assert_eq!(query, frozen);
        let dir = test_util::tempdir();
        let (server, client, config) = installed_config(
            &dir,
            capture::config(&fixture(StagingTarget::DirectPut)),
            true,
        );
        let node = server.test_storage_node();
        let pg_id = context.id().pg_id();
        complete_application::anchor(&node.get_pg(pg_id.get()).unwrap(), context.id());
        let before = snapshot(&node.get_pg(pg_id.get()).unwrap());
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
        let old = ordinary(context.id());
        cases.push(
            wire::encode(wire::BorrowedRequest {
                node_id: placement::NodeId::new(4),
                epoch: context.id().cluster_epoch(),
                pg_id,
                checksum: old.checksum_crc64(),
                command: &old.command_bytes(),
            })
            .unwrap(),
        );
        assert_eq!(
            client
                .rpc_request(kind.message_kind(), query.clone())
                .unwrap(),
            [1]
        );
        for bytes in cases {
            let error = client.rpc_request(kind.message_kind(), bytes).unwrap_err();
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
            client.rpc_request(kind.message_kind(), wrong_node),
            Err(StoreError::StorageRpc {
                failure: Code::UnknownNode,
                ..
            })
        ));
        assert_eq!(snapshot(&node.get_pg(pg_id.get()).unwrap()), before);
    }
}

#[test]
fn staging_complete_acceptance_rpc_pretransport_scope_and_production_separation() {
    let (_dir, pg, context) = complete_application::setup(5);
    let pending = pending(&context);
    let id = context.id();
    let dir = test_util::tempdir();
    let absent = CompletePendingRequest::new(id.pg_id(), id.cluster_epoch(), &topology())
        .unwrap()
        .read(&pg, 4)
        .unwrap();
    for kind in [K::Apply, K::Abandon] {
        assert!(absent
            .acceptance_probe(placement::NodeId::new(4), &topology(), kind)
            .is_err());
        let client = UnixStorageNodeClient::new(
            placement::NodeId::new(4),
            id.cluster_epoch(),
            dir.path().join("no-server"),
        );
        assert!(matches!(
            client.complete_command_acceptance_until(&pending, kind, deadline()),
            Err(StoreError::StorageRpc {
                failure: Code::PayloadDecode,
                ..
            })
        ));
        let client = client.with_pg_topology(Arc::new(
            crate::PgTopology::new(&[id.pg_id().get()]).unwrap(),
        ));
        assert!(matches!(
            client.complete_command_acceptance_until(&pending, kind, deadline()),
            Err(StoreError::StorageRpc {
                failure: Code::PayloadDecode,
                ..
            })
        ));
        let client = UnixStorageNodeClient::new(
            placement::NodeId::new(4),
            ClusterEpoch::INITIAL,
            dir.path().join("no-server"),
        )
        .with_pg_topology(Arc::new(topology()));
        assert!(matches!(
            client.complete_command_acceptance_until(&pending, kind, deadline()),
            Err(StoreError::StalePayloadOperation { .. })
        ));
    }
    let (server, client, config) = installed_config(
        &dir,
        capture::config(&fixture(StagingTarget::DirectPut)),
        false,
    );
    let node = server.test_storage_node();
    let local = LocalStorageNodeClient::new(config.node_id, Arc::clone(&node));
    let before = snapshot(&node.get_pg(id.pg_id().get()).unwrap());
    let _running = Running::start(server, config.socket_path);
    for kind in [K::Apply, K::Abandon] {
        assert!(matches!(
            local.complete_command_acceptance_until(&pending, kind, Instant::now()),
            Err(CompletePendingInspectionError::Store(
                StoreError::OperationDeadlineExceeded { .. }
            ))
        ));
        assert!(matches!(
            client.complete_command_acceptance_until(&pending, kind, deadline()),
            Err(StoreError::StorageRpc {
                failure: Code::PayloadDecode,
                ..
            })
        ));
    }
    assert_eq!(snapshot(&node.get_pg(id.pg_id().get()).unwrap()), before);
}
