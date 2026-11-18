// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::metadata_command::{
    MetadataCommandEnvelope, MetadataCommandPayload, ReserveObjectVersionCommand,
};
use crate::node_runtime::clients::UnixStorageNodeClient;
use crate::node_runtime::pg_store::{
    CompleteRetirementQuery, CompleteRetirementSendError, TerminalSlotRetirementObservation as O,
};
use crate::storage_node_server::StorageNodeServer;
use crate::storage_rpc::{
    self as rpc, complete_retirement as wire, StorageRpcErrorCode as Code,
    StorageRpcMessageKind as Kind, StorageRpcOperationDeadline,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

const KIND: Kind = Kind::MetadataCommandPendingSlotRemove;

fn encode(context: &ContextualCommand, node: NodeId, cutoff: u64) -> Vec<u8> {
    let command = context.encode_command().unwrap();
    wire::encode_request(wire::BorrowedRequest {
        command: rpc::complete_applied::BorrowedRequest {
            node_id: node,
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

fn state(
    pg: &PgStore,
) -> (
    Vec<crate::node_runtime::pg_store::MetadataCheckpointTableBlock>,
    Vec<Vec<String>>,
    crate::metadata_command::MetadataCommandReplicaState,
) {
    (
        introduction::state(pg),
        complete_maintenance::log_state(pg),
        pg.metadata_command_replica_state().unwrap(),
    )
}

fn exchange(
    server: &StorageNodeServer,
    client: &UnixStorageNodeClient,
    pg: PgId,
    payload: Vec<u8>,
    expire: bool,
    lose: bool,
) -> Result<Vec<u8>, StoreError> {
    std::thread::scope(|scope| {
        let worker = scope.spawn(|| {
            server
                .test_accept_one_retirement_fault(expire, lose)
                .unwrap()
        });
        let section = client
            .open_metadata_command_critical_section_until(pg, deadline())
            .unwrap();
        let result = section.test_retirement_payload(payload, deadline());
        drop(section);
        worker.join().unwrap();
        result
    })
}

#[test]
fn staging_retirement_receiver_unix_tls_owning_sender_lost_reply_exact_retry() {
    for tls in [false, true] {
        for case in [0, 1, 3, 4] {
            terminal_retirement::with_terminal_transport(
                tls,
                case,
                "uncertain",
                |server, _, context, terminal, _, client| {
                    let node = server.test_storage_node();
                    let pg_id = context.id().pg_id();
                    let before = state(&node.get_pg(pg_id.get()).unwrap());
                    let count = Arc::new(AtomicUsize::new(0));
                    let observed = Arc::clone(&count);
                    server.set_response_frame_test_hook(Arc::new(move |kind, _| {
                        if kind == KIND {
                            observed.fetch_add(1, Ordering::SeqCst);
                        }
                    }));
                    std::thread::scope(|scope| {
                        let worker = scope.spawn(|| {
                            server
                                .test_accept_one_retirement_fault(false, true)
                                .unwrap()
                        });
                        let session = client
                            .open_metadata_command_critical_section_until(pg_id, deadline())
                            .unwrap();
                        assert!(matches!(
                            terminal.retire_remote(client, &session, deadline()),
                            Err(CompleteRetirementSendError::Unconfirmed(_))
                        ));
                        assert_eq!(terminal.slot_retirement_observation(), O::Unconfirmed);
                        drop(session);
                        worker.join().unwrap();
                    });
                    assert!(
                        complete_pending::observe(&node.get_pg(pg_id.get()).unwrap())
                            .iter()
                            .all(Vec::is_empty)
                    );
                    assert_eq!(state(&node.get_pg(pg_id.get()).unwrap()), before);
                    let writes = node.get_pg(pg_id.get()).unwrap().conn.total_changes();
                    std::thread::scope(|scope| {
                        let worker = scope.spawn(|| server.accept_one().unwrap());
                        let session = client
                            .open_metadata_command_critical_section_until(pg_id, deadline())
                            .unwrap();
                        terminal
                            .retire_remote(client, &session, deadline())
                            .unwrap();
                        assert_eq!(terminal.slot_retirement_observation(), O::Retired);
                        let replies = count.load(Ordering::SeqCst);
                        terminal
                            .retire_remote(client, &session, deadline())
                            .unwrap();
                        assert_eq!(count.load(Ordering::SeqCst), replies, "no redispatch");
                        drop(session);
                        worker.join().unwrap();
                    });
                    assert_eq!(state(&node.get_pg(pg_id.get()).unwrap()), before);
                    assert_eq!(
                        node.get_pg(pg_id.get()).unwrap().conn.total_changes(),
                        writes,
                        "exact absence is read-only"
                    );
                },
            );
        }
    }
}

#[test]
fn staging_retirement_receiver_unix_tls_reinspection_after_restart_and_compaction() {
    for tls in [false, true] {
        let dir = test_util::tempdir();
        let (server, client, config, context) = application_rpc::setup(tls, 1, false, &dir);
        let pg_id = context.id().pg_id();
        let node = server.test_storage_node();
        apply(&node.get_pg(pg_id.get()).unwrap(), &context);
        let payload = encode(
            &context,
            config.node_id,
            crate::clock::current_time_millis() + 60_000,
        );
        assert!(exchange(&server, &client, pg_id, payload.clone(), false, true).is_err());
        let before = snapshot(&node.get_pg(pg_id.get()).unwrap());
        drop(node);
        drop(client);
        drop(server);
        let (server, client) = if tls {
            super::tls::tls_server_at(config.clone(), true, config.cluster_epoch)
        } else {
            let (prepared, client) = prepared(&config);
            (prepared.bind_complete_for_test().unwrap(), client)
        };
        let node = server.test_storage_node();
        assert_eq!(snapshot(&node.get_pg(pg_id.get()).unwrap()), before);
        let pg = node.get_pg(pg_id.get()).unwrap();
        pg.record_complete_metadata_command_checkpoint(config.node_id.as_u32(), &topology())
            .unwrap();
        pg.compact_complete_metadata_command_log(&topology())
            .unwrap();
        assert!(complete_maintenance::log_state(&pg)[0].is_empty());
        let before = snapshot(&pg);
        let writes = pg.conn.total_changes();
        drop(pg);
        let response = exchange(&server, &client, pg_id, payload, false, false).unwrap();
        wire::decode_confirmation_for(
            &response,
            config.node_id,
            context.id(),
            context.encode_command().unwrap().command_sha256(),
        )
        .unwrap();
        assert_eq!(snapshot(&node.get_pg(pg_id.get()).unwrap()), before);
        assert_eq!(
            node.get_pg(pg_id.get()).unwrap().conn.total_changes(),
            writes
        );
    }
}

#[test]
fn staging_retirement_receiver_wire_expiry_inside_transaction_is_nonmutating() {
    for tls in [false, true] {
        let dir = test_util::tempdir();
        let (server, client, config, context) = application_rpc::setup(tls, 1, false, &dir);
        let node = server.test_storage_node();
        let pg_id = context.id().pg_id();
        apply(&node.get_pg(pg_id.get()).unwrap(), &context);
        let before = snapshot(&node.get_pg(pg_id.get()).unwrap());
        let writes = node.get_pg(pg_id.get()).unwrap().conn.total_changes();
        let payload = encode(
            &context,
            config.node_id,
            crate::clock::current_time_millis() + 2_000,
        );
        let error = exchange(&server, &client, pg_id, payload, true, false).unwrap_err();
        assert!(
            matches!(
                error,
                StoreError::StorageRpc {
                    failure: Code::TransportTimeout,
                    ..
                }
            ),
            "{error:?}"
        );
        assert_eq!(snapshot(&node.get_pg(pg_id.get()).unwrap()), before);
        assert_eq!(
            node.get_pg(pg_id.get()).unwrap().conn.total_changes(),
            writes
        );
    }
}

#[test]
fn staging_retirement_receiver_rejects_ineligible_missing_applied_and_corrupt_slot() {
    for mode in [
        "ineligible",
        "unapplied",
        "scope",
        "marker",
        "route-index",
        "staging-index",
        "orphan",
        "crossed-command",
        "expired-at-arrival",
        "legacy-dispatch",
    ] {
        let dir = test_util::tempdir();
        let case = if mode == "ineligible" { 2 } else { 1 };
        let (server, client, config, context) = if mode == "legacy-dispatch" {
            let (server, client, config) = installed_config(&dir, config(0), false);
            let node = server.test_storage_node();
            let pg = node
                .get_pg(fixture(StagingTarget::DirectPut).subject.object_pg.get())
                .unwrap();
            let context = context(&pg, 0);
            drop(pg);
            drop(node);
            (server, client, config, context)
        } else {
            application_rpc::setup(false, case, false, &dir)
        };
        let node = server.test_storage_node();
        let pg_id = context.id().pg_id();
        let pg = node.get_pg(pg_id.get()).unwrap();
        if mode != "unapplied" && mode != "legacy-dispatch" {
            apply(&pg, &context);
        }
        let damage = match mode {
            "scope" => "UPDATE metadata_command_pending_slot SET scope_bucket = NULL",
            "marker" => "UPDATE metadata_command_pending_slot SET publication_started = 0",
            "route-index" => "DELETE FROM metadata_command_pending_route_dependencies",
            "staging-index" => "DELETE FROM metadata_command_pending_staging_protection",
            "orphan" => "PRAGMA foreign_keys=OFF; DELETE FROM metadata_command_pending_slot; PRAGMA foreign_keys=ON;",
            _ => "",
        };
        pg.conn.execute_batch(damage).unwrap();
        let before = snapshot(&pg);
        let writes = pg.conn.total_changes();
        drop(pg);
        let mut payload = encode(
            &context,
            config.node_id,
            if mode == "expired-at-arrival" {
                0
            } else {
                crate::clock::current_time_millis() + 60_000
            },
        );
        if mode == "crossed-command" {
            let owner = fixture(StagingTarget::DirectPut);
            let crossed = ContextualCommand::decode_field(
                MetadataCommandEnvelope::new(
                    context.id(),
                    MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
                        owner.subject.bucket,
                        owner.subject.key,
                        crate::VersionId::from_u64(99),
                    )),
                ),
                &[1, 0, 0],
                &topology(),
            )
            .unwrap();
            payload = encode(
                &crossed,
                config.node_id,
                crate::clock::current_time_millis() + 60_000,
            );
        }
        let error = exchange(&server, &client, pg_id, payload, false, false).unwrap_err();
        if mode == "expired-at-arrival" {
            assert!(
                matches!(
                    error,
                    StoreError::StorageRpc {
                        failure: Code::TransportTimeout,
                        ..
                    }
                ),
                "{error:?}"
            );
        }
        assert!(
            matches!(error, StoreError::StorageRpc { .. }),
            "{mode}: {error}"
        );
        assert_eq!(
            snapshot(&node.get_pg(pg_id.get()).unwrap()),
            before,
            "{mode}"
        );
        assert_eq!(
            node.get_pg(pg_id.get()).unwrap().conn.total_changes(),
            writes,
            "{mode}"
        );
    }
}

#[test]
fn staging_retirement_receiver_admission_domains_sections_and_routes() {
    let dir = test_util::tempdir();
    let foreign_dir = test_util::tempdir();
    let (server, _, config, context) = application_rpc::setup(false, 0, false, &dir);
    let (foreign, _, _, _) = application_rpc::setup(false, 0, false, &foreign_dir);
    let bytes = encode(
        &context,
        config.node_id,
        crate::clock::current_time_millis() + 60_000,
    );
    let query = CompleteRetirementQuery::decode(&bytes, &topology()).unwrap();
    server.test_retirement_crossed_admission(&foreign, &query.request().unwrap());
    for mode in [
        "peering",
        "replica",
        "old-epoch",
        "future-epoch",
        "wrong-node",
    ] {
        let dir = test_util::tempdir();
        let mut crossed = config.clone();
        for route in &mut crossed.pg_routes {
            match mode {
                "peering" => route.state = PgState::Peering,
                "replica" => route.primary_node_id = NodeId::new(2),
                "old-epoch" => route.cluster_epoch = ClusterEpoch::new(6).unwrap(),
                "future-epoch" => route.cluster_epoch = ClusterEpoch::new(4).unwrap(),
                _ => (),
            }
        }
        if mode == "old-epoch" {
            crossed.cluster_epoch = ClusterEpoch::new(6).unwrap();
        }
        if mode == "future-epoch" {
            crossed.cluster_epoch = ClusterEpoch::new(4).unwrap();
        }
        let (server, _, _) = installed_config(&dir, crossed, true);
        let bytes = encode(
            &context,
            if mode == "wrong-node" {
                NodeId::new(99)
            } else {
                config.node_id
            },
            crate::clock::current_time_millis() + 60_000,
        );
        let query = CompleteRetirementQuery::decode(&bytes, &topology()).unwrap();
        let error = server
            .test_with_retirement_authority(&query.request().unwrap(), deadline(), |_, _| {
                panic!("{mode} admitted")
            })
            .unwrap_err();
        assert_ne!(error.code, Code::Internal, "{mode}: {error:?}");
    }
}

#[test]
fn staging_retirement_receiver_rechecks_wire_work_and_server_route_before_effect_and_commit() {
    for bound in ["wire", "server-work", "server-route"] {
        for stage in ["before-delete", "before-commit"] {
            let clock = crate::clock::test_time_override_guard(1_000);
            let dir = test_util::tempdir();
            let mut config = config(1);
            config.route_map_validity = RouteMapValidity::until_ms(5_000).unwrap();
            let (server, _, config) = installed_config(&dir, config, true);
            let node = server.test_storage_node();
            let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
            let context = context(&node.get_pg(pg_id.get()).unwrap(), 1);
            install(&node.get_pg(pg_id.get()).unwrap(), &context);
            let pending = pending(&context);
            let publication = pending
                .publication_request(config.node_id, &topology())
                .unwrap();
            server
                .test_with_complete_publication_authority(
                    &publication,
                    deadline(),
                    |pg, authority| publication.start(pg, authority),
                )
                .unwrap()
                .unwrap();
            apply(&node.get_pg(pg_id.get()).unwrap(), &context);
            let query = CompleteRetirementQuery::decode(
                &encode(
                    &context,
                    config.node_id,
                    if bound == "wire" { 2_000 } else { 60_000 },
                ),
                &topology(),
            )
            .unwrap();
            let request = query.request().unwrap();
            let budget = if bound == "server-work" {
                Instant::now() + Duration::from_secs(1)
            } else {
                deadline()
            };
            let cutoff = request.bound_deadline(budget);
            server
                .test_with_retirement_authority(&request, budget, |pg, authority| {
                    let before = snapshot(pg);
                    let writes = pg.conn.total_changes();
                    let mut reached = false;
                    let error = request
                        .test_retire_with_hook(pg, authority, |at| {
                            if at == stage {
                                reached = true;
                                if bound == "server-route" {
                                    let mut renewed = config.clone();
                                    renewed.route_map_validity =
                                        RouteMapValidity::until_ms(100_000).unwrap();
                                    server
                                        .install_control_plane_runtime_config(renewed)
                                        .unwrap();
                                    clock.set(6_000);
                                } else {
                                    assert!(
                                        Instant::now() < cutoff,
                                        "must expire inside transaction"
                                    );
                                    while let Some(remaining) =
                                        cutoff.checked_duration_since(Instant::now())
                                    {
                                        std::thread::sleep(remaining);
                                    }
                                }
                            }
                            Ok(())
                        })
                        .unwrap_err();
                    assert!(reached);
                    let error = error.into_store_error().unwrap();
                    if bound == "server-route" {
                        assert!(
                            matches!(error, StoreError::RouteMapExpired { .. }),
                            "{error}"
                        );
                    } else {
                        assert!(
                            matches!(error, StoreError::OperationDeadlineExceeded { .. }),
                            "{error}"
                        );
                    }
                    assert!(pg.conn.is_autocommit());
                    assert_eq!(snapshot(pg), before);
                    if stage == "before-delete" {
                        assert_eq!(pg.conn.total_changes(), writes);
                    }
                })
                .unwrap();
        }
    }
}
