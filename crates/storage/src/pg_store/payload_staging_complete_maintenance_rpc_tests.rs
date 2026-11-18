// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Installed complete-grammar maintenance, not protocol activation. These are
//! owner-local metadata-only fixtures; no physical cleanup is authorized.

use super::complete_log_range::{index, ordinary};
use super::complete_log_rpc::{installed_at_epoch, installed_config, seed_context, Running};
use super::complete_maintenance::log_state;
use super::*;
use crate::metadata_command::MetadataCommandReplicaState;
use crate::node_runtime::pg_store::{
    CompleteLogMaintenanceRequest, CompleteMaintenanceError, MetadataCommandLogCompactionStatus,
    PgStore, PgStoreRecoveryContext,
};
use crate::node_runtime::server::{CompleteLogMutation, CompleteLogMutationAuthority};
use crate::{PgState, StoreError};

// Pin mandatory authority at the actual production API, not a fixture facade.
const _: fn(
    &CompleteLogMaintenanceRequest<'static>,
    &PgStore,
    &CompleteLogMutationAuthority<'_>,
) -> Result<crate::MetadataCommandCheckpoint, CompleteMaintenanceError> =
    CompleteLogMaintenanceRequest::record_checkpoint;
const _: fn(
    &CompleteLogMaintenanceRequest<'static>,
    &PgStore,
    &CompleteLogMutationAuthority<'_>,
) -> Result<MetadataCommandLogCompactionStatus, CompleteMaintenanceError> =
    CompleteLogMaintenanceRequest::compact;

#[test]
fn staging_complete_maintenance_authority_rejects_crossed_subject_store_and_operation() {
    let dir = test_util::tempdir();
    let epoch = ClusterEpoch::new(5).unwrap();
    let (server, _, config) = installed_at_epoch(&dir, PgState::Active, epoch, true);
    let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
    let topology = topology();
    let other_dir = test_util::tempdir();
    let other_store = PgStore::open(other_dir.path(), pg_id.get()).unwrap();
    let node = server.test_storage_node();
    seed_context(&node.get_pg(pg_id.get()).unwrap(), 0);
    for mutation in [CompleteLogMutation::Record, CompleteLogMutation::Compact] {
        server
            .test_with_complete_maintenance_authority(
                crate::storage_rpc::StorageRpcMetadataCommandStateRequest {
                    node_id: config.node_id,
                    pg_id,
                    cluster_epoch: epoch,
                },
                mutation,
                |pg, authority| {
                    let writes = pg.conn.total_changes();
                    for crossed in [
                        "node",
                        "pg",
                        "epoch",
                        "topology",
                        "store",
                        "operation",
                        "transaction",
                    ] {
                        let crossed_topology =
                            PgTopology::new(&(0..16).collect::<Vec<_>>()).unwrap();
                        let scope = CompleteLogMaintenanceRequest::new(
                            if crossed == "node" {
                                NodeId::new(5)
                            } else {
                                config.node_id
                            },
                            if crossed == "pg" {
                                PgId::new((pg_id.get() + 1) % 8)
                            } else {
                                pg_id
                            },
                            if crossed == "epoch" {
                                ClusterEpoch::new(6).unwrap()
                            } else {
                                epoch
                            },
                            if crossed == "topology" {
                                &crossed_topology
                            } else {
                                &topology
                            },
                        )
                        .unwrap();
                        let store = if crossed == "store" { &other_store } else { pg };
                        if crossed == "transaction" {
                            pg.conn.execute_batch("BEGIN").unwrap();
                        }
                        let requested = if crossed == "operation" {
                            if mutation == CompleteLogMutation::Record {
                                CompleteLogMutation::Compact
                            } else {
                                CompleteLogMutation::Record
                            }
                        } else {
                            mutation
                        };
                        let result = match requested {
                            CompleteLogMutation::Record => {
                                scope.record_checkpoint(store, authority).map(|_| ())
                            }
                            CompleteLogMutation::Compact => {
                                scope.compact(store, authority).map(|_| ())
                            }
                        };
                        assert!(
                            matches!(
                                result,
                                Err(CompleteMaintenanceError::Store(
                                    StoreError::RouteCapabilitySubjectMismatch { .. }
                                ))
                            ),
                            "{crossed}: {result:?}"
                        );
                        if crossed == "transaction" {
                            pg.conn.execute_batch("ROLLBACK").unwrap();
                        }
                    }
                    assert_eq!(pg.conn.total_changes(), writes);
                },
            )
            .unwrap();
    }
}

#[test]
fn staging_complete_maintenance_authority_rechecks_expiry_at_both_sqlite_effect_boundaries() {
    let clock = crate::clock::test_time_override_guard(1_000);
    for mutation in [CompleteLogMutation::Record, CompleteLogMutation::Compact] {
        clock.set(1_000);
        let dir = test_util::tempdir();
        let mut config = capture::config(&fixture(StagingTarget::DirectPut));
        config.cluster_epoch = ClusterEpoch::new(5).unwrap();
        config.route_map_validity = crate::RouteMapValidity::until_ms(5_000).unwrap();
        for route in &mut config.pg_routes {
            route.cluster_epoch = config.cluster_epoch;
        }
        let (server, _, config) = installed_config(&dir, config, true);
        let node = server.test_storage_node();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
        {
            let pg = node.get_pg(pg_id.get()).unwrap();
            seed_context(&pg, 0);
            if mutation == CompleteLogMutation::Compact {
                pg.record_complete_metadata_command_checkpoint(4, &topology())
                    .unwrap();
            }
        }
        server
            .test_with_complete_maintenance_authority(
                crate::storage_rpc::StorageRpcMetadataCommandStateRequest {
                    node_id: config.node_id,
                    pg_id,
                    cluster_epoch: config.cluster_epoch,
                },
                mutation,
                |pg, authority| {
                    let topology = topology();
                    let scope = CompleteLogMaintenanceRequest::new(
                        config.node_id,
                        pg_id,
                        config.cluster_epoch,
                        &topology,
                    )
                    .unwrap();
                    let before = log_state(pg);
                    let writes = pg.conn.total_changes();
                    let reached = std::rc::Rc::new(std::cell::Cell::new(false));
                    let effect_reached = reached.clone();
                    let control = clock.control();
                    let result = pg.test_with_before_complete_maintenance_effect(
                        move || {
                            effect_reached.set(true);
                            control.set(6_000);
                        },
                        || match mutation {
                            CompleteLogMutation::Record => {
                                scope.record_checkpoint(pg, authority).map(|_| ())
                            }
                            CompleteLogMutation::Compact => {
                                scope.compact(pg, authority).map(|_| ())
                            }
                        },
                    );
                    assert!(reached.get(), "did not reach final pre-write check");
                    assert!(
                        matches!(
                            result,
                            Err(CompleteMaintenanceError::Store(
                                StoreError::RouteMapExpired { .. }
                            ))
                        ),
                        "{result:?}"
                    );
                    assert_eq!(log_state(pg), before);
                    assert_eq!(pg.conn.total_changes(), writes);
                    assert!(pg.conn.is_autocommit());
                    pg.require_usable_connection().unwrap();
                },
            )
            .unwrap();
    }
}

#[test]
fn staging_complete_maintenance_rpc_exports_records_compacts_and_reopens_all_context_kinds() {
    for case in 0..3 {
        let dir = test_util::tempdir();
        let epoch = ClusterEpoch::new(if case == 2 { 6 } else { 5 }).unwrap();
        let (server, client, config) = installed_at_epoch(&dir, PgState::Active, epoch, true);
        let node = server.test_storage_node();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
        let (checkpoint, materialized, prefix, writes) = {
            let pg = node.get_pg(pg_id.get()).unwrap();
            seed_context(&pg, case);
            (
                pg.complete_metadata_command_checkpoint(4, &topology())
                    .unwrap(),
                introduction::state(&pg),
                pg.metadata_command_replica_state().unwrap(),
                pg.conn.total_changes(),
            )
        };
        let running = Running::start(server, config.socket_path);
        assert_eq!(
            client.metadata_command_checkpoint(pg_id, epoch).unwrap(),
            checkpoint
        );
        assert_eq!(
            node.get_pg(pg_id.get()).unwrap().conn.total_changes(),
            writes
        );
        assert_eq!(
            client
                .record_current_metadata_command_checkpoint(pg_id, epoch)
                .unwrap(),
            prefix
        );
        assert_eq!(
            client.compact_metadata_command_log(pg_id, epoch).unwrap(),
            MetadataCommandLogCompactionStatus::Compacted {
                deleted_entries: prefix.applied_log_index,
                compacted_before: prefix.applied_log_index + 1,
            }
        );
        {
            let pg = node.get_pg(pg_id.get()).unwrap();
            assert!(log_state(&pg)[0].is_empty());
            assert!(!log_state(&pg)[1].is_empty());
            assert_eq!(introduction::state(&pg), materialized);
            assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        }
        assert_eq!(
            client.metadata_command_checkpoint(pg_id, epoch).unwrap(),
            checkpoint
        );
        drop(running);
        drop(node);
        // Reopen only after all source runtime holders have gone. Startup itself
        // remains explicitly selected at the PG owner, not live node dispatch.
        let pg = PgStore::open(
            &config.data_dir.join(format!("pg-{:04}", pg_id.get())),
            pg_id.get(),
        )
        .unwrap();
        assert_eq!(
            pg.recover_complete_command_state(
                PgStoreRecoveryContext::for_node(config.node_id),
                &topology()
            )
            .unwrap(),
            prefix
        );
        assert_eq!(
            pg.complete_metadata_command_checkpoint(4, &topology())
                .unwrap(),
            checkpoint
        );
        assert_eq!(introduction::state(&pg), materialized);
    }
}

#[test]
fn staging_complete_maintenance_rpc_preserves_unresolved_applied_and_abandoned_pending_custody() {
    for terminal in [
        "unresolved",
        "applied",
        "abandoned",
        "damaged_index",
        "damaged_scope",
    ] {
        let dir = test_util::tempdir();
        let epoch = ClusterEpoch::new(5).unwrap();
        let (server, client, config) = installed_at_epoch(&dir, PgState::Active, epoch, true);
        let node = server.test_storage_node();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
        let (pending, log, materialized, prefix, writes) = {
            let pg = node.get_pg(pg_id.get()).unwrap();
            let introduced = seed_context(&pg, 1);
            let next =
                MetadataCommandId::new(epoch, pg_id, index(introduced.id().log_index().get() + 1));
            let context =
                ContextualCommand::decode_field(ordinary(next), &[1, 0, 0], &topology()).unwrap();
            let encoded = context.encode_command().unwrap();
            let duplicate = ContextualCommand::decode_command(
                encoded.bytes(),
                encoded.checksum_crc64(),
                &topology(),
            )
            .unwrap();
            complete_pending::install(&mut duplicate.into_submission(), &pg).unwrap();
            match terminal {
                "applied" => {
                    pg.apply_complete_metadata_command_and_record(4, &encoded, &topology())
                        .unwrap();
                }
                "abandoned" => {
                    pg.test_record_complete_command_abandoned(4, &encoded)
                        .unwrap();
                }
                "damaged_index" => {
                    pg.conn
                        .execute(
                            "DELETE FROM metadata_command_pending_staging_protection",
                            [],
                        )
                        .unwrap();
                }
                "damaged_scope" => {
                    pg.conn
                        .execute(
                            "UPDATE metadata_command_pending_slot SET scope_bucket = NULL",
                            [],
                        )
                        .unwrap();
                }
                "unresolved" => {}
                _ => unreachable!(),
            }
            (
                complete_pending::observe(&pg),
                log_state(&pg),
                introduction::state(&pg),
                pg.metadata_command_replica_state().unwrap(),
                pg.conn.total_changes(),
            )
        };
        let _running = Running::start(server, config.socket_path);
        assert!(client.metadata_command_checkpoint(pg_id, epoch).is_err());
        assert!(client
            .record_current_metadata_command_checkpoint(pg_id, epoch)
            .is_err());
        let compact = client.compact_metadata_command_log(pg_id, epoch);
        if terminal == "damaged_scope" {
            assert!(compact.is_err());
        } else {
            assert!(matches!(
                compact.unwrap(),
                MetadataCommandLogCompactionStatus::PendingCommand { .. }
            ));
        }
        let pg = node.get_pg(pg_id.get()).unwrap();
        assert_eq!(complete_pending::observe(&pg), pending);
        assert_eq!(log_state(&pg), log);
        assert_eq!(introduction::state(&pg), materialized);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        assert_eq!(pg.conn.total_changes(), writes);
    }
}

#[test]
fn staging_complete_maintenance_rpc_keeps_inspection_separate_from_primary_mutation() {
    for mode in [
        "peering",
        "replica",
        "historical",
        "wrong_epoch",
        "expired",
        "default",
    ] {
        let dir = test_util::tempdir();
        let mut setup = capture::config(&fixture(StagingTarget::DirectPut));
        if mode == "expired" {
            setup.route_map_validity = crate::RouteMapValidity::until_ms(0).unwrap();
        }
        for route in &mut setup.pg_routes {
            if mode == "peering" || mode == "historical" {
                route.state = PgState::Peering;
            }
            if mode == "replica" {
                route.primary_node_id = NodeId::new(0);
            }
            if mode == "historical" || mode == "wrong_epoch" {
                route.cluster_epoch = ClusterEpoch::new(6).unwrap();
            }
        }
        if mode == "historical" || mode == "wrong_epoch" {
            setup.cluster_epoch = ClusterEpoch::new(6).unwrap();
        }
        let (server, client, config) = installed_config(&dir, setup, mode != "default");
        let node = server.test_storage_node();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
        let (checkpoint, log, writes) = {
            let pg = node.get_pg(pg_id.get()).unwrap();
            if mode == "historical" {
                // The physical store remains at epoch 5 while the node's route
                // advances to Peering epoch 6. No future-epoch command is seeded.
                pg.conn
                    .execute(
                        "UPDATE metadata_command_replica_state SET cluster_epoch = 5",
                        [],
                    )
                    .unwrap();
            }
            if mode != "wrong_epoch" {
                seed_context(&pg, 0);
            }
            (
                pg.complete_metadata_command_checkpoint(4, &topology())
                    .unwrap(),
                log_state(&pg),
                pg.conn.total_changes(),
            )
        };
        let _running = Running::start(server, config.socket_path);
        let requested = ClusterEpoch::new(5).unwrap();
        if mode == "default" || mode == "wrong_epoch" || mode == "expired" {
            assert!(client
                .metadata_command_checkpoint(pg_id, requested)
                .is_err());
        } else {
            assert_eq!(
                client
                    .metadata_command_checkpoint(pg_id, requested)
                    .unwrap(),
                checkpoint
            );
        }
        assert!(
            client
                .record_current_metadata_command_checkpoint(pg_id, requested)
                .is_err(),
            "{mode}"
        );
        let compact = client.compact_metadata_command_log(pg_id, requested);
        if mode == "default" {
            // No checkpoint means no log decode or mutation in either grammar.
            assert!(matches!(
                compact.unwrap(),
                MetadataCommandLogCompactionStatus::NoCheckpoint { .. }
            ));
        } else {
            assert!(compact.is_err(), "{mode}");
        }
        let pg = node.get_pg(pg_id.get()).unwrap();
        assert_eq!(pg.conn.total_changes(), writes, "{mode}");
        assert_eq!(log_state(&pg), log, "{mode}");
    }
}

#[test]
fn staging_complete_maintenance_request_rejects_crossed_store_epoch_and_caller_transaction() {
    let dir = test_util::tempdir();
    let pg = PgStore::open(dir.path(), 0).unwrap();
    let topology = topology();
    for (pg_id, epoch, transaction) in [(1, 1, false), (0, 2, false), (0, 1, true)] {
        if transaction {
            pg.conn.execute_batch("BEGIN").unwrap();
        }
        let scope = CompleteLogMaintenanceRequest::new(
            NodeId::new(4),
            PgId::new(pg_id),
            ClusterEpoch::new(epoch).unwrap(),
            &topology,
        )
        .unwrap();
        let writes = pg.conn.total_changes();
        assert!(matches!(
            scope.checkpoint(&pg),
            Err(CompleteMaintenanceError::Store(
                StoreError::RouteCapabilitySubjectMismatch { .. }
                    | StoreError::StaleMetadataCommand { .. }
            ))
        ));
        assert_eq!(pg.conn.total_changes(), writes);
        if transaction {
            assert!(!pg.conn.is_autocommit());
            pg.conn.execute_batch("ROLLBACK").unwrap();
        }
        pg.require_usable_connection().unwrap();
    }
}

#[test]
fn staging_complete_maintenance_rpc_rejects_requested_epoch_different_from_the_stored_prefix() {
    let dir = test_util::tempdir();
    let epoch = ClusterEpoch::new(5).unwrap();
    let (server, client, config) = installed_at_epoch(&dir, PgState::Active, epoch, true);
    let node = server.test_storage_node();
    let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
    let (before, writes) = {
        let pg = node.get_pg(pg_id.get()).unwrap();
        seed_context(&pg, 2); // durable prefix advances to 6, route remains 5
        (log_state(&pg), pg.conn.total_changes())
    };
    let _running = Running::start(server, config.socket_path);
    assert!(client.metadata_command_checkpoint(pg_id, epoch).is_err());
    assert!(client
        .record_current_metadata_command_checkpoint(pg_id, epoch)
        .is_err());
    assert!(client.compact_metadata_command_log(pg_id, epoch).is_err());
    let pg = node.get_pg(pg_id.get()).unwrap();
    assert_eq!(log_state(&pg), before);
    assert_eq!(pg.conn.total_changes(), writes);
}

#[test]
fn staging_complete_maintenance_rpc_rejects_corrupt_log_before_checkpoint_or_compaction_writes() {
    for compact in [false, true] {
        let dir = test_util::tempdir();
        let epoch = ClusterEpoch::new(5).unwrap();
        let (server, client, config) = installed_at_epoch(&dir, PgState::Active, epoch, true);
        let node = server.test_storage_node();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
        seed_context(&node.get_pg(pg_id.get()).unwrap(), 1);
        let _running = Running::start(server, config.socket_path);
        if compact {
            client
                .record_current_metadata_command_checkpoint(pg_id, epoch)
                .unwrap();
        }
        let (before, materialized, prefix, writes) = {
            let pg = node.get_pg(pg_id.get()).unwrap();
            pg.conn.execute("UPDATE metadata_command_log SET command_checksum = command_checksum + 1 WHERE log_index = (SELECT MAX(log_index) FROM metadata_command_log)", []).unwrap();
            (
                log_state(&pg),
                introduction::state(&pg),
                pg.metadata_command_replica_state().unwrap(),
                pg.conn.total_changes(),
            )
        };
        if compact {
            assert!(client.compact_metadata_command_log(pg_id, epoch).is_err());
        } else {
            assert!(client.metadata_command_checkpoint(pg_id, epoch).is_err());
            assert!(client
                .record_current_metadata_command_checkpoint(pg_id, epoch)
                .is_err());
        }
        let pg = node.get_pg(pg_id.get()).unwrap();
        assert_eq!(log_state(&pg), before);
        assert_eq!(introduction::state(&pg), materialized);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        assert_eq!(pg.conn.total_changes(), writes);
    }
}

#[test]
fn staging_complete_maintenance_rpc_failed_rollback_blocks_access_until_reopen() {
    for compact in [false, true] {
        let dir = test_util::tempdir();
        let epoch = ClusterEpoch::new(5).unwrap();
        let (server, client, config) = installed_at_epoch(&dir, PgState::Active, epoch, true);
        let node = server.test_storage_node();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
        let prefix: MetadataCommandReplicaState;
        {
            let pg = node.get_pg(pg_id.get()).unwrap();
            seed_context(&pg, 1);
            prefix = pg.metadata_command_replica_state().unwrap();
        }
        let running = Running::start(server, config.socket_path);
        if compact {
            client
                .record_current_metadata_command_checkpoint(pg_id, epoch)
                .unwrap();
        }
        let before = {
            let pg = node.get_pg(pg_id.get()).unwrap();
            let event = if compact {
                "DELETE ON metadata_command_log"
            } else {
                "INSERT ON metadata_command_checkpoints"
            };
            pg.conn.execute_batch("CREATE TEMP TABLE deferred_failure (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES deferred_failure(id) DEFERRABLE INITIALLY DEFERRED)").unwrap();
            pg.conn.execute_batch(&format!("CREATE TEMP TRIGGER fail_maintenance AFTER {event} BEGIN INSERT INTO deferred_failure(parent) VALUES (0); END;")).unwrap();
            pg.test_fail_metadata_command_rollbacks(1);
            log_state(&pg)
        };
        let error = if compact {
            client
                .compact_metadata_command_log(pg_id, epoch)
                .map(|_| ())
        } else {
            client
                .record_current_metadata_command_checkpoint(pg_id, epoch)
                .map(|_| ())
        }
        .unwrap_err();
        assert!(matches!(error, StoreError::StorageRpc { .. }), "{error:?}");
        assert!(
            node.get_pg(pg_id.get()).is_err(),
            "retired store escaped node guard"
        );
        assert!(client.metadata_command_checkpoint(pg_id, epoch).is_err());
        assert!(client
            .record_current_metadata_command_checkpoint(pg_id, epoch)
            .is_err());
        assert!(client.compact_metadata_command_log(pg_id, epoch).is_err());
        drop(running);
        drop(node);
        let pg = PgStore::open(
            &config.data_dir.join(format!("pg-{:04}", pg_id.get())),
            pg_id.get(),
        )
        .unwrap();
        assert_eq!(log_state(&pg), before);
        assert_eq!(
            pg.recover_complete_command_state(
                PgStoreRecoveryContext::for_node(config.node_id),
                &topology()
            )
            .unwrap(),
            prefix
        );
    }
}
