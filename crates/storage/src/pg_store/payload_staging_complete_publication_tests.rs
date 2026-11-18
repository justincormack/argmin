// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Admitted current-primary store boundary, not RPC/front-end activation.
//! Metadata-only fixtures: no shard writes or physical cleanup authority.

use super::complete_applied_rpc::{context, deadline, pending};
use super::complete_log_range::apply;
use super::complete_log_rpc::{installed_config, prepared};
use super::complete_node_startup::snapshot;
use super::*;
use crate::node_runtime::pg_store::{
    CompletePublicationError, CompletePublicationRequest, PgStore,
};
use crate::node_runtime::server::CompletePublicationAuthority;
use crate::{PgState, RouteMapValidity, StoreError};
use std::time::{Duration, Instant};

#[path = "payload_staging_complete_publication_transport_tests.rs"]
mod transport;

#[path = "payload_staging_complete_publication_rpc_tests.rs"]
mod rpc;

#[path = "payload_staging_complete_publication_tls_tests.rs"]
mod tls;

#[path = "payload_staging_complete_publication_embedded_tests.rs"]
mod embedded;

#[path = "payload_staging_complete_publication_sender_tests.rs"]
mod sender;

#[path = "payload_staging_publication_handoff_tests.rs"]
mod handoff;

#[path = "payload_staging_complete_publication_historical_tests.rs"]
mod historical;

#[path = "payload_staging_complete_publication_embedded_historical_tests.rs"]
mod embedded_historical;

#[path = "payload_staging_complete_application_admission_tests.rs"]
mod application_admission;

#[path = "payload_staging_complete_application_embedded_tests.rs"]
mod application_embedded;

#[path = "payload_staging_complete_fanout_embedded_tests.rs"]
mod fanout_embedded;
#[path = "payload_staging_complete_fanout_mixed_tests.rs"]
mod fanout_mixed;
#[path = "payload_staging_complete_fanout_remote_primary_tests.rs"]
mod fanout_remote_primary;

#[path = "payload_staging_complete_application_rpc_tests.rs"]
mod application_rpc;

#[path = "payload_staging_complete_application_historical_tests.rs"]
mod application_historical;

#[path = "payload_staging_complete_application_sender_tests.rs"]
mod application_sender;

#[path = "payload_staging_complete_fanout_tests.rs"]
mod fanout;

#[path = "payload_staging_terminal_release_tests.rs"]
mod terminal_release;

#[path = "payload_staging_prefix_transition_tests.rs"]
mod prefix_transitions;
#[path = "payload_staging_retirement_embedded_tests.rs"]
mod retirement_embedded;
#[path = "payload_staging_terminal_retirement_tests.rs"]
mod terminal_retirement;
#[path = "payload_staging_window_confirmation_tests.rs"]
mod window_confirmation;

#[path = "payload_staging_retirement_transport_tests.rs"]
mod retirement_transport;

#[path = "payload_staging_retirement_receiver_tests.rs"]
mod retirement_receiver;

#[path = "payload_staging_abandoned_part_application_tests.rs"]
mod abandoned_part_application;
#[path = "payload_staging_abandoned_part_publication_tests.rs"]
mod abandoned_part_publication;
#[path = "payload_staging_abandoned_part_release_tests.rs"]
mod abandoned_part_release;
#[path = "payload_staging_abandoned_part_remote_tests.rs"]
mod abandoned_part_remote;
#[path = "payload_staging_abandoned_part_terminal_tests.rs"]
mod abandoned_part_terminal;
#[path = "payload_staging_abandonment_handoff_tests.rs"]
mod abandonment_handoff;
#[path = "payload_staging_abandonment_installation_tests.rs"]
mod abandonment_installation;
#[path = "payload_staging_abandonment_preparation_tests.rs"]
mod abandonment_preparation;
#[path = "payload_staging_abandonment_remote_tests.rs"]
mod abandonment_remote;
#[path = "payload_staging_replica_abandonment_tests.rs"]
mod replica_abandonment;
#[path = "payload_staging_replica_abandonment_receiver_tests.rs"]
mod replica_abandonment_receiver;
#[path = "payload_staging_replica_abandonment_sender_tests.rs"]
mod replica_abandonment_sender;
#[path = "payload_staging_unstarted_abandonment_tests.rs"]
mod unstarted_abandonment;

// A raw encoded command, closure, topology or deadline cannot replace admitted
// authority at the actual production store entry point.
const _: fn(
    &CompletePublicationRequest<'_>,
    &PgStore,
    &CompletePublicationAuthority<'_>,
) -> Result<(), CompletePublicationError> = |request, pg, authority| request.start(pg, authority);

fn install(pg: &PgStore, context: &ContextualCommand) {
    let encoded = context.encode_command().unwrap();
    let copy =
        ContextualCommand::decode_command(encoded.bytes(), encoded.checksum_crc64(), &topology())
            .unwrap();
    complete_pending::install(&mut copy.into_submission(), pg).unwrap();
    let observation = crate::node::SharedStorageNode::pg_heartbeat_observation_from_pg(
        pg,
        NodeId::new(4),
        context.id().pg_id(),
        PgState::Active,
    )
    .unwrap()
    .pending_metadata_command
    .unwrap();
    assert_eq!(observation.command_sha256(), encoded.command_sha256());
    assert_eq!(observation.command_checksum(), encoded.checksum_crc64());
}

fn started(pg: &PgStore) -> bool {
    pg.conn
        .query_row(
            "SELECT publication_started FROM metadata_command_pending_slot",
            [],
            |row| row.get(0),
        )
        .unwrap()
}

#[derive(Debug, PartialEq, Eq)]
struct WithoutMarker {
    materialized: Vec<crate::node_runtime::pg_store::MetadataCheckpointTableBlock>,
    prefix: crate::metadata_command::MetadataCommandReplicaState,
    log: Vec<Vec<String>>,
    indexes: Vec<Vec<String>>,
    slot: String,
}

fn without_marker(pg: &PgStore) -> WithoutMarker {
    WithoutMarker {
        materialized: introduction::state(pg), prefix: pg.metadata_command_replica_state().unwrap(), log: complete_maintenance::log_state(pg),
        indexes: complete_pending::observe(pg)[1..].to_vec(),
        slot: pg.conn.query_row("SELECT printf('%lld:%lld:%lld:%lld:%lld:%lld:%s', cluster_epoch, pg_id, log_index, command_checksum, placed_segment_reference_count, reclaim_reference_count, scope_bucket) || ':' || hex(command_bytes) FROM metadata_command_pending_slot", [], |row| row.get(0)).unwrap(),
    }
}

fn config(case: usize) -> crate::storage_node_server::StorageNodeProcessConfig {
    let mut config = capture::config(&fixture(StagingTarget::DirectPut));
    config.cluster_epoch = ClusterEpoch::new(if case == 2 { 6 } else { 5 }).unwrap();
    for route in &mut config.pg_routes {
        route.cluster_epoch = config.cluster_epoch;
    }
    config
}

#[test]
fn staging_complete_publication_trials_roll_back_before_marker_and_exact_replay_survives_restart() {
    for case in 0..4 {
        let dir = test_util::tempdir();
        let (server, _, config) = installed_config(&dir, config(case), true);
        let node = server.test_storage_node();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
        let context = context(&node.get_pg(pg_id.get()).unwrap(), case);
        install(&node.get_pg(pg_id.get()).unwrap(), &context);
        let pending = pending(&context);
        let request = pending
            .publication_request(config.node_id, &topology())
            .unwrap();
        let before = without_marker(&node.get_pg(pg_id.get()).unwrap());
        assert!(!started(&node.get_pg(pg_id.get()).unwrap()));
        server
            .test_with_complete_publication_authority(&request, deadline(), |pg, authority| {
                let mut trial = false;
                let mut marker = false;
                request
                    .test_start_with_hook(pg, authority, |stage| {
                        assert_eq!(
                            pg.conn.is_autocommit(),
                            stage == "after-commit",
                            "trial/marker checks belong to one transaction"
                        );
                        if stage != "after-commit" {
                            let error = pg
                                .commit_unmanaged_test_transaction("crossed publication commit")
                                .unwrap_err();
                            assert!(
                                error
                                    .to_string()
                                    .contains("owning metadata transaction scope"),
                                "{case}, {stage}: {error}"
                            );
                            assert!(!pg.conn.is_autocommit());
                        }
                        match stage {
                            "after-trial" => {
                                trial = true;
                                assert_ne!(
                                    introduction::state(pg),
                                    before.materialized,
                                    "{case}: trial must reach real application"
                                );
                                assert!(!started(pg));
                            }
                            "before-marker" => {
                                assert!(trial);
                                assert_eq!(without_marker(pg), before);
                                assert!(!started(pg));
                            }
                            "before-commit" => {
                                marker = true;
                                assert!(started(pg));
                                assert_eq!(without_marker(pg), before);
                            }
                            // COMMIT has ended the transaction by design.
                            _ => {}
                        }
                        Ok(())
                    })
                    .unwrap();
                assert!(trial && marker);
                assert!(pg.conn.is_autocommit());
                assert_eq!(without_marker(pg), before);
                assert!(started(pg));
                request.start(pg, authority).unwrap();
                assert_eq!(without_marker(pg), before);
                apply(pg, &context);
                let applied = snapshot(pg);
                let writes = pg.conn.total_changes();
                request
                    .test_start_with_hook(pg, authority, |stage| {
                        assert_ne!(
                            stage, "after-trial",
                            "exact-applied replay must not repeat owner transfers"
                        );
                        Ok(())
                    })
                    .unwrap();
                assert_eq!(snapshot(pg), applied);
                assert_eq!(pg.conn.total_changes(), writes);
            })
            .unwrap();
        let final_state = snapshot(&node.get_pg(pg_id.get()).unwrap());
        drop(node);
        drop(server);
        let (prepared, _) = prepared(&config);
        let server = prepared.bind_complete_for_test().unwrap();
        server
            .test_with_complete_publication_authority(&request, deadline(), |pg, authority| {
                let writes = pg.conn.total_changes();
                request.start(pg, authority).unwrap();
                assert_eq!(snapshot(pg), final_state);
                assert_eq!(pg.conn.total_changes(), writes);
            })
            .unwrap();
    }
}

#[test]
fn staging_complete_publication_failure_matrix_preserves_custody_and_marker_ambiguity() {
    for stage in [
        "before-trial",
        "after-trial",
        "before-marker",
        "before-commit",
        "after-commit",
    ] {
        let dir = test_util::tempdir();
        let (server, _, config) = installed_config(&dir, config(1), true);
        let node = server.test_storage_node();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
        let context = context(&node.get_pg(pg_id.get()).unwrap(), 1);
        install(&node.get_pg(pg_id.get()).unwrap(), &context);
        let pending = pending(&context);
        let request = pending
            .publication_request(config.node_id, &topology())
            .unwrap();
        let before = without_marker(&node.get_pg(pg_id.get()).unwrap());
        server
            .test_with_complete_publication_authority(&request, deadline(), |pg, authority| {
                let mut reached = false;
                let error = request
                    .test_start_with_hook(pg, authority, |at| {
                        if at == stage {
                            reached = true;
                            return Err(StoreError::RouteCapabilitySubjectMismatch {
                                operation: "injected complete publication failure",
                            });
                        }
                        Ok(())
                    })
                    .unwrap_err();
                assert!(reached, "{stage}");
                assert!(
                    error
                        .to_string()
                        .contains("injected complete publication failure"),
                    "{error}"
                );
                assert_eq!(without_marker(pg), before);
                assert_eq!(started(pg), stage == "after-commit");
                assert!(pg.conn.is_autocommit());
                pg.require_usable_connection().unwrap();
                request.start(pg, authority).unwrap();
                assert!(started(pg));
                assert_eq!(without_marker(pg), before);
            })
            .unwrap();
    }
}

#[test]
fn staging_complete_publication_rollback_failure_and_unwind_retire_unconfirmed_connections() {
    for mode in [
        "trial-rollback",
        "operation-and-trial-rollback",
        "outer-rollback",
        "unwind",
        "unwind-rollback",
    ] {
        let dir = test_util::tempdir();
        let (server, _, config) = installed_config(&dir, config(3), true);
        let node = server.test_storage_node();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
        // Use a real applied introduction before this unresolved append. Startup
        // deliberately rejects future-epoch pending slots; that separate boundary
        // must not mask rollback/reopen behavior in this test.
        let context = context(&node.get_pg(pg_id.get()).unwrap(), 3);
        assert_eq!(
            node.get_pg(pg_id.get())
                .unwrap()
                .metadata_command_replica_state()
                .unwrap()
                .cluster_epoch,
            context.id().cluster_epoch()
        );
        install(&node.get_pg(pg_id.get()).unwrap(), &context);
        let pending = pending(&context);
        let request = pending
            .publication_request(config.node_id, &topology())
            .unwrap();
        let before = snapshot(&node.get_pg(pg_id.get()).unwrap());
        server
            .test_with_complete_publication_authority(&request, deadline(), |pg, authority| {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    request.test_start_with_hook(pg, authority, |stage| {
                        if stage == "after-trial" {
                            match mode {
                                "trial-rollback" => pg.test_fail_metadata_command_rollbacks(1),
                                "operation-and-trial-rollback" => {
                                    pg.test_fail_metadata_command_rollbacks(1);
                                    return Err(StoreError::RouteCapabilitySubjectMismatch {
                                        operation: "injected trial operation failure",
                                    });
                                }
                                "outer-rollback" => {}
                                "unwind" => panic!("publication trial unwind"),
                                "unwind-rollback" => {
                                    pg.test_fail_metadata_command_rollbacks(1);
                                    panic!("publication trial unwind with failed rollback");
                                }
                                _ => unreachable!(),
                            }
                        }
                        if stage == "before-commit" && mode == "outer-rollback" {
                            assert!(started(pg), "outer failure must follow marker mutation");
                            pg.test_fail_metadata_command_rollbacks(1);
                            return Err(StoreError::RouteCapabilitySubjectMismatch {
                                operation: "injected marker operation failure",
                            });
                        }
                        Ok(())
                    })
                }));
                if mode.starts_with("unwind") {
                    assert!(result.is_err());
                } else {
                    let error = result.unwrap().unwrap_err().to_string();
                    assert!(error.contains("rollback"), "{mode}: {error}");
                    assert!(
                        error.contains("injected metadata-command rollback failure"),
                        "{mode}: {error}"
                    );
                    if mode == "operation-and-trial-rollback" {
                        assert!(
                            error.contains("injected trial operation failure"),
                            "{error}"
                        );
                    }
                    if mode == "outer-rollback" {
                        assert!(
                            error.contains("injected marker operation failure"),
                            "{error}"
                        );
                    }
                }
                if mode != "unwind" {
                    // Even the first failed child rollback is sticky. The outer
                    // owner must not hide it with a second rollback or commit.
                    assert!(!pg.conn.is_autocommit());
                    assert!(pg.require_usable_connection().is_err());
                    assert!(request.start(pg, authority).is_err());
                    assert_eq!(started(pg), mode == "outer-rollback");
                    // End the SQL transaction only to inspect residue; retirement
                    // must remain sticky and may not be repaired by this rollback.
                    pg.conn.execute_batch("ROLLBACK").unwrap();
                    assert!(pg.require_usable_connection().is_err());
                } else {
                    assert!(pg.conn.is_autocommit());
                    pg.require_usable_connection().unwrap();
                    assert_eq!(snapshot(pg), before);
                }
            })
            .unwrap();
        drop(node);
        drop(server);
        let (prepared, _) = prepared(&config);
        let server = prepared
            .bind_complete_for_test()
            .unwrap_or_else(|error| panic!("{mode}: {error:?}"));
        server
            .test_with_complete_publication_authority(&request, deadline(), |pg, authority| {
                pg.require_usable_connection().unwrap();
                assert_eq!(snapshot(pg), before);
                request.start(pg, authority).unwrap();
                assert!(started(pg));
            })
            .unwrap();
    }
}

#[test]
fn staging_complete_publication_revalidates_route_and_work_deadlines_at_effects() {
    for work in [false, true] {
        for stage in ["before-trial", "before-marker", "before-commit"] {
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
            let request = pending
                .publication_request(config.node_id, &topology())
                .unwrap();
            let before = snapshot(&node.get_pg(pg_id.get()).unwrap());
            let deadline = if work {
                Instant::now() + Duration::from_secs(1)
            } else {
                deadline()
            };
            server
                .test_with_complete_publication_authority(&request, deadline, |pg, authority| {
                    let mut reached = false;
                    let error = request
                        .test_start_with_hook(pg, authority, |at| {
                            if at == stage {
                                reached = true;
                                if work {
                                    while let Some(remaining) =
                                        deadline.checked_duration_since(Instant::now())
                                    {
                                        std::thread::sleep(remaining);
                                    }
                                } else {
                                    clock.set(6_000);
                                }
                            }
                            Ok(())
                        })
                        .unwrap_err();
                    assert!(reached, "{work}/{stage}");
                    let message = error.to_string();
                    assert!(
                        message.contains(if work {
                            "complete publication start"
                        } else {
                            "expired"
                        }),
                        "{message}"
                    );
                    assert_eq!(snapshot(pg), before);
                    assert!(!started(pg));
                    assert!(pg.conn.is_autocommit());
                })
                .unwrap();
        }
    }
}

#[test]
fn staging_complete_publication_rejects_damaged_slot_scope_indexes_and_terminal_before_trial() {
    for damage in [
        "scope-null",
        "scope-crossed",
        "routes",
        "window",
        "page",
        "abandoned",
        "terminal-bytes",
    ] {
        let dir = test_util::tempdir();
        let (server, _, config) = installed_config(&dir, config(3), true);
        let node = server.test_storage_node();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
        let context = context(&node.get_pg(pg_id.get()).unwrap(), 3);
        install(&node.get_pg(pg_id.get()).unwrap(), &context);
        let pending = pending(&context);
        let request = pending
            .publication_request(config.node_id, &topology())
            .unwrap();
        let pg = node.get_pg(pg_id.get()).unwrap();
        match damage {
            "scope-null" => {
                assert_eq!(
                    pg.conn
                        .execute(
                            "UPDATE metadata_command_pending_slot SET scope_bucket = NULL",
                            []
                        )
                        .unwrap(),
                    1
                );
            }
            "scope-crossed" => {
                assert_eq!(
                    pg.conn
                        .execute(
                            "UPDATE metadata_command_pending_slot SET scope_bucket = 'crossed'",
                            []
                        )
                        .unwrap(),
                    1
                );
            }
            "routes" => {
                assert_eq!(
                    pg.conn
                        .execute(
                            "DELETE FROM metadata_command_pending_route_dependencies",
                            []
                        )
                        .unwrap(),
                    1
                );
            }
            "window" => {
                assert_eq!(
                    pg.conn
                        .execute(
                            "DELETE FROM metadata_command_pending_staging_protection",
                            []
                        )
                        .unwrap(),
                    1
                );
            }
            "page" => {
                assert!(
                    pg.conn
                        .execute(
                            "DELETE FROM metadata_command_pending_placed_reference_pages",
                            []
                        )
                        .unwrap()
                        > 0
                );
            }
            "abandoned" => {
                pg.test_record_complete_command_abandoned(4, &context.encode_command().unwrap())
                    .unwrap();
            }
            "terminal-bytes" => {
                apply(&pg, &context);
                assert_eq!(pg.conn.execute("UPDATE metadata_command_log SET command_checksum = command_checksum + 1 WHERE log_index = ?1", [context.id().log_index().get() as i64]).unwrap(), 1);
            }
            _ => unreachable!(),
        }
        let before = snapshot(&pg);
        let writes = pg.conn.total_changes();
        drop(pg);
        server
            .test_with_complete_publication_authority(&request, deadline(), |pg, authority| {
                let result = request.test_start_with_hook(pg, authority, |stage| {
                    assert_ne!(stage, "after-trial", "{damage}");
                    Ok(())
                });
                assert!(result.is_err(), "{damage}");
                assert_eq!(snapshot(pg), before);
                assert_eq!(pg.conn.total_changes(), writes);
                assert!(pg.conn.is_autocommit());
            })
            .unwrap();
    }
}

#[test]
fn staging_complete_publication_requires_real_application_not_only_position_acceptance() {
    use crate::node_runtime::pg_store::{CompleteAcceptanceDisposition, CompleteAcceptanceKind};
    let dir = test_util::tempdir();
    let (server, _, config) = installed_config(&dir, config(3), true);
    let node = server.test_storage_node();
    let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
    let context = context(&node.get_pg(pg_id.get()).unwrap(), 3);
    install(&node.get_pg(pg_id.get()).unwrap(), &context);
    let pending = pending(&context);
    let request = pending
        .publication_request(config.node_id, &topology())
        .unwrap();
    let pg = node.get_pg(pg_id.get()).unwrap();
    assert_eq!(
        pg.conn.execute("DELETE FROM stream_uploads", []).unwrap(),
        1
    );
    pg.refresh_metadata_command_state_digest().unwrap();
    assert_eq!(
        pending
            .acceptance_probe(config.node_id, &topology(), CompleteAcceptanceKind::Apply)
            .unwrap()
            .read(&pg)
            .unwrap()
            .disposition(),
        CompleteAcceptanceDisposition::NextPosition
    );
    let before = snapshot(&pg);
    drop(pg);
    server
        .test_with_complete_publication_authority(&request, deadline(), |pg, authority| {
            let mut reached = false;
            let result = request.test_start_with_hook(pg, authority, |stage| {
                if stage == "before-trial" {
                    reached = true;
                }
                assert_ne!(
                    stage, "after-trial",
                    "missing session must fail real application"
                );
                Ok(())
            });
            assert!(reached && result.is_err());
            assert!(!started(pg));
            assert!(pg.conn.is_autocommit());
            assert_eq!(snapshot(pg), before);
        })
        .unwrap();
}

#[test]
fn staging_complete_publication_rejects_another_servers_active_permit() {
    reject_wrong_permit(false);
}

#[test]
fn staging_complete_publication_rejects_own_retained_cleanup_permit() {
    reject_wrong_permit(true);
}

fn reject_wrong_permit(retained_cleanup: bool) {
    let dir = test_util::tempdir();
    let foreign_dir = test_util::tempdir();
    let (server, _, config) = installed_config(&dir, config(0), true);
    // Identical node/route/topology configuration, but a different process-local
    // admission domain. No other subject mismatch may mask the permit check.
    let (foreign, _, _) = installed_config(&foreign_dir, self::config(0), true);
    let node = server.test_storage_node();
    let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
    let context = context(&node.get_pg(pg_id.get()).unwrap(), 0);
    install(&node.get_pg(pg_id.get()).unwrap(), &context);
    let pending = pending(&context);
    let request = pending
        .publication_request(config.node_id, &topology())
        .unwrap();
    let pg = node.get_pg(pg_id.get()).unwrap();
    let before = snapshot(&pg);
    let writes = pg.conn.total_changes();
    drop(pg);
    for draining in [false, true] {
        server.test_complete_publication_rejects_wrong_permit(
            &foreign,
            &request,
            retained_cleanup,
            draining,
        );
        let pg = node.get_pg(pg_id.get()).unwrap();
        assert_eq!(snapshot(&pg), before);
        assert_eq!(pg.conn.total_changes(), writes);
        assert!(!started(&pg));
    }
    server
        .test_with_complete_publication_authority(&request, deadline(), |pg, authority| {
            request.start(pg, authority).unwrap();
            assert!(started(pg));
        })
        .unwrap();
}

#[test]
fn staging_complete_publication_authority_binds_exact_command_store_topology_and_current_primary() {
    let dir = test_util::tempdir();
    let (server, _, config) = installed_config(&dir, config(0), true);
    let node = server.test_storage_node();
    let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
    let context = context(&node.get_pg(pg_id.get()).unwrap(), 0);
    install(&node.get_pg(pg_id.get()).unwrap(), &context);
    let pending = pending(&context);
    let request = pending
        .publication_request(config.node_id, &topology())
        .unwrap();
    let other_dir = test_util::tempdir();
    let other_pg = PgStore::open(other_dir.path(), pg_id.get()).unwrap();
    server.test_complete_publication_rejects_wrong_sections(&request);
    server
        .test_with_complete_publication_authority(&request, deadline(), |pg, authority| {
            let before = snapshot(pg);
            let writes = pg.conn.total_changes();
            assert!(request.start(&other_pg, authority).is_err());
            let crossed = crate::metadata_command::MetadataCommandEnvelope::new(
                context.id(),
                crate::metadata_command::MetadataCommandPayload::ReserveObjectVersion(
                    crate::metadata_command::ReserveObjectVersionCommand::new(
                        fixture(StagingTarget::DirectPut).subject.bucket,
                        fixture(StagingTarget::DirectPut).subject.key,
                        crate::VersionId::from_u64(99),
                    ),
                ),
            );
            let crossed =
                ContextualCommand::decode_field(crossed, &[1, 0, 0], &topology()).unwrap();
            let crossed = super::complete_applied_rpc::pending(&crossed);
            let crossed = crossed
                .publication_request(config.node_id, &topology())
                .unwrap();
            assert!(crossed.start(pg, authority).is_err());
            let crossed_node = pending
                .publication_request(NodeId::new(2), &topology())
                .unwrap();
            assert!(crossed_node.start(pg, authority).is_err());
            pg.conn.execute_batch("BEGIN").unwrap();
            assert!(request.start(pg, authority).is_err());
            assert!(!pg.conn.is_autocommit());
            pg.conn.execute_batch("ROLLBACK").unwrap();
            assert_eq!(snapshot(pg), before);
            assert_eq!(pg.conn.total_changes(), writes);
        })
        .unwrap();
    assert!(pending
        .publication_request(config.node_id, &PgTopology::new(&[pg_id.get()]).unwrap())
        .is_err());
    assert!(server
        .test_with_complete_publication_authority(&request, Instant::now(), |_, _| panic!(
            "expired admission minted authority"
        ))
        .is_err());
    for state in ["replica", "peering", "historical"] {
        let other_dir = test_util::tempdir();
        let mut config = self::config(0);
        if state == "historical" {
            config.historical_pg_routes = config.pg_routes.clone();
            config.cluster_epoch = ClusterEpoch::new(6).unwrap();
            for route in &mut config.pg_routes {
                route.cluster_epoch = config.cluster_epoch;
                route.state = PgState::Peering;
            }
        } else {
            let route = config
                .pg_routes
                .iter_mut()
                .find(|r| r.pg_id == pg_id.get())
                .unwrap();
            if state == "replica" {
                route.primary_node_id = NodeId::new(2);
            } else {
                route.state = PgState::Peering;
            }
        }
        let (server, _, _) = installed_config(&other_dir, config, true);
        assert!(server
            .test_with_complete_publication_authority(&request, deadline(), |_, _| panic!(
                "{state}: invalid authority"
            ))
            .is_err());
    }
    // Keep a valid request as a positive admission/application canary.
    server
        .test_with_complete_publication_authority(&request, deadline(), |pg, authority| {
            request.start(pg, authority).unwrap()
        })
        .unwrap();
}
