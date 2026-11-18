// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Genuine five-member application followed by the closed local transaction
//! seam. This is NOT a retirement RPC test or live publisher activation.

use super::*;
use crate::node_runtime::pg_store::{
    CompleteTerminalHandoff, TerminalSlotRetirementObservation as O,
};
use crate::storage_node_server::{StorageNodeProcessConfig, StorageNodeServer};
use std::sync::Arc;

pub(super) fn with_terminal(
    case: usize,
    history: &str,
    action: impl FnOnce(
        &StorageNodeServer,
        &StorageNodeProcessConfig,
        &ContextualCommand,
        &mut CompleteTerminalHandoff<'_>,
        &Arc<crate::LocalClusterMap>,
    ),
) {
    with_terminal_client(
        case,
        history,
        |server, config, context, terminal, map, _| {
            action(server, config, context, terminal, map);
        },
    );
}

pub(super) fn with_terminal_client(
    case: usize,
    history: &str,
    action: impl FnOnce(
        &StorageNodeServer,
        &StorageNodeProcessConfig,
        &ContextualCommand,
        &mut CompleteTerminalHandoff<'_>,
        &Arc<crate::LocalClusterMap>,
        &crate::node_runtime::clients::UnixStorageNodeClient,
    ),
) {
    with_terminal_transport(false, case, history, action);
}

pub(super) fn with_terminal_transport(
    tls: bool,
    case: usize,
    history: &str,
    action: impl FnOnce(
        &StorageNodeServer,
        &StorageNodeProcessConfig,
        &ContextualCommand,
        &mut CompleteTerminalHandoff<'_>,
        &Arc<crate::LocalClusterMap>,
        &crate::node_runtime::clients::UnixStorageNodeClient,
    ),
) {
    let dir = test_util::tempdir();
    let (server, client, config, context) = if case < 4 {
        application_rpc::setup(tls, case, false, &dir)
    } else {
        let (server, client, config) = if tls {
            super::tls::installed_tls(&dir, 0)
        } else {
            installed_config(&dir, config(0), true)
        };
        let node = server.test_storage_node();
        let pg = node
            .get_pg(fixture(StagingTarget::DirectPut).subject.object_pg.get())
            .unwrap();
        let clock = crate::clock::test_time_override_guard(2_000);
        let mut owner = fixture(if case == 4 {
            StagingTarget::StreamPut
        } else {
            multipart_target()
        });
        owner.subject.id.introduction = MetadataCommandId::new(
            owner.subject.originating_epoch,
            owner.subject.object_pg,
            crate::metadata_command::MetadataCommandLogIndex::new(1).unwrap(),
        );
        let command = fresh_capture::candidate(&pg, &mut owner);
        let context =
            ContextualCommand::from_described(candidate::capture(&pg, &owner, command, false));
        complete_application::anchor(&pg, context.id());
        install(&pg, &context);
        drop(clock);
        drop(pg);
        (server, client, config, context)
    };
    let client = Arc::new(client);
    let dirs: Vec<_> = (0..4).map(|_| test_util::tempdir()).collect();
    let peers: Vec<_> = [2, 0, 3, 1]
        .into_iter()
        .zip(&dirs)
        .map(|(id, dir)| {
            let (peer, client) = application_sender::replica_from_primary(
                tls,
                &server,
                &config,
                context.id().pg_id(),
                dir,
                NodeId::new(id),
            );
            (Arc::new(peer), Arc::new(client))
        })
        .collect();
    let mut clients = vec![Arc::clone(&client)];
    clients.extend(peers.iter().map(|(_, client)| Arc::clone(client)));
    let map_dir = test_util::tempdir();
    let (map, handle) = fanout::map(&map_dir, &config, &clients);
    let admission = handle.admit_current_route().unwrap();
    let route = admission
        .complete_fanout_route(context.id().pg_id(), context.id().cluster_epoch())
        .unwrap();
    let sender = if history == "inspected" {
        pending(&context).into_publication_sender(config.node_id, deadline())
    } else {
        let bytes = context.encode_command().unwrap();
        let mut submission =
            ContextualCommand::decode_command(bytes.bytes(), bytes.checksum_crc64(), &topology())
                .unwrap()
                .into_submission();
        let node = server.test_storage_node();
        let pg = node.get_pg(context.id().pg_id().get()).unwrap();
        if history == "uncertain" {
            pg.conn
                .execute_batch("DELETE FROM metadata_command_pending_slot")
                .unwrap();
            handoff::uncertain_install(&mut submission, &pg);
            install(&pg, &context);
        } else {
            complete_pending::install(&mut submission, &pg).unwrap();
        }
        submission.into_publication_sender(config.node_id, deadline())
    };
    let mut fanout = sender
        .into_fanout(route)
        .unwrap_or_else(|(_, error)| panic!("{error}"));
    let original = fanout.installation_observation();
    let running: Vec<_> = peers
        .iter()
        .map(|(server, _)| application_sender::RunningReplica::start(Arc::clone(server), tls))
        .collect();
    let mut terminal = std::thread::scope(|scope| {
        let worker = scope.spawn(|| server.accept_one().unwrap());
        let section = client
            .open_metadata_command_critical_section_until(context.id().pg_id(), deadline())
            .unwrap();
        fanout.publish(&client, &section, deadline()).unwrap();
        fanout.apply_replica(&peers[1].1, deadline()).unwrap();
        fanout.apply_primary(&client, &section, deadline()).unwrap();
        for (index, (_, client)) in peers.iter().enumerate() {
            if index != 1 {
                fanout.apply_replica(client, deadline()).unwrap();
            }
        }
        drop(section);
        worker.join().unwrap();
        fanout
            .into_terminal()
            .unwrap_or_else(|failure| panic!("{}", failure.error()))
    });
    assert_eq!(terminal.slot_retirement_observation(), O::NoAttemptObserved);
    let before = snapshot(
        &server
            .test_storage_node()
            .get_pg(context.id().pg_id().get())
            .unwrap(),
    );
    action(&server, &config, &context, &mut terminal, &map, &client);
    assert_eq!(terminal.installation_observation(), original);
    let node = server.test_storage_node();
    let observe_drop = || {
        node.get_pg(context.id().pg_id().get())
            .map(|pg| (complete_pending::observe(&pg), pg.conn.total_changes()))
            .map_err(|error| error.to_string())
    };
    let before_drop = observe_drop();
    let retired = terminal.slot_retirement_observation() == O::Retired;
    let after = if retired {
        Some(snapshot(&node.get_pg(context.id().pg_id().get()).unwrap()))
    } else {
        None
    };
    drop(terminal);
    assert_eq!(
        observe_drop(),
        before_drop,
        "Drop must not retry retirement"
    );
    drop(running);
    if before_drop.is_err() || retired {
        drop(node);
        drop(server);
        let reopened = if tls {
            super::tls::tls_server_at(config.clone(), true, config.cluster_epoch).0
        } else {
            prepared(&config).0.bind_complete_for_test().unwrap()
        };
        let node = reopened.test_storage_node();
        let pg = node.get_pg(context.id().pg_id().get()).unwrap();
        pg.require_usable_connection().unwrap();
        assert_eq!(
            snapshot(&pg),
            after.unwrap_or(before),
            "reopen must preserve the exact terminal state"
        );
    }
}

#[test]
fn staging_terminal_slot_retirement_preserves_owners_prefix_and_original_custody() {
    for case in [0, 1, 3, 4] {
        for history in ["inspected", "installed", "uncertain"] {
            with_terminal(case, history, |server, config, context, terminal, _| {
                let evidence = pending(context);
                let request = evidence
                    .publication_request(config.node_id, &topology())
                    .unwrap();
                server
                    .test_with_complete_publication_authority(
                        &request,
                        deadline(),
                        |pg, authority| {
                            let materialized = introduction::state(pg);
                            let log = complete_maintenance::log_state(pg);
                            let prefix = pg.metadata_command_replica_state().unwrap();
                            assert!(!complete_pending::observe(pg)[0].is_empty());
                            terminal
                                .test_retire_quiescent_pending(pg, authority, |_| Ok(()))
                                .unwrap();
                            assert_eq!(terminal.slot_retirement_observation(), O::Retired);
                            assert!(complete_pending::observe(pg).iter().all(Vec::is_empty));
                            assert_eq!(introduction::state(pg), materialized);
                            assert_eq!(complete_maintenance::log_state(pg), log);
                            assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
                            let writes = pg.conn.total_changes();
                            terminal
                                .test_retire_quiescent_pending(pg, authority, |_| Ok(()))
                                .unwrap();
                            assert_eq!(
                                pg.conn.total_changes(),
                                writes,
                                "confirmed retirement must not dispatch again"
                            );
                        },
                    )
                    .unwrap();
            });
        }
    }
}

#[test]
fn staging_terminal_slot_retirement_rejects_work_even_without_physical_segments() {
    for case in [2, 5] {
        with_terminal(case, "inspected", |server, config, context, terminal, _| {
            let evidence = pending(context);
            let request = evidence
                .publication_request(config.node_id, &topology())
                .unwrap();
            server
                .test_with_complete_publication_authority(&request, deadline(), |pg, authority| {
                    let before = snapshot(pg);
                    let error = terminal
                        .test_retire_quiescent_pending(pg, authority, |_| {
                            panic!("gated work reached transaction")
                        })
                        .unwrap_err();
                    assert!(
                        error
                            .to_string()
                            .contains("still requires terminal work integration"),
                        "{error}"
                    );
                    assert_eq!(terminal.slot_retirement_observation(), O::NoAttemptObserved);
                    assert_eq!(snapshot(pg), before);
                })
                .unwrap();
        });
    }
}

fn failure() -> StoreError {
    StoreError::RouteCapabilitySubjectMismatch {
        operation: "injected slot retirement failure",
    }
}

#[test]
fn staging_terminal_slot_retirement_rolls_back_and_retains_ambiguity_after_commit_or_unwind() {
    for stage in ["before-delete", "before-commit", "after-commit"] {
        for unwind in [false, true] {
            with_terminal(3, "uncertain", |server, config, context, terminal, _| {
                let evidence = pending(context);
                let request = evidence
                    .publication_request(config.node_id, &topology())
                    .unwrap();
                server
                    .test_with_complete_publication_authority(
                        &request,
                        deadline(),
                        |pg, authority| {
                            let before = snapshot(pg);
                            let materialized = introduction::state(pg);
                            let mut reached = false;
                            let result =
                                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    terminal.test_retire_quiescent_pending(pg, authority, |at| {
                                        if at == stage {
                                            reached = true;
                                            if unwind {
                                                panic!("injected retirement unwind");
                                            }
                                            return Err(failure());
                                        }
                                        Ok(())
                                    })
                                }));
                            assert!(reached);
                            if unwind {
                                assert!(result.is_err());
                            } else {
                                assert!(result.unwrap().is_err());
                            }
                            assert_eq!(terminal.slot_retirement_observation(), O::Unconfirmed);
                            assert!(pg.conn.is_autocommit());
                            assert_eq!(introduction::state(pg), materialized);
                            if stage == "after-commit" {
                                assert!(complete_pending::observe(pg).iter().all(Vec::is_empty));
                                pg.record_complete_metadata_command_checkpoint(
                                    config.node_id.as_u32(),
                                    &topology(),
                                )
                                .unwrap();
                                pg.compact_complete_metadata_command_log(&topology())
                                    .unwrap();
                                assert!(complete_maintenance::log_state(pg)[0].is_empty());
                            } else {
                                assert_eq!(snapshot(pg), before);
                            }
                            terminal
                                .test_retire_quiescent_pending(pg, authority, |_| Ok(()))
                                .unwrap();
                            assert_eq!(terminal.slot_retirement_observation(), O::Retired);
                        },
                    )
                    .unwrap();
            });
        }
    }
}

#[test]
fn staging_terminal_slot_retirement_rejects_damaged_exact_slot_without_repair() {
    for case in [0, 1, 3] {
        for damage in [
            "UPDATE metadata_command_pending_slot SET scope_bucket = NULL",
            "UPDATE metadata_command_pending_slot SET scope_bucket = 'crossed-retirement-bucket'",
            "UPDATE metadata_command_pending_slot SET publication_started = 0",
            "UPDATE metadata_command_pending_slot SET command_checksum = command_checksum + 1",
            "DELETE FROM metadata_command_pending_route_dependencies",
            "UPDATE metadata_command_pending_route_dependencies SET dependency_checksum = dependency_checksum + 1",
            "DELETE FROM metadata_command_pending_staging_protection",
            "UPDATE metadata_command_pending_staging_protection SET protection_checksum = protection_checksum + 1",
            "UPDATE metadata_command_pending_slot SET placed_segment_reference_count = placed_segment_reference_count + 1",
        ] {
            with_terminal(case, "installed", |server, config, context, terminal, _| {
                let evidence = pending(context);
                let request = evidence.publication_request(config.node_id, &topology()).unwrap();
                server.test_with_complete_publication_authority(&request, deadline(), |pg, authority| {
                    pg.conn.execute_batch(damage).unwrap();
                    let before = snapshot(pg);
                    let writes = pg.conn.total_changes();
                    let error = terminal.test_retire_quiescent_pending(pg, authority, |_| panic!("damaged custody reached delete")).unwrap_err();
                    let error = error.into_store_error().unwrap();
                    if damage.contains("command_checksum =") {
                        assert!(matches!(error, StoreError::MetadataCommandLogChecksumMismatch { .. }), "{case}: {damage}: {error}");
                    } else if damage.contains("metadata_command_pending_staging_protection") {
                        assert!(matches!(error, StoreError::RouteCapabilitySubjectMismatch { operation: "pending-staging-protection-index" }), "{case}: {damage}: {error}");
                    } else if damage.contains("placed_segment_reference_count") {
                        assert!(matches!(error, StoreError::RouteCapabilitySubjectMismatch { operation: "pending-staging-install-existing-payload-pages" }), "{case}: {damage}: {error}");
                    } else {
                        assert!(matches!(error, StoreError::MetadataCommandLogConflict { .. }), "{case}: {damage}: {error}");
                    }
                    assert_eq!(terminal.slot_retirement_observation(), O::Unconfirmed);
                    assert_eq!(snapshot(pg), before);
                    assert_eq!(pg.conn.total_changes(), writes);
                }).unwrap();
            });
        }
    }
}

#[test]
fn staging_terminal_slot_retirement_rechecks_original_authority_at_delete() {
    with_terminal(3, "inspected", |server, config, context, terminal, map| {
        let evidence = pending(context);
        let request = evidence
            .publication_request(config.node_id, &topology())
            .unwrap();
        server
            .test_with_complete_publication_authority(&request, deadline(), |pg, authority| {
                let before = snapshot(pg);
                let writes = pg.conn.total_changes();
                let now = crate::clock::current_time_millis();
                let clock = crate::clock::test_time_override_guard(now);
                let error = terminal
                    .test_retire_quiescent_pending(pg, authority, |stage| {
                        if stage == "before-delete" {
                            clock.set(now + 120_000);
                            map.test_store_route_map_validity(
                                RouteMapValidity::until_ms(now + 240_000).unwrap(),
                            );
                            map.require_route_map_valid_now().unwrap();
                        }
                        Ok(())
                    })
                    .unwrap_err();
                assert!(
                    error.to_string().contains("deadline") || error.to_string().contains("expired"),
                    "{error}"
                );
                assert_eq!(terminal.slot_retirement_observation(), O::Unconfirmed);
                assert_eq!(snapshot(pg), before);
                assert_eq!(pg.conn.total_changes(), writes);
            })
            .unwrap();
    });
}

#[test]
fn staging_terminal_slot_retirement_retires_connection_on_unconfirmed_rollback() {
    with_terminal(3, "inspected", |server, config, context, terminal, _| {
        let evidence = pending(context);
        let request = evidence
            .publication_request(config.node_id, &topology())
            .unwrap();
        server
            .test_with_complete_publication_authority(&request, deadline(), |pg, authority| {
                let before = complete_pending::observe(pg);
                let error = terminal
                    .test_retire_quiescent_pending(pg, authority, |stage| {
                        if stage == "before-commit" {
                            pg.test_fail_metadata_command_rollbacks(1);
                            return Err(failure());
                        }
                        Ok(())
                    })
                    .unwrap_err();
                assert!(error.to_string().contains("rollback failed"), "{error}");
                assert_eq!(terminal.slot_retirement_observation(), O::Unconfirmed);
                assert!(!pg.conn.is_autocommit());
                assert!(pg.require_usable_connection().is_err());
                assert!(terminal
                    .test_retire_quiescent_pending(pg, authority, |_| Ok(()))
                    .is_err());
                pg.conn.execute_batch("ROLLBACK").unwrap();
                assert!(
                    pg.require_usable_connection().is_err(),
                    "manual rollback must not revive the store"
                );
                // Inspection below uses owner-test SQL, not an allowed production read.
                assert_eq!(complete_pending::observe(pg), before);
            })
            .unwrap();
    });
}

#[test]
fn staging_terminal_slot_retirement_rejects_missing_terminal_or_orphan_indexes() {
    for damage in [
        "DELETE FROM metadata_command_log; DELETE FROM metadata_command_terminal_receipts",
        "PRAGMA foreign_keys = OFF; DELETE FROM metadata_command_pending_slot; PRAGMA foreign_keys = ON",
    ] {
        with_terminal(3, "inspected", |server, config, context, terminal, _| {
            let evidence = pending(context);
            let request = evidence.publication_request(config.node_id, &topology()).unwrap();
            server.test_with_complete_publication_authority(&request, deadline(), |pg, authority| {
                pg.conn.execute_batch(damage).unwrap();
                let before = snapshot(pg);
                let writes = pg.conn.total_changes();
                assert!(terminal.test_retire_quiescent_pending(pg, authority, |_| panic!("invalid terminal evidence reached deletion")).is_err());
                assert_eq!(terminal.slot_retirement_observation(), O::Unconfirmed);
                assert_eq!(snapshot(pg), before);
                assert_eq!(pg.conn.total_changes(), writes);
            }).unwrap();
        });
    }
}

#[test]
fn staging_terminal_slot_retirement_rejects_newer_slot_and_crossed_primary_request() {
    with_terminal(0, "inspected", |server, config, context, terminal, _| {
        let node = server.test_storage_node();
        let next = complete_node_startup::next(
            &node.get_pg(context.id().pg_id().get()).unwrap(),
            context.id().cluster_epoch(),
        );
        let crossed = pending(&next);
        let crossed_request = crossed
            .publication_request(config.node_id, &topology())
            .unwrap();
        server
            .test_with_complete_publication_authority(
                &crossed_request,
                deadline(),
                |pg, authority| {
                    let before = snapshot(pg);
                    let error = terminal
                        .test_retire_quiescent_pending(pg, authority, |_| {
                            panic!("crossed authority reached transaction")
                        })
                        .unwrap_err();
                    assert!(
                        error.to_string().contains("admitted command/store/primary"),
                        "{error}"
                    );
                    assert_eq!(snapshot(pg), before);
                },
            )
            .unwrap();
        let evidence = pending(context);
        let request = evidence
            .publication_request(config.node_id, &topology())
            .unwrap();
        server
            .test_with_complete_publication_authority(&request, deadline(), |pg, authority| {
                pg.conn
                    .execute_batch("DELETE FROM metadata_command_pending_slot")
                    .unwrap();
                install(pg, &next);
                let before = snapshot(pg);
                let writes = pg.conn.total_changes();
                let error = terminal
                    .test_retire_quiescent_pending(pg, authority, |_| {
                        panic!("newer slot reached deletion")
                    })
                    .unwrap_err();
                assert!(matches!(
                    error.into_store_error(),
                    Ok(StoreError::MetadataCommandLogConflict { .. })
                ));
                assert_eq!(terminal.slot_retirement_observation(), O::Unconfirmed);
                assert_eq!(snapshot(pg), before);
                assert_eq!(pg.conn.total_changes(), writes);
            })
            .unwrap();
    });
}

#[test]
fn staging_terminal_slot_retirement_rechecks_receivers_shorter_cutoff() {
    with_terminal(3, "inspected", |server, config, context, terminal, _| {
        let evidence = pending(context);
        let request = evidence
            .publication_request(config.node_id, &topology())
            .unwrap();
        let cutoff = Instant::now() + Duration::from_secs(2);
        server
            .test_with_complete_publication_authority(&request, cutoff, |pg, authority| {
                let before = snapshot(pg);
                let writes = pg.conn.total_changes();
                let mut reached = false;
                let error = terminal
                    .test_retire_quiescent_pending(pg, authority, |stage| {
                        if stage == "before-delete" {
                            reached = true;
                            assert!(
                                Instant::now() < cutoff,
                                "must expire inside the transaction"
                            );
                            while let Some(remaining) =
                                cutoff.checked_duration_since(Instant::now())
                            {
                                std::thread::sleep(remaining);
                            }
                        }
                        Ok(())
                    })
                    .unwrap_err();
                assert!(reached);
                assert!(matches!(
                    error.into_store_error(),
                    Ok(StoreError::OperationDeadlineExceeded { .. })
                ));
                assert_eq!(terminal.slot_retirement_observation(), O::Unconfirmed);
                assert_eq!(snapshot(pg), before);
                assert_eq!(pg.conn.total_changes(), writes);
            })
            .unwrap();
    });
}

#[test]
fn staging_terminal_slot_retirement_failed_sqlite_commit_keeps_exact_retry_custody() {
    with_terminal(3, "uncertain", |server, config, context, terminal, _| {
        let evidence = pending(context);
        let request = evidence
            .publication_request(config.node_id, &topology())
            .unwrap();
        server.test_with_complete_publication_authority(&request, deadline(), |pg, authority| {
            pg.conn.execute_batch("CREATE TEMP TABLE retirement_commit_fault (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES retirement_commit_fault(id) DEFERRABLE INITIALLY DEFERRED); CREATE TEMP TRIGGER fail_retirement AFTER DELETE ON metadata_command_pending_slot BEGIN INSERT INTO retirement_commit_fault VALUES (1, 2); END;").unwrap();
            let before = snapshot(pg);
            let mut deleted = false;
            let error = terminal.test_retire_quiescent_pending(pg, authority, |stage| {
                if stage == "before-commit" {
                    deleted = true;
                    assert!(complete_pending::observe(pg).iter().all(Vec::is_empty));
                }
                Ok(())
            }).unwrap_err();
            assert!(deleted);
            assert!(error.to_string().contains("complete slot retirement commit"), "{error}");
            assert_eq!(terminal.slot_retirement_observation(), O::Unconfirmed);
            assert!(pg.conn.is_autocommit());
            assert_eq!(snapshot(pg), before);
            pg.conn.execute_batch("DROP TRIGGER fail_retirement; DROP TABLE retirement_commit_fault").unwrap();
            terminal.test_retire_quiescent_pending(pg, authority, |_| Ok(())).unwrap();
            assert_eq!(terminal.slot_retirement_observation(), O::Retired);
        }).unwrap();
    });
}
