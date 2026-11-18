// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Exercise the fixed preservation entries, including failure after real effects.
//! These observations remain local; no physical authority or live activation.

use super::*;
use crate::node_runtime::pg_store::{
    command_log::CompleteSettledPrefixError, PgStoreRecoveryContext,
};

fn recover(pg: &PgStore) {
    pg.recover_complete_command_state(
        PgStoreRecoveryContext::for_node(NodeId::new(4)),
        &topology(),
    )
    .unwrap();
}

fn probe(pg: &PgStore) {
    pg.conn.execute_batch("CREATE TABLE prefix_transition_probe (value INTEGER); INSERT INTO prefix_transition_probe VALUES (0)").unwrap();
}

fn change(pg: &PgStore) {
    pg.conn
        .execute("UPDATE prefix_transition_probe SET value = value + 1", [])
        .unwrap();
}

fn observation(pg: &PgStore, warm: bool, pending: bool) {
    let state = pg.metadata_command_replica_state().unwrap();
    let writes = pg.conn.total_changes();
    let result = pg.inspect_complete_settled_prefix(4, state.cluster_epoch, &topology());
    match (warm, pending, result) {
        (false, _, Err(CompleteSettledPrefixError::ValidationRequired))
        | (
            true,
            true,
            Err(CompleteSettledPrefixError::Store(StoreError::MetadataCommandContention {
                ..
            })),
        ) => {}
        (true, false, Ok(read)) => {
            assert_eq!(*read.prefix(), state);
            read.finish().unwrap();
        }
        (_, _, Err(error)) => panic!("warm={warm}, pending={pending}: {error}"),
        (_, _, Ok(_)) => panic!("unexpected positive prefix: warm={warm}, pending={pending}"),
    }
    assert_eq!(pg.conn.total_changes(), writes);
    assert!(pg.conn.is_autocommit());
}

fn injected() -> StoreError {
    StoreError::RouteCapabilitySubjectMismatch {
        operation: "injected prefix transition failure",
    }
}

fn with_publication(
    action: impl FnOnce(&PgStore, &CompletePublicationRequest<'_>, &CompletePublicationAuthority<'_>),
) {
    let _clock = crate::clock::test_time_override_guard(2_000);
    let dir = test_util::tempdir();
    let (server, _, config) = installed_config(&dir, config(1), true);
    let node = server.test_storage_node();
    let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
    {
        let pg = node.get_pg(pg_id.get()).unwrap();
        let digest = pg.metadata_command_replica_state().unwrap().state_digest;
        pg.initialize_metadata_transfer_empty_state(4, config.cluster_epoch, digest)
            .unwrap();
    }
    let context = context(&node.get_pg(pg_id.get()).unwrap(), 1);
    install(&node.get_pg(pg_id.get()).unwrap(), &context);
    let pending = pending(&context);
    let request = pending
        .publication_request(config.node_id, &topology())
        .unwrap();
    server
        .test_with_complete_publication_authority(&request, deadline(), |pg, authority| {
            probe(pg);
            recover(pg);
            observation(pg, true, true);
            action(pg, &request, authority);
        })
        .unwrap();
}

#[test]
fn staging_prefix_install_preserves_only_existing_validation_without_extra_writes() {
    let _clock = crate::clock::test_time_override_guard(2_000);
    for case in 0..5 {
        let mut work = Vec::new();
        for warm in [false, true] {
            let (_dir, pg, context) = complete_application::setup(case);
            // The initial fixture may introduce epoch 5 from the default epoch
            // 1 prefix. Preservation intentionally does not cross epochs. Give
            // every case a real same-epoch predecessor before warming it.
            let id = context.id();
            let next = MetadataCommandId::new(
                id.cluster_epoch(),
                id.pg_id(),
                MetadataCommandLogIndex::new(id.log_index().get() + 1).unwrap(),
            );
            complete_application::anchor(&pg, next);
            let context = context.into_reissue(next).unwrap();
            if warm {
                recover(&pg);
            }
            let writes = pg.conn.total_changes();
            let scans = pg.test_metadata_command_log_replay_validation_entries();
            let mut holder = context.into_submission();
            complete_pending::install(&mut holder, &pg).unwrap();
            work.push((
                pg.conn.total_changes() - writes,
                pg.test_metadata_command_log_replay_validation_entries() - scans,
            ));
            observation(&pg, warm, true);
            let writes = pg.conn.total_changes();
            complete_pending::install(&mut holder, &pg).unwrap();
            assert_eq!(pg.conn.total_changes(), writes);
            observation(&pg, warm, true);
        }
        assert_eq!(
            work[0], work[1],
            "case {case}: extra writes or prefix scans"
        );
    }
}

#[test]
fn staging_prefix_install_rejects_intervening_writes_and_unconfirmed_finish() {
    let _clock = crate::clock::test_time_override_guard(2_000);
    for outcome in [
        "before",
        "before-commit",
        "unwind",
        "commit-failure",
        "rollback-failure",
        "external",
    ] {
        let (dir, pg, context) = complete_application::setup(0);
        probe(&pg);
        if outcome.contains("failure") {
            pg.conn.execute_batch("CREATE TEMP TABLE prefix_deferred_failure (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES prefix_deferred_failure(id) DEFERRABLE INITIALLY DEFERRED); CREATE TEMP TRIGGER prefix_fail_commit AFTER INSERT ON main.metadata_command_pending_slot BEGIN INSERT INTO prefix_deferred_failure VALUES (1, 2); END;").unwrap();
        }
        let other = PgStore::open(dir.path(), pg.pg_id).unwrap();
        recover(&pg);
        if outcome == "before" {
            change(&pg);
        }
        if outcome == "external" {
            change(&other);
        }
        if outcome == "rollback-failure" {
            pg.test_fail_metadata_command_rollbacks(1);
        }
        let mut holder = context.into_submission();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            PgStore::test_with_before_pending_slot_finish_hook(
                move |pg| {
                    assert_ne!(outcome, "unwind", "injected prefix transition unwind");
                    if outcome == "before-commit" {
                        change(pg);
                    }
                },
                || complete_pending::install(&mut holder, &pg),
            )
        }));
        if outcome == "unwind" {
            assert!(result.is_err());
        } else if outcome.contains("failure") {
            assert!(result
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("commit pending"));
        } else {
            result.unwrap().unwrap();
        }
        if outcome == "rollback-failure" {
            assert!(pg.require_usable_connection().is_err());
        } else {
            observation(&pg, false, false);
        }
    }
}

#[test]
fn staging_prefix_publication_rejects_interleaved_writes_at_every_boundary() {
    for stage in [
        "before-trial",
        "after-trial",
        "before-marker",
        "before-commit",
        "after-commit",
    ] {
        with_publication(|pg, request, authority| {
            let mut reached = false;
            request
                .test_start_with_hook(pg, authority, |at| {
                    if at == stage {
                        reached = true;
                        change(pg);
                    }
                    Ok(())
                })
                .unwrap();
            assert!(reached);
            observation(pg, false, true);
            // Exact retry cannot turn a cold entry hot.
            request.start(pg, authority).unwrap();
            observation(pg, false, true);
        });
    }
}

#[test]
fn staging_prefix_publication_failure_and_unwind_do_not_preserve_validation() {
    for stage in [
        "before-trial",
        "after-trial",
        "before-marker",
        "before-commit",
        "after-commit",
    ] {
        for unwind in [false, true] {
            with_publication(|pg, request, authority| {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    request.test_start_with_hook(pg, authority, |at| {
                        if at == stage {
                            assert!(!unwind, "injected prefix transition unwind");
                            return Err(injected());
                        }
                        Ok(())
                    })
                }));
                if unwind {
                    assert!(result.is_err());
                } else {
                    assert!(result
                        .unwrap()
                        .unwrap_err()
                        .to_string()
                        .contains("injected prefix transition failure"));
                }
                assert_eq!(started(pg), stage == "after-commit");
                observation(pg, false, true);
                request.start(pg, authority).unwrap();
                observation(pg, false, true);
            });
        }
    }
}

#[test]
fn staging_prefix_publication_commit_and_rollback_failure_never_preserve() {
    for rollback_failure in [false, true] {
        with_publication(|pg, request, authority| {
            pg.conn.execute_batch("CREATE TEMP TABLE prefix_deferred_failure (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES prefix_deferred_failure(id) DEFERRABLE INITIALLY DEFERRED); CREATE TEMP TRIGGER prefix_fail_commit AFTER UPDATE OF publication_started ON main.metadata_command_pending_slot BEGIN INSERT INTO prefix_deferred_failure VALUES (1, 2); END;").unwrap();
            recover(pg);
            let error = request
                .test_start_with_hook(pg, authority, |at| {
                    if rollback_failure && at == "before-commit" {
                        pg.test_fail_metadata_command_rollbacks(1);
                    }
                    Ok(())
                })
                .unwrap_err();
            assert!(
                error.to_string().contains("complete publication commit"),
                "{error}"
            );
            if rollback_failure {
                assert!(pg.require_usable_connection().is_err());
            } else {
                observation(pg, false, true);
            }
        });
    }
}

#[test]
fn staging_prefix_publication_preserves_validation_without_extra_writes_or_recovery() {
    let mut work = Vec::new();
    for warm in [false, true] {
        with_publication(|pg, request, authority| {
            if !warm {
                change(pg);
            }
            let writes = pg.conn.total_changes();
            let scans = pg.test_metadata_command_log_replay_validation_entries();
            request.start(pg, authority).unwrap();
            work.push((
                pg.conn.total_changes() - writes,
                pg.test_metadata_command_log_replay_validation_entries() - scans,
            ));
            observation(pg, warm, true);
            // A repeated trial is rolled back and may increment total_changes;
            // it still has to preserve precisely the same validated prefix.
            request.start(pg, authority).unwrap();
            observation(pg, warm, true);
        });
    }
    assert_eq!(
        work[0], work[1],
        "publication added writes or prefix validation"
    );
}

#[test]
fn staging_prefix_publication_rechecks_external_writers_and_retired_lifetimes() {
    for outcome in [
        "external-before",
        "external-after",
        "retired-before",
        "retired-after",
    ] {
        with_publication(|pg, request, authority| {
            // An independent connection changes data_version, not total_changes
            // on the admitted connection. No exclusive-opening change is needed.
            let other = rusqlite::Connection::open(pg.conn.path().unwrap()).unwrap();
            if outcome == "external-before" {
                other
                    .execute("UPDATE prefix_transition_probe SET value = value + 1", [])
                    .unwrap();
            }
            if outcome == "retired-before" {
                drop(pg.begin_allocation_lifetime_replacement().unwrap());
            }
            request
                .test_start_with_hook(pg, authority, |at| {
                    if at == "after-commit" {
                        if outcome == "external-after" {
                            other
                                .execute("UPDATE prefix_transition_probe SET value = value + 1", [])
                                .unwrap();
                        }
                        if outcome == "retired-after" {
                            drop(pg.begin_allocation_lifetime_replacement().unwrap());
                        }
                    }
                    Ok(())
                })
                .unwrap();
            assert!(started(pg));
            if outcome.starts_with("external") {
                observation(pg, false, true);
            } else {
                assert!(matches!(
                    pg.inspect_complete_settled_prefix(
                        4,
                        request.id().cluster_epoch(),
                        &topology()
                    ),
                    Err(CompleteSettledPrefixError::Store(
                        StoreError::PgAllocationLifetimeUnavailable { .. }
                    ))
                ));
            }
        });
    }
}

#[test]
fn staging_prefix_retirement_preserves_only_existing_validation_without_extra_writes() {
    let mut work = Vec::new();
    for warm in [false, true] {
        terminal_retirement::with_terminal(
            1,
            "installed",
            |server, config, context, terminal, _| {
                let evidence = pending(context);
                let request = evidence
                    .publication_request(config.node_id, &topology())
                    .unwrap();
                server
                    .test_with_complete_publication_authority(
                        &request,
                        deadline(),
                        |pg, authority| {
                            // The full flow may already preserve validation. Explicitly cool
                            // the negative canary so both entry conditions are independent.
                            probe(pg);
                            if warm {
                                recover(pg);
                            }
                            observation(pg, warm, true);
                            let writes = pg.conn.total_changes();
                            let scans = pg.test_metadata_command_log_replay_validation_entries();
                            terminal
                                .test_retire_quiescent_pending(pg, authority, |_| Ok(()))
                                .unwrap();
                            work.push((
                                pg.conn.total_changes() - writes,
                                pg.test_metadata_command_log_replay_validation_entries() - scans,
                            ));
                            observation(pg, warm, false);
                        },
                    )
                    .unwrap();
            },
        );
    }
    assert_eq!(
        work[0], work[1],
        "retirement added writes or prefix validation"
    );
}

#[test]
fn staging_prefix_retirement_interleaving_failures_and_unwind_leave_validation_cold() {
    for stage in ["before-delete", "before-commit", "after-commit"] {
        for action in ["write", "fail", "unwind"] {
            terminal_retirement::with_terminal(
                1,
                "installed",
                |server, config, context, terminal, _| {
                    let evidence = pending(context);
                    let request = evidence
                        .publication_request(config.node_id, &topology())
                        .unwrap();
                    server
                        .test_with_complete_publication_authority(
                            &request,
                            deadline(),
                            |pg, authority| {
                                probe(pg);
                                recover(pg);
                                let mut reached = false;
                                let result =
                                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                        terminal.test_retire_quiescent_pending(
                                            pg,
                                            authority,
                                            |at| {
                                                if at == stage {
                                                    reached = true;
                                                    match action {
                                                        "write" => change(pg),
                                                        "fail" => return Err(injected()),
                                                        "unwind" => panic!(
                                                            "injected prefix transition unwind"
                                                        ),
                                                        _ => unreachable!(),
                                                    }
                                                }
                                                Ok(())
                                            },
                                        )
                                    }));
                                assert!(reached);
                                match action {
                                    "write" => result.unwrap().unwrap(),
                                    "fail" => assert!(result
                                        .unwrap()
                                        .unwrap_err()
                                        .to_string()
                                        .contains("injected prefix transition failure")),
                                    "unwind" => assert!(result.is_err()),
                                    _ => unreachable!(),
                                }
                                observation(pg, false, false);
                                let removed = complete_pending::observe(pg)[0].is_empty();
                                assert_eq!(removed, action == "write" || stage == "after-commit");
                                // Retrying may settle custody, but must not manufacture local validation.
                                terminal
                                    .test_retire_quiescent_pending(pg, authority, |_| Ok(()))
                                    .unwrap();
                                observation(pg, false, false);
                            },
                        )
                        .unwrap();
                },
            );
        }
    }
}

#[test]
fn staging_prefix_retirement_commit_and_rollback_failure_never_preserve() {
    for rollback_failure in [false, true] {
        terminal_retirement::with_terminal(
            1,
            "installed",
            |server, config, context, terminal, _| {
                let evidence = pending(context);
                let request = evidence
                    .publication_request(config.node_id, &topology())
                    .unwrap();
                server.test_with_complete_publication_authority(&request, deadline(), |pg, authority| {
                pg.conn.execute_batch("CREATE TEMP TABLE prefix_deferred_failure (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES prefix_deferred_failure(id) DEFERRABLE INITIALLY DEFERRED); CREATE TEMP TRIGGER prefix_fail_commit AFTER DELETE ON main.metadata_command_pending_slot BEGIN INSERT INTO prefix_deferred_failure VALUES (1, 2); END;").unwrap();
                recover(pg);
                if rollback_failure { pg.test_fail_metadata_command_rollbacks(1); }
                let error = terminal.test_retire_quiescent_pending(pg, authority, |_| Ok(())).unwrap_err();
                assert!(error.to_string().contains("complete slot retirement commit"), "{error}");
                if rollback_failure {
                    assert!(pg.require_usable_connection().is_err());
                } else {
                    assert!(!complete_pending::observe(pg)[0].is_empty());
                    observation(pg, false, true);
                }
            }).unwrap();
            },
        );
    }
}
