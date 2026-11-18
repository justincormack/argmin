// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Complete-carrier replacements through the actual single slot transaction.
//! Metadata-only tests; not a live recovery/custody or physical-write switch.

use super::super::command_context::ContextualCommand;
use super::super::submission::{
    ContextualReissue, ContextualSubmission, InstallationObservation as Observed, SubmissionError,
};
use super::complete_pending::{install, observe, setup};
use super::*;
use crate::clock::test_time_override_guard;
use crate::node_runtime::pg_store::PgStore;
use crate::{PgTopology, StoreError};
use std::sync::atomic::Ordering;

fn next(id: MetadataCommandId) -> MetadataCommandId {
    MetadataCommandId::new(
        id.cluster_epoch(),
        id.pg_id(),
        MetadataCommandLogIndex::new(id.log_index().get() + 1).unwrap(),
    )
}

fn reissue(source: ContextualSubmission) -> ContextualReissue {
    let id = next(source.observation().command_id);
    source.into_same_epoch_reissue(id).unwrap()
}

fn unresolved(pair: ContextualReissue) -> ContextualReissue {
    match pair.into_observed_successor() {
        Ok(_) => panic!("unconfirmed replacement detached its original context"),
        Err(pair) => *pair,
    }
}

fn successor(pair: ContextualReissue) -> ContextualSubmission {
    match pair.into_observed_successor() {
        Ok(successor) => successor,
        Err(_) => panic!("observed exact replacement did not converge"),
    }
}

fn stored(pg: &PgStore) -> ContextualCommand {
    let (bytes, crc): (Vec<u8>, i64) = pg
        .conn
        .query_row(
            "SELECT command_bytes, command_checksum FROM metadata_command_pending_slot",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    ContextualCommand::decode_command(&bytes, crc as u64, &topology()).unwrap()
}

#[test]
fn staging_complete_reissue_preserves_context_through_replacements_and_reopen() {
    let clock = test_time_override_guard(2_000);
    for case in 0..7 {
        clock.set(2_000);
        let (dir, mut pg, context) = setup(case);
        let before = introduction::state(&pg);
        let prefix = pg.metadata_command_replica_state().unwrap();
        let original_context = context.encode_field().unwrap();
        let mut holder = context.into_submission();
        install(&mut holder, &pg).unwrap();
        let original_window = pg
            .pending_staging_protection(&topology())
            .unwrap()
            .unwrap()
            .window;
        let original_routes = pg
            .pending_metadata_command_route_references(0)
            .unwrap()
            .unwrap()
            .dependencies;
        clock.set(900_000); // Convergence must preserve, not renew, the original cutoff.
        for _ in 0..2 {
            let source = stored(&pg);
            let mut pair = unresolved(reissue(holder));
            assert!(pair.replace(&pg, 0, &topology()).unwrap());
            assert_eq!(pair.source().installation, Observed::Installed);
            assert_eq!(pair.successor().installation, Observed::Installed);
            let replacement = stored(&pg);
            assert_eq!(replacement.id(), pair.successor().command_id);
            assert_eq!(replacement.encode_field().unwrap(), original_context);
            pair.assert_contexts_for_test(&source, &replacement, &topology());
            let encoded = replacement.encode_command().unwrap();
            let indexed = pg.pending_staging_protection(&topology()).unwrap().unwrap();
            assert_eq!(indexed.command, replacement.id());
            assert_eq!(indexed.command_checksum, encoded.checksum_crc64());
            assert_eq!(indexed.window, original_window);
            let routes = pg
                .pending_metadata_command_route_references(0)
                .unwrap()
                .unwrap();
            assert_eq!(routes.id, replacement.id());
            assert_eq!(routes.command_checksum, encoded.checksum_crc64());
            assert_eq!(routes.dependencies, original_routes);
            let pending = observe(&pg);
            let writes = pg.conn.total_changes();
            assert!(pair.replace(&pg, 0, &topology()).unwrap());
            assert_eq!(
                pg.conn.total_changes(),
                writes,
                "exact replay rewrote indexes"
            );
            assert_eq!(observe(&pg), pending);
            assert_eq!(introduction::state(&pg), before);
            assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
            assert!(
                pg.pending_metadata_command_slot_any_epoch(0).is_err(),
                "live decoder accepted V14"
            );
            drop(pg);
            pg = PgStore::open(dir.path(), replacement.id().pg_id().get()).unwrap();
            assert_eq!(observe(&pg), pending);
            assert!(pair.replace(&pg, 0, &topology()).unwrap());
            holder = successor(pair);
            holder.assert_context_for_test(&replacement, &topology());
        }
        let pending = observe(&pg);
        let writes = pg.conn.total_changes();
        drop(holder);
        assert_eq!(observe(&pg), pending);
        assert_eq!(pg.conn.total_changes(), writes, "hidden Drop cleanup");
    }
}

#[test]
fn staging_complete_reissue_lost_reply_retains_pair_across_rejected_retries() {
    let _clock = test_time_override_guard(2_000);
    for case in 0..7 {
        for fatal in [false, true] {
            let (_dir, pg, context) = setup(case);
            let mut source = context.into_submission();
            install(&mut source, &pg).unwrap();
            let expected_source = stored(&pg);
            let mut pair = reissue(source);
            pg.fail_next_pending_slot_replace_after_commit
                .store(true, Ordering::Relaxed);
            pg.fail_pending_slot_replace_after_commit_with_fatal_error
                .store(fatal, Ordering::Relaxed);
            let error = pair.replace(&pg, 0, &topology()).unwrap_err();
            assert!(
                matches!(error, SubmissionError::Unconfirmed(_)),
                "{error:?}"
            );
            let mut pair = unresolved(pair);
            let replacement = stored(&pg);
            pair.assert_contexts_for_test(&expected_source, &replacement, &topology());
            let pending = observe(&pg);
            let writes = pg.conn.total_changes();
            let foreign = PgTopology::new(&[pair.source().command_id.pg_id().get()]).unwrap();
            assert!(matches!(
                pair.replace(&pg, 0, &foreign),
                Err(SubmissionError::Rejected(_))
            ));
            assert_eq!(pair.successor().installation, Observed::Unconfirmed);
            pg.fail_next_pending_slot_inspection
                .store(true, Ordering::Relaxed);
            assert!(matches!(
                pair.replace(&pg, 0, &topology()),
                Err(SubmissionError::Unconfirmed(_))
            ));
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            assert!(matches!(
                pair.replace(&pg, 0, &topology()),
                Err(SubmissionError::OpenTransaction)
            ));
            assert!(!pg.conn.is_autocommit());
            pg.conn.execute_batch("ROLLBACK").unwrap();
            assert_eq!(pair.source().installation, Observed::Installed);
            assert_eq!(pair.successor().installation, Observed::Unconfirmed);
            let mut pair = unresolved(pair);
            assert!(pair.replace(&pg, 0, &topology()).unwrap());
            successor(pair).assert_context_for_test(&replacement, &topology());
            assert_eq!(observe(&pg), pending);
            assert_eq!(pg.conn.total_changes(), writes);
        }
    }
}

#[test]
fn staging_complete_reissue_rejects_damaged_source_and_successor_indexes_without_repair() {
    let _clock = test_time_override_guard(2_000);
    for case in 0..7 {
        for replace_first in [false, true] {
            for damage in [
                "UPDATE metadata_command_pending_slot SET scope_bucket = NULL",
                "UPDATE metadata_command_pending_slot SET scope_bucket = 'crossed-reissue-bucket'",
                "DELETE FROM metadata_command_pending_route_dependencies",
                "UPDATE metadata_command_pending_route_dependencies SET dependency_checksum = dependency_checksum + 1",
                "DELETE FROM metadata_command_pending_staging_protection",
                "UPDATE metadata_command_pending_staging_protection SET protection_checksum = protection_checksum + 1",
                "UPDATE metadata_command_pending_slot SET placed_segment_reference_count = placed_segment_reference_count + 1",
            ] {
                let (_dir, pg, context) = setup(case);
                let mut holder = context.into_submission();
                install(&mut holder, &pg).unwrap();
                let mut pair = reissue(holder);
                if replace_first {
                    assert!(pair.replace(&pg, 0, &topology()).unwrap());
                }
                pg.conn.execute_batch(damage).unwrap();
                let before = observe(&pg);
                let writes = pg.conn.total_changes();
                let error = pair.replace(&pg, 0, &topology()).unwrap_err();
                assert!(matches!(error, SubmissionError::Unconfirmed(_)), "{case} {replace_first} {damage}: {error:?}");
                if damage.contains("scope_bucket") {
                    assert!(matches!(error, SubmissionError::Unconfirmed(StoreError::RouteCapabilitySubjectMismatch {
                        operation: "replace pending metadata command existing bucket scope",
                    })), "{error:?}");
                }
                assert_eq!(pair.source().installation, Observed::Installed);
                assert_eq!(pair.successor().installation, Observed::Unconfirmed);
                drop(unresolved(pair));
                assert_eq!(observe(&pg), before);
                assert_eq!(pg.conn.total_changes(), writes);
            }
        }
    }
}

#[test]
fn staging_complete_reissue_false_or_definitive_result_does_not_reset_uncertainty() {
    let _clock = test_time_override_guard(2_000);
    for case in 0..7 {
        for state in 0..4 {
            let (_dir, pg, context) = setup(case);
            let mut holder = context.into_submission();
            install(&mut holder, &pg).unwrap();
            let original = stored(&pg);
            let mut pair = reissue(holder);
            pg.fail_next_pending_slot_replace_before_commit
                .store(true, Ordering::Relaxed);
            assert!(matches!(
                pair.replace(&pg, 0, &topology()),
                Err(SubmissionError::Unconfirmed(_))
            ));
            assert!(matches!(
                pair.replace(&pg, 0, &topology()),
                Err(SubmissionError::Unconfirmed(_))
            ));
            match state {
                0 => {
                    pg.conn
                        .execute_batch("DELETE FROM metadata_command_pending_slot")
                        .unwrap();
                }
                1 => {
                    pg.conn
                        .execute_batch(
                            "UPDATE metadata_command_pending_slot SET publication_started = 1",
                        )
                        .unwrap();
                }
                2 => {
                    let contender_id = next(pair.successor().command_id);
                    let contender = original
                        .into_same_epoch_reissue_pair(contender_id)
                        .unwrap()
                        .1;
                    pg.conn
                        .execute_batch("DELETE FROM metadata_command_pending_slot")
                        .unwrap();
                    install(&mut contender.into_submission(), &pg).unwrap();
                }
                3 => {
                    pg.fail_next_pending_slot_replace_definitively_with_fatal_error
                        .store(true, Ordering::Relaxed);
                }
                _ => unreachable!(),
            }
            let before = observe(&pg);
            let writes = pg.conn.total_changes();
            let result = pair.replace(&pg, 0, &topology());
            if state == 3 {
                assert!(
                    matches!(result, Err(SubmissionError::Rejected(_))),
                    "{result:?}"
                );
            } else {
                assert!(!result.unwrap());
            }
            assert_eq!(pair.successor().installation, Observed::Unconfirmed);
            drop(unresolved(pair));
            assert_eq!(observe(&pg), before);
            assert_eq!(pg.conn.total_changes(), writes);
        }
    }
}

#[test]
fn staging_complete_reissue_transaction_failures_preserve_both_commands_and_restore_indexes() {
    let _clock = test_time_override_guard(2_000);
    for case in 0..7 {
        for fault in 0..5 {
            let (dir, pg, context) = setup(case);
            let before = introduction::state(&pg);
            let mut holder = context.into_submission();
            install(&mut holder, &pg).unwrap();
            let pending = observe(&pg);
            let mut pair = reissue(holder);
            let sql = match fault {
                0 => "CREATE TEMP TRIGGER fail_reissue BEFORE INSERT ON metadata_command_pending_route_dependencies BEGIN SELECT RAISE(ABORT, 'route replacement failure'); END;",
                1 | 3 | 4 => "CREATE TEMP TRIGGER fail_reissue BEFORE INSERT ON metadata_command_pending_staging_protection BEGIN SELECT RAISE(ABORT, 'staging replacement failure'); END;",
                2 => "CREATE TEMP TABLE deferred_failure (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES deferred_failure(id) DEFERRABLE INITIALLY DEFERRED); CREATE TEMP TRIGGER fail_reissue AFTER UPDATE ON metadata_command_pending_staging_protection BEGIN INSERT INTO deferred_failure VALUES (1, 2); END;",
                _ => unreachable!(),
            };
            pg.conn.execute_batch(sql).unwrap();
            if fault >= 3 {
                pg.test_fail_metadata_command_rollbacks(fault - 2);
            }
            let writes = pg.conn.total_changes();
            let error = pair
                .replace(&pg, 0, &topology())
                .expect_err(&format!("case {case}, fault {fault}"));
            assert!(
                matches!(error, SubmissionError::Unconfirmed(_)),
                "{error:?}"
            );
            assert!(
                pg.conn.total_changes() > writes,
                "fault occurred before replacement writes"
            );
            if fault == 2 {
                assert!(
                    matches!(
                        error,
                        SubmissionError::Unconfirmed(StoreError::Db {
                            context: "commit pending metadata command slot transaction",
                            ..
                        })
                    ),
                    "{error:?}"
                );
            }
            assert_eq!(pair.successor().installation, Observed::Unconfirmed);
            if fault >= 3 {
                assert!(!pg.conn.is_autocommit());
                assert_eq!(
                    pg.fail_metadata_command_rollbacks.get(),
                    fault - 3,
                    "teardown secretly retried rollback"
                );
                let error = pair.replace(&pg, 0, &topology()).unwrap_err();
                assert!(
                    matches!(
                        error,
                        SubmissionError::Unconfirmed(StoreError::Db {
                            context: "access retired PG metadata connection",
                            ..
                        })
                    ),
                    "{error:?}"
                );
            } else {
                assert!(pg.conn.is_autocommit());
                assert_eq!(observe(&pg), pending);
            }
            let mut pair = unresolved(pair);
            drop(pg);
            let pg = PgStore::open(dir.path(), pair.source().command_id.pg_id().get()).unwrap();
            assert_eq!(observe(&pg), pending);
            assert_eq!(introduction::state(&pg), before);
            assert!(pair.replace(&pg, 0, &topology()).unwrap());
            assert_eq!(
                successor(pair).observation().installation,
                Observed::Installed
            );
        }
    }
}

#[test]
fn staging_complete_reissue_reauthenticates_abandoned_source_on_source_and_successor_retry() {
    let _clock = test_time_override_guard(2_000);
    for replaced in [false, true] {
        let (_dir, pg, context) = setup(6);
        let mut holder = context.into_submission();
        install(&mut holder, &pg).unwrap();
        let mut pair = reissue(holder);
        if replaced {
            assert!(pair.replace(&pg, 0, &topology()).unwrap());
        }
        let prepared = pair
            .prepare_abandoned_part_pending(&pg, 0, &topology())
            .unwrap();
        let mut pair = prepared.finish().unwrap();
        pg.conn.execute_batch("DELETE FROM metadata_command_log; DELETE FROM metadata_command_terminal_receipts; DELETE FROM metadata_command_checkpoints").unwrap();
        let before = observe(&pg);
        let writes = pg.conn.total_changes();
        let error = pair.replace(&pg, 0, &topology()).unwrap_err();
        assert!(
            matches!(error, SubmissionError::Unconfirmed(_)),
            "{error:?}"
        );
        drop(unresolved(pair));
        assert_eq!(observe(&pg), before);
        assert_eq!(pg.conn.total_changes(), writes);
    }
}

#[test]
fn staging_complete_reissue_rejects_context_downgrade_at_replacement_boundary() {
    let _clock = test_time_override_guard(2_000);
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(multipart_target()).subject.object_pg.get(),
    )
    .unwrap();
    let owner = part::initialize(&pg, 2);
    let source = part::command(&pg, &owner);
    let cleanup = abandonment::cleanup(&pg, &owner, &source);
    let ordinary =
        ContextualCommand::decode_field(cleanup.clone(), &[1, 0, 0], &topology()).unwrap();
    let bound =
        super::super::abandonment::source::PartFinalizationSource::capture_for_test(&source)
            .unwrap()
            .bind_cleanup(cleanup, &topology())
            .unwrap();
    let complete = ContextualCommand::abandoned_part(bound, &topology()).unwrap();
    abandonment::terminal(&pg, &source, true);
    pg.conn.execute_batch("COMMIT").unwrap();
    let (_, downgraded) = ordinary
        .into_same_epoch_reissue_pair(next(complete.id()))
        .unwrap();
    let encoded = complete.encode_command().unwrap();
    let install_context =
        ContextualCommand::decode_command(encoded.bytes(), encoded.checksum_crc64(), &topology())
            .unwrap();
    install(&mut install_context.into_submission(), &pg).unwrap();
    let before = observe(&pg);
    let writes = pg.conn.total_changes();
    let mut pair = ContextualReissue::from_contexts_for_test(complete, downgraded);
    let result = pair.replace(&pg, 0, &topology());
    assert!(
        matches!(
            result,
            Err(SubmissionError::Rejected(
                StoreError::RouteCapabilitySubjectMismatch {
                    operation: "replace complete pending command context",
                }
            ))
        ),
        "{result:?}"
    );
    assert_eq!(
        pair.successor().installation,
        Observed::NoAcceptanceObserved
    );
    drop(unresolved(pair));
    assert_eq!(observe(&pg), before);
    assert_eq!(pg.conn.total_changes(), writes);
}

#[test]
fn staging_complete_reissue_unwind_keeps_both_candidates_and_unconfirmed_history() {
    let _clock = test_time_override_guard(2_000);
    for case in 0..7 {
        let (_dir, pg, context) = setup(case);
        let mut holder = context.into_submission();
        install(&mut holder, &pg).unwrap();
        let before = observe(&pg);
        let writes = pg.conn.total_changes();
        let mut pair = reissue(holder);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            PgStore::test_with_before_pending_slot_effect_hook(
                |in_transaction| {
                    assert!(in_transaction);
                    panic!("replacement interrupted before its SQL effect");
                },
                || pair.replace(&pg, 0, &topology()),
            )
        }));
        assert!(result.is_err());
        assert_eq!(pair.source().installation, Observed::Installed);
        assert_eq!(pair.successor().installation, Observed::Unconfirmed);
        assert!(pg.conn.is_autocommit());
        pg.require_usable_connection().unwrap();
        assert_eq!(observe(&pg), before);
        assert_eq!(pg.conn.total_changes(), writes);
        let mut pair = unresolved(pair);
        assert!(pair.replace(&pg, 0, &topology()).unwrap());
        assert_eq!(
            successor(pair).observation().installation,
            Observed::Installed
        );
    }
}

#[test]
fn staging_complete_reissue_unwind_after_replacement_retains_pair_until_reopen() {
    let _clock = test_time_override_guard(2_000);
    for case in 0..7 {
        for failed_rollback in [false, true] {
            let (dir, pg, context) = setup(case);
            let pg_id = pg.pg_id;
            let before = introduction::state(&pg);
            let mut source = context.into_submission();
            install(&mut source, &pg).unwrap();
            let pending = observe(&pg);
            let expected = pending.clone();
            let mut pair = reissue(source);
            let id = pair.successor().command_id;
            let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                PgStore::test_with_before_pending_slot_finish_hook(
                    move |pg| {
                        assert_ne!(observe(pg), expected);
                        assert_eq!(stored(pg).id(), id);
                        assert!(pg
                            .pending_staging_protection(&topology())
                            .unwrap()
                            .is_some());
                        if failed_rollback {
                            pg.test_fail_metadata_command_rollbacks(1);
                        }
                        panic!("interrupted after complete pending replacement");
                    },
                    || pair.replace(&pg, 0, &topology()),
                )
            }));
            assert!(panic.is_err());
            assert_eq!(pair.source().installation, Observed::Installed);
            assert_eq!(pair.successor().installation, Observed::Unconfirmed);
            let mut pair = unresolved(pair);
            if failed_rollback {
                assert!(!pg.conn.is_autocommit());
                assert!(pg.require_usable_connection().is_err());
                assert!(matches!(
                    pair.replace(&pg, 0, &topology()),
                    Err(SubmissionError::Unconfirmed(_))
                ));
                assert!(!pg.conn.is_autocommit());
            } else {
                assert!(pg.conn.is_autocommit());
                assert_eq!(observe(&pg), pending);
            }
            drop(pg);
            let pg = PgStore::open(dir.path(), pg_id).unwrap();
            assert_eq!(observe(&pg), pending);
            assert_eq!(introduction::state(&pg), before);
            assert!(pair.replace(&pg, 0, &topology()).unwrap());
            assert_eq!(
                successor(pair).observation().installation,
                Observed::Installed
            );
        }
    }
}

#[test]
fn staging_complete_reissue_rejects_changed_descriptor_before_replacement_writes() {
    use super::super::candidate::DescribedStagingCommand;
    use super::super::descriptor::{AppendDescriptor, IntroductionDescriptor};
    let _clock = test_time_override_guard(2_000);
    for (target, is_append) in candidate::cases() {
        let owner = fixture(target);
        let command = if is_append {
            append::command(&owner)
        } else {
            introduction::command(&owner)
        };
        let description = |shorten: bool| {
            let bytes = if is_append {
                let mut lookahead = window(&owner.subject, 1, owner.reserved_end);
                lookahead.cutoff_ms -= u64::from(shorten);
                AppendDescriptor::capture(&owner.validate(&topology()).unwrap(), lookahead)
                    .unwrap()
                    .encode()
                    .unwrap()
            } else {
                let mut changed = owner.clone();
                changed.windows[0].cutoff_ms -= u64::from(shorten);
                changed.maximum_cutoff_ms = changed.windows[0].cutoff_ms;
                IntroductionDescriptor::capture(&changed.validate(&topology()).unwrap())
                    .unwrap()
                    .encode()
                    .unwrap()
            };
            ContextualCommand::from_described(
                DescribedStagingCommand::decode_descriptor_field(
                    command.clone(),
                    &bytes,
                    &topology(),
                )
                .unwrap(),
            )
        };
        let source = description(false);
        let changed = description(true)
            .into_same_epoch_reissue_pair(next(source.id()))
            .unwrap()
            .1;
        assert_ne!(
            source.encode_field().unwrap(),
            changed.encode_field().unwrap()
        );
        let dir = test_util::tempdir();
        let pg = PgStore::open(dir.path(), source.id().pg_id().get()).unwrap();
        let before = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        let mut pair = ContextualReissue::from_contexts_for_test(source, changed);
        let error = pair.replace(&pg, 0, &topology()).unwrap_err();
        assert!(
            matches!(
                error,
                SubmissionError::Rejected(StoreError::RouteCapabilitySubjectMismatch {
                    operation: "replace complete pending command context",
                })
            ),
            "{error:?}"
        );
        assert_eq!(
            pair.successor().installation,
            Observed::NoAcceptanceObserved
        );
        drop(unresolved(pair));
        assert_eq!(introduction::state(&pg), before);
        assert_eq!(pg.conn.total_changes(), writes);
        pending_install::assert_empty(&pg);
    }
}

#[test]
fn staging_complete_reissue_store_boundary_requires_and_records_owning_history() {
    use super::super::CompletePendingReplacement;
    // Pin the actual mutation API's capability-only shape, not a wrapper that
    // leaves a parallel independent-encoding entry available in production.
    let replace: fn(
        &PgStore,
        u32,
        CompletePendingReplacement<'_>,
    ) -> Result<bool, SubmissionError> =
        PgStore::replace_complete_pending_metadata_command_slot_for_reissue;
    let _clock = test_time_override_guard(2_000);
    let topology = topology();
    for case in 0..7 {
        for confirm in [false, true] {
            let (_dir, pg, context) = setup(case);
            let mut holder = context.into_submission();
            install(&mut holder, &pg).unwrap();
            let source = stored(&pg);
            let mut pair = reissue(holder);
            pg.fail_next_pending_slot_replace_after_commit
                .store(true, Ordering::Relaxed);
            // Skip ContextualReissue::replace deliberately. The store entry
            // must itself preserve both candidates and mark the lost reply.
            assert!(matches!(
                replace(&pg, 0, pair.replacement_attempt_for_test(&topology)),
                Err(SubmissionError::Unconfirmed(_))
            ));
            let replacement = stored(&pg);
            pair.assert_contexts_for_test(&source, &replacement, &topology);
            assert_eq!(pair.source().installation, Observed::Installed);
            assert_eq!(pair.successor().installation, Observed::Unconfirmed);
            let mut pair = unresolved(pair);
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            assert!(matches!(
                replace(&pg, 0, pair.replacement_attempt_for_test(&topology)),
                Err(SubmissionError::OpenTransaction)
            ));
            assert!(!pg.conn.is_autocommit());
            assert_eq!(pair.successor().installation, Observed::Unconfirmed);
            pg.conn.execute_batch("ROLLBACK").unwrap();
            let foreign = PgTopology::new(&[pair.source().command_id.pg_id().get()]).unwrap();
            assert!(matches!(
                replace(&pg, 0, pair.replacement_attempt_for_test(&foreign)),
                Err(SubmissionError::Rejected(_))
            ));
            assert_eq!(pair.successor().installation, Observed::Unconfirmed);
            if !confirm {
                pg.conn
                    .execute_batch("DELETE FROM metadata_command_pending_slot")
                    .unwrap();
            }
            let before = observe(&pg);
            let writes = pg.conn.total_changes();
            assert_eq!(
                replace(&pg, 0, pair.replacement_attempt_for_test(&topology)).unwrap(),
                confirm
            );
            if confirm {
                let holder = successor(pair);
                assert_eq!(holder.observation().installation, Observed::Installed);
                holder.assert_context_for_test(&replacement, &topology);
            } else {
                assert_eq!(pair.successor().installation, Observed::Unconfirmed);
                drop(unresolved(pair));
            }
            assert_eq!(observe(&pg), before);
            assert_eq!(pg.conn.total_changes(), writes);
        }
    }
}

#[test]
fn staging_complete_reissue_uses_one_existing_transaction_for_slot_and_indexes() {
    use rusqlite::trace::{TraceEvent, TraceEventCodes};
    use std::sync::{Arc, Mutex};
    let _clock = test_time_override_guard(2_000);
    for case in 0..7 {
        let (_dir, mut pg, context) = setup(case);
        let mut holder = context.into_submission();
        install(&mut holder, &pg).unwrap();
        let mut pair = reissue(holder);
        let statements = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&statements);
        pg.conn
            .trace_v2(
                TraceEventCodes::SQLITE_TRACE_STMT,
                Some(move |event: TraceEvent<'_>| {
                    if let TraceEvent::Stmt(_, sql) = event {
                        captured.lock().unwrap().push(sql.to_owned());
                    }
                }),
            )
            .unwrap();
        assert!(pair.replace(&pg, 0, &topology()).unwrap());
        let statements = statements.lock().unwrap();
        assert_eq!(
            statements
                .iter()
                .filter(|sql| sql.starts_with("BEGIN") || sql.starts_with("SAVEPOINT"))
                .count(),
            1
        );
        assert_eq!(
            statements
                .iter()
                .filter(|sql| sql.as_str() == "BEGIN IMMEDIATE")
                .count(),
            1
        );
        assert_eq!(
            statements
                .iter()
                .filter(|sql| sql.as_str() == "COMMIT")
                .count(),
            1
        );
        let update = statements
            .iter()
            .position(|sql| sql.starts_with("UPDATE metadata_command_pending_slot "))
            .unwrap();
        let commit = statements.iter().position(|sql| sql == "COMMIT").unwrap();
        for table in [
            "metadata_command_pending_route_dependencies",
            "metadata_command_pending_staging_protection",
        ] {
            let insert = statements
                .iter()
                .position(|sql| sql.starts_with(&format!("INSERT INTO {table} ")))
                .unwrap();
            assert!(update < insert && insert < commit);
        }
    }
}
