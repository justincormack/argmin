// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Complete descriptions through real local pending transactions. These are
//! metadata-only component tests, not frontend custody or RPC activation tests.

use super::super::candidate::CandidateBindingError;
use super::super::submission::{
    InstallationObservation as Observed, StagingSubmission, SubmissionError,
};
use super::*;
use crate::clock::test_time_override_guard;
use crate::node_runtime::pg_store::PgStore;
use crate::types::AdmittedRouteEffectFence;
use std::time::Instant;

pub(super) fn install(
    submission: &mut StagingSubmission,
    pg: &PgStore,
) -> Result<(), SubmissionError> {
    let epoch = submission.description().command().id().cluster_epoch();
    submission.install(
        pg,
        0,
        &topology(),
        AdmittedRouteEffectFence::unbounded(epoch),
        None,
    )
}

fn expired_retry(submission: &mut StagingSubmission, pg: &PgStore) {
    let epoch = submission.description().command().id().cluster_epoch();
    let before = pending_install::observe(pg);
    let changes = pg.conn.total_changes();
    let result = submission.install(
        pg,
        0,
        &topology(),
        AdmittedRouteEffectFence::unbounded(epoch),
        Some(Instant::now()),
    );
    assert!(
        matches!(
            result,
            Err(SubmissionError::Rejected(
                crate::StoreError::OperationDeadlineExceeded { .. }
            ))
        ),
        "{result:?}"
    );
    assert_eq!(pending_install::observe(pg), before);
    assert_eq!(pg.conn.total_changes(), changes);
}

#[test]
fn staging_submission_preserves_complete_candidate_through_install_retry_and_reopen() {
    let _clock = test_time_override_guard(2_000);
    for (target, is_append) in candidate::cases() {
        let (dir, pg, owner, command) = candidate::setup(target, is_append);
        let expected_command = command.clone();
        let captured = candidate::capture(&pg, &owner, command, is_append);
        let expected_descriptor = captured.descriptor_bytes();
        let before = introduction::state(&pg);
        let mut submission = captured.into_submission();
        assert_eq!(submission.observation(), Observed::NoAcceptanceObserved);
        install(&mut submission, &pg).unwrap();
        assert_eq!(submission.observation(), Observed::Installed);
        let pending = pending_install::observe(&pg);
        let changes = pg.conn.total_changes();
        install(&mut submission, &pg).unwrap();
        assert_eq!(
            pg.conn.total_changes(),
            changes,
            "exact retry rewrote indexes"
        );
        assert_eq!(pending_install::observe(&pg), pending);
        assert!(pg
            .pending_staging_protection(&topology())
            .unwrap()
            .unwrap()
            .window
            .is_some());
        assert_eq!(introduction::state(&pg), before, "install applied an owner");
        drop(pg);
        let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
        assert_eq!(pending_install::observe(&pg), pending);
        install(&mut submission, &pg).unwrap();
        assert_eq!(submission.observation(), Observed::Installed);
        assert_eq!(submission.description().command(), &expected_command);
        assert_eq!(
            submission.description().descriptor_bytes(),
            expected_descriptor
        );
        // A later rejection does not cancel the earlier committed observation.
        expired_retry(&mut submission, &pg);
        assert_eq!(submission.observation(), Observed::Installed);
        let changes = pg.conn.total_changes();
        drop(submission);
        assert_eq!(pg.conn.total_changes(), changes, "hidden Drop mutation");
        assert_eq!(pending_install::observe(&pg), pending);
        assert_eq!(introduction::state(&pg), before);
    }
}

#[test]
fn staging_submission_rejected_index_insert_keeps_description_and_rolls_back_every_index() {
    let _clock = test_time_override_guard(2_000);
    for (target, is_append) in candidate::cases() {
        for table in [
            "metadata_command_pending_route_dependencies",
            "metadata_command_pending_staging_protection",
        ] {
            let (_dir, pg, owner, command) = candidate::setup(target.clone(), is_append);
            let expected = command.clone();
            let captured = candidate::capture(&pg, &owner, command, is_append);
            let bytes = captured.descriptor_bytes();
            let before = introduction::state(&pg);
            let mut submission = captured.into_submission();
            pg.conn.execute_batch(&format!("CREATE TEMP TRIGGER fail_index BEFORE INSERT ON {table} BEGIN SELECT RAISE(ABORT, 'submission index failure'); END;")).unwrap();
            let error = install(&mut submission, &pg).unwrap_err();
            assert!(matches!(&error, SubmissionError::Rejected(_)), "{error:?}");
            assert!(error.to_string().contains("submission index failure"));
            assert_eq!(submission.observation(), Observed::NoAcceptanceObserved);
            assert_eq!(submission.description().command(), &expected);
            assert_eq!(submission.description().descriptor_bytes(), bytes);
            pending_install::assert_empty(&pg);
            assert!(pg.conn.is_autocommit());
            assert_eq!(introduction::state(&pg), before);
            pg.conn.execute_batch("DROP TRIGGER fail_index").unwrap();
            install(&mut submission, &pg).unwrap();
            assert_eq!(submission.observation(), Observed::Installed);
        }
    }
}

#[test]
fn staging_submission_uncertain_commit_survives_rejected_retries_until_exact_install() {
    let _clock = test_time_override_guard(2_000);
    for (target, is_append) in candidate::cases() {
        let (_dir, pg, owner, command) = candidate::setup(target, is_append);
        let expected = command.clone();
        let captured = candidate::capture(&pg, &owner, command, is_append);
        let bytes = captured.descriptor_bytes();
        let mut submission = captured.into_submission();
        let before = introduction::state(&pg);
        pg.conn.execute_batch("CREATE TEMP TABLE fail_commit (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES fail_commit(id) DEFERRABLE INITIALLY DEFERRED);
            CREATE TEMP TRIGGER fail_commit_after_index AFTER INSERT ON metadata_command_pending_staging_protection BEGIN INSERT INTO fail_commit VALUES (1, 2); END;").unwrap();
        let error = install(&mut submission, &pg).unwrap_err();
        assert!(
            matches!(&error, SubmissionError::Unconfirmed(_)),
            "{error:?}"
        );
        assert!(error
            .to_string()
            .contains("commit pending metadata command slot transaction"));
        assert_eq!(submission.observation(), Observed::Unconfirmed);
        pending_install::assert_empty(&pg);
        assert!(pg.conn.is_autocommit());
        assert_eq!(introduction::state(&pg), before);
        // The diagnostic knows the local rollback succeeded; the handoff cannot
        // infer a global absence certificate from that or a later expired call.
        expired_retry(&mut submission, &pg);
        assert_eq!(submission.observation(), Observed::Unconfirmed);
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        assert!(matches!(
            install(&mut submission, &pg),
            Err(SubmissionError::OpenTransaction)
        ));
        assert_eq!(submission.observation(), Observed::Unconfirmed);
        pg.conn
            .execute_batch("ROLLBACK; DROP TRIGGER fail_commit_after_index")
            .unwrap();
        assert_eq!(submission.description().command(), &expected);
        assert_eq!(submission.description().descriptor_bytes(), bytes);
        install(&mut submission, &pg).unwrap();
        assert_eq!(submission.observation(), Observed::Installed);
        assert_eq!(introduction::state(&pg), before);
    }
}

#[test]
fn staging_submission_reobserves_lost_reply_and_rejects_damaged_exact_pending_indexes() {
    let _clock = test_time_override_guard(2_000);
    for (target, is_append) in candidate::cases() {
        for damaged in [
            None,
            Some("metadata_command_pending_route_dependencies"),
            Some("metadata_command_pending_staging_protection"),
        ] {
            let (_dir, pg, owner, command) = candidate::setup(target.clone(), is_append);
            let captured = candidate::capture(&pg, &owner, command, is_append);
            let bytes = captured.descriptor_bytes();
            let before = introduction::state(&pg);
            // Install through the real transaction without reporting its success
            // to a holder: the local analogue of a lost installation reply.
            // Use setup inputs before transferring the complete description;
            // a holder exposes only an inspection view, not bindable authority.
            let topology = topology();
            let bound = captured.bind(&topology).unwrap();
            pg.try_insert_bound_staging_pending_metadata_command_slot(
                0,
                &bound.pending_protection(),
                &topology,
                Some(captured.command().bucket_name()),
                AdmittedRouteEffectFence::unbounded(captured.command().id().cluster_epoch()),
                None,
            )
            .unwrap();
            let mut submission = captured.into_submission();
            assert_eq!(submission.observation(), Observed::NoAcceptanceObserved);
            if let Some(table) = damaged {
                pg.conn
                    .execute_batch(&format!("DELETE FROM {table}"))
                    .unwrap();
            }
            let pending = pending_install::observe(&pg);
            let changes = pg.conn.total_changes();
            let result = install(&mut submission, &pg);
            if damaged.is_some() {
                assert!(
                    matches!(result, Err(SubmissionError::Unconfirmed(_))),
                    "{result:?}"
                );
                assert_eq!(submission.observation(), Observed::Unconfirmed);
                expired_retry(&mut submission, &pg);
                assert_eq!(submission.observation(), Observed::Unconfirmed);
            } else {
                result.unwrap();
                assert_eq!(submission.observation(), Observed::Installed);
            }
            assert_eq!(submission.description().descriptor_bytes(), bytes);
            assert_eq!(pg.conn.total_changes(), changes, "retry repaired evidence");
            drop(submission);
            assert_eq!(
                pg.conn.total_changes(),
                changes,
                "Drop discarded pending evidence"
            );
            assert_eq!(pending_install::observe(&pg), pending);
            assert_eq!(introduction::state(&pg), before);
        }
    }
}

#[test]
fn staging_submission_rejects_outer_transaction_topology_and_effect_expiry_without_writes() {
    let clock = test_time_override_guard(2_000);
    for (target, is_append) in candidate::cases() {
        clock.set(2_000);
        let (_dir, pg, owner, command) = candidate::setup(target, is_append);
        let epoch = command.id().cluster_epoch();
        let captured = candidate::capture(&pg, &owner, command, is_append);
        let bytes = captured.descriptor_bytes();
        let mut submission = captured.into_submission();
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        assert!(matches!(
            install(&mut submission, &pg),
            Err(SubmissionError::OpenTransaction)
        ));
        assert!(!pg.conn.is_autocommit(), "committed caller transaction");
        pg.conn.execute_batch("ROLLBACK").unwrap();
        let foreign = PgTopology::new(&[0, 1, 2]).unwrap();
        assert_ne!(foreign, topology());
        assert!(matches!(
            submission.install(
                &pg,
                0,
                &foreign,
                AdmittedRouteEffectFence::unbounded(epoch),
                None
            ),
            Err(SubmissionError::Binding(CandidateBindingError::Topology))
        ));
        let control = clock.control();
        let result = PgStore::test_with_before_pending_slot_effect_hook(
            move |in_transaction| {
                assert!(in_transaction);
                control.set(4_500);
            },
            || {
                submission.install(
                    &pg,
                    0,
                    &topology(),
                    AdmittedRouteEffectFence::bounded(epoch, 5_000, 4_000),
                    None,
                )
            },
        );
        assert!(
            matches!(result, Err(SubmissionError::Rejected(_))),
            "{result:?}"
        );
        assert_eq!(submission.observation(), Observed::NoAcceptanceObserved);
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);
        pending_install::assert_empty(&pg);
        assert_eq!(submission.description().descriptor_bytes(), bytes);
        clock.set(2_000);
        install(&mut submission, &pg).unwrap();
        assert_eq!(submission.observation(), Observed::Installed);
    }
}

#[test]
fn staging_submission_unwind_keeps_description_and_uncertainty_without_cleanup() {
    let _clock = test_time_override_guard(2_000);
    for (target, is_append) in candidate::cases() {
        let (_dir, pg, owner, command) = candidate::setup(target, is_append);
        let expected = command.clone();
        let captured = candidate::capture(&pg, &owner, command, is_append);
        let bytes = captured.descriptor_bytes();
        let mut submission = captured.into_submission();
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            PgStore::test_with_before_pending_slot_effect_hook(
                |in_transaction| {
                    assert!(in_transaction);
                    panic!("submission interrupted before effect");
                },
                || install(&mut submission, &pg),
            )
        }));
        assert!(panic.is_err());
        assert_eq!(submission.observation(), Observed::Unconfirmed);
        assert_eq!(submission.description().command(), &expected);
        assert_eq!(submission.description().descriptor_bytes(), bytes);
        // The SQL owner, not the descriptive holder, handles its connection
        // after an unwind. Rolling it back must not clear installation history.
        assert!(!pg.conn.is_autocommit());
        pg.conn.execute_batch("ROLLBACK").unwrap();
        expired_retry(&mut submission, &pg);
        assert_eq!(submission.observation(), Observed::Unconfirmed);
        drop(submission);
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);
        pending_install::assert_empty(&pg);
    }
}

#[test]
fn staging_submission_rollback_failure_is_uncertain_with_connection_recovery_or_retirement() {
    let _clock = test_time_override_guard(2_000);
    for (target, is_append) in candidate::cases() {
        for commit_failure in [false, true] {
            for rollback_failures in [1, 2] {
                let (dir, pg, owner, command) = candidate::setup(target.clone(), is_append);
                let expected = command.clone();
                let captured = candidate::capture(&pg, &owner, command, is_append);
                let descriptor = captured.descriptor_bytes();
                let mut submission = captured.into_submission();
                let before = introduction::state(&pg);
                let writes = pg.conn.total_changes();
                if commit_failure {
                    pg.conn.execute_batch("CREATE TEMP TABLE fail_commit (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES fail_commit(id) DEFERRABLE INITIALLY DEFERRED);
                        CREATE TEMP TRIGGER fail_pending AFTER INSERT ON metadata_command_pending_staging_protection BEGIN INSERT INTO fail_commit VALUES (1, 2); END;").unwrap();
                } else {
                    // Slot and route index writes precede this failure. Rejecting
                    // before INSERT would not exercise rollback's custody gap.
                    pg.conn.execute_batch("CREATE TEMP TRIGGER fail_pending BEFORE INSERT ON metadata_command_pending_staging_protection BEGIN SELECT RAISE(ABORT, 'submission sidecar failure'); END;").unwrap();
                }
                pg.test_fail_pending_slot_rollbacks(rollback_failures);
                let error = install(&mut submission, &pg).unwrap_err();
                assert!(
                    matches!(&error, SubmissionError::Unconfirmed(_)),
                    "{error:?}"
                );
                let diagnostic = error.to_string();
                assert!(
                    diagnostic.contains("injected pending-slot rollback failure"),
                    "{diagnostic}"
                );
                assert!(
                    diagnostic.contains(if commit_failure {
                        "commit pending metadata command slot transaction"
                    } else {
                        "submission sidecar failure"
                    }),
                    "{diagnostic}"
                );
                assert!(
                    pg.conn.total_changes() > writes,
                    "failure preceded slot/index writes"
                );
                assert_eq!(submission.observation(), Observed::Unconfirmed);
                assert_eq!(submission.description().command(), &expected);
                assert_eq!(submission.description().descriptor_bytes(), descriptor);
                if rollback_failures == 1 {
                    assert!(pg.conn.is_autocommit());
                    pg.require_usable_connection().unwrap();
                    pending_install::assert_empty(&pg);
                    assert_eq!(introduction::state(&pg), before);
                    expired_retry(&mut submission, &pg);
                    assert_eq!(submission.observation(), Observed::Unconfirmed);
                    pg.conn.execute_batch("DROP TRIGGER fail_pending").unwrap();
                    install(&mut submission, &pg).unwrap();
                    assert_eq!(submission.observation(), Observed::Installed);
                } else {
                    assert!(!pg.conn.is_autocommit());
                    assert!(pg.require_usable_connection().is_err());
                    assert!(pg.require_live_allocation_lifetime().is_err());
                    assert!(pg.begin_allocation_lifetime_replacement().is_err());
                    assert!(
                        diagnostic.contains("connection retired: true"),
                        "{diagnostic}"
                    );
                    // Owner-local inspection proves the poisoned transaction is
                    // still present. It must not be exposed by the node facade.
                    let slot_count: u32 = pg
                        .conn
                        .query_row(
                            "SELECT count(*) FROM metadata_command_pending_slot",
                            [],
                            |row| row.get(0),
                        )
                        .unwrap();
                    assert_eq!(slot_count, 1);
                    let changes = pg.conn.total_changes();
                    assert!(matches!(
                        install(&mut submission, &pg),
                        Err(SubmissionError::Unconfirmed(_))
                    ));
                    assert_eq!(pg.conn.total_changes(), changes);
                    assert_eq!(submission.observation(), Observed::Unconfirmed);
                    drop(pg);
                    let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
                    pg.require_usable_connection().unwrap();
                    pending_install::assert_empty(&pg);
                    assert_eq!(introduction::state(&pg), before);
                    install(&mut submission, &pg).unwrap();
                    assert_eq!(submission.observation(), Observed::Installed);
                }
            }
        }
    }
}
