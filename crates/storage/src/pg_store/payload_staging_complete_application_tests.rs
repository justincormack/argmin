// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Complete carrier -> owner mutations -> applied identity, in one transaction.
//! Metadata-only fixtures; no physical writes or production dispatch activation.

use super::super::command_context::ContextualCommand;
use super::*;
use crate::clock::test_time_override_guard;
use crate::metadata_command::{
    MetadataCommandEnvelope, MetadataCommandPayload, MetadataCommandReplicaState,
    ReserveObjectVersionCommand,
};
use crate::node_runtime::pg_store::{
    command_log::CompleteApplicationError, MetadataCommandStartupDisposition, PgStore,
};
use crate::{StoreError, VersionId};

fn apply(
    pg: &PgStore,
    context: &ContextualCommand,
) -> Result<crate::metadata_command::MetadataCommandReplicaState, CompleteApplicationError> {
    pg.apply_complete_metadata_command_and_record(
        0,
        &context.encode_command().unwrap(),
        &topology(),
    )
}

pub(super) fn anchor(pg: &PgStore, id: MetadataCommandId) {
    // Existing component fixtures describe materialized metadata, not an
    // installed command history. Anchor it, then populate missing prefix links
    // using real tombstone recording (no forged hash/receipt SQL).
    pg.refresh_metadata_command_state_digest().unwrap();
    if !pg.conn.is_autocommit() {
        pg.conn.execute_batch("COMMIT").unwrap();
    }
    let prefix = pg.metadata_command_replica_state().unwrap();
    let start = if prefix.cluster_epoch == id.cluster_epoch() {
        prefix.applied_log_index + 1
    } else {
        1
    };
    for index in start..id.log_index().get() {
        let owner = fixture(StagingTarget::DirectPut);
        let unrelated = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                id.cluster_epoch(),
                id.pg_id(),
                MetadataCommandLogIndex::new(index).unwrap(),
            ),
            MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
                owner.subject.bucket,
                owner.subject.key,
                VersionId::from_u64(100 + index),
            )),
        );
        pg.record_metadata_command_abandoned(0, &unrelated).unwrap();
    }
}

pub(super) fn setup(case: usize) -> (test_util::TempDir, PgStore, ContextualCommand) {
    let (dir, pg, context) = if (3..5).contains(&case) {
        let target = candidate::cases()[case].0.clone();
        let (dir, pg, template, command) = candidate::setup(target, false);
        let introduction =
            ContextualCommand::from_described(candidate::capture(&pg, &template, command, false));
        anchor(&pg, introduction.id());
        apply(&pg, &introduction).unwrap();
        let snapshot = pg.command_build_snapshot().unwrap();
        let owner = append::load(&pg, &template);
        snapshot.finish().unwrap();
        fresh_append_capture::prepare(&pg, &owner);
        let command = append::command(&owner);
        let context =
            ContextualCommand::from_described(candidate::capture(&pg, &owner, command, true));
        (dir, pg, context)
    } else if case < 7 {
        complete_pending::setup(case)
    } else {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(StagingTarget::DirectPut).subject.object_pg.get(),
        )
        .unwrap();
        let command = application::setup_terminal(&pg, application::TERMINALS[case - 7]);
        let context = application::ordinary_context(&command, &topology()).unwrap();
        (dir, pg, context)
    };
    anchor(&pg, context.id());
    (dir, pg, context)
}

fn applied(pg: &PgStore, context: &ContextualCommand) {
    let snapshot = pg.command_build_snapshot().unwrap();
    assert!(matches!(
        context
            .encode_command()
            .unwrap()
            .inspect_terminal_in_open_transaction(pg, 0)
            .unwrap(),
        MetadataCommandStartupDisposition::Applied { .. }
    ));
    snapshot.finish().unwrap();
}

#[test]
fn staging_complete_application_records_exact_identity_and_replays_without_owner_mutations() {
    let _clock = test_time_override_guard(2_000);
    for case in 0..18 {
        let (dir, pg, context) = setup(case);
        let encoded = context.encode_command().unwrap();
        let before = introduction::state(&pg);
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        context
            .application(&topology())
            .unwrap()
            .apply_in_open_transaction(&pg, 0)
            .unwrap();
        let expected = introduction::state(&pg);
        pg.conn.execute_batch("ROLLBACK").unwrap();
        assert_ne!(
            expected, before,
            "case {case} must change materialized state"
        );
        let decoded = ContextualCommand::decode_command(
            encoded.bytes(),
            encoded.checksum_crc64(),
            &topology(),
        )
        .unwrap();
        let state = apply(&pg, &decoded).unwrap();
        assert_eq!(state.applied_log_index, context.id().log_index().get());
        assert_eq!(introduction::state(&pg), expected, "case {case}");
        let stored: (Vec<u8>, i64, i64) = pg.conn.query_row(
            "SELECT command_bytes, command_checksum, abandoned FROM metadata_command_log WHERE cluster_epoch = ?1 AND log_index = ?2",
            rusqlite::params![context.id().cluster_epoch().get() as i64, context.id().log_index().get() as i64],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        ).unwrap();
        assert_eq!(
            stored,
            (encoded.bytes().to_vec(), encoded.checksum_crc64() as i64, 0)
        );
        assert!(
            crate::metadata_command::decode_metadata_command_log_entry_header(&stored.0).is_err(),
            "live reader must still reject v14"
        );
        applied(&pg, &context);
        let writes = pg.conn.total_changes();
        assert_eq!(apply(&pg, &context).unwrap(), state);
        assert_eq!(
            pg.conn.total_changes(),
            writes,
            "exact replay rewrote case {case}"
        );
        assert_eq!(introduction::state(&pg), expected);
        let pg_id = pg.pg_id;
        drop(pg);
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        applied(&pg, &context);
        let writes = pg.conn.total_changes();
        assert_eq!(apply(&pg, &context).unwrap(), state);
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), expected);
        // Exercise the real maintenance implementation with the explicit
        // complete grammar, not fabricated receipts. Live dispatch is still
        // current-only and must not silently accept this applied prefix.
        assert!(pg
            .metadata_command_checkpoint(0, context.id().cluster_epoch())
            .is_err());
        pg.test_checkpoint_and_compact_complete_command(0, &context.encode_command().unwrap(), 2)
            .unwrap();
        assert_eq!(
            pg.metadata_command_log_stats(context.id().cluster_epoch())
                .unwrap()
                .retained_entries,
            0
        );
        drop(pg);
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        applied(&pg, &context);
        let writes = pg.conn.total_changes();
        assert_eq!(apply(&pg, &context).unwrap(), state);
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), expected);
    }
}

fn failure() -> StoreError {
    StoreError::RouteCapabilitySubjectMismatch {
        operation: "injected complete apply commit guard",
    }
}

#[test]
fn staging_complete_application_callback_is_test_only_and_never_runs_on_replay() {
    let _clock = test_time_override_guard(2_000);
    // Pin the callback-free production signature, including in test builds.
    let production_apply: fn(
        &PgStore,
        u32,
        &EncodedContextualCommand<'_>,
        &PgTopology,
    ) -> Result<MetadataCommandReplicaState, CompleteApplicationError> =
        PgStore::apply_complete_metadata_command_and_record;
    for case in 0..18 {
        let (_dir, pg, context) = setup(case);
        let encoded = context.encode_command().unwrap();
        let before = introduction::state(&pg);
        let calls = std::cell::Cell::new(0);
        let state = pg
            .test_apply_complete_metadata_command_with_commit_guard(
                0,
                &encoded,
                &topology(),
                || {
                    assert_ne!(introduction::state(&pg), before);
                    assert_eq!(
                        pg.metadata_command_replica_state()?.applied_log_index,
                        context.id().log_index().get()
                    );
                    assert!(!pg.conn.is_autocommit());
                    calls.set(calls.get() + 1);
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(calls.get(), 1, "fresh-apply hook must run for case {case}");
        let expected = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        assert_eq!(
            pg.test_apply_complete_metadata_command_with_commit_guard(
                0,
                &encoded,
                &topology(),
                || panic!("exact replay invoked the fresh-apply hook for case {case}"),
            )
            .unwrap(),
            state
        );
        assert_eq!(
            production_apply(&pg, 0, &encoded, &topology()).unwrap(),
            state
        );
        assert_eq!(pg.conn.total_changes(), writes, "case {case}");
        assert_eq!(introduction::state(&pg), expected, "case {case}");
    }
}

#[test]
fn staging_complete_application_owns_commit_through_every_context() {
    let _clock = test_time_override_guard(2_000);
    for case in 0..18 {
        let (_dir, pg, context) = setup(case);
        let mut stages = Vec::new();
        let state = pg
            .test_apply_complete_command_with_hook(&context.encode_command().unwrap(), |stage| {
                stages.push(stage);
                assert_eq!(pg.conn.is_autocommit(), stage == "after-commit");
                if stage != "after-commit" {
                    let error = pg
                        .commit_unmanaged_test_transaction("crossed legacy commit")
                        .unwrap_err();
                    assert!(
                        error
                            .to_string()
                            .contains("owning metadata transaction scope"),
                        "case {case}, {stage}: {error}"
                    );
                    assert!(!pg.conn.is_autocommit());
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(
            stages,
            [
                "before-effect",
                "after-effect",
                "before-commit",
                "after-commit"
            ]
        );
        assert_eq!(state.applied_log_index, context.id().log_index().get());
        applied(&pg, &context);
        let writes = pg.conn.total_changes();
        assert_eq!(apply(&pg, &context).unwrap(), state);
        assert_eq!(
            pg.conn.total_changes(),
            writes,
            "replay changed case {case}"
        );
    }
}

#[test]
fn staging_complete_application_rolls_back_owner_and_log_on_failure_and_unwind() {
    let _clock = test_time_override_guard(2_000);
    for case in 0..18 {
        let (_dir, pg, context) = setup(case);
        let encoded = context.encode_command().unwrap();
        let before = introduction::state(&pg);
        let prefix = pg.metadata_command_replica_state().unwrap();
        let max = pg
            .max_metadata_command_log_index(context.id().cluster_epoch())
            .unwrap();
        let error = pg
            .test_apply_complete_metadata_command_with_commit_guard(
                0,
                &encoded,
                &topology(),
                || {
                    assert_ne!(introduction::state(&pg), before);
                    assert_eq!(
                        pg.metadata_command_replica_state()
                            .unwrap()
                            .applied_log_index,
                        context.id().log_index().get()
                    );
                    Err(failure())
                },
            )
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("injected complete apply commit guard"),
            "{error:?}"
        );
        assert!(pg.conn.is_autocommit());
        assert_eq!(introduction::state(&pg), before);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        assert_eq!(
            pg.max_metadata_command_log_index(context.id().cluster_epoch())
                .unwrap(),
            max
        );
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            pg.test_apply_complete_metadata_command_with_commit_guard(
                0,
                &encoded,
                &topology(),
                || panic!("injected complete apply unwind"),
            )
        }))
        .is_err());
        assert!(pg.conn.is_autocommit());
        assert_eq!(introduction::state(&pg), before);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        assert_eq!(
            pg.max_metadata_command_log_index(context.id().cluster_epoch())
                .unwrap(),
            max
        );
        apply(&pg, &context).unwrap();
        applied(&pg, &context);
    }
}

#[test]
fn staging_complete_application_log_insert_failure_rolls_back_materialized_owner() {
    let _clock = test_time_override_guard(2_000);
    for case in 0..18 {
        let (_dir, pg, context) = setup(case);
        let before = introduction::state(&pg);
        let prefix = pg.metadata_command_replica_state().unwrap();
        pg.conn.execute_batch("CREATE TEMP TRIGGER reject_complete_log BEFORE INSERT ON main.metadata_command_log BEGIN SELECT RAISE(ABORT, 'injected complete log failure'); END;").unwrap();
        let error = apply(&pg, &context).unwrap_err();
        assert!(
            error.to_string().contains("injected complete log failure"),
            "{error:?}"
        );
        assert!(pg.conn.is_autocommit());
        assert_eq!(introduction::state(&pg), before);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        pg.conn
            .execute_batch("DROP TRIGGER temp.reject_complete_log")
            .unwrap();
        apply(&pg, &context).unwrap();
    }
}

#[test]
fn staging_complete_application_rejects_outer_transaction_topology_and_wrong_pg() {
    let _clock = test_time_override_guard(2_000);
    for case in 0..18 {
        let (_dir, pg, context) = setup(case);
        let encoded = context.encode_command().unwrap();
        let writes = pg.conn.total_changes();
        let before = introduction::state(&pg);
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let error = apply(&pg, &context).unwrap_err();
        assert!(
            error.to_string().contains("inside caller transaction"),
            "{error:?}"
        );
        assert!(
            !pg.conn.is_autocommit(),
            "must not finish caller's transaction"
        );
        pg.conn.execute_batch("ROLLBACK").unwrap();
        let foreign = PgTopology::new(&(0..16).collect::<Vec<_>>()).unwrap();
        assert_ne!(foreign, topology());
        assert!(pg
            .apply_complete_metadata_command_and_record(0, &encoded, &foreign)
            .is_err());
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), before);
        let dir = test_util::tempdir();
        let crossed = PgStore::open(dir.path(), pg.pg_id ^ 1).unwrap();
        let writes = crossed.conn.total_changes();
        let error = apply(&crossed, &context).unwrap_err();
        assert!(error.to_string().contains("targets PG"), "{error:?}");
        assert_eq!(crossed.conn.total_changes(), writes);
    }
}

#[test]
fn staging_complete_application_failed_commit_cannot_leave_owner_without_log() {
    let _clock = test_time_override_guard(2_000);
    for case in 0..18 {
        let (_dir, pg, context) = setup(case);
        let before = introduction::state(&pg);
        let prefix = pg.metadata_command_replica_state().unwrap();
        pg.conn.execute_batch("CREATE TEMP TABLE deferred_failure (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES deferred_failure(id) DEFERRABLE INITIALLY DEFERRED); CREATE TEMP TRIGGER fail_complete_commit AFTER INSERT ON main.metadata_command_log BEGIN INSERT INTO deferred_failure VALUES (1, 2); END;").unwrap();
        let error = apply(&pg, &context).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("apply complete command and record (commit txn)"),
            "{error:?}"
        );
        assert!(pg.conn.is_autocommit());
        assert_eq!(introduction::state(&pg), before);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        pg.conn
            .execute_batch("DROP TRIGGER temp.fail_complete_commit")
            .unwrap();
        apply(&pg, &context).unwrap();
        applied(&pg, &context);
    }
}

#[test]
fn staging_complete_application_rejects_changed_context_at_the_same_applied_position() {
    use super::super::candidate::DescribedStagingCommand;
    use super::super::descriptor::IntroductionDescriptor;
    let _clock = test_time_override_guard(2_000);
    for target in [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ] {
        let (_dir, pg, template, command) = candidate::setup(target, false);
        let context = ContextualCommand::from_described(candidate::capture(
            &pg,
            &template,
            command.clone(),
            false,
        ));
        anchor(&pg, context.id());
        apply(&pg, &context).unwrap();
        let snapshot = pg.command_build_snapshot().unwrap();
        let mut changed = append::load(&pg, &template);
        snapshot.finish().unwrap();
        changed.windows[0].cutoff_ms -= 1;
        changed.maximum_cutoff_ms = changed.windows[0].cutoff_ms;
        let descriptor = IntroductionDescriptor::capture(&changed.validate(&topology()).unwrap())
            .unwrap()
            .encode()
            .unwrap();
        let crossed = ContextualCommand::from_described(
            DescribedStagingCommand::decode_descriptor_field(command, &descriptor, &topology())
                .unwrap(),
        );
        assert_ne!(
            context.encode_field().unwrap(),
            crossed.encode_field().unwrap()
        );
        let before = introduction::state(&pg);
        let prefix = pg.metadata_command_replica_state().unwrap();
        let writes = pg.conn.total_changes();
        let error = apply(&pg, &crossed).unwrap_err();
        assert!(error.to_string().contains("conflict"), "{error:?}");
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), before);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        apply(&pg, &context).unwrap();
    }
}

#[test]
fn staging_complete_application_rejects_abandoned_and_corrupted_records_before_mutation() {
    let _clock = test_time_override_guard(2_000);
    for case in 0..18 {
        for abandoned in [false, true] {
            let (_dir, pg, context) = setup(case);
            if abandoned {
                pg.test_record_complete_command_abandoned(0, &context.encode_command().unwrap())
                    .unwrap();
            } else {
                apply(&pg, &context).unwrap();
                pg.conn.execute("UPDATE metadata_command_log SET command_checksum = command_checksum + 1 WHERE log_index = ?1", [context.id().log_index().get() as i64]).unwrap();
            }
            let before = introduction::state(&pg);
            let prefix = pg.metadata_command_replica_state().unwrap();
            let writes = pg.conn.total_changes();
            let error = apply(&pg, &context).unwrap_err();
            assert!(
                error.to_string().contains(if abandoned {
                    "conflict"
                } else {
                    "checksum mismatch"
                }),
                "{error:?}"
            );
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(introduction::state(&pg), before);
            assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        }
    }
}

#[test]
fn staging_complete_application_uses_one_transaction_for_owner_and_applied_record() {
    use rusqlite::trace::{TraceEvent, TraceEventCodes};
    use std::sync::{Arc, Mutex};
    let _clock = test_time_override_guard(2_000);
    for case in 0..18 {
        let (_dir, mut pg, context) = setup(case);
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
        apply(&pg, &context).unwrap();
        let statements = statements.lock().unwrap();
        assert_eq!(
            statements
                .iter()
                // Existing mutation helpers use nested savepoints. Only the
                // outer transaction publishes state and adds a commit/fsync.
                .filter(|s| s.starts_with("BEGIN"))
                .count(),
            1
        );
        assert_eq!(
            statements.iter().filter(|s| s.as_str() == "COMMIT").count(),
            1
        );
        let begin = statements
            .iter()
            .position(|s| s == "BEGIN IMMEDIATE")
            .unwrap();
        let record = statements
            .iter()
            .position(|s| s.starts_with("INSERT INTO metadata_command_log "))
            .unwrap();
        let commit = statements.iter().position(|s| s == "COMMIT").unwrap();
        assert!(begin < record && record < commit);
        if case != 5 {
            let mutation = statements
                .iter()
                .position(|s| {
                    (s.starts_with("INSERT INTO payload_staging_")
                        || s.starts_with("UPDATE payload_staging_"))
                        && !s.starts_with("--")
                })
                .unwrap();
            assert!(begin < mutation && mutation < record, "case {case}");
        }
    }
}

#[test]
fn staging_complete_application_replay_requires_the_applied_prefix_and_authenticated_chain() {
    let _clock = test_time_override_guard(2_000);
    for corruption in [
        "UPDATE metadata_command_replica_state SET applied_log_index = applied_log_index - 1",
        "UPDATE metadata_command_log SET previous_log_hash = NULL",
        "UPDATE metadata_command_log SET log_hash = log_hash + 1",
    ] {
        let (_dir, pg, context) = setup(0);
        apply(&pg, &context).unwrap();
        pg.conn.execute_batch(corruption).unwrap();
        let before = introduction::state(&pg);
        let prefix = pg.metadata_command_replica_state().unwrap();
        let writes = pg.conn.total_changes();
        assert!(apply(&pg, &context).is_err(), "accepted {corruption}");
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), before);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
    }
}

#[test]
fn staging_complete_application_failed_rollback_retires_the_connection_until_reopen() {
    let _clock = test_time_override_guard(2_000);
    for unwind in [false, true] {
        let (dir, pg, context) = setup(0);
        let before = introduction::state(&pg);
        let prefix = pg.metadata_command_replica_state().unwrap();
        pg.test_fail_metadata_command_rollbacks(1);
        let encoded = context.encode_command().unwrap();
        if unwind {
            assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                pg.test_apply_complete_metadata_command_with_commit_guard(
                    0,
                    &encoded,
                    &topology(),
                    || panic!("injected unwind before denied rollback"),
                )
            }))
            .is_err());
        } else {
            let error = pg
                .test_apply_complete_metadata_command_with_commit_guard(
                    0,
                    &encoded,
                    &topology(),
                    || Err(failure()),
                )
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("injected complete apply commit guard"),
                "{error:?}"
            );
            assert!(error.to_string().contains("rollback failed"), "{error:?}");
        }
        assert!(!pg.conn.is_autocommit());
        let writes = pg.conn.total_changes();
        let error = apply(&pg, &context).unwrap_err();
        assert!(
            error.to_string().contains("retired PG metadata connection"),
            "{error:?}"
        );
        assert_eq!(pg.conn.total_changes(), writes);
        pg.conn.execute_batch("ROLLBACK").unwrap();
        assert!(apply(&pg, &context)
            .unwrap_err()
            .to_string()
            .contains("retired PG metadata connection"));
        let pg_id = pg.pg_id;
        drop(pg);
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        assert_eq!(introduction::state(&pg), before);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        apply(&pg, &context).unwrap();
    }
}
