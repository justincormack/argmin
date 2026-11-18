// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Closed application advances existing local validation, never cold state or
//! a physical-authority certificate. Metadata-only fixtures, no payload IO.

use super::*;

fn prepared(case: usize) -> (test_util::TempDir, PgStore, ContextualCommand) {
    let (dir, pg, context) = complete_application::setup(case);
    let id = context.id();
    // Advance through an unrelated real tombstone so every fixture has a valid
    // same-epoch predecessor, including introductions initially at index one.
    stage_ahead_evidence(&pg, id, AheadEvidence::AbandonedLog);
    let next = MetadataCommandId::new(
        id.cluster_epoch(),
        id.pg_id(),
        MetadataCommandLogIndex::new(id.log_index().get() + 1).unwrap(),
    );
    (dir, pg, context.into_reissue(next).unwrap())
}

fn apply(pg: &PgStore, context: &ContextualCommand) -> MetadataCommandReplicaState {
    pg.apply_complete_metadata_command_and_record(
        0,
        &context.encode_command().unwrap(),
        &topology(),
    )
    .unwrap()
}

#[test]
fn staging_prefix_apply_retired_allocator_converges_with_warm_or_cold_cache() {
    let _clock = test_time_override_guard(2_000);
    for case in 0..18 {
        let mut work = Vec::new();
        for warm in [true, false] {
            let (_dir, pg, context) = prepared(case);
            if warm {
                let prefix = recover(&pg);
                assert_eq!(observe(&pg, prefix.cluster_epoch).unwrap(), prefix);
            }
            // No intervening metadata write or failed lookup may cool the
            // cache before the exact regression reaches fresh application.
            let writes = pg.conn.total_changes();
            drop(pg.begin_allocation_lifetime_replacement().unwrap());
            assert_eq!(pg.conn.total_changes(), writes);
            assert!(matches!(
                pg.require_live_allocation_lifetime(),
                Err(StoreError::PgAllocationLifetimeUnavailable { .. })
            ));
            let result = pg.apply_complete_metadata_command_and_record(
                0,
                &context.encode_command().unwrap(),
                &topology(),
            );
            let state =
                result.unwrap_or_else(|error| panic!("case {case}, warm={warm}: {error:?}"));
            work.push(pg.conn.total_changes() - writes);
            assert_eq!(state.cluster_epoch, context.id().cluster_epoch());
            assert_eq!(state.applied_log_index, context.id().log_index().get());
            let writes = pg.conn.total_changes();
            assert_eq!(apply(&pg, &context), state);
            assert_eq!(
                pg.conn.total_changes(),
                writes,
                "retired replay rewrote case {case}"
            );
            assert_eq!(recover(&pg), state);
            assert!(pg.require_live_allocation_lifetime().is_err());
            assert!(matches!(
                observe(&pg, state.cluster_epoch),
                Err(CompleteSettledPrefixError::Store(
                    StoreError::PgAllocationLifetimeUnavailable { .. }
                ))
            ));
            // A new lifetime does not inherit old validation. Only explicit
            // full validation can make local observation eligible again.
            pg.begin_allocation_lifetime_replacement()
                .unwrap()
                .complete()
                .unwrap();
            cold(&pg, state.cluster_epoch);
            assert_eq!(recover(&pg), state);
            assert_eq!(observe(&pg, state.cluster_epoch).unwrap(), state);
        }
        assert_eq!(
            work[0], work[1],
            "cache changed durable apply work for case {case}"
        );
    }
}

#[test]
fn staging_prefix_apply_advances_all_contexts_without_extra_writes_or_prefix_validation() {
    let _clock = test_time_override_guard(2_000);
    for case in 0..18 {
        let mut work = Vec::new();
        for warm in [false, true] {
            let (_dir, pg, context) = prepared(case);
            if warm {
                recover(&pg);
            }
            let writes = pg.conn.total_changes();
            let scans = pg.test_metadata_command_log_replay_validation_entries();
            let state = apply(&pg, &context);
            work.push((
                pg.conn.total_changes() - writes,
                pg.test_metadata_command_log_replay_validation_entries() - scans,
            ));
            if warm {
                assert_eq!(
                    observe(&pg, state.cluster_epoch).unwrap(),
                    state,
                    "case {case}"
                );
            } else {
                cold(&pg, state.cluster_epoch);
            }
            let writes = pg.conn.total_changes();
            assert_eq!(apply(&pg, &context), state);
            assert_eq!(pg.conn.total_changes(), writes);
            if warm {
                assert_eq!(observe(&pg, state.cluster_epoch).unwrap(), state);
            } else {
                cold(&pg, state.cluster_epoch);
            }
            assert_eq!(recover(&pg), state);
            assert_eq!(observe(&pg, state.cluster_epoch).unwrap(), state);
        }
        assert_eq!(work[0], work[1], "extra write/replay work for case {case}");
    }
}

fn probe(pg: &PgStore) {
    pg.conn.execute_batch("CREATE TABLE prefix_apply_probe (value INTEGER); INSERT INTO prefix_apply_probe VALUES (0)").unwrap();
}

fn change(pg: &PgStore) {
    pg.conn
        .execute("UPDATE prefix_apply_probe SET value = value + 1", [])
        .unwrap();
}

#[test]
fn staging_prefix_apply_does_not_bless_interleaved_local_mutations() {
    let _clock = test_time_override_guard(2_000);
    for stage in [
        "before-effect",
        "after-effect",
        "before-commit",
        "after-commit",
    ] {
        let (_dir, pg, context) = prepared(0);
        probe(&pg);
        recover(&pg);
        let mut called = false;
        let state = pg
            .test_apply_complete_command_with_hook(&context.encode_command().unwrap(), |at| {
                if at == stage {
                    called = true;
                    change(&pg);
                }
                Ok(())
            })
            .unwrap();
        assert!(called);
        cold(&pg, state.cluster_epoch);
        assert_eq!(recover(&pg), state);
        assert_eq!(observe(&pg, state.cluster_epoch).unwrap(), state);
    }
}

#[test]
fn staging_prefix_apply_rechecks_external_changes_before_and_after_commit() {
    let _clock = test_time_override_guard(2_000);
    for after_commit in [false, true] {
        let (dir, pg, context) = prepared(0);
        probe(&pg);
        let other = PgStore::open(dir.path(), pg.pg_id).unwrap();
        recover(&pg);
        if !after_commit {
            change(&other);
        }
        let state = pg
            .test_apply_complete_command_with_hook(&context.encode_command().unwrap(), |at| {
                if after_commit && at == "after-commit" {
                    change(&other);
                }
                Ok(())
            })
            .unwrap();
        cold(&pg, state.cluster_epoch);
        assert_eq!(recover(&pg), state);
    }
}

fn failure() -> StoreError {
    StoreError::RouteCapabilitySubjectMismatch {
        operation: "injected prefix advancement failure",
    }
}

#[test]
fn staging_prefix_apply_failure_and_unwind_never_publish_transaction_local_validation() {
    let _clock = test_time_override_guard(2_000);
    for stage in [
        "before-effect",
        "after-effect",
        "before-commit",
        "after-commit",
    ] {
        for unwind in [false, true] {
            let (_dir, pg, context) = prepared(0);
            let before = recover(&pg);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                pg.test_apply_complete_command_with_hook(&context.encode_command().unwrap(), |at| {
                    if at == stage {
                        assert!(!unwind, "injected prefix advancement unwind");
                        return Err(failure());
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
                    .contains("injected prefix advancement failure"));
            }
            assert!(pg.conn.is_autocommit());
            cold(&pg, before.cluster_epoch);
            let state = pg.metadata_command_replica_state().unwrap();
            if stage == "after-commit" {
                assert_eq!(state.applied_log_index, context.id().log_index().get());
            } else {
                assert_eq!(state, before);
            }
            // A retry can converge, but cannot turn a cold prefix hot.
            let applied = apply(&pg, &context);
            cold(&pg, applied.cluster_epoch);
            assert_eq!(recover(&pg), applied);
        }
    }
}

#[test]
fn staging_prefix_apply_failed_commit_or_rollback_requires_explicit_recovery() {
    let _clock = test_time_override_guard(2_000);
    for rollback_failure in [false, true] {
        let (dir, pg, context) = prepared(0);
        pg.conn.execute_batch("CREATE TEMP TABLE prefix_deferred_failure (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES prefix_deferred_failure(id) DEFERRABLE INITIALLY DEFERRED); CREATE TEMP TRIGGER prefix_fail_commit AFTER INSERT ON main.metadata_command_log BEGIN INSERT INTO prefix_deferred_failure VALUES (1, 2); END;").unwrap();
        let before = recover(&pg);
        if rollback_failure {
            pg.test_fail_metadata_command_rollbacks(1);
        }
        let error = pg
            .apply_complete_metadata_command_and_record(
                0,
                &context.encode_command().unwrap(),
                &topology(),
            )
            .unwrap_err();
        assert!(error.to_string().contains("commit txn"), "{error:?}");
        if rollback_failure {
            assert!(error.to_string().contains("rollback failed"));
            assert!(pg.require_usable_connection().is_err());
            pg.conn.execute_batch("ROLLBACK").unwrap();
            assert!(pg.require_usable_connection().is_err());
        } else {
            assert!(pg.conn.is_autocommit());
            cold(&pg, before.cluster_epoch);
            assert_eq!(pg.metadata_command_replica_state().unwrap(), before);
        }
        let pg_id = pg.pg_id;
        drop(pg);
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        cold(&pg, before.cluster_epoch);
        assert_eq!(recover(&pg), before);
        let state = apply(&pg, &context);
        assert_eq!(observe(&pg, state.cluster_epoch).unwrap(), state);
    }
}

#[test]
fn staging_prefix_apply_epoch_transition_and_pending_custody_remain_blocked() {
    let _clock = test_time_override_guard(2_000);
    let (_dir, pg, context) = prepared(0);
    recover(&pg);
    let next_epoch = MetadataCommandId::new(
        ClusterEpoch::new(context.id().cluster_epoch().get() + 1).unwrap(),
        context.id().pg_id(),
        MetadataCommandLogIndex::new(1).unwrap(),
    );
    let context = context.into_reissue(next_epoch).unwrap();
    let state = apply(&pg, &context);
    cold(&pg, state.cluster_epoch);
    assert_eq!(recover(&pg), state);

    let (_dir, pg, context) = prepared(0);
    let encoded = context.encode_command().unwrap();
    let decoded =
        ContextualCommand::decode_command(encoded.bytes(), encoded.checksum_crc64(), &topology())
            .unwrap();
    complete_pending::install(&mut decoded.into_submission(), &pg).unwrap();
    recover(&pg);
    let state = apply(&pg, &context);
    assert!(matches!(
        observe(&pg, state.cluster_epoch),
        Err(CompleteSettledPrefixError::Store(
            StoreError::MetadataCommandContention { .. }
        ))
    ));
}

#[test]
fn staging_prefix_apply_uses_only_the_existing_commit_boundary() {
    let _clock = test_time_override_guard(2_000);
    let (_dir, mut pg, context) = prepared(0);
    recover(&pg);
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
    let state = apply(&pg, &context);
    {
        let statements = statements.lock().unwrap();
        assert_eq!(
            statements.iter().filter(|s| s.starts_with("BEGIN")).count(),
            1
        );
        assert_eq!(
            statements.iter().filter(|s| s.as_str() == "COMMIT").count(),
            1
        );
        assert!(
            !statements.iter().any(|s| s.contains("WHERE pg_id <>")),
            "startup scan in hot apply: {statements:?}"
        );
    }
    assert_eq!(observe(&pg, state.cluster_epoch).unwrap(), state);
}
