// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Local cache/snapshot tests. These are not primary/witness confirmation or
//! physical-authority tests. Fixtures contain metadata, not physical payloads.

use super::*;
use crate::clock::test_time_override_guard;
use crate::metadata_command::MetadataCommandReplicaState;
use crate::node_runtime::pg_store::{
    command_log::CompleteSettledPrefixError, PgStore, PgStoreRecoveryContext,
};
use crate::StoreError;
use rusqlite::trace::{TraceEvent, TraceEventCodes};
use std::sync::{Arc, Mutex};

#[path = "payload_staging_prefix_apply_tests.rs"]
mod advancement;

#[path = "payload_staging_prefix_validation_budget_tests.rs"]
mod validation_budget;

fn validate_reinspection(
    pg: &PgStore,
    epoch: ClusterEpoch,
) -> Result<(), CompleteSettledPrefixError> {
    pg.validate_complete_direct_reinspection_prefix(
        NodeId::new(0),
        epoch,
        &topology(),
        std::time::Instant::now() + std::time::Duration::from_secs(60),
        crate::types::AdmittedRouteEffectFence::unbounded(epoch),
    )
}

fn recover(pg: &PgStore) -> MetadataCommandReplicaState {
    pg.recover_complete_command_state(
        PgStoreRecoveryContext::for_node(NodeId::new(0)),
        &topology(),
    )
    .unwrap()
}

fn setup(case: usize) -> (test_util::TempDir, PgStore, ContextualCommand) {
    let (dir, pg, context) = complete_application::setup(case);
    pg.apply_complete_metadata_command_and_record(
        0,
        &context.encode_command().unwrap(),
        &topology(),
    )
    .unwrap();
    (dir, pg, context)
}

fn observe(
    pg: &PgStore,
    epoch: ClusterEpoch,
) -> Result<MetadataCommandReplicaState, CompleteSettledPrefixError> {
    let read = pg.inspect_complete_settled_prefix(0, epoch, &topology())?;
    let state = *read.prefix();
    read.finish()?;
    Ok(state)
}

fn cold(pg: &PgStore, epoch: ClusterEpoch) {
    let writes = pg.conn.total_changes();
    let result = observe(pg, epoch);
    assert!(
        matches!(result, Err(CompleteSettledPrefixError::ValidationRequired)),
        "{result:?}"
    );
    assert!(pg.conn.is_autocommit());
    assert_eq!(pg.conn.total_changes(), writes);
}

#[test]
fn staging_direct_reinspection_prefix_validates_read_only_and_reuses_warm_evidence() {
    let _clock = test_time_override_guard(2_000);
    for case in 0..18 {
        let (_dir, pg, context) = setup(case);
        let epoch = context.id().cluster_epoch();
        let expected = pg.metadata_command_replica_state().unwrap();
        cold(&pg, epoch);
        if expected.applied_log_index > 4096 {
            // These component fixtures anchor large synthetic log positions by
            // writing real tombstones. Foreground validation must not walk that
            // entire history; separate fixture maintenance establishes a base.
            let writes = pg.conn.total_changes();
            let reads = pg.test_metadata_command_log_replay_validation_entries();
            let error = validate_reinspection(&pg, epoch).unwrap_err();
            assert!(
                error.to_string().contains("entry budget exhausted"),
                "case {case}: {error}"
            );
            assert_eq!(
                pg.test_metadata_command_log_replay_validation_entries() - reads,
                4096
            );
            assert_eq!(pg.conn.total_changes(), writes);
            cold(&pg, epoch);
            pg.record_complete_metadata_command_checkpoint(0, &topology())
                .unwrap();
        }
        let before = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        validate_reinspection(&pg, epoch).unwrap();
        assert_eq!(observe(&pg, epoch).unwrap(), expected);
        let reads = pg.test_metadata_command_log_replay_validation_entries();
        for _ in 0..3 {
            validate_reinspection(&pg, epoch).unwrap();
            assert_eq!(observe(&pg, epoch).unwrap(), expected);
        }
        assert_eq!(
            pg.test_metadata_command_log_replay_validation_entries(),
            reads
        );
        assert_eq!(introduction::state(&pg), before);
        assert_eq!(pg.conn.total_changes(), writes);
    }
}

#[test]
fn staging_direct_reinspection_prefix_rejects_corruption_without_repair() {
    let _clock = test_time_override_guard(2_000);
    for damage in [
        "UPDATE metadata_command_log SET command_checksum = command_checksum + 1",
        "UPDATE metadata_command_log SET pg_id = pg_id + 1",
        "UPDATE metadata_command_log SET cluster_epoch = cluster_epoch + 1",
        "UPDATE metadata_command_replica_state SET state_digest = state_digest + 1",
        "UPDATE payload_staging_windows SET stored_bound = stored_bound + 1",
        "UPDATE metadata_table_digests SET table_digest = table_digest + 1 WHERE table_name = 'buckets'",
        "DELETE FROM metadata_table_digests WHERE table_name = 'buckets'",
    ] {
        let (_dir, pg, context) = setup(3);
        let epoch = context.id().cluster_epoch();
        pg.conn.execute_batch(damage).unwrap();
        let before = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        assert!(validate_reinspection(&pg, epoch).is_err(), "{damage}");
        assert_eq!(introduction::state(&pg), before, "{damage}");
        assert_eq!(pg.conn.total_changes(), writes, "{damage}");
        assert!(pg.conn.is_autocommit());
        cold(&pg, epoch);
    }
}

#[test]
fn staging_direct_reinspection_prefix_checks_pending_indexes_without_clearing_custody() {
    let _clock = test_time_override_guard(2_000);
    for damage in [
        None,
        Some("UPDATE metadata_command_pending_slot SET scope_bucket = NULL"),
        Some("DELETE FROM metadata_command_pending_route_dependencies"),
        Some("DELETE FROM metadata_command_pending_staging_protection"),
        Some("DELETE FROM metadata_command_pending_placed_reference_pages"),
    ] {
        let (_dir, pg, context) = complete_application::setup(8);
        complete_pending::install(&mut context.into_submission(), &pg).unwrap();
        let epoch = pg.metadata_command_replica_state().unwrap().cluster_epoch;
        if let Some(sql) = damage {
            pg.conn.execute_batch(sql).unwrap();
        }
        let before = complete_pending::observe(&pg);
        let writes = pg.conn.total_changes();
        let result = validate_reinspection(&pg, epoch);
        if damage.is_some() {
            assert!(result.is_err(), "{damage:?}");
        } else {
            result.unwrap();
            assert!(matches!(
                observe(&pg, epoch),
                Err(CompleteSettledPrefixError::Store(
                    StoreError::MetadataCommandContention { .. }
                ))
            ));
        }
        assert_eq!(complete_pending::observe(&pg), before);
        assert_eq!(pg.conn.total_changes(), writes);
    }
}

#[test]
fn staging_direct_reinspection_prefix_failed_teardown_retires_without_seeding() {
    let _clock = test_time_override_guard(2_000);
    let (_dir, pg, context) = setup(0);
    pg.fail_next_snapshot_rollback.set(true);
    let writes = pg.conn.total_changes();
    assert!(validate_reinspection(&pg, context.id().cluster_epoch()).is_err());
    assert!(pg.require_usable_connection().is_err());
    assert_eq!(pg.conn.total_changes(), writes);
    pg.conn.execute_batch("ROLLBACK").unwrap();
    assert!(observe(&pg, context.id().cluster_epoch()).is_err());
}

#[test]
fn staging_settled_prefix_all_contexts_require_validation_and_reuse_without_writes() {
    let _clock = test_time_override_guard(2_000);
    for case in 0..18 {
        let (_dir, pg, context) = setup(case);
        let epoch = context.id().cluster_epoch();
        cold(&pg, epoch);
        let expected = recover(&pg);
        let writes = pg.conn.total_changes();
        let materialized = introduction::state(&pg);
        let replay_reads = pg.test_metadata_command_log_replay_validation_entries();
        // Moving the store does not destroy or transplant its own identity.
        let pg = Box::new(pg);
        for _ in 0..4 {
            assert_eq!(observe(&pg, epoch).unwrap(), expected);
        }
        assert_eq!(pg.conn.total_changes(), writes, "case {case}");
        assert_eq!(
            pg.test_metadata_command_log_replay_validation_entries(),
            replay_reads
        );
        assert_eq!(introduction::state(&pg), materialized);
    }
}

#[test]
fn staging_settled_prefix_lookup_uses_only_bounded_scalar_queries() {
    let _clock = test_time_override_guard(2_000);
    let (_dir, mut pg, context) = setup(3);
    let expected = recover(&pg);
    let statements = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&statements);
    pg.conn
        .trace_v2(
            TraceEventCodes::SQLITE_TRACE_STMT,
            Some(move |event: TraceEvent<'_>| {
                if let TraceEvent::Stmt(_, sql) = event {
                    captured.lock().unwrap().push(sql.to_string());
                }
            }),
        )
        .unwrap();
    let writes = pg.conn.total_changes();
    assert_eq!(
        observe(&pg, context.id().cluster_epoch()).unwrap(),
        expected
    );
    let statements = statements.lock().unwrap();
    assert!(!statements.is_empty());
    // Existing query-only guard uses rusqlite's validated pragma helper, which
    // also enumerates SQLite's fixed pragma-name list. Never database contents.
    assert!(statements.len() <= 18, "{statements:?}");
    for sql in statements.iter() {
        let sql = sql.to_ascii_lowercase();
        assert!(
            sql.starts_with("pragma ")
                || sql == "begin deferred"
                || sql == "rollback"
                || sql == "select 1 from pragma_pragma_list where name = ?"
                || sql == "-- pragma pragma_list"
                || sql.contains("from metadata_command_replica_state where singleton = 0")
                || sql == "select 1 from metadata_command_pending_slot limit 1"
                || ([
                    "select cluster_epoch, log_index from metadata_command_log where pg_id = ?2 and (cluster_epoch, pg_id, log_index) >",
                    "select cluster_epoch, log_index from metadata_command_terminal_receipts where pg_id = ?2 and (cluster_epoch, pg_id, log_index) >",
                    "select cluster_epoch, applied_log_index from metadata_command_checkpoints where pg_id = ?2 and (cluster_epoch, pg_id, applied_log_index) >",
                ].iter().any(|prefix| sql.starts_with(prefix)) && sql.ends_with("limit 1")),
            "unexpected lookup SQL: {sql}"
        );
    }
    assert_eq!(pg.conn.total_changes(), writes);
}

#[test]
fn staging_settled_prefix_local_changes_rollback_and_schema_changes_invalidate() {
    let _clock = test_time_override_guard(2_000);
    for change in [
        "UPDATE metadata_command_replica_state SET applied_log_index = applied_log_index",
        "BEGIN IMMEDIATE; UPDATE metadata_command_replica_state SET applied_log_index = applied_log_index; ROLLBACK",
        "UPDATE metadata_command_log SET command_checksum = command_checksum + 1 WHERE log_index = 1",
        "UPDATE payload_staging_windows SET stored_bound = stored_bound + 1",
        "DELETE FROM metadata_table_digests WHERE table_name = 'buckets'",
        "CREATE TABLE untracked_test_table (value INTEGER)",
    ] {
        let (_dir, pg, context) = setup(0);
        let expected = recover(&pg);
        assert_eq!(observe(&pg, expected.cluster_epoch).unwrap(), expected);
        pg.conn.execute_batch(change).unwrap();
        cold(&pg, context.id().cluster_epoch());
        // A cache miss cannot silently repair even missing digest-cache rows.
        cold(&pg, context.id().cluster_epoch());
    }
}

#[test]
fn staging_settled_prefix_external_commits_and_post_validation_races_stay_cold() {
    let _clock = test_time_override_guard(2_000);
    for after_validation in [false, true] {
        let (dir, pg, context) = setup(0);
        pg.conn.execute_batch("CREATE TABLE external_change_probe (value INTEGER); INSERT INTO external_change_probe VALUES (0)").unwrap();
        let other = PgStore::open(dir.path(), pg.pg_id).unwrap();
        let change = move || {
            // Identical prefix/digest/schema and unchanged local total_changes:
            // only the other-connection data_version proves this real write.
            // SQLite may elide a no-op UPDATE, which would not test this guard.
            other
                .conn
                .execute("UPDATE external_change_probe SET value = value + 1", [])
                .unwrap();
        };
        if after_validation {
            pg.test_after_complete_recovery_commit(change);
            recover(&pg);
        } else {
            let expected = recover(&pg);
            assert_eq!(observe(&pg, expected.cluster_epoch).unwrap(), expected);
            change();
        }
        cold(&pg, context.id().cluster_epoch());
        let expected = recover(&pg);
        assert_eq!(observe(&pg, expected.cluster_epoch).unwrap(), expected);
    }
}

#[test]
fn staging_settled_prefix_scope_holds_read_view_and_rejects_crossed_requests() {
    let _clock = test_time_override_guard(2_000);
    let (dir, pg, context) = setup(0);
    pg.conn.execute_batch("CREATE TABLE external_change_probe (value INTEGER); INSERT INTO external_change_probe VALUES (0)").unwrap();
    let expected = recover(&pg);
    let crossed_topology = PgTopology::new(&[pg.pg_id, 100]).unwrap();
    assert!(matches!(
        pg.inspect_complete_settled_prefix(0, expected.cluster_epoch, &crossed_topology),
        Err(CompleteSettledPrefixError::Store(
            StoreError::RouteCapabilitySubjectMismatch { .. }
        ))
    ));
    assert!(matches!(
        observe(
            &pg,
            ClusterEpoch::new(expected.cluster_epoch.get() + 1).unwrap()
        ),
        Err(CompleteSettledPrefixError::Store(
            StoreError::StaleMetadataCommand { .. }
        ))
    ));
    let other = PgStore::open(dir.path(), pg.pg_id).unwrap();
    let read = pg
        .inspect_complete_settled_prefix(0, expected.cluster_epoch, &topology())
        .unwrap();
    assert!(!pg.conn.is_autocommit());
    assert!(pg
        .conn
        .execute(
            "UPDATE metadata_command_replica_state SET applied_log_index = applied_log_index",
            []
        )
        .is_err());
    assert!(pg
        .inspect_complete_settled_prefix(0, expected.cluster_epoch, &topology())
        .is_err());
    other
        .conn
        .execute("UPDATE external_change_probe SET value = value + 1", [])
        .unwrap();
    assert_eq!(*read.prefix(), expected);
    read.finish().unwrap();
    cold(&pg, context.id().cluster_epoch());
}

#[test]
fn staging_settled_prefix_pending_tail_and_failed_recovery_cannot_confirm() {
    let _clock = test_time_override_guard(2_000);
    let (_dir, pg, context) = complete_application::setup(0);
    let encoded = context.encode_command().unwrap();
    let decoded =
        ContextualCommand::decode_command(encoded.bytes(), encoded.checksum_crc64(), &topology())
            .unwrap();
    complete_pending::install(&mut decoded.into_submission(), &pg).unwrap();
    pg.apply_complete_metadata_command_and_record(0, &encoded, &topology())
        .unwrap();
    let expected = recover(&pg);
    let writes = pg.conn.total_changes();
    assert!(matches!(
        observe(&pg, expected.cluster_epoch),
        Err(CompleteSettledPrefixError::Store(
            StoreError::MetadataCommandContention { .. }
        ))
    ));
    assert_eq!(pg.conn.total_changes(), writes);
    let (_dir, pg, context) = setup(0);
    recover(&pg);
    pg.conn
        .execute(
            "UPDATE metadata_command_replica_state SET applied_log_index = 0, applied_log_hash = 0",
            [],
        )
        .unwrap();
    cold(&pg, context.id().cluster_epoch());
    assert!(pg
        .recover_complete_command_state(
            PgStoreRecoveryContext::for_node(NodeId::new(0)),
            &topology()
        )
        .is_err());
    cold(&pg, context.id().cluster_epoch());
}

#[test]
fn staging_settled_prefix_cache_cannot_transfer_between_stores_or_survive_reopen() {
    let _clock = test_time_override_guard(2_000);
    // Empty databases have identical canonical contents, unlike separately
    // minted staging commands whose issuer lifetimes intentionally differ.
    let dir = test_util::tempdir();
    let other_dir = test_util::tempdir();
    let mut first = PgStore::open(dir.path(), 0).unwrap();
    let mut second = PgStore::open(other_dir.path(), 0).unwrap();
    let expected = recover(&first);
    assert_eq!(recover(&second), expected);
    assert_eq!(first.conn.total_changes(), second.conn.total_changes());
    // Same topology, PG, SQL values and write count; only live store identity
    // prevents transplanting the whole otherwise-private cache container.
    std::mem::swap(
        &mut first.complete_prefix_validation,
        &mut second.complete_prefix_validation,
    );
    cold(&first, expected.cluster_epoch);
    cold(&second, expected.cluster_epoch);
    recover(&first);
    let id = first.pg_id;
    drop(first);
    let reopened = PgStore::open(dir.path(), id).unwrap();
    cold(&reopened, expected.cluster_epoch);
    assert_eq!(recover(&reopened), expected);
    assert_eq!(
        observe(&reopened, expected.cluster_epoch).unwrap(),
        expected
    );
}

#[test]
fn staging_settled_prefix_replacement_and_import_require_fresh_validation() {
    let _clock = test_time_override_guard(2_000);
    for complete in [false, true] {
        let (_dir, pg, _) = setup(0);
        let expected = recover(&pg);
        let replacement = pg.begin_allocation_lifetime_replacement().unwrap();
        assert!(observe(&pg, expected.cluster_epoch).is_err());
        if complete {
            replacement.complete().unwrap();
            cold(&pg, expected.cluster_epoch);
            assert_eq!(recover(&pg), expected);
            assert_eq!(observe(&pg, expected.cluster_epoch).unwrap(), expected);
        } else {
            drop(replacement);
            assert_eq!(recover(&pg), expected);
            assert!(observe(&pg, expected.cluster_epoch).is_err());
        }
    }
    let (_dir, pg, _) = setup(0);
    recover(&pg);
    let checkpoint = pg
        .complete_metadata_command_checkpoint(0, &topology())
        .unwrap();
    let epoch = ClusterEpoch::new(checkpoint.cluster_epoch.get() + 1).unwrap();
    pg.install_metadata_transfer_checkpoint_base(0, epoch, &checkpoint, &topology())
        .unwrap();
    cold(&pg, epoch);
    let expected = recover(&pg);
    assert_eq!(expected.applied_log_index, 0);
    assert_eq!(observe(&pg, epoch).unwrap(), expected);
}

#[test]
fn staging_settled_prefix_failed_snapshot_teardown_retires_cache_and_connection() {
    let _clock = test_time_override_guard(2_000);
    for explicit in [false, true] {
        let (dir, pg, _) = setup(0);
        let expected = recover(&pg);
        let read = pg
            .inspect_complete_settled_prefix(0, expected.cluster_epoch, &topology())
            .unwrap();
        pg.fail_next_snapshot_rollback.set(true);
        if explicit {
            assert!(read.finish().is_err());
        } else {
            drop(read);
        }
        assert!(pg.require_usable_connection().is_err());
        // Even manually ending SQLite's transaction cannot revive the store.
        pg.conn.execute_batch("ROLLBACK").unwrap();
        assert!(observe(&pg, expected.cluster_epoch).is_err());
        let id = pg.pg_id;
        drop(pg);
        let reopened = PgStore::open(dir.path(), id).unwrap();
        cold(&reopened, expected.cluster_epoch);
        recover(&reopened);
        assert_eq!(
            observe(&reopened, expected.cluster_epoch).unwrap(),
            expected
        );
    }
}

#[test]
fn staging_settled_prefix_application_keeps_same_epoch_validation_but_compaction_stays_cold() {
    let _clock = test_time_override_guard(2_000);
    for case in [0, 3, 8] {
        let (_dir, pg, context) = complete_application::setup(case);
        let before = recover(&pg);
        assert_eq!(observe(&pg, before.cluster_epoch).unwrap(), before);
        let after = pg
            .apply_complete_metadata_command_and_record(
                0,
                &context.encode_command().unwrap(),
                &topology(),
            )
            .unwrap();
        assert_ne!(after, before);
        if before.cluster_epoch == after.cluster_epoch {
            assert_eq!(observe(&pg, after.cluster_epoch).unwrap(), after);
        } else {
            cold(&pg, after.cluster_epoch);
        }
        assert_eq!(recover(&pg), after);
        assert_eq!(observe(&pg, after.cluster_epoch).unwrap(), after);
        pg.record_complete_metadata_command_checkpoint(0, &topology())
            .unwrap();
        pg.compact_complete_metadata_command_log(&topology())
            .unwrap();
        assert!(super::complete_maintenance::log_state(&pg)[0].is_empty());
        cold(&pg, after.cluster_epoch);
        assert_eq!(recover(&pg), after);
        assert_eq!(observe(&pg, after.cluster_epoch).unwrap(), after);
    }
}

#[derive(Clone, Copy, Debug)]
enum AheadEvidence {
    UnappliedLog,
    AbandonedLog,
    Receipt,
    Checkpoint,
}

fn stage_ahead_evidence(pg: &PgStore, id: MetadataCommandId, evidence: AheadEvidence) {
    let command = super::complete_log_range::ordinary(id);
    let context = application::ordinary_context(&command, &topology()).unwrap();
    let encoded = context.encode_command().unwrap();
    if matches!(evidence, AheadEvidence::UnappliedLog) {
        pg.conn.execute(
            "INSERT INTO metadata_command_log (cluster_epoch, pg_id, log_index, command_checksum, command_bytes, abandoned) VALUES (?1, ?2, ?3, ?4, ?5, 0)",
            rusqlite::params![id.cluster_epoch().get() as i64, id.pg_id().get(), id.log_index().get() as i64, encoded.checksum_crc64() as i64, encoded.bytes()],
        ).unwrap();
    } else {
        // Real terminal bytes, hashes and checkpoint/receipt generation. This
        // command is abandoned, so the materialized digest stays unchanged.
        pg.test_record_complete_command_abandoned(0, &encoded)
            .unwrap();
        if matches!(evidence, AheadEvidence::Receipt | AheadEvidence::Checkpoint) {
            pg.record_complete_metadata_command_checkpoint(0, &topology())
                .unwrap();
            pg.compact_complete_metadata_command_log(&topology())
                .unwrap();
            // Isolate the carrier under test: another future row must not mask
            // omission of the receipt or checkpoint check.
            let sql = match evidence {
                AheadEvidence::Receipt => "DELETE FROM metadata_command_checkpoints WHERE cluster_epoch = ?1 AND applied_log_index = ?2",
                AheadEvidence::Checkpoint => "DELETE FROM metadata_command_terminal_receipts WHERE cluster_epoch = ?1 AND log_index = ?2",
                AheadEvidence::UnappliedLog | AheadEvidence::AbandonedLog => unreachable!(),
            };
            assert_eq!(
                pg.conn
                    .execute(
                        sql,
                        [id.cluster_epoch().get() as i64, id.log_index().get() as i64]
                    )
                    .unwrap(),
                1
            );
        }
    }
}

fn reject_ahead_evidence(evidence: AheadEvidence, future_epoch: bool) {
    let _clock = test_time_override_guard(2_000);
    let (dir, mut pg, _) = setup(0);
    let prefix = recover(&pg);
    assert_eq!(observe(&pg, prefix.cluster_epoch).unwrap(), prefix);
    // Keep an authentic base for the old prefix when isolating a same-epoch
    // compacted receipt/checkpoint beyond it. Missing older log rows must not
    // be the reason recovery fails.
    pg.record_complete_metadata_command_checkpoint(0, &topology())
        .unwrap();
    let id = MetadataCommandId::new(
        if future_epoch {
            ClusterEpoch::new(prefix.cluster_epoch.get() + 1).unwrap()
        } else {
            prefix.cluster_epoch
        },
        PgId::new(pg.pg_id),
        MetadataCommandLogIndex::new(if future_epoch {
            1
        } else {
            prefix.applied_log_index + 1
        })
        .unwrap(),
    );
    let materialized = introduction::state(&pg);
    stage_ahead_evidence(&pg, id, evidence);
    // Simulate a durable command/terminal carrier ahead of the stored replica
    // prefix. The older prefix and materialized state remain independently valid.
    pg.test_replace_metadata_command_replica_state(prefix)
        .unwrap();
    assert_eq!(introduction::state(&pg), materialized);
    for reopen in [false, true] {
        if reopen {
            let pg_id = pg.pg_id;
            drop(pg);
            pg = PgStore::open(dir.path(), pg_id).unwrap();
        }
        let retained = super::complete_maintenance::log_state(&pg);
        let writes = pg.conn.total_changes();
        let result = pg.recover_complete_command_state(
            PgStoreRecoveryContext::for_node(NodeId::new(0)),
            &topology(),
        );
        assert!(
            matches!(result, Err(StoreError::MetadataCommandLogConflict { cluster_epoch, log_index, .. }) if cluster_epoch == id.cluster_epoch() && log_index == id.log_index().get()),
            "{evidence:?}, reopen={reopen}: {result:?}"
        );
        assert_eq!(pg.conn.total_changes(), writes);
        assert!(pg.conn.is_autocommit());
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        assert_eq!(introduction::state(&pg), materialized);
        assert_eq!(super::complete_maintenance::log_state(&pg), retained);
        cold(&pg, prefix.cluster_epoch);
    }
}

#[test]
fn staging_settled_prefix_rejects_future_unapplied_command_after_restart() {
    reject_ahead_evidence(AheadEvidence::UnappliedLog, true);
}

#[test]
fn staging_settled_prefix_rejects_future_abandoned_command_after_restart() {
    reject_ahead_evidence(AheadEvidence::AbandonedLog, true);
}

#[test]
fn staging_settled_prefix_rejects_future_receipt_after_restart() {
    reject_ahead_evidence(AheadEvidence::Receipt, true);
}

#[test]
fn staging_settled_prefix_rejects_future_checkpoint_after_restart() {
    reject_ahead_evidence(AheadEvidence::Checkpoint, true);
}

#[test]
fn staging_settled_prefix_rejects_same_epoch_receipt_ahead_after_restart() {
    reject_ahead_evidence(AheadEvidence::Receipt, false);
}

#[test]
fn staging_settled_prefix_rejects_same_epoch_checkpoint_ahead_after_restart() {
    reject_ahead_evidence(AheadEvidence::Checkpoint, false);
}

#[test]
fn staging_settled_prefix_preserves_older_evidence_after_coherent_epoch_advance() {
    let _clock = test_time_override_guard(2_000);
    let (dir, mut pg, _) = setup(0);
    let old = recover(&pg);
    pg.record_complete_metadata_command_checkpoint(0, &topology())
        .unwrap();
    pg.compact_complete_metadata_command_log(&topology())
        .unwrap();
    let id = MetadataCommandId::new(
        ClusterEpoch::new(old.cluster_epoch.get() + 1).unwrap(),
        PgId::new(pg.pg_id),
        MetadataCommandLogIndex::new(1).unwrap(),
    );
    stage_ahead_evidence(&pg, id, AheadEvidence::AbandonedLog);
    let expected = pg.metadata_command_replica_state().unwrap();
    let retained = super::complete_maintenance::log_state(&pg);
    assert!(retained.iter().all(|rows| !rows.is_empty()));
    for reopen in [false, true] {
        if reopen {
            let pg_id = pg.pg_id;
            drop(pg);
            pg = PgStore::open(dir.path(), pg_id).unwrap();
        }
        assert_eq!(recover(&pg), expected);
        assert_eq!(observe(&pg, expected.cluster_epoch).unwrap(), expected);
        assert_eq!(super::complete_maintenance::log_state(&pg), retained);
    }
}

fn reject_foreign_pg_evidence(evidence: AheadEvidence) {
    let _clock = test_time_override_guard(2_000);
    // Do not use PG zero: the old lexicographic bound rejected larger PGs but
    // silently accepted a smaller PG at the same epoch, irrespective of index.
    for foreign_pg in [4, 6] {
        for foreign_epoch in [5, 4, 6] {
            let dir = test_util::tempdir();
            let mut pg = PgStore::open(dir.path(), 5).unwrap();
            let id = MetadataCommandId::new(
                ClusterEpoch::new(5).unwrap(),
                PgId::new(5),
                MetadataCommandLogIndex::new(1).unwrap(),
            );
            stage_ahead_evidence(&pg, id, AheadEvidence::AbandonedLog);
            pg.record_complete_metadata_command_checkpoint(0, &topology())
                .unwrap();
            pg.compact_complete_metadata_command_log(&topology())
                .unwrap();
            stage_ahead_evidence(
                &pg,
                MetadataCommandId::new(
                    id.cluster_epoch(),
                    id.pg_id(),
                    MetadataCommandLogIndex::new(2).unwrap(),
                ),
                AheadEvidence::AbandonedLog,
            );
            // All three healthy carriers must be accepted for this nonzero PG.
            assert!(super::complete_maintenance::log_state(&pg)
                .iter()
                .all(|rows| !rows.is_empty()));
            let prefix = recover(&pg);
            assert_eq!(observe(&pg, prefix.cluster_epoch).unwrap(), prefix);
            pg.record_complete_metadata_command_checkpoint(0, &topology())
                .unwrap();
            let materialized = introduction::state(&pg);
            let next = MetadataCommandId::new(
                id.cluster_epoch(),
                id.pg_id(),
                MetadataCommandLogIndex::new(3).unwrap(),
            );
            stage_ahead_evidence(&pg, next, evidence);
            let (sql, operation) = match evidence {
                AheadEvidence::UnappliedLog | AheadEvidence::AbandonedLog => (
                    "UPDATE metadata_command_log SET pg_id = ?1, cluster_epoch = ?2 WHERE cluster_epoch = 5 AND pg_id = 5 AND log_index = 3",
                    "complete startup log PG",
                ),
                AheadEvidence::Receipt => (
                    "UPDATE metadata_command_terminal_receipts SET pg_id = ?1, cluster_epoch = ?2 WHERE cluster_epoch = 5 AND pg_id = 5 AND log_index = 3",
                    "complete startup terminal receipt PG",
                ),
                AheadEvidence::Checkpoint => (
                    "UPDATE metadata_command_checkpoints SET pg_id = ?1, cluster_epoch = ?2 WHERE cluster_epoch = 5 AND pg_id = 5 AND applied_log_index = 3",
                    "complete startup checkpoint PG",
                ),
            };
            // Corrupt only this carrier's SQL subject, retaining the authentic
            // old prefix and independently valid materialized state. Compacted
            // cases isolate receipts/checkpoints from the other later carriers.
            assert_eq!(
                pg.conn.execute(sql, [foreign_pg, foreign_epoch]).unwrap(),
                1
            );
            pg.test_replace_metadata_command_replica_state(prefix)
                .unwrap();
            for reopen in [false, true] {
                if reopen {
                    drop(pg);
                    pg = PgStore::open(dir.path(), 5).unwrap();
                }
                let retained = super::complete_maintenance::log_state(&pg);
                let writes = pg.conn.total_changes();
                // A write must revoke the prior positive entry even before
                // explicit validation; reopen likewise starts with a cold cache.
                cold(&pg, prefix.cluster_epoch);
                let result = pg.recover_complete_command_state(
                    PgStoreRecoveryContext::for_node(NodeId::new(0)),
                    &topology(),
                );
                assert!(
                    matches!(result, Err(StoreError::RouteCapabilitySubjectMismatch { operation: actual }) if actual == operation),
                    "{evidence:?}, foreign_pg={foreign_pg}, foreign_epoch={foreign_epoch}, reopen={reopen}: {result:?}"
                );
                assert!(pg.conn.is_autocommit());
                assert_eq!(pg.conn.total_changes(), writes);
                assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
                assert_eq!(introduction::state(&pg), materialized);
                assert_eq!(super::complete_maintenance::log_state(&pg), retained);
                cold(&pg, prefix.cluster_epoch);
            }
        }
    }
}

#[test]
fn staging_settled_prefix_rejects_foreign_pg_unapplied_log_after_restart() {
    reject_foreign_pg_evidence(AheadEvidence::UnappliedLog);
}

#[test]
fn staging_settled_prefix_rejects_foreign_pg_abandoned_log_after_restart() {
    reject_foreign_pg_evidence(AheadEvidence::AbandonedLog);
}

#[test]
fn staging_settled_prefix_rejects_foreign_pg_receipt_after_restart() {
    reject_foreign_pg_evidence(AheadEvidence::Receipt);
}

#[test]
fn staging_settled_prefix_rejects_foreign_pg_checkpoint_after_restart() {
    reject_foreign_pg_evidence(AheadEvidence::Checkpoint);
}
