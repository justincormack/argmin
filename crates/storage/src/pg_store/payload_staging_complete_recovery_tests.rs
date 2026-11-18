// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Startup prefix validation and local repair, never fanout or slot retirement.
//! Metadata-only fixtures; incomplete-tail faults alter no physical payload.

use super::complete_maintenance::log_state;
use super::*;
use crate::clock::test_time_override_guard;
use crate::metadata_command::{
    MetadataCommandEnvelope, MetadataCommandPayload, MetadataCommandReplicaState,
    ReserveObjectVersionCommand,
};
use crate::node_runtime::pg_store::{PgStore, PgStoreRecoveryContext};
use crate::{NodeId, StoreError, VersionId};

fn recover(pg: &PgStore) -> Result<MetadataCommandReplicaState, StoreError> {
    pg.recover_complete_command_state(
        PgStoreRecoveryContext::for_node(NodeId::new(0)),
        &topology(),
    )
}

fn install(pg: &PgStore, context: &ContextualCommand) {
    let encoded = context.encode_command().unwrap();
    let duplicate =
        ContextualCommand::decode_command(encoded.bytes(), encoded.checksum_crc64(), &topology())
            .unwrap();
    complete_pending::install(&mut duplicate.into_submission(), pg).unwrap();
}

fn apply(pg: &PgStore, context: &ContextualCommand) {
    pg.apply_complete_metadata_command_and_record(
        0,
        &context.encode_command().unwrap(),
        &topology(),
    )
    .unwrap();
}

fn caches(pg: &PgStore) -> Vec<String> {
    pg.conn.prepare("SELECT printf('%s:%lld:%lld:%lld:%lld', table_name, table_digest, row_count, row_hash_xor, row_hash_sum) FROM metadata_table_digests ORDER BY table_name").unwrap().query_map([], |row| row.get(0)).unwrap().collect::<Result<_, _>>().unwrap()
}

fn reject_unchanged(pg: &PgStore) -> StoreError {
    let materialized = introduction::state(pg);
    let log = log_state(pg);
    let pending = complete_pending::observe(pg);
    let prefix = pg.metadata_command_replica_state().unwrap();
    let cache = caches(pg);
    let error = recover(pg).unwrap_err();
    assert!(pg.conn.is_autocommit());
    pg.require_usable_connection().unwrap();
    assert_eq!(introduction::state(pg), materialized);
    assert_eq!(log_state(pg), log);
    assert_eq!(complete_pending::observe(pg), pending);
    assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
    assert_eq!(caches(pg), cache);
    error
}

// Produce a real authenticated tombstone, then model interrupted linking by
// restoring the prior prefix and removing ONLY this terminal's hash links.
fn interrupted_tail(pg: &PgStore, context: &ContextualCommand) -> MetadataCommandReplicaState {
    let prior = pg.metadata_command_replica_state().unwrap();
    pg.test_record_complete_command_abandoned(0, &context.encode_command().unwrap())
        .unwrap();
    let expected = pg.metadata_command_replica_state().unwrap();
    assert_eq!(expected.applied_log_index, context.id().log_index().get());
    let (index, hash) = if prior.cluster_epoch == expected.cluster_epoch {
        (prior.applied_log_index, prior.applied_log_hash.value())
    } else {
        (0, 0)
    };
    assert_eq!(index + 1, expected.applied_log_index);
    pg.conn.execute("UPDATE metadata_command_replica_state SET applied_log_index = ?1, applied_log_hash = ?2", rusqlite::params![index as i64, hash as i64]).unwrap();
    pg.conn.execute("UPDATE metadata_command_log SET previous_log_hash = NULL, log_hash = NULL WHERE cluster_epoch = ?1 AND log_index = ?2", rusqlite::params![context.id().cluster_epoch().get() as i64, context.id().log_index().get() as i64]).unwrap();
    expected
}

#[test]
fn staging_complete_recovery_preserves_all_terminal_and_unresolved_pending_custody() {
    let _clock = test_time_override_guard(2_000);
    let mut unresolved = 0;
    let mut future = 0;
    for case in 0..18 {
        for abandoned in [false, true] {
            let (dir, pg, context) = complete_application::setup(case);
            install(&pg, &context);
            pg.conn
                .execute(
                    "UPDATE metadata_command_pending_slot SET publication_started = 1",
                    [],
                )
                .unwrap();
            let pending = complete_pending::observe(&pg);
            let prior = pg.metadata_command_replica_state().unwrap();
            if prior.cluster_epoch == context.id().cluster_epoch() {
                let writes = pg.conn.total_changes();
                assert_eq!(recover(&pg).unwrap(), prior);
                assert_eq!(pg.conn.total_changes(), writes);
                unresolved += 1;
            } else {
                // These component fixtures have not advanced the store's epoch.
                // Local startup cannot authorize that future-epoch pending slot.
                assert!(prior.cluster_epoch < context.id().cluster_epoch());
                assert!(matches!(
                    reject_unchanged(&pg),
                    StoreError::MetadataCommandLogConflict { .. }
                ));
                future += 1;
            }
            if abandoned {
                pg.test_record_complete_command_abandoned(0, &context.encode_command().unwrap())
                    .unwrap();
            } else {
                apply(&pg, &context);
            }
            let before = introduction::state(&pg);
            let prefix = pg.metadata_command_replica_state().unwrap();
            let writes = pg.conn.total_changes();
            assert_eq!(recover(&pg).unwrap(), prefix);
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(complete_pending::observe(&pg), pending);
            assert_eq!(introduction::state(&pg), before);
            let id = pg.pg_id;
            drop(pg);
            let pg = PgStore::open(dir.path(), id).unwrap();
            let writes = pg.conn.total_changes();
            assert_eq!(recover(&pg).unwrap(), prefix);
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(complete_pending::observe(&pg), pending);
            assert_eq!(introduction::state(&pg), before);
        }
    }
    assert!(unresolved > 0 && future > 0);
}

#[test]
fn staging_complete_recovery_links_abandoned_tail_without_clearing_any_pending_context() {
    let _clock = test_time_override_guard(2_000);
    for case in 0..18 {
        let (dir, pg, context) = complete_application::setup(case);
        install(&pg, &context);
        let expected = interrupted_tail(&pg, &context);
        let pending = complete_pending::observe(&pg);
        let before = introduction::state(&pg);
        let pg_id = pg.pg_id;
        drop(pg);
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        assert_eq!(recover(&pg).unwrap(), expected, "case {case}");
        assert_eq!(complete_pending::observe(&pg), pending);
        assert_eq!(introduction::state(&pg), before);
        let observed = pg
            .inspect_complete_pending_command(0, &topology())
            .unwrap()
            .unwrap();
        assert!(matches!(
            observed.terminal,
            crate::node_runtime::pg_store::MetadataCommandStartupDisposition::Abandoned { .. }
        ));
        let writes = pg.conn.total_changes();
        assert_eq!(recover(&pg).unwrap(), expected);
        assert_eq!(pg.conn.total_changes(), writes);
    }
}

#[test]
fn staging_complete_recovery_damaged_indexes_roll_back_tail_links_and_keep_custody() {
    let _clock = test_time_override_guard(2_000);
    for damage in [
        "UPDATE metadata_command_pending_slot SET scope_bucket = NULL",
        "DELETE FROM metadata_command_pending_route_dependencies",
        "DELETE FROM metadata_command_pending_staging_protection",
        "DELETE FROM metadata_command_pending_placed_reference_pages",
    ] {
        let (_dir, pg, context) = complete_application::setup(8);
        install(&pg, &context);
        interrupted_tail(&pg, &context);
        pg.conn.execute_batch(damage).unwrap();
        reject_unchanged(&pg);
    }
}

#[test]
fn staging_complete_recovery_rejects_damaged_prefix_tail_gaps_and_materialized_state() {
    let _clock = test_time_override_guard(2_000);
    for damage in [
        "UPDATE metadata_command_log SET command_checksum = command_checksum + 1 WHERE log_index = 1",
        "UPDATE metadata_command_log SET command_bytes = x'0000' WHERE log_index = 1",
        "UPDATE metadata_command_log SET log_hash = log_hash + 1 WHERE log_index = 1",
        "DELETE FROM metadata_command_log WHERE log_index = 1",
        "UPDATE metadata_command_replica_state SET state_digest = state_digest + 1",
        "UPDATE payload_staging_windows SET stored_bound = stored_bound + 1",
    ] {
        let (_dir, pg, context) = complete_application::setup(3);
        install(&pg, &context);
        interrupted_tail(&pg, &context);
        pg.conn.execute_batch(damage).unwrap();
        reject_unchanged(&pg);
    }
    // An otherwise valid tombstone after a hole must not be silently ignored.
    let (_dir, pg, context) = complete_application::setup(5);
    let encoded = context.encode_command().unwrap();
    let id = MetadataCommandId::new(
        context.id().cluster_epoch(),
        context.id().pg_id(),
        MetadataCommandLogIndex::new(context.id().log_index().get() + 1).unwrap(),
    );
    let bytes = crate::metadata_command::abandoned_command_log_bytes(
        id,
        encoded.checksum_crc64(),
        checksum::sha256::digest(encoded.bytes()),
    );
    pg.conn
        .execute(
            "UPDATE metadata_command_replica_state SET cluster_epoch = ?1",
            [id.cluster_epoch().get() as i64],
        )
        .unwrap();
    pg.conn.execute("INSERT INTO metadata_command_log(cluster_epoch, pg_id, log_index, command_checksum, command_bytes, abandoned) VALUES (?1, ?2, ?3, ?4, ?5, 1)", rusqlite::params![id.cluster_epoch().get() as i64, id.pg_id().get(), id.log_index().get() as i64, checksum::crc64::checksum(&bytes) as i64, bytes]).unwrap();
    assert!(
        matches!(reject_unchanged(&pg), StoreError::MetadataCommandLogConflict { log_index, .. } if log_index == id.log_index().get())
    );
}

#[test]
fn staging_complete_recovery_repairs_only_digest_cache_after_all_validation() {
    let _clock = test_time_override_guard(2_000);
    let (dir, pg, context) = complete_application::setup(0);
    apply(&pg, &context);
    let prefix = pg.metadata_command_replica_state().unwrap();
    let before = introduction::state(&pg);
    let log = log_state(&pg);
    pg.conn.execute("UPDATE metadata_table_digests SET table_digest = table_digest + 1, row_hash_sum = row_hash_sum + 1 WHERE table_name = 'buckets'", []).unwrap();
    assert!(!pg
        .test_metadata_digest_table_mismatches()
        .unwrap()
        .is_empty());
    let id = pg.pg_id;
    drop(pg);
    let pg = PgStore::open(dir.path(), id).unwrap();
    assert_eq!(recover(&pg).unwrap(), prefix);
    assert!(pg
        .test_metadata_digest_table_mismatches()
        .unwrap()
        .is_empty());
    assert_eq!(introduction::state(&pg), before);
    assert_eq!(log_state(&pg), log);
    assert_eq!(
        pg.cached_metadata_state_digest().unwrap(),
        pg.metadata_state_digest().unwrap()
    );
    let writes = pg.conn.total_changes();
    assert_eq!(recover(&pg).unwrap(), prefix);
    assert_eq!(pg.conn.total_changes(), writes);
}

#[test]
fn staging_complete_recovery_uses_checkpoint_prefix_after_compaction() {
    let _clock = test_time_override_guard(2_000);
    for case in 0..18 {
        let (dir, pg, context) = complete_application::setup(case);
        apply(&pg, &context);
        pg.record_complete_metadata_command_checkpoint(0, &topology())
            .unwrap();
        pg.compact_complete_metadata_command_log(&topology())
            .unwrap();
        assert!(log_state(&pg)[0].is_empty());
        let prefix = pg.metadata_command_replica_state().unwrap();
        let log = log_state(&pg);
        let before = introduction::state(&pg);
        let id = pg.pg_id;
        drop(pg);
        let pg = PgStore::open(dir.path(), id).unwrap();
        let writes = pg.conn.total_changes();
        assert_eq!(recover(&pg).unwrap(), prefix);
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(log_state(&pg), log);
        assert_eq!(introduction::state(&pg), before);
    }
}

#[test]
fn staging_complete_recovery_preserves_historical_pending_under_stored_epoch() {
    let _clock = test_time_override_guard(2_000);
    for terminal in ["unresolved", "applied", "abandoned"] {
        let (dir, pg, context) = complete_application::setup(8);
        install(&pg, &context);
        match terminal {
            "applied" => apply(&pg, &context),
            "abandoned" => {
                pg.test_record_complete_command_abandoned(0, &context.encode_command().unwrap())
                    .unwrap();
            }
            _ => {}
        }
        let owner = fixture(StagingTarget::DirectPut);
        let successor = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(context.id().cluster_epoch().get() + 1).unwrap(),
                context.id().pg_id(),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
                owner.subject.bucket,
                owner.subject.key,
                VersionId::from_u64(100),
            )),
        );
        pg.record_metadata_command_abandoned(0, &successor).unwrap();
        let prefix = pg.metadata_command_replica_state().unwrap();
        let pending = complete_pending::observe(&pg);
        let id = pg.pg_id;
        drop(pg);
        let pg = PgStore::open(dir.path(), id).unwrap();
        assert_eq!(recover(&pg).unwrap(), prefix);
        assert_eq!(complete_pending::observe(&pg), pending);
    }
}

#[test]
fn staging_complete_recovery_rejects_caller_transaction_and_foreign_topology() {
    let _clock = test_time_override_guard(2_000);
    let (_dir, pg, context) = complete_application::setup(3);
    install(&pg, &context);
    let pending = complete_pending::observe(&pg);
    let writes = pg.conn.total_changes();
    for foreign in [
        PgTopology::new(&[99]).unwrap(),
        PgTopology::new(&(0..16).collect::<Vec<_>>()).unwrap(),
    ] {
        assert!(pg
            .recover_complete_command_state(
                PgStoreRecoveryContext::for_node(NodeId::new(0)),
                &foreign
            )
            .is_err());
    }
    pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    assert!(recover(&pg).is_err());
    assert!(!pg.conn.is_autocommit());
    pg.require_usable_connection().unwrap();
    pg.conn.execute_batch("ROLLBACK").unwrap();
    assert_eq!(pg.conn.total_changes(), writes);
    assert_eq!(complete_pending::observe(&pg), pending);
}

#[test]
fn staging_complete_recovery_failed_commit_rolls_back_and_failed_rollback_retires() {
    let _clock = test_time_override_guard(2_000);
    for fail_rollback in [false, true] {
        let (dir, pg, context) = complete_application::setup(8);
        install(&pg, &context);
        let expected = interrupted_tail(&pg, &context);
        let prefix = pg.metadata_command_replica_state().unwrap();
        let log = log_state(&pg);
        let pending = complete_pending::observe(&pg);
        let before = introduction::state(&pg);
        pg.conn.execute("UPDATE metadata_table_digests SET table_digest = table_digest + 1, row_hash_sum = row_hash_sum + 1 WHERE table_name = 'buckets'", []).unwrap();
        let cache = caches(&pg);
        assert!(!pg
            .test_metadata_digest_table_mismatches()
            .unwrap()
            .is_empty());
        pg.conn.execute_batch("CREATE TEMP TABLE deferred_recovery_failure (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES deferred_recovery_failure(id) DEFERRABLE INITIALLY DEFERRED); CREATE TEMP TRIGGER fail_recovery AFTER UPDATE ON metadata_command_replica_state BEGIN INSERT INTO deferred_recovery_failure(parent) VALUES (0); END;").unwrap();
        if fail_rollback {
            pg.test_fail_metadata_command_rollbacks(1);
        }
        let error = recover(&pg).unwrap_err();
        assert!(
            error.to_string().contains("FOREIGN KEY constraint failed"),
            "{error}"
        );
        assert_eq!(
            pg.clean_metadata_digest_revision
                .load(std::sync::atomic::Ordering::Relaxed),
            crate::node_runtime::pg_store::UNCLEAN_METADATA_DIGEST_REVISION
        );
        if fail_rollback {
            assert!(!pg.conn.is_autocommit());
            assert_ne!(log_state(&pg), log, "fault must follow tail linking");
            assert!(pg.require_usable_connection().is_err());
            assert!(recover(&pg).is_err());
        } else {
            assert!(pg.conn.is_autocommit());
            pg.require_usable_connection().unwrap();
            assert_eq!(log_state(&pg), log);
            assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
            assert_eq!(caches(&pg), cache, "failed commit retained cache repair");
        }
        let id = pg.pg_id;
        drop(pg);
        let pg = PgStore::open(dir.path(), id).unwrap();
        pg.require_usable_connection().unwrap();
        assert_eq!(log_state(&pg), log);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        assert_eq!(complete_pending::observe(&pg), pending);
        assert_eq!(introduction::state(&pg), before);
        assert_eq!(caches(&pg), cache);
        assert_eq!(recover(&pg).unwrap(), expected);
        assert_eq!(complete_pending::observe(&pg), pending);
        assert!(pg
            .test_metadata_digest_table_mismatches()
            .unwrap()
            .is_empty());
    }
}

#[test]
fn staging_complete_recovery_unwind_rolls_back_or_retires_uncommitted_tail() {
    let _clock = test_time_override_guard(2_000);
    for fail_rollback in [false, true] {
        let (dir, pg, context) = complete_application::setup(8);
        install(&pg, &context);
        let expected = interrupted_tail(&pg, &context);
        let prefix = pg.metadata_command_replica_state().unwrap();
        let log = log_state(&pg);
        let pending = complete_pending::observe(&pg);
        let before = introduction::state(&pg);
        if fail_rollback {
            pg.test_fail_metadata_command_rollbacks(1);
        }
        pg.test_after_pending_slot_row_read(|| panic!("complete startup validation unwind"));
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| recover(&pg))).is_err());
        if fail_rollback {
            assert!(!pg.conn.is_autocommit());
            assert_ne!(log_state(&pg), log, "unwind must follow tail linking");
            assert!(pg.require_usable_connection().is_err());
            assert!(recover(&pg).is_err());
        } else {
            assert!(pg.conn.is_autocommit());
            pg.require_usable_connection().unwrap();
            assert_eq!(log_state(&pg), log);
            assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
            assert_eq!(complete_pending::observe(&pg), pending);
            assert_eq!(introduction::state(&pg), before);
        }
        let id = pg.pg_id;
        drop(pg);
        let pg = PgStore::open(dir.path(), id).unwrap();
        pg.require_usable_connection().unwrap();
        assert_eq!(log_state(&pg), log);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        assert_eq!(complete_pending::observe(&pg), pending);
        assert_eq!(introduction::state(&pg), before);
        assert_eq!(recover(&pg).unwrap(), expected);
        assert_eq!(complete_pending::observe(&pg), pending);
    }
}

#[test]
fn staging_complete_recovery_marks_only_the_revision_validated_before_commit_clean() {
    let _clock = test_time_override_guard(2_000);
    let (dir, pg, context) = complete_application::setup(0);
    apply(&pg, &context);
    let prefix = pg.metadata_command_replica_state().unwrap();
    let validated_revision = pg.metadata_digest_revision().unwrap();
    let other = PgStore::open(dir.path(), pg.pg_id).unwrap();
    // Owner-local second connection: inject a post-commit durable corruption,
    // before the recovering connection publishes its volatile clean marker.
    pg.test_after_complete_recovery_commit(move || {
        other
            .conn
            .execute(
                "UPDATE payload_staging_windows SET stored_bound = stored_bound + 1",
                [],
            )
            .unwrap();
    });
    assert_eq!(recover(&pg).unwrap(), prefix);
    assert!(pg.metadata_digest_revision().unwrap() > validated_revision);
    assert_eq!(
        pg.clean_metadata_digest_revision
            .load(std::sync::atomic::Ordering::Relaxed),
        validated_revision
    );
    let owner = fixture(StagingTarget::DirectPut);
    let next = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            prefix.cluster_epoch,
            context.id().pg_id(),
            MetadataCommandLogIndex::new(prefix.applied_log_index + 1).unwrap(),
        ),
        MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
            owner.subject.bucket,
            owner.subject.key,
            VersionId::from_u64(100),
        )),
    );
    assert!(matches!(
        pg.metadata_command_acceptance(0, &next),
        Err(StoreError::MetadataStateDigestMismatch { .. })
    ));
}
