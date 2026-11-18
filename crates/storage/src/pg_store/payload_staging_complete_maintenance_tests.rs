// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Complete local maintenance, not startup convergence or live activation.
//! Metadata-only fixtures do not perform physical payload IO.

use super::*;
use crate::clock::test_time_override_guard;
use crate::metadata_command::{
    MetadataCommandEnvelope, MetadataCommandPayload, ReserveObjectVersionCommand,
};
use crate::node_runtime::pg_store::{
    command_log::{CompleteMaintenanceError, MetadataCommandLogCompactionStatus},
    PgStore,
};
use crate::{StoreError, VersionId};

fn apply(pg: &PgStore, context: &ContextualCommand) {
    pg.apply_complete_metadata_command_and_record(
        0,
        &context.encode_command().unwrap(),
        &topology(),
    )
    .unwrap();
}

pub(super) fn log_state(pg: &PgStore) -> Vec<Vec<String>> {
    [
        "SELECT printf('%lld:%lld:%lld:%lld:%lld:%s:%s:%s:%s', cluster_epoch, pg_id, log_index, command_checksum, abandoned, previous_log_hash, log_hash, pre_state_digest, post_state_digest) || ':' || hex(command_bytes) FROM metadata_command_log ORDER BY cluster_epoch, log_index",
        "SELECT printf('%lld:%lld:%lld:%lld:%lld:%lld:%lld', cluster_epoch, pg_id, log_index, command_checksum, abandoned, previous_log_hash, log_hash) || ':' || hex(command_sha256) FROM metadata_command_terminal_receipts ORDER BY cluster_epoch, log_index",
        "SELECT printf('%lld:%lld:%lld:%lld:%lld:%lld', cluster_epoch, pg_id, applied_log_index, applied_log_hash, state_digest, checkpoint_crc64) || ':' || hex(checkpoint_bytes) FROM metadata_command_checkpoints ORDER BY cluster_epoch, applied_log_index",
    ].into_iter().map(|sql| pg.conn.prepare(sql).unwrap().query_map([], |row| row.get(0)).unwrap().collect::<Result<Vec<_>, _>>().unwrap()).collect()
}

#[test]
fn staging_complete_maintenance_checkpoints_compacts_and_replays_all_contexts_after_reopen() {
    let _clock = test_time_override_guard(2_000);
    for case in 0..18 {
        let (dir, pg, context) = complete_application::setup(case);
        apply(&pg, &context);
        let materialized = introduction::state(&pg);
        let prefix = pg.metadata_command_replica_state().unwrap();
        let writes = pg.conn.total_changes();
        let checkpoint = pg
            .complete_metadata_command_checkpoint(0, &topology())
            .unwrap();
        assert_eq!(
            pg.conn.total_changes(),
            writes,
            "checkpoint capture wrote case {case}"
        );
        assert_eq!(
            checkpoint,
            pg.record_complete_metadata_command_checkpoint(0, &topology())
                .unwrap()
        );
        assert_eq!(
            pg.compact_complete_metadata_command_log(&topology())
                .unwrap(),
            MetadataCommandLogCompactionStatus::Compacted {
                deleted_entries: context.id().log_index().get(),
                compacted_before: context.id().log_index().get() + 1,
            }
        );
        assert_eq!(introduction::state(&pg), materialized);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        assert!(log_state(&pg)[0].is_empty());
        assert!(
            !log_state(&pg)[1].is_empty(),
            "no terminal receipts for case {case}"
        );
        let pg_id = pg.pg_id;
        drop(pg);
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        let writes = pg.conn.total_changes();
        assert_eq!(
            pg.complete_metadata_command_checkpoint(0, &topology())
                .unwrap(),
            checkpoint
        );
        apply(&pg, &context);
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), materialized);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
    }
}

fn require_pending_barrier(pg: &PgStore) {
    let before = introduction::state(pg);
    let pending = complete_pending::observe(pg);
    let log = log_state(pg);
    let prefix = pg.metadata_command_replica_state().unwrap();
    let writes = pg.conn.total_changes();
    for error in [
        pg.complete_metadata_command_checkpoint(0, &topology())
            .unwrap_err(),
        pg.record_complete_metadata_command_checkpoint(0, &topology())
            .unwrap_err(),
    ] {
        assert!(
            matches!(
                error,
                CompleteMaintenanceError::Store(StoreError::MetadataCommandContention { .. })
            ),
            "{error:?}"
        );
    }
    assert!(matches!(
        pg.compact_complete_metadata_command_log(&topology())
            .unwrap(),
        MetadataCommandLogCompactionStatus::PendingCommand { .. }
    ));
    assert_eq!(pg.conn.total_changes(), writes);
    assert_eq!(introduction::state(pg), before);
    assert_eq!(complete_pending::observe(pg), pending);
    assert_eq!(log_state(pg), log);
    assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
    assert!(pg.conn.is_autocommit());
}

#[test]
fn staging_complete_maintenance_preserves_unresolved_applied_and_abandoned_pending_custody() {
    let _clock = test_time_override_guard(2_000);
    for case in 0..18 {
        for terminal in ["unresolved", "applied", "abandoned"] {
            let (dir, pg, context) = complete_application::setup(case);
            let encoded = context.encode_command().unwrap();
            let retained = ContextualCommand::decode_command(
                encoded.bytes(),
                encoded.checksum_crc64(),
                &topology(),
            )
            .unwrap();
            let mut holder = context.into_submission();
            complete_pending::install(&mut holder, &pg).unwrap();
            match terminal {
                "applied" => apply(&pg, &retained),
                "abandoned" => {
                    pg.test_record_complete_command_abandoned(
                        0,
                        &retained.encode_command().unwrap(),
                    )
                    .unwrap();
                }
                _ => {}
            }
            require_pending_barrier(&pg);
            let pg_id = pg.pg_id;
            drop(pg);
            let pg = PgStore::open(dir.path(), pg_id).unwrap();
            require_pending_barrier(&pg);
        }
    }
}

#[test]
fn staging_complete_maintenance_preserves_older_epoch_pending_slot_without_checkpoint() {
    let _clock = test_time_override_guard(2_000);
    for case in [0, 5, 6] {
        let (dir, pg, context) = complete_application::setup(case);
        let id = context.id();
        let mut holder = context.into_submission();
        complete_pending::install(&mut holder, &pg).unwrap();
        let owner = fixture(StagingTarget::DirectPut);
        let successor = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(id.cluster_epoch().get() + 1).unwrap(),
                id.pg_id(),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
                owner.subject.bucket,
                owner.subject.key,
                VersionId::from_u64(100),
            )),
        );
        pg.record_metadata_command_abandoned(0, &successor).unwrap();
        assert_eq!(
            pg.metadata_command_replica_state().unwrap().cluster_epoch,
            successor.id().cluster_epoch()
        );
        assert!(log_state(&pg)[2].is_empty());
        require_pending_barrier(&pg);
        drop(pg);
        let pg = PgStore::open(dir.path(), id.pg_id().get()).unwrap();
        require_pending_barrier(&pg);
    }
}

#[test]
fn staging_complete_maintenance_rejects_damaged_pending_slots_without_mutation() {
    let _clock = test_time_override_guard(2_000);
    for damage in [
        "scope_bucket = NULL",
        "scope_bucket = 'foreign-bucket'",
        "command_checksum = command_checksum + 1",
        "command_bytes = x'0000'",
        "log_index = log_index + 1",
        "cluster_epoch = cluster_epoch + 1",
        "pg_id = pg_id + 1",
        "publication_started = 7",
    ] {
        let (_dir, pg, context) = complete_application::setup(0);
        let mut holder = context.into_submission();
        complete_pending::install(&mut holder, &pg).unwrap();
        pg.conn
            .execute_batch("PRAGMA ignore_check_constraints = ON")
            .unwrap();
        pg.conn
            .execute(
                &format!("UPDATE metadata_command_pending_slot SET {damage}"),
                [],
            )
            .unwrap();
        let pending = complete_pending::observe(&pg);
        let log = log_state(&pg);
        let before = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        assert!(
            pg.complete_metadata_command_checkpoint(0, &topology())
                .is_err(),
            "{damage}"
        );
        assert!(
            pg.record_complete_metadata_command_checkpoint(0, &topology())
                .is_err(),
            "{damage}"
        );
        assert!(
            pg.compact_complete_metadata_command_log(&topology())
                .is_err(),
            "{damage}"
        );
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(complete_pending::observe(&pg), pending);
        assert_eq!(log_state(&pg), log);
        assert_eq!(introduction::state(&pg), before);
        assert!(pg.conn.is_autocommit());
    }
}

#[test]
fn staging_complete_maintenance_rejects_damaged_prefix_without_recording_checkpoint() {
    let _clock = test_time_override_guard(2_000);
    for damage in [
        "UPDATE metadata_command_log SET command_checksum = command_checksum + 1 WHERE log_index = (SELECT max(log_index) FROM metadata_command_log)",
        "UPDATE metadata_command_log SET command_bytes = x'0000' WHERE log_index = (SELECT max(log_index) FROM metadata_command_log)",
        "UPDATE metadata_command_log SET log_hash = log_hash + 1 WHERE log_index = (SELECT max(log_index) FROM metadata_command_log)",
        "DELETE FROM object_write_counters",
        "UPDATE metadata_command_log SET log_index = log_index + 1 WHERE log_index = (SELECT max(log_index) FROM metadata_command_log)",
    ] {
        // A real introduction precedes this append in the same epoch, so the
        // checkpoint covers a predecessor, not the damaged suffix under test.
        let (_dir, pg, context) = complete_application::setup(3);
        let checkpoint = pg.record_complete_metadata_command_checkpoint(0, &topology()).unwrap();
        assert_eq!(checkpoint.cluster_epoch, context.id().cluster_epoch());
        assert!(checkpoint.applied_log_index < context.id().log_index().get());
        apply(&pg, &context);
        pg.conn.execute(damage, []).unwrap_or_else(|error| panic!("inject {damage}: {error}"));
        let log = log_state(&pg);
        let before = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        assert!(pg.complete_metadata_command_checkpoint(0, &topology()).is_err(), "{damage}");
        assert!(pg.record_complete_metadata_command_checkpoint(0, &topology()).is_err(), "{damage}");
        let compact = pg.compact_complete_metadata_command_log(&topology());
        assert!(compact.is_err() || matches!(compact, Ok(MetadataCommandLogCompactionStatus::PendingCommand { .. })), "{damage}: {compact:?}");
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(log_state(&pg), log);
        assert_eq!(introduction::state(&pg), before);
        assert!(pg.conn.is_autocommit());
    }
}

#[test]
fn staging_complete_maintenance_rolls_back_checkpoint_and_compaction_failures_then_retries() {
    let _clock = test_time_override_guard(2_000);
    for compact in [false, true] {
        for deferred in [false, true] {
            let (_dir, pg, context) = complete_application::setup(0);
            apply(&pg, &context);
            if compact {
                pg.record_complete_metadata_command_checkpoint(0, &topology())
                    .unwrap();
            }
            let event = if compact {
                "DELETE ON metadata_command_log"
            } else {
                "INSERT ON metadata_command_checkpoints"
            };
            let action = if deferred {
                pg.conn.execute_batch("CREATE TEMP TABLE deferred_failure (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES deferred_failure(id) DEFERRABLE INITIALLY DEFERRED)").unwrap();
                "INSERT INTO deferred_failure(parent) VALUES (0);"
            } else {
                "SELECT RAISE(ABORT, 'injected maintenance failure');"
            };
            pg.conn
                .execute_batch(&format!(
                    "CREATE TEMP TRIGGER fail_maintenance AFTER {event} BEGIN {action} END;"
                ))
                .unwrap();
            let log = log_state(&pg);
            let before = introduction::state(&pg);
            let prefix = pg.metadata_command_replica_state().unwrap();
            let error = if compact {
                pg.compact_complete_metadata_command_log(&topology())
                    .unwrap_err()
                    .to_string()
            } else {
                pg.record_complete_metadata_command_checkpoint(0, &topology())
                    .unwrap_err()
                    .to_string()
            };
            assert!(
                error.contains(if deferred {
                    "commit metadata command"
                } else {
                    "injected maintenance failure"
                }),
                "{error}"
            );
            assert_eq!(log_state(&pg), log);
            assert_eq!(introduction::state(&pg), before);
            assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
            assert!(pg.conn.is_autocommit());
            pg.require_usable_connection().unwrap();
            pg.conn
                .execute_batch("DROP TRIGGER fail_maintenance")
                .unwrap();
            pg.record_complete_metadata_command_checkpoint(0, &topology())
                .unwrap();
            assert!(matches!(
                pg.compact_complete_metadata_command_log(&topology())
                    .unwrap(),
                MetadataCommandLogCompactionStatus::Compacted { .. }
            ));
            apply(&pg, &context);
        }
    }
}

#[test]
fn staging_complete_maintenance_failed_rollback_retires_connection_until_reopen() {
    let _clock = test_time_override_guard(2_000);
    for compact in [false, true] {
        let (dir, pg, context) = complete_application::setup(0);
        apply(&pg, &context);
        if compact {
            pg.record_complete_metadata_command_checkpoint(0, &topology())
                .unwrap();
        }
        let event = if compact {
            "DELETE ON metadata_command_log"
        } else {
            "INSERT ON metadata_command_checkpoints"
        };
        pg.conn.execute_batch("CREATE TEMP TABLE deferred_failure (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES deferred_failure(id) DEFERRABLE INITIALLY DEFERRED)").unwrap();
        pg.conn.execute_batch(&format!("CREATE TEMP TRIGGER fail_maintenance AFTER {event} BEGIN INSERT INTO deferred_failure(parent) VALUES (0); END;")).unwrap();
        let log = log_state(&pg);
        let before = introduction::state(&pg);
        let prefix = pg.metadata_command_replica_state().unwrap();
        pg.test_fail_metadata_command_rollbacks(1);
        if compact {
            assert!(pg
                .compact_complete_metadata_command_log(&topology())
                .is_err());
        } else {
            assert!(pg
                .record_complete_metadata_command_checkpoint(0, &topology())
                .is_err());
        }
        assert!(
            !pg.conn.is_autocommit(),
            "rollback injection did not leave an open transaction"
        );
        assert_ne!(
            log_state(&pg),
            log,
            "failure did not follow maintenance writes"
        );
        let error = pg.require_usable_connection().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unconfirmed metadata transaction"),
            "{error}"
        );
        assert!(pg
            .complete_metadata_command_checkpoint(0, &topology())
            .is_err());
        assert!(pg
            .record_complete_metadata_command_checkpoint(0, &topology())
            .is_err());
        assert!(pg
            .compact_complete_metadata_command_log(&topology())
            .is_err());
        let pg_id = pg.pg_id;
        drop(pg);
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        pg.require_usable_connection().unwrap();
        assert_eq!(log_state(&pg), log);
        assert_eq!(introduction::state(&pg), before);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        pg.record_complete_metadata_command_checkpoint(0, &topology())
            .unwrap();
        assert!(matches!(
            pg.compact_complete_metadata_command_log(&topology())
                .unwrap(),
            MetadataCommandLogCompactionStatus::Compacted { .. }
        ));
        apply(&pg, &context);
    }
}

#[test]
fn staging_complete_maintenance_rejects_foreign_topology_and_caller_transaction() {
    let _clock = test_time_override_guard(2_000);
    let (_dir, pg, context) = complete_application::setup(0);
    apply(&pg, &context);
    assert!(matches!(
        pg.compact_complete_metadata_command_log(&topology())
            .unwrap(),
        MetadataCommandLogCompactionStatus::NoCheckpoint { .. }
    ));
    let writes = pg.conn.total_changes();
    let before = introduction::state(&pg);
    let log = log_state(&pg);
    for foreign in [
        PgTopology::new(&[99]).unwrap(),
        PgTopology::new(&(0..16).collect::<Vec<_>>()).unwrap(),
    ] {
        assert!(pg
            .complete_metadata_command_checkpoint(0, &foreign)
            .is_err());
        assert!(pg
            .record_complete_metadata_command_checkpoint(0, &foreign)
            .is_err());
    }
    pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    assert!(pg
        .complete_metadata_command_checkpoint(0, &topology())
        .is_err());
    assert!(pg
        .record_complete_metadata_command_checkpoint(0, &topology())
        .is_err());
    assert!(pg
        .compact_complete_metadata_command_log(&topology())
        .is_err());
    assert!(!pg.conn.is_autocommit());
    pg.conn.execute_batch("ROLLBACK").unwrap();
    pg.require_usable_connection().unwrap();
    assert_eq!(pg.conn.total_changes(), writes);
    assert_eq!(introduction::state(&pg), before);
    assert_eq!(log_state(&pg), log);
}
