// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Owner-local, metadata-only startup observations. No serving admission,
//! acting-set convergence, physical effects or pending custody release.

use super::*;
use crate::clock::test_time_override_guard;
use crate::metadata_command::{
    MetadataCommandEnvelope, MetadataCommandPayload, ReserveObjectVersionCommand,
};
use crate::node_runtime::pg_store::{
    command_log::CompletePendingInspectionError, MetadataCommandStartupDisposition as Terminal,
    PgStore,
};
use crate::{StoreError, VersionId};

fn duplicate(context: &ContextualCommand) -> ContextualCommand {
    let encoded = context.encode_command().unwrap();
    ContextualCommand::decode_command(encoded.bytes(), encoded.checksum_crc64(), &topology())
        .unwrap()
}

fn install(pg: &PgStore, context: &ContextualCommand) {
    complete_pending::install(&mut duplicate(context).into_submission(), pg).unwrap();
}

fn apply(pg: &PgStore, context: &ContextualCommand) {
    pg.apply_complete_metadata_command_and_record(
        0,
        &context.encode_command().unwrap(),
        &topology(),
    )
    .unwrap();
}

fn check(pg: &PgStore, id: MetadataCommandId, expected: &str, started: bool) {
    let pending = complete_pending::observe(pg);
    let before = introduction::state(pg);
    let prefix = pg.metadata_command_replica_state().unwrap();
    let writes = pg.conn.total_changes();
    let result = pg
        .inspect_complete_pending_command(0, &topology())
        .unwrap()
        .unwrap();
    assert_eq!(result.id, id);
    assert_eq!(result.stored_epoch, prefix.cluster_epoch);
    assert_eq!(result.publication_started, started);
    assert!(
        matches!(
            (expected, result.terminal),
            ("unresolved", Terminal::Absent)
                | ("applied", Terminal::Applied { .. })
                | ("abandoned", Terminal::Abandoned { .. })
        ),
        "{expected}: {result:?}"
    );
    assert_eq!(pg.conn.total_changes(), writes);
    assert_eq!(complete_pending::observe(pg), pending);
    assert_eq!(introduction::state(pg), before);
    assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
    assert!(pg.conn.is_autocommit());
}

fn reject(pg: &PgStore) -> CompletePendingInspectionError {
    let pending = complete_pending::observe(pg);
    let before = introduction::state(pg);
    let prefix = pg.metadata_command_replica_state().unwrap();
    let writes = pg.conn.total_changes();
    let error = pg
        .inspect_complete_pending_command(0, &topology())
        .unwrap_err();
    assert_eq!(pg.conn.total_changes(), writes);
    assert_eq!(complete_pending::observe(pg), pending);
    assert_eq!(introduction::state(pg), before);
    assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
    assert!(pg.conn.is_autocommit());
    assert!(!pg
        .conn
        .pragma_query_value(None, "query_only", |row| row.get::<_, bool>(0))
        .unwrap());
    error
}

#[test]
fn staging_complete_inspection_retains_all_contexts_before_and_after_terminal_reopen() {
    let _clock = test_time_override_guard(2_000);
    for case in 0..18 {
        for terminal in ["applied", "abandoned"] {
            let (dir, pg, context) = complete_application::setup(case);
            assert!(pg
                .inspect_complete_pending_command(0, &topology())
                .unwrap()
                .is_none());
            install(&pg, &context);
            check(&pg, context.id(), "unresolved", false);
            pg.conn
                .execute(
                    "UPDATE metadata_command_pending_slot SET publication_started = 1",
                    [],
                )
                .unwrap();
            check(&pg, context.id(), "unresolved", true);
            if terminal == "applied" {
                apply(&pg, &context);
            } else {
                pg.test_record_complete_command_abandoned(0, &context.encode_command().unwrap())
                    .unwrap();
            }
            check(&pg, context.id(), terminal, true);
            let pg_id = pg.pg_id;
            drop(pg);
            let pg = PgStore::open(dir.path(), pg_id).unwrap();
            check(&pg, context.id(), terminal, true);
        }
    }
}

#[test]
fn staging_complete_inspection_abort_uses_retained_geometry_not_a_removed_session() {
    let _clock = test_time_override_guard(2_000);
    for case in [8, 9] {
        let (dir, pg, context) = complete_application::setup(case);
        install(&pg, &context);
        let pages = complete_pending::observe(&pg);
        assert!(!pages[3].is_empty(), "abort must have placed payload pages");
        let sessions: u64 = pg
            .conn
            .query_row("SELECT count(*) FROM stream_uploads", [], |row| row.get(0))
            .unwrap();
        assert!(sessions > 0);
        apply(&pg, &context);
        let sessions: u64 = pg
            .conn
            .query_row("SELECT count(*) FROM stream_uploads", [], |row| row.get(0))
            .unwrap();
        assert_eq!(sessions, 0);
        check(&pg, context.id(), "applied", false);
        // Fresh installation still demands its live inputs. Observation is
        // not a second install entry or a session-reconstruction fallback.
        assert!(
            complete_pending::install(&mut duplicate(&context).into_submission(), &pg).is_err()
        );
        assert_eq!(complete_pending::observe(&pg), pages);
        let pg_id = pg.pg_id;
        drop(pg);
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        check(&pg, context.id(), "applied", false);
    }
}

#[test]
fn staging_complete_inspection_rejects_every_damaged_index_before_and_after_abort() {
    let _clock = test_time_override_guard(2_000);
    for applied in [false, true] {
        for damage in [
            "UPDATE metadata_command_pending_slot SET scope_bucket = NULL",
            "UPDATE metadata_command_pending_slot SET scope_bucket = 'crossed-bucket'",
            "DELETE FROM metadata_command_pending_route_dependencies",
            "UPDATE metadata_command_pending_route_dependencies SET dependency_checksum = dependency_checksum + 1",
            "DELETE FROM metadata_command_pending_staging_protection",
            "UPDATE metadata_command_pending_staging_protection SET protection_checksum = protection_checksum + 1",
            "UPDATE metadata_command_pending_slot SET placed_segment_reference_count = placed_segment_reference_count + 1",
            "DELETE FROM metadata_command_pending_placed_reference_pages",
            "UPDATE metadata_command_pending_placed_reference_pages SET encoded_references = zeroblob(length(encoded_references))",
            "INSERT INTO metadata_command_pending_placed_reference_pages SELECT singleton, 99, reference_count, encoded_references FROM metadata_command_pending_placed_reference_pages",
            "INSERT INTO metadata_command_pending_reclaim_reference_pages VALUES (0, 0, 1, zeroblob(30))",
        ] {
            let (_dir, pg, context) = complete_application::setup(8);
            install(&pg, &context);
            if applied { apply(&pg, &context); }
            check(&pg, context.id(), if applied { "applied" } else { "unresolved" }, false);
            pg.conn.execute_batch(damage).unwrap();
            let error = reject(&pg);
            assert!(matches!(error, CompletePendingInspectionError::Store(_)), "{damage}: {error:?}");
        }
    }
}

#[test]
fn staging_complete_inspection_retained_windows_and_generation_fence_are_required() {
    let _clock = test_time_override_guard(2_000);
    for case in [3, 4, 8, 9] {
        for damage in [
            "DELETE FROM payload_staging_owners",
            "DELETE FROM object_write_counters",
            "UPDATE object_write_counters SET max_committed_generation = (SELECT nullif(min(payload_generation) - 1, 0) FROM payload_staging_owners WHERE bucket = object_write_counters.bucket AND key = object_write_counters.key)",
            "UPDATE payload_staging_windows SET stored_bound = stored_bound + 1",
            "UPDATE payload_staging_windows SET segment_hash = zeroblob(16)",
        ] {
            let (_dir, pg, context) = complete_application::setup(case);
            install(&pg, &context);
            apply(&pg, &context);
            check(&pg, context.id(), "applied", false);
            pg.conn.execute_batch(damage).unwrap();
            if damage.starts_with("UPDATE object_write_counters") {
                let protected: bool = pg.conn.query_row("SELECT EXISTS(SELECT 1 FROM payload_staging_owners o JOIN object_write_counters c ON c.bucket = o.bucket AND c.key = o.key WHERE c.max_committed_generation >= o.payload_generation)", [], |row| row.get(0)).unwrap();
                assert!(!protected, "counter fault did not invalidate the owner's fence");
            }
            let error = reject(&pg);
            assert!(
                matches!(
                    error,
                    CompletePendingInspectionError::Store(
                        StoreError::ShardScavengerScanIncomplete { .. }
                    )
                ),
                "{damage}: {error:?}"
            );
        }
    }
}

#[test]
fn staging_complete_inspection_preserves_older_epoch_unresolved_and_terminal_custody() {
    let _clock = test_time_override_guard(2_000);
    for case in [0, 3, 6, 8] {
        for terminal in ["unresolved", "applied", "abandoned"] {
            let (dir, pg, context) = complete_application::setup(case);
            install(&pg, &context);
            match terminal {
                "applied" => apply(&pg, &context),
                "abandoned" => {
                    pg.test_record_complete_command_abandoned(
                        0,
                        &context.encode_command().unwrap(),
                    )
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
            assert!(
                pg.metadata_command_replica_state().unwrap().cluster_epoch
                    > context.id().cluster_epoch()
            );
            check(&pg, context.id(), terminal, false);
            let pg_id = pg.pg_id;
            drop(pg);
            let pg = PgStore::open(dir.path(), pg_id).unwrap();
            check(&pg, context.id(), terminal, false);
        }
    }
}

#[test]
fn staging_complete_inspection_requires_exact_terminal_and_original_abandonment_source() {
    let _clock = test_time_override_guard(2_000);
    for case in [0, 6, 8] {
        for damage in [
            "command_checksum = command_checksum + 1",
            "log_hash = log_hash + 1",
            "command_bytes = x'0000'",
        ] {
            let (_dir, pg, context) = complete_application::setup(case);
            install(&pg, &context);
            apply(&pg, &context);
            check(&pg, context.id(), "applied", false);
            // Source-bearing cleanup also needs its original tombstone, not
            // just the locally applied cleanup. Corrupt all records here.
            pg.conn
                .execute(&format!("UPDATE metadata_command_log SET {damage}"), [])
                .unwrap();
            reject(&pg);
        }
    }
    let (_dir, pg, context) = complete_application::setup(6);
    install(&pg, &context);
    apply(&pg, &context);
    pg.conn
        .execute("DELETE FROM metadata_command_log WHERE abandoned = 1", [])
        .unwrap();
    reject(&pg);
}

#[test]
fn staging_complete_inspection_reads_slot_and_indexes_in_one_snapshot() {
    let _clock = test_time_override_guard(2_000);
    let (dir, pg, context) = complete_application::setup(8);
    install(&pg, &context);
    apply(&pg, &context);
    let other = PgStore::open(dir.path(), pg.pg_id).unwrap();
    let writes = pg.conn.total_changes();
    pg.test_after_pending_slot_row_read(move || {
        other
            .conn
            .execute(
                "DELETE FROM metadata_command_pending_placed_reference_pages",
                [],
            )
            .unwrap();
    });
    let observation = pg
        .inspect_complete_pending_command(0, &topology())
        .unwrap()
        .unwrap();
    assert!(matches!(observation.terminal, Terminal::Applied { .. }));
    assert_eq!(pg.conn.total_changes(), writes);
    assert!(
        complete_pending::observe(&pg)[3].is_empty(),
        "concurrent deletion did not run"
    );
    reject(&pg);
}

#[test]
fn staging_complete_inspection_rejects_wrong_topology_and_does_not_adopt_transactions() {
    let _clock = test_time_override_guard(2_000);
    let (_dir, pg, context) = complete_application::setup(3);
    install(&pg, &context);
    let before = complete_pending::observe(&pg);
    let writes = pg.conn.total_changes();
    for foreign in [
        PgTopology::new(&[99]).unwrap(),
        PgTopology::new(&(0..16).collect::<Vec<_>>()).unwrap(),
    ] {
        assert!(pg.inspect_complete_pending_command(0, &foreign).is_err());
    }
    pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    assert!(matches!(
        pg.inspect_complete_pending_command(0, &topology()),
        Err(CompletePendingInspectionError::Snapshot(_))
    ));
    assert!(!pg.conn.is_autocommit());
    pg.conn.execute_batch("ROLLBACK").unwrap();
    assert_eq!(pg.conn.total_changes(), writes);
    assert_eq!(complete_pending::observe(&pg), before);
    check(&pg, context.id(), "unresolved", false);
}

#[test]
fn staging_complete_inspection_failed_snapshot_rollback_blocks_metadata_until_reopen() {
    let _clock = test_time_override_guard(2_000);
    for exit in ["finish", "error", "unwind"] {
        let (dir, pg, context) = complete_application::setup(8);
        install(&pg, &context);
        apply(&pg, &context);
        if exit == "error" {
            pg.conn
                .execute(
                    "UPDATE metadata_command_pending_slot SET scope_bucket = NULL",
                    [],
                )
                .unwrap();
        }
        let before = introduction::state(&pg);
        let pending = complete_pending::observe(&pg);
        let prefix = pg.metadata_command_replica_state().unwrap();
        let writes = pg.conn.total_changes();
        if exit == "unwind" {
            pg.test_after_pending_slot_row_read(|| panic!("inspection teardown unwind"));
        }
        pg.fail_next_snapshot_rollback.set(true);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            pg.inspect_complete_pending_command(0, &topology())
        }));
        match exit {
            "finish" => {
                let error = result.unwrap().unwrap_err();
                assert!(matches!(error, CompletePendingInspectionError::Snapshot(_)));
                assert!(error
                    .to_string()
                    .contains("injected read-snapshot rollback failure"));
            }
            "error" => assert!(matches!(
                result.unwrap(),
                Err(CompletePendingInspectionError::Store(
                    StoreError::RouteCapabilitySubjectMismatch {
                        operation: "pending-staging-install-existing-scope"
                    }
                ))
            )),
            "unwind" => assert!(result.is_err()),
            _ => unreachable!(),
        }
        assert!(
            !pg.conn.is_autocommit(),
            "failed rollback was hidden by a retry"
        );
        let error = pg.require_usable_connection().unwrap_err();
        assert!(error
            .to_string()
            .contains("unconfirmed metadata transaction"));
        assert!(pg.inspect_complete_pending_command(0, &topology()).is_err());
        assert!(pg
            .complete_metadata_command_checkpoint(0, &topology())
            .is_err());
        assert!(pg.command_build_snapshot().is_err());
        assert_eq!(pg.conn.total_changes(), writes);
        let pg_id = pg.pg_id;
        drop(pg);
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        pg.require_usable_connection().unwrap();
        assert_eq!(introduction::state(&pg), before);
        assert_eq!(complete_pending::observe(&pg), pending);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        pg.command_build_snapshot().unwrap().finish().unwrap();
        if exit == "error" {
            reject(&pg); // Corrupted scope persists, but the connection is usable.
            pg.require_usable_connection().unwrap();
        } else {
            check(&pg, context.id(), "applied", false);
        }
    }
}
