// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Metadata-only exact retained-custody diagnostics. No physical deletion or
//! slot/owner retirement is permitted by these observations.

use super::super::abandonment::source::PartFinalizationSource;
use super::super::command_context::ContextualCommand;
use super::*;
use crate::metadata_command::{MetadataCommandEnvelope, MetadataCommandPayload};
use crate::node_runtime::pg_store::PgStore;

struct Fixture {
    dir: test_util::TempDir,
    pg: PgStore,
    owner: StagingOwnerRecord,
    source: MetadataCommandEnvelope,
    cleanup: MetadataCommandEnvelope,
    context: ContextualCommand,
}

fn context(
    source: &MetadataCommandEnvelope,
    cleanup: &MetadataCommandEnvelope,
) -> ContextualCommand {
    ContextualCommand::abandoned_part(
        PartFinalizationSource::capture_for_test(source)
            .unwrap()
            .bind_cleanup(cleanup.clone(), &topology())
            .unwrap(),
        &topology(),
    )
    .unwrap()
}

fn setup(count: usize, published: bool) -> Fixture {
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(multipart_target()).subject.object_pg.get(),
    )
    .unwrap();
    let owner = part::initialize(&pg, count);
    pg.conn
        .execute_batch(
            "CREATE TEMP TABLE saved_session AS SELECT * FROM stream_uploads;
        CREATE TEMP TABLE saved_segments AS SELECT * FROM stream_upload_segments",
        )
        .unwrap();
    pg.conn
        .execute(
            "CREATE TEMP TABLE saved_generation AS SELECT * FROM object_generation_reservations
             WHERE reservation_id = ?1",
            [&owner.subject.id.attempt],
        )
        .unwrap();
    let source = part::command(&pg, &owner);
    let mut cleanup = abandonment::cleanup(&pg, &owner, &source);
    abandonment::terminal(&pg, &source, true);
    pg.conn.execute_batch("COMMIT").unwrap();
    if published {
        let MetadataCommandPayload::CommitStreamPart(mut winner) = source.payload().clone() else {
            unreachable!()
        };
        winner.bucket_write_reservation.reservation_id = "winning-finalizer".to_owned();
        let winner = MetadataCommandEnvelope::new(
            cleanup.id(),
            MetadataCommandPayload::CommitStreamPart(winner),
        );
        let ordinary = abandonment::source_context(&winner);
        pg.apply_complete_metadata_command_and_record(
            0,
            &ordinary.encode_command().unwrap(),
            &topology(),
        )
        .unwrap();
        let MetadataCommandPayload::AbortStreamUpload(mut abort) = cleanup.payload().clone() else {
            unreachable!()
        };
        abort.staged_segments.clear();
        cleanup = MetadataCommandEnvelope::new(
            crate::metadata_command::MetadataCommandId::new(
                cleanup.id().cluster_epoch(),
                cleanup.id().pg_id(),
                crate::metadata_command::MetadataCommandLogIndex::new(
                    cleanup.id().log_index().get() + 1,
                )
                .unwrap(),
            ),
            MetadataCommandPayload::AbortStreamUpload(abort),
        );
    }
    let context = context(&source, &cleanup);
    let error = context.inspect_abandoned_part_custody(&pg, 0).unwrap_err();
    assert!(
        error.to_string().contains("exact applied cleanup"),
        "{error}"
    );
    pg.apply_complete_metadata_command_and_record(
        0,
        &context.encode_command().unwrap(),
        &topology(),
    )
    .unwrap();
    Fixture {
        dir,
        pg,
        owner,
        source,
        cleanup,
        context,
    }
}

fn observe(
    pg: &PgStore,
) -> (
    Vec<crate::node_runtime::pg_store::command_log::MetadataCheckpointTableBlock>,
    u64,
) {
    (introduction::state(pg), pg.conn.total_changes())
}

#[test]
fn staging_terminal_custody_preserves_closed_and_published_owners_through_restart_and_compaction() {
    for published in [false, true] {
        for count in [0, 2] {
            let Fixture {
                dir,
                mut pg,
                owner,
                source,
                context,
                ..
            } = setup(count, published);
            for restart in [false, true] {
                if restart {
                    pg.record_complete_metadata_command_checkpoint(0, &topology())
                        .unwrap();
                    pg.compact_complete_metadata_command_log(&topology())
                        .unwrap();
                    assert_eq!(
                        pg.metadata_command_log_stats(source.id().cluster_epoch())
                            .unwrap()
                            .retained_entries,
                        0
                    );
                    drop(pg);
                    pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
                }
                let before = observe(&pg);
                let prefix = pg.metadata_command_replica_state().unwrap();
                let result = context.inspect_abandoned_part_custody(&pg, 0).unwrap();
                assert_eq!(result.retained_windows, count + 1);
                assert_eq!(
                    result.selected_references,
                    if published { count } else { 0 }
                );
                assert_eq!(result.additional_finalizers, usize::from(published));
                assert_eq!(observe(&pg), before);
                assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
                assert!(pg.conn.is_autocommit());
            }
        }
    }
}

#[test]
fn staging_terminal_custody_rejects_damaged_retention_without_repair() {
    for sql in [
        "DELETE FROM payload_staging_owners",
        "DELETE FROM payload_staging_resources WHERE role = 0",
        "DELETE FROM payload_staging_resources WHERE role = 1",
        "UPDATE payload_staging_resources SET owner_token = 'another-owner' WHERE role = 1",
        "UPDATE payload_staging_resources SET reservation_id = 'another-finalizer' WHERE role = 1",
        "UPDATE payload_staging_resources SET release_pg = release_pg + 1 WHERE role = 1",
        "DELETE FROM object_write_counters",
        "UPDATE object_write_counters SET max_committed_generation = 1",
        "DELETE FROM payload_staging_windows WHERE window_index = 2",
        "UPDATE payload_staging_windows SET segment_hash = zeroblob(16) WHERE window_index = 0",
        "UPDATE payload_staging_owners SET payload_generation = payload_generation + 1",
        "UPDATE payload_staging_owners SET part_number = part_number + 1",
        "INSERT INTO stream_uploads SELECT * FROM saved_session",
        // Owner-local corruption fixture: isolate orphan segment detection
        // from live-session detection despite the normal foreign key.
        "PRAGMA foreign_keys=OFF; INSERT INTO stream_upload_segments SELECT * FROM saved_segments; PRAGMA foreign_keys=ON",
        "INSERT INTO object_generation_reservations SELECT * FROM saved_generation",
    ] {
        let Fixture {
            dir: _dir,
            pg,
            context,
            ..
        } = setup(2, false);
        pg.conn.execute_batch(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
        let before = observe(&pg);
        let prefix = pg.metadata_command_replica_state().unwrap();
        assert!(
            context.inspect_abandoned_part_custody(&pg, 0).is_err(),
            "accepted {sql}"
        );
        assert_eq!(observe(&pg), before, "inspection repaired {sql}");
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        assert!(pg.conn.is_autocommit());
    }
}

#[test]
fn staging_terminal_custody_empty_unpublished_cleanup_rejects_extra_window() {
    let Fixture {
        dir,
        mut pg,
        owner,
        cleanup,
        context,
        ..
    } = setup(0, false);
    let MetadataCommandPayload::AbortStreamUpload(abort) = cleanup.payload() else {
        unreachable!()
    };
    assert!(abort.staged_segments.is_empty());
    let original = context.inspect_abandoned_part_custody(&pg, 0).unwrap();
    assert_eq!(original.retained_windows, 1);
    assert_eq!(original.selected_references, 0);

    // Owner-local corruption: add a structurally coherent window without any
    // authenticated append/cleanup manifest authorizing that retained prefix.
    // Preserve the exact owner and every resource, and update both allocator
    // summaries so an unrelated structural check cannot mask the count gap.
    let topology = topology();
    pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    let mut damaged = append::load(&pg, &owner);
    assert_eq!(
        damaged.lifecycle,
        StagingLifecycle::Retiring(StagingRetirement::Unpublished)
    );
    let mut extra = damaged.windows[0].clone();
    extra.index = 1;
    extra.segment_hash = damaged.subject.segment_hash(extra.index);
    extra.start = damaged.reserved_end;
    extra.end = GenerationId::new(extra.start.get() + 1).unwrap();
    extra.issued_at_ms += 1;
    extra.cutoff_ms += 1;
    extra.participants.data_pg = PgId::new(damaged.subject.data_pg(&topology, extra.index));
    damaged.reserved_end = extra.end;
    damaged.maximum_cutoff_ms = extra.cutoff_ms;
    damaged.windows.push(extra);
    let checked = damaged.validate(&topology).unwrap();
    pg.conn
        .execute(
            "DELETE FROM payload_staging_owners WHERE attempt = ?1",
            [&owner.subject.id.attempt],
        )
        .unwrap();
    sql::insert(&pg, owner.subject.object_pg, &topology, &checked).unwrap();
    pg.conn.execute_batch("COMMIT").unwrap();

    for restart in [false, true] {
        if restart {
            drop(pg);
            pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
        }
        let snapshot = pg.command_build_snapshot().unwrap();
        let loaded = append::load(&pg, &owner);
        assert_eq!(loaded, damaged);
        loaded.validate(&topology).unwrap();
        snapshot.finish().unwrap();
        let before = observe(&pg);
        let prefix = pg.metadata_command_replica_state().unwrap();
        let error = context
            .inspect_abandoned_part_custody(&pg, 0)
            .expect_err("empty unpublished cleanup accepted an extra retained window");
        assert!(
            error
                .to_string()
                .contains("conflicting terminal responsibility"),
            "{error}"
        );
        assert_eq!(observe(&pg), before, "inspection must not repair the owner");
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        assert!(pg.conn.is_autocommit());
    }
}

#[test]
fn staging_terminal_custody_binds_published_payload_to_authenticated_source_manifest() {
    for (sql, structurally_valid) in [
        (
            "UPDATE payload_staging_windows SET segment_hash = zeroblob(16) WHERE window_index = 0;
             UPDATE payload_staging_references SET segment_hash = zeroblob(16) WHERE window_index = 0",
            false,
        ),
        (
            "UPDATE payload_staging_references SET logical_size = logical_size + 1,
             stored_size = stored_size + 1 WHERE window_index = 0",
            true,
        ),
        (
            "UPDATE payload_staging_references SET logical_size = logical_size + 1,
             stored_size = stored_size + 1 WHERE window_index = 1",
            true,
        ),
        (
            "UPDATE payload_staging_windows SET placement_epoch = placement_epoch + 1
             WHERE window_index = 0",
            true,
        ),
        (
            "DELETE FROM payload_staging_windows WHERE window_index = 2;
             DELETE FROM payload_staging_references WHERE window_index = 1;
             UPDATE payload_staging_owners SET
               reserved_end = (SELECT end_vid FROM payload_staging_windows WHERE window_index = 1),
               maximum_cutoff_ms = (SELECT MAX(cutoff_ms) FROM payload_staging_windows)",
            true,
        ),
    ] {
        let Fixture { dir: _dir, pg, owner, cleanup, context, .. } = setup(2, true);
        let MetadataCommandPayload::AbortStreamUpload(abort) = cleanup.payload() else {
            unreachable!()
        };
        assert!(abort.staged_segments.is_empty(), "published cleanup is proof-only");
        assert_eq!(context.inspect_abandoned_part_custody(&pg, 0).unwrap().selected_references, 2);
        pg.conn.execute_batch(sql).unwrap();
        let snapshot = pg.command_build_snapshot().unwrap();
        let valid = sql::load(&pg.conn, owner.subject.object_pg, &owner.subject.id.attempt)
            .is_ok_and(|retained| retained.is_some_and(|record| record.validate(&topology()).is_ok()));
        assert_eq!(valid, structurally_valid, "{sql}");
        snapshot.finish().unwrap();
        let before = observe(&pg);
        let prefix = pg.metadata_command_replica_state().unwrap();
        let error = context.inspect_abandoned_part_custody(&pg, 0)
            .unwrap_err();
        if structurally_valid {
            assert!(error.to_string().contains("authenticated source manifest"), "{sql}: {error}");
        }
        assert_eq!(observe(&pg), before, "inspection repaired {sql}");
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        assert!(pg.conn.is_autocommit());
    }
}

#[test]
fn staging_terminal_custody_preserves_completed_object_and_reclaim_destinations() {
    for count in [0, 2] {
        let Fixture {
            dir: _dir,
            pg,
            owner,
            context,
            ..
        } = setup(count, true);
        for reclaim in [false, true] {
            let raw = if reclaim {
                delete::command(
                    &pg,
                    &owner,
                    crate::VersionId::Null,
                    crate::metadata_command::DeleteObjectVersionMode::Specific,
                )
            } else {
                completion::command(&pg, &owner, crate::VersionId::Null)
            };
            // These component fixtures choose later epochs; start each exact
            // complete-command prefix at one and retain the original receipts.
            let command = MetadataCommandEnvelope::new(
                MetadataCommandId::new(
                    raw.id().cluster_epoch(),
                    raw.id().pg_id(),
                    MetadataCommandLogIndex::new(1).unwrap(),
                ),
                raw.payload().clone(),
            );
            let transition = abandonment::source_context(&command);
            pg.apply_complete_metadata_command_and_record(
                0,
                &transition.encode_command().unwrap(),
                &topology(),
            )
            .unwrap();
            let snapshot = pg.command_build_snapshot().unwrap();
            let retained = append::load(&pg, &owner);
            assert_eq!(retained.references.len(), count);
            for reference in &retained.references {
                assert!(if reclaim {
                    matches!(reference.destination, StagingDestination::Reclaim { .. })
                } else {
                    matches!(reference.destination, StagingDestination::Object { .. })
                });
            }
            snapshot.finish().unwrap();
            let before = observe(&pg);
            let observation = context.inspect_abandoned_part_custody(&pg, 0).unwrap();
            assert_eq!(observation.retained_windows, count + 1);
            assert_eq!(observation.selected_references, count);
            assert_eq!(observation.additional_finalizers, 1);
            assert_eq!(observe(&pg), before);
            assert!(pg.conn.is_autocommit());
        }
    }
}

#[test]
fn staging_terminal_custody_rejects_crossed_complete_context_and_caller_transaction() {
    let Fixture {
        dir: _dir,
        pg,
        source,
        cleanup,
        context: original,
        ..
    } = setup(2, false);
    let MetadataCommandPayload::CommitStreamPart(mut crossed) = source.payload().clone() else {
        unreachable!()
    };
    // Keep the exact reservation and owner subject. Only authenticated complete
    // evidence, not downstream owner/proof matching, can reject this change.
    crossed.part.etag[0] ^= 1;
    let crossed = MetadataCommandEnvelope::new(
        source.id(),
        MetadataCommandPayload::CommitStreamPart(crossed),
    );
    let changed = context(&crossed, &cleanup);
    let before = observe(&pg);
    assert!(changed.inspect_abandoned_part_custody(&pg, 0).is_err());
    let MetadataCommandPayload::AbortStreamUpload(mut crossed_cleanup) = cleanup.payload().clone()
    else {
        unreachable!()
    };
    crossed_cleanup.staged_segments[0].segment_crc64 ^= 1;
    let crossed_cleanup = MetadataCommandEnvelope::new(
        cleanup.id(),
        MetadataCommandPayload::AbortStreamUpload(crossed_cleanup),
    );
    assert!(context(&source, &crossed_cleanup)
        .inspect_abandoned_part_custody(&pg, 0)
        .is_err());
    let ordinary = ContextualCommand::decode_field(cleanup, &[1, 0, 0], &topology()).unwrap();
    assert!(ordinary
        .inspect_abandoned_part_custody(&pg, 0)
        .unwrap_err()
        .to_string()
        .contains("source-bearing"));
    assert_eq!(observe(&pg), before);
    pg.conn.execute_batch("BEGIN").unwrap();
    assert!(original
        .inspect_abandoned_part_custody(&pg, 0)
        .unwrap_err()
        .to_string()
        .contains("own snapshot"));
    assert!(
        !pg.conn.is_autocommit(),
        "must not roll back caller transaction"
    );
    pg.conn.execute_batch("ROLLBACK").unwrap();
    let foreign_dir = test_util::tempdir();
    let foreign = PgStore::open(foreign_dir.path(), source.id().pg_id().get() ^ 1).unwrap();
    let foreign_before = observe(&foreign);
    assert!(original
        .inspect_abandoned_part_custody(&foreign, 0)
        .is_err());
    assert_eq!(observe(&foreign), foreign_before);
    assert!(foreign.conn.is_autocommit());
    assert_eq!(observe(&pg), before);
}

#[test]
fn staging_terminal_custody_failed_snapshot_teardown_retires_connection_until_reopen() {
    for validation_error in [false, true] {
        let Fixture {
            dir,
            pg,
            source,
            cleanup,
            context: original,
            ..
        } = setup(2, false);
        let MetadataCommandPayload::CommitStreamPart(mut crossed) = source.payload().clone() else {
            unreachable!()
        };
        crossed.part.etag[0] ^= 1;
        let crossed = MetadataCommandEnvelope::new(
            source.id(),
            MetadataCommandPayload::CommitStreamPart(crossed),
        );
        let changed = context(&crossed, &cleanup);
        let before = introduction::state(&pg);
        let prefix = pg.metadata_command_replica_state().unwrap();
        let writes = pg.conn.total_changes();
        pg.fail_next_snapshot_rollback.set(true);
        let error = if validation_error {
            &changed
        } else {
            &original
        }
        .inspect_abandoned_part_custody(&pg, 0)
        .unwrap_err();
        if !validation_error {
            assert!(
                error
                    .to_string()
                    .contains("injected read-snapshot rollback failure"),
                "{error}"
            );
        }
        assert!(
            !pg.conn.is_autocommit(),
            "must not hide teardown failure with a retry"
        );
        assert!(pg.require_usable_connection().is_err());
        assert!(original.inspect_abandoned_part_custody(&pg, 0).is_err());
        assert!(pg.command_build_snapshot().is_err());
        assert_eq!(pg.conn.total_changes(), writes);
        let pg_id = pg.pg_id;
        drop(pg);
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        pg.require_usable_connection().unwrap();
        assert_eq!(introduction::state(&pg), before);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        assert_eq!(
            original
                .inspect_abandoned_part_custody(&pg, 0)
                .unwrap()
                .retained_windows,
            3
        );
        assert!(pg.conn.is_autocommit());
    }
}
