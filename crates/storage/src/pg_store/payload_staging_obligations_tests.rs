// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Independently authenticated finalization obligations on one closed attempt.
//! Metadata-only component schedules, not physical-release receipts.

use super::*;

fn next_source(
    pg: &PgStore,
    original: &MetadataCommandEnvelope,
    suffix: &str,
    renewal: u64,
) -> MetadataCommandEnvelope {
    let mut command = payload(original).clone();
    command
        .bucket_write_reservation
        .reservation_id
        .push_str(suffix);
    command
        .bucket_write_reservation
        .owner_token
        .push_str(suffix);
    command.bucket_write_reservation.lease_deadline += renewal;
    let state = pg.metadata_command_replica_state().unwrap();
    MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            state.cluster_epoch,
            original.id().pg_id(),
            MetadataCommandLogIndex::new(state.applied_log_index + 1).unwrap(),
        ),
        MetadataCommandPayload::CommitStreamPart(Box::new(command)),
    )
}

fn follow_up(
    source: &MetadataCommandEnvelope,
    original: &MetadataCommandEnvelope,
) -> MetadataCommandEnvelope {
    let MetadataCommandPayload::AbortStreamUpload(mut abort) = original.payload().clone() else {
        unreachable!()
    };
    abort.staged_segments.clear();
    MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            source.id().cluster_epoch(),
            source.id().pg_id(),
            MetadataCommandLogIndex::new(source.id().log_index().get() + 1).unwrap(),
        ),
        MetadataCommandPayload::AbortStreamUpload(abort),
    )
}

#[test]
fn staging_obligations_distinct_sources_survive_compaction_restart_and_renewed_replay() {
    for count in [0, 2] {
        let dir = test_util::tempdir();
        let pg_id = fixture(multipart_target()).subject.object_pg.get();
        let mut pg = PgStore::open(dir.path(), pg_id).unwrap();
        let owner = part::initialize(&pg, count);
        let source = part::command(&pg, &owner);
        let cleanup = cleanup(&pg, &owner, &source);
        terminal(&pg, &source, true);
        assert_eq!(
            apply(&pg, &source, &cleanup).unwrap(),
            AbandonmentOutcome::ClosedAndRetained
        );
        pg.record_metadata_command_applied_inner(0, &cleanup)
            .unwrap();
        let primary = append::load(&pg, &owner);
        let mut retained = primary.clone();
        for (suffix, renewal) in [("-z", 0), ("-a", 0), ("-z", 1_000)] {
            let source = next_source(&pg, &source, suffix, renewal);
            let follow_up = follow_up(&source, &cleanup);
            let before = introduction::state(&pg);
            // Valid CRC/SHA and a matching owner still do not authenticate an
            // unrecorded source, even if its proof is already retained.
            let changes = pg.conn.total_changes();
            assert!(matches!(
                apply(&pg, &source, &follow_up),
                Err(AbandonmentError::NotAbandoned)
            ));
            assert_eq!(pg.conn.total_changes(), changes);
            assert_eq!(introduction::state(&pg), before);
            pg.conn.execute_batch("COMMIT").unwrap();
            pg.record_metadata_command_abandoned(0, &source).unwrap();
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            let outcome = apply(&pg, &source, &follow_up).unwrap();
            if renewal == 0 {
                assert_eq!(outcome, AbandonmentOutcome::RetainedOnClosedOwner);
                retained
                    .resources
                    .abandoned_finalizations
                    .push(StagingBucketResource::capture(
                        &payload(&source).bucket_write_reservation,
                        PgId::new(topology().bucket_pg_for(&owner.subject.bucket)),
                    ));
            } else {
                assert_eq!(outcome, AbandonmentOutcome::AlreadyRetained);
                assert_eq!(
                    introduction::state(&pg),
                    before,
                    "renewal must not rewrite retained evidence"
                );
            }
            assert_eq!(append::load(&pg, &owner), retained);
            assert_eq!(
                retained.resources.finalization,
                primary.resources.finalization
            );
            pg.record_metadata_command_applied_inner(0, &follow_up)
                .unwrap();
            pg.conn.execute_batch("COMMIT").unwrap();
            let checkpoint = pg
                .record_current_metadata_command_checkpoint(0, source.id().cluster_epoch())
                .unwrap();
            checkpoint.verify().unwrap();
            pg.compact_metadata_command_log_with_receipt_retention(source.id().cluster_epoch(), 2)
                .unwrap();
            drop(pg);
            pg = PgStore::open(dir.path(), pg_id).unwrap();
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            assert_eq!(append::load(&pg, &owner), retained);
            assert_eq!(
                pg.cached_metadata_state_digest().unwrap(),
                pg.metadata_state_digest().unwrap()
            );
            let changes = pg.conn.total_changes();
            assert_eq!(
                apply(&pg, &source, &follow_up).unwrap(),
                AbandonmentOutcome::AlreadyRetained
            );
            assert_eq!(pg.conn.total_changes(), changes);
        }
        let roles = pg
            .conn
            .prepare("SELECT role FROM payload_staging_resources ORDER BY role")
            .unwrap()
            .query_map([], |row| row.get::<_, u32>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(roles, [0, 1, 2, 3]);
        assert_eq!(retained.windows, primary.windows);
        assert_eq!(retained.references, primary.references);
    }
}

#[test]
fn staging_obligations_failed_insert_preserves_earlier_proofs_and_outer_transaction() {
    for trigger_time in ["BEFORE", "AFTER"] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(multipart_target()).subject.object_pg.get(),
        )
        .unwrap();
        let owner = part::initialize(&pg, 2);
        let first = part::command(&pg, &owner);
        let cleanup = cleanup(&pg, &owner, &first);
        terminal(&pg, &first, true);
        apply(&pg, &first, &cleanup).unwrap();
        pg.record_metadata_command_applied_inner(0, &cleanup)
            .unwrap();
        let second = next_source(&pg, &first, "-second", 0);
        let follow_up = follow_up(&second, &cleanup);
        pg.conn.execute_batch("COMMIT").unwrap();
        pg.record_metadata_command_abandoned(0, &second).unwrap();
        pg.conn.execute_batch(&format!("BEGIN IMMEDIATE;
            CREATE TEMP TABLE outer_work(value INTEGER); INSERT INTO outer_work VALUES (42);
            CREATE TEMP TRIGGER fail_extra {trigger_time} INSERT ON payload_staging_resources WHEN NEW.role = 2
            BEGIN SELECT RAISE(FAIL, 'injected additional proof failure'); END;")).unwrap();
        let before = introduction::state(&pg);
        let error = apply(&pg, &second, &follow_up).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("injected additional proof failure"),
            "{error}"
        );
        assert_eq!(introduction::state(&pg), before);
        assert_eq!(
            pg.cached_metadata_state_digest().unwrap(),
            pg.metadata_state_digest().unwrap()
        );
        assert!(!pg.conn.is_autocommit());
        assert_eq!(
            pg.conn
                .query_row("SELECT value FROM outer_work", [], |row| row
                    .get::<_, u64>(0))
                .unwrap(),
            42
        );
        pg.conn.execute_batch("DROP TRIGGER fail_extra").unwrap();
        assert_eq!(
            apply(&pg, &second, &follow_up).unwrap(),
            AbandonmentOutcome::RetainedOnClosedOwner
        );
        assert_eq!(
            append::load(&pg, &owner)
                .resources
                .abandoned_finalizations
                .len(),
            1
        );
    }
}
