// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Metadata-only tests of the whole-MPU abort/owner transaction. These use real
//! introduction, append and publication components, not physical shard cleanup.

use super::super::multipart::{apply_in_open_transaction, MultipartTerminalError as AbortError};
use super::*;
use crate::metadata_command::{
    AbortMultipartUploadCommand, MetadataCommandEnvelope, MetadataCommandPayload,
    ABORT_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
};
use crate::node_runtime::traits::PgMetadataStore;
use crate::pg_store::PgStore;
use crate::types::{AbortMultipartUploadCleanup, ListPartsReq};
use crate::{MetadataError, UploadState};

fn command(pg: &PgStore, owner: &StagingOwnerRecord) -> MetadataCommandEnvelope {
    let StagingTarget::UploadPart { upload_id, .. } = &owner.subject.target else {
        unreachable!()
    };
    let sessions = pg
        .list_stream_uploads_for_multipart_upload(upload_id)
        .unwrap();
    let mut reservation = proof(
        &owner.subject,
        ABORT_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
    );
    reservation.cluster_epoch = ClusterEpoch::new(7).unwrap();
    MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            reservation.cluster_epoch,
            owner.subject.object_pg,
            MetadataCommandLogIndex::new(5000).unwrap(),
        ),
        MetadataCommandPayload::AbortMultipartUpload(Box::new(AbortMultipartUploadCommand {
            bucket: owner.subject.bucket.clone(),
            key: owner.subject.key.clone(),
            upload_id: upload_id.clone(),
            cleanup: AbortMultipartUploadCleanup {
                upload: pg.get_multipart_upload(upload_id).unwrap(),
                parts: pg
                    .list_multipart_parts(&ListPartsReq {
                        upload_id: upload_id.clone(),
                        part_number_marker: None,
                        max_parts: u32::MAX,
                    })
                    .unwrap()
                    .parts,
                streaming_segments: pg
                    .get_all_multipart_part_segments_for_upload(upload_id)
                    .unwrap(),
                stream_upload_segments: pg.list_stream_segments_for_sessions(&sessions).unwrap(),
                stream_uploads: sessions.iter().map(Into::into).collect(),
            },
            bucket_write_reservation: reservation,
        })),
    )
}

fn payload(command: &MetadataCommandEnvelope) -> &AbortMultipartUploadCommand {
    let MetadataCommandPayload::AbortMultipartUpload(command) = command.payload() else {
        unreachable!()
    };
    command
}

fn apply(pg: &PgStore, command: &MetadataCommandEnvelope) -> Result<(), AbortError> {
    let decoded = crate::metadata_command::decode_metadata_command_envelope(
        &command.command_bytes(),
        &crate::node_runtime::MetadataCommandDecodeAuthority::new_for_test(),
    )
    .unwrap();
    apply_in_open_transaction(pg, &topology(), &decoded)
}

fn assert_aborted(pg: &PgStore, command: &MetadataCommandEnvelope, owners: &[StagingOwnerRecord]) {
    let command = payload(command);
    assert!(matches!(
        pg.get_multipart_upload(&command.upload_id),
        Err(MetadataError::NoSuchUpload { .. })
    ));
    assert!(pg
        .get_all_multipart_part_segments_for_upload(&command.upload_id)
        .unwrap()
        .is_empty());
    assert!(pg
        .list_stream_uploads_for_multipart_upload(&command.upload_id)
        .unwrap()
        .is_empty());
    let parts: u64 = pg
        .conn
        .query_row(
            "SELECT COUNT(*) FROM multipart_parts WHERE upload_id = ?1",
            [&command.upload_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(parts, 0);
    let upload_reservations: u64 = pg
        .conn
        .query_row(
            "SELECT COUNT(*) FROM object_generation_reservations WHERE reservation_id = ?1",
            [command.upload_id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(upload_reservations, 0);
    for before in owners {
        let mut expected = before.clone();
        match before.lifecycle {
            StagingLifecycle::Active => {
                expected.lifecycle = StagingLifecycle::Retiring(StagingRetirement::Unpublished);
                expected.references.clear();
                release_due(&mut expected.resources.generation);
            }
            StagingLifecycle::Published
            | StagingLifecycle::Retiring(StagingRetirement::PublishedRetained) => {
                expected.lifecycle =
                    StagingLifecycle::Retiring(StagingRetirement::PublishedUnreferenced);
                expected.references.clear();
            }
            _ => {}
        }
        assert_eq!(append::load(pg, before), expected);
        assert!(pg
            .list_stream_segments(&before.subject.id.attempt)
            .unwrap()
            .is_empty());
        assert!(matches!(
            pg.get_stream_upload(&before.subject.id.attempt),
            Err(MetadataError::StreamSessionNotFound { .. })
        ));
        assert!(matches!(
            pg.get_object_generation_reservation(
                &before.subject.bucket,
                &before.subject.key,
                &before.subject.id.attempt
            ),
            Err(MetadataError::ObjectGenerationReservationNotFound { .. })
        ));
    }
}

#[test]
fn staging_epoch_reset_multipart_abort_retains_all_owners() {
    for count in [0, 2] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(multipart_target()).subject.object_pg.get(),
        )
        .unwrap();
        let owners = completion::setup(&pg, count);
        let original = command(&pg, &owners[0]);
        let command = epoch_order::at_epoch_start(&pg, &original, original.id().cluster_epoch());
        apply(&pg, &command).unwrap();
        assert_aborted(&pg, &command, &owners);
        let pg = epoch_order::record_and_reopen(pg, dir.path(), &command);
        assert_aborted(&pg, &command, &owners);
    }
}

#[test]
fn staging_abort_retains_all_part_session_and_closed_obligations_through_restart() {
    for count in [0, 1, 2] {
        for retiring in [false, true] {
            for state in [UploadState::InProgress, UploadState::Aborting] {
                let dir = test_util::tempdir();
                let pg_id = fixture(multipart_target()).subject.object_pg.get();
                let pg = PgStore::open(dir.path(), pg_id).unwrap();
                let mut owners = completion::setup(&pg, count);
                if retiring {
                    pg.conn
                        .execute_batch(
                            "UPDATE payload_staging_owners SET lifecycle = 3 WHERE lifecycle = 1",
                        )
                        .unwrap();
                    owners = owners.iter().map(|o| append::load(&pg, o)).collect();
                }
                let initial = command(&pg, &owners[0]);
                pg.conn
                    .execute(
                        "UPDATE multipart_uploads SET state = ?1 WHERE upload_id = ?2",
                        rusqlite::params![state as u8, payload(&initial).upload_id],
                    )
                    .unwrap();
                let good = command(&pg, &owners[0]);
                pg.conn.execute_batch("COMMIT").unwrap();
                drop(pg);
                let pg = PgStore::open(dir.path(), pg_id).unwrap();
                assert_ne!(
                    pg.stream_allocation_issuer(owners[0].subject.issuer.node_id)
                        .unwrap(),
                    owners[0].subject.issuer
                );
                pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
                crate::clock::with_time_and_monotonic_override(9_000_000, 90_000_000, || {
                    apply(&pg, &good)
                })
                .unwrap();
                assert_aborted(&pg, &good, &owners);
                // Only the outer accepted-command boundary may certify replay.
                let before = introduction::state(&pg);
                let changes = pg.conn.total_changes();
                assert!(matches!(
                    apply(&pg, &good),
                    Err(AbortError::Metadata(MetadataError::NoSuchUpload { .. }))
                ));
                assert_eq!(pg.conn.total_changes(), changes);
                assert_eq!(introduction::state(&pg), before);
                pg.conn.execute_batch("COMMIT").unwrap();
                drop(pg);
                let pg = PgStore::open(dir.path(), pg_id).unwrap();
                pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
                assert_aborted(&pg, &good, &owners);
            }
        }
    }
}

#[test]
fn staging_abort_preserves_another_uploads_parts_and_active_attempts_on_the_same_key() {
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(multipart_target()).subject.object_pg.get(),
    )
    .unwrap();
    let owners = completion::setup(&pg, 2);
    let mut other = owners[0].clone();
    let StagingTarget::UploadPart { upload_id, .. } = &mut other.subject.target else {
        unreachable!()
    };
    *upload_id = crate::tests::multipart_upload_id("unrelated-abort");
    introduction::seed_upload(&pg, &mut other);
    let committed = completion::new_attempt(&pg, &other, 10, 3, 2);
    part::apply(&pg, &part::command(&pg, &committed)).unwrap();
    let committed = append::load(&pg, &committed);
    let active = completion::new_attempt(&pg, &other, 11, 3, 1);
    let other_snapshot = command(&pg, &other);
    assert_eq!(owners[0].subject.bucket, other.subject.bucket);
    assert_eq!(owners[0].subject.key, other.subject.key);
    let good = command(&pg, &owners[0]);
    apply(&pg, &good).unwrap();
    assert_aborted(&pg, &good, &owners);
    assert_eq!(command(&pg, &other), other_snapshot);
    assert_eq!(append::load(&pg, &committed), committed);
    assert_eq!(append::load(&pg, &active), active);
    assert_eq!(
        pg.get_object_generation_reservation(
            &active.subject.bucket,
            &active.subject.key,
            &active.subject.id.attempt,
        )
        .unwrap(),
        active.subject.payload_generation,
    );
    let upload = &payload(&other_snapshot).cleanup.upload;
    assert_eq!(
        pg.conn.query_row(
            "SELECT generation_id FROM object_generation_reservations WHERE reservation_id = ?1 AND bucket = ?2 AND key = ?3",
            rusqlite::params![upload.upload_id.as_str(), upload.bucket, upload.key],
            |row| row.get::<_, u64>(0),
        ).unwrap(),
        upload.object_generation_id.get(),
    );
}

#[test]
fn staging_abort_upload_without_part_attempts_requires_no_synthetic_owner() {
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(multipart_target()).subject.object_pg.get(),
    )
    .unwrap();
    let mut subject = fixture(multipart_target());
    introduction::seed_upload(&pg, &mut subject);
    pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    let good = command(&pg, &subject);
    apply(&pg, &good).unwrap();
    assert_aborted(&pg, &good, &[]);
    assert_eq!(
        pg.conn
            .query_row("SELECT COUNT(*) FROM payload_staging_owners", [], |row| row
                .get::<_, u64>(0))
            .unwrap(),
        0
    );
}

#[test]
fn staging_abort_rejects_crossed_command_proof_and_cleanup_before_mutation() {
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(multipart_target()).subject.object_pg.get(),
    )
    .unwrap();
    let owners = completion::setup(&pg, 2);
    let good = command(&pg, &owners[0]);
    for case in 0..19 {
        let mut bad = payload(&good).clone();
        match case {
            0 => bad.bucket = BucketName::try_from("other").unwrap(),
            1 => bad.key = ObjectKey::try_from("other").unwrap(),
            2 => {
                bad.bucket_write_reservation.operation_kind =
                    UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND.into()
            }
            3 => bad.bucket_write_reservation.cluster_epoch = ClusterEpoch::new(6).unwrap(),
            4 => bad.bucket_write_reservation.bucket_incarnation_generation += 1,
            5 => bad.bucket_write_reservation.target_context = Some("other".into()),
            6 => bad.bucket_write_reservation.bucket = BucketName::try_from("other").unwrap(),
            7 => {
                bad.cleanup.parts.pop();
            }
            8 => {
                bad.cleanup.streaming_segments.pop();
            }
            9 => {
                bad.cleanup.stream_uploads.pop();
            }
            10 => {
                bad.cleanup.stream_upload_segments.pop();
            }
            11 => bad.cleanup.parts[0].part_vid = GenerationId::new(99).unwrap(),
            12 => bad.cleanup.parts[0].placement_cluster_epoch = ClusterEpoch::new(7).unwrap(),
            13 => bad.cleanup.streaming_segments[0].segment_okh = [0x99; 16],
            14 => bad.cleanup.streaming_segments[0].data_pg_id = 99,
            15 => {
                bad.cleanup.stream_upload_segments[0].segment_vid = GenerationId::new(99).unwrap()
            }
            16 => bad.cleanup.upload.object_generation_id = GenerationId::new(99).unwrap(),
            17 => bad.cleanup.upload.initiated_at += 1,
            18 => {
                bad.bucket = BucketName::try_from("other").unwrap();
                bad.bucket_write_reservation.bucket = bad.bucket.clone();
            }
            _ => unreachable!(),
        }
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        let bad = MetadataCommandEnvelope::new(
            good.id(),
            MetadataCommandPayload::AbortMultipartUpload(Box::new(bad)),
        );
        assert!(apply(&pg, &bad).is_err(), "case {case}");
        assert_eq!(pg.conn.total_changes(), changes, "case {case}");
        assert_eq!(introduction::state(&pg), before, "case {case}");
    }
    apply(&pg, &good).unwrap();
    assert_aborted(&pg, &good, &owners);
}

#[test]
fn staging_abort_rebuilt_cleanup_cannot_hide_missing_or_crossed_owners() {
    for count in [0, 2] {
        for mutation in [
            "DELETE FROM payload_staging_owners WHERE part_number = 3 AND lifecycle = 1",
            "DELETE FROM payload_staging_owners WHERE part_number = 15 AND lifecycle = 0",
            "DELETE FROM multipart_part_segments WHERE part_number = 3; DELETE FROM multipart_parts WHERE part_number = 3",
            "DELETE FROM stream_uploads WHERE part_number = 15",
            "UPDATE multipart_parts SET placement_cluster_epoch = 7 WHERE part_number = 3",
            "UPDATE multipart_parts SET part_vid = 99 WHERE part_number = 11",
            "UPDATE multipart_part_segments SET segment_okh = zeroblob(16) WHERE part_number = 7",
            "UPDATE stream_uploads SET next_segment_vid = 99 WHERE part_number = 15",
            "DELETE FROM object_generation_reservations WHERE reservation_id IN (SELECT session_id FROM stream_uploads WHERE part_number = 15)",
            "UPDATE object_write_counters SET max_committed_generation = 1",
        ] {
            // Segment corruption requires a nonempty prefix.
            if count == 0 && mutation.contains("SET segment_okh") { continue; }
            let dir = test_util::tempdir();
            let pg = PgStore::open(dir.path(), fixture(multipart_target()).subject.object_pg.get()).unwrap();
            let owners = completion::setup(&pg, count);
            let good = command(&pg, &owners[0]);
            pg.conn.execute_batch("SAVEPOINT damaged_upload").unwrap();
            pg.conn.execute_batch(mutation).unwrap();
            let bad = command(&pg, &owners[0]);
            let before = introduction::state(&pg);
            let changes = pg.conn.total_changes();
            assert!(apply(&pg, &bad).is_err(), "{count}/{mutation}");
            assert_eq!(pg.conn.total_changes(), changes, "{count}/{mutation}");
            assert_eq!(introduction::state(&pg), before, "{count}/{mutation}");
            pg.conn.execute_batch("ROLLBACK TO damaged_upload; RELEASE damaged_upload").unwrap();
            apply(&pg, &good).unwrap();
            assert_aborted(&pg, &good, &owners);
        }
    }
}

#[test]
fn staging_abort_rejects_completing_upload_wrong_pg_and_missing_transaction() {
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(multipart_target()).subject.object_pg.get(),
    )
    .unwrap();
    let owners = completion::setup(&pg, 1);
    let good = command(&pg, &owners[0]);
    pg.conn.execute_batch("SAVEPOINT terminal_upload").unwrap();
    pg.conn
        .execute(
            "UPDATE multipart_uploads SET state = ?1 WHERE upload_id = ?2",
            rusqlite::params![UploadState::Completing as u8, payload(&good).upload_id],
        )
        .unwrap();
    let before = introduction::state(&pg);
    let changes = pg.conn.total_changes();
    assert!(matches!(
        apply(&pg, &command(&pg, &owners[0])),
        Err(AbortError::CommandBinding)
    ));
    assert_eq!(pg.conn.total_changes(), changes);
    assert_eq!(introduction::state(&pg), before);
    pg.conn
        .execute_batch("ROLLBACK TO terminal_upload; RELEASE terminal_upload")
        .unwrap();
    let before = introduction::state(&pg);
    let changes = pg.conn.total_changes();
    let crossed = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            good.id().cluster_epoch(),
            PgId::new((pg.pg_id + 1) % 8),
            good.id().log_index(),
        ),
        good.payload().clone(),
    );
    assert!(matches!(
        apply(&pg, &crossed),
        Err(AbortError::CommandBinding)
    ));
    assert_eq!(pg.conn.total_changes(), changes);
    assert_eq!(introduction::state(&pg), before);
    pg.conn.execute_batch("COMMIT").unwrap();
    assert!(matches!(
        apply(&pg, &good),
        Err(AbortError::TransactionRequired)
    ));
    pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    assert_eq!(introduction::state(&pg), before);
    apply(&pg, &good).unwrap();
    assert_aborted(&pg, &good, &owners);
}

#[test]
fn staging_abort_rolls_back_all_live_removal_and_owner_transfers() {
    for event in [
        "BEFORE DELETE ON stream_uploads",
        "BEFORE DELETE ON stream_upload_segments",
        "BEFORE DELETE ON multipart_part_segments",
        "BEFORE DELETE ON multipart_parts",
        "BEFORE DELETE ON object_generation_reservations",
        "BEFORE DELETE ON multipart_uploads",
        "BEFORE DELETE ON payload_staging_references WHEN OLD.part_number = 11",
        "BEFORE UPDATE ON payload_staging_owners WHEN NEW.lifecycle = 4",
        "BEFORE UPDATE ON payload_staging_owners WHEN NEW.lifecycle = 2",
    ] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(multipart_target()).subject.object_pg.get(),
        )
        .unwrap();
        let owners = completion::setup(&pg, 2);
        let good = command(&pg, &owners[0]);
        pg.conn
            .execute_batch(
                "CREATE TEMP TABLE outer_work(value INTEGER); INSERT INTO outer_work VALUES (42)",
            )
            .unwrap();
        pg.conn.execute_batch(&format!("CREATE TEMP TRIGGER fail_abort {event} BEGIN SELECT RAISE(ABORT, 'abort handoff failure'); END")).unwrap();
        let before = introduction::state(&pg);
        let error = apply(&pg, &good).unwrap_err();
        assert!(
            error.to_string().contains("abort handoff failure"),
            "{event}: {error}"
        );
        assert_eq!(introduction::state(&pg), before, "{event}");
        assert!(!pg.conn.is_autocommit());
        assert_eq!(
            pg.conn
                .query_row("SELECT value FROM outer_work", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            42
        );
        pg.conn.execute_batch("DROP TRIGGER fail_abort").unwrap();
        apply(&pg, &good).unwrap();
        assert_aborted(&pg, &good, &owners);
    }
}
