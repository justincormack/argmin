// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Metadata-only standard-object -> MPU overwrite transactions. Production
//! dispatch, physical cleanup and accepted-command replay are separate gates.

use super::super::multipart::MultipartTerminalError;
use super::super::object::ObjectError;
use super::*;
use crate::metadata_command::{
    MetadataCommandEnvelope, MetadataCommandPayload, ObjectPayloadReclaimCommand,
};
use crate::node_runtime::traits::PgMetadataStore;
use crate::pg_store::PgStore;
use crate::types::{ObjectSegmentsReclaimRecord, ObjectSegmentsReclaimSegmentRecord};
use crate::{MetadataError, StoredObject, VersionId};

pub(super) fn incoming_upload(
    pg: &PgStore,
    old: &StagingOwnerRecord,
    count: usize,
) -> Vec<StagingOwnerRecord> {
    let mut template = old.clone();
    template.subject.target = StagingTarget::UploadPart {
        upload_id: crate::tests::multipart_upload_id("overwrite-mpu"),
        upload_generation: GenerationId::new(1).unwrap(),
        part_number: 3,
    };
    introduction::seed_upload(pg, &mut template);
    let mut owners = Vec::new();
    for (index, number) in [(10, 3), (11, 7), (12, 11)] {
        let owner = completion::new_attempt(pg, &template, index, number, count);
        part::apply(pg, &part::command(pg, &owner)).unwrap();
        owners.push(append::load(pg, &owner));
    }
    owners.push(completion::new_attempt(pg, &template, 13, 15, 1));
    owners
}

fn current_reclaim(pg: &PgStore, old: &StagingOwnerRecord) -> ObjectSegmentsReclaimRecord {
    let subject = &old.subject;
    let StoredObject::Live(live) = pg
        .get_object_version(&subject.bucket, &subject.key, VersionId::Null)
        .unwrap()
    else {
        unreachable!()
    };
    ObjectSegmentsReclaimRecord {
        bucket: subject.bucket.clone(),
        key: subject.key.clone(),
        generation_id: live.generation_id,
        created_at: 4_000,
        segments: pg
            .get_object_segments(&subject.bucket, &subject.key, VersionId::Null)
            .unwrap()
            .iter()
            .map(|segment| ObjectSegmentsReclaimSegmentRecord {
                segment_index: segment.segment_index,
                segment_okh: segment.segment_okh,
                segment_vid: segment.segment_vid,
                data_pg_id: segment.data_pg_id,
                ec: EcShape {
                    k: segment.ec_k,
                    m: segment.ec_m,
                },
            })
            .collect(),
    }
}

pub(super) fn command(
    pg: &PgStore,
    incoming: &[StagingOwnerRecord],
    version: VersionId,
    reclaim: Option<ObjectPayloadReclaimCommand>,
) -> MetadataCommandEnvelope {
    let base = completion::command(pg, &incoming[0], version);
    let mut payload = completion::payload(&base).clone();
    payload.stale_payload = reclaim;
    MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            base.id().cluster_epoch(),
            base.id().pg_id(),
            MetadataCommandLogIndex::new(5000).unwrap(),
        ),
        MetadataCommandPayload::CommitMultipartObject(Box::new(payload)),
    )
}

fn assert_replaced(
    pg: &PgStore,
    command: &MetadataCommandEnvelope,
    old: &StagingOwnerRecord,
    incoming: &[StagingOwnerRecord],
) {
    completion::assert_complete(pg, command, incoming);
    assert_eq!(append::load(pg, old), object_overwrite::retained(old));
    let Some(ObjectPayloadReclaimCommand::Segments(reclaim)) =
        &completion::payload(command).stale_payload
    else {
        unreachable!()
    };
    assert_eq!(
        pg.get_object_segments_reclaim(
            &old.subject.bucket,
            &old.subject.key,
            old.subject.payload_generation
        )
        .unwrap(),
        Some(reclaim.clone())
    );
    assert!(pg
        .get_object_segments(&old.subject.bucket, &old.subject.key, VersionId::Null)
        .unwrap()
        .is_empty());
}

#[test]
fn staging_completion_overwrite_retains_standard_owner_and_publishes_parts_atomically_through_restart(
) {
    for (target, old_count) in [
        (StagingTarget::DirectPut, 1),
        (StagingTarget::StreamPut, 0),
        (StagingTarget::StreamPut, 2),
    ] {
        for count in [0, 2] {
            for retiring in [false, true] {
                let dir = test_util::tempdir();
                let pg_id = fixture(target.clone()).subject.object_pg.get();
                let pg = PgStore::open(dir.path(), pg_id).unwrap();
                let old = object::initialize(&pg, target.clone(), old_count);
                object::apply(&pg, &object::command(&pg, &old, VersionId::Null)).unwrap();
                if retiring {
                    pg.conn
                        .execute_batch(
                            "UPDATE payload_staging_owners SET lifecycle = 3 WHERE lifecycle = 1",
                        )
                        .unwrap();
                }
                let old = append::load(&pg, &old);
                let incoming = incoming_upload(&pg, &old, count);
                let good = command(
                    &pg,
                    &incoming,
                    VersionId::Null,
                    Some(ObjectPayloadReclaimCommand::Segments(current_reclaim(
                        &pg, &old,
                    ))),
                );
                pg.conn.execute_batch("COMMIT").unwrap();
                drop(pg);
                let pg = PgStore::open(dir.path(), pg_id).unwrap();
                assert_ne!(
                    pg.stream_allocation_issuer(old.subject.issuer.node_id)
                        .unwrap(),
                    old.subject.issuer
                );
                pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
                crate::clock::with_time_and_monotonic_override(9_000_000, 90_000_000, || {
                    completion::apply(&pg, &good)
                })
                .unwrap();
                assert_replaced(&pg, &good, &old, &incoming);
                let before = introduction::state(&pg);
                let changes = pg.conn.total_changes();
                assert!(matches!(
                    completion::apply(&pg, &good),
                    Err(MultipartTerminalError::Metadata(
                        MetadataError::NoSuchUpload { .. }
                    ))
                ));
                assert_eq!(pg.conn.total_changes(), changes);
                assert_eq!(introduction::state(&pg), before);
                pg.conn.execute_batch("COMMIT").unwrap();
                drop(pg);
                let pg = PgStore::open(dir.path(), pg_id).unwrap();
                pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
                assert_replaced(&pg, &good, &old, &incoming);
            }
        }
    }
}

#[test]
fn staging_completion_numbered_publication_does_not_reclaim_the_null_version() {
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(StagingTarget::StreamPut).subject.object_pg.get(),
    )
    .unwrap();
    let old = object::initialize(&pg, StagingTarget::StreamPut, 2);
    let first = object::command(&pg, &old, VersionId::Null);
    object::apply(&pg, &first).unwrap();
    let old = append::load(&pg, &old);
    let incoming = incoming_upload(&pg, &old, 2);
    let good = command(&pg, &incoming, VersionId::from_u64(3), None);
    completion::apply(&pg, &good).unwrap();
    completion::assert_complete(&pg, &good, &incoming);
    object::assert_object(&pg, &first);
    assert_eq!(append::load(&pg, &old), old);
    assert!(pg
        .get_object_segments_reclaim(
            &old.subject.bucket,
            &old.subject.key,
            old.subject.payload_generation
        )
        .unwrap()
        .is_none());
}

#[test]
fn staging_completion_overwrite_rejects_missing_and_forged_reclaim_before_mutation() {
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(StagingTarget::StreamPut).subject.object_pg.get(),
    )
    .unwrap();
    let old = object::initialize(&pg, StagingTarget::StreamPut, 2);
    object::apply(&pg, &object::command(&pg, &old, VersionId::Null)).unwrap();
    let old = append::load(&pg, &old);
    let incoming = incoming_upload(&pg, &old, 2);
    let original = current_reclaim(&pg, &old);
    for case in 0..12 {
        let mut crossed = original.clone();
        match case {
            0 => crossed.bucket = crate::tests::bucket_name("crossed"),
            1 => crossed.key = crate::tests::object_key("crossed"),
            2 => crossed.generation_id = GenerationId::new(99).unwrap(),
            3 => crossed.segments.clear(),
            4 => {
                crossed.segments.pop();
            }
            5 => crossed.segments.push(crossed.segments[0].clone()),
            6 => crossed.segments.swap(0, 1),
            7 => crossed.segments[0].segment_okh[0] ^= 1,
            8 => crossed.segments[0].segment_vid = GenerationId::new(99).unwrap(),
            9 => crossed.segments[0].data_pg_id ^= 1,
            10 => crossed.segments[0].ec.k += 1,
            11 => crossed.segments[0].ec.m += 1,
            _ => unreachable!(),
        }
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        let error = completion::apply(
            &pg,
            &command(
                &pg,
                &incoming,
                VersionId::Null,
                Some(ObjectPayloadReclaimCommand::Segments(crossed)),
            ),
        )
        .unwrap_err();
        assert!(
            matches!(
                error,
                MultipartTerminalError::Displacement(ObjectError::Metadata(
                    MetadataError::InvariantViolation {
                        context: "staging displaced object reclaim binding",
                        ..
                    }
                ))
            ),
            "{case}: {error}"
        );
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);
    }
    let before = introduction::state(&pg);
    let changes = pg.conn.total_changes();
    assert!(matches!(
        completion::apply(&pg, &command(&pg, &incoming, VersionId::Null, None)),
        Err(MultipartTerminalError::DisplacedPayload)
    ));
    assert_eq!(pg.conn.total_changes(), changes);
    assert_eq!(introduction::state(&pg), before);
    let good = command(
        &pg,
        &incoming,
        VersionId::Null,
        Some(ObjectPayloadReclaimCommand::Segments(original)),
    );
    completion::apply(&pg, &good).unwrap();
    assert_replaced(&pg, &good, &old, &incoming);
}

#[test]
fn staging_completion_overwrite_requires_unique_bound_owner_even_for_empty_standard_objects() {
    for count in [0, 2] {
        for missing in [false, true] {
            let dir = test_util::tempdir();
            let pg = PgStore::open(
                dir.path(),
                fixture(StagingTarget::StreamPut).subject.object_pg.get(),
            )
            .unwrap();
            let old = object::initialize(&pg, StagingTarget::StreamPut, count);
            object::apply(&pg, &object::command(&pg, &old, VersionId::Null)).unwrap();
            let old = append::load(&pg, &old);
            let incoming = incoming_upload(&pg, &old, 2);
            let good = command(
                &pg,
                &incoming,
                VersionId::Null,
                Some(ObjectPayloadReclaimCommand::Segments(current_reclaim(
                    &pg, &old,
                ))),
            );
            pg.conn.execute_batch("SAVEPOINT crossed_owner").unwrap();
            if missing {
                pg.conn
                    .execute(
                        "DELETE FROM payload_staging_owners WHERE attempt = ?1",
                        [&old.subject.id.attempt],
                    )
                    .unwrap();
            } else {
                let mut duplicate = old.clone();
                duplicate.subject.id.attempt = crate::tests::stream_session_id("duplicate-old");
                for window in &mut duplicate.windows {
                    window.owner = duplicate.subject.id.clone();
                    window.segment_hash = duplicate.subject.segment_hash(window.index);
                }
                for reference in &mut duplicate.references {
                    reference.segment_hash = duplicate.subject.segment_hash(reference.window_index);
                }
                if let ResourceResponsibility::ReleaseDue(generation) =
                    &mut duplicate.resources.generation
                {
                    generation.reservation_id = duplicate.subject.id.attempt.clone();
                }
                super::super::sql::insert(
                    &pg.conn,
                    PgId::new(pg.pg_id),
                    &topology(),
                    &duplicate.validate(&topology()).unwrap(),
                )
                .unwrap();
            }
            let before = introduction::state(&pg);
            let changes = pg.conn.total_changes();
            assert!(matches!(
                completion::apply(&pg, &good),
                Err(MultipartTerminalError::Displacement(
                    ObjectError::Persistence(super::super::sql::PersistenceError::ObjectOwner)
                ))
            ));
            assert_eq!(pg.conn.total_changes(), changes);
            assert_eq!(introduction::state(&pg), before);
            pg.conn
                .execute_batch("ROLLBACK TO crossed_owner; RELEASE crossed_owner")
                .unwrap();
            completion::apply(&pg, &good).unwrap();
            assert_replaced(&pg, &good, &old, &incoming);
        }
    }
}

#[test]
fn staging_completion_overwrite_rebuilt_reclaim_cannot_replace_original_owner_provenance() {
    for mutation in [
        "UPDATE object_segments SET segment_okh = zeroblob(16) WHERE segment_index = 0",
        "UPDATE object_segments SET segment_vid = 99 WHERE segment_index = 0",
        "UPDATE object_segments SET data_pg_id = (data_pg_id + 1) % 8 WHERE segment_index = 0",
        "UPDATE object_segments SET placement_cluster_epoch = placement_cluster_epoch + 1 WHERE segment_index = 0",
        "UPDATE object_segments SET ec_k = ec_k + 1 WHERE segment_index = 0",
        "UPDATE object_segments SET size = size + 1 WHERE segment_index = 0",
        "UPDATE objects SET size = size + 1",
        "UPDATE objects SET ec_k = ec_k + 1",
        "UPDATE payload_staging_resources SET incarnation = incarnation + 1 WHERE attempt IN (SELECT attempt FROM payload_staging_owners WHERE target = 1); UPDATE payload_staging_owners SET incarnation = incarnation + 1 WHERE target = 1",
    ] {
        let dir = test_util::tempdir(); let pg = PgStore::open(dir.path(), fixture(StagingTarget::StreamPut).subject.object_pg.get()).unwrap();
        let old = object::initialize(&pg, StagingTarget::StreamPut, 2);
        object::apply(&pg, &object::command(&pg, &old, VersionId::Null)).unwrap();
        let old = append::load(&pg, &old); let incoming = incoming_upload(&pg, &old, 2);
        let good = command(&pg, &incoming, VersionId::Null, Some(ObjectPayloadReclaimCommand::Segments(current_reclaim(&pg, &old))));
        pg.conn.execute_batch("SAVEPOINT crossed_payload").unwrap(); pg.conn.execute_batch(mutation).unwrap();
        let bad = command(&pg, &incoming, VersionId::Null, Some(ObjectPayloadReclaimCommand::Segments(current_reclaim(&pg, &old))));
        let before = introduction::state(&pg); let changes = pg.conn.total_changes();
        assert!(completion::apply(&pg, &bad).is_err(), "{mutation}");
        assert_eq!(pg.conn.total_changes(), changes); assert_eq!(introduction::state(&pg), before);
        pg.conn.execute_batch("ROLLBACK TO crossed_payload; RELEASE crossed_payload").unwrap();
        completion::apply(&pg, &good).unwrap(); assert_replaced(&pg, &good, &old, &incoming);
    }
}

#[test]
fn staging_completion_overwrite_rolls_back_old_reclaim_live_object_and_all_new_owners() {
    for event in [
        "BEFORE INSERT ON object_segments_reclaims",
        "BEFORE INSERT ON object_segment_reclaim_segments",
        "BEFORE DELETE ON object_segments",
        "BEFORE INSERT ON object_parts",
        "BEFORE DELETE ON multipart_uploads",
        "BEFORE UPDATE ON payload_staging_references WHEN NEW.destination = 2",
        "BEFORE UPDATE ON payload_staging_owners WHEN NEW.lifecycle = 4",
        "BEFORE UPDATE ON payload_staging_references WHEN NEW.destination = 3",
        "BEFORE UPDATE ON payload_staging_owners WHEN NEW.lifecycle = 3",
    ] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(StagingTarget::StreamPut).subject.object_pg.get(),
        )
        .unwrap();
        let old = object::initialize(&pg, StagingTarget::StreamPut, 2);
        object::apply(&pg, &object::command(&pg, &old, VersionId::Null)).unwrap();
        let old = append::load(&pg, &old);
        let incoming = incoming_upload(&pg, &old, 2);
        let good = command(
            &pg,
            &incoming,
            VersionId::Null,
            Some(ObjectPayloadReclaimCommand::Segments(current_reclaim(
                &pg, &old,
            ))),
        );
        pg.conn
            .execute_batch(
                "CREATE TEMP TABLE outer_work(value INTEGER); INSERT INTO outer_work VALUES (42)",
            )
            .unwrap();
        pg.conn.execute_batch(&format!("CREATE TEMP TRIGGER fail_overwrite {event} BEGIN SELECT RAISE(ABORT, 'completion overwrite handoff failure'); END")).unwrap();
        let before = introduction::state(&pg);
        let error = completion::apply(&pg, &good).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("completion overwrite handoff failure"),
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
        pg.conn
            .execute_batch("DROP TRIGGER fail_overwrite")
            .unwrap();
        completion::apply(&pg, &good).unwrap();
        assert_replaced(&pg, &good, &old, &incoming);
    }
}

#[test]
fn staging_completion_still_rejects_numbered_source_replacement() {
    for multipart in [false, true] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(StagingTarget::StreamPut).subject.object_pg.get(),
        )
        .unwrap();
        let (old, version, reclaim) = if multipart {
            let old_parts = completion::setup(&pg, 1);
            let first = completion::command(&pg, &old_parts[0], VersionId::from_u64(3));
            completion::apply(&pg, &first).unwrap();
            let payload = completion::payload(&first);
            let reclaim = crate::types::MultipartReclaimRecord::from_object_parts(
                &payload.object.bucket,
                &payload.object.key,
                payload.object.generation_id,
                4_000,
                &payload.parts,
                &payload.selected_streaming_segments,
            );
            (
                old_parts.last().unwrap().clone(),
                VersionId::from_u64(3),
                ObjectPayloadReclaimCommand::Multipart(reclaim),
            )
        } else {
            let old = object::initialize(&pg, StagingTarget::StreamPut, 2);
            let first = object::command(&pg, &old, VersionId::from_u64(3));
            object::apply(&pg, &first).unwrap();
            (
                append::load(&pg, &old),
                VersionId::from_u64(3),
                ObjectPayloadReclaimCommand::Segments(object_overwrite::reclaim(&first)),
            )
        };
        let incoming = incoming_upload(&pg, &old, 2);
        let bad = command(&pg, &incoming, version, Some(reclaim));
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        assert!(matches!(
            completion::apply(&pg, &bad),
            Err(MultipartTerminalError::Displacement(
                ObjectError::DisplacedPayload
            ))
        ));
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);
    }
}
