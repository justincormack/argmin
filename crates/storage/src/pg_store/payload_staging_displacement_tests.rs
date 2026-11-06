// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Metadata-only completed multipart -> PUT/MPU ownership transactions.
//! No physical shard or payload-decryption coverage is claimed here.

use super::*;
use crate::metadata_command::{
    MetadataCommandEnvelope, MetadataCommandPayload, ObjectPayloadReclaimCommand,
};
use crate::node_runtime::traits::PgMetadataStore;
use crate::pg_store::PgStore;
use crate::types::MultipartReclaimRecord;
use crate::{StoredObject, VersionId};

fn source(pg: &PgStore, count: usize) -> Vec<StagingOwnerRecord> {
    let owners = completion::setup(pg, count);
    completion::apply(pg, &completion::command(pg, &owners[0], VersionId::Null)).unwrap();
    owners.iter().map(|owner| append::load(pg, owner)).collect()
}

fn reclaim(pg: &PgStore, owner: &StagingOwnerRecord) -> ObjectPayloadReclaimCommand {
    let subject = &owner.subject;
    let StoredObject::Live(live) = pg
        .get_object_version(&subject.bucket, &subject.key, VersionId::Null)
        .unwrap()
    else {
        unreachable!()
    };
    let parts = pg
        .get_object_parts(&subject.bucket, &subject.key, VersionId::Null)
        .unwrap();
    let segments: Vec<_> = parts
        .iter()
        .flat_map(|part| {
            pg.get_multipart_part_segments(
                &subject.bucket,
                &subject.key,
                VersionId::Null,
                part.part_number,
            )
            .unwrap()
        })
        .collect();
    ObjectPayloadReclaimCommand::Multipart(MultipartReclaimRecord::from_object_parts(
        &subject.bucket,
        &subject.key,
        live.generation_id,
        4_000,
        &parts,
        &segments,
    ))
}

fn incoming(
    pg: &PgStore,
    old: &[StagingOwnerRecord],
    kind: u8,
) -> (MetadataCommandEnvelope, Vec<StagingOwnerRecord>) {
    let mut template = old
        .iter()
        .max_by_key(|owner| owner.subject.payload_generation.get())
        .unwrap()
        .clone();
    template.subject.originating_epoch = ClusterEpoch::new(7).unwrap();
    let stale = Some(reclaim(pg, &old[0]));
    if kind == 2 {
        let owners = completion_overwrite::incoming_upload(pg, &template, 2);
        return (
            completion_overwrite::command(pg, &owners, VersionId::Null, stale),
            owners,
        );
    }
    let (target, count) = if kind == 0 {
        (StagingTarget::DirectPut, 1)
    } else {
        (StagingTarget::StreamPut, 2)
    };
    let owner = object::next_attempt(pg, &template, target, count);
    let base = object::command(pg, &owner, VersionId::Null);
    let mut payload = object::payload(&base).clone();
    payload.stale_payload = stale;
    (
        MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                base.id().cluster_epoch(),
                base.id().pg_id(),
                MetadataCommandLogIndex::new(5000).unwrap(),
            ),
            MetadataCommandPayload::CommitDirectPutObject(Box::new(payload)),
        ),
        vec![owner],
    )
}

fn with_reclaim(
    command: &MetadataCommandEnvelope,
    reclaim: Option<ObjectPayloadReclaimCommand>,
) -> MetadataCommandEnvelope {
    let mut payload = command.payload().clone();
    match &mut payload {
        MetadataCommandPayload::CommitDirectPutObject(command) => command.stale_payload = reclaim,
        MetadataCommandPayload::CommitMultipartObject(command) => command.stale_payload = reclaim,
        _ => unreachable!(),
    }
    MetadataCommandEnvelope::new(command.id(), payload)
}

fn apply(pg: &PgStore, command: &MetadataCommandEnvelope) -> Result<(), String> {
    // Both helpers round-trip through the production command codec first.
    match command.payload() {
        MetadataCommandPayload::CommitDirectPutObject(_) => {
            object::apply(pg, command).map_err(|e| e.to_string())
        }
        MetadataCommandPayload::CommitMultipartObject(_) => {
            completion::apply(pg, command).map_err(|e| e.to_string())
        }
        _ => unreachable!(),
    }
}

fn assert_replaced(
    pg: &PgStore,
    old: &[StagingOwnerRecord],
    new: &[StagingOwnerRecord],
    command: &MetadataCommandEnvelope,
) {
    let stale = match command.payload() {
        MetadataCommandPayload::CommitDirectPutObject(payload) => {
            object::assert_object(pg, command);
            assert_eq!(
                append::load(pg, &new[0]),
                object::expected(&new[0], command)
            );
            &payload.stale_payload
        }
        MetadataCommandPayload::CommitMultipartObject(payload) => {
            completion::assert_complete(pg, command, new);
            &payload.stale_payload
        }
        _ => unreachable!(),
    };
    let Some(ObjectPayloadReclaimCommand::Multipart(stale)) = stale else {
        unreachable!()
    };
    assert_eq!(
        pg.get_multipart_reclaim(&stale.bucket, &stale.key, stale.generation_id)
            .unwrap(),
        Some(stale.clone())
    );
    for owner in old {
        let mut expected = owner.clone();
        if matches!(
            owner.lifecycle,
            StagingLifecycle::Published
                | StagingLifecycle::Retiring(StagingRetirement::PublishedRetained)
        ) {
            let StagingTarget::UploadPart {
                upload_generation,
                part_number,
                ..
            } = owner.subject.target
            else {
                unreachable!()
            };
            expected.lifecycle = StagingLifecycle::Retiring(StagingRetirement::PublishedRetained);
            for reference in &mut expected.references {
                reference.destination = StagingDestination::Reclaim {
                    generation: upload_generation,
                    part_number: Some(part_number),
                };
            }
        }
        assert_eq!(append::load(pg, owner), expected);
    }
    let StagingTarget::UploadPart { upload_id, .. } = &old[0].subject.target else {
        unreachable!()
    };
    assert!(pg
        .get_all_multipart_part_segments_for_upload(upload_id)
        .unwrap()
        .is_empty());
}

#[test]
fn staging_multipart_displacement_transfers_all_selected_owners_through_restart() {
    for kind in 0..3 {
        for count in [0, 2] {
            for retiring in [false, true] {
                let dir = test_util::tempdir();
                let pg_id = fixture(multipart_target()).subject.object_pg.get();
                let pg = PgStore::open(dir.path(), pg_id).unwrap();
                let mut old = source(&pg, count);
                if retiring {
                    pg.conn
                        .execute_batch(
                            "UPDATE payload_staging_owners SET lifecycle = 3 WHERE lifecycle = 1",
                        )
                        .unwrap();
                    old = old.iter().map(|owner| append::load(&pg, owner)).collect();
                }
                let (command, new) = incoming(&pg, &old, kind);
                pg.conn.execute_batch("COMMIT").unwrap();
                drop(pg);
                let pg = PgStore::open(dir.path(), pg_id).unwrap();
                pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
                crate::clock::with_time_and_monotonic_override(9_000_000, 90_000_000, || {
                    apply(&pg, &command)
                })
                .unwrap();
                assert_replaced(&pg, &old, &new, &command);
                pg.conn.execute_batch("COMMIT").unwrap();
                drop(pg);
                let pg = PgStore::open(dir.path(), pg_id).unwrap();
                pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
                assert_replaced(&pg, &old, &new, &command);
            }
        }
    }
}

#[test]
fn staging_multipart_displacement_rejects_forged_or_missing_reclaim_without_mutation() {
    for kind in 0..3 {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(multipart_target()).subject.object_pg.get(),
        )
        .unwrap();
        let old = source(&pg, 2);
        let (good, _) = incoming(&pg, &old, kind);
        let ObjectPayloadReclaimCommand::Multipart(root) = reclaim(&pg, &old[0]) else {
            unreachable!()
        };
        let mut forged = root.clone();
        forged.parts.clear();
        for bad in [None, Some(ObjectPayloadReclaimCommand::Multipart(forged))] {
            let before = introduction::state(&pg);
            let changes = pg.conn.total_changes();
            assert!(apply(&pg, &with_reclaim(&good, bad)).is_err());
            assert_eq!(pg.conn.total_changes(), changes);
            assert_eq!(introduction::state(&pg), before);
        }
        apply(&pg, &good).unwrap();
    }
}

/// Deliberately malformed durable state: segment rows have no object_parts FK.
/// Use a distinct physical key so dropping this row would lose its own payload
/// descriptor, not merely a duplicate reference to a selected shard.
pub(super) fn insert_orphan_segment(
    pg: &PgStore,
    owner: &StagingOwnerRecord,
    upload: &UploadId,
    version: VersionId,
    part_number: u32,
) {
    assert_eq!(pg.conn.execute(
        "INSERT INTO multipart_part_segments \
         SELECT bucket, key, ?1, ?2, ?3, segment_index, size, segment_crc64, zeroblob(16), \
                segment_vid + 100, data_pg_id, placement_cluster_epoch, ec_k, ec_m, shard_integrity \
         FROM multipart_part_segments \
         WHERE bucket = ?4 AND key = ?5 AND version_id = 0 AND part_number = 3 AND segment_index = 0",
        rusqlite::params![upload, version.to_u64() as i64, part_number, owner.subject.bucket, owner.subject.key],
    ).unwrap(), 1);
}

#[test]
fn staging_multipart_displacement_rejects_orphan_version_segments_before_mutation() {
    for kind in 0..3 {
        // Orphans before, between and after the selected parts (3 and 11).
        for part_number in [1, 7, 12] {
            for foreign_upload in [false, true] {
                let dir = test_util::tempdir();
                let pg = PgStore::open(
                    dir.path(),
                    fixture(multipart_target()).subject.object_pg.get(),
                )
                .unwrap();
                let old = source(&pg, 2);
                let (command, _) = incoming(&pg, &old, kind);
                let StagingTarget::UploadPart { upload_id, .. } = &old[0].subject.target else {
                    unreachable!()
                };
                let upload = if foreign_upload {
                    crate::tests::multipart_upload_id("orphan-upload")
                } else {
                    upload_id.clone()
                };
                insert_orphan_segment(&pg, &old[0], &upload, VersionId::Null, part_number);
                // The original reclaim construction cannot see the orphan.
                assert_eq!(with_reclaim(&command, Some(reclaim(&pg, &old[0]))), command);
                let before = introduction::state(&pg);
                let changes = pg.conn.total_changes();
                let error = apply(&pg, &command)
                    .expect_err("unlisted version segment must reject overwrite");
                assert_eq!(
                    error,
                    "displaced object does not match its exact retained owner"
                );
                assert_eq!(pg.conn.total_changes(), changes);
                assert_eq!(introduction::state(&pg), before);
                let orphan = pg
                    .get_multipart_part_segments(
                        &old[0].subject.bucket,
                        &old[0].subject.key,
                        VersionId::Null,
                        part_number,
                    )
                    .unwrap();
                assert_eq!(orphan.len(), 1);
                assert_eq!(orphan[0].upload_id, upload);
                assert_eq!(orphan[0].segment_okh, [0; 16]);
            }
        }
    }
}

#[test]
fn staging_multipart_displacement_does_not_consume_another_versions_segments() {
    for kind in 0..3 {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(multipart_target()).subject.object_pg.get(),
        )
        .unwrap();
        let old = source(&pg, 2);
        let (command, new) = incoming(&pg, &old, kind);
        let upload = crate::tests::multipart_upload_id("other-version");
        let version = VersionId::from_u64(9);
        insert_orphan_segment(&pg, &old[0], &upload, version, 7);
        let before = pg
            .get_multipart_part_segments(&old[0].subject.bucket, &old[0].subject.key, version, 7)
            .unwrap();
        assert_eq!(before.len(), 1);
        apply(&pg, &command).unwrap();
        assert_replaced(&pg, &old, &new, &command);
        assert_eq!(
            pg.get_multipart_part_segments(&old[0].subject.bucket, &old[0].subject.key, version, 7)
                .unwrap(),
            before
        );
    }
}

#[test]
fn staging_multipart_displacement_rebuilt_manifest_cannot_replace_owner_provenance() {
    for kind in 0..3 {
        for count in [0, 2] {
            for mutation in [
                "UPDATE object_parts SET placement_cluster_epoch = placement_cluster_epoch + 1 WHERE part_number = 3",
                "UPDATE object_parts SET part_vid = part_vid + 100 WHERE part_number = 3",
                "UPDATE object_parts SET data_pg_id = (data_pg_id + 1) % 8 WHERE part_number = 3",
                "UPDATE object_parts SET ec_k = ec_k + 1 WHERE part_number = 3",
                "UPDATE objects SET multipart_completion_upload_id = NULL, multipart_completion_fingerprint = NULL",
                "DELETE FROM payload_staging_owners WHERE part_number = 3 AND lifecycle = 1",
                "UPDATE payload_staging_owners SET upload_generation = upload_generation + 1 WHERE part_number = 3 AND lifecycle = 1",
                "UPDATE payload_staging_resources SET incarnation = incarnation + 1 WHERE attempt IN (SELECT attempt FROM payload_staging_owners WHERE part_number = 3 AND lifecycle = 1); UPDATE payload_staging_owners SET incarnation = incarnation + 1 WHERE part_number = 3 AND lifecycle = 1",
            ] {
                let dir = test_util::tempdir();
                let pg = PgStore::open(dir.path(), fixture(multipart_target()).subject.object_pg.get()).unwrap();
                let old = source(&pg, count);
                let (good, _) = incoming(&pg, &old, kind);
                pg.conn.execute_batch(mutation).unwrap();
                let bad = with_reclaim(&good, Some(reclaim(&pg, &old[0])));
                let before = introduction::state(&pg);
                let changes = pg.conn.total_changes();
                let error = apply(&pg, &bad).expect_err(mutation);
                assert!(!error.is_empty());
                assert_eq!(pg.conn.total_changes(), changes, "{mutation}");
                assert_eq!(introduction::state(&pg), before, "{mutation}");
            }
        }
    }
}

#[test]
fn staging_multipart_displacement_requires_the_complete_unique_owner_set() {
    for kind in 0..3 {
        for count in [0, 2] {
            for duplicate in [false, true] {
                let dir = test_util::tempdir();
                let pg = PgStore::open(
                    dir.path(),
                    fixture(multipart_target()).subject.object_pg.get(),
                )
                .unwrap();
                let old = source(&pg, count);
                let (good, _) = incoming(&pg, &old, kind);
                if duplicate {
                    let mut extra = old[0].clone();
                    extra.subject.id.attempt = crate::tests::stream_session_id("duplicate-old");
                    for window in &mut extra.windows {
                        window.owner = extra.subject.id.clone();
                        window.segment_hash = extra.subject.segment_hash(window.index);
                    }
                    for reference in &mut extra.references {
                        reference.segment_hash = extra.subject.segment_hash(reference.window_index);
                    }
                    let ResourceResponsibility::ReleaseDue(generation) =
                        &mut extra.resources.generation
                    else {
                        unreachable!()
                    };
                    generation.reservation_id = extra.subject.id.attempt.clone();
                    sql::insert(
                        &pg.conn,
                        PgId::new(pg.pg_id),
                        &topology(),
                        &extra.validate(&topology()).unwrap(),
                    )
                    .unwrap();
                } else {
                    // Keep count/size/reclaim internally consistent while
                    // omitting a real completed owner's entire selected part.
                    pg.conn.execute_batch("UPDATE objects SET size = size - (SELECT size FROM object_parts WHERE part_number = 3), parts_count = parts_count - 1; DELETE FROM multipart_part_segments WHERE part_number = 3; DELETE FROM object_parts WHERE part_number = 3").unwrap();
                }
                let bad = with_reclaim(&good, Some(reclaim(&pg, &old[0])));
                let before = introduction::state(&pg);
                let changes = pg.conn.total_changes();
                assert!(apply(&pg, &bad).unwrap_err().contains("displaced object"));
                assert_eq!(pg.conn.total_changes(), changes);
                assert_eq!(introduction::state(&pg), before);
            }
        }
    }
}

#[test]
fn staging_multipart_displacement_rebuilt_segments_do_not_replace_original_windows() {
    for kind in 0..3 {
        for mutation in [
            "UPDATE multipart_part_segments SET segment_okh = zeroblob(16) WHERE part_number = 3",
            "UPDATE multipart_part_segments SET segment_vid = segment_vid + 100 WHERE part_number = 3",
            "UPDATE multipart_part_segments SET data_pg_id = (data_pg_id + 1) % 8 WHERE part_number = 3",
            "UPDATE multipart_part_segments SET placement_cluster_epoch = placement_cluster_epoch + 1 WHERE part_number = 3",
            "UPDATE multipart_part_segments SET segment_crc64 = segment_crc64 + 1 WHERE part_number = 3",
        ] {
            let dir = test_util::tempdir();
            let pg = PgStore::open(dir.path(), fixture(multipart_target()).subject.object_pg.get()).unwrap();
            let old = source(&pg, 2);
            let (good, _) = incoming(&pg, &old, kind);
            pg.conn.execute_batch(mutation).unwrap();
            let bad = with_reclaim(&good, Some(reclaim(&pg, &old[0])));
            let before = introduction::state(&pg);
            let changes = pg.conn.total_changes();
            assert!(apply(&pg, &bad).unwrap_err().contains("displaced object"), "{mutation}");
            assert_eq!(pg.conn.total_changes(), changes);
            assert_eq!(introduction::state(&pg), before);
        }
    }
}

#[test]
fn staging_multipart_displacement_rolls_back_all_owners_and_manifests() {
    for kind in 0..3 {
        for event in [
            "BEFORE INSERT ON multipart_reclaims",
            "BEFORE INSERT ON multipart_reclaim_parts",
            "BEFORE DELETE ON multipart_part_segments WHEN OLD.upload_id IS NOT NULL",
            "BEFORE DELETE ON object_parts",
            "BEFORE UPDATE OF destination ON payload_staging_references WHEN NEW.destination = 3 AND OLD.part_number = 11",
            "BEFORE UPDATE OF lifecycle ON payload_staging_owners WHEN NEW.lifecycle = 3 AND OLD.part_number = 11",
        ] {
            let dir = test_util::tempdir();
            let pg = PgStore::open(dir.path(), fixture(multipart_target()).subject.object_pg.get()).unwrap();
            let old = source(&pg, 2);
            let (good, new) = incoming(&pg, &old, kind);
            pg.conn.execute_batch("CREATE TEMP TABLE outer_work(value INTEGER); INSERT INTO outer_work VALUES (42)").unwrap();
            pg.conn.execute_batch(&format!("CREATE TEMP TRIGGER fail_displacement {event} BEGIN SELECT RAISE(ABORT, 'multipart displacement failure'); END")).unwrap();
            let before = introduction::state(&pg);
            let error = apply(&pg, &good).unwrap_err();
            assert!(error.contains("multipart displacement failure"), "{event}: {error}");
            assert_eq!(introduction::state(&pg), before, "{event}");
            assert!(!pg.conn.is_autocommit());
            assert_eq!(pg.conn.query_row("SELECT value FROM outer_work", [], |row| row.get::<_, i64>(0)).unwrap(), 42);
            pg.conn.execute_batch("DROP TRIGGER fail_displacement").unwrap();
            apply(&pg, &good).unwrap();
            assert_replaced(&pg, &old, &new, &good);
        }
    }
}
