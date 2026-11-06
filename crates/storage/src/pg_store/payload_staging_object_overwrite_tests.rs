// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Metadata-only owner/manifest/reclaim transactions. No physical deletion or
//! frontend-custody acceptance is claimed by these component fixtures.

use super::super::object::ObjectError;
use super::object::{apply, assert_object, command, expected, initialize, next_attempt, payload};
use super::*;
use crate::metadata_command::{
    MetadataCommandEnvelope, MetadataCommandPayload, ObjectPayloadReclaimCommand,
};
use crate::node_runtime::traits::PgMetadataStore;
use crate::pg_store::PgStore;
use crate::types::{ObjectSegmentsReclaimRecord, ObjectSegmentsReclaimSegmentRecord};
use crate::VersionId;

pub(super) fn reclaim(first: &MetadataCommandEnvelope) -> ObjectSegmentsReclaimRecord {
    let command = payload(first);
    ObjectSegmentsReclaimRecord {
        bucket: command.object.bucket.clone(),
        key: command.object.key.clone(),
        generation_id: command.object.generation_id,
        created_at: 4_000,
        segments: command
            .segments
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

fn replacement(
    pg: &PgStore,
    next: &StagingOwnerRecord,
    first: &MetadataCommandEnvelope,
) -> MetadataCommandEnvelope {
    let base = command(pg, next, VersionId::Null);
    let mut changed = payload(&base).clone();
    changed.stale_payload = Some(ObjectPayloadReclaimCommand::Segments(reclaim(first)));
    MetadataCommandEnvelope::new(
        base.id(),
        MetadataCommandPayload::CommitDirectPutObject(Box::new(changed)),
    )
}

pub(super) fn retained(old: &StagingOwnerRecord) -> StagingOwnerRecord {
    let mut expected = old.clone();
    expected.lifecycle = StagingLifecycle::Retiring(StagingRetirement::PublishedRetained);
    for reference in &mut expected.references {
        reference.destination = StagingDestination::Reclaim {
            generation: old.subject.payload_generation,
            part_number: None,
        };
    }
    expected
}

fn assert_reclaim(pg: &PgStore, first: &MetadataCommandEnvelope) {
    let expected = reclaim(first);
    assert_eq!(
        pg.get_object_segments_reclaim(&expected.bucket, &expected.key, expected.generation_id)
            .unwrap(),
        Some(expected)
    );
}

#[test]
fn staging_object_overwrite_atomically_retains_exact_old_windows_and_resources_through_restart() {
    for (old_target, count) in [
        (StagingTarget::DirectPut, 1),
        (StagingTarget::StreamPut, 0),
        (StagingTarget::StreamPut, 1),
        (StagingTarget::StreamPut, 2),
    ] {
        for (new_target, new_count) in [
            (StagingTarget::DirectPut, 1),
            (StagingTarget::StreamPut, 0),
            (StagingTarget::StreamPut, 2),
        ] {
            for already_retiring in [false, true] {
                let dir = test_util::tempdir();
                let pg_id = fixture(old_target.clone()).subject.object_pg.get();
                let pg = PgStore::open(dir.path(), pg_id).unwrap();
                let old = initialize(&pg, old_target.clone(), count);
                let first = command(&pg, &old, VersionId::Null);
                apply(&pg, &first).unwrap();
                if already_retiring {
                    pg.conn
                        .execute_batch("UPDATE payload_staging_owners SET lifecycle = 3")
                        .unwrap();
                }
                let published = append::load(&pg, &old);
                let next = next_attempt(&pg, &old, new_target.clone(), new_count);
                let replacement = replacement(&pg, &next, &first);
                // Expired cutoffs and a retired local allocation lifetime do not
                // prevent deterministic metadata transfer of original evidence.
                pg.conn.execute_batch("COMMIT").unwrap();
                drop(pg);
                let pg = PgStore::open(dir.path(), pg_id).unwrap();
                assert_ne!(
                    pg.stream_allocation_issuer(old.subject.issuer.node_id)
                        .unwrap(),
                    old.subject.issuer
                );
                pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
                crate::clock::with_time_and_monotonic_override(9_000_000, 10_000_000, || {
                    apply(&pg, &replacement)
                })
                .unwrap();
                assert_object(&pg, &replacement);
                assert_reclaim(&pg, &first);
                assert_eq!(append::load(&pg, &old), retained(&published));
                assert_eq!(append::load(&pg, &next), expected(&next, &replacement));
                let before = introduction::state(&pg);
                let changes = pg.conn.total_changes();
                assert!(matches!(
                    apply(&pg, &replacement),
                    Err(ObjectError::CommandBinding)
                ));
                assert_eq!(pg.conn.total_changes(), changes);
                assert_eq!(introduction::state(&pg), before);
                pg.conn.execute_batch("COMMIT").unwrap();
                drop(pg);
                let pg = PgStore::open(dir.path(), pg_id).unwrap();
                pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
                assert_object(&pg, &replacement);
                assert_reclaim(&pg, &first);
                assert_eq!(append::load(&pg, &old), retained(&published));
                assert_eq!(append::load(&pg, &next), expected(&next, &replacement));
            }
        }
    }
}

#[test]
fn staging_object_overwrite_rejects_forged_reclaim_subject_and_full_segment_list_before_mutation() {
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(StagingTarget::StreamPut).subject.object_pg.get(),
    )
    .unwrap();
    let old = initialize(&pg, StagingTarget::StreamPut, 2);
    let first = command(&pg, &old, VersionId::Null);
    apply(&pg, &first).unwrap();
    let next = next_attempt(&pg, &old, StagingTarget::DirectPut, 1);
    let good = replacement(&pg, &next, &first);
    let before = introduction::state(&pg);
    let changes = pg.conn.total_changes();
    for case in 0..12 {
        let mut bad = payload(&good).clone();
        let Some(ObjectPayloadReclaimCommand::Segments(reclaim)) = &mut bad.stale_payload else {
            unreachable!()
        };
        match case {
            0 => reclaim.bucket = crate::tests::bucket_name("crossed"),
            1 => reclaim.key = crate::tests::object_key("crossed"),
            2 => reclaim.generation_id = next.subject.payload_generation,
            3 => reclaim.segments.clear(),
            4 => {
                reclaim.segments.pop();
            }
            5 => reclaim.segments.push(reclaim.segments[0].clone()),
            6 => reclaim.segments.swap(0, 1),
            7 => reclaim.segments[0].segment_okh[0] ^= 1,
            8 => reclaim.segments[0].segment_vid = GenerationId::new(99).unwrap(),
            9 => reclaim.segments[0].data_pg_id ^= 1,
            10 => reclaim.segments[0].ec.k += 1,
            11 => reclaim.segments[0].ec.m += 1,
            _ => unreachable!(),
        }
        let bad = MetadataCommandEnvelope::new(
            good.id(),
            MetadataCommandPayload::CommitDirectPutObject(Box::new(bad)),
        );
        let error = apply(&pg, &bad).unwrap_err();
        assert!(
            matches!(
                error,
                ObjectError::Metadata(crate::MetadataError::InvariantViolation {
                    context: "staging displaced object reclaim binding",
                    ..
                })
            ),
            "case {case}: {error}"
        );
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);
    }
    apply(&pg, &good).unwrap();
    assert_reclaim(&pg, &first);
}

#[test]
fn staging_object_overwrite_requires_unique_owner_even_for_empty_objects() {
    for count in [0, 2] {
        for case in ["missing", "duplicate", "incarnation", "unreferenced"] {
            let dir = test_util::tempdir();
            let pg = PgStore::open(
                dir.path(),
                fixture(StagingTarget::StreamPut).subject.object_pg.get(),
            )
            .unwrap();
            let old = initialize(&pg, StagingTarget::StreamPut, count);
            let first = command(&pg, &old, VersionId::Null);
            apply(&pg, &first).unwrap();
            let published = append::load(&pg, &old);
            let next = next_attempt(&pg, &old, StagingTarget::DirectPut, 1);
            let good = replacement(&pg, &next, &first);
            match case {
                "missing" => {
                    pg.conn
                        .execute(
                            "DELETE FROM payload_staging_owners WHERE attempt = ?1",
                            [&old.subject.id.attempt],
                        )
                        .unwrap();
                }
                "duplicate" => {
                    let mut duplicate = published.clone();
                    duplicate.subject.id.attempt =
                        crate::tests::stream_session_id("duplicate-owner");
                    for window in &mut duplicate.windows {
                        window.owner = duplicate.subject.id.clone();
                        window.segment_hash = duplicate.subject.segment_hash(window.index);
                    }
                    for reference in &mut duplicate.references {
                        reference.segment_hash =
                            duplicate.subject.segment_hash(reference.window_index);
                    }
                    let ResourceResponsibility::ReleaseDue(generation) =
                        &mut duplicate.resources.generation
                    else {
                        unreachable!()
                    };
                    generation.reservation_id = duplicate.subject.id.attempt.clone();
                    sql::insert(
                        &pg.conn,
                        duplicate.subject.object_pg,
                        &topology(),
                        &duplicate.validate(&topology()).unwrap(),
                    )
                    .unwrap();
                }
                "incarnation" => {
                    pg.conn.execute("UPDATE payload_staging_owners SET incarnation = incarnation + 1 WHERE attempt = ?1", [&old.subject.id.attempt]).unwrap();
                    pg.conn.execute("UPDATE payload_staging_resources SET incarnation = incarnation + 1 WHERE attempt = ?1", [&old.subject.id.attempt]).unwrap();
                    // Still a structurally valid owner: reject the cross-owner
                    // incarnation, not malformed row decoding.
                    append::load(&pg, &old).validate(&topology()).unwrap();
                }
                "unreferenced" => {
                    pg.conn
                        .execute(
                            "DELETE FROM payload_staging_references WHERE attempt = ?1",
                            [&old.subject.id.attempt],
                        )
                        .unwrap();
                    pg.conn
                        .execute(
                            "UPDATE payload_staging_owners SET lifecycle = 4 WHERE attempt = ?1",
                            [&old.subject.id.attempt],
                        )
                        .unwrap();
                    append::load(&pg, &old).validate(&topology()).unwrap();
                }
                _ => unreachable!(),
            }
            let before = introduction::state(&pg);
            let changes = pg.conn.total_changes();
            let error = apply(&pg, &good).unwrap_err();
            assert!(
                matches!(
                    error,
                    ObjectError::Persistence(sql::PersistenceError::ObjectOwner)
                        | ObjectError::DisplacedOwner
                        | ObjectError::Invariant(StagingInvariant::Lifecycle)
                ),
                "{count}/{case}: {error}"
            );
            assert_eq!(pg.conn.total_changes(), changes);
            assert_eq!(introduction::state(&pg), before);
        }
    }
}

#[test]
fn staging_object_overwrite_does_not_adopt_an_existing_reclaim_root() {
    for existing_kind in ["exact", "crossed", "multipart"] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(StagingTarget::DirectPut).subject.object_pg.get(),
        )
        .unwrap();
        let old = initialize(&pg, StagingTarget::DirectPut, 1);
        let first = command(&pg, &old, VersionId::Null);
        apply(&pg, &first).unwrap();
        let next = next_attempt(&pg, &old, StagingTarget::StreamPut, 2);
        let good = replacement(&pg, &next, &first);
        let mut existing = reclaim(&first);
        if existing_kind == "crossed" {
            existing.segments[0].segment_okh[0] ^= 1;
        }
        pg.conn.execute_batch("COMMIT").unwrap();
        if existing_kind == "multipart" {
            pg.put_multipart_reclaim(&crate::types::MultipartReclaimRecord {
                bucket: existing.bucket,
                key: existing.key,
                generation_id: existing.generation_id,
                created_at: existing.created_at,
                parts: Vec::new(),
            })
            .unwrap();
        } else {
            pg.put_object_segments_reclaim(&existing).unwrap();
        }
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        assert!(matches!(
            apply(&pg, &good),
            Err(ObjectError::DisplacedOwner)
        ));
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);
    }
}

#[test]
fn staging_object_overwrite_owner_lookup_binds_bucket_key_and_generation_independently() {
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(StagingTarget::StreamPut).subject.object_pg.get(),
    )
    .unwrap();
    let old = initialize(&pg, StagingTarget::StreamPut, 0);
    let first = command(&pg, &old, VersionId::Null);
    apply(&pg, &first).unwrap();
    let published = append::load(&pg, &old);
    let subject = &old.subject;
    let crossed_bucket = (0..1000)
        .map(|index| crate::tests::bucket_name(format!("crossed-{index}")))
        .find(|bucket| topology().object_pg_for(bucket, &subject.key) == subject.object_pg.get())
        .unwrap();
    let crossed_key = (0..1000)
        .map(|index| crate::tests::object_key(format!("crossed-{index}")))
        .find(|key| topology().object_pg_for(&subject.bucket, key) == subject.object_pg.get())
        .unwrap();
    assert_eq!(
        topology().object_pg_for(&crossed_bucket, &subject.key),
        subject.object_pg.get()
    );
    assert_eq!(
        topology().object_pg_for(&subject.bucket, &crossed_key),
        subject.object_pg.get()
    );
    let before = introduction::state(&pg);
    let changes = pg.conn.total_changes();
    for (bucket, key, generation) in [
        (&crossed_bucket, &subject.key, subject.payload_generation),
        (&subject.bucket, &crossed_key, subject.payload_generation),
        (
            &subject.bucket,
            &subject.key,
            GenerationId::new(subject.payload_generation.get() + 1).unwrap(),
        ),
    ] {
        assert!(matches!(
            sql::owner_for_standard(&pg.conn, subject.object_pg, bucket, key, generation),
            Err(sql::PersistenceError::ObjectOwner)
        ));
        assert_eq!(
            sql::owner_for_standard(
                &pg.conn,
                subject.object_pg,
                &subject.bucket,
                &subject.key,
                subject.payload_generation
            )
            .unwrap(),
            published
        );
    }
    assert_eq!(pg.conn.total_changes(), changes);
    assert_eq!(introduction::state(&pg), before);
}

#[test]
fn staging_object_overwrite_rejects_independent_displaced_object_geometry() {
    for count in [0, 2] {
        for mutation in [
            "UPDATE objects SET ec_k = ec_k + 1",
            "UPDATE objects SET ec_m = ec_m + 1",
            "UPDATE objects SET size = size + 1",
        ] {
            let dir = test_util::tempdir();
            let pg = PgStore::open(
                dir.path(),
                fixture(StagingTarget::StreamPut).subject.object_pg.get(),
            )
            .unwrap();
            let old = initialize(&pg, StagingTarget::StreamPut, count);
            let first = command(&pg, &old, VersionId::Null);
            apply(&pg, &first).unwrap();
            let next = next_attempt(&pg, &old, StagingTarget::DirectPut, 1);
            let good = replacement(&pg, &next, &first);
            pg.conn.execute_batch("SAVEPOINT crossed_geometry").unwrap();
            pg.conn.execute_batch(mutation).unwrap();
            let before = introduction::state(&pg);
            let changes = pg.conn.total_changes();
            assert!(
                matches!(apply(&pg, &good), Err(ObjectError::DisplacedOwner)),
                "{mutation}"
            );
            assert_eq!(pg.conn.total_changes(), changes);
            assert_eq!(introduction::state(&pg), before);
            pg.conn
                .execute_batch("ROLLBACK TO crossed_geometry; RELEASE crossed_geometry")
                .unwrap();
            apply(&pg, &good).unwrap();
            assert_reclaim(&pg, &first);
        }
    }
}

#[test]
fn staging_object_overwrite_rejects_co_crossed_manifest_and_reclaim_against_retained_windows() {
    for (field, value) in [
        ("segment_okh", "zeroblob(16)"),
        ("segment_vid", "99"),
        ("data_pg_id", "(data_pg_id + 1) % 8"),
        ("placement_cluster_epoch", "placement_cluster_epoch + 1"),
        ("ec_k", "ec_k + 1"),
        ("ec_m", "ec_m + 1"),
        ("size", "size + 1"),
    ] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(StagingTarget::StreamPut).subject.object_pg.get(),
        )
        .unwrap();
        let old = initialize(&pg, StagingTarget::StreamPut, 2);
        let first = command(&pg, &old, VersionId::Null);
        apply(&pg, &first).unwrap();
        let next = next_attempt(&pg, &old, StagingTarget::DirectPut, 1);
        // Mutate durable metadata and the command consistently. Exact-reclaim
        // validation alone must not mask the retained-window correspondence.
        pg.conn
            .execute_batch(&format!(
                "UPDATE object_segments SET {field} = {value} WHERE segment_index = 0"
            ))
            .unwrap();
        let mut changed_first = payload(&first).clone();
        changed_first.segments = pg
            .get_object_segments(&old.subject.bucket, &old.subject.key, VersionId::Null)
            .unwrap();
        let changed_first = MetadataCommandEnvelope::new(
            first.id(),
            MetadataCommandPayload::CommitDirectPutObject(Box::new(changed_first)),
        );
        let bad = replacement(&pg, &next, &changed_first);
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        assert!(
            matches!(apply(&pg, &bad), Err(ObjectError::DisplacedOwner)),
            "{field}"
        );
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);
    }
}

#[test]
fn staging_object_overwrite_rolls_back_manifest_root_and_both_owners_at_every_handoff() {
    for injection in [
        "manifest",
        "root",
        "root-segment",
        "new-proof",
        "new-owner",
        "old-reference",
        "old-owner",
    ] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(StagingTarget::StreamPut).subject.object_pg.get(),
        )
        .unwrap();
        let old = initialize(&pg, StagingTarget::StreamPut, 2);
        let first = command(&pg, &old, VersionId::Null);
        apply(&pg, &first).unwrap();
        let published = append::load(&pg, &old);
        let next = next_attempt(&pg, &old, StagingTarget::DirectPut, 1);
        let good = replacement(&pg, &next, &first);
        let event = match injection {
            "manifest" => "BEFORE INSERT ON object_segments".into(),
            "root" => "BEFORE INSERT ON object_segments_reclaims".into(),
            "root-segment" => "BEFORE INSERT ON object_segment_reclaim_segments".into(),
            "new-proof" => "BEFORE UPDATE ON payload_staging_resources".into(),
            "new-owner" => format!(
                "BEFORE UPDATE ON payload_staging_owners WHEN NEW.attempt = '{}'",
                next.subject.id.attempt.as_str()
            ),
            "old-reference" => format!(
                "BEFORE UPDATE ON payload_staging_references WHEN NEW.attempt = '{}'",
                old.subject.id.attempt.as_str()
            ),
            "old-owner" => format!(
                "BEFORE UPDATE ON payload_staging_owners WHEN NEW.attempt = '{}'",
                old.subject.id.attempt.as_str()
            ),
            _ => unreachable!(),
        };
        pg.conn
            .execute_batch(
                "CREATE TEMP TABLE outer_work(value INTEGER); INSERT INTO outer_work VALUES (42)",
            )
            .unwrap();
        pg.conn.execute_batch(&format!("CREATE TEMP TRIGGER fail_overwrite {event} BEGIN SELECT RAISE(ABORT, 'overwrite handoff failure'); END")).unwrap();
        let before = introduction::state(&pg);
        let error = apply(&pg, &good).unwrap_err();
        assert!(
            error.to_string().contains("overwrite handoff failure"),
            "{injection}: {error}"
        );
        assert_eq!(introduction::state(&pg), before, "{injection}");
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
        apply(&pg, &good).unwrap();
        assert_object(&pg, &good);
        assert_reclaim(&pg, &first);
        assert_eq!(append::load(&pg, &old), retained(&published));
        assert_eq!(append::load(&pg, &next), expected(&next, &good));
    }
}
