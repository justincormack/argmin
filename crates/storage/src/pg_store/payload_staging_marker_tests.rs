// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Metadata-only marker publication and retained-owner handoff. No physical
//! deletion, payload decryptability or production-dispatch activation is claimed.

use super::super::marker::{apply_in_open_transaction, MarkerError};
use super::*;
use crate::metadata_command::{
    DeleteObjectVersionMode, DeleteObjectVersionTarget, InsertDeleteMarkerCommand,
    MetadataCommandEnvelope, MetadataCommandPayload, ObjectPayloadReclaimCommand,
    INSERT_DELETE_MARKER_BUCKET_WRITE_OPERATION_KIND,
};
use crate::node_runtime::traits::PgMetadataStore;
use crate::pg_store::PgStore;
use crate::types::{MultipartPartSegmentRecord, ObjectPartRecord};
use crate::{MetadataError, ObjectSegmentRecord, OwnerIdentity, StoredObject, VersionId};

fn command(
    pg: &PgStore,
    owner: &StagingOwnerRecord,
    version: VersionId,
) -> MetadataCommandEnvelope {
    let subject = &owner.subject;
    let stale_payload = match pg.get_object_version(&subject.bucket, &subject.key, version) {
        Ok(StoredObject::Live(_)) if version.is_null() => {
            let delete = delete::command(pg, owner, version, DeleteObjectVersionMode::Specific);
            let MetadataCommandPayload::DeleteObjectVersion(delete) = delete.payload() else {
                unreachable!()
            };
            let DeleteObjectVersionTarget::Live { payload, .. } = &delete.target else {
                unreachable!()
            };
            Some(payload.clone())
        }
        Ok(_) | Err(MetadataError::ObjectNotFound) => None,
        Err(error) => panic!("load marker target: {error}"),
    };
    let mut reservation = proof(subject, INSERT_DELETE_MARKER_BUCKET_WRITE_OPERATION_KIND);
    reservation.cluster_epoch = ClusterEpoch::new(9).unwrap();
    let write_sequence = pg
        .next_object_write_sequence(subject.bucket.as_str(), subject.key.as_str())
        .unwrap();
    MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            reservation.cluster_epoch,
            subject.object_pg,
            MetadataCommandLogIndex::new(6000 + write_sequence).unwrap(),
        ),
        MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
            bucket_write_reservation: reservation,
            bucket: subject.bucket.clone(),
            key: subject.key.clone(),
            version_id: version,
            owner: OwnerIdentity::from_principal("marker-owner"),
            write_sequence,
            last_modified_millis: 4_000,
            stale_payload,
        }),
    )
}

fn payload(envelope: &MetadataCommandEnvelope) -> &InsertDeleteMarkerCommand {
    let MetadataCommandPayload::InsertDeleteMarker(command) = envelope.payload() else {
        unreachable!()
    };
    command
}

fn apply(pg: &PgStore, envelope: &MetadataCommandEnvelope) -> Result<(), MarkerError> {
    let decoded = crate::metadata_command::decode_metadata_command_envelope(
        &envelope.command_bytes(),
        &crate::node_runtime::MetadataCommandDecodeAuthority::new_for_test(),
    )
    .unwrap();
    apply_in_open_transaction(pg, &topology(), &decoded)
}

fn assert_marker(pg: &PgStore, envelope: &MetadataCommandEnvelope, old: &[StagingOwnerRecord]) {
    let command = payload(envelope);
    let StoredObject::DeleteMarker(marker) = pg
        .get_object_version(&command.bucket, &command.key, command.version_id)
        .unwrap()
    else {
        panic!("marker missing")
    };
    assert_eq!(marker.owner, command.owner);
    assert_eq!(marker.last_modified, command.last_modified_millis);
    assert_eq!(
        pg.get_object_meta(&command.bucket, &command.key)
            .unwrap()
            .version_id(),
        command.version_id
    );
    assert_eq!(
        pg.object_write_sequence(
            command.bucket.as_str(),
            command.key.as_str(),
            command.version_id
        )
        .unwrap(),
        Some(command.write_sequence)
    );
    if let Some(reclaim) = &command.stale_payload {
        match reclaim {
            ObjectPayloadReclaimCommand::Segments(root) => assert_eq!(
                pg.get_object_segments_reclaim(&root.bucket, &root.key, root.generation_id)
                    .unwrap(),
                Some(root.clone())
            ),
            ObjectPayloadReclaimCommand::Multipart(root) => assert_eq!(
                pg.get_multipart_reclaim(&root.bucket, &root.key, root.generation_id)
                    .unwrap(),
                Some(root.clone())
            ),
        }
        assert!(pg
            .get_object_segments(&command.bucket, &command.key, command.version_id)
            .unwrap()
            .is_empty());
        assert!(pg
            .get_object_parts(&command.bucket, &command.key, command.version_id)
            .unwrap()
            .is_empty());
        assert_eq!(pg.conn.query_row("SELECT COUNT(*) FROM multipart_part_segments WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
            rusqlite::params![command.bucket, command.key, command.version_id.to_u64() as i64], |row| row.get::<_, u64>(0)).unwrap(), 0);
    }
    for before in old {
        let mut after = before.clone();
        if command.stale_payload.is_some()
            && matches!(
                before.lifecycle,
                StagingLifecycle::Published
                    | StagingLifecycle::Retiring(StagingRetirement::PublishedRetained)
            )
        {
            after.lifecycle = StagingLifecycle::Retiring(StagingRetirement::PublishedRetained);
            for reference in &mut after.references {
                let StagingDestination::Object {
                    generation,
                    part_number,
                } = reference.destination
                else {
                    unreachable!()
                };
                reference.destination = StagingDestination::Reclaim {
                    generation,
                    part_number,
                };
            }
        }
        assert_eq!(append::load(pg, before), after);
    }
}

#[derive(Debug, PartialEq)]
struct PayloadSnapshot {
    record: StoredObject,
    segments: Vec<ObjectSegmentRecord>,
    parts: Vec<ObjectPartRecord>,
    part_segments: Vec<MultipartPartSegmentRecord>,
}

fn snapshot(pg: &PgStore, owner: &StagingOwnerRecord, version: VersionId) -> PayloadSnapshot {
    let s = &owner.subject;
    let parts = pg.get_object_parts(&s.bucket, &s.key, version).unwrap();
    PayloadSnapshot {
        record: pg.get_object_version(&s.bucket, &s.key, version).unwrap(),
        segments: pg.get_object_segments(&s.bucket, &s.key, version).unwrap(),
        part_segments: parts
            .iter()
            .flat_map(|p| {
                pg.get_multipart_part_segments(&s.bucket, &s.key, version, p.part_number)
                    .unwrap()
            })
            .collect(),
        parts,
    }
}

#[test]
fn staging_epoch_reset_marker_preserves_displaced_null_ownership() {
    for kind in 0..5 {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(StagingTarget::DirectPut).subject.object_pg.get(),
        )
        .unwrap();
        let owners = delete::source(&pg, kind, VersionId::Null);
        let original = command(&pg, &owners[0], VersionId::Null);
        let command = epoch_order::at_epoch_start(&pg, &original, original.id().cluster_epoch());
        apply(&pg, &command).unwrap();
        assert_marker(&pg, &command, &owners);
        let pg = epoch_order::record_and_reopen(pg, dir.path(), &command);
        assert_marker(&pg, &command, &owners);
    }
}

#[test]
fn staging_marker_transfers_exact_null_payload_ownership_through_restart() {
    for kind in 0..5 {
        for retiring in [false, true] {
            let dir = test_util::tempdir();
            let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg.get();
            let pg = PgStore::open(dir.path(), pg_id).unwrap();
            let mut owners = delete::source(&pg, kind, VersionId::Null);
            if retiring {
                pg.conn
                    .execute_batch(
                        "UPDATE payload_staging_owners SET lifecycle = 3 WHERE lifecycle = 1",
                    )
                    .unwrap();
                owners = owners.iter().map(|o| append::load(&pg, o)).collect();
            }
            let marker = command(&pg, &owners[0], VersionId::Null);
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
                apply(&pg, &marker)
            })
            .unwrap();
            assert_marker(&pg, &marker, &owners);
            let before = introduction::state(&pg);
            let changes = pg.conn.total_changes();
            assert!(matches!(apply(&pg, &marker), Err(MarkerError::Target)));
            assert_eq!(pg.conn.total_changes(), changes);
            assert_eq!(introduction::state(&pg), before);
            pg.conn.execute_batch("COMMIT").unwrap();
            drop(pg);
            let pg = PgStore::open(dir.path(), pg_id).unwrap();
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            assert_marker(&pg, &marker, &owners);
        }
    }
}

#[test]
fn staging_marker_numbered_publication_preserves_existing_payloads_and_owners() {
    for kind in 0..5 {
        for source_version in [VersionId::Null, VersionId::from_u64(3)] {
            let dir = test_util::tempdir();
            let pg = PgStore::open(
                dir.path(),
                fixture(StagingTarget::DirectPut).subject.object_pg.get(),
            )
            .unwrap();
            let owners = delete::source(&pg, kind, source_version);
            let mut expected = snapshot(&pg, &owners[0], source_version);
            let marker = command(&pg, &owners[0], VersionId::from_u64(4));
            assert!(payload(&marker).stale_payload.is_none());
            apply(&pg, &marker).unwrap();
            assert_marker(&pg, &marker, &owners);
            let StoredObject::Live(record) = &mut expected.record else {
                unreachable!()
            };
            record.became_noncurrent_at = Some(payload(&marker).last_modified_millis);
            assert_eq!(snapshot(&pg, &owners[0], source_version), expected);
            let delete = delete::command(
                &pg,
                &owners[0],
                VersionId::from_u64(4),
                DeleteObjectVersionMode::Specific,
            );
            delete::apply(&pg, &delete).unwrap();
            let StoredObject::Live(record) = &mut expected.record else {
                unreachable!()
            };
            record.became_noncurrent_at = None;
            assert_eq!(snapshot(&pg, &owners[0], source_version), expected);
            for owner in &owners {
                assert_eq!(append::load(&pg, owner), *owner);
            }
        }
    }
}

#[test]
fn staging_marker_null_displaces_noncurrent_null_not_current_numbered_payload() {
    for kind in 0..5 {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(StagingTarget::DirectPut).subject.object_pg.get(),
        )
        .unwrap();
        let old = delete::source(&pg, kind, VersionId::Null);
        let mut template = old
            .iter()
            .max_by_key(|o| o.subject.payload_generation.get())
            .unwrap()
            .clone();
        template.subject.originating_epoch = ClusterEpoch::new(8).unwrap();
        let new = object::next_attempt(&pg, &template, StagingTarget::DirectPut, 1);
        object::apply(&pg, &object::command(&pg, &new, VersionId::from_u64(4))).unwrap();
        let new = append::load(&pg, &new);
        let mut expected = snapshot(&pg, &new, VersionId::from_u64(4));
        let marker = command(&pg, &old[0], VersionId::Null);
        apply(&pg, &marker).unwrap();
        assert_marker(&pg, &marker, &old);
        let StoredObject::Live(record) = &mut expected.record else {
            unreachable!()
        };
        record.became_noncurrent_at = Some(payload(&marker).last_modified_millis);
        assert_eq!(snapshot(&pg, &new, VersionId::from_u64(4)), expected);
        assert_eq!(append::load(&pg, &new), new);
        let delete = delete::command(
            &pg,
            &new,
            VersionId::Null,
            DeleteObjectVersionMode::Specific,
        );
        delete::apply(&pg, &delete).unwrap();
        let StoredObject::Live(record) = &mut expected.record else {
            unreachable!()
        };
        record.became_noncurrent_at = None;
        assert_eq!(snapshot(&pg, &new, VersionId::from_u64(4)), expected);
        assert_eq!(append::load(&pg, &new), new);
    }
}

#[test]
fn staging_marker_payloadless_targets_and_null_marker_replacement_need_no_owner() {
    for version in [VersionId::Null, VersionId::from_u64(3)] {
        let dir = test_util::tempdir();
        let template = fixture(StagingTarget::DirectPut);
        let pg = PgStore::open(dir.path(), template.subject.object_pg.get()).unwrap();
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let first = command(&pg, &template, version);
        apply(&pg, &first).unwrap();
        assert_marker(&pg, &first, &[]);
        let next = command(&pg, &template, version);
        if version.is_null() {
            apply(&pg, &next).unwrap();
            assert_marker(&pg, &next, &[]);
            assert!(payload(&next).write_sequence > payload(&first).write_sequence);
        } else {
            let before = introduction::state(&pg);
            let changes = pg.conn.total_changes();
            assert!(matches!(apply(&pg, &next), Err(MarkerError::Target)));
            assert_eq!(pg.conn.total_changes(), changes);
            assert_eq!(introduction::state(&pg), before);
        }
        assert_eq!(
            pg.conn
                .query_row("SELECT COUNT(*) FROM payload_staging_owners", [], |row| row
                    .get::<_, u64>(0))
                .unwrap(),
            0
        );
    }
}

#[test]
fn staging_marker_crossed_authority_and_reclaim_fail_before_mutation() {
    for kind in [0, 2, 4] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(StagingTarget::DirectPut).subject.object_pg.get(),
        )
        .unwrap();
        let owners = delete::source(&pg, kind, VersionId::Null);
        let good = command(&pg, &owners[0], VersionId::Null);
        for field in [
            "operation",
            "bucket",
            "target",
            "epoch",
            "incarnation",
            "pg",
            "old-sequence",
            "future-sequence",
            "missing",
            "generation",
            "payload",
            "numbered",
        ] {
            let mut bad = payload(&good).clone();
            let mut id = good.id();
            match field {
                "operation" => {
                    bad.bucket_write_reservation.operation_kind =
                        crate::metadata_command::DELETE_CURRENT_OBJECT_BUCKET_WRITE_OPERATION_KIND
                            .into()
                }
                "bucket" => {
                    bad.bucket_write_reservation.bucket = crate::tests::bucket_name("crossed")
                }
                "target" => bad.bucket_write_reservation.target_context = Some("crossed".into()),
                "epoch" => {
                    bad.bucket_write_reservation.cluster_epoch = ClusterEpoch::new(10).unwrap()
                }
                "incarnation" => bad.bucket_write_reservation.bucket_incarnation_generation += 1,
                "pg" => {
                    id = MetadataCommandId::new(
                        id.cluster_epoch(),
                        PgId::new((pg.pg_id + 1) % 8),
                        id.log_index(),
                    )
                }
                "old-sequence" => bad.write_sequence -= 1,
                "future-sequence" => bad.write_sequence += 1,
                "missing" => bad.stale_payload = None,
                "numbered" => bad.version_id = VersionId::from_u64(4),
                _ => match bad.stale_payload.as_mut().unwrap() {
                    ObjectPayloadReclaimCommand::Segments(root) => {
                        if field == "generation" {
                            root.generation_id =
                                GenerationId::new(root.generation_id.get() + 100).unwrap();
                        } else {
                            root.segments.clear();
                        }
                    }
                    ObjectPayloadReclaimCommand::Multipart(root) => {
                        if field == "generation" {
                            root.generation_id =
                                GenerationId::new(root.generation_id.get() + 100).unwrap();
                        } else {
                            root.parts.clear();
                        }
                    }
                },
            }
            let bad =
                MetadataCommandEnvelope::new(id, MetadataCommandPayload::InsertDeleteMarker(bad));
            let before = introduction::state(&pg);
            let changes = pg.conn.total_changes();
            assert!(apply(&pg, &bad).is_err(), "{field}");
            assert_eq!(pg.conn.total_changes(), changes, "{field}");
            assert_eq!(introduction::state(&pg), before, "{field}");
        }
        apply(&pg, &good).unwrap();
        assert_marker(&pg, &good, &owners);
        // Fresh null-marker replacement cannot reuse the old live reclaim.
        let mut bad = payload(&command(&pg, &owners[0], VersionId::Null)).clone();
        bad.stale_payload = payload(&good).stale_payload.clone();
        let bad = MetadataCommandEnvelope::new(
            good.id(),
            MetadataCommandPayload::InsertDeleteMarker(bad),
        );
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        assert!(matches!(apply(&pg, &bad), Err(MarkerError::Target)));
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);
    }
}

#[test]
fn staging_marker_requires_owner_even_for_empty_payload_and_rejects_orphans() {
    for kind in [1, 3, 4] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(StagingTarget::DirectPut).subject.object_pg.get(),
        )
        .unwrap();
        let owners = delete::source(&pg, kind, VersionId::Null);
        let good = command(&pg, &owners[0], VersionId::Null);
        if kind == 4 {
            displacement::insert_orphan_segment(
                &pg,
                &owners[0],
                &crate::tests::multipart_upload_id("orphan-marker"),
                VersionId::Null,
                7,
            );
        } else {
            pg.conn
                .execute(
                    "DELETE FROM payload_staging_owners WHERE attempt = ?1",
                    [&owners[0].subject.id.attempt],
                )
                .unwrap();
        }
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        assert!(matches!(apply(&pg, &good), Err(MarkerError::Reclaim(_))));
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);
    }
}

#[test]
fn staging_marker_cannot_replace_numbered_live_version() {
    for kind in 0..5 {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(StagingTarget::DirectPut).subject.object_pg.get(),
        )
        .unwrap();
        let owners = delete::source(&pg, kind, VersionId::from_u64(3));
        let bad = command(&pg, &owners[0], VersionId::from_u64(3));
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        assert!(matches!(apply(&pg, &bad), Err(MarkerError::Target)));
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);
    }
}

#[test]
fn staging_marker_rolls_back_reclaim_marker_and_later_owner_transfers() {
    for kind in [0, 2, 4] {
        let mut events = vec![
            "BEFORE INSERT ON objects WHEN NEW.status = 1",
            "BEFORE UPDATE ON object_write_counters",
        ];
        if kind == 4 {
            events.extend(["BEFORE INSERT ON multipart_reclaims", "BEFORE DELETE ON object_parts", "BEFORE DELETE ON multipart_part_segments",
                "BEFORE UPDATE OF destination ON payload_staging_references WHEN NEW.destination = 3 AND OLD.part_number = 11",
                "BEFORE UPDATE OF lifecycle ON payload_staging_owners WHEN NEW.lifecycle = 3 AND OLD.part_number = 11"]);
        } else {
            events.extend(["BEFORE INSERT ON object_segments_reclaims", "BEFORE DELETE ON object_segments",
                "BEFORE UPDATE OF destination ON payload_staging_references WHEN NEW.destination = 3",
                "BEFORE UPDATE OF lifecycle ON payload_staging_owners WHEN NEW.lifecycle = 3"]);
        }
        for event in events {
            let dir = test_util::tempdir();
            let pg = PgStore::open(
                dir.path(),
                fixture(StagingTarget::DirectPut).subject.object_pg.get(),
            )
            .unwrap();
            let owners = delete::source(&pg, kind, VersionId::Null);
            let good = command(&pg, &owners[0], VersionId::Null);
            pg.conn.execute_batch("CREATE TEMP TABLE outer_work(value INTEGER); INSERT INTO outer_work VALUES (42)").unwrap();
            pg.conn.execute_batch(&format!("CREATE TEMP TRIGGER fail_marker {event} BEGIN SELECT RAISE(ABORT, 'staging marker failure'); END")).unwrap();
            let before = introduction::state(&pg);
            let error = apply(&pg, &good).unwrap_err();
            assert!(
                error.to_string().contains("staging marker failure"),
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
            pg.conn.execute_batch("DROP TRIGGER fail_marker").unwrap();
            apply(&pg, &good).unwrap();
            assert_marker(&pg, &good, &owners);
        }
    }
}
