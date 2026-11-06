// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Metadata-only deletion handoff: actual publication/deletion transactions,
//! original retained owners and canonical decoded commands, without shard I/O.

use super::super::delete::{apply_in_open_transaction, DeleteError};
use super::*;
use crate::metadata_command::{
    DeleteObjectVersionCommand, DeleteObjectVersionMode, DeleteObjectVersionTarget,
    InsertDeleteMarkerCommand, MetadataCommandEnvelope, MetadataCommandPayload,
    ObjectPayloadReclaimCommand, DELETE_CURRENT_OBJECT_BUCKET_WRITE_OPERATION_KIND,
    DELETE_OBJECT_VERSION_BUCKET_WRITE_OPERATION_KIND,
    INSERT_DELETE_MARKER_BUCKET_WRITE_OPERATION_KIND,
};
use crate::node_runtime::traits::PgMetadataStore;
use crate::pg_store::PgStore;
use crate::types::{
    MultipartReclaimRecord, ObjectSegmentsReclaimRecord, ObjectSegmentsReclaimSegmentRecord,
};
use crate::{MetadataError, ObjectLayout, OwnerIdentity, StoredObject, VersionId};

pub(super) fn source(pg: &PgStore, kind: u8, version: VersionId) -> Vec<StagingOwnerRecord> {
    let owners = if kind >= 3 {
        let owners = completion::setup(pg, if kind == 3 { 0 } else { 2 });
        completion::apply(pg, &completion::command(pg, &owners[0], version)).unwrap();
        owners
    } else {
        let (target, count) = match kind {
            0 => (StagingTarget::DirectPut, 1),
            1 => (StagingTarget::StreamPut, 0),
            2 => (StagingTarget::StreamPut, 2),
            _ => unreachable!(),
        };
        let owner = object::initialize(pg, target, count);
        object::apply(pg, &object::command(pg, &owner, version)).unwrap();
        vec![owner]
    };
    owners.iter().map(|owner| append::load(pg, owner)).collect()
}

pub(super) fn command(
    pg: &PgStore,
    owner: &StagingOwnerRecord,
    version: VersionId,
    mode: DeleteObjectVersionMode,
) -> MetadataCommandEnvelope {
    let subject = &owner.subject;
    let target = match pg
        .get_object_version(&subject.bucket, &subject.key, version)
        .unwrap()
    {
        StoredObject::DeleteMarker(_) => DeleteObjectVersionTarget::DeleteMarker {
            write_sequence: pg
                .object_write_sequence(subject.bucket.as_str(), subject.key.as_str(), version)
                .unwrap()
                .unwrap(),
        },
        StoredObject::Live(record) => {
            let payload = match record.layout {
                ObjectLayout::Standard => {
                    ObjectPayloadReclaimCommand::Segments(ObjectSegmentsReclaimRecord {
                        bucket: subject.bucket.clone(),
                        key: subject.key.clone(),
                        generation_id: record.generation_id,
                        created_at: 4_000,
                        segments: pg
                            .get_object_segments(&subject.bucket, &subject.key, version)
                            .unwrap()
                            .into_iter()
                            .map(|s| ObjectSegmentsReclaimSegmentRecord {
                                segment_index: s.segment_index,
                                segment_okh: s.segment_okh,
                                segment_vid: s.segment_vid,
                                data_pg_id: s.data_pg_id,
                                ec: EcShape {
                                    k: s.ec_k,
                                    m: s.ec_m,
                                },
                            })
                            .collect(),
                    })
                }
                ObjectLayout::MultipartManifest { .. } => {
                    let parts = pg
                        .get_object_parts(&subject.bucket, &subject.key, version)
                        .unwrap();
                    let segments: Vec<_> = parts
                        .iter()
                        .flat_map(|p| {
                            pg.get_multipart_part_segments(
                                &subject.bucket,
                                &subject.key,
                                version,
                                p.part_number,
                            )
                            .unwrap()
                        })
                        .collect();
                    ObjectPayloadReclaimCommand::Multipart(
                        MultipartReclaimRecord::from_object_parts(
                            &subject.bucket,
                            &subject.key,
                            record.generation_id,
                            4_000,
                            &parts,
                            &segments,
                        ),
                    )
                }
            };
            DeleteObjectVersionTarget::Live {
                generation_id: record.generation_id,
                layout: record.layout,
                payload,
            }
        }
    };
    let mut reservation = proof(subject, operation(mode));
    reservation.cluster_epoch = ClusterEpoch::new(8).unwrap();
    MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            reservation.cluster_epoch,
            subject.object_pg,
            MetadataCommandLogIndex::new(5000).unwrap(),
        ),
        MetadataCommandPayload::DeleteObjectVersion(Box::new(DeleteObjectVersionCommand {
            bucket_write_reservation: reservation,
            bucket: subject.bucket.clone(),
            key: subject.key.clone(),
            version_id: version,
            mode,
            target,
        })),
    )
}

fn operation(mode: DeleteObjectVersionMode) -> &'static str {
    match mode {
        DeleteObjectVersionMode::Current => DELETE_CURRENT_OBJECT_BUCKET_WRITE_OPERATION_KIND,
        DeleteObjectVersionMode::Specific => DELETE_OBJECT_VERSION_BUCKET_WRITE_OPERATION_KIND,
    }
}

fn payload(command: &MetadataCommandEnvelope) -> &DeleteObjectVersionCommand {
    let MetadataCommandPayload::DeleteObjectVersion(payload) = command.payload() else {
        unreachable!()
    };
    payload
}

pub(super) fn apply(pg: &PgStore, command: &MetadataCommandEnvelope) -> Result<(), DeleteError> {
    let decoded = crate::metadata_command::decode_metadata_command_envelope(
        &command.command_bytes(),
        &crate::node_runtime::MetadataCommandDecodeAuthority::new_for_test(),
    )
    .unwrap();
    apply_in_open_transaction(pg, &topology(), &decoded)
}

fn assert_deleted(pg: &PgStore, command: &MetadataCommandEnvelope, owners: &[StagingOwnerRecord]) {
    let command = payload(command);
    assert!(matches!(
        pg.get_object_version(&command.bucket, &command.key, command.version_id),
        Err(MetadataError::ObjectNotFound)
    ));
    let DeleteObjectVersionTarget::Live {
        generation_id,
        payload,
        ..
    } = &command.target
    else {
        unreachable!()
    };
    match payload {
        ObjectPayloadReclaimCommand::Segments(expected) => assert_eq!(
            pg.get_object_segments_reclaim(&command.bucket, &command.key, *generation_id)
                .unwrap(),
            Some(expected.clone())
        ),
        ObjectPayloadReclaimCommand::Multipart(expected) => assert_eq!(
            pg.get_multipart_reclaim(&command.bucket, &command.key, *generation_id)
                .unwrap(),
            Some(expected.clone())
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
    let rows: u64 = pg.conn.query_row("SELECT COUNT(*) FROM multipart_part_segments WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
        rusqlite::params![command.bucket, command.key, command.version_id.to_u64() as i64], |row| row.get(0)).unwrap();
    assert_eq!(rows, 0);
    for old in owners {
        let mut expected = old.clone();
        if matches!(
            old.lifecycle,
            StagingLifecycle::Published
                | StagingLifecycle::Retiring(StagingRetirement::PublishedRetained)
        ) {
            expected.lifecycle = StagingLifecycle::Retiring(StagingRetirement::PublishedRetained);
            for reference in &mut expected.references {
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
        assert_eq!(append::load(pg, old), expected);
    }
}

#[test]
fn staging_epoch_reset_deletion_preserves_exact_reclaim_ownership() {
    for kind in 0..5 {
        for version in [VersionId::Null, VersionId::from_u64(3)] {
            for mode in [
                DeleteObjectVersionMode::Current,
                DeleteObjectVersionMode::Specific,
            ] {
                let dir = test_util::tempdir();
                let pg = PgStore::open(
                    dir.path(),
                    fixture(StagingTarget::DirectPut).subject.object_pg.get(),
                )
                .unwrap();
                let owners = source(&pg, kind, version);
                let original = command(&pg, &owners[0], version, mode);
                let command =
                    epoch_order::at_epoch_start(&pg, &original, original.id().cluster_epoch());
                apply(&pg, &command).unwrap();
                assert_deleted(&pg, &command, &owners);
                let pg = epoch_order::record_and_reopen(pg, dir.path(), &command);
                assert_deleted(&pg, &command, &owners);
            }
        }
    }
}

#[test]
fn staging_delete_transfers_null_and_numbered_objects_atomically_through_restart() {
    for kind in 0..5 {
        for version in [VersionId::Null, VersionId::from_u64(3)] {
            for mode in [
                DeleteObjectVersionMode::Current,
                DeleteObjectVersionMode::Specific,
            ] {
                for retiring in [false, true] {
                    let dir = test_util::tempdir();
                    let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg.get();
                    let pg = PgStore::open(dir.path(), pg_id).unwrap();
                    let mut owners = source(&pg, kind, version);
                    if retiring {
                        pg.conn.execute_batch("UPDATE payload_staging_owners SET lifecycle = 3 WHERE lifecycle = 1").unwrap();
                        owners = owners
                            .iter()
                            .map(|owner| append::load(&pg, owner))
                            .collect();
                    }
                    let delete = command(&pg, &owners[0], version, mode);
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
                        apply(&pg, &delete)
                    })
                    .unwrap();
                    assert_deleted(&pg, &delete, &owners);
                    let before = introduction::state(&pg);
                    let changes = pg.conn.total_changes();
                    assert!(matches!(
                        apply(&pg, &delete),
                        Err(DeleteError::Metadata(MetadataError::ObjectNotFound))
                    ));
                    assert_eq!(pg.conn.total_changes(), changes);
                    assert_eq!(introduction::state(&pg), before);
                    pg.conn.execute_batch("COMMIT").unwrap();
                    drop(pg);
                    let pg = PgStore::open(dir.path(), pg_id).unwrap();
                    pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
                    assert_deleted(&pg, &delete, &owners);
                }
            }
        }
    }
}

#[test]
fn staging_delete_rejects_crossed_command_proof_and_target_without_mutation() {
    for kind in [0, 2, 4] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(StagingTarget::DirectPut).subject.object_pg.get(),
        )
        .unwrap();
        let owners = source(&pg, kind, VersionId::Null);
        let good = command(
            &pg,
            &owners[0],
            VersionId::Null,
            DeleteObjectVersionMode::Specific,
        );
        for field in [
            "mode",
            "operation",
            "bucket",
            "target",
            "epoch",
            "incarnation",
            "generation",
            "layout",
            "reclaim",
            "marker",
            "pg",
        ] {
            let mut bad = payload(&good).clone();
            let mut id = good.id();
            match field {
                "mode" => bad.mode = DeleteObjectVersionMode::Current,
                "operation" => {
                    bad.bucket_write_reservation.operation_kind =
                        PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND.into()
                }
                "bucket" => {
                    bad.bucket_write_reservation.bucket = crate::tests::bucket_name("crossed")
                }
                "target" => bad.bucket_write_reservation.target_context = Some("crossed".into()),
                "epoch" => {
                    bad.bucket_write_reservation.cluster_epoch = ClusterEpoch::new(9).unwrap()
                }
                "incarnation" => bad.bucket_write_reservation.bucket_incarnation_generation += 1,
                "marker" => {
                    bad.target = DeleteObjectVersionTarget::DeleteMarker { write_sequence: 1 }
                }
                "pg" => {
                    id = MetadataCommandId::new(
                        id.cluster_epoch(),
                        PgId::new((pg.pg_id + 1) % 8),
                        id.log_index(),
                    )
                }
                _ => {
                    let DeleteObjectVersionTarget::Live {
                        generation_id,
                        layout,
                        payload,
                    } = &mut bad.target
                    else {
                        unreachable!()
                    };
                    match field {
                        "generation" => {
                            *generation_id = GenerationId::new(generation_id.get() + 100).unwrap()
                        }
                        "layout" => {
                            *layout = if *layout == ObjectLayout::Standard {
                                ObjectLayout::MultipartManifest {
                                    parts_count: std::num::NonZeroU32::new(1).unwrap(),
                                }
                            } else {
                                ObjectLayout::Standard
                            }
                        }
                        "reclaim" => match payload {
                            ObjectPayloadReclaimCommand::Segments(root) => root.segments.clear(),
                            ObjectPayloadReclaimCommand::Multipart(root) => root.parts.clear(),
                        },
                        _ => unreachable!(),
                    }
                }
            }
            let bad = MetadataCommandEnvelope::new(
                id,
                MetadataCommandPayload::DeleteObjectVersion(Box::new(bad)),
            );
            let before = introduction::state(&pg);
            let changes = pg.conn.total_changes();
            assert!(apply(&pg, &bad).is_err(), "{field}");
            assert_eq!(pg.conn.total_changes(), changes, "{field}");
            assert_eq!(introduction::state(&pg), before, "{field}");
        }
        apply(&pg, &good).unwrap();
        assert_deleted(&pg, &good, &owners);
    }
}

#[test]
fn staging_delete_current_cannot_remove_noncurrent_but_specific_preserves_new_owner() {
    for kind in 0..5 {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(StagingTarget::DirectPut).subject.object_pg.get(),
        )
        .unwrap();
        let owners = source(&pg, kind, VersionId::from_u64(3));
        let mut template = owners
            .iter()
            .max_by_key(|o| o.subject.payload_generation.get())
            .unwrap()
            .clone();
        template.subject.originating_epoch = ClusterEpoch::new(8).unwrap();
        let new = object::next_attempt(&pg, &template, StagingTarget::DirectPut, 1);
        let published = object::command(&pg, &new, VersionId::from_u64(4));
        object::apply(&pg, &published).unwrap();
        let new = append::load(&pg, &new);
        assert_eq!(
            pg.get_object_meta(&new.subject.bucket, &new.subject.key)
                .unwrap()
                .version_id(),
            VersionId::from_u64(4)
        );
        let bad = command(
            &pg,
            &owners[0],
            VersionId::from_u64(3),
            DeleteObjectVersionMode::Current,
        );
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        assert!(matches!(apply(&pg, &bad), Err(DeleteError::Target)));
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);
        let good = command(
            &pg,
            &owners[0],
            VersionId::from_u64(3),
            DeleteObjectVersionMode::Specific,
        );
        apply(&pg, &good).unwrap();
        assert_deleted(&pg, &good, &owners);
        object::assert_object(&pg, &published);
        assert_eq!(append::load(&pg, &new), new);
    }
}

#[test]
fn staging_delete_rejects_missing_empty_owner_and_orphan_multipart_segments() {
    for kind in [1, 3, 4] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(StagingTarget::DirectPut).subject.object_pg.get(),
        )
        .unwrap();
        let owners = source(&pg, kind, VersionId::Null);
        let good = command(
            &pg,
            &owners[0],
            VersionId::Null,
            DeleteObjectVersionMode::Specific,
        );
        if kind == 4 {
            displacement::insert_orphan_segment(
                &pg,
                &owners[0],
                &crate::tests::multipart_upload_id("orphan-delete"),
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
        assert!(matches!(apply(&pg, &good), Err(DeleteError::Reclaim(_))));
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);
    }
}

#[test]
fn staging_delete_current_restores_older_version_without_transferring_its_owner() {
    for kind in 0..5 {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(StagingTarget::DirectPut).subject.object_pg.get(),
        )
        .unwrap();
        let owners = source(&pg, kind, VersionId::from_u64(3));
        let subject = &owners[0].subject;
        let original = pg.get_object_meta(&subject.bucket, &subject.key).unwrap();
        let segments = pg
            .get_object_segments(&subject.bucket, &subject.key, original.version_id())
            .unwrap();
        let parts = pg
            .get_object_parts(&subject.bucket, &subject.key, original.version_id())
            .unwrap();
        let part_segments: Vec<_> = parts
            .iter()
            .map(|part| {
                pg.get_multipart_part_segments(
                    &subject.bucket,
                    &subject.key,
                    original.version_id(),
                    part.part_number,
                )
                .unwrap()
            })
            .collect();
        let mut template = owners
            .iter()
            .max_by_key(|o| o.subject.payload_generation.get())
            .unwrap()
            .clone();
        template.subject.originating_epoch = ClusterEpoch::new(8).unwrap();
        let new = object::next_attempt(&pg, &template, StagingTarget::DirectPut, 1);
        object::apply(&pg, &object::command(&pg, &new, VersionId::from_u64(4))).unwrap();
        let new = append::load(&pg, &new);
        assert_eq!(
            pg.get_object_meta(&subject.bucket, &subject.key)
                .unwrap()
                .version_id(),
            VersionId::from_u64(4)
        );
        let delete = command(
            &pg,
            &new,
            VersionId::from_u64(4),
            DeleteObjectVersionMode::Current,
        );
        apply(&pg, &delete).unwrap();
        assert_deleted(&pg, &delete, &[new]);
        assert_eq!(
            pg.get_object_meta(&subject.bucket, &subject.key).unwrap(),
            original
        );
        assert_eq!(
            pg.get_object_segments(&subject.bucket, &subject.key, original.version_id())
                .unwrap(),
            segments
        );
        assert_eq!(
            pg.get_object_parts(&subject.bucket, &subject.key, original.version_id())
                .unwrap(),
            parts
        );
        for (part, expected) in parts.iter().zip(part_segments) {
            assert_eq!(
                pg.get_multipart_part_segments(
                    &subject.bucket,
                    &subject.key,
                    original.version_id(),
                    part.part_number
                )
                .unwrap(),
                expected
            );
        }
        for owner in &owners {
            assert_eq!(append::load(&pg, owner), *owner);
        }
    }
}

#[test]
fn staging_delete_rolls_back_reclaim_object_and_later_owner_transfers() {
    for kind in [0, 2, 4] {
        let mut events = vec![
            "BEFORE DELETE ON objects",
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
            let owners = source(&pg, kind, VersionId::from_u64(3));
            let good = command(
                &pg,
                &owners[0],
                VersionId::from_u64(3),
                DeleteObjectVersionMode::Specific,
            );
            pg.conn.execute_batch("CREATE TEMP TABLE outer_work(value INTEGER); INSERT INTO outer_work VALUES (42)").unwrap();
            pg.conn.execute_batch(&format!("CREATE TEMP TRIGGER fail_delete {event} BEGIN SELECT RAISE(ABORT, 'staging deletion failure'); END")).unwrap();
            let before = introduction::state(&pg);
            let error = apply(&pg, &good).unwrap_err();
            assert!(
                error.to_string().contains("staging deletion failure"),
                "{event}: {error}"
            );
            assert_eq!(introduction::state(&pg), before, "{event}");
            assert!(!pg.conn.is_autocommit());
            assert_eq!(
                pg.conn
                    .query_row("SELECT value FROM outer_work", [], |r| r.get::<_, i64>(0))
                    .unwrap(),
                42
            );
            pg.conn.execute_batch("DROP TRIGGER fail_delete").unwrap();
            apply(&pg, &good).unwrap();
            assert_deleted(&pg, &good, &owners);
        }
    }
}

#[test]
fn staging_delete_payloadless_marker_preserves_live_version_and_owner() {
    for version in [VersionId::Null, VersionId::from_u64(4)] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(StagingTarget::DirectPut).subject.object_pg.get(),
        )
        .unwrap();
        let owners = source(&pg, 2, VersionId::from_u64(3));
        let subject = &owners[0].subject;
        let mut reservation = proof(subject, INSERT_DELETE_MARKER_BUCKET_WRITE_OPERATION_KIND);
        reservation.cluster_epoch = ClusterEpoch::new(8).unwrap();
        let marker = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                reservation.cluster_epoch,
                subject.object_pg,
                MetadataCommandLogIndex::new(1000).unwrap(),
            ),
            MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
                bucket_write_reservation: reservation,
                bucket: subject.bucket.clone(),
                key: subject.key.clone(),
                version_id: version,
                owner: OwnerIdentity::from_principal("owner"),
                write_sequence: pg
                    .next_object_write_sequence(subject.bucket.as_str(), subject.key.as_str())
                    .unwrap(),
                last_modified_millis: 4_000,
                stale_payload: None,
            }),
        );
        pg.apply_metadata_command(&marker).unwrap();
        let good = command(&pg, &owners[0], version, DeleteObjectVersionMode::Specific);
        let mut stale = payload(&good).clone();
        let DeleteObjectVersionTarget::DeleteMarker { write_sequence } = &mut stale.target else {
            unreachable!()
        };
        *write_sequence += 1;
        let stale = MetadataCommandEnvelope::new(
            good.id(),
            MetadataCommandPayload::DeleteObjectVersion(Box::new(stale)),
        );
        let current = command(&pg, &owners[0], version, DeleteObjectVersionMode::Current);
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        for bad in [stale, current] {
            assert!(matches!(apply(&pg, &bad), Err(DeleteError::Target)));
            assert_eq!(pg.conn.total_changes(), changes);
            assert_eq!(introduction::state(&pg), before);
        }
        apply(&pg, &good).unwrap();
        assert!(matches!(
            pg.get_object_version(&subject.bucket, &subject.key, version),
            Err(MetadataError::ObjectNotFound)
        ));
        assert_eq!(
            pg.get_object_meta(&subject.bucket, &subject.key)
                .unwrap()
                .version_id(),
            VersionId::from_u64(3)
        );
        assert_eq!(append::load(&pg, &owners[0]), owners[0]);
        assert!(pg
            .get_object_segments_reclaim(&subject.bucket, &subject.key, subject.payload_generation)
            .unwrap()
            .is_none());
    }
}
