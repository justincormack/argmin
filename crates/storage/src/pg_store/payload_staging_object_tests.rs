// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Metadata-only transaction fixtures: no shard files or payload decryption.
//! Real introduction/append/publication mutations run in one caller-owned txn.

use super::super::object::{apply_in_open_transaction, ObjectError};
use super::*;
use crate::clock::with_time_and_monotonic_override;
use crate::metadata_command::{
    CommitDirectPutObjectCommand, MetadataCommandEnvelope, MetadataCommandPayload,
};
use crate::node_runtime::traits::PgMetadataStore;
use crate::pg_store::PgStore;
use crate::types::{ObjectEncryptionSubject, SegmentShardIntegrityRecord};
use crate::{
    AclGrants, MetadataError, ObjectEtag, ObjectLayout, ObjectLockState, ObjectSegmentRecord,
    OwnerIdentity, PutLiveObjectReq, StoredObject, VersionId,
};

pub(super) fn initialize(pg: &PgStore, target: StagingTarget, count: usize) -> StagingOwnerRecord {
    let mut owner = append::initialize(pg, target);
    if owner.subject.target == StagingTarget::StreamPut {
        for _ in 0..count {
            let command = append::command(&owner);
            super::super::descriptor::apply_append_for_test(
                pg,
                &topology(),
                &command,
                &append::next(pg, &owner),
            )
            .unwrap();
            owner = append::load(pg, &owner);
        }
    }
    owner
}

pub(super) fn command(
    pg: &PgStore,
    owner: &StagingOwnerRecord,
    version: VersionId,
) -> MetadataCommandEnvelope {
    let subject = &owner.subject;
    let staged = if subject.target == StagingTarget::DirectPut {
        vec![append::segment(owner)]
    } else {
        pg.list_stream_segments(&subject.id.attempt).unwrap()
    };
    let segments: Vec<_> = staged
        .iter()
        .map(|segment| ObjectSegmentRecord {
            bucket: subject.bucket.clone(),
            key: subject.key.clone(),
            version_id: version,
            segment_index: segment.segment_index,
            size: segment.size,
            segment_crc64: segment.segment_crc64,
            segment_okh: segment.segment_okh,
            segment_vid: segment.segment_vid,
            data_pg_id: segment.data_pg_id,
            placement_cluster_epoch: segment.placement_cluster_epoch,
            ec_k: segment.ec_k,
            ec_m: segment.ec_m,
            shard_integrity: segment.shard_integrity.clone(),
        })
        .collect();
    let provisional = crate::tests::metadata_only_test_encryption();
    let encryption_subject = ObjectEncryptionSubject::standard_object(
        subject.bucket_incarnation,
        &subject.bucket,
        &subject.key,
        subject.payload_generation,
        &provisional,
    );
    let encryption = provisional
        .bind_committed_object_ciphertexts(
            &encryption_subject,
            [0x31; crate::types::OBJECT_ENCRYPTION_WRAP_NONCE_LEN],
            [0xB3; crate::types::OBJECT_ENCRYPTION_WRAPPED_DEK_LEN],
            [0; crate::types::OBJECT_ENCRYPTION_CHECKSUM_NONCE_LEN],
            Vec::new(),
        )
        .unwrap();
    let ResourceResponsibility::Live(resource) = &owner.resources.creation else {
        unreachable!()
    };
    let mut creation = proof(subject, subject.creation_operation());
    creation
        .reservation_id
        .clone_from(&resource.identity.reservation_id);
    creation
        .owner_token
        .clone_from(&resource.identity.owner_token);
    MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            subject.originating_epoch,
            subject.object_pg,
            MetadataCommandLogIndex::new(subject.id.introduction.log_index().get() + 19).unwrap(),
        ),
        MetadataCommandPayload::CommitDirectPutObject(Box::new(CommitDirectPutObjectCommand {
            object: PutLiveObjectReq {
                bucket: subject.bucket.clone(),
                key: subject.key.clone(),
                version_id: version,
                owner: OwnerIdentity::from_principal("owner"),
                acl_grants: AclGrants::default(),
                public_read: false,
                generation_id: subject.payload_generation,
                size: segments.iter().map(|s| s.size).sum(),
                etag: ObjectEtag::single_part(123),
                ec: owner.windows[0].geometry.ec,
                layout: ObjectLayout::Standard,
                tags: None,
                metadata_blob: None,
                system_metadata_blob: None,
                object_lock: ObjectLockState::default(),
                encryption,
            },
            segments,
            generation_reservation_id: subject.id.attempt.clone(),
            write_sequence: pg
                .next_object_write_sequence(subject.bucket.as_str(), subject.key.as_str())
                .unwrap(),
            last_modified_millis: 3_000,
            stale_payload: None,
            bucket_write_reservation: creation,
        })),
    )
}

pub(super) fn payload(command: &MetadataCommandEnvelope) -> &CommitDirectPutObjectCommand {
    let MetadataCommandPayload::CommitDirectPutObject(command) = command.payload() else {
        unreachable!()
    };
    command
}

fn next_direct_attempt(pg: &PgStore, old: &StagingOwnerRecord) -> StagingOwnerRecord {
    next_attempt(pg, old, StagingTarget::DirectPut, 1)
}

pub(super) fn next_attempt(
    pg: &PgStore,
    old: &StagingOwnerRecord,
    target: StagingTarget,
    count: usize,
) -> StagingOwnerRecord {
    use super::super::capture::{StagingCapture, StagingIntroduction};
    use crate::node_runtime::payload_placement::PayloadPlacementInput;
    use crate::types::AdmittedRouteEffectFence;
    let config = capture::config(old);
    let operation = match target {
        StagingTarget::DirectPut => PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
        StagingTarget::StreamPut => PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
        StagingTarget::UploadPart { .. } => unreachable!(),
    };
    let mut creation = proof(&old.subject, operation);
    creation.reservation_id.push_str("-next");
    creation.owner_token.push_str("-next");
    let mut owner = with_time_and_monotonic_override(2_000, 71_000, || {
        StagingCapture {
            topology: &topology(),
            placement: PayloadPlacementInput::from_server_config(&config),
            effect_fence: AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
        }
        .introduction(
            pg,
            config.node_id,
            StagingIntroduction {
                id: StagingOwnerId {
                    attempt: crate::tests::stream_session_id("next-object"),
                    introduction: MetadataCommandId::new(
                        config.cluster_epoch,
                        old.subject.object_pg,
                        MetadataCommandLogIndex::new(30).unwrap(),
                    ),
                },
                bucket: old.subject.bucket.clone(),
                key: old.subject.key.clone(),
                payload_generation: GenerationId::new(old.subject.payload_generation.get() + 1)
                    .unwrap(),
                target,
            },
            &creation,
        )
        .unwrap()
    });
    let raw = introduction::command(&owner);
    let mut introduction_payload = raw.payload().clone();
    if let MetadataCommandPayload::CreateStreamUpload(create) = &mut introduction_payload {
        create.bucket_write_reservation = creation;
    }
    super::super::descriptor::apply_introduction_for_test(
        pg,
        &topology(),
        &MetadataCommandEnvelope::new(raw.id(), introduction_payload),
        &owner,
    )
    .unwrap();
    if owner.subject.target == StagingTarget::StreamPut {
        for _ in 0..count {
            super::super::descriptor::apply_append_for_test(
                pg,
                &topology(),
                &append::command(&owner),
                &append::next(pg, &owner),
            )
            .unwrap();
            owner = append::load(pg, &owner);
        }
    }
    owner
}

pub(super) fn apply(pg: &PgStore, command: &MetadataCommandEnvelope) -> Result<(), ObjectError> {
    let decoded = crate::metadata_command::decode_metadata_command_envelope(
        &command.command_bytes(),
        &crate::node_runtime::MetadataCommandDecodeAuthority::new_for_test(),
    )
    .unwrap();
    apply_in_open_transaction(pg, &topology(), &decoded)
}

pub(super) fn expected(
    owner: &StagingOwnerRecord,
    command: &MetadataCommandEnvelope,
) -> StagingOwnerRecord {
    let mut after = owner.clone();
    after.lifecycle = StagingLifecycle::Published;
    let ResourceResponsibility::Live(generation) = &owner.resources.generation else {
        unreachable!()
    };
    after.resources.generation = ResourceResponsibility::ReleaseDue(generation.clone());
    let ResourceResponsibility::Live(creation) = &owner.resources.creation else {
        unreachable!()
    };
    after.resources.creation = ResourceResponsibility::ReleaseDue(creation.clone());
    after.references = payload(command)
        .segments
        .iter()
        .map(|segment| StagingReference {
            window_index: segment.segment_index,
            segment_hash: segment.segment_okh,
            physical_vid: segment.segment_vid,
            logical_size: segment.size,
            stored_size: payload(command)
                .object
                .encryption
                .segment_stored_size(segment.size)
                .unwrap(),
            destination: StagingDestination::Object {
                generation: owner.subject.payload_generation,
                part_number: None,
            },
        })
        .collect();
    after
}

pub(super) fn assert_object(pg: &PgStore, command: &MetadataCommandEnvelope) {
    let command = payload(command);
    let object = &command.object;
    let StoredObject::Live(stored) = pg
        .get_object_version(&object.bucket, &object.key, object.version_id)
        .unwrap()
    else {
        panic!("expected live object")
    };
    assert_eq!(stored.generation_id, object.generation_id);
    assert_eq!(stored.size, object.size);
    assert_eq!(stored.ec, object.ec);
    assert_eq!(stored.encryption, object.encryption);
    assert_eq!(stored.etag, object.etag);
    assert_eq!(
        pg.get_object_segments(&object.bucket, &object.key, object.version_id)
            .unwrap(),
        command.segments
    );
    assert!(matches!(
        pg.get_object_generation_reservation(
            &object.bucket,
            &object.key,
            &command.generation_reservation_id
        ),
        Err(MetadataError::ObjectGenerationReservationNotFound { .. })
    ));
    assert!(matches!(
        pg.get_stream_upload(&command.generation_reservation_id),
        Err(MetadataError::StreamSessionNotFound { .. })
    ));
}

#[test]
fn staging_epoch_reset_object_publication_preserves_original_payload_and_proof() {
    for (target, count) in [
        (StagingTarget::DirectPut, 1),
        (StagingTarget::StreamPut, 0),
        (StagingTarget::StreamPut, 2),
    ] {
        for version in [VersionId::Null, VersionId::from_u64(3)] {
            let dir = test_util::tempdir();
            let pg =
                PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
            let owner = initialize(&pg, target.clone(), count);
            let original = command(&pg, &owner, version);
            let reissued =
                epoch_order::at_epoch_start(&pg, &original, ClusterEpoch::new(6).unwrap());
            apply(&pg, &reissued).unwrap();
            assert_object(&pg, &reissued);
            assert_eq!(append::load(&pg, &owner), expected(&owner, &reissued));
            let pg = epoch_order::record_and_reopen(pg, dir.path(), &reissued);
            assert_object(&pg, &reissued);
            assert_eq!(append::load(&pg, &owner), expected(&owner, &reissued));
        }
    }
}

#[test]
fn staging_object_publication_preserves_exact_windows_and_proof_after_reopen() {
    for (target, count) in [
        (StagingTarget::DirectPut, 1),
        (StagingTarget::StreamPut, 0),
        (StagingTarget::StreamPut, 1),
        (StagingTarget::StreamPut, 2),
    ] {
        for version in [VersionId::Null, VersionId::from_u64(3)] {
            let dir = test_util::tempdir();
            let pg_id = fixture(target.clone()).subject.object_pg.get();
            let pg = PgStore::open(dir.path(), pg_id).unwrap();
            let owner = initialize(&pg, target.clone(), count);
            let original = command(&pg, &owner, version);
            // A heartbeat can change the observed deadline, not proof identity.
            let mut renewed = payload(&original).clone();
            renewed.bucket_write_reservation.lease_deadline += 500;
            let command = MetadataCommandEnvelope::new(
                original.id(),
                MetadataCommandPayload::CommitDirectPutObject(Box::new(renewed)),
            );
            let expected = expected(&owner, &command);
            pg.conn.execute_batch("COMMIT").unwrap();
            drop(pg);
            let pg = PgStore::open(dir.path(), pg_id).unwrap();
            assert_ne!(
                pg.stream_allocation_issuer(owner.subject.issuer.node_id)
                    .unwrap(),
                owner.subject.issuer
            );
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            with_time_and_monotonic_override(9_000_000, 90_000_000, || {
                apply(&pg, &command).unwrap()
            });
            assert_object(&pg, &command);
            assert_eq!(append::load(&pg, &owner), expected);
            let before = introduction::state(&pg);
            let changes = pg.conn.total_changes();
            assert!(matches!(
                apply(&pg, &command),
                Err(ObjectError::CommandBinding)
            ));
            assert_eq!(pg.conn.total_changes(), changes);
            assert_eq!(introduction::state(&pg), before);
            pg.conn.execute_batch("COMMIT").unwrap();
            drop(pg);
            let pg = PgStore::open(dir.path(), pg_id).unwrap();
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            assert_object(&pg, &command);
            assert_eq!(append::load(&pg, &owner), expected);
        }
    }
}

#[test]
fn staging_object_replaces_payloadless_null_marker_and_retains_consumed_lookahead() {
    let dir = test_util::tempdir();
    let template = fixture(StagingTarget::StreamPut);
    let pg = PgStore::open(dir.path(), template.subject.object_pg.get()).unwrap();
    pg.put_object_meta(&crate::PutObjectReq::DeleteMarker(
        crate::PutDeleteMarkerReq {
            bucket: template.subject.bucket.clone(),
            key: template.subject.key.clone(),
            version_id: VersionId::Null,
            owner: OwnerIdentity::from_principal("owner"),
        },
    ))
    .unwrap();
    assert!(matches!(
        pg.get_object_version(
            &template.subject.bucket,
            &template.subject.key,
            VersionId::Null
        )
        .unwrap(),
        StoredObject::DeleteMarker(_)
    ));
    let owner = initialize(&pg, StagingTarget::StreamPut, 2);
    // Preparation may have consumed the last slot without publishing an append.
    // Its entire physical window remains an obligation, not selected payload.
    pg.conn
        .execute(
            "UPDATE stream_uploads SET next_segment_vid = ?1 WHERE session_id = ?2",
            rusqlite::params![
                owner.windows.last().unwrap().end.get(),
                owner.subject.id.attempt
            ],
        )
        .unwrap();
    let command = command(&pg, &owner, VersionId::Null);
    apply(&pg, &command).unwrap();
    assert_object(&pg, &command);
    let published = append::load(&pg, &owner);
    assert_eq!(published, expected(&owner, &command));
    assert_eq!(published.references.len(), 2);
    assert_eq!(published.windows.len(), 3);
}

#[test]
fn staging_object_requires_reclaim_for_null_displacement_and_preserves_numbered_history() {
    use crate::metadata_command::ObjectPayloadReclaimCommand;
    use crate::types::{ObjectSegmentsReclaimRecord, ObjectSegmentsReclaimSegmentRecord};
    for version in [VersionId::Null, VersionId::from_u64(3)] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(StagingTarget::DirectPut).subject.object_pg.get(),
        )
        .unwrap();
        let old = initialize(&pg, StagingTarget::DirectPut, 1);
        let first = command(&pg, &old, version);
        apply(&pg, &first).unwrap();
        let published = append::load(&pg, &old);
        let next = next_direct_attempt(&pg, &old);
        let mut replacement = payload(&command(&pg, &next, version)).clone();
        for supplied_reclaim in [false, true] {
            if version.is_null() && supplied_reclaim {
                // The exact null overwrite is covered by the transfer matrix;
                // a numbered version may never be displaced by this command.
                continue;
            }
            if supplied_reclaim {
                replacement.stale_payload = Some(ObjectPayloadReclaimCommand::Segments(
                    ObjectSegmentsReclaimRecord {
                        bucket: old.subject.bucket.clone(),
                        key: old.subject.key.clone(),
                        generation_id: old.subject.payload_generation,
                        created_at: 3_000,
                        segments: payload(&first)
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
                    },
                ));
            }
            let replacement = MetadataCommandEnvelope::new(
                MetadataCommandId::new(
                    next.subject.originating_epoch,
                    next.subject.object_pg,
                    MetadataCommandLogIndex::new(49).unwrap(),
                ),
                MetadataCommandPayload::CommitDirectPutObject(Box::new(replacement.clone())),
            );
            let before = introduction::state(&pg);
            let changes = pg.conn.total_changes();
            assert!(matches!(
                apply(&pg, &replacement),
                Err(ObjectError::DisplacedPayload)
            ));
            assert_eq!(pg.conn.total_changes(), changes);
            assert_eq!(introduction::state(&pg), before);
        }
        // Publishing another numbered version does not displace the old null
        // or numbered row: its exact references/resources remain protected.
        let new_version = command(&pg, &next, VersionId::from_u64(4));
        apply(&pg, &new_version).unwrap();
        assert_object(&pg, &first);
        assert_object(&pg, &new_version);
        assert_eq!(append::load(&pg, &old), published);
        assert_eq!(append::load(&pg, &next), expected(&next, &new_version));
    }
}

#[test]
fn staging_object_rejects_crossed_object_ec_without_mutation() {
    for (target, count) in [
        (StagingTarget::DirectPut, 1),
        (StagingTarget::StreamPut, 0),
        (StagingTarget::StreamPut, 1),
        (StagingTarget::StreamPut, 2),
    ] {
        let dir = test_util::tempdir();
        let pg =
            PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
        let owner = initialize(&pg, target, count);
        let good = command(&pg, &owner, VersionId::Null);
        assert_eq!(payload(&good).object.ec, EcShape { k: 2, m: 1 });
        assert!(owner
            .windows
            .iter()
            .all(|window| window.geometry.ec == payload(&good).object.ec));
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        // Each shape is valid on its own. Change only the object-level field,
        // independently crossing k and m; keep every segment/witness unchanged.
        for crossed in [EcShape { k: 3, m: 1 }, EcShape { k: 2, m: 2 }] {
            let mut bad = payload(&good).clone();
            bad.object.ec = crossed;
            let bad = MetadataCommandEnvelope::new(
                good.id(),
                MetadataCommandPayload::CommitDirectPutObject(Box::new(bad)),
            );
            let result = apply(&pg, &bad);
            assert!(
                matches!(result, Err(ObjectError::CommandBinding)),
                "crossed object EC {crossed:?} was not rejected at the owner boundary: {result:?}"
            );
            assert_eq!(pg.conn.total_changes(), changes);
            assert_eq!(introduction::state(&pg), before);
        }
        apply(&pg, &good).unwrap();
        assert_object(&pg, &good);
        assert_eq!(append::load(&pg, &owner), expected(&owner, &good));
    }
}

#[test]
fn staging_object_rejects_independently_crossed_command_fields_without_mutation() {
    for target in [StagingTarget::DirectPut, StagingTarget::StreamPut] {
        let dir = test_util::tempdir();
        let pg =
            PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
        let owner = initialize(&pg, target, 1);
        let good = command(&pg, &owner, VersionId::Null);
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        for case in 0..15 {
            let mut bad = payload(&good).clone();
            match case {
                0 => bad.bucket_write_reservation.operation_kind = "crossed".into(),
                1 => bad.bucket_write_reservation.owner_token.push_str("crossed"),
                2 => bad.bucket_write_reservation.bucket_incarnation_generation += 1,
                3 => {
                    bad.object.generation_id =
                        GenerationId::new(bad.object.generation_id.get() + 1).unwrap()
                }
                4 => bad.object.size += 1,
                5 => bad.segments.clear(),
                6 => bad.segments[0].segment_index += 1,
                7 => bad.segments[0].bucket = crate::tests::bucket_name("crossed"),
                8 => bad.segments[0].key = crate::tests::object_key("crossed"),
                9 => bad.segments[0].version_id = VersionId::from_u64(3),
                10 => bad.segments[0].segment_okh[0] ^= 1,
                11 => {
                    bad.segments[0].segment_vid =
                        GenerationId::new(bad.segments[0].segment_vid.get() + 1).unwrap()
                }
                12 => bad.segments[0].data_pg_id ^= 1,
                13 => bad.segments[0].placement_cluster_epoch = ClusterEpoch::new(99).unwrap(),
                14 => bad.segments[0].ec_m += 1,
                _ => unreachable!(),
            }
            let bad = MetadataCommandEnvelope::new(
                good.id(),
                MetadataCommandPayload::CommitDirectPutObject(Box::new(bad)),
            );
            assert!(apply(&pg, &bad).is_err(), "case {case}");
            assert_eq!(pg.conn.total_changes(), changes);
            assert_eq!(introduction::state(&pg), before);
        }
        apply(&pg, &good).unwrap();
    }
}

#[test]
fn staging_object_stream_rejects_valid_but_changed_witness() {
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(StagingTarget::StreamPut).subject.object_pg.get(),
    )
    .unwrap();
    let owner = initialize(&pg, StagingTarget::StreamPut, 1);
    let good = command(&pg, &owner, VersionId::Null);
    let mut bad = payload(&good).clone();
    let segment = &mut bad.segments[0];
    let mut bytes = segment.shard_integrity.as_ref().unwrap().encode();
    *bytes.last_mut().unwrap() ^= 1;
    let witness = SegmentShardIntegrityRecord::decode(&bytes).unwrap();
    witness
        .validate(
            owner.windows[0].geometry.ec,
            owner.windows[0]
                .geometry
                .codec
                .stored_size(segment.size)
                .unwrap(),
            segment.segment_crc64,
        )
        .unwrap();
    segment.shard_integrity = Some(witness);
    let before = introduction::state(&pg);
    let changes = pg.conn.total_changes();
    assert!(matches!(
        apply(
            &pg,
            &MetadataCommandEnvelope::new(
                good.id(),
                MetadataCommandPayload::CommitDirectPutObject(Box::new(bad))
            )
        ),
        Err(ObjectError::SelectedPrefix)
    ));
    assert_eq!(pg.conn.total_changes(), changes);
    assert_eq!(introduction::state(&pg), before);
}

#[test]
fn staging_object_live_authority_changes_fail_before_any_effect() {
    for mutation in [
        "UPDATE stream_uploads SET bucket_incarnation_generation = bucket_incarnation_generation + 1",
        "UPDATE stream_uploads SET allocation_issuer_lifetime = zeroblob(16)",
        "UPDATE stream_uploads SET next_segment_vid = 99",
        "UPDATE stream_uploads SET bucket_write_owner_token = bucket_write_owner_token || '-crossed'",
        "UPDATE object_generation_reservations SET generation_id = generation_id + 1",
        "DELETE FROM object_write_counters",
        "UPDATE object_write_counters SET max_committed_generation = 1",
    ] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(dir.path(), fixture(StagingTarget::StreamPut).subject.object_pg.get()).unwrap();
        let owner = initialize(&pg, StagingTarget::StreamPut, 1);
        let command = command(&pg, &owner, VersionId::Null);
        pg.conn.execute_batch(mutation).unwrap();
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        assert!(apply(&pg, &command).is_err(), "{mutation}");
        assert_eq!(pg.conn.total_changes(), changes, "{mutation}");
        assert_eq!(introduction::state(&pg), before);
    }
}

#[test]
fn staging_object_rolls_back_live_publication_and_custody_together() {
    for target in [StagingTarget::DirectPut, StagingTarget::StreamPut] {
        for event in [
            "BEFORE INSERT ON object_segments",
            "BEFORE DELETE ON object_generation_reservations",
            "BEFORE UPDATE ON payload_staging_resources",
            "BEFORE UPDATE ON payload_staging_owners",
        ] {
            let dir = test_util::tempdir();
            let pg =
                PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
            let owner = initialize(&pg, target.clone(), 2);
            let command = command(&pg, &owner, VersionId::Null);
            pg.conn.execute_batch("CREATE TEMP TABLE outer_work(value INTEGER); INSERT INTO outer_work VALUES (42)").unwrap();
            pg.conn.execute_batch(&format!("CREATE TEMP TRIGGER fail_object {event} BEGIN SELECT RAISE(ABORT, 'object transition failure'); END")).unwrap();
            let before = introduction::state(&pg);
            let error = apply(&pg, &command).unwrap_err();
            assert!(
                error.to_string().contains("object transition failure"),
                "{error}"
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
            pg.conn.execute_batch("DROP TRIGGER fail_object").unwrap();
            apply(&pg, &command).unwrap();
            assert_object(&pg, &command);
            assert_eq!(append::load(&pg, &owner), expected(&owner, &command));
        }
    }
}
