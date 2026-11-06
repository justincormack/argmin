// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Metadata-only component tests using real introduction, append and part apply.
//! No shard I/O, physical deletion authority or production dispatch activation.

use super::super::part::{apply_in_open_transaction, PartError};
use super::*;
use crate::clock::with_time_and_monotonic_override;
use crate::metadata_command::{
    CommitStreamPartCommand, MetadataCommandEnvelope, MetadataCommandPayload,
};
use crate::node_runtime::traits::PgMetadataStore;
use crate::pg_store::PgStore;
use crate::types::{MultipartPartRecord, MultipartPartSegmentRecord, SegmentShardIntegrityRecord};
use crate::{EtagKind, MetadataError, MULTIPART_PART_SEGMENT_STAGING_VERSION_ID};

pub(super) fn append_segments(
    pg: &PgStore,
    mut owner: StagingOwnerRecord,
    count: usize,
) -> StagingOwnerRecord {
    for index in 0..count {
        let raw = append::command(&owner);
        let id = MetadataCommandId::new(
            owner.subject.originating_epoch,
            owner.subject.object_pg,
            MetadataCommandLogIndex::new(
                owner.subject.id.introduction.log_index().get() + 1 + index as u64,
            )
            .unwrap(),
        );
        let command = MetadataCommandEnvelope::new(id, raw.payload().clone());
        let next = append::next(pg, &owner);
        super::super::descriptor::apply_append_for_test(pg, &topology(), &command, &next).unwrap();
        owner = append::load(pg, &owner);
    }
    owner
}

pub(super) fn initialize(pg: &PgStore, count: usize) -> StagingOwnerRecord {
    append_segments(pg, append::initialize(pg, multipart_target()), count)
}

pub(super) fn next_attempt(
    pg: &PgStore,
    template: &StagingOwnerRecord,
    count: usize,
) -> StagingOwnerRecord {
    use super::super::capture::{StagingCapture, StagingIntroduction};
    use crate::node_runtime::payload_placement::PayloadPlacementInput;
    use crate::types::AdmittedRouteEffectFence;
    // The previous publication advanced to epoch 6. Capture the replacement
    // against that current topology, not the previous attempt's epoch 5.
    let mut capture_template = template.clone();
    capture_template.subject.originating_epoch = ClusterEpoch::new(6).unwrap();
    let config = capture::config(&capture_template);
    let mut creation = proof(
        &capture_template.subject,
        capture_template.subject.creation_operation(),
    );
    creation.reservation_id.push_str("-replacement");
    creation.owner_token.push_str("-replacement");
    let owner = with_time_and_monotonic_override(1_000, 70_000, || {
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
                    attempt: crate::tests::stream_session_id("replacement-part"),
                    introduction: MetadataCommandId::new(
                        config.cluster_epoch,
                        template.subject.object_pg,
                        MetadataCommandLogIndex::new(30).unwrap(),
                    ),
                },
                bucket: template.subject.bucket.clone(),
                key: template.subject.key.clone(),
                payload_generation: GenerationId::new(
                    template.subject.payload_generation.get() + 1,
                )
                .unwrap(),
                target: template.subject.target.clone(),
            },
            &creation,
        )
        .unwrap()
    });
    let raw = introduction::command(&owner);
    let MetadataCommandPayload::CreateStreamUpload(mut create) = raw.payload().clone() else {
        unreachable!()
    };
    create.bucket_write_reservation = creation;
    super::super::descriptor::apply_introduction_for_test(
        pg,
        &topology(),
        &MetadataCommandEnvelope::new(raw.id(), MetadataCommandPayload::CreateStreamUpload(create)),
        &owner,
    )
    .unwrap();
    append_segments(pg, owner, count)
}

pub(super) fn command(pg: &PgStore, owner: &StagingOwnerRecord) -> MetadataCommandEnvelope {
    let subject = &owner.subject;
    let StagingTarget::UploadPart {
        upload_id,
        part_number,
        ..
    } = &subject.target
    else {
        unreachable!()
    };
    let upload = pg.get_multipart_upload(upload_id).unwrap();
    let staged = pg.list_stream_segments(&subject.id.attempt).unwrap();
    let segments = staged
        .iter()
        .map(|segment| MultipartPartSegmentRecord {
            bucket: subject.bucket.clone(),
            key: subject.key.clone(),
            upload_id: upload_id.clone(),
            version_id: MULTIPART_PART_SEGMENT_STAGING_VERSION_ID.to_u64(),
            part_number: *part_number,
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
    let existing_part = match pg.get_multipart_part(upload_id, *part_number) {
        Ok(part) => Some(part),
        Err(MetadataError::PartNotFound { .. }) => None,
        Err(error) => panic!("{error}"),
    };
    let displaced_segments = pg
        .get_multipart_part_segments_for_upload_part(
            &subject.bucket,
            &subject.key,
            upload_id,
            *part_number,
        )
        .unwrap();
    let payload_crc64 = staged
        .iter()
        .fold(checksum::crc64::checksum(&[]), |crc, segment| {
            checksum::crc64::combine(crc, segment.payload_crc64, segment.size)
        });
    let ec = owner.windows[0].geometry.ec;
    let part = MultipartPartRecord {
        upload_id: upload_id.clone(),
        part_number: *part_number,
        generation: existing_part.as_ref().map_or(0, |part| part.generation + 1),
        size: staged.iter().map(|segment| segment.size).sum(),
        payload_crc64,
        etag: format!("{payload_crc64:016x}").into_bytes(),
        etag_kind: EtagKind::Crc64,
        part_vid: subject.payload_generation,
        // Production uses the first staged placement, or the finalization
        // operation's epoch for an empty part (not the session's birth epoch).
        placement_cluster_epoch: staged
            .first()
            .map_or(ClusterEpoch::new(6).unwrap(), |s| s.placement_cluster_epoch),
        ec_k: ec.k,
        ec_m: ec.m,
        last_modified: 3_000,
        checksum: None,
    };
    let mut finalization = proof(
        subject,
        UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND,
    );
    finalization
        .reservation_id
        .push_str(subject.id.attempt.as_str());
    finalization
        .owner_token
        .push_str(subject.id.attempt.as_str());
    // The initial attempt was created in epoch 5; its distinct release route
    // must survive. A replacement attempt is introduced in the current epoch 6.
    finalization.cluster_epoch = ClusterEpoch::new(6).unwrap();
    MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            finalization.cluster_epoch,
            subject.object_pg,
            MetadataCommandLogIndex::new(subject.id.introduction.log_index().get() + 10).unwrap(),
        ),
        MetadataCommandPayload::CommitStreamPart(Box::new(CommitStreamPartCommand {
            bucket: subject.bucket.clone(),
            key: subject.key.clone(),
            session_id: subject.id.attempt.clone(),
            upload,
            part,
            segments,
            existing_part,
            displaced_segments,
            bucket_write_reservation: finalization,
        })),
    )
}

pub(super) fn apply(pg: &PgStore, command: &MetadataCommandEnvelope) -> Result<(), PartError> {
    let decoded = crate::metadata_command::decode_metadata_command_envelope(
        &command.command_bytes(),
        &crate::node_runtime::MetadataCommandDecodeAuthority::new_for_test(),
    )
    .unwrap();
    apply_in_open_transaction(pg, &topology(), &decoded)
}

fn payload(envelope: &MetadataCommandEnvelope) -> &CommitStreamPartCommand {
    let MetadataCommandPayload::CommitStreamPart(command) = envelope.payload() else {
        unreachable!()
    };
    command
}

fn expected_publication(
    owner: &StagingOwnerRecord,
    command: &MetadataCommandEnvelope,
) -> StagingOwnerRecord {
    let mut expected = owner.clone();
    publish(&mut expected);
    expected.resources.finalization =
        ResourceResponsibility::ReleaseDue(StagingBucketResource::capture(
            &payload(command).bucket_write_reservation,
            PgId::new(topology().bucket_pg_for(&owner.subject.bucket)),
        ));
    expected
}

fn assert_part(pg: &PgStore, command: &MetadataCommandEnvelope) {
    let command = payload(command);
    assert_eq!(
        pg.get_multipart_part(&command.part.upload_id, command.part.part_number)
            .unwrap(),
        command.part
    );
    assert_eq!(
        pg.get_multipart_part_segments_for_upload_part(
            &command.bucket,
            &command.key,
            &command.upload.upload_id,
            command.part.part_number
        )
        .unwrap(),
        command.segments
    );
}

fn replacement(
    pg: &PgStore,
) -> (
    StagingOwnerRecord,
    StagingOwnerRecord,
    MetadataCommandEnvelope,
) {
    let first = initialize(pg, 2);
    apply(pg, &command(pg, &first)).unwrap();
    let old = append::load(pg, &first);
    let new = next_attempt(pg, &old, 1);
    let command = command(pg, &new);
    (old, new, command)
}

#[test]
fn staging_epoch_reset_part_publication_transfers_exact_prefix_and_proofs() {
    for count in [0, 2] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(multipart_target()).subject.object_pg.get(),
        )
        .unwrap();
        let owner = initialize(&pg, count);
        let original = command(&pg, &owner);
        let command = epoch_order::at_epoch_start(&pg, &original, original.id().cluster_epoch());
        apply(&pg, &command).unwrap();
        assert_part(&pg, &command);
        assert_eq!(
            append::load(&pg, &owner),
            expected_publication(&owner, &command)
        );
        let pg = epoch_order::record_and_reopen(pg, dir.path(), &command);
        assert_part(&pg, &command);
        assert_eq!(
            append::load(&pg, &owner),
            expected_publication(&owner, &command)
        );
    }
}

#[test]
fn staging_part_publication_transfers_exact_prefix_and_both_proofs_after_reopen() {
    for count in [0, 1, 2] {
        let dir = test_util::tempdir();
        let pg_id = fixture(multipart_target()).subject.object_pg.get();
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        let owner = initialize(&pg, count);
        let command = command(&pg, &owner);
        let expected = expected_publication(&owner, &command);
        pg.conn.execute_batch("COMMIT").unwrap();
        drop(pg);
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        assert_ne!(
            pg.stream_allocation_issuer(owner.subject.issuer.node_id)
                .unwrap(),
            owner.subject.issuer
        );
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        with_time_and_monotonic_override(9_000_000, 90_000_000, || apply(&pg, &command).unwrap());
        assert_part(&pg, &command);
        assert_eq!(append::load(&pg, &owner), expected);
        assert!(matches!(
            pg.get_stream_upload(&owner.subject.id.attempt),
            Err(MetadataError::StreamSessionNotFound { .. })
        ));
        assert!(matches!(
            pg.get_object_generation_reservation(
                &owner.subject.bucket,
                &owner.subject.key,
                &owner.subject.id.attempt
            ),
            Err(MetadataError::ObjectGenerationReservationNotFound { .. })
        ));
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        assert!(matches!(
            apply(&pg, &command),
            Err(PartError::CommandBinding)
        ));
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);
        pg.conn.execute_batch("COMMIT").unwrap();
        drop(pg);
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        assert_part(&pg, &command);
        assert_eq!(append::load(&pg, &owner), expected);
    }
}

#[test]
fn staging_part_replacement_preserves_displaced_windows_and_release_evidence() {
    for old_count in [0, 2] {
        for retained in [false, true] {
            let dir = test_util::tempdir();
            let pg = PgStore::open(
                dir.path(),
                fixture(multipart_target()).subject.object_pg.get(),
            )
            .unwrap();
            let first = initialize(&pg, old_count);
            apply(&pg, &command(&pg, &first)).unwrap();
            if retained {
                pg.conn
                    .execute_batch("UPDATE payload_staging_owners SET lifecycle = 3")
                    .unwrap();
            }
            let old = append::load(&pg, &first);
            let new = next_attempt(&pg, &old, 1);
            let command = command(&pg, &new);
            let mut retired = old.clone();
            retired.lifecycle =
                StagingLifecycle::Retiring(StagingRetirement::PublishedUnreferenced);
            retired.references.clear();
            let expected = expected_publication(&new, &command);
            apply(&pg, &command).unwrap();
            assert_part(&pg, &command);
            assert_eq!(append::load(&pg, &old), retired);
            assert_eq!(append::load(&pg, &new), expected);
            assert_eq!(payload(&command).part.generation, 1);
            assert_ne!(
                payload(&command).part.part_vid,
                payload(&command).existing_part.as_ref().unwrap().part_vid
            );
            assert!(!pg.conn.is_autocommit());
        }
    }
}

#[test]
fn staging_part_rejects_crossed_finalization_and_authority_without_mutation() {
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(multipart_target()).subject.object_pg.get(),
    )
    .unwrap();
    let owner = initialize(&pg, 2);
    let good = command(&pg, &owner);
    let before = introduction::state(&pg);
    let changes = pg.conn.total_changes();
    for case in 0..8 {
        let mut crossed = payload(&good).clone();
        match case {
            0 => {
                crossed.bucket_write_reservation.operation_kind =
                    UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND.into()
            }
            1 => crossed.bucket_write_reservation.bucket = crate::tests::bucket_name("crossed"),
            2 => crossed.bucket_write_reservation.target_context = Some("crossed".into()),
            3 => {
                crossed
                    .bucket_write_reservation
                    .bucket_incarnation_generation += 1
            }
            4 => crossed.bucket_write_reservation.cluster_epoch = owner.subject.originating_epoch,
            5 => {
                crossed.part.part_vid =
                    GenerationId::new(owner.subject.payload_generation.get() + 1).unwrap()
            }
            6 => crossed.part.part_number += 1,
            7 => {
                crossed.upload.object_generation_id =
                    GenerationId::new(crossed.upload.object_generation_id.get() + 1).unwrap()
            }
            _ => unreachable!(),
        }
        let crossed = MetadataCommandEnvelope::new(
            good.id(),
            MetadataCommandPayload::CommitStreamPart(Box::new(crossed)),
        );
        assert!(apply(&pg, &crossed).is_err(), "case {case}");
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);
    }
    apply(&pg, &good).unwrap();
}

#[test]
fn staging_part_requires_owner_bound_live_state_even_when_part_preflight_passes() {
    for mutation in [
        "UPDATE stream_uploads SET bucket_incarnation_generation = bucket_incarnation_generation + 1",
        "UPDATE stream_uploads SET allocation_issuer_lifetime = zeroblob(16)",
        "UPDATE stream_uploads SET next_segment_vid = 99",
        "UPDATE multipart_uploads SET object_generation_id = object_generation_id + 1",
        "DELETE FROM object_write_counters",
        "UPDATE object_write_counters SET max_committed_generation = 1",
    ] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(multipart_target()).subject.object_pg.get(),
        )
        .unwrap();
        let owner = initialize(&pg, 1);
        pg.conn.execute_batch(mutation).unwrap();
        // In particular, reload the changed upload into the command: rejecting
        // a stale upload DTO must not mask the owner's enclosing-generation check.
        let command = command(&pg, &owner);
        pg.validate_commit_stream_part_command(payload(&command))
            .unwrap();
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        let error = apply(&pg, &command).unwrap_err();
        assert!(
            matches!(
                error,
                PartError::LiveResponsibility
                    | PartError::CommandBinding
                    | PartError::Invariant(StagingInvariant::GenerationFence)
            ),
            "{mutation}: {error}"
        );
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);
    }
}

#[test]
fn staging_part_rejects_different_valid_witness_before_mutation() {
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(multipart_target()).subject.object_pg.get(),
    )
    .unwrap();
    let owner = initialize(&pg, 1);
    let good = command(&pg, &owner);
    let mut crossed = payload(&good).clone();
    let segment = &mut crossed.segments[0];
    let mut encoded = segment.shard_integrity.as_ref().unwrap().encode();
    *encoded.last_mut().unwrap() ^= 1; // parity CRC, preserving valid data/whole-segment geometry
    let witness = SegmentShardIntegrityRecord::decode(&encoded).unwrap();
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
    pg.validate_commit_stream_part_command(&crossed).unwrap(); // cannot mask the added equality check
    let before = introduction::state(&pg);
    let changes = pg.conn.total_changes();
    assert!(matches!(
        apply(
            &pg,
            &MetadataCommandEnvelope::new(
                good.id(),
                MetadataCommandPayload::CommitStreamPart(Box::new(crossed))
            )
        ),
        Err(PartError::SelectedPrefix)
    ));
    assert_eq!(pg.conn.total_changes(), changes);
    assert_eq!(introduction::state(&pg), before);
}

#[test]
fn staging_part_replacement_requires_unique_exact_displaced_owner() {
    for case in ["missing", "duplicate", "crossed-prefix", "unreferenced"] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(multipart_target()).subject.object_pg.get(),
        )
        .unwrap();
        let (old, new, _) = replacement(&pg);
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
                let mut duplicate = old.clone();
                duplicate.subject.id.attempt = crate::tests::stream_session_id("duplicate-owner");
                for window in &mut duplicate.windows {
                    window.owner = duplicate.subject.id.clone();
                    window.segment_hash = duplicate.subject.segment_hash(window.index);
                }
                for reference in &mut duplicate.references {
                    reference.segment_hash = duplicate.subject.segment_hash(reference.window_index);
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
            "crossed-prefix" => {
                pg.conn
                    .execute_batch(
                        "UPDATE multipart_part_segments SET data_pg_id = (data_pg_id + 1) % 8",
                    )
                    .unwrap();
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
            }
            _ => unreachable!(),
        }
        let command = command(&pg, &new); // co-crossed durable/command rows still pass existing preflight
        pg.validate_commit_stream_part_command(payload(&command))
            .unwrap();
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        assert!(apply(&pg, &command).is_err(), "accepted {case}");
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);
    }
}

#[test]
fn staging_part_replacement_rolls_back_part_session_and_both_owners() {
    for injection in ["part", "proof", "new-owner", "old-owner"] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(multipart_target()).subject.object_pg.get(),
        )
        .unwrap();
        let (old, new, command) = replacement(&pg);
        let event = match injection {
            "part" => "BEFORE INSERT ON multipart_parts".to_owned(),
            "proof" => "BEFORE INSERT ON payload_staging_resources".to_owned(),
            "new-owner" => format!(
                "BEFORE UPDATE ON payload_staging_owners WHEN NEW.attempt = '{}'",
                new.subject.id.attempt.as_str()
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
        pg.conn.execute_batch(&format!("CREATE TEMP TRIGGER fail_part {event} BEGIN SELECT RAISE(ABORT, 'part transition failure'); END")).unwrap();
        let before = introduction::state(&pg);
        let error = apply(&pg, &command).unwrap_err();
        assert!(
            error.to_string().contains("part transition failure"),
            "{error}"
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
        pg.conn.execute_batch("DROP TRIGGER fail_part").unwrap();
        apply(&pg, &command).unwrap();
        assert_part(&pg, &command);
    }
}
