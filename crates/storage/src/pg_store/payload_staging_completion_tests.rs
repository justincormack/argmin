// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Metadata-only fixtures using real introduction/append/part/publication
//! transactions. No ciphertext files or physical-cleanup acceptance claims.

use super::super::capture::{StagingCapture, StagingIntroduction};
use super::super::multipart::{
    apply_in_open_transaction, MultipartTerminalError as CompletionError,
};
use super::*;
use crate::metadata_command::{
    CommitMultipartObjectCommand, MetadataCommandEnvelope, MetadataCommandPayload,
    COMPLETE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
};
use crate::node_runtime::payload_placement::PayloadPlacementInput;
use crate::node_runtime::traits::PgMetadataStore;
use crate::pg_store::PgStore;
use crate::types::{
    AdmittedRouteEffectFence, ListPartsReq, MultipartCompletionFingerprint,
    ObjectEncryptionSubject, ObjectPartRecord,
};
use crate::{
    AclGrants, MetadataError, ObjectEtag, ObjectLayout, ObjectLockState, OwnerIdentity,
    PutLiveObjectReq, StoredObject, VersionId,
};

pub(super) fn new_attempt(
    pg: &PgStore,
    template: &StagingOwnerRecord,
    index: u64,
    part_number: u32,
    count: usize,
) -> StagingOwnerRecord {
    let mut source = template.clone();
    source.subject.originating_epoch = ClusterEpoch::new(6).unwrap();
    let config = capture::config(&source);
    let mut creation = proof(
        &source.subject,
        UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
    );
    creation.reservation_id.push_str(&format!("-{index}"));
    creation.owner_token.push_str(&format!("-{index}"));
    let StagingTarget::UploadPart {
        upload_id,
        upload_generation,
        ..
    } = &template.subject.target
    else {
        unreachable!()
    };
    let owner = crate::clock::with_time_and_monotonic_override(2_000, 71_000, || {
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
                    attempt: crate::tests::stream_session_id(format!("complete-part-{index}")),
                    introduction: MetadataCommandId::new(
                        config.cluster_epoch,
                        template.subject.object_pg,
                        MetadataCommandLogIndex::new(30 + index * 20).unwrap(),
                    ),
                },
                bucket: template.subject.bucket.clone(),
                key: template.subject.key.clone(),
                payload_generation: GenerationId::new(
                    template.subject.payload_generation.get() + index,
                )
                .unwrap(),
                target: StagingTarget::UploadPart {
                    upload_id: upload_id.clone(),
                    upload_generation: *upload_generation,
                    part_number,
                },
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
    part::append_segments(pg, owner, count)
}

/// Two selected parts (3,11), one omitted (7), two active attempts (3,15),
/// and an already-closed attempt (20), all with distinct payload generations.
pub(super) fn setup(pg: &PgStore, count: usize) -> Vec<StagingOwnerRecord> {
    let first = part::initialize(pg, count);
    part::apply(pg, &part::command(pg, &first)).unwrap();
    let mut owners = vec![append::load(pg, &first)];
    for (index, number) in [(1, 7), (2, 11)] {
        let owner = new_attempt(pg, &first, index, number, count);
        part::apply(pg, &part::command(pg, &owner)).unwrap();
        owners.push(append::load(pg, &owner));
    }
    owners.push(new_attempt(pg, &first, 3, 3, count));
    owners.push(new_attempt(pg, &first, 4, 15, 0));
    let closed = new_attempt(pg, &first, 5, 20, count);
    let abort = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::new(6).unwrap(),
            first.subject.object_pg,
            MetadataCommandLogIndex::new(140).unwrap(),
        ),
        MetadataCommandPayload::AbortStreamUpload(Box::new(
            crate::metadata_command::AbortStreamUploadCommand {
                bucket: closed.subject.bucket.clone(),
                key: closed.subject.key.clone(),
                session_id: closed.subject.id.attempt.clone(),
                staged_segments: pg.list_stream_segments(&closed.subject.id.attempt).unwrap(),
                stream_create_bucket_write_reservation: None,
            },
        )),
    );
    super::super::close::apply_in_open_transaction(pg, &topology(), &abort).unwrap();
    owners.push(append::load(pg, &closed));
    owners
}

pub(super) fn command(
    pg: &PgStore,
    owner: &StagingOwnerRecord,
    version: VersionId,
) -> MetadataCommandEnvelope {
    let StagingTarget::UploadPart { upload_id, .. } = &owner.subject.target else {
        unreachable!()
    };
    let upload = pg.get_multipart_upload(upload_id).unwrap();
    let all = pg
        .list_multipart_parts(&ListPartsReq {
            upload_id: upload_id.clone(),
            part_number_marker: None,
            max_parts: u32::MAX,
        })
        .unwrap()
        .parts;
    let selected: Vec<_> = all.iter().filter(|part| part.part_number != 7).collect();
    let parts: Vec<_> = selected
        .iter()
        .map(|part| ObjectPartRecord {
            bucket: upload.bucket.clone(),
            key: upload.key.clone(),
            version_id: version,
            part_number: part.part_number,
            size: part.size,
            payload_crc64: part.payload_crc64,
            etag: part.etag.clone(),
            etag_kind: part.etag_kind,
            part_vid: part.part_vid,
            placement_cluster_epoch: part.placement_cluster_epoch,
            ec_k: part.ec_k,
            ec_m: part.ec_m,
            data_pg_id: topology()
                .object_generation_multipart_part_data_pg(
                    &upload.bucket,
                    &upload.key,
                    upload.object_generation_id,
                    part.part_number,
                )
                .get(),
            checksum: part.checksum.clone(),
        })
        .collect();
    let all_segments = pg
        .get_all_multipart_part_segments_for_upload(upload_id)
        .unwrap();
    let selected_streaming_segments = all_segments
        .iter()
        .filter(|s| s.part_number != 7)
        .cloned()
        .map(|mut s| {
            s.version_id = version.to_u64();
            s
        })
        .collect();
    let sessions = pg
        .list_stream_uploads_for_multipart_upload(upload_id)
        .unwrap();
    let encryption_subject = ObjectEncryptionSubject::standard_object(
        owner.subject.bucket_incarnation,
        &upload.bucket,
        &upload.key,
        upload.object_generation_id,
        &upload.encryption,
    );
    let encryption = upload
        .encryption
        .bind_committed_object_ciphertexts(
            &encryption_subject,
            [0x31; crate::types::OBJECT_ENCRYPTION_WRAP_NONCE_LEN],
            [0xB3; crate::types::OBJECT_ENCRYPTION_WRAPPED_DEK_LEN],
            [0; crate::types::OBJECT_ENCRYPTION_CHECKSUM_NONCE_LEN],
            Vec::new(),
        )
        .unwrap();
    let parts_count = std::num::NonZeroU32::new(parts.len() as u32).unwrap();
    let mut reservation = proof(
        &owner.subject,
        COMPLETE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
    );
    // Completion can occur after both part creation (5/6) and finalization (6).
    reservation.cluster_epoch = ClusterEpoch::new(7).unwrap();
    MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            reservation.cluster_epoch,
            owner.subject.object_pg,
            MetadataCommandLogIndex::new(200).unwrap(),
        ),
        MetadataCommandPayload::CommitMultipartObject(Box::new(CommitMultipartObjectCommand {
            upload_id: upload_id.clone(),
            completion_fingerprint: MultipartCompletionFingerprint::from_bytes([0x41; 32]),
            bucket_write_reservation: reservation,
            object: PutLiveObjectReq {
                bucket: upload.bucket.clone(),
                key: upload.key.clone(),
                version_id: version,
                owner: OwnerIdentity::from_principal("owner"),
                acl_grants: AclGrants::default(),
                public_read: false,
                generation_id: upload.object_generation_id,
                size: parts.iter().map(|p| p.size).sum(),
                etag: ObjectEtag::MultipartComposite {
                    crc64: 123_u64.to_be_bytes(),
                    parts: parts_count,
                },
                ec: EcShape { k: 0, m: 0 },
                layout: ObjectLayout::MultipartManifest { parts_count },
                tags: None,
                metadata_blob: None,
                system_metadata_blob: None,
                object_lock: ObjectLockState::default(),
                encryption,
            },
            parts,
            selected_streaming_segments,
            omitted_parts: all.into_iter().filter(|p| p.part_number == 7).collect(),
            omitted_streaming_segments: all_segments
                .into_iter()
                .filter(|s| s.part_number == 7)
                .collect(),
            stream_upload_segments: pg.list_stream_segments_for_sessions(&sessions).unwrap(),
            stream_uploads: sessions.iter().map(Into::into).collect(),
            write_sequence: pg
                .next_object_write_sequence(upload.bucket.as_str(), upload.key.as_str())
                .unwrap(),
            last_modified_millis: 4_000,
            stale_payload: None,
        })),
    )
}

pub(super) fn payload(command: &MetadataCommandEnvelope) -> &CommitMultipartObjectCommand {
    let MetadataCommandPayload::CommitMultipartObject(command) = command.payload() else {
        unreachable!()
    };
    command
}

pub(super) fn apply(
    pg: &PgStore,
    command: &MetadataCommandEnvelope,
) -> Result<(), CompletionError> {
    let decoded = crate::metadata_command::decode_metadata_command_envelope(
        &command.command_bytes(),
        &crate::node_runtime::MetadataCommandDecodeAuthority::new_for_test(),
    )
    .unwrap();
    apply_in_open_transaction(pg, &topology(), &decoded)
}

fn expected(before: &StagingOwnerRecord) -> StagingOwnerRecord {
    let mut after = before.clone();
    let StagingTarget::UploadPart {
        upload_generation,
        part_number,
        ..
    } = before.subject.target
    else {
        unreachable!()
    };
    match before.lifecycle {
        StagingLifecycle::Active => {
            after.lifecycle = StagingLifecycle::Retiring(StagingRetirement::Unpublished);
            after.references.clear();
            release_due(&mut after.resources.generation);
        }
        StagingLifecycle::Published
        | StagingLifecycle::Retiring(StagingRetirement::PublishedRetained) => {
            if part_number == 7 {
                after.lifecycle =
                    StagingLifecycle::Retiring(StagingRetirement::PublishedUnreferenced);
                after.references.clear();
            } else {
                for reference in &mut after.references {
                    reference.destination = StagingDestination::Object {
                        generation: upload_generation,
                        part_number: Some(part_number),
                    };
                }
            }
        }
        _ => {}
    }
    after
}

pub(super) fn assert_complete(
    pg: &PgStore,
    command: &MetadataCommandEnvelope,
    owners: &[StagingOwnerRecord],
) {
    let command = payload(command);
    let object = &command.object;
    let StoredObject::Live(live) = pg
        .get_object_version(&object.bucket, &object.key, object.version_id)
        .unwrap()
    else {
        unreachable!()
    };
    assert_eq!(live.generation_id, object.generation_id);
    assert_eq!(live.encryption, object.encryption);
    assert_eq!(live.size, object.size);
    assert_eq!(live.etag, object.etag);
    assert_eq!(live.layout, object.layout);
    assert_eq!(
        pg.get_object_parts(&object.bucket, &object.key, object.version_id)
            .unwrap(),
        command.parts
    );
    let mut segments = Vec::new();
    for part in &command.parts {
        segments.extend(
            pg.get_multipart_part_segments(
                &object.bucket,
                &object.key,
                object.version_id,
                part.part_number,
            )
            .unwrap(),
        );
    }
    assert_eq!(segments, command.selected_streaming_segments);
    assert!(matches!(
        pg.get_multipart_upload(&command.upload_id),
        Err(MetadataError::NoSuchUpload { .. })
    ));
    assert_eq!(
        pg.get_all_multipart_part_segments_for_upload(&command.upload_id)
            .unwrap(),
        command.selected_streaming_segments
    );
    let high_water: u64 = pg.conn.query_row(
        "SELECT max_committed_generation FROM object_write_counters WHERE bucket = ?1 AND key = ?2",
        rusqlite::params![object.bucket, object.key], |row| row.get(0),
    ).unwrap();
    assert!(owners
        .iter()
        .all(|owner| high_water >= owner.subject.payload_generation.get()));
    for owner in owners {
        assert_eq!(append::load(pg, owner), expected(owner));
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
    }
}

#[test]
fn staging_epoch_reset_completion_transfers_all_selected_and_omitted_owners() {
    for count in [0, 2] {
        for version in [VersionId::Null, VersionId::from_u64(3)] {
            let dir = test_util::tempdir();
            let pg = PgStore::open(
                dir.path(),
                fixture(multipart_target()).subject.object_pg.get(),
            )
            .unwrap();
            let owners = setup(&pg, count);
            let original = command(&pg, &owners[0], version);
            let command =
                epoch_order::at_epoch_start(&pg, &original, original.id().cluster_epoch());
            apply(&pg, &command).unwrap();
            assert_complete(&pg, &command, &owners);
            let pg = epoch_order::record_and_reopen(pg, dir.path(), &command);
            assert_complete(&pg, &command, &owners);
        }
    }
}

#[test]
fn staging_completion_transfers_selected_and_retains_omitted_active_and_closed_owners_after_restart(
) {
    for count in [0, 1, 2] {
        for version in [VersionId::Null, VersionId::from_u64(3)] {
            for retiring in [false, true] {
                let dir = test_util::tempdir();
                let pg_id = fixture(multipart_target()).subject.object_pg.get();
                let pg = PgStore::open(dir.path(), pg_id).unwrap();
                let mut owners = setup(&pg, count);
                if retiring {
                    pg.conn
                        .execute_batch(
                            "UPDATE payload_staging_owners SET lifecycle = 3 WHERE lifecycle = 1",
                        )
                        .unwrap();
                    owners = owners.iter().map(|o| append::load(&pg, o)).collect();
                }
                let good = command(&pg, &owners[0], version);
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
                assert_complete(&pg, &good, &owners);
                let before = introduction::state(&pg);
                let changes = pg.conn.total_changes();
                assert!(matches!(
                    apply(&pg, &good),
                    Err(CompletionError::Metadata(
                        MetadataError::NoSuchUpload { .. }
                    ))
                ));
                assert_eq!(pg.conn.total_changes(), changes);
                assert_eq!(introduction::state(&pg), before);
                pg.conn.execute_batch("COMMIT").unwrap();
                drop(pg);
                let pg = PgStore::open(dir.path(), pg_id).unwrap();
                pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
                assert_complete(&pg, &good, &owners);
            }
        }
    }
}

#[test]
fn staging_completion_rejects_crossed_subject_proof_and_partition_before_mutation() {
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(multipart_target()).subject.object_pg.get(),
    )
    .unwrap();
    let owners = setup(&pg, 2);
    let good = command(&pg, &owners[0], VersionId::Null);
    let before = introduction::state(&pg);
    let changes = pg.conn.total_changes();
    for case in 0..19 {
        let mut bad = payload(&good).clone();
        match case {
            0 => {
                bad.bucket_write_reservation.operation_kind =
                    UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND.into()
            }
            1 => bad.bucket_write_reservation.target_context = Some("crossed".into()),
            2 => bad.bucket_write_reservation.bucket = crate::tests::bucket_name("crossed"),
            3 => bad.bucket_write_reservation.cluster_epoch = ClusterEpoch::new(5).unwrap(),
            4 => bad.bucket_write_reservation.bucket_incarnation_generation += 1,
            5 => bad.object.generation_id = owners[0].subject.payload_generation,
            6 => bad.object.size += 1,
            7 => bad.object.ec = EcShape { k: 2, m: 1 },
            8 => bad.parts.swap(0, 1),
            9 => bad.parts[0].part_vid = owners[3].subject.payload_generation,
            10 => bad.parts[0].data_pg_id ^= 1,
            11 => {
                bad.omitted_parts.clear();
            }
            12 => {
                bad.omitted_streaming_segments.pop();
            }
            13 => {
                bad.selected_streaming_segments.pop();
            }
            14 => bad.selected_streaming_segments[0].segment_okh[0] ^= 1,
            15 => {
                bad.stream_uploads.pop();
            }
            16 => {
                bad.stream_upload_segments.pop();
            }
            17 => {
                bad.omitted_streaming_segments[0].segment_vid = owners[3].subject.payload_generation
            }
            18 => bad.parts[0].etag[0] ^= 1,
            _ => unreachable!(),
        }
        let bad = MetadataCommandEnvelope::new(
            good.id(),
            MetadataCommandPayload::CommitMultipartObject(Box::new(bad)),
        );
        assert!(apply(&pg, &bad).is_err(), "case {case}");
        assert_eq!(pg.conn.total_changes(), changes, "case {case}");
        assert_eq!(introduction::state(&pg), before, "case {case}");
    }
    apply(&pg, &good).unwrap();
    assert_complete(&pg, &good, &owners);
}

#[test]
fn staging_completion_rejects_missing_or_unaccounted_durable_ownership() {
    for case in [
        "selected-owner",
        "omitted-owner",
        "active-owner",
        "selected-part",
        "omitted-part",
        "active-session",
        "generation",
        "issuer",
        "cursor",
        "incarnation",
        "high-water",
    ] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(multipart_target()).subject.object_pg.get(),
        )
        .unwrap();
        let owners = setup(&pg, 2);
        let command = command(&pg, &owners[0], VersionId::Null);
        match case {
            "selected-owner" | "omitted-owner" | "active-owner" => {
                let index = match case {
                    "selected-owner" => 0,
                    "omitted-owner" => 1,
                    _ => 3,
                };
                pg.conn
                    .execute(
                        "DELETE FROM payload_staging_owners WHERE attempt = ?1",
                        [&owners[index].subject.id.attempt],
                    )
                    .unwrap();
            }
            "selected-part" => {
                pg.conn
                    .execute_batch("DELETE FROM multipart_parts WHERE part_number = 3")
                    .unwrap();
            }
            "omitted-part" => {
                pg.conn
                    .execute_batch("DELETE FROM multipart_parts WHERE part_number = 7")
                    .unwrap();
            }
            "active-session" => {
                pg.conn
                    .execute(
                        "DELETE FROM stream_uploads WHERE session_id = ?1",
                        [&owners[4].subject.id.attempt],
                    )
                    .unwrap();
            }
            "generation" => {
                pg.conn
                    .execute(
                        "DELETE FROM object_generation_reservations WHERE reservation_id = ?1",
                        [&owners[3].subject.id.attempt],
                    )
                    .unwrap();
            }
            "issuer" => {
                pg.conn
                    .execute_batch(
                        "UPDATE stream_uploads SET allocation_issuer_lifetime = zeroblob(16)",
                    )
                    .unwrap();
            }
            "cursor" => {
                pg.conn
                    .execute_batch("UPDATE stream_uploads SET next_segment_vid = 99")
                    .unwrap();
            }
            "incarnation" => {
                pg.conn.execute_batch("UPDATE stream_uploads SET bucket_incarnation_generation = bucket_incarnation_generation + 1").unwrap();
            }
            "high-water" => {
                pg.conn
                    .execute_batch("UPDATE object_write_counters SET max_committed_generation = 1")
                    .unwrap();
            }
            _ => unreachable!(),
        }
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        assert!(apply(&pg, &command).is_err(), "{case}");
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);
    }
}

#[test]
fn staging_completion_cannot_hide_selected_or_active_owners_by_rebuilding_an_incomplete_command() {
    for count in [0, 2] {
        for missing in ["selected-part", "omitted-part", "active-session"] {
            let dir = test_util::tempdir();
            let pg = PgStore::open(
                dir.path(),
                fixture(multipart_target()).subject.object_pg.get(),
            )
            .unwrap();
            let owners = setup(&pg, count);
            let good = command(&pg, &owners[0], VersionId::Null);
            pg.conn.execute_batch("SAVEPOINT missing_manifest").unwrap();
            if missing == "active-session" {
                pg.conn
                    .execute(
                        "DELETE FROM stream_uploads WHERE session_id = ?1",
                        [&owners[4].subject.id.attempt],
                    )
                    .unwrap();
            } else {
                let number = if missing == "selected-part" { 3 } else { 7 };
                pg.conn
                    .execute(
                        "DELETE FROM multipart_part_segments WHERE part_number = ?1",
                        [number],
                    )
                    .unwrap();
                pg.conn
                    .execute(
                        "DELETE FROM multipart_parts WHERE part_number = ?1",
                        [number],
                    )
                    .unwrap();
            }
            // Every command field agrees with the now-incomplete live tables.
            // Only discovery of the still-selected/active owner can reject it.
            let incomplete = command(&pg, &owners[0], VersionId::Null);
            let before = introduction::state(&pg);
            let changes = pg.conn.total_changes();
            assert!(
                matches!(apply(&pg, &incomplete), Err(CompletionError::Owner)),
                "{count}/{missing}"
            );
            assert_eq!(pg.conn.total_changes(), changes);
            assert_eq!(introduction::state(&pg), before);
            pg.conn
                .execute_batch("ROLLBACK TO missing_manifest; RELEASE missing_manifest")
                .unwrap();
            apply(&pg, &good).unwrap();
            assert_complete(&pg, &good, &owners);
        }
    }
}

#[test]
fn staging_completion_co_crossed_durable_metadata_and_command_cannot_replace_owner_provenance() {
    for mutation in [
        "UPDATE multipart_parts SET ec_k = ec_k + 1 WHERE part_number = 3",
        "UPDATE multipart_parts SET ec_m = ec_m + 1 WHERE part_number = 7",
        "UPDATE multipart_parts SET size = size + 1 WHERE part_number = 11",
        "UPDATE multipart_part_segments SET segment_okh = zeroblob(16) WHERE part_number = 3 AND segment_index = 0",
        "UPDATE multipart_part_segments SET segment_vid = 99 WHERE part_number = 7 AND segment_index = 0",
        "UPDATE multipart_part_segments SET placement_cluster_epoch = placement_cluster_epoch + 1 WHERE part_number = 11 AND segment_index = 0",
        "UPDATE multipart_part_segments SET data_pg_id = (data_pg_id + 1) % 8 WHERE part_number = 3 AND segment_index = 0",
    ] {
        let dir = test_util::tempdir(); let pg = PgStore::open(dir.path(), fixture(multipart_target()).subject.object_pg.get()).unwrap();
        let owners = setup(&pg, 2); let good = command(&pg, &owners[0], VersionId::Null);
        pg.conn.execute_batch("SAVEPOINT crossed_manifest").unwrap(); pg.conn.execute_batch(mutation).unwrap();
        let crossed = command(&pg, &owners[0], VersionId::Null);
        let before = introduction::state(&pg); let changes = pg.conn.total_changes();
        assert!(matches!(apply(&pg, &crossed), Err(CompletionError::Owner)), "{mutation}");
        assert_eq!(pg.conn.total_changes(), changes); assert_eq!(introduction::state(&pg), before);
        pg.conn.execute_batch("ROLLBACK TO crossed_manifest; RELEASE crossed_manifest").unwrap();
        apply(&pg, &good).unwrap(); assert_complete(&pg, &good, &owners);
    }
}

#[test]
fn staging_completion_rejects_crossed_nonempty_part_placement_before_mutation() {
    for count in [1, 2] {
        assert_crossed_part_placement_rejected(count);
    }
}

#[test]
fn staging_completion_rejects_crossed_empty_part_placement_before_mutation() {
    assert_crossed_part_placement_rejected(0);
}

fn assert_crossed_part_placement_rejected(count: usize) {
    // Cover both selected parts and the omitted part. Cross only the aggregate
    // epoch in durable state, then rebuild a checksummed command from that state.
    for part_number in [3, 7, 11] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(multipart_target()).subject.object_pg.get(),
        )
        .unwrap();
        let owners = setup(&pg, count);
        let good = command(&pg, &owners[0], VersionId::Null);
        let expected_epoch = if part_number == 3 && count != 0 { 5 } else { 6 };
        let durable = pg
            .get_multipart_part(&payload(&good).upload_id, part_number)
            .unwrap();
        assert_eq!(durable.placement_cluster_epoch.get(), expected_epoch);
        assert_eq!(good.id().cluster_epoch().get(), 7);
        assert_eq!(owners[0].subject.originating_epoch.get(), 5);

        // For the first part this also crosses empty/nonempty semantics: an
        // empty part must not use its unused window's epoch 5, and a nonempty
        // part must not substitute its finalization epoch 6. Neither may use
        // the later MPU completion epoch 7.
        for wrong_epoch in [if expected_epoch == 5 { 6 } else { 5 }, 7] {
            pg.conn
                .execute_batch("SAVEPOINT crossed_part_epoch")
                .unwrap();
            assert_eq!(pg.conn.execute(
                "UPDATE multipart_parts SET placement_cluster_epoch = ?1 WHERE upload_id = ?2 AND part_number = ?3",
                rusqlite::params![wrong_epoch, payload(&good).upload_id, part_number],
            ).unwrap(), 1);
            let crossed = command(&pg, &owners[0], VersionId::Null);
            let crossed_payload = payload(&crossed);
            let copied_epoch = if part_number == 7 {
                crossed_payload
                    .omitted_parts
                    .iter()
                    .find(|p| p.part_number == part_number)
                    .unwrap()
                    .placement_cluster_epoch
            } else {
                crossed_payload
                    .parts
                    .iter()
                    .find(|p| p.part_number == part_number)
                    .unwrap()
                    .placement_cluster_epoch
            };
            assert_eq!(copied_epoch.get(), wrong_epoch);
            let before = introduction::state(&pg);
            let changes = pg.conn.total_changes();
            let result = apply(&pg, &crossed);
            assert!(
                matches!(result, Err(CompletionError::Owner)),
                "{count}/{part_number}/{wrong_epoch}: {result:?}"
            );
            assert_eq!(pg.conn.total_changes(), changes);
            assert_eq!(introduction::state(&pg), before);
            pg.conn
                .execute_batch("ROLLBACK TO crossed_part_epoch; RELEASE crossed_part_epoch")
                .unwrap();
        }
        // The original aggregate placement remains valid after the rejection;
        // successful publication must preserve it in the completed manifest.
        apply(&pg, &good).unwrap();
        assert_complete(&pg, &good, &owners);
    }
}

#[test]
fn staging_completion_rolls_back_live_metadata_and_all_owner_transfers() {
    for event in [
        "BEFORE INSERT ON object_parts",
        "BEFORE INSERT ON multipart_part_segments WHEN NEW.version_id = 0",
        "BEFORE DELETE ON multipart_uploads",
        "BEFORE DELETE ON stream_uploads",
        "BEFORE UPDATE ON payload_staging_references WHEN OLD.part_number = 11",
        "BEFORE DELETE ON payload_staging_references WHEN OLD.part_number = 7",
        "BEFORE UPDATE ON payload_staging_owners WHEN NEW.lifecycle = 4",
        "BEFORE UPDATE ON payload_staging_owners WHEN NEW.lifecycle = 2",
    ] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(multipart_target()).subject.object_pg.get(),
        )
        .unwrap();
        let owners = setup(&pg, 2);
        let good = command(&pg, &owners[0], VersionId::Null);
        pg.conn
            .execute_batch(
                "CREATE TEMP TABLE outer_work(value INTEGER); INSERT INTO outer_work VALUES (42)",
            )
            .unwrap();
        pg.conn.execute_batch(&format!("CREATE TEMP TRIGGER fail_completion {event} BEGIN SELECT RAISE(ABORT, 'completion handoff failure'); END")).unwrap();
        let before = introduction::state(&pg);
        let error = apply(&pg, &good).unwrap_err();
        assert!(
            error.to_string().contains("completion handoff failure"),
            "{event}: {error}"
        );
        assert_eq!(introduction::state(&pg), before);
        assert!(!pg.conn.is_autocommit());
        assert_eq!(
            pg.conn
                .query_row("SELECT value FROM outer_work", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            42
        );
        pg.conn
            .execute_batch("DROP TRIGGER fail_completion")
            .unwrap();
        apply(&pg, &good).unwrap();
        assert_complete(&pg, &good, &owners);
    }
}
