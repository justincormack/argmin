// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Descriptor grammar and application-component evidence. The live enclosing
//! command still needs mandatory fields and its coordinated format bump; these
//! tests do not claim installed-RPC or frontend custody activation.

use super::super::descriptor::{AppendDescriptor, DescriptorError, IntroductionDescriptor};
use super::*;
use crate::metadata_command::{MetadataCommandEnvelope, MetadataCommandPayload};
use crate::pg_store::PgStore;

fn introductions() -> Vec<(StagingOwnerRecord, IntroductionDescriptor)> {
    [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ]
    .into_iter()
    .map(|target| {
        let owner = fixture(target);
        let descriptor =
            IntroductionDescriptor::capture(&owner.validate(&topology()).unwrap()).unwrap();
        (owner, descriptor)
    })
    .collect()
}

fn append_descriptor(owner: &StagingOwnerRecord) -> AppendDescriptor {
    AppendDescriptor::capture(
        &owner.validate(&topology()).unwrap(),
        window(&owner.subject, 1, owner.reserved_end),
    )
    .unwrap()
}

#[test]
fn staging_descriptor_round_trips_all_targets_without_resampling() {
    for (owner, original) in introductions() {
        let bytes = original.encode().unwrap();
        crate::clock::with_time_and_monotonic_override(99_000_000, 1, || {
            let decoded = IntroductionDescriptor::decode(&bytes).unwrap();
            assert_eq!(decoded, original);
            assert_eq!(decoded.encode().unwrap(), bytes);
            // This is a descriptive cutoff, not a new lifetime on receipt.
            assert!(owner.maximum_cutoff_ms < crate::clock::current_time_millis());
        });
        if !matches!(owner.subject.target, StagingTarget::DirectPut) {
            let original = append_descriptor(&owner);
            let bytes = original.encode().unwrap();
            let decoded = AppendDescriptor::decode(&bytes).unwrap();
            assert_eq!(decoded, original);
            assert_eq!(decoded.encode().unwrap(), bytes);
        }
    }
}

#[test]
fn staging_descriptor_rejects_truncation_bounds_versions_and_crossed_kinds() {
    for (owner, introduction) in introductions() {
        let intro = introduction.encode().unwrap();
        for end in 0..intro.len() {
            assert!(
                IntroductionDescriptor::decode(&intro[..end]).is_err(),
                "prefix {end}"
            );
        }
        assert!(AppendDescriptor::decode(&intro).is_err());
        for version in [0_u16, 2, u16::MAX] {
            let mut invalid = intro.clone();
            invalid[8..10].copy_from_slice(&version.to_le_bytes());
            assert!(IntroductionDescriptor::decode(&invalid).is_err());
        }
        let mut invalid = intro.clone();
        invalid[0] ^= 1;
        assert!(IntroductionDescriptor::decode(&invalid).is_err());
        invalid = intro.clone();
        invalid[10] = 2;
        assert!(IntroductionDescriptor::decode(&invalid).is_err());
        invalid = intro.clone();
        invalid.push(0);
        assert!(IntroductionDescriptor::decode(&invalid).is_err());
        invalid = intro.clone();
        invalid[11..15].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(IntroductionDescriptor::decode(&invalid).is_err());
        invalid = intro;
        invalid[15] = 0xff;
        assert!(IntroductionDescriptor::decode(&invalid).is_err());

        if !matches!(owner.subject.target, StagingTarget::DirectPut) {
            let append = append_descriptor(&owner).encode().unwrap();
            for end in 0..append.len() {
                assert!(
                    AppendDescriptor::decode(&append[..end]).is_err(),
                    "append prefix {end}"
                );
            }
            assert!(IntroductionDescriptor::decode(&append).is_err());
            let mut trailing = append;
            trailing.push(0);
            assert!(AppendDescriptor::decode(&trailing).is_err());
        }
    }
    let oversized = vec![0; 16 * 1024 + 1];
    assert!(matches!(
        IntroductionDescriptor::decode(&oversized),
        Err(DescriptorError::Encoding("descriptor exceeds bound"))
    ));
    assert!(matches!(
        AppendDescriptor::decode(&oversized),
        Err(DescriptorError::Encoding("descriptor exceeds bound"))
    ));
}

// Independent grammar offsets, not production serializer/decoder metadata.
fn window_offset(owner: &StagingOwnerRecord) -> usize {
    11 + 4
        + owner.subject.id.attempt.as_str().len()
        + 8
        + 4
        + 8
        + 4
        + owner.subject.bucket.as_str().len()
        + 8
        + 4
        + owner.subject.key.as_str().len()
        + 8
        + 1
        + match &owner.subject.target {
            StagingTarget::DirectPut | StagingTarget::StreamPut => 0,
            StagingTarget::UploadPart { upload_id, .. } => 4 + upload_id.as_str().len() + 8 + 4,
        }
        + 4
        + 16
}

fn replace_string(bytes: &[u8], offset: usize, value: &str) -> Vec<u8> {
    let length = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
    let mut replaced = bytes.to_vec();
    replaced.splice(
        offset..offset + 4 + length,
        (value.len() as u32)
            .to_le_bytes()
            .into_iter()
            .chain(value.bytes()),
    );
    replaced
}

#[test]
fn staging_descriptor_reservation_identity_bounds_are_independent_of_frame_cap() {
    for (owner, descriptor) in introductions() {
        let bytes = descriptor.encode().unwrap();
        let (ResourceResponsibility::Live(proof) | ResourceResponsibility::ReleaseDue(proof)) =
            &owner.resources.creation
        else {
            unreachable!()
        };
        let resource =
            window_offset(&owner) + 92 + owner.windows[0].participants.files.len() * 4 + 4;
        let reservation = resource + 4 + proof.identity.bucket.as_str().len();
        let token = reservation + 4 + proof.identity.reservation_id.len();
        for (field, offset) in [("reservation_id", reservation), ("owner_token", token)] {
            for valid in ["x".repeat(256), "é".repeat(128)] {
                assert_eq!(valid.len(), 256);
                let at_limit = replace_string(&bytes, offset, &valid);
                let decoded = IntroductionDescriptor::decode(&at_limit).unwrap();
                assert_eq!(decoded.encode().unwrap(), at_limit, "{field}");
            }
            for invalid in ["x".repeat(257), "é".repeat(129), "\0identity".into()] {
                let encoded = replace_string(&bytes, offset, &invalid);
                assert!(encoded.len() < 16 * 1024);
                assert!(
                    matches!(
                        IntroductionDescriptor::decode(&encoded),
                        Err(DescriptorError::Invariant(StagingInvariant::Resource))
                    ),
                    "{field} with {} bytes must fail resource validation for {:?}",
                    invalid.len(),
                    owner.subject.target
                );
            }
        }
    }
}

#[test]
fn staging_descriptor_at_limit_proof_round_trips_authoritative_reservation_and_release_codec() {
    use crate::node_runtime::traits::{DurableBucketWriteReservationAcquire, PgMetadataStore};
    use crate::storage_rpc::{
        decode_bucket_write_reservation_proof_request,
        encode_bucket_write_reservation_proof_request,
        StorageRpcBucketWriteReservationProofRequest,
    };

    let _clock = crate::clock::test_time_override_guard(1_000);
    let mut owner = fixture(StagingTarget::DirectPut);
    let release_pg = PgId::new(topology().bucket_pg_for(&owner.subject.bucket));
    let dir = test_util::tempdir();
    let bucket_pg = PgStore::open(dir.path(), release_pg.get()).unwrap();
    bucket_pg
        .create_bucket(
            &owner.subject.bucket,
            "owner",
            &crate::CanonicalUserId::from_principal("owner"),
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
    let reservation_id = "r".repeat(256);
    let owner_token = "o".repeat(256);
    let record = bucket_pg
        .acquire_durable_bucket_write_reservation(DurableBucketWriteReservationAcquire {
            name: &owner.subject.bucket,
            reservation_id: &reservation_id,
            owner_token: &owner_token,
            cluster_epoch: owner.subject.originating_epoch,
            operation_kind: owner.subject.creation_operation(),
            created_at: 1_000,
            lease_deadline: 10_000,
            target_context: Some(owner.subject.key.as_str()),
        })
        .unwrap();
    let proof = BucketWriteReservationProof::from(&record);
    assert_eq!(
        bucket_pg
            .durable_bucket_write_reservations(&owner.subject.bucket)
            .unwrap(),
        vec![record]
    );
    owner.subject.bucket_incarnation = proof.bucket_incarnation_generation;
    owner.resources.creation =
        ResourceResponsibility::Live(StagingBucketResource::capture(&proof, release_pg));
    let descriptor =
        IntroductionDescriptor::capture(&owner.validate(&topology()).unwrap()).unwrap();
    assert_eq!(
        IntroductionDescriptor::decode(&descriptor.encode().unwrap()).unwrap(),
        descriptor
    );

    // This is the production release request codec, not the descriptor codec.
    let request = StorageRpcBucketWriteReservationProofRequest {
        node_id: NodeId::new(4),
        route_cluster_epoch: proof.cluster_epoch,
        pg_id: release_pg,
        proof,
    };
    let decoded = decode_bucket_write_reservation_proof_request(
        &encode_bucket_write_reservation_proof_request(&request).unwrap(),
    )
    .unwrap();
    assert_eq!(decoded, request);
    bucket_pg
        .release_metadata_command_bucket_write_reservation(&decoded.proof)
        .unwrap();
    assert!(bucket_pg
        .durable_bucket_write_reservations(&owner.subject.bucket)
        .unwrap()
        .is_empty());
    for (reservation_id, owner_token) in [
        ("r".repeat(257), "o".repeat(256)),
        ("r".repeat(256), "o".repeat(257)),
        ("\0id".into(), "owner".into()),
        ("id".into(), "\0owner".into()),
    ] {
        let changes = bucket_pg.conn.total_changes();
        assert!(bucket_pg
            .acquire_durable_bucket_write_reservation(DurableBucketWriteReservationAcquire {
                name: &owner.subject.bucket,
                reservation_id: &reservation_id,
                owner_token: &owner_token,
                cluster_epoch: owner.subject.originating_epoch,
                operation_kind: owner.subject.creation_operation(),
                created_at: 1_000,
                lease_deadline: 10_000,
                target_context: Some(owner.subject.key.as_str()),
            })
            .is_err());
        assert_eq!(bucket_pg.conn.total_changes(), changes);
    }
}

#[test]
fn staging_descriptor_rejects_malformed_window_fields_independently() {
    for (owner, descriptor) in introductions() {
        let original = descriptor.encode().unwrap();
        let w = window_offset(&owner);
        // Index, hash, start, end, issued time, cutoff, codec, k, m,
        // logical/stored/shard sizes, placement epoch and participant count.
        for (offset, replacement) in [
            (0, 1_u32.to_le_bytes().to_vec()),
            (4, vec![0xff; 16]),
            (20, 0_u64.to_le_bytes().to_vec()),
            (28, owner.windows[0].start.get().to_le_bytes().to_vec()),
            (36, owner.windows[0].cutoff_ms.to_le_bytes().to_vec()),
            (44, (owner.windows[0].cutoff_ms + 1).to_le_bytes().to_vec()),
            (52, vec![0]),
            (53, vec![0]),
            (54, vec![255]),
            (55, u64::MAX.to_le_bytes().to_vec()),
            (63, 1_u64.to_le_bytes().to_vec()),
            (71, 1_u64.to_le_bytes().to_vec()),
            (83, 0_u64.to_le_bytes().to_vec()),
            (91, vec![255]),
            // Copy the first node into the second slot, retaining the count.
            (
                96,
                owner.windows[0].participants.files[0]
                    .as_u32()
                    .to_le_bytes()
                    .to_vec(),
            ),
        ] {
            let mut invalid = original.clone();
            invalid[w + offset..w + offset + replacement.len()].copy_from_slice(&replacement);
            assert!(
                IntroductionDescriptor::decode(&invalid).is_err(),
                "offset {offset}, target {:?}",
                owner.subject.target
            );
        }
    }
}

#[test]
fn staging_descriptor_capture_rejects_terminal_owners_and_nonadjacent_windows() {
    for (owner, _) in introductions() {
        let closed = owner.closed_unpublished().unwrap();
        assert!(IntroductionDescriptor::capture(&closed.validate(&topology()).unwrap()).is_err());
        let next = window(&owner.subject, 1, owner.reserved_end);
        if matches!(owner.subject.target, StagingTarget::DirectPut) {
            assert!(
                AppendDescriptor::capture(&owner.validate(&topology()).unwrap(), next).is_err()
            );
            continue;
        }
        assert!(
            AppendDescriptor::capture(&closed.validate(&topology()).unwrap(), next.clone())
                .is_err()
        );
        let topology = topology();
        let valid = owner.validate(&topology).unwrap();
        let mut bad = next.clone();
        bad.start = GenerationId::new(bad.start.get() + 1).unwrap();
        bad.end = GenerationId::new(bad.end.get() + 1).unwrap();
        assert!(AppendDescriptor::capture(&valid, bad).is_err());
        let mut bad = next.clone();
        bad.index += 1;
        bad.segment_hash = owner.subject.segment_hash(bad.index);
        assert!(AppendDescriptor::capture(&valid, bad).is_err());
        let mut bad = next;
        bad.issuer.lifetime[0] ^= 1;
        assert!(AppendDescriptor::capture(&valid, bad).is_err());
    }
}

#[test]
fn staging_descriptor_frame_bound_does_not_authorize_invalid_creation_proofs() {
    for (owner, descriptor) in introductions() {
        let bytes = descriptor.encode().unwrap();
        let (ResourceResponsibility::Live(proof) | ResourceResponsibility::ReleaseDue(proof)) =
            &owner.resources.creation
        else {
            unreachable!()
        };
        let resource =
            window_offset(&owner) + 92 + owner.windows[0].participants.files.len() * 4 + 4;
        let reservation = resource + 4 + proof.identity.bucket.as_str().len();
        let token = reservation + 4 + proof.identity.reservation_id.len();
        let epoch = token + 4 + proof.identity.owner_token.len();
        let operation = epoch + 24;
        let created_at = operation + 4 + proof.identity.operation_kind.len();
        let target = created_at + 8;
        for offset in [resource, reservation, token, operation, target + 1] {
            let len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
            let mut invalid = bytes.clone();
            // Preserve framing; reject empty identity/operation/target fields,
            // rather than relying on shifted or truncated subsequent fields.
            invalid.splice(offset..offset + 4 + len, 0_u32.to_le_bytes());
            assert!(
                IntroductionDescriptor::decode(&invalid).is_err(),
                "field at {offset}"
            );
        }
        for (offset, value) in [
            (epoch, owner.subject.originating_epoch.get() + 1),
            (epoch + 8, owner.subject.bucket_incarnation - 1),
            (epoch + 16, owner.subject.bucket_incarnation + 1),
            (created_at, proof.observed_lease_deadline),
        ] {
            let mut invalid = bytes.clone();
            invalid[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
            assert!(matches!(
                IntroductionDescriptor::decode(&invalid),
                Err(DescriptorError::Invariant(StagingInvariant::Resource))
            ));
        }
        let mut invalid = bytes.clone();
        invalid[target] = 2;
        assert!(matches!(
            IntroductionDescriptor::decode(&invalid),
            Err(DescriptorError::Encoding("optional target tag"))
        ));
        // A separately valid, nonempty wrong operation/target must also fail.
        for offset in [operation + 4, target + 5] {
            let mut invalid = bytes.clone();
            invalid[offset] = b'x';
            assert!(matches!(
                IntroductionDescriptor::decode(&invalid),
                Err(DescriptorError::Invariant(StagingInvariant::Resource))
            ));
        }
        // A complete, well-framed descriptor at the overall cap is still an
        // invalid proof. Do not construct it through the checked capture API.
        let oversized_token =
            "x".repeat(proof.identity.owner_token.len() + 16 * 1024 - bytes.len());
        let at_cap = replace_string(&bytes, token, &oversized_token);
        assert_eq!(at_cap.len(), 16 * 1024);
        assert!(matches!(
            IntroductionDescriptor::decode(&at_cap),
            Err(DescriptorError::Invariant(StagingInvariant::Resource))
        ));
        let over_cap = replace_string(&bytes, token, &(oversized_token + "x"));
        assert_eq!(over_cap.len(), 16 * 1024 + 1);
        assert!(matches!(
            IntroductionDescriptor::decode(&over_cap),
            Err(DescriptorError::Encoding("descriptor exceeds bound"))
        ));
    }
}

#[test]
fn staging_descriptor_decoded_introduction_checks_actual_topology_before_effects() {
    for (template, _) in introductions() {
        let dir = test_util::tempdir();
        let pg = PgStore::open(dir.path(), template.subject.object_pg.get()).unwrap();
        let (owner, command) = introduction::setup(&pg, template.subject.target);
        let bytes = IntroductionDescriptor::capture(&owner.validate(&topology()).unwrap())
            .unwrap()
            .encode()
            .unwrap();
        let w = window_offset(&owner);
        let release = bytes.len() - 4;
        for (offset, wrong_pg) in [
            (
                w + 79,
                (owner.windows[0].participants.data_pg.get() + 1) % 8,
            ),
            (
                release,
                (topology().bucket_pg_for(&owner.subject.bucket) + 1) % 8,
            ),
        ] {
            let mut crossed = bytes.clone();
            crossed[offset..offset + 4].copy_from_slice(&wrong_pg.to_le_bytes());
            // Wire shape is valid. Only the configured store topology rejects it.
            let crossed = IntroductionDescriptor::decode(&crossed).unwrap();
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            let before = pg.conn.total_changes();
            assert!(crossed
                .bind(&topology(), &command)
                .and_then(|bound| bound.apply_in_open_transaction(&pg))
                .is_err());
            assert_eq!(pg.conn.total_changes(), before);
            pg.conn.execute_batch("COMMIT").unwrap();
        }
    }
}

#[test]
fn staging_descriptor_v1_frozen_bytes() {
    // Immutable complete V1 specimens, not regenerated from current constants.
    let expected = [
        (
            include_str!("testdata/staging_descriptor_v1_intro_direct_put.hex"),
            None,
            417,
        ),
        (
            include_str!("testdata/staging_descriptor_v1_intro_stream_put.hex"),
            Some(include_str!(
                "testdata/staging_descriptor_v1_append_stream_put.hex"
            )),
            417,
        ),
        (
            include_str!("testdata/staging_descriptor_v1_intro_upload_part.hex"),
            Some(include_str!(
                "testdata/staging_descriptor_v1_append_upload_part.hex"
            )),
            564,
        ),
    ];
    fn decode_hex(text: &str) -> Vec<u8> {
        let hex = text.split_whitespace().collect::<String>();
        assert_eq!(hex.len() % 2, 0);
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect()
    }
    for (index, (owner, descriptor)) in introductions().into_iter().enumerate() {
        let bytes = descriptor.encode().unwrap();
        let (intro, append, len) = expected[index];
        let literal = decode_hex(intro);
        assert_eq!(literal.len(), len);
        assert_eq!(bytes, literal);
        assert_eq!(
            IntroductionDescriptor::decode(&literal).unwrap(),
            descriptor
        );
        if !matches!(owner.subject.target, StagingTarget::DirectPut) {
            let descriptor = append_descriptor(&owner);
            let literal = decode_hex(append.unwrap());
            assert_eq!(literal.len(), if index == 1 { 241 } else { 385 });
            assert_eq!(descriptor.encode().unwrap(), literal);
            assert_eq!(AppendDescriptor::decode(&literal).unwrap(), descriptor);
        }
    }
}

#[test]
fn staging_descriptor_append_rejects_valid_windows_at_invalid_positions() {
    for target in [StagingTarget::StreamPut, multipart_target()] {
        let owner = fixture(target);
        let bytes = append_descriptor(&owner).encode().unwrap();
        let w = window_offset(&owner);
        for index in [0, MAX_STREAM_SEGMENTS + 1] {
            let mut invalid = bytes.clone();
            // Keep the window internally consistent, so its ordinary hash/VID
            // validation cannot mask the append-specific position check.
            let start = owner.subject.first_vid().get() + u64::from(index);
            invalid[w..w + 4].copy_from_slice(&index.to_le_bytes());
            invalid[w + 4..w + 20].copy_from_slice(&owner.subject.segment_hash(index));
            invalid[w + 20..w + 28].copy_from_slice(&start.to_le_bytes());
            invalid[w + 28..w + 36].copy_from_slice(&(start + 1).to_le_bytes());
            assert!(matches!(
                AppendDescriptor::decode(&invalid),
                Err(DescriptorError::Encoding("invalid append position"))
            ));
        }
        let mut invalid = bytes.clone();
        invalid[w + 20..w + 28].copy_from_slice(&17_u64.to_le_bytes());
        invalid[w + 28..w + 36].copy_from_slice(&18_u64.to_le_bytes());
        assert!(matches!(
            AppendDescriptor::decode(&invalid),
            Err(DescriptorError::Encoding("invalid append position"))
        ));
    }
}

#[test]
fn staging_descriptor_decoded_introduction_applies_on_replica_and_replays_exactly() {
    for (template, _) in introductions() {
        let primary_dir = test_util::tempdir();
        let replica_dir = test_util::tempdir();
        let primary = PgStore::open(primary_dir.path(), template.subject.object_pg.get()).unwrap();
        let replica = PgStore::open(replica_dir.path(), template.subject.object_pg.get()).unwrap();
        let (owner, command) = introduction::setup(&primary, template.subject.target.clone());
        let mut replica_template = template;
        introduction::seed_upload(&replica, &mut replica_template);
        let encoded = IntroductionDescriptor::capture(&owner.validate(&topology()).unwrap())
            .unwrap()
            .encode()
            .unwrap();
        let descriptor = IntroductionDescriptor::decode(&encoded).unwrap();
        assert_ne!(
            primary.stream_allocation_issuer(NodeId::new(4)).unwrap(),
            replica.stream_allocation_issuer(NodeId::new(4)).unwrap()
        );
        for pg in [&replica, &primary] {
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            assert_eq!(
                descriptor
                    .bind(&topology(), &command)
                    .and_then(|bound| bound.apply_in_open_transaction(pg))
                    .unwrap(),
                super::super::introduction::IntroductionApplied::Created
            );
            let stored = sql::load(&pg.conn, owner.subject.object_pg, &owner.subject.id.attempt)
                .unwrap()
                .unwrap();
            assert_eq!(stored, owner);
            let changes = pg.conn.total_changes();
            assert_eq!(
                descriptor
                    .bind(&topology(), &command)
                    .and_then(|bound| bound.apply_in_open_transaction(pg))
                    .unwrap(),
                super::super::introduction::IntroductionApplied::ExactExisting
            );
            assert_eq!(pg.conn.total_changes(), changes);
            pg.conn.execute_batch("COMMIT").unwrap();
        }
    }
}

#[test]
fn staging_descriptor_decoded_append_binds_immutable_subject_before_mutation() {
    for target in [StagingTarget::StreamPut, multipart_target()] {
        let dir = test_util::tempdir();
        let pg =
            PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
        let owner = append::initialize(&pg, target);
        let next = append::next(&pg, &owner);
        let command = append::command(&owner);
        let bytes = AppendDescriptor::capture(&owner.validate(&topology()).unwrap(), next.clone())
            .unwrap()
            .encode()
            .unwrap();
        let descriptor = AppendDescriptor::decode(&bytes).unwrap();
        // Incarnation is valid structurally and the bucket/PG/session/issuer
        // remain identical. Only the actual retained subject can reject it.
        let mut crossed = bytes.clone();
        let incarnation = 11
            + 4
            + owner.subject.id.attempt.as_str().len()
            + 8
            + 4
            + 8
            + 4
            + owner.subject.bucket.as_str().len();
        crossed[incarnation..incarnation + 8]
            .copy_from_slice(&(owner.subject.bucket_incarnation + 1).to_le_bytes());
        let crossed = AppendDescriptor::decode(&crossed).unwrap();
        let before = pg.conn.total_changes();
        assert!(matches!(
            crossed
                .bind(&topology(), &command)
                .and_then(|bound| super::super::append::apply_in_open_transaction(&pg, bound)),
            Err(super::super::append::AppendError::CommandBinding)
        ));
        assert_eq!(pg.conn.total_changes(), before);
        let mut payload = command.payload().clone();
        let MetadataCommandPayload::AppendStreamSegment(append) = &mut payload else {
            unreachable!()
        };
        append.bucket = crate::tests::bucket_name("other-bucket");
        let crossed_command = MetadataCommandEnvelope::new(command.id(), payload);
        assert!(descriptor
            .bind(&topology(), &crossed_command)
            .and_then(|bound| bound.apply_in_open_transaction(&pg))
            .is_err());
        assert_eq!(pg.conn.total_changes(), before);
        assert_eq!(
            descriptor
                .bind(&topology(), &command)
                .and_then(|bound| bound.apply_in_open_transaction(&pg))
                .unwrap(),
            super::super::append::AppendApplied::Appended
        );
        assert_eq!(append::load(&pg, &owner).windows.last(), Some(&next));
        let after = pg.conn.total_changes();
        assert_eq!(
            descriptor
                .bind(&topology(), &command)
                .and_then(|bound| bound.apply_in_open_transaction(&pg))
                .unwrap(),
            super::super::append::AppendApplied::ExactExisting
        );
        assert_eq!(pg.conn.total_changes(), after);
    }
}
