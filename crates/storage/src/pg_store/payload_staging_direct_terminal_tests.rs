// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::metadata_command::MetadataCommandPayload;
use crate::node_runtime::traits::PgMetadataStore;
use crate::{BucketVersioningState, StreamPutCommitInput, VersionId};

fn input(owner: &StagingOwnerRecord) -> StreamPutCommitInput {
    let encrypted = crate::tests::sse_s3_segment_test_fixture(b"data", 0);
    let subject = crate::ObjectEncryptionSubject::standard_object(
        owner.subject.bucket_incarnation,
        &owner.subject.bucket,
        &owner.subject.key,
        owner.subject.payload_generation,
        &encrypted.encryption,
    );
    StreamPutCommitInput {
        versioning: BucketVersioningState::Suspended,
        version_id: VersionId::Null,
        owner: crate::OwnerIdentity::from_principal("direct-owner"),
        acl_grants: Default::default(),
        public_read: true,
        etag_crc64: encrypted.payload_crc64,
        tags: None,
        metadata_blob: Default::default(),
        system_metadata_blob: Default::default(),
        object_lock: Default::default(),
        encryption: encrypted.committed_encryption(&subject),
    }
}

fn evidence(owner: &StagingOwnerRecord) -> crate::types::CommittedObjectEncryption {
    let fixture = crate::tests::sse_s3_segment_test_fixture(b"data", 0);
    let subject = crate::ObjectEncryptionSubject::standard_object(
        owner.subject.bucket_incarnation,
        &owner.subject.bucket,
        &owner.subject.key,
        owner.subject.payload_generation,
        &fixture.encryption,
    );
    fixture.committed_encryption_evidence(&subject)
}

macro_rules! witnessed {
    ($owner:expr, $writer:ident, $section:expr) => {{
        let shards = files::shards($owner, b"data");
        for (i, bytes) in shards.iter().enumerate() {
            $writer.write(i as u8, bytes);
        }
        $writer.persist().unwrap();
        let mut stored = shards[..usize::from($owner.windows[0].geometry.ec.k)].concat();
        stored.truncate($owner.windows[0].geometry.codec.stored_size(4).unwrap() as usize);
        let Writer::Direct(writer) = $writer else {
            unreachable!()
        };
        writer
            .into_acknowledged()
            .unwrap_or_else(|e| panic!("{}", e.error()))
            .prepare_direct_embedded($section)
            .unwrap_or_else(|e| panic!("{}", e.error()))
            .with_integrity(checksum::crc64::checksum(&stored))
            .unwrap_or_else(|e| panic!("{}", e.error()))
    }};
}

#[path = "payload_staging_direct_terminal_install_tests.rs"]
mod installation_tests;

fn changes(f: &Setup) -> Vec<u64> {
    (0..5)
        .map(|n| {
            f.map
                .node(NodeId::new(n))
                .unwrap()
                .test_node()
                .get_pg(f.context.id().pg_id().get())
                .unwrap()
                .conn
                .total_changes()
        })
        .collect()
}

#[test]
fn staging_direct_terminal_preparation_derives_command_without_effects() {
    for versioning in [
        BucketVersioningState::Disabled,
        BucketVersioningState::Suspended,
        BucketVersioningState::Enabled,
    ] {
        with_writer_and_section(
            StagingTarget::DirectPut,
            false,
            |f, owner, mut writer, section| {
                let witnessed = witnessed!(owner, writer, section);
                let history = witnessed.test_binding_state();
                let manifest = witnessed.test_manifest();
                let integrity = manifest.integrity.durable_record();
                let crc = manifest.crc64;
                let mut input = input(owner);
                input.versioning = versioning;
                if versioning == BucketVersioningState::Enabled {
                    input.version_id = VersionId::from_u64(100);
                }
                let before = changes(f);
                let prepared = witnessed
                    .prepare_terminal_embedded(section, &input, &evidence(owner))
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                assert_eq!(prepared.test_binding_state(), history);
                assert_eq!(
                    prepared.id().log_index().get(),
                    f.context.id().log_index().get() + 1
                );
                let command = prepared.test_command();
                assert!(command.verify_checksum());
                let MetadataCommandPayload::CommitDirectPutObject(commit) = command.payload()
                else {
                    panic!("direct command")
                };
                assert_eq!(commit.object.bucket, owner.subject.bucket);
                assert_eq!(commit.object.key, owner.subject.key);
                assert_eq!(
                    commit.object.generation_id,
                    owner.subject.payload_generation
                );
                assert_eq!(commit.object.version_id, input.version_id);
                assert_eq!(commit.object.size, 4);
                assert_eq!(commit.object.owner, input.owner);
                assert_eq!(commit.object.acl_grants, input.acl_grants);
                assert_eq!(commit.object.public_read, input.public_read);
                assert_eq!(commit.object.encryption, input.encryption);
                assert_eq!(
                    commit.object.etag,
                    crate::ObjectEtag::single_part(input.etag_crc64)
                );
                assert_eq!(commit.object.tags, input.tags);
                assert_eq!(commit.object.metadata_blob, Some(input.metadata_blob));
                assert_eq!(
                    commit.object.system_metadata_blob,
                    Some(input.system_metadata_blob)
                );
                assert_eq!(commit.object.object_lock, input.object_lock);
                assert_eq!(commit.generation_reservation_id, owner.subject.id.attempt);
                let ResourceResponsibility::Live(resource) = &owner.resources.creation else {
                    panic!("live creation")
                };
                assert_eq!(
                    StagingBucketResource::capture(
                        &commit.bucket_write_reservation,
                        resource.release_pg
                    ),
                    *resource
                );
                assert_eq!(commit.segments.len(), 1);
                let segment = &commit.segments[0];
                let window = &owner.windows[0];
                assert_eq!(segment.segment_index, 0);
                assert_eq!(segment.segment_crc64, crc);
                assert_eq!(segment.segment_okh, window.segment_hash);
                assert_eq!(segment.segment_vid, window.start);
                assert_eq!(segment.data_pg_id, window.participants.data_pg.get());
                assert_eq!(
                    segment.placement_cluster_epoch,
                    window.participants.placement_epoch
                );
                assert_eq!(
                    (segment.ec_k, segment.ec_m),
                    (window.geometry.ec.k, window.geometry.ec.m)
                );
                assert_eq!(segment.shard_integrity, Some(integrity));
                assert!(commit.stale_payload.is_none());
                drop(prepared);
                assert_eq!(changes(f), before);
                installation::assert_payload_retained(f, owner);
            },
        );
    }
}

#[test]
fn staging_direct_terminal_preparation_rejection_returns_unchanged_witnessed_custody() {
    for fault in [
        "encryption",
        "version",
        "snapshot",
        "reservation",
        "expiry",
        "foreign",
    ] {
        let foreign = allocation::setup_live(StagingTarget::DirectPut, false);
        with_writer_and_section(
            StagingTarget::DirectPut,
            false,
            |f, owner, mut writer, section| {
                let mut witnessed = witnessed!(owner, writer, section);
                let history = witnessed.test_binding_state();
                let mut input = input(owner);
                let good = input.clone();
                let admission = foreign.handle.admit_current_route().unwrap();
                let route = admission
                    .embedded_publication_route(
                        NodeId::new(4),
                        witnessed.id().pg_id(),
                        witnessed.id().cluster_epoch(),
                    )
                    .unwrap();
                let foreign_section = route.lock_until(deadline()).unwrap();
                match fault {
                    "encryption" => {
                        input.encryption =
                            crate::tests::sse_s3_segment_test_fixture(b"data", 0).encryption
                    }
                    "version" => input.versioning = BucketVersioningState::Enabled,
                    "snapshot" => witnessed.test_cross_snapshot(),
                    "reservation" => {
                        f.map.node(NodeId::new(4)).unwrap().test_node().get_pg(owner.subject.object_pg.get()).unwrap()
                        .conn.execute_batch("UPDATE object_generation_reservations SET generation_id = generation_id + 1").unwrap();
                    }
                    "expiry" | "foreign" => (),
                    _ => unreachable!(),
                }
                let before = [changes(f), changes(&foreign)];
                let result = if fault == "expiry" {
                    crate::clock::with_time_override(owner.windows[0].cutoff_ms, || {
                        witnessed.prepare_terminal_embedded(section, &input, &evidence(owner))
                    })
                } else {
                    witnessed.prepare_terminal_embedded(
                        if fault == "foreign" {
                            &foreign_section
                        } else {
                            section
                        },
                        &input,
                        &evidence(owner),
                    )
                };
                let failure = result.err().expect(fault);
                if fault == "snapshot" {
                    assert!(
                        matches!(failure.error(), E::Window(message) if message == "direct terminal snapshot changed")
                    );
                }
                let witnessed = failure.into_witnessed();
                assert_eq!(witnessed.test_binding_state(), history);
                assert_eq!([changes(f), changes(&foreign)], before);
                if matches!(fault, "encryption" | "version" | "foreign") {
                    let prepared = witnessed
                        .prepare_terminal_embedded(section, &good, &evidence(owner))
                        .unwrap_or_else(|e| panic!("{fault}: {}", e.error()));
                    assert_eq!(prepared.test_binding_state(), history);
                    drop(prepared);
                } else {
                    drop(witnessed);
                }
                assert_eq!([changes(f), changes(&foreign)], before);
                installation::assert_files_retained(f, owner);
                if fault != "reservation" {
                    assert_eq!(custody(f, owner), *owner);
                }
            },
        );
    }
}

#[test]
fn staging_direct_terminal_preparation_requires_exact_displacement_owner() {
    for retained_owner in [false, true] {
        let f = allocation::setup_live_with_floor(StagingTarget::DirectPut, false, Some(10));
        // Metadata-only empty predecessor from an earlier epoch. No predecessor
        // shard is read or fabricated; the replacement below uses real ciphertext.
        let mut old = fixture(StagingTarget::StreamPut);
        old.subject.payload_generation = GenerationId::MIN;
        old.subject.originating_epoch = ClusterEpoch::new(4).unwrap();
        old.subject.id = StagingOwnerId {
            attempt: crate::tests::stream_session_id("prior-terminal"),
            introduction: MetadataCommandId::new(
                old.subject.originating_epoch,
                old.subject.object_pg,
                crate::metadata_command::MetadataCommandLogIndex::new(1).unwrap(),
            ),
        };
        old.windows = vec![window(&old.subject, 0, old.subject.first_vid())];
        old.reserved_end = old.windows[0].end;
        old.maximum_cutoff_ms = old.windows[0].cutoff_ms;
        old.resources.generation = ResourceResponsibility::Live(StagingGenerationResource {
            reservation_id: old.subject.id.attempt.clone(),
            generation: old.subject.payload_generation,
        });
        old.resources.creation = ResourceResponsibility::Live(StagingBucketResource::capture(
            &proof(&old.subject, old.subject.creation_operation()),
            PgId::new(topology().bucket_pg_for(&old.subject.bucket)),
        ));
        let old = old.published_standard(&[]).unwrap();
        let topology = topology();
        let checked = old.validate(&topology).unwrap();
        let seed_clock =
            crate::clock::test_time_override_guard(crate::clock::current_time_millis());
        for n in 0..5 {
            let node = f.map.node(NodeId::new(n)).unwrap().test_node();
            let pg = node.get_pg(old.subject.object_pg.get()).unwrap();
            pg.put_object_with_segments(
                &crate::PutLiveObjectReq {
                    bucket: old.subject.bucket.clone(),
                    key: old.subject.key.clone(),
                    version_id: VersionId::Null,
                    owner: crate::OwnerIdentity::from_principal("prior-owner"),
                    acl_grants: Default::default(),
                    public_read: false,
                    generation_id: old.subject.payload_generation,
                    size: 0,
                    etag: crate::ObjectEtag::single_part(42),
                    ec: old.windows[0].geometry.ec,
                    layout: crate::ObjectLayout::Standard,
                    tags: None,
                    metadata_blob: None,
                    system_metadata_blob: None,
                    object_lock: Default::default(),
                    encryption: crate::tests::metadata_only_test_encryption(),
                },
                &[],
            )
            .unwrap();
            if retained_owner {
                pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
                sql::insert(&pg, old.subject.object_pg, &topology, &checked).unwrap();
                pg.conn.execute_batch("COMMIT").unwrap();
            }
            complete_application::anchor(&pg, f.context.id());
            pg.recover_complete_command_state(
                PgStoreRecoveryContext::for_node(NodeId::new(n)),
                &topology,
            )
            .unwrap();
        }
        drop(seed_clock);
        allocation::with_confirmed_duration(
            &f,
            "installed",
            1,
            Duration::from_secs(5),
            |section, mut windows| {
                let owner = files::load_owner(&f);
                let mut writer = Writer::Direct(Box::new(
                    windows
                        .pop()
                        .unwrap()
                        .into_direct_file_writer_embedded(
                            section,
                            &crate::tests::sse_s3_segment_test_fixture(b"data", 0).encryption,
                        )
                        .unwrap_or_else(|e| panic!("{}", e.error())),
                ));
                let witnessed = witnessed!(&owner, writer, section);
                assert_eq!(
                    witnessed.auth_snapshot().existing_etag,
                    Some(crate::ObjectEtag::single_part(42).format())
                );
                let before = changes(&f);
                let result =
                    witnessed.prepare_terminal_embedded(section, &input(&owner), &evidence(&owner));
                if retained_owner {
                    let prepared = result.unwrap_or_else(|e| panic!("{}", e.error()));
                    let command = prepared.test_command();
                    let MetadataCommandPayload::CommitDirectPutObject(commit) = command.payload()
                    else {
                        unreachable!()
                    };
                    let Some(crate::metadata_command::ObjectPayloadReclaimCommand::Segments(root)) =
                        &commit.stale_payload
                    else {
                        panic!("exact old reclaim")
                    };
                    assert_eq!(root.bucket, old.subject.bucket);
                    assert_eq!(root.key, old.subject.key);
                    assert_eq!(root.generation_id, old.subject.payload_generation);
                    assert!(root.segments.is_empty());
                    assert_eq!(root.created_at, commit.last_modified_millis);
                    drop(prepared);
                } else {
                    let failure = result
                        .err()
                        .expect("missing displaced owner must fail before command escape");
                    drop(failure.into_witnessed());
                }
                assert_eq!(changes(&f), before);
                installation::assert_payload_retained(&f, &owner);
                let pg = f
                    .map
                    .node(NodeId::new(4))
                    .unwrap()
                    .test_node()
                    .get_pg(old.subject.object_pg.get())
                    .unwrap();
                assert_eq!(
                    pg.get_object_version(&old.subject.bucket, &old.subject.key, VersionId::Null)
                        .unwrap()
                        .as_live()
                        .unwrap()
                        .generation_id,
                    old.subject.payload_generation
                );
                assert!(pg
                    .get_object_segments_reclaim(
                        &old.subject.bucket,
                        &old.subject.key,
                        old.subject.payload_generation
                    )
                    .unwrap()
                    .is_none());
                let snapshot = pg.command_build_snapshot().unwrap();
                assert_eq!(
                    sql::load(&pg.conn, old.subject.object_pg, &old.subject.id.attempt).unwrap(),
                    retained_owner.then_some(old.clone())
                );
                snapshot.finish().unwrap();
            },
        );
    }
}

#[test]
fn staging_direct_terminal_rejects_same_codec_crossed_encryption_subjects() {
    with_writer_and_section(
        StagingTarget::DirectPut,
        false,
        |f, owner, mut writer, section| {
            let mut witnessed = witnessed!(owner, writer, section);
            let original_history = witnessed.test_binding_state();
            let before = changes(f);
            for field in [
                "bucket",
                "key",
                "generation",
                "incarnation",
                "instance",
                "material",
                "state",
            ] {
                let mut encrypted = crate::tests::sse_s3_segment_test_fixture(b"data", 0);
                let mut crossed = owner.clone();
                match field {
                    "bucket" => {
                        crossed.subject.bucket = crate::tests::bucket_name("crossed-bucket")
                    }
                    "key" => crossed.subject.key = crate::tests::object_key("crossed-key"),
                    "generation" => {
                        crossed.subject.payload_generation =
                            GenerationId::new(owner.subject.payload_generation.get() + 1).unwrap()
                    }
                    "incarnation" => crossed.subject.bucket_incarnation += 1,
                    "instance" => {
                        let crate::ObjectEncryption::SseS3(state) = encrypted.encryption else {
                            unreachable!()
                        };
                        encrypted.encryption = crate::ObjectEncryption::SseS3(
                            state.with_encryption_instance_id([0xAB; 16]),
                        );
                    }
                    "material" => {
                        let crate::ObjectEncryption::SseS3(state) = &encrypted.encryption else {
                            unreachable!()
                        };
                        encrypted.encryption = crate::ObjectEncryption::SseS3(
                            crate::SseS3ObjectState::new_with_encryption_instance_id(
                                *state.encryption_instance_id(),
                                state.wrapping_key_id(),
                                *state.wrap_nonce(),
                                [0xAB; crate::types::SSE_S3_WRAPPED_DEK_LEN],
                                *state.segment_nonce_prefix(),
                            ),
                        );
                    }
                    "state" => (),
                    _ => unreachable!(),
                }
                let subject = crate::ObjectEncryptionSubject::standard_object(
                    crossed.subject.bucket_incarnation,
                    &crossed.subject.bucket,
                    &crossed.subject.key,
                    crossed.subject.payload_generation,
                    &encrypted.encryption,
                );
                let committed = encrypted.committed_encryption_evidence(&subject);
                let mut input = input(owner);
                input.encryption = committed.encryption().clone();
                assert_eq!(
                    input.encryption.payload_codec(),
                    owner.windows[0].geometry.codec
                );
                assert_eq!(
                    input.encryption.key_binding(),
                    crate::types::ObjectEncryptionKeyBinding::ObjectSubjectV2
                );
                // Logical crossings deliberately preserve the encryption instance:
                // the successor check alone cannot distinguish these real rewraps.
                if matches!(
                    field,
                    "bucket" | "key" | "generation" | "incarnation" | "material"
                ) {
                    assert!(witnessed
                        .encryption_subject()
                        .matches_committed_encryption(&input.encryption));
                }
                if field == "state" {
                    input.encryption = encrypted.committed_encryption(
                        &crate::ObjectEncryptionSubject::standard_object(
                            owner.subject.bucket_incarnation + 1,
                            &owner.subject.bucket,
                            &owner.subject.key,
                            owner.subject.payload_generation,
                            &encrypted.encryption,
                        ),
                    );
                }
                let failure = witnessed
                    .prepare_terminal_embedded(section, &input, &committed)
                    .err()
                    .expect(field);
                assert!(
                    matches!(failure.error(), E::Window(message) if message == "direct terminal committed encryption subject"),
                    "{field}: {}",
                    failure.error()
                );
                witnessed = failure.into_witnessed();
                assert_eq!(witnessed.test_binding_state(), original_history);
                assert_eq!(changes(f), before);
                installation::assert_payload_retained(f, owner);
            }
            let fixture = crate::tests::sse_s3_segment_test_fixture(b"data", 0);
            let committed = fixture.committed_encryption_evidence(witnessed.encryption_subject());
            let mut input = input(owner);
            input.encryption = committed.encryption().clone();
            let prepared = witnessed
                .prepare_terminal_embedded(section, &input, &committed)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            assert_eq!(prepared.test_binding_state(), original_history);
            drop(prepared);
            assert_eq!(changes(f), before);
            installation::assert_payload_retained(f, owner);
        },
    );
}
