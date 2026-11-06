// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Immutable evidence for the untagged pending indexes present in PG schema 10
//! and unchanged in schema 11. These are metadata-only fixtures: no command is
//! applied and no shard payload is written. Keep these literals when the indexes
//! acquire staging-window protection; do not regenerate them with a new writer.

use super::*;
use crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND;

fn hex(value: &str) -> Vec<u8> {
    crate::tests::frozen_hex_fixture(value)
}

#[test]
fn pending_index_route_v1_frozen_checksum_inputs_and_sql_roundtrip() {
    let id = MetadataCommandId::new(
        ClusterEpoch::new(0x0102_0304_0506_0708).unwrap(),
        PgId::new(0x1122_3344),
        MetadataCommandLogIndex::new(0x2122_2324_2526_2728).unwrap(),
    );
    let command_checksum = 0x8182_8384_8586_8788;
    let dependencies = [
        PendingMetadataCommandRouteDependency {
            cluster_epoch: ClusterEpoch::new(0x3132_3334_3536_3738).unwrap(),
            bucket_route_hash: 0x9192_9394_9596_9798,
        },
        PendingMetadataCommandRouteDependency {
            cluster_epoch: ClusterEpoch::new(0x4142_4344_4546_4748).unwrap(),
            bucket_route_hash: 0xa1a2_a3a4_a5a6_a7a8,
        },
    ];
    // Independent byte specimens: command epoch, eight-byte PG, index, checksum,
    // one-byte count, then ordered (epoch, bucket-hash) pairs. The two-entry case
    // pins the schema's accepted capacity, even though current commands project
    // at most one reservation proof. CRC literals were independently checked
    // using bitwise CRC-64/NVME (reflected polynomial 0x9a6c9329ac4bc9b5,
    // all-ones initial state and final XOR); never derive expectations from
    // the production checksum implementation.
    let specimens = [
        (
            concat!(
                "0102030405060708",
                "0000000011223344",
                "2122232425262728",
                "8182838485868788",
                "00",
            ),
            0xaa81_fe2c_aff1_fe8f_u64,
        ),
        (
            concat!(
                "0102030405060708",
                "0000000011223344",
                "2122232425262728",
                "8182838485868788",
                "01",
                "3132333435363738",
                "9192939495969798",
            ),
            0xc6cf_cd43_70d8_40aa_u64,
        ),
        (
            concat!(
                "0102030405060708",
                "0000000011223344",
                "2122232425262728",
                "8182838485868788",
                "02",
                "3132333435363738",
                "9192939495969798",
                "4142434445464748",
                "a1a2a3a4a5a6a7a8",
            ),
            0xa6a2_44c4_dc12_03bd_u64,
        ),
    ];
    for (count, (specimen, expected_checksum)) in specimens.into_iter().enumerate() {
        let mut input = b"argmin.metadata.pending-route-dependencies.v1".to_vec();
        input.extend(hex(specimen));
        assert_eq!(
            checksum::crc64::checksum(&input),
            expected_checksum,
            "pending route index CRC parameters changed for count {count}",
        );
        let checksum = PgStore::pending_metadata_command_route_dependency_checksum(
            id,
            command_checksum,
            &dependencies[..count],
        );
        assert_eq!(
            checksum, expected_checksum,
            "pending route index grammar changed for count {count}",
        );

        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), id.pg_id().get()).unwrap();
        // The lease-critical reader must use the sidecar, not decode command
        // bytes. This raw test seam deliberately supplies undecodable bytes.
        store
            .test_insert_raw_pending_metadata_command_slot(id, command_checksum, b"opaque", None)
            .unwrap();
        store
            .replace_pending_metadata_command_route_dependency_sidecar(
                &PendingMetadataCommandRouteDependencySidecar {
                    command_checksum,
                    dependencies: dependencies[..count].to_vec(),
                    checksum,
                },
            )
            .unwrap();
        drop(store);
        let store = PgStore::open(tmp.path(), id.pg_id().get()).unwrap();
        let persisted_checksum: i64 = store
            .conn
            .query_row(
                "SELECT dependency_checksum FROM metadata_command_pending_route_dependencies \
                 WHERE singleton = 0",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(persisted_checksum as u64, expected_checksum);
        assert_eq!(
            store.pending_metadata_command_route_references(0).unwrap(),
            Some(PendingMetadataCommandRouteReferences {
                id,
                command_checksum,
                dependencies: dependencies[..count].to_vec(),
            }),
        );
        store
            .conn
            .execute(
                "UPDATE metadata_command_pending_route_dependencies \
                 SET dependency_checksum = ?1 WHERE singleton = 0",
                [checksum.wrapping_add(1) as i64],
            )
            .unwrap();
        assert!(store.pending_metadata_command_route_references(0).is_err());
    }
}

#[test]
fn pending_index_placed_and_reclaim_v1_frozen_records_from_slot_installation() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), 7).unwrap();
    let bucket = BucketName::try_from("pending-index-format").unwrap();
    let key = ObjectKey::try_from("object").unwrap();
    let generation = GenerationId::new(2).unwrap();
    let epoch = ClusterEpoch::new(0x3132_3334_3536_3738).unwrap();
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            epoch,
            PgId::new(7),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::CommitDirectPutObject(Box::new(CommitDirectPutObjectCommand {
            object: PutLiveObjectReq {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::Null,
                owner: OwnerIdentity::from_principal("owner"),
                acl_grants: AclGrants::default(),
                public_read: false,
                generation_id: generation,
                size: 1024,
                etag: ObjectEtag::single_part(0),
                ec: EcShape { k: 4, m: 2 },
                layout: ObjectLayout::Standard,
                tags: None,
                metadata_blob: Some(SerializedMetadataBlob::default()),
                system_metadata_blob: Some(SerializedSystemMetadataBlob::default()),
                object_lock: ObjectLockState::default(),
                encryption: crate::tests::committed_metadata_only_test_encryption(
                    1, &bucket, &key, generation,
                ),
            },
            segments: vec![ObjectSegmentRecord {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::Null,
                segment_index: 0,
                size: 1024,
                segment_crc64: 0x9192_9394_9596_9798,
                segment_okh: std::array::from_fn(|index| index as u8),
                segment_vid: GenerationId::new(0x1112_1314_1516_1718).unwrap(),
                data_pg_id: 0x0102_0304,
                placement_cluster_epoch: ClusterEpoch::new(0x2122_2324_2526_2728).unwrap(),
                ec_k: 4,
                ec_m: 2,
                shard_integrity: None,
            }],
            generation_reservation_id: SessionId::try_from("12".repeat(16)).unwrap(),
            write_sequence: 1,
            last_modified_millis: 1,
            stale_payload: Some(ObjectPayloadReclaimCommand::Segments(
                ObjectSegmentsReclaimRecord {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    generation_id: GenerationId::MIN,
                    created_at: 1,
                    segments: vec![ObjectSegmentsReclaimSegmentRecord {
                        segment_index: 0,
                        segment_okh: std::array::from_fn(|index| 0xf0 + index as u8),
                        segment_vid: GenerationId::new(0x1122_3344_5566_7788).unwrap(),
                        data_pg_id: 0x1122_3344,
                        ec: EcShape { k: 4, m: 2 },
                    }],
                },
            )),
            bucket_write_reservation: BucketWriteReservationProof {
                bucket: bucket.clone(),
                reservation_id: "pending-index-proof".to_owned(),
                owner_token: "pending-index-owner".to_owned(),
                cluster_epoch: epoch,
                bucket_execution_generation: 1,
                bucket_incarnation_generation: 1,
                operation_kind: PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND.to_owned(),
                created_at: 1,
                lease_deadline: 2,
                target_context: Some(key.as_str().to_owned()),
            },
        })),
    );
    store
        .try_insert_pending_metadata_command_slot(0, &command, Some(&bucket))
        .unwrap();
    drop(store);
    let store = PgStore::open(tmp.path(), 7).unwrap();
    // The stored size is 1024 + 56-byte Cobblestone header + 16-byte terminal
    // tag = 1096 (0x448), not plaintext size or per-shard padded size.
    let placed = hex(concat!(
        "01020304",
        "000102030405060708090a0b0c0d0e0f",
        "1112131415161718",
        "2122232425262728",
        "0000000000000448",
        "9192939495969798",
        "0402",
    ));
    let reclaim = hex(concat!(
        "11223344",
        "f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff",
        "1122334455667788",
        "0402",
    ));
    assert_eq!(placed.len(), 54);
    assert_eq!(reclaim.len(), 30);
    let stored: (i64, i64, Vec<u8>, Vec<u8>) = store.conn.query_row(
        "SELECT placed_segment_reference_count, reclaim_reference_count, \
         (SELECT encoded_references FROM metadata_command_pending_placed_reference_pages WHERE page_index = 0), \
         (SELECT encoded_references FROM metadata_command_pending_reclaim_reference_pages WHERE page_index = 0) \
         FROM metadata_command_pending_slot WHERE singleton = 0",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    ).unwrap();
    assert_eq!(stored, (1, 1, placed, reclaim));
    // Read the frozen index independently of the envelope, just as bounded
    // scavenger discovery does. Neither reference is reconstructed from bytes
    // decoded out of the command in this canary.
    store
        .test_set_pending_metadata_command_bytes(b"opaque")
        .unwrap();
    let page = store
        .list_shard_scavenger_reference_page(
            &crate::PgTopology::new(&[7, 0x0102_0304, 0x1122_3344]).unwrap(),
            None,
            std::num::NonZeroU16::new(64).unwrap(),
        )
        .unwrap();
    assert!(page.complete);
    assert_eq!(
        page.items
            .into_iter()
            .map(|item| item.reference)
            .collect::<Vec<_>>(),
        vec![
            ShardScavengerPayloadReference::Placed(ShardScavengerPlacedShardSetReference {
                data_pg_id: 0x0102_0304,
                okh: std::array::from_fn(|index| index as u8),
                generation_id: GenerationId::new(0x1112_1314_1516_1718).unwrap(),
                placement_cluster_epoch: ClusterEpoch::new(0x2122_2324_2526_2728).unwrap(),
                stored_size: 1096,
                crc64: 0x9192_9394_9596_9798,
                ec: EcShape { k: 4, m: 2 },
                shard_integrity: None,
            }),
            ShardScavengerPayloadReference::ProtectionOnly(
                ShardScavengerProtectedShardSetReference {
                    data_pg_id: 0x1122_3344,
                    okh: std::array::from_fn(|index| 0xf0 + index as u8),
                    generation_id: GenerationId::new(0x1122_3344_5566_7788).unwrap(),
                    ec: EcShape { k: 4, m: 2 },
                }
            ),
        ],
    );
}
