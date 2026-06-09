use super::{bucket_name, multipart_upload_id, object_key, stream_session_id};
use crate::traits::{PgMetadataStore, ShardStore, StorageNode};
use crate::types::*;
use std::num::NonZeroU64;

fn test_owner() -> OwnerIdentity {
    OwnerIdentity::from_principal("owner")
}

/// Integration test: write shard data + object metadata, read both back.
#[test]
fn shard_and_metadata_roundtrip() {
    let dir = test_util::tempdir();
    let pg_dir = dir.path().join("pg-0000");
    let store = crate::PgStore::open(&pg_dir, 0).unwrap();

    let hash = [0x42u8; 16];
    let shard_key = ShardKey::new(&hash, 1, 0);
    let shard_data = b"the actual object data payload";

    // Write shard.
    let ack = store.write_shard(&shard_key, shard_data).unwrap();

    // Write object metadata referencing the shard.
    let req = PutObjectReq::Live(PutLiveObjectReq {
        bucket: bucket_name("test-bucket"),
        key: object_key("my/object.txt"),
        version_id: VersionId::Null,
        owner: test_owner(),
        acl_grants: AclGrants::default(),
        public_read: false,
        generation_id: GenerationId::MIN,
        size: shard_data.len() as u64,
        etag: ObjectEtag::single_part(ack.crc64),
        ec: EcShape { k: 4, m: 2 },
        layout: ObjectLayout::Standard,
        tags: None,
        metadata_blob: None,
        system_metadata_blob: None,
        object_lock: ObjectLockState::default(),
        encryption: ObjectEncryption::None,
    });
    store.put_object_meta(&req).unwrap();

    // Read back both.
    let read = store.read_shard(&shard_key).unwrap();
    assert_eq!(read.data, shard_data);
    assert_eq!(read.crc64, ack.crc64);

    let obj = store
        .get_object_meta(&bucket_name("test-bucket"), &object_key("my/object.txt"))
        .unwrap();
    let live = obj.as_live().unwrap();
    assert_eq!(live.size, shard_data.len() as u64);
    assert_eq!(live.etag, ObjectEtag::single_part(ack.crc64));
    assert_eq!(live.owner, test_owner());
}

/// Integration test: LocalStorageNode with multiple PGs.
#[test]
fn local_storage_node_multi_pg() {
    let dir = test_util::tempdir();
    let node = crate::LocalStorageNode::open(dir.path(), &[0, 1, 2]).unwrap();

    assert_eq!(node.pg_ids(), &[0, 1, 2]);

    // Write to different PGs.
    for pg_id in 0..3u32 {
        let pg = node.get_pg(pg_id).unwrap();
        let hash = [pg_id as u8; 16];
        let key = ShardKey::new(&hash, 1, 0);
        pg.write_shard(&key, &[pg_id as u8; 100]).unwrap();
    }

    // Read back from each PG.
    for pg_id in 0..3u32 {
        let pg = node.get_pg(pg_id).unwrap();
        let hash = [pg_id as u8; 16];
        let key = ShardKey::new(&hash, 1, 0);
        let read = pg.read_shard(&key).unwrap();
        assert_eq!(read.data, vec![pg_id as u8; 100]);
    }

    // Non-existent PG.
    assert!(matches!(
        node.get_pg(99),
        Err(crate::StoreError::PgNotFound { pg_id: 99 })
    ));
}

/// Integration test: StorageNode trait (dyn dispatch).
#[test]
fn storage_node_trait() {
    let dir = test_util::tempdir();
    let node = crate::LocalStorageNode::open(dir.path(), &[5, 10]).unwrap();
    let node: &dyn StorageNode = &node;

    assert_eq!(node.pg_ids(), &[5, 10]);

    let pg = node.get_pg_store(5).unwrap();
    let key = ShardKey::new(&[0x55; 16], 1, 0);
    pg.write_shard(&key, b"trait test").unwrap();

    let read = pg.read_shard(&key).unwrap();
    assert_eq!(read.data, b"trait test");
}

/// Integration test: bucket + object metadata lifecycle on a single PG.
#[test]
fn full_lifecycle() {
    let dir = test_util::tempdir();

    // Create storage node.
    let node = crate::LocalStorageNode::open(&dir.path().join("data"), &[0]).unwrap();
    let pg = node.get_pg(0).unwrap();
    pg.create_bucket(
        &bucket_name("my-bucket"),
        "owner-1",
        &CanonicalUserId::from_principal("owner-1"),
        &AclGrants::default(),
        false,
        false,
    )
    .unwrap();

    // PutObject: write shard + metadata.
    let hash = [0xAA; 16];
    let shard_key = ShardKey::new(&hash, 1, 0);
    let data = b"hello world";
    let ack = pg.write_shard(&shard_key, data).unwrap();

    pg.put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
        bucket: bucket_name("my-bucket"),
        key: object_key("greeting.txt"),
        version_id: VersionId::Null,
        owner: test_owner(),
        acl_grants: AclGrants::default(),
        public_read: false,
        generation_id: GenerationId::MIN,
        size: data.len() as u64,
        etag: ObjectEtag::single_part(ack.crc64),
        ec: EcShape { k: 4, m: 2 },
        layout: ObjectLayout::Standard,
        tags: None,
        metadata_blob: None,
        system_metadata_blob: None,
        object_lock: ObjectLockState::default(),
        encryption: ObjectEncryption::None,
    }))
    .unwrap();

    // HeadBucket.
    let info = pg.head_bucket(&bucket_name("my-bucket")).unwrap();
    assert_eq!(info.name, "my-bucket");

    // GetObject: read metadata + shard.
    let obj = pg
        .get_object_meta(&bucket_name("my-bucket"), &object_key("greeting.txt"))
        .unwrap();
    assert_eq!(obj.as_live().unwrap().size, 11);

    let read = pg.read_shard(&shard_key).unwrap();
    assert_eq!(read.data, b"hello world");

    // DeleteObject: remove metadata + shard.
    pg.delete_object_meta(&bucket_name("my-bucket"), &object_key("greeting.txt"))
        .unwrap();
    pg.delete_shard(&shard_key).unwrap();

    // Verify gone.
    let err = pg
        .get_object_meta(&bucket_name("my-bucket"), &object_key("greeting.txt"))
        .unwrap_err();
    assert!(matches!(err, crate::MetadataError::ObjectNotFound));

    let err = pg.read_shard(&shard_key).unwrap_err();
    assert!(matches!(err, crate::StoreError::NotFound));
}

/// Test PgStore reopening with data persistence.
#[test]
fn pg_store_persistence() {
    let dir = test_util::tempdir();
    let pg_dir = dir.path().join("pg-0000");

    let key = ShardKey::new(&[0xBB; 16], 1, 0);
    let data = b"persistent data";

    // Write data, then drop the store.
    {
        let store = crate::PgStore::open(&pg_dir, 0).unwrap();
        store.write_shard(&key, data).unwrap();
        store
            .put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
                bucket: bucket_name("bucket"),
                key: object_key("k"),
                version_id: VersionId::Null,
                owner: test_owner(),
                acl_grants: AclGrants::default(),
                public_read: false,
                generation_id: GenerationId::MIN,
                size: data.len() as u64,
                etag: ObjectEtag::SinglePart([1, 0, 0, 0, 0, 0, 0, 0]),
                ec: EcShape { k: 4, m: 2 },
                layout: ObjectLayout::Standard,
                tags: None,
                metadata_blob: None,
                system_metadata_blob: None,
                object_lock: ObjectLockState::default(),
                encryption: ObjectEncryption::None,
            }))
            .unwrap();
    }

    // Reopen and verify data persists.
    {
        let store = crate::PgStore::open(&pg_dir, 0).unwrap();

        let read = store.read_shard(&key).unwrap();
        assert_eq!(read.data, data);

        let obj = store
            .get_object_meta(&bucket_name("bucket"), &object_key("k"))
            .unwrap();
        assert_eq!(obj.as_live().unwrap().size, data.len() as u64);
    }
}

// ── 1. Multipart upload lifecycle ──────────────────────────────────────

/// Full multipart flow: create upload → upsert parts with shards →
/// commit_object_parts → complete_multipart_commit → read back everything.
#[test]
fn multipart_upload_lifecycle() {
    let dir = test_util::tempdir();
    let pg_dir = dir.path().join("pg-0000");
    let store = crate::PgStore::open(&pg_dir, 0).unwrap();

    store
        .create_bucket(
            &bucket_name("bucket"),
            "owner",
            &CanonicalUserId::from_principal("owner"),
            &AclGrants::default(),
            false,
            false,
        )
        .unwrap();

    // Create multipart upload.
    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: multipart_upload_id("mpu-1"),
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            tags: None,
            metadata_blob: vec![].into(),
            system_metadata_blob: SerializedSystemMetadataBlob::default(),
            initiator: Some(test_owner()),

            owner: test_owner(),
            acl_grants: AclGrants::default(),
            public_read: false,
            object_lock: ObjectLockState::default(),
            checksum: None,
            encryption: ObjectEncryption::None,
        })
        .unwrap();

    // Write shard data for two parts.
    let part1_data = b"part-one-data-here";
    let part2_data = b"part-two-data-here";

    let hash1 = [0x11u8; 16];
    let shard_key1 = ShardKey::new(&hash1, 1, 0);
    let ack1 = store.write_shard(&shard_key1, part1_data).unwrap();

    let hash2 = [0x22u8; 16];
    let shard_key2 = ShardKey::new(&hash2, 1, 0);
    let ack2 = store.write_shard(&shard_key2, part2_data).unwrap();

    // Upsert parts.
    store
        .upsert_multipart_part(&MultipartPartRecord {
            upload_id: multipart_upload_id("mpu-1"),
            part_number: 1,
            generation: 0,
            size: part1_data.len() as u64,
            etag: ack1.crc64.to_be_bytes().to_vec(),
            etag_kind: EtagKind::Crc64,
            part_okh: hash1,
            part_vid: GenerationId::MIN,
            ec_k: 4,
            ec_m: 2,
            last_modified: 0,
            checksum: None,
        })
        .unwrap();
    store
        .upsert_multipart_part(&MultipartPartRecord {
            upload_id: multipart_upload_id("mpu-1"),
            part_number: 2,
            generation: 0,
            size: part2_data.len() as u64,
            etag: ack2.crc64.to_be_bytes().to_vec(),
            etag_kind: EtagKind::Crc64,
            part_okh: hash2,
            part_vid: GenerationId::MIN,
            ec_k: 4,
            ec_m: 2,
            last_modified: 0,
            checksum: None,
        })
        .unwrap();

    // Verify parts are listed.
    let parts = store
        .list_multipart_parts(&ListPartsReq {
            upload_id: multipart_upload_id("mpu-1"),
            part_number_marker: None,
            max_parts: 10,
        })
        .unwrap();
    assert_eq!(parts.parts.len(), 2);

    // Complete multipart: commit object + parts.
    let total_size = (part1_data.len() + part2_data.len()) as u64;
    let obj = CommitMultipartReq {
        bucket: bucket_name("bucket"),
        key: object_key("k"),
        version_id: VersionId::Null,
        owner: test_owner(),
        acl_grants: AclGrants::default(),
        public_read: false,
        generation_id: GenerationId::MIN,
        size: total_size,
        etag_crc64: [0xCC, 0, 0, 0, 0, 0, 0, 0],
        ec: EcShape { k: 4, m: 2 },
        tags: None,
        metadata_blob: Some(vec![].into()),
        system_metadata_blob: None,
        object_lock: ObjectLockState::default(),
        encryption: ObjectEncryption::None,
    };
    let committed_parts = vec![
        ObjectPartRecord {
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            version_id: VersionId::Null,
            part_number: 1,
            size: part1_data.len() as u64,
            etag: ack1.crc64.to_be_bytes().to_vec(),
            etag_kind: EtagKind::Crc64,
            part_okh: hash1,
            part_vid: GenerationId::MIN,
            ec_k: 4,
            ec_m: 2,
            data_pg_id: 0,
            checksum: None,
        },
        ObjectPartRecord {
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            version_id: VersionId::Null,
            part_number: 2,
            size: part2_data.len() as u64,
            etag: ack2.crc64.to_be_bytes().to_vec(),
            etag_kind: EtagKind::Crc64,
            part_okh: hash2,
            part_vid: GenerationId::MIN,
            ec_k: 4,
            ec_m: 2,
            data_pg_id: 0,
            checksum: None,
        },
    ];

    store
        .set_upload_state(&multipart_upload_id("mpu-1"), UploadState::Completing)
        .unwrap();
    store
        .complete_multipart_commit(&multipart_upload_id("mpu-1"), 1, &obj, &committed_parts)
        .unwrap();

    // Read back the committed object.
    let read_obj = store
        .get_object_meta(&bucket_name("bucket"), &object_key("k"))
        .unwrap();
    let live = read_obj.as_live().unwrap();
    assert_eq!(live.size, total_size);

    // Read back committed parts.
    let read_parts = store
        .get_object_parts(&bucket_name("bucket"), &object_key("k"), VersionId::Null)
        .unwrap();
    assert_eq!(read_parts.len(), 2);
    assert_eq!(read_parts[0].part_number, 1);
    assert_eq!(read_parts[1].part_number, 2);

    // Verify shards are still readable.
    let r1 = store.read_shard(&shard_key1).unwrap();
    assert_eq!(r1.data, part1_data);
    let r2 = store.read_shard(&shard_key2).unwrap();
    assert_eq!(r2.data, part2_data);
}

// ── 2. Streaming UploadPart lifecycle ──────────────────────────────────

/// Streaming part: create multipart → create stream session(UploadPart) →
/// append segments → commit_stream_part → verify part + part segments.
#[test]
fn streaming_upload_part_lifecycle() {
    let dir = test_util::tempdir();
    let pg_dir = dir.path().join("pg-0000");
    let store = crate::PgStore::open(&pg_dir, 0).unwrap();

    // Create multipart upload first.
    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: multipart_upload_id("mpu-sp"),
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            tags: None,
            metadata_blob: vec![].into(),
            system_metadata_blob: SerializedSystemMetadataBlob::default(),
            initiator: None,

            owner: test_owner(),
            acl_grants: AclGrants::default(),
            public_read: false,
            object_lock: ObjectLockState::default(),
            checksum: None,
            encryption: ObjectEncryption::None,
        })
        .unwrap();

    // Write shard data.
    let seg_data = b"streaming-part-segment";
    let hash = [0xBBu8; 16];
    let shard_key = ShardKey::new(&hash, 1, 0);
    let ack = store.write_shard(&shard_key, seg_data).unwrap();

    // Create streaming session for part 1.
    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: stream_session_id("ss-part"),
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            target: StreamUploadTarget::UploadPart {
                upload_id: multipart_upload_id("mpu-sp"),
                part_number: 1,
            },
            encryption: ObjectEncryption::None,
        })
        .unwrap();

    // Append staging segment.
    store
        .append_stream_segment(&StreamUploadSegmentRecord {
            session_id: stream_session_id("ss-part"),
            segment_index: 0,
            size: seg_data.len() as u64,
            segment_crc64: Some(ack.crc64),
            segment_okh: hash,
            segment_vid: GenerationId::MIN,
            data_pg_id: 0,
            ec_k: 4,
            ec_m: 2,
        })
        .unwrap();

    // Commit stream part.
    let part_segments = vec![MultipartPartSegmentRecord {
        bucket: bucket_name("bucket"),
        key: object_key("k"),
        upload_id: multipart_upload_id("mpu-sp"),
        version_id: MULTIPART_PART_SEGMENT_STAGING_VERSION_ID.to_u64(),
        part_number: 1,
        segment_index: 0,
        size: seg_data.len() as u64,
        segment_crc64: Some(ack.crc64),
        segment_okh: hash,
        segment_vid: GenerationId::MIN,
        data_pg_id: 0,
        ec_k: 4,
        ec_m: 2,
    }];

    let displaced_segments = store
        .commit_stream_part(
            &stream_session_id("ss-part"),
            &MultipartPartRecord {
                upload_id: multipart_upload_id("mpu-sp"),
                part_number: 1,
                generation: 0,
                size: seg_data.len() as u64,
                etag: ack.crc64.to_be_bytes().to_vec(),
                etag_kind: EtagKind::Crc64,
                part_okh: [0u8; 16], // zero sentinel for segmented parts
                part_vid: GenerationId::MIN,
                ec_k: 4,
                ec_m: 2,
                last_modified: 0,
                checksum: None,
            },
            &part_segments,
        )
        .unwrap();
    assert!(
        displaced_segments.is_empty(),
        "first stream-part commit should not displace any prior segments"
    );

    // Session gone.
    let err = store
        .get_stream_upload(&stream_session_id("ss-part"))
        .unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::StreamSessionNotFound { .. }
    ));

    // Part readable.
    let part = store
        .get_multipart_part(&multipart_upload_id("mpu-sp"), 1)
        .unwrap();
    assert_eq!(part.size, seg_data.len() as u64);

    // Part segments readable.
    let segs = store
        .get_all_multipart_part_segments_for_upload(&multipart_upload_id("mpu-sp"))
        .unwrap();
    assert_eq!(segs.len(), 1);
    assert_eq!(segs[0].segment_okh, hash);

    // Shard still readable.
    assert_eq!(store.read_shard(&shard_key).unwrap().data, seg_data);
}

// ── 4. Versioned object lifecycle ──────────────────────────────────────

/// Enable versioning → put multiple versions → list → get specific →
/// delete one → verify others intact.
#[test]
fn versioned_object_lifecycle() {
    let dir = test_util::tempdir();
    let pg_dir = dir.path().join("pg-0000");
    let store = crate::PgStore::open(&pg_dir, 0).unwrap();

    store
        .create_bucket(
            &bucket_name("bucket"),
            "owner",
            &CanonicalUserId::from_principal("owner"),
            &AclGrants::default(),
            false,
            false,
        )
        .unwrap();
    store
        .put_bucket_versioning(&bucket_name("bucket"), BucketVersioningState::Enabled)
        .unwrap();

    // Write 3 versions with shards.
    let mut shard_keys = Vec::new();
    let mut version_ids = Vec::new();
    for i in 1..=3u64 {
        let vid = VersionId::Versioned(NonZeroU64::new(i).unwrap());
        version_ids.push(vid);

        let hash = [i as u8; 16];
        let sk = ShardKey::new(&hash, 1, 0);
        let data = format!("version-{i}-data");
        store.write_shard(&sk, data.as_bytes()).unwrap();
        shard_keys.push(sk);

        store
            .put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
                bucket: bucket_name("bucket"),
                key: object_key("k"),
                version_id: vid,
                owner: test_owner(),
                acl_grants: AclGrants::default(),
                public_read: false,
                generation_id: GenerationId::MIN,
                ec: EcShape { k: 4, m: 2 },
                size: data.len() as u64,
                etag: ObjectEtag::SinglePart([i as u8, 0, 0, 0, 0, 0, 0, 0]),
                layout: ObjectLayout::Standard,
                tags: None,
                metadata_blob: None,
                system_metadata_blob: None,
                object_lock: ObjectLockState::default(),
                encryption: ObjectEncryption::None,
            }))
            .unwrap();
    }

    // List versions — should see all 3.
    let resp = store
        .list_object_versions(&ListObjectVersionsReq {
            bucket: bucket_name("bucket"),
            prefix: None,
            key_marker: None,
            version_id_marker: None,
            start_at: None,
            max_keys: 10,
        })
        .unwrap();
    assert_eq!(resp.versions.len(), 3);

    // Get specific version.
    let obj = store
        .get_object_version(&bucket_name("bucket"), &object_key("k"), version_ids[1])
        .unwrap();
    assert_eq!(obj.as_live().unwrap().size, "version-2-data".len() as u64);

    // Delete version 2.
    store
        .delete_object_version(&bucket_name("bucket"), &object_key("k"), version_ids[1])
        .unwrap();
    store.delete_shard(&shard_keys[1]).unwrap();

    // Version 2 gone.
    let err = store
        .get_object_version(&bucket_name("bucket"), &object_key("k"), version_ids[1])
        .unwrap_err();
    assert!(matches!(err, crate::MetadataError::ObjectNotFound));

    // Versions 1 and 3 intact.
    store
        .get_object_version(&bucket_name("bucket"), &object_key("k"), version_ids[0])
        .unwrap();
    store
        .get_object_version(&bucket_name("bucket"), &object_key("k"), version_ids[2])
        .unwrap();

    // List now shows 2.
    let resp = store
        .list_object_versions(&ListObjectVersionsReq {
            bucket: bucket_name("bucket"),
            prefix: None,
            key_marker: None,
            version_id_marker: None,
            start_at: None,
            max_keys: 10,
        })
        .unwrap();
    assert_eq!(resp.versions.len(), 2);

    // Remaining shards still readable.
    assert_eq!(
        store.read_shard(&shard_keys[0]).unwrap().data,
        b"version-1-data"
    );
    assert_eq!(
        store.read_shard(&shard_keys[2]).unwrap().data,
        b"version-3-data"
    );
}

// ── 5. Object overwrite with reclaim ───────────────────────────────────

/// Put object (gen 1) → record segment reclaim → overwrite (gen 2) → verify
/// reclaim points to old gen → delete reclaim.
#[test]
fn object_overwrite_with_reclaim() {
    let dir = test_util::tempdir();
    let pg_dir = dir.path().join("pg-0000");
    let store = crate::PgStore::open(&pg_dir, 0).unwrap();

    let gen1 = GenerationId::MIN;
    let gen2 = GenerationId::new(2).unwrap();

    // Write gen 1 object + shard.
    let hash1 = [0x10u8; 16];
    let sk1 = ShardKey::new(&hash1, gen1.get(), 0);
    store.write_shard(&sk1, b"gen-1-data").unwrap();

    store
        .put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            version_id: VersionId::Null,
            owner: test_owner(),
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id: gen1,
            ec: EcShape { k: 4, m: 2 },
            size: 10,
            etag: ObjectEtag::SinglePart([1, 0, 0, 0, 0, 0, 0, 0]),
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: None,
            system_metadata_blob: None,
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
        }))
        .unwrap();

    // Record reclaim for gen 1 before overwriting.
    store
        .put_object_segments_reclaim(&ObjectSegmentsReclaimRecord {
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            generation_id: gen1,
            created_at: 100,
            segments: vec![ObjectSegmentsReclaimSegmentRecord {
                segment_index: 0,
                segment_okh: hash1,
                segment_vid: gen1,
                data_pg_id: 0,
                ec: EcShape { k: 4, m: 2 },
            }],
        })
        .unwrap();

    // Overwrite with gen 2.
    let hash2 = [0x20u8; 16];
    let sk2 = ShardKey::new(&hash2, gen2.get(), 0);
    store.write_shard(&sk2, b"gen-2-data").unwrap();

    store
        .put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            version_id: VersionId::Null,
            owner: test_owner(),
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id: gen2,
            ec: EcShape { k: 4, m: 2 },
            size: 10,
            etag: ObjectEtag::SinglePart([2, 0, 0, 0, 0, 0, 0, 0]),
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: None,
            system_metadata_blob: None,
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
        }))
        .unwrap();

    // Current object is gen 2.
    let obj = store
        .get_object_meta(&bucket_name("bucket"), &object_key("k"))
        .unwrap();
    assert_eq!(
        obj.as_live().unwrap().etag,
        ObjectEtag::SinglePart([2, 0, 0, 0, 0, 0, 0, 0])
    );

    // Reclaim record for gen 1 still exists.
    let reclaim = store
        .get_object_segments_reclaim(&bucket_name("bucket"), &object_key("k"), gen1)
        .unwrap()
        .expect("reclaim should exist");
    assert_eq!(reclaim.generation_id, gen1);

    // Reclaim root points to gen 1 (earliest reclaim in bucket).
    let root = store
        .get_bucket_payload_reclaim_root(&bucket_name("bucket"))
        .unwrap()
        .expect("root should exist");
    assert_eq!(root.generation_id, gen1);

    // Clean up reclaim, then old shard.
    store
        .delete_object_segments_reclaim(&bucket_name("bucket"), &object_key("k"), gen1)
        .unwrap();
    store.delete_shard(&sk1).unwrap();

    // Reclaim gone.
    assert!(store
        .get_object_segments_reclaim(&bucket_name("bucket"), &object_key("k"), gen1)
        .unwrap()
        .is_none());

    // Gen 2 shard still readable.
    assert_eq!(store.read_shard(&sk2).unwrap().data, b"gen-2-data");
}

// ── 6. Bucket deletion lifecycle ───────────────────────────────────────

/// Create bucket → put objects → delete objects → mark deleting →
/// head_bucket_raw sees it → delete_bucket → gone.
#[test]
fn bucket_deletion_lifecycle() {
    let dir = test_util::tempdir();
    let pg_dir = dir.path().join("pg-0000");
    let store = crate::PgStore::open(&pg_dir, 0).unwrap();

    store
        .create_bucket(
            &bucket_name("doomed"),
            "owner",
            &CanonicalUserId::from_principal("owner"),
            &AclGrants::default(),
            false,
            false,
        )
        .unwrap();

    // Put an object.
    store
        .put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
            bucket: bucket_name("doomed"),
            key: object_key("file.txt"),
            version_id: VersionId::Null,
            owner: test_owner(),
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id: GenerationId::MIN,
            ec: EcShape { k: 4, m: 2 },
            size: 5,
            etag: ObjectEtag::SinglePart([1, 0, 0, 0, 0, 0, 0, 0]),
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: None,
            system_metadata_blob: None,
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
        }))
        .unwrap();

    // Add tags to the object.
    store
        .put_object_tags(
            &bucket_name("doomed"),
            &object_key("file.txt"),
            VersionId::Null,
            "<t>v</t>",
        )
        .unwrap();

    // Delete the object.
    store
        .delete_object_meta(&bucket_name("doomed"), &object_key("file.txt"))
        .unwrap();

    // Mark bucket as deleting.
    store.mark_bucket_deleting(&bucket_name("doomed")).unwrap();

    // head_bucket no longer sees it.
    let err = store.head_bucket(&bucket_name("doomed")).unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::BucketNotFound { .. }
    ));

    // head_bucket_raw still sees it in Deleting state.
    let info = store.head_bucket_raw(&bucket_name("doomed")).unwrap();
    assert_eq!(info.state, BucketState::Deleting);

    // list_buckets does not include it.
    let buckets = store
        .list_buckets(CanonicalUserId::from_principal("owner").as_str())
        .unwrap();
    assert!(buckets.is_empty());

    // Final delete.
    store
        .delete_finalized_bucket(&bucket_name("doomed"))
        .unwrap();

    // Completely gone.
    let err = store.head_bucket_raw(&bucket_name("doomed")).unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::BucketNotFound { .. }
    ));
}

// ── 7. Multipart abort cleanup ─────────────────────────────────────────

/// Create upload → upsert parts with segments → abort: delete segments
/// by upload_id → delete parts → delete upload → verify all cleaned up.
#[test]
fn multipart_abort_cleanup() {
    let dir = test_util::tempdir();
    let pg_dir = dir.path().join("pg-0000");
    let store = crate::PgStore::open(&pg_dir, 0).unwrap();

    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: multipart_upload_id("mpu-abort"),
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            tags: None,
            metadata_blob: vec![].into(),
            system_metadata_blob: SerializedSystemMetadataBlob::default(),
            initiator: None,

            owner: test_owner(),
            acl_grants: AclGrants::default(),
            public_read: false,
            object_lock: ObjectLockState::default(),
            checksum: None,
            encryption: ObjectEncryption::None,
        })
        .unwrap();

    // Write a shard.
    let hash = [0xDD; 16];
    let sk = ShardKey::new(&hash, 1, 0);
    store.write_shard(&sk, b"abort-me").unwrap();

    // Upsert part with segments.
    let part = MultipartPartRecord {
        upload_id: multipart_upload_id("mpu-abort"),
        part_number: 1,
        generation: 0,
        size: 8,
        etag: vec![0xAA],
        etag_kind: EtagKind::Crc64,
        part_okh: [0u8; 16], // zero sentinel for segmented parts
        part_vid: GenerationId::MIN,
        ec_k: 4,
        ec_m: 2,
        last_modified: 0,
        checksum: None,
    };
    let segments = vec![MultipartPartSegmentRecord {
        bucket: bucket_name("bucket"),
        key: object_key("k"),
        upload_id: multipart_upload_id("mpu-abort"),
        version_id: MULTIPART_PART_SEGMENT_STAGING_VERSION_ID.to_u64(),
        part_number: 1,
        segment_index: 0,
        size: 8,
        segment_crc64: Some(999),
        segment_okh: hash,
        segment_vid: GenerationId::MIN,
        data_pg_id: 0,
        ec_k: 4,
        ec_m: 2,
    }];
    store
        .upsert_multipart_part_segments(&part, &segments)
        .unwrap();

    // Verify part and segments exist.
    let p = store
        .get_multipart_part(&multipart_upload_id("mpu-abort"), 1)
        .unwrap();
    assert_eq!(p.size, 8);
    let segs = store
        .get_all_multipart_part_segments_for_upload(&multipart_upload_id("mpu-abort"))
        .unwrap();
    assert_eq!(segs.len(), 1);

    // Abort: clean up segments, then set state, then delete upload.
    store
        .delete_multipart_part_segments_by_upload_id(&multipart_upload_id("mpu-abort"))
        .unwrap();
    store
        .set_upload_state(&multipart_upload_id("mpu-abort"), UploadState::Aborting)
        .unwrap();
    store
        .delete_multipart_upload(&multipart_upload_id("mpu-abort"))
        .unwrap();

    // Verify upload gone.
    let err = store
        .get_multipart_upload(&multipart_upload_id("mpu-abort"))
        .unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::NoSuchUpload { .. }
    ));

    // Segments gone.
    let segs = store
        .get_all_multipart_part_segments_for_upload(&multipart_upload_id("mpu-abort"))
        .unwrap();
    assert!(segs.is_empty());

    // Shard still exists (shard cleanup is caller's responsibility).
    assert_eq!(store.read_shard(&sk).unwrap().data, b"abort-me");

    // Clean up shard.
    store.delete_shard(&sk).unwrap();
    assert!(matches!(
        store.read_shard(&sk).unwrap_err(),
        crate::StoreError::NotFound
    ));
}

// ── 8. Object tags through overwrite and versioned delete ──────────────

/// Tags are stored per-version. Overwriting an unversioned object clears
/// tags (since the old row is replaced). Versioned objects have
/// independent tags per version.
#[test]
fn object_tags_through_overwrite_and_versioned_delete() {
    let dir = test_util::tempdir();
    let pg_dir = dir.path().join("pg-0000");
    let store = crate::PgStore::open(&pg_dir, 0).unwrap();

    store
        .create_bucket(
            &bucket_name("bucket"),
            "owner",
            &CanonicalUserId::from_principal("owner"),
            &AclGrants::default(),
            false,
            false,
        )
        .unwrap();

    // Unversioned: put object, add tags, overwrite → tags gone.
    store
        .put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            version_id: VersionId::Null,
            owner: test_owner(),
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id: GenerationId::MIN,
            ec: EcShape { k: 4, m: 2 },
            size: 10,
            etag: ObjectEtag::SinglePart([1, 0, 0, 0, 0, 0, 0, 0]),
            layout: ObjectLayout::Standard,
            tags: Some("<t>old</t>".into()),
            metadata_blob: None,
            system_metadata_blob: None,
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
        }))
        .unwrap();

    let tags = store
        .get_object_tags(&bucket_name("bucket"), &object_key("k"), VersionId::Null)
        .unwrap();
    assert_eq!(tags.as_deref(), Some("<t>old</t>"));

    // Overwrite with no tags.
    store
        .put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            version_id: VersionId::Null,
            owner: test_owner(),
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id: GenerationId::new(2).unwrap(),
            ec: EcShape { k: 4, m: 2 },
            size: 20,
            etag: ObjectEtag::SinglePart([2, 0, 0, 0, 0, 0, 0, 0]),
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: None,
            system_metadata_blob: None,
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
        }))
        .unwrap();

    let tags = store
        .get_object_tags(&bucket_name("bucket"), &object_key("k"), VersionId::Null)
        .unwrap();
    assert!(tags.is_none(), "tags should be cleared after overwrite");

    // Versioned: put two versions with independent tags.
    store
        .put_bucket_versioning(&bucket_name("bucket"), BucketVersioningState::Enabled)
        .unwrap();

    let vid1 = VersionId::Versioned(NonZeroU64::new(10).unwrap());
    let vid2 = VersionId::Versioned(NonZeroU64::new(20).unwrap());

    for (vid, tag) in [(vid1, "<t>v1</t>"), (vid2, "<t>v2</t>")] {
        store
            .put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
                bucket: bucket_name("bucket"),
                key: object_key("tagged"),
                version_id: vid,
                owner: test_owner(),
                acl_grants: AclGrants::default(),
                public_read: false,
                generation_id: GenerationId::MIN,
                ec: EcShape { k: 4, m: 2 },
                size: 5,
                etag: ObjectEtag::SinglePart([1, 0, 0, 0, 0, 0, 0, 0]),
                layout: ObjectLayout::Standard,
                tags: Some(tag.into()),
                metadata_blob: None,
                system_metadata_blob: None,
                object_lock: ObjectLockState::default(),
                encryption: ObjectEncryption::None,
            }))
            .unwrap();
    }

    assert_eq!(
        store
            .get_object_tags(&bucket_name("bucket"), &object_key("tagged"), vid1)
            .unwrap()
            .as_deref(),
        Some("<t>v1</t>")
    );
    assert_eq!(
        store
            .get_object_tags(&bucket_name("bucket"), &object_key("tagged"), vid2)
            .unwrap()
            .as_deref(),
        Some("<t>v2</t>")
    );

    // Delete version 1 — version 2 tags unaffected.
    store
        .delete_object_version(&bucket_name("bucket"), &object_key("tagged"), vid1)
        .unwrap();

    let err = store
        .get_object_tags(&bucket_name("bucket"), &object_key("tagged"), vid1)
        .unwrap_err();
    assert!(matches!(err, crate::MetadataError::ObjectNotFound));

    assert_eq!(
        store
            .get_object_tags(&bucket_name("bucket"), &object_key("tagged"), vid2)
            .unwrap()
            .as_deref(),
        Some("<t>v2</t>")
    );
}

// ── 10. Persistence through reopen for complex state ───────────────────

/// Stream session and multipart upload survive PgStore reopen.
#[test]
fn persistence_complex_state_through_reopen() {
    let dir = test_util::tempdir();
    let pg_dir = dir.path().join("pg-0000");

    let persisted_session_id = stream_session_id("ss-persist");
    let upload_id = multipart_upload_id("mpu-persist");

    // Phase 1: create state, then drop the store.
    {
        let store = crate::PgStore::open(&pg_dir, 0).unwrap();

        // Create a streaming session.
        store
            .create_stream_upload(&CreateStreamUploadReq {
                session_id: persisted_session_id.clone(),
                bucket: bucket_name("bucket"),
                key: object_key("k1"),
                target: StreamUploadTarget::PutObject,
                encryption: ObjectEncryption::None,
            })
            .unwrap();

        // Append a staging segment.
        store
            .append_stream_segment(&StreamUploadSegmentRecord {
                session_id: persisted_session_id.clone(),
                segment_index: 0,
                size: 100,
                segment_crc64: Some(42),
                segment_okh: [0xEE; 16],
                segment_vid: GenerationId::MIN,
                data_pg_id: 0,
                ec_k: 4,
                ec_m: 2,
            })
            .unwrap();

        // Create a multipart upload with a part.
        store
            .create_multipart_upload(&CreateMultipartUploadReq {
                upload_id: upload_id.clone(),
                bucket: bucket_name("bucket"),
                key: object_key("k2"),
                tags: None,
                metadata_blob: vec![].into(),
                system_metadata_blob: SerializedSystemMetadataBlob::default(),
                initiator: Some(test_owner()),

                owner: test_owner(),
                acl_grants: AclGrants::default(),
                public_read: false,
                object_lock: ObjectLockState::default(),
                checksum: None,
                encryption: ObjectEncryption::None,
            })
            .unwrap();

        store
            .upsert_multipart_part(&MultipartPartRecord {
                upload_id: upload_id.clone(),
                part_number: 1,
                generation: 0,
                size: 200,
                etag: vec![0xBB],
                etag_kind: EtagKind::Crc64,
                part_okh: [0xFF; 16],
                part_vid: GenerationId::MIN,
                ec_k: 4,
                ec_m: 2,
                last_modified: 0,
                checksum: None,
            })
            .unwrap();

        // Write a shard.
        let sk = ShardKey::new(&[0xCC; 16], 1, 0);
        store.write_shard(&sk, b"persistent-shard").unwrap();
    }

    // Phase 2: reopen and verify everything survived.
    {
        let store = crate::PgStore::open(&pg_dir, 0).unwrap();

        // Stream session survives.
        let session = store.get_stream_upload(&persisted_session_id).unwrap();
        assert_eq!(session.bucket.as_str(), "bucket");
        assert_eq!(session.key.as_str(), "k1");

        // list_all_stream_uploads finds it.
        let all = store.list_all_stream_uploads().unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].session_id, persisted_session_id);

        // Staging segment survives.
        let segs = store.list_stream_segments(&persisted_session_id).unwrap();
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].segment_okh, [0xEE; 16]);

        // Multipart upload survives.
        let upload = store.get_multipart_upload(&upload_id).unwrap();
        assert_eq!(upload.state, UploadState::InProgress);

        // Part survives.
        let part = store.get_multipart_part(&upload_id, 1).unwrap();
        assert_eq!(part.size, 200);

        // Shard survives.
        let sk = ShardKey::new(&[0xCC; 16], 1, 0);
        let read = store.read_shard(&sk).unwrap();
        assert_eq!(read.data, b"persistent-shard");
    }
}
