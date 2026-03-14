use crate::traits::PgMetadataStore;
use crate::types::*;

/// Run the common metadata test suite against any PgMetadataStore implementation.
fn metadata_put_get_delete(store: &dyn PgMetadataStore) {
    let req = PutObjectReq::Live(PutLiveObjectReq {
        bucket: "test-bucket".into(),
        key: "test-key".into(),
        version_id: VersionId::Null,
        generation_id: GenerationId::MIN,
        ec: EcShape { k: 4, m: 2 },
        size: 1024,
        etag: ObjectEtag::SinglePart([0xAB, 0xCD, 0, 0, 0, 0, 0, 0]),
        layout: ObjectLayout::Standard,
        tags: None,
        metadata_blob: None,
    });

    // Put
    store.put_object_meta(&req).unwrap();

    // Get
    let obj = store.get_object_meta("test-bucket", "test-key").unwrap();
    assert_eq!(*obj.bucket(), "test-bucket");
    assert_eq!(*obj.key(), "test-key");
    assert_eq!(obj.version_id(), VersionId::Null);
    let live = obj.as_live().unwrap();
    assert_eq!(live.size, 1024);
    assert_eq!(
        live.etag,
        ObjectEtag::SinglePart([0xAB, 0xCD, 0, 0, 0, 0, 0, 0])
    );
    assert_eq!(live.etag.etag_kind(), EtagKind::Crc64);
    assert_eq!(live.ec.k, 4);
    assert_eq!(live.ec.m, 2);
    assert!(!obj.is_delete_marker());
    assert!(obj.last_modified() > 0);

    // Delete
    store.delete_object_meta("test-bucket", "test-key").unwrap();

    let err = store
        .get_object_meta("test-bucket", "test-key")
        .unwrap_err();
    assert!(matches!(err, crate::MetadataError::ObjectNotFound));
}

fn metadata_put_overwrites(store: &dyn PgMetadataStore) {
    let req1 = PutObjectReq::Live(PutLiveObjectReq {
        bucket: "b".into(),
        key: "k".into(),
        version_id: VersionId::Null,
        generation_id: GenerationId::MIN,
        ec: EcShape { k: 4, m: 2 },
        size: 100,
        etag: ObjectEtag::SinglePart([1, 0, 0, 0, 0, 0, 0, 0]),
        layout: ObjectLayout::Standard,
        tags: None,
        metadata_blob: None,
    });
    store.put_object_meta(&req1).unwrap();

    let req2 = PutObjectReq::Live(PutLiveObjectReq {
        bucket: "b".into(),
        key: "k".into(),
        version_id: VersionId::Null,
        generation_id: GenerationId::MIN,
        ec: EcShape { k: 4, m: 2 },
        size: 200,
        etag: ObjectEtag::SinglePart([2, 0, 0, 0, 0, 0, 0, 0]),
        layout: ObjectLayout::Standard,
        tags: None,
        metadata_blob: None,
    });
    store.put_object_meta(&req2).unwrap();

    let obj = store.get_object_meta("b", "k").unwrap();
    let live = obj.as_live().unwrap();
    assert_eq!(live.size, 200);
    assert_eq!(live.etag, ObjectEtag::SinglePart([2, 0, 0, 0, 0, 0, 0, 0]));
}

fn metadata_get_nonexistent(store: &dyn PgMetadataStore) {
    let err = store.get_object_meta("no-bucket", "no-key").unwrap_err();
    assert!(matches!(err, crate::MetadataError::ObjectNotFound));
}

fn metadata_list_basic(store: &dyn PgMetadataStore) {
    for i in 0..5 {
        let req = PutObjectReq::Live(PutLiveObjectReq {
            bucket: "list-bucket".into(),
            key: format!("obj-{i:02}").into(),
            version_id: VersionId::Null,
            generation_id: GenerationId::MIN,
            size: i * 100,
            etag: ObjectEtag::SinglePart([i as u8, 0, 0, 0, 0, 0, 0, 0]),
            ec: EcShape { k: 4, m: 2 },
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: None,
        });
        store.put_object_meta(&req).unwrap();
    }

    let resp = store
        .list_objects(&ListObjectsReq {
            bucket: "list-bucket".into(),
            prefix: None,
            start_after: None,
            max_keys: 100,
        })
        .unwrap();

    assert_eq!(resp.objects.len(), 5);
    assert!(!resp.is_truncated);
    // Verify ordering.
    for (i, obj) in resp.objects.iter().enumerate() {
        assert_eq!(*obj.key(), format!("obj-{i:02}"));
    }
}

fn metadata_list_with_prefix(store: &dyn PgMetadataStore) {
    for key in &["photos/a.jpg", "photos/b.jpg", "docs/c.txt", "photos/d.jpg"] {
        let req = PutObjectReq::Live(PutLiveObjectReq {
            bucket: "prefix-bucket".into(),
            key: (*key).into(),
            version_id: VersionId::Null,
            generation_id: GenerationId::MIN,
            ec: EcShape { k: 4, m: 2 },
            size: 100,
            etag: ObjectEtag::SinglePart([0; 8]),
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: None,
        });
        store.put_object_meta(&req).unwrap();
    }

    let resp = store
        .list_objects(&ListObjectsReq {
            bucket: "prefix-bucket".into(),
            prefix: Some("photos/".into()),
            start_after: None,
            max_keys: 100,
        })
        .unwrap();

    assert_eq!(resp.objects.len(), 3);
    assert!(resp.objects.iter().all(|o| o.key().starts_with("photos/")));
}

fn metadata_list_pagination(store: &dyn PgMetadataStore) {
    for i in 0..10 {
        let req = PutObjectReq::Live(PutLiveObjectReq {
            bucket: "page-bucket".into(),
            key: format!("item-{i:02}").into(),
            version_id: VersionId::Null,
            generation_id: GenerationId::MIN,
            size: 0,
            etag: ObjectEtag::SinglePart([0; 8]),
            ec: EcShape { k: 4, m: 2 },
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: None,
        });
        store.put_object_meta(&req).unwrap();
    }

    // First page: 3 items.
    let resp1 = store
        .list_objects(&ListObjectsReq {
            bucket: "page-bucket".into(),
            prefix: None,
            start_after: None,
            max_keys: 3,
        })
        .unwrap();

    assert_eq!(resp1.objects.len(), 3);
    assert!(resp1.is_truncated);
    assert_eq!(resp1.objects[0].key(), "item-00");
    assert_eq!(resp1.objects[2].key(), "item-02");
    assert_eq!(resp1.next_start_after, Some("item-02".into()));

    // Second page.
    let resp2 = store
        .list_objects(&ListObjectsReq {
            bucket: "page-bucket".into(),
            prefix: None,
            start_after: resp1.next_start_after,
            max_keys: 3,
        })
        .unwrap();

    assert_eq!(resp2.objects.len(), 3);
    assert!(resp2.is_truncated);
    assert_eq!(resp2.objects[0].key(), "item-03");

    // Continue until not truncated.
    let mut all_keys = Vec::new();
    all_keys.extend(resp1.objects.iter().map(|o| o.key().clone()));
    all_keys.extend(resp2.objects.iter().map(|o| o.key().clone()));

    let mut start_after = resp2.next_start_after;
    loop {
        let resp = store
            .list_objects(&ListObjectsReq {
                bucket: "page-bucket".into(),
                prefix: None,
                start_after: start_after.clone(),
                max_keys: 3,
            })
            .unwrap();

        all_keys.extend(resp.objects.iter().map(|o| o.key().clone()));
        if !resp.is_truncated {
            break;
        }
        start_after = resp.next_start_after;
    }

    assert_eq!(all_keys.len(), 10);
    for (i, key) in all_keys.iter().enumerate() {
        assert_eq!(*key, format!("item-{i:02}"));
    }
}

fn metadata_list_empty_bucket(store: &dyn PgMetadataStore) {
    let resp = store
        .list_objects(&ListObjectsReq {
            bucket: "empty-bucket".into(),
            prefix: None,
            start_after: None,
            max_keys: 100,
        })
        .unwrap();

    assert!(resp.objects.is_empty());
    assert!(!resp.is_truncated);
    assert!(resp.next_start_after.is_none());
}

fn metadata_empty_key(store: &dyn PgMetadataStore) {
    // S3 allows empty keys (though unusual).
    let req = PutObjectReq::Live(PutLiveObjectReq {
        bucket: "b".into(),
        key: "".into(),
        version_id: VersionId::Null,
        generation_id: GenerationId::MIN,
        ec: EcShape { k: 4, m: 2 },
        size: 0,
        etag: ObjectEtag::SinglePart([0; 8]),
        layout: ObjectLayout::Standard,
        tags: None,
        metadata_blob: None,
    });
    store.put_object_meta(&req).unwrap();

    let obj = store.get_object_meta("b", "").unwrap();
    assert_eq!(*obj.key(), "");
}

fn metadata_long_key(store: &dyn PgMetadataStore) {
    // S3 allows keys up to 1024 bytes.
    let long_key = "x".repeat(1024);
    let req = PutObjectReq::Live(PutLiveObjectReq {
        bucket: "b".into(),
        key: long_key.clone().into(),
        version_id: VersionId::Null,
        generation_id: GenerationId::MIN,
        ec: EcShape { k: 4, m: 2 },
        size: 0,
        etag: ObjectEtag::SinglePart([0; 8]),
        layout: ObjectLayout::Standard,
        tags: None,
        metadata_blob: None,
    });
    store.put_object_meta(&req).unwrap();

    let obj = store.get_object_meta("b", &long_key).unwrap();
    assert_eq!(*obj.key(), long_key);
}

fn metadata_zero_size_object(store: &dyn PgMetadataStore) {
    let req = PutObjectReq::Live(PutLiveObjectReq {
        bucket: "b".into(),
        key: "empty-obj".into(),
        version_id: VersionId::Null,
        generation_id: GenerationId::MIN,
        ec: EcShape { k: 4, m: 2 },
        size: 0,
        etag: ObjectEtag::SinglePart([0; 8]),
        layout: ObjectLayout::Standard,
        tags: None,
        metadata_blob: None,
    });
    store.put_object_meta(&req).unwrap();

    let obj = store.get_object_meta("b", "empty-obj").unwrap();
    assert_eq!(obj.as_live().unwrap().size, 0);
}

// --- PgStore (filesystem) tests ---

fn make_pg_store() -> (test_util::TempDir, crate::PgStore) {
    let dir = test_util::tempdir();
    let pg_dir = dir.path().join("pg-0000");
    let store = crate::PgStore::open(&pg_dir, 0).unwrap();
    (dir, store)
}

#[test]
fn file_metadata_put_get_delete() {
    let (_dir, store) = make_pg_store();
    metadata_put_get_delete(&store);
}

#[test]
fn file_metadata_put_overwrites() {
    let (_dir, store) = make_pg_store();
    metadata_put_overwrites(&store);
}

#[test]
fn file_metadata_get_nonexistent() {
    let (_dir, store) = make_pg_store();
    metadata_get_nonexistent(&store);
}

#[test]
fn file_metadata_list_basic() {
    let (_dir, store) = make_pg_store();
    metadata_list_basic(&store);
}

#[test]
fn file_metadata_list_with_prefix() {
    let (_dir, store) = make_pg_store();
    metadata_list_with_prefix(&store);
}

#[test]
fn file_metadata_list_pagination() {
    let (_dir, store) = make_pg_store();
    metadata_list_pagination(&store);
}

#[test]
fn file_metadata_list_empty_bucket() {
    let (_dir, store) = make_pg_store();
    metadata_list_empty_bucket(&store);
}

#[test]
fn file_metadata_empty_key() {
    let (_dir, store) = make_pg_store();
    metadata_empty_key(&store);
}

#[test]
fn file_metadata_long_key() {
    let (_dir, store) = make_pg_store();
    metadata_long_key(&store);
}

#[test]
fn file_metadata_zero_size() {
    let (_dir, store) = make_pg_store();
    metadata_zero_size_object(&store);
}

#[test]
fn file_bucket_metadata_create_head_list_delete() {
    let (_dir, store) = make_pg_store();

    store
        .create_bucket(
            "alpha",
            "owner-1",
            &CanonicalUserId::from_principal("owner-1"),
            false,
            false,
        )
        .unwrap();
    store
        .create_bucket(
            "beta",
            "owner-1",
            &CanonicalUserId::from_principal("owner-1"),
            true,
            false,
        )
        .unwrap();
    store
        .create_bucket(
            "gamma",
            "owner-2",
            &CanonicalUserId::from_principal("owner-2"),
            false,
            false,
        )
        .unwrap();

    let err = store
        .create_bucket(
            "alpha",
            "owner-1",
            &CanonicalUserId::from_principal("owner-1"),
            false,
            false,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::BucketAlreadyExists
    ));

    let beta = store.head_bucket("beta").unwrap();
    assert_eq!(beta.name, "beta");
    assert_eq!(beta.owner_principal, "owner-1");
    assert_eq!(
        beta.owner_canonical_id,
        CanonicalUserId::from_principal("owner-1")
    );
    assert!(beta.public_read);

    let owner1 = store.list_buckets("owner-1").unwrap();
    assert_eq!(owner1.len(), 2);
    assert_eq!(owner1[0].name, "alpha");
    assert_eq!(owner1[1].name, "beta");

    store.delete_bucket("alpha").unwrap();
    let err = store.head_bucket("alpha").unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::BucketNotFound { .. }
    ));
}

#[test]
fn file_bucket_metadata_delete_nonexistent() {
    let (_dir, store) = make_pg_store();
    let err = store.delete_bucket("no-such-bucket").unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::BucketNotFound { .. }
    ));
}

#[test]
fn file_bucket_metadata_versioning_transitions() {
    let (_dir, store) = make_pg_store();
    store
        .create_bucket(
            "bucket",
            "owner",
            &CanonicalUserId::from_principal("owner"),
            false,
            false,
        )
        .unwrap();

    store
        .put_bucket_versioning("bucket", BucketVersioningState::Enabled)
        .unwrap();
    store
        .put_bucket_versioning("bucket", BucketVersioningState::Suspended)
        .unwrap();
    store
        .put_bucket_versioning("bucket", BucketVersioningState::Enabled)
        .unwrap();

    let err = store
        .put_bucket_versioning("bucket", BucketVersioningState::Disabled)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::InvalidVersioningTransition { .. }
    ));
}

#[test]
fn file_bucket_metadata_versioning_disabled_noop() {
    let (_dir, store) = make_pg_store();
    store
        .create_bucket(
            "bucket",
            "owner",
            &CanonicalUserId::from_principal("owner"),
            false,
            false,
        )
        .unwrap();
    store
        .put_bucket_versioning("bucket", BucketVersioningState::Disabled)
        .unwrap();
    assert_eq!(
        store.head_bucket("bucket").unwrap().versioning,
        BucketVersioningState::Disabled
    );
}

#[test]
fn file_bucket_metadata_config_roundtrip() {
    let (_dir, store) = make_pg_store();
    store
        .create_bucket(
            "bucket",
            "owner",
            &CanonicalUserId::from_principal("owner"),
            false,
            false,
        )
        .unwrap();

    store.put_bucket_cors("bucket", "<Cors/>").unwrap();
    assert_eq!(
        store.get_bucket_cors("bucket").unwrap(),
        Some("<Cors/>".to_string())
    );
    store.delete_bucket_cors("bucket").unwrap();
    assert_eq!(store.get_bucket_cors("bucket").unwrap(), None);

    store.put_bucket_tags("bucket", "<Tagging/>").unwrap();
    assert_eq!(
        store.get_bucket_tags("bucket").unwrap(),
        Some("<Tagging/>".to_string())
    );
    store.delete_bucket_tags("bucket").unwrap();
    assert_eq!(store.get_bucket_tags("bucket").unwrap(), None);

    store
        .put_bucket_public_access_block("bucket", "<PublicAccessBlock/>")
        .unwrap();
    assert_eq!(
        store.get_bucket_public_access_block("bucket").unwrap(),
        Some("<PublicAccessBlock/>".to_string())
    );
    store.delete_bucket_public_access_block("bucket").unwrap();
    assert_eq!(
        store.get_bucket_public_access_block("bucket").unwrap(),
        None
    );

    store
        .put_bucket_ownership_controls("bucket", "<OwnershipControls/>")
        .unwrap();
    assert_eq!(
        store.get_bucket_ownership_controls("bucket").unwrap(),
        Some("<OwnershipControls/>".to_string())
    );
    store.delete_bucket_ownership_controls("bucket").unwrap();
    assert_eq!(store.get_bucket_ownership_controls("bucket").unwrap(), None);

    store.put_bucket_acl("bucket", true, false).unwrap();
    assert!(store.head_bucket("bucket").unwrap().public_read);
    store.put_bucket_acl("bucket", false, false).unwrap();
    assert!(!store.head_bucket("bucket").unwrap().public_read);
}

#[test]
fn file_bucket_metadata_config_on_nonexistent_bucket() {
    let (_dir, store) = make_pg_store();

    let err = store.put_bucket_cors("nope", "<Cors/>").unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::BucketNotFound { .. }
    ));

    let err = store.get_bucket_tags("nope").unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::BucketNotFound { .. }
    ));

    let err = store.delete_bucket_public_access_block("nope").unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::BucketNotFound { .. }
    ));
}

// --- DataLayout decode tests ---

#[test]
fn file_metadata_object_has_inline_legacy_layout() {
    let (_dir, store) = make_pg_store();
    store
        .put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
            bucket: "b".into(),
            key: "k".into(),
            version_id: VersionId::Null,
            generation_id: GenerationId::MIN,
            ec: EcShape { k: 4, m: 2 },
            size: 100,
            etag: ObjectEtag::SinglePart([1, 2, 3, 0, 0, 0, 0, 0]),
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: None,
        }))
        .unwrap();

    let obj = store.get_object_meta("b", "k").unwrap();
    let live = obj.as_live().unwrap();
    assert_eq!(live.layout, ObjectLayout::Standard);
    assert_eq!(live.metadata_blob, None);
}

#[test]
fn file_metadata_invalid_data_layout_returns_error() {
    let (_dir, store) = make_pg_store();

    // Insert a valid object first
    store
        .put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
            bucket: "b".into(),
            key: "k".into(),
            version_id: VersionId::Null,
            generation_id: GenerationId::MIN,
            ec: EcShape { k: 4, m: 2 },
            size: 100,
            etag: ObjectEtag::SinglePart([1, 2, 3, 0, 0, 0, 0, 0]),
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: None,
        }))
        .unwrap();

    // Corrupting data_layout is blocked by DB CHECK constraints.
    let err = store
        .connection()
        .execute(
            "UPDATE objects SET data_layout = 99 WHERE bucket = 'b' AND key = 'k'",
            [],
        )
        .unwrap_err();
    assert!(
        matches!(err, rusqlite::Error::SqliteFailure(_, _)),
        "expected sqlite constraint failure, got: {err:?}"
    );

    // Record remains readable and unchanged.
    let obj = store.get_object_meta("b", "k").unwrap();
    assert_eq!(obj.as_live().unwrap().layout, ObjectLayout::Standard);
}

// --- Multipart metadata tests (PgStore only — needs SQL) ---

#[test]
fn mpu_create_and_get_upload() {
    let (_dir, store) = make_pg_store();
    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: "uid-1".into(),
            bucket: "b".into(),
            key: "k".into(),
            metadata_blob: vec![1, 2, 3],
            owner_principal: Some("alice".to_string()),
            checksum: None,
        })
        .unwrap();

    let rec = store.get_multipart_upload("uid-1").unwrap();
    assert_eq!(rec.upload_id, "uid-1");
    assert_eq!(rec.bucket, "b");
    assert_eq!(rec.key, "k");
    assert_eq!(rec.state, UploadState::InProgress);
    assert_eq!(rec.metadata_blob, vec![1, 2, 3]);
    assert_eq!(rec.owner_principal, Some("alice".to_string()));
    assert!(rec.initiated_at > 0);
}

#[test]
fn mpu_create_upload_with_checksum_fields() {
    let (_dir, store) = make_pg_store();
    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: "uid-cksum".into(),
            bucket: "b".into(),
            key: "k".into(),
            metadata_blob: vec![],
            owner_principal: None,
            checksum: Some(
                MultipartChecksumConfig::new(
                    ChecksumAlgorithm::Sha256,
                    Some(ChecksumType::Composite),
                )
                .unwrap(),
            ),
        })
        .unwrap();

    let rec = store.get_multipart_upload("uid-cksum").unwrap();
    assert_eq!(
        rec.checksum.map(|c| c.algorithm()),
        Some(ChecksumAlgorithm::Sha256)
    );
    assert_eq!(
        rec.checksum.map(|c| c.checksum_type()),
        Some(ChecksumType::Composite)
    );

    // None case round-trips as well
    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: "uid-no-cksum".into(),
            bucket: "b".into(),
            key: "k2".into(),
            metadata_blob: vec![],
            owner_principal: None,
            checksum: None,
        })
        .unwrap();
    let rec2 = store.get_multipart_upload("uid-no-cksum").unwrap();
    assert_eq!(rec2.checksum, None);
}

#[test]
fn mpu_part_checksum_round_trip() {
    let (_dir, store) = make_pg_store();
    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: "uid-pc".into(),
            bucket: "b".into(),
            key: "k".into(),
            metadata_blob: vec![],
            owner_principal: None,
            checksum: Some(
                MultipartChecksumConfig::new(
                    ChecksumAlgorithm::Crc32,
                    Some(ChecksumType::FullObject),
                )
                .unwrap(),
            ),
        })
        .unwrap();

    let checksum_bytes = vec![0xDE, 0xAD, 0xBE, 0xEF];
    store
        .upsert_multipart_part(&MultipartPartRecord {
            upload_id: "uid-pc".into(),
            part_number: 1,
            generation: 0,
            size: 1024,
            etag: vec![0xAA],
            etag_kind: EtagKind::Crc64,
            part_okh: [1u8; 16],
            part_vid: GenerationId::MIN,
            ec_k: 4,
            ec_m: 2,
            last_modified: 100,
            checksum: Some(checksum_bytes.clone()),
        })
        .unwrap();

    let part = store.get_multipart_part("uid-pc", 1).unwrap();
    assert_eq!(part.checksum, Some(checksum_bytes));

    // None checksum round-trips
    store
        .upsert_multipart_part(&MultipartPartRecord {
            upload_id: "uid-pc".into(),
            part_number: 2,
            generation: 0,
            size: 512,
            etag: vec![0xBB],
            etag_kind: EtagKind::Crc64,
            part_okh: [2u8; 16],
            part_vid: GenerationId::new(1).unwrap(),
            ec_k: 4,
            ec_m: 2,
            last_modified: 200,
            checksum: None,
        })
        .unwrap();

    let part2 = store.get_multipart_part("uid-pc", 2).unwrap();
    assert_eq!(part2.checksum, None);
}

#[test]
fn mpu_object_part_checksum_round_trip() {
    let (_dir, store) = make_pg_store();

    let checksum_bytes = vec![0x01, 0x02, 0x03, 0x04];
    store
        .commit_object_parts(&[
            ObjectPartRecord {
                bucket: "b".into(),
                key: "k".into(),
                version_id: VersionId::from_u64(1),
                part_number: 1,
                size: 5 * 1024 * 1024,
                etag: vec![0xAA],
                etag_kind: EtagKind::Crc64,
                part_okh: [1u8; 16],
                part_vid: GenerationId::MIN,
                ec_k: 4,
                ec_m: 2,
                shard_pg_id: 0,
                checksum: Some(checksum_bytes.clone()),
            },
            ObjectPartRecord {
                bucket: "b".into(),
                key: "k".into(),
                version_id: VersionId::from_u64(1),
                part_number: 2,
                size: 1024,
                etag: vec![0xBB],
                etag_kind: EtagKind::Crc64,
                part_okh: [2u8; 16],
                part_vid: GenerationId::new(1).unwrap(),
                ec_k: 4,
                ec_m: 2,
                shard_pg_id: 0,
                checksum: None,
            },
        ])
        .unwrap();

    let committed = store
        .get_object_parts("b", "k", VersionId::from_u64(1))
        .unwrap();
    assert_eq!(committed.len(), 2);
    assert_eq!(committed[0].checksum, Some(checksum_bytes));
    assert_eq!(committed[1].checksum, None);
}

#[test]
fn mpu_complete_multipart_commit_preserves_checksums() {
    let (_dir, store) = make_pg_store();

    // Create upload and object row (needed for complete_multipart_commit).
    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: "uid-cmc".into(),
            bucket: "b".into(),
            key: "k".into(),
            metadata_blob: vec![],
            owner_principal: None,
            checksum: Some(
                MultipartChecksumConfig::new(
                    ChecksumAlgorithm::Sha256,
                    Some(ChecksumType::Composite),
                )
                .unwrap(),
            ),
        })
        .unwrap();

    let obj = CommitMultipartReq {
        bucket: "b".into(),
        key: "k".into(),
        version_id: VersionId::Null,
        generation_id: GenerationId::MIN,
        size: 6 * 1024 * 1024,
        etag_crc64: [0xCC, 0, 0, 0, 0, 0, 0, 0],
        ec: EcShape { k: 4, m: 2 },
        metadata_blob: Some(vec![]),
    };

    let cksum = vec![0xDE, 0xAD];
    let parts = vec![
        ObjectPartRecord {
            bucket: "b".into(),
            key: "k".into(),
            version_id: VersionId::Null,
            part_number: 1,
            size: 5 * 1024 * 1024,
            etag: vec![0xAA],
            etag_kind: EtagKind::Crc64,
            part_okh: [1u8; 16],
            part_vid: GenerationId::MIN,
            ec_k: 4,
            ec_m: 2,
            shard_pg_id: 0,
            checksum: Some(cksum.clone()),
        },
        ObjectPartRecord {
            bucket: "b".into(),
            key: "k".into(),
            version_id: VersionId::Null,
            part_number: 2,
            size: 1024 * 1024,
            etag: vec![0xBB],
            etag_kind: EtagKind::Crc64,
            part_okh: [2u8; 16],
            part_vid: GenerationId::new(1).unwrap(),
            ec_k: 4,
            ec_m: 2,
            shard_pg_id: 0,
            checksum: None,
        },
    ];

    store
        .complete_multipart_commit("uid-cmc", &obj, &parts)
        .unwrap();

    let committed = store.get_object_parts("b", "k", VersionId::Null).unwrap();
    assert_eq!(committed.len(), 2);
    assert_eq!(committed[0].checksum, Some(cksum));
    assert_eq!(committed[1].checksum, None);
}

#[test]
fn mpu_get_missing_upload_returns_no_such_upload() {
    let (_dir, store) = make_pg_store();
    let err = store.get_multipart_upload("nonexistent").unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::NoSuchUpload { .. }
    ));
}

#[test]
fn mpu_set_upload_state_transition() {
    let (_dir, store) = make_pg_store();
    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: "uid-2".into(),
            bucket: "b".into(),
            key: "k".into(),
            metadata_blob: vec![],
            owner_principal: None,
            checksum: None,
        })
        .unwrap();

    // InProgress -> Completing succeeds
    store
        .set_upload_state("uid-2", UploadState::Completing)
        .unwrap();
    let rec = store.get_multipart_upload("uid-2").unwrap();
    assert_eq!(rec.state, UploadState::Completing);

    // Completing -> Aborting fails (not InProgress)
    let err = store
        .set_upload_state("uid-2", UploadState::Aborting)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::UploadNotInProgress { state: 1 }
    ));
}

#[test]
fn mpu_set_upload_state_missing_returns_no_such_upload() {
    let (_dir, store) = make_pg_store();
    let err = store
        .set_upload_state("nonexistent", UploadState::Completing)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::NoSuchUpload { .. }
    ));
}

#[test]
fn mpu_delete_upload_cascades_parts() {
    let (_dir, store) = make_pg_store();
    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: "uid-3".into(),
            bucket: "b".into(),
            key: "k".into(),
            metadata_blob: vec![],
            owner_principal: None,
            checksum: None,
        })
        .unwrap();

    // Add a part
    store
        .upsert_multipart_part(&MultipartPartRecord {
            upload_id: "uid-3".into(),
            part_number: 1,
            generation: 0,
            size: 1024,
            etag: vec![0xAA],
            etag_kind: EtagKind::Crc64,
            part_okh: [1u8; 16],
            part_vid: GenerationId::MIN,
            ec_k: 4,
            ec_m: 2,
            last_modified: 0,
            checksum: None,
        })
        .unwrap();

    // Delete upload — should cascade to parts
    store.delete_multipart_upload("uid-3").unwrap();

    assert!(matches!(
        store.get_multipart_upload("uid-3").unwrap_err(),
        crate::error::MetadataError::NoSuchUpload { .. }
    ));
}

#[test]
fn mpu_delete_missing_upload_returns_no_such_upload() {
    let (_dir, store) = make_pg_store();
    let err = store.delete_multipart_upload("nonexistent").unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::NoSuchUpload { .. }
    ));
}

#[test]
fn mpu_upsert_part_and_get() {
    let (_dir, store) = make_pg_store();
    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: "uid-4".into(),
            bucket: "b".into(),
            key: "k".into(),
            metadata_blob: vec![],
            owner_principal: None,
            checksum: None,
        })
        .unwrap();

    // First upload: no previous generation
    let prev = store
        .upsert_multipart_part(&MultipartPartRecord {
            upload_id: "uid-4".into(),
            part_number: 1,
            generation: 0,
            size: 5 * 1024 * 1024,
            etag: vec![0xBB],
            etag_kind: EtagKind::Crc64,
            part_okh: [2u8; 16],
            part_vid: GenerationId::MIN,
            ec_k: 4,
            ec_m: 2,
            last_modified: 100,
            checksum: None,
        })
        .unwrap();
    assert_eq!(prev, None);

    // Verify get
    let part = store.get_multipart_part("uid-4", 1).unwrap();
    assert_eq!(part.generation, 0);
    assert_eq!(part.size, 5 * 1024 * 1024);
    assert_eq!(part.etag, vec![0xBB]);
    assert_eq!(part.part_okh, [2u8; 16]);

    // Re-upload same part: returns previous generation
    let prev = store
        .upsert_multipart_part(&MultipartPartRecord {
            upload_id: "uid-4".into(),
            part_number: 1,
            generation: 1,
            size: 6 * 1024 * 1024,
            etag: vec![0xCC],
            etag_kind: EtagKind::Crc64,
            part_okh: [3u8; 16],
            part_vid: GenerationId::new(1).unwrap(),
            ec_k: 4,
            ec_m: 2,
            last_modified: 200,
            checksum: None,
        })
        .unwrap();
    assert_eq!(prev, Some(0));

    // Verify updated
    let part = store.get_multipart_part("uid-4", 1).unwrap();
    assert_eq!(part.generation, 1);
    assert_eq!(part.size, 6 * 1024 * 1024);
}

#[test]
fn mpu_list_parts_pagination() {
    let (_dir, store) = make_pg_store();
    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: "uid-5".into(),
            bucket: "b".into(),
            key: "k".into(),
            metadata_blob: vec![],
            owner_principal: None,
            checksum: None,
        })
        .unwrap();

    // Insert 5 parts
    for i in 1..=5 {
        store
            .upsert_multipart_part(&MultipartPartRecord {
                upload_id: "uid-5".into(),
                part_number: i,
                generation: 0,
                size: 1024 * i as u64,
                etag: vec![i as u8],
                etag_kind: EtagKind::Crc64,
                part_okh: [i as u8; 16],
                part_vid: GenerationId::MIN,
                ec_k: 4,
                ec_m: 2,
                last_modified: 0,
                checksum: None,
            })
            .unwrap();
    }

    // List first page (max 2)
    let resp = store
        .list_multipart_parts(&ListPartsReq {
            upload_id: "uid-5".into(),
            part_number_marker: None,
            max_parts: 2,
        })
        .unwrap();
    assert!(resp.is_truncated);
    assert_eq!(resp.parts.len(), 2);
    assert_eq!(resp.parts[0].part_number, 1);
    assert_eq!(resp.parts[1].part_number, 2);
    assert_eq!(resp.next_part_number_marker, Some(2));

    // List second page
    let resp = store
        .list_multipart_parts(&ListPartsReq {
            upload_id: "uid-5".into(),
            part_number_marker: Some(2),
            max_parts: 2,
        })
        .unwrap();
    assert!(resp.is_truncated);
    assert_eq!(resp.parts.len(), 2);
    assert_eq!(resp.parts[0].part_number, 3);
    assert_eq!(resp.parts[1].part_number, 4);

    // List last page
    let resp = store
        .list_multipart_parts(&ListPartsReq {
            upload_id: "uid-5".into(),
            part_number_marker: Some(4),
            max_parts: 2,
        })
        .unwrap();
    assert!(!resp.is_truncated);
    assert_eq!(resp.parts.len(), 1);
    assert_eq!(resp.parts[0].part_number, 5);
}

#[test]
fn mpu_list_uploads_pagination() {
    let (_dir, store) = make_pg_store();

    // Create 3 uploads for different keys
    for (uid, key) in [("u1", "a"), ("u2", "b"), ("u3", "c")] {
        store
            .create_multipart_upload(&CreateMultipartUploadReq {
                upload_id: uid.into(),
                bucket: "bkt".into(),
                key: key.into(),
                metadata_blob: vec![],
                owner_principal: None,
                checksum: None,
            })
            .unwrap();
    }

    // List first page (max 2)
    let resp = store
        .list_multipart_uploads(&ListMultipartUploadsReq {
            bucket: "bkt".into(),
            prefix: None,
            key_marker: None,
            upload_id_marker: None,
            max_uploads: 2,
        })
        .unwrap();
    assert!(resp.is_truncated);
    assert_eq!(resp.uploads.len(), 2);
    assert_eq!(resp.uploads[0].key, "a");
    assert_eq!(resp.uploads[1].key, "b");

    // List second page using markers
    let resp = store
        .list_multipart_uploads(&ListMultipartUploadsReq {
            bucket: "bkt".into(),
            prefix: None,
            key_marker: resp.next_key_marker,
            upload_id_marker: resp.next_upload_id_marker,
            max_uploads: 2,
        })
        .unwrap();
    assert!(!resp.is_truncated);
    assert_eq!(resp.uploads.len(), 1);
    assert_eq!(resp.uploads[0].key, "c");
}

#[test]
fn mpu_list_uploads_with_prefix() {
    let (_dir, store) = make_pg_store();

    for (uid, key) in [
        ("u1", "photos/a.jpg"),
        ("u2", "photos/b.jpg"),
        ("u3", "docs/c.txt"),
    ] {
        store
            .create_multipart_upload(&CreateMultipartUploadReq {
                upload_id: uid.into(),
                bucket: "bkt".into(),
                key: key.into(),
                metadata_blob: vec![],
                owner_principal: None,
                checksum: None,
            })
            .unwrap();
    }

    let resp = store
        .list_multipart_uploads(&ListMultipartUploadsReq {
            bucket: "bkt".into(),
            prefix: Some("photos/".into()),
            key_marker: None,
            upload_id_marker: None,
            max_uploads: 100,
        })
        .unwrap();
    assert_eq!(resp.uploads.len(), 2);
    assert!(resp.uploads.iter().all(|u| u.key.starts_with("photos/")));
}

#[test]
fn mpu_list_uploads_same_key_multiple_upload_ids() {
    let (_dir, store) = make_pg_store();

    // Three uploads for the same key
    for uid in ["u-a", "u-b", "u-c"] {
        store
            .create_multipart_upload(&CreateMultipartUploadReq {
                upload_id: uid.into(),
                bucket: "bkt".into(),
                key: "same-key".into(),
                metadata_blob: vec![],
                owner_principal: None,
                checksum: None,
            })
            .unwrap();
    }

    // Page 1: max_uploads=2
    let resp = store
        .list_multipart_uploads(&ListMultipartUploadsReq {
            bucket: "bkt".into(),
            prefix: None,
            key_marker: None,
            upload_id_marker: None,
            max_uploads: 2,
        })
        .unwrap();
    assert!(resp.is_truncated);
    assert_eq!(resp.uploads.len(), 2);
    // All same key, ordered by upload_id
    assert_eq!(resp.uploads[0].upload_id, "u-a");
    assert_eq!(resp.uploads[1].upload_id, "u-b");

    // Page 2: resume with markers
    let resp2 = store
        .list_multipart_uploads(&ListMultipartUploadsReq {
            bucket: "bkt".into(),
            prefix: None,
            key_marker: resp.next_key_marker,
            upload_id_marker: resp.next_upload_id_marker,
            max_uploads: 2,
        })
        .unwrap();
    assert!(!resp2.is_truncated);
    assert_eq!(resp2.uploads.len(), 1);
    assert_eq!(resp2.uploads[0].upload_id, "u-c");
}

#[test]
fn mpu_list_uploads_stale_marker_returns_remaining() {
    let (_dir, store) = make_pg_store();

    // Create 3 uploads for the same key.
    for uid in ["u-x", "u-y", "u-z"] {
        store
            .create_multipart_upload(&CreateMultipartUploadReq {
                upload_id: uid.into(),
                bucket: "bkt".into(),
                key: "key".into(),
                metadata_blob: vec![],
                owner_principal: None,
                checksum: None,
            })
            .unwrap();
    }

    // Delete the middle upload (simulating it being aborted between pages).
    store.delete_multipart_upload("u-y").unwrap();

    // Paginate using u-y as the marker — it no longer exists.
    // COALESCE to 0 means all remaining uploads for "key" are returned.
    let resp = store
        .list_multipart_uploads(&ListMultipartUploadsReq {
            bucket: "bkt".into(),
            prefix: None,
            key_marker: Some("key".into()),
            upload_id_marker: Some("u-y".into()),
            max_uploads: 10,
        })
        .unwrap();

    // u-x and u-z should both appear (safe re-return of u-x, plus u-z).
    // The stale marker must not cause u-z to be silently dropped.
    let ids: Vec<&str> = resp.uploads.iter().map(|u| u.upload_id.as_str()).collect();
    assert!(
        ids.contains(&"u-z"),
        "u-z must not be dropped; got: {ids:?}"
    );
}

#[test]
fn mpu_corrupted_part_okh_returns_error() {
    let (_dir, store) = make_pg_store();
    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: "uid-okh".into(),
            bucket: "b".into(),
            key: "k".into(),
            metadata_blob: vec![],
            owner_principal: None,
            checksum: None,
        })
        .unwrap();

    // Insert a part with valid okh
    store
        .upsert_multipart_part(&MultipartPartRecord {
            upload_id: "uid-okh".into(),
            part_number: 1,
            generation: 0,
            size: 1024,
            etag: vec![0xAA],
            etag_kind: EtagKind::Crc64,
            part_okh: [1u8; 16],
            part_vid: GenerationId::MIN,
            ec_k: 4,
            ec_m: 2,
            last_modified: 0,
            checksum: None,
        })
        .unwrap();

    // Corrupt the part_okh via raw SQL (wrong length blob)
    store
        .connection()
        .execute(
            "UPDATE multipart_parts SET part_okh = X'AABB' \
             WHERE upload_id = 'uid-okh' AND part_number = 1",
            [],
        )
        .unwrap();

    // Reading should fail, not silently zero the okh
    let err = store.get_multipart_part("uid-okh", 1).unwrap_err();
    assert!(
        matches!(err, crate::error::MetadataError::Db { .. }),
        "expected Db error for corrupted part_okh, got: {err:?}"
    );
}

#[test]
fn mpu_corrupted_object_part_okh_returns_error() {
    let (_dir, store) = make_pg_store();

    store
        .commit_object_parts(&[ObjectPartRecord {
            bucket: "b".into(),
            key: "k".into(),
            version_id: VersionId::from_u64(1),
            part_number: 1,
            size: 1024,
            etag: vec![0xAA],
            etag_kind: EtagKind::Crc64,
            part_okh: [1u8; 16],
            part_vid: GenerationId::MIN,
            ec_k: 4,
            ec_m: 2,
            shard_pg_id: 0,
            checksum: None,
        }])
        .unwrap();

    // Corrupt the part_okh
    store
        .connection()
        .execute(
            "UPDATE object_parts SET part_okh = X'AABB' \
             WHERE bucket = 'b' AND key = 'k' AND version_id = 1",
            [],
        )
        .unwrap();

    let err = store
        .get_object_parts("b", "k", VersionId::from_u64(1))
        .unwrap_err();
    assert!(
        matches!(err, crate::error::MetadataError::Db { .. }),
        "expected Db error for corrupted object part_okh, got: {err:?}"
    );
}

#[test]
fn mpu_get_missing_part_returns_part_not_found() {
    let (_dir, store) = make_pg_store();
    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: "uid-pnf".into(),
            bucket: "b".into(),
            key: "k".into(),
            metadata_blob: vec![],
            owner_principal: None,
            checksum: None,
        })
        .unwrap();

    let err = store.get_multipart_part("uid-pnf", 42).unwrap_err();
    assert!(
        matches!(
            err,
            crate::error::MetadataError::PartNotFound {
                ref upload_id,
                part_number: 42
            } if upload_id == "uid-pnf"
        ),
        "expected PartNotFound, got: {err:?}"
    );
}

#[test]
fn mpu_set_upload_state_rejects_in_progress_target() {
    let (_dir, store) = make_pg_store();
    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: "uid-ip".into(),
            bucket: "b".into(),
            key: "k".into(),
            metadata_blob: vec![],
            owner_principal: None,
            checksum: None,
        })
        .unwrap();

    // InProgress -> InProgress should be rejected, reporting actual state
    let err = store
        .set_upload_state("uid-ip", UploadState::InProgress)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::error::MetadataError::UploadNotInProgress { state: 0 }
        ),
        "expected UploadNotInProgress {{ state: 0 }}, got: {err:?}"
    );

    // Transition to Completing, then try InProgress again — should report state 1
    store
        .set_upload_state("uid-ip", UploadState::Completing)
        .unwrap();
    let err = store
        .set_upload_state("uid-ip", UploadState::InProgress)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::error::MetadataError::UploadNotInProgress { state: 1 }
        ),
        "expected UploadNotInProgress {{ state: 1 }}, got: {err:?}"
    );

    // Upload should still be Completing (InProgress target was rejected)
    let rec = store.get_multipart_upload("uid-ip").unwrap();
    assert_eq!(rec.state, UploadState::Completing);
}

#[test]
fn mpu_set_upload_state_in_progress_target_nonexistent_returns_no_such_upload() {
    let (_dir, store) = make_pg_store();
    let err = store
        .set_upload_state("nonexistent", UploadState::InProgress)
        .unwrap_err();
    assert!(
        matches!(err, crate::error::MetadataError::NoSuchUpload { .. }),
        "expected NoSuchUpload for nonexistent upload with InProgress target, got: {err:?}"
    );
}

#[test]
fn mpu_upsert_part_nonexistent_upload_returns_no_such_upload() {
    let (_dir, store) = make_pg_store();
    let err = store
        .upsert_multipart_part(&MultipartPartRecord {
            upload_id: "nonexistent".into(),
            part_number: 1,
            generation: 0,
            size: 1024,
            etag: vec![0xAA],
            etag_kind: EtagKind::Crc64,
            part_okh: [1u8; 16],
            part_vid: GenerationId::MIN,
            ec_k: 4,
            ec_m: 2,
            last_modified: 0,
            checksum: None,
        })
        .unwrap_err();
    assert!(
        matches!(err, crate::error::MetadataError::NoSuchUpload { .. }),
        "expected NoSuchUpload for FK violation, got: {err:?}"
    );
}

#[test]
fn mpu_list_parts_nonexistent_upload_returns_no_such_upload() {
    let (_dir, store) = make_pg_store();
    let err = store
        .list_multipart_parts(&ListPartsReq {
            upload_id: "nonexistent".into(),
            part_number_marker: None,
            max_parts: 10,
        })
        .unwrap_err();
    assert!(
        matches!(err, crate::error::MetadataError::NoSuchUpload { .. }),
        "expected NoSuchUpload for nonexistent upload, got: {err:?}"
    );
}

#[test]
fn mpu_commit_object_parts_rollback_on_duplicate() {
    let (_dir, store) = make_pg_store();

    let part = ObjectPartRecord {
        bucket: "b".into(),
        key: "k".into(),
        version_id: VersionId::from_u64(1),
        part_number: 1,
        size: 1024,
        etag: vec![0xAA],
        etag_kind: EtagKind::Crc64,
        part_okh: [1u8; 16],
        part_vid: GenerationId::MIN,
        ec_k: 4,
        ec_m: 2,
        shard_pg_id: 0,
        checksum: None,
    };

    // First commit succeeds
    store
        .commit_object_parts(std::slice::from_ref(&part))
        .unwrap();

    // Second commit with same PK should fail (duplicate)
    let err = store.commit_object_parts(&[part]).unwrap_err();
    assert!(
        matches!(err, crate::error::MetadataError::Db { .. }),
        "expected Db error for duplicate PK, got: {err:?}"
    );

    // The connection should still be usable (no poisoned transaction)
    let parts = store
        .get_object_parts("b", "k", VersionId::from_u64(1))
        .unwrap();
    assert_eq!(parts.len(), 1, "original commit should still be intact");
}

#[test]
fn mpu_commit_and_get_object_parts() {
    let (_dir, store) = make_pg_store();

    let parts = vec![
        ObjectPartRecord {
            bucket: "b".into(),
            key: "k".into(),
            version_id: VersionId::from_u64(1),
            part_number: 1,
            size: 5 * 1024 * 1024,
            etag: vec![0xAA],
            etag_kind: EtagKind::Crc64,
            part_okh: [1u8; 16],
            part_vid: GenerationId::MIN,
            ec_k: 4,
            ec_m: 2,
            shard_pg_id: 0,
            checksum: None,
        },
        ObjectPartRecord {
            bucket: "b".into(),
            key: "k".into(),
            version_id: VersionId::from_u64(1),
            part_number: 2,
            size: 3 * 1024 * 1024,
            etag: vec![0xBB],
            etag_kind: EtagKind::Crc64,
            part_okh: [2u8; 16],
            part_vid: GenerationId::new(1).unwrap(),
            ec_k: 4,
            ec_m: 2,
            shard_pg_id: 0,
            checksum: None,
        },
    ];

    store.commit_object_parts(&parts).unwrap();

    let committed = store
        .get_object_parts("b", "k", VersionId::from_u64(1))
        .unwrap();
    assert_eq!(committed.len(), 2);
    assert_eq!(committed[0].part_number, 1);
    assert_eq!(committed[0].size, 5 * 1024 * 1024);
    assert_eq!(committed[0].part_okh, [1u8; 16]);
    assert_eq!(committed[1].part_number, 2);
    assert_eq!(committed[1].size, 3 * 1024 * 1024);

    // Get for non-existent version returns empty
    let empty = store
        .get_object_parts("b", "k", VersionId::from_u64(999))
        .unwrap();
    assert!(empty.is_empty());
}

#[test]
fn mpu_delete_object_parts() {
    let (_dir, store) = make_pg_store();

    store
        .commit_object_parts(&[ObjectPartRecord {
            bucket: "b".into(),
            key: "k".into(),
            version_id: VersionId::from_u64(1),
            part_number: 1,
            size: 1024,
            etag: vec![0xAA],
            etag_kind: EtagKind::Crc64,
            part_okh: [1u8; 16],
            part_vid: GenerationId::MIN,
            ec_k: 4,
            ec_m: 2,
            shard_pg_id: 0,
            checksum: None,
        }])
        .unwrap();

    store
        .delete_object_parts("b", "k", VersionId::from_u64(1))
        .unwrap();
    let parts = store
        .get_object_parts("b", "k", VersionId::from_u64(1))
        .unwrap();
    assert!(parts.is_empty());

    // Delete again is idempotent (no error)
    store
        .delete_object_parts("b", "k", VersionId::from_u64(1))
        .unwrap();
}

// --- Step 3b: Transaction rollback and concurrency hardening tests ---

/// Helper: create an upload with the given ID in the given store.
fn create_upload(store: &dyn PgMetadataStore, upload_id: &str) {
    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: upload_id.into(),
            bucket: "b".into(),
            key: "k".into(),
            metadata_blob: vec![],
            owner_principal: None,
            checksum: None,
        })
        .unwrap();
}

/// Helper: build a MultipartPartRecord for a given upload/part/generation.
fn make_part(upload_id: &str, part_number: u32, generation: u32) -> MultipartPartRecord {
    MultipartPartRecord {
        upload_id: upload_id.into(),
        part_number,
        generation,
        size: 1024,
        etag: vec![0xAA],
        etag_kind: EtagKind::Crc64,
        part_okh: [part_number as u8; 16],
        part_vid: GenerationId::new(generation as u64 + 1).unwrap(),
        ec_k: 4,
        ec_m: 2,
        last_modified: 1000,
        checksum: None,
    }
}

#[test]
fn mpu_upsert_fk_rollback_leaves_connection_usable() {
    let (_dir, store) = make_pg_store();

    // Upsert without creating the upload first → FK violation → NoSuchUpload.
    let err = store
        .upsert_multipart_part(&make_part("nonexistent", 1, 0))
        .unwrap_err();
    assert!(
        matches!(err, crate::error::MetadataError::NoSuchUpload { .. }),
        "expected NoSuchUpload, got: {err:?}"
    );

    // Connection still usable: create upload and upsert should succeed.
    create_upload(&store, "uid-ok");
    let prev = store
        .upsert_multipart_part(&make_part("uid-ok", 1, 0))
        .unwrap();
    assert_eq!(prev, None);
}

#[test]
fn mpu_upsert_fk_rollback_preserves_existing_parts() {
    let (_dir, store) = make_pg_store();

    create_upload(&store, "uid-a");
    store
        .upsert_multipart_part(&make_part("uid-a", 1, 0))
        .unwrap();

    // Attempt upsert to a nonexistent upload → FK failure → rollback.
    let err = store
        .upsert_multipart_part(&make_part("nonexistent", 1, 0))
        .unwrap_err();
    assert!(
        matches!(err, crate::error::MetadataError::NoSuchUpload { .. }),
        "expected NoSuchUpload for FK violation, got: {err:?}"
    );

    // Original part should be unaffected.
    let part = store.get_multipart_part("uid-a", 1).unwrap();
    assert_eq!(part.generation, 0);
}

#[test]
fn mpu_commit_partial_batch_failure_rolls_back_all() {
    let (_dir, store) = make_pg_store();

    // Commit part 1 for version 1.
    let part1 = ObjectPartRecord {
        bucket: "b".into(),
        key: "k".into(),
        version_id: VersionId::from_u64(1),
        part_number: 1,
        size: 1024,
        etag: vec![0xAA],
        etag_kind: EtagKind::Crc64,
        part_okh: [1u8; 16],
        part_vid: GenerationId::MIN,
        ec_k: 4,
        ec_m: 2,
        shard_pg_id: 0,
        checksum: None,
    };
    store
        .commit_object_parts(std::slice::from_ref(&part1))
        .unwrap();

    // Now try to commit a batch of 3 parts where part 1 duplicates the existing row.
    // The entire batch should fail atomically.
    let batch = vec![
        ObjectPartRecord {
            part_number: 2,
            ..part1.clone()
        },
        ObjectPartRecord {
            part_number: 1, // duplicate PK
            ..part1.clone()
        },
        ObjectPartRecord {
            part_number: 3,
            ..part1.clone()
        },
    ];
    let err = store.commit_object_parts(&batch).unwrap_err();
    assert!(
        matches!(err, crate::error::MetadataError::Db { .. }),
        "expected Db error, got: {err:?}"
    );

    // Only part 1 from the original commit should exist — parts 2 and 3 were rolled back.
    let parts = store
        .get_object_parts("b", "k", VersionId::from_u64(1))
        .unwrap();
    assert_eq!(parts.len(), 1, "batch should have been fully rolled back");
    assert_eq!(parts[0].part_number, 1);
}

#[test]
fn mpu_commit_empty_batch_succeeds() {
    let (_dir, store) = make_pg_store();
    // Empty batch should be a no-op.
    store.commit_object_parts(&[]).unwrap();
}

#[test]
fn mpu_upsert_rapid_generation_overwrites() {
    let (_dir, store) = make_pg_store();
    create_upload(&store, "uid-rapid");

    // Rapidly overwrite the same part 10 times with increasing generations.
    for gen in 0..10u32 {
        let prev = store
            .upsert_multipart_part(&make_part("uid-rapid", 1, gen))
            .unwrap();
        if gen == 0 {
            assert_eq!(prev, None);
        } else {
            assert_eq!(prev, Some(gen - 1));
        }
    }

    // Only the last generation should be visible.
    let part = store.get_multipart_part("uid-rapid", 1).unwrap();
    assert_eq!(part.generation, 9);

    // List should return exactly one part.
    let resp = store
        .list_multipart_parts(&ListPartsReq {
            upload_id: "uid-rapid".into(),
            part_number_marker: None,
            max_parts: 100,
        })
        .unwrap();
    assert_eq!(resp.parts.len(), 1);
    assert_eq!(resp.parts[0].generation, 9);
}

#[test]
fn mpu_concurrent_upserts_different_parts() {
    // Two connections to the same DB, upserting different parts of the same upload.
    let dir = test_util::tempdir();
    let pg_dir = dir.path().join("pg-0000");
    let store1 = crate::PgStore::open(&pg_dir, 0).unwrap();
    let store2 = crate::PgStore::open(&pg_dir, 0).unwrap();

    create_upload(&store1, "uid-conc");

    // Store1 upserts part 1, store2 upserts part 2.
    store1
        .upsert_multipart_part(&make_part("uid-conc", 1, 0))
        .unwrap();
    store2
        .upsert_multipart_part(&make_part("uid-conc", 2, 0))
        .unwrap();

    // Both parts should be visible from either connection.
    let p1 = store1.get_multipart_part("uid-conc", 1).unwrap();
    let p2 = store1.get_multipart_part("uid-conc", 2).unwrap();
    assert_eq!(p1.part_number, 1);
    assert_eq!(p2.part_number, 2);

    let p1b = store2.get_multipart_part("uid-conc", 1).unwrap();
    let p2b = store2.get_multipart_part("uid-conc", 2).unwrap();
    assert_eq!(p1b.part_number, 1);
    assert_eq!(p2b.part_number, 2);
}

#[test]
fn mpu_concurrent_upserts_same_part() {
    // Two connections racing to overwrite the same part — both should succeed
    // (serialized by SQLite's IMMEDIATE lock) and the last writer wins.
    let dir = test_util::tempdir();
    let pg_dir = dir.path().join("pg-0000");
    let store1 = crate::PgStore::open(&pg_dir, 0).unwrap();
    let store2 = crate::PgStore::open(&pg_dir, 0).unwrap();

    create_upload(&store1, "uid-race");

    // Store1 writes generation 0.
    store1
        .upsert_multipart_part(&make_part("uid-race", 1, 0))
        .unwrap();

    // Store2 writes generation 1 (overwriting store1's write).
    let prev = store2
        .upsert_multipart_part(&make_part("uid-race", 1, 1))
        .unwrap();
    assert_eq!(prev, Some(0), "store2 should see store1's generation 0");

    // The latest generation should be visible from both connections.
    let p1 = store1.get_multipart_part("uid-race", 1).unwrap();
    assert_eq!(p1.generation, 1);
    let p2 = store2.get_multipart_part("uid-race", 1).unwrap();
    assert_eq!(p2.generation, 1);
}

#[test]
fn mpu_concurrent_state_transition_one_wins() {
    // Two connections try to transition the same upload from InProgress.
    // Both target different states — only one can succeed on the WHERE state=0 predicate.
    let dir = test_util::tempdir();
    let pg_dir = dir.path().join("pg-0000");
    let store1 = crate::PgStore::open(&pg_dir, 0).unwrap();
    let store2 = crate::PgStore::open(&pg_dir, 0).unwrap();

    create_upload(&store1, "uid-trans");

    // Store1 transitions to Completing.
    store1
        .set_upload_state("uid-trans", UploadState::Completing)
        .unwrap();

    // Store2 tries to transition to Aborting — should fail (no longer InProgress).
    let err = store2
        .set_upload_state("uid-trans", UploadState::Aborting)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::error::MetadataError::UploadNotInProgress { state: 1 }
        ),
        "expected UploadNotInProgress {{ state: 1 }}, got: {err:?}"
    );
}

#[test]
fn mpu_commit_object_parts_connection_usable_after_multiple_failures() {
    let (_dir, store) = make_pg_store();

    let part = ObjectPartRecord {
        bucket: "b".into(),
        key: "k".into(),
        version_id: VersionId::from_u64(1),
        part_number: 1,
        size: 1024,
        etag: vec![0xAA],
        etag_kind: EtagKind::Crc64,
        part_okh: [1u8; 16],
        part_vid: GenerationId::MIN,
        ec_k: 4,
        ec_m: 2,
        shard_pg_id: 0,
        checksum: None,
    };

    store
        .commit_object_parts(std::slice::from_ref(&part))
        .unwrap();

    // Trigger 3 consecutive transaction failures.
    for _ in 0..3 {
        let err = store
            .commit_object_parts(std::slice::from_ref(&part))
            .unwrap_err();
        assert!(matches!(err, crate::error::MetadataError::Db { .. }));
    }

    // Connection should still work fine after repeated failures.
    let parts = store
        .get_object_parts("b", "k", VersionId::from_u64(1))
        .unwrap();
    assert_eq!(parts.len(), 1);

    // And a non-conflicting commit should succeed.
    let part2 = ObjectPartRecord {
        part_number: 2,
        ..part
    };
    store
        .commit_object_parts(std::slice::from_ref(&part2))
        .unwrap();
    let parts = store
        .get_object_parts("b", "k", VersionId::from_u64(1))
        .unwrap();
    assert_eq!(parts.len(), 2);
}

#[test]
fn mpu_delete_upload_during_upsert_returns_no_such_upload() {
    // Verify that deleting an upload makes subsequent upserts fail with NoSuchUpload.
    let (_dir, store) = make_pg_store();
    create_upload(&store, "uid-del");

    store
        .upsert_multipart_part(&make_part("uid-del", 1, 0))
        .unwrap();

    // Delete the upload (cascades parts).
    store.delete_multipart_upload("uid-del").unwrap();

    // Upsert should now fail with NoSuchUpload (FK violation).
    let err = store
        .upsert_multipart_part(&make_part("uid-del", 2, 0))
        .unwrap_err();
    assert!(
        matches!(err, crate::error::MetadataError::NoSuchUpload { .. }),
        "expected NoSuchUpload after delete, got: {err:?}"
    );

    // Part from before delete should also be gone (CASCADE).
    let err = store.get_multipart_part("uid-del", 1).unwrap_err();
    assert!(
        matches!(err, crate::error::MetadataError::PartNotFound { .. }),
        "expected PartNotFound after cascade delete, got: {err:?}"
    );
}

// --- COMMIT failure path tests ---

#[test]
fn mpu_upsert_commit_failure_via_deferred_fk() {
    // Use PRAGMA defer_foreign_keys to defer FK checks to COMMIT time.
    // INSERT succeeds (FK deferred), COMMIT fails with constraint violation.
    // This exercises the COMMIT failure branch in upsert_multipart_part.
    let (_dir, store) = make_pg_store();

    store
        .connection()
        .execute_batch("PRAGMA defer_foreign_keys=ON")
        .unwrap();

    let err = store
        .upsert_multipart_part(&make_part("nonexistent", 1, 0))
        .unwrap_err();

    // Must come from the COMMIT failure path, not the statement failure path.
    match err {
        crate::error::MetadataError::Db { context, .. } => {
            assert_eq!(
                context, "upsert part (commit txn)",
                "error should come from COMMIT failure path"
            );
        }
        other => panic!("expected Db error from COMMIT path, got: {other:?}"),
    }

    // Connection should be usable after the COMMIT failure + ROLLBACK.
    create_upload(&store, "uid-after");
    store
        .upsert_multipart_part(&make_part("uid-after", 1, 0))
        .unwrap();
    let part = store.get_multipart_part("uid-after", 1).unwrap();
    assert_eq!(part.generation, 0);
}

#[test]
fn mpu_upsert_commit_failure_preserves_prior_state() {
    // Verify that a COMMIT failure via deferred FK doesn't corrupt existing data.
    let (_dir, store) = make_pg_store();

    create_upload(&store, "uid-prior");
    store
        .upsert_multipart_part(&make_part("uid-prior", 1, 0))
        .unwrap();

    // Force a COMMIT failure on a different upload (nonexistent).
    store
        .connection()
        .execute_batch("PRAGMA defer_foreign_keys=ON")
        .unwrap();
    let _ = store
        .upsert_multipart_part(&make_part("nonexistent", 1, 0))
        .unwrap_err();

    // Prior data should be intact.
    let part = store.get_multipart_part("uid-prior", 1).unwrap();
    assert_eq!(part.generation, 0);
}

#[test]
fn mpu_commit_object_parts_commit_failure_via_lock_contention() {
    // Switch to DELETE journal mode so a reader holding SHARED lock prevents
    // COMMIT from acquiring EXCLUSIVE lock, forcing the COMMIT failure path.
    let dir = test_util::tempdir();
    let pg_dir = dir.path().join("pg-0000");
    let store = crate::PgStore::open(&pg_dir, 0).unwrap();

    // Switch from WAL to DELETE journal mode.
    store
        .connection()
        .execute_batch("PRAGMA journal_mode=DELETE")
        .unwrap();

    // Open a raw blocker connection that holds a SHARED lock via read transaction.
    let db_path = pg_dir.join("metadata.db");
    let blocker = rusqlite::Connection::open(&db_path).unwrap();
    blocker
        .execute_batch("BEGIN; SELECT * FROM object_parts;")
        .unwrap();

    // commit_object_parts: BEGIN IMMEDIATE gets RESERVED (OK), INSERT succeeds,
    // COMMIT fails with SQLITE_BUSY (can't upgrade to EXCLUSIVE).
    let part = ObjectPartRecord {
        bucket: "b".into(),
        key: "k".into(),
        version_id: VersionId::from_u64(1),
        part_number: 1,
        size: 1024,
        etag: vec![0xAA],
        etag_kind: EtagKind::Crc64,
        part_okh: [1u8; 16],
        part_vid: GenerationId::MIN,
        ec_k: 4,
        ec_m: 2,
        shard_pg_id: 0,
        checksum: None,
    };
    let err = store
        .commit_object_parts(std::slice::from_ref(&part))
        .unwrap_err();
    match err {
        crate::error::MetadataError::Db { context, .. } => {
            assert_eq!(
                context, "commit object parts (commit txn)",
                "error should come from COMMIT failure path"
            );
        }
        other => panic!("expected Db error from COMMIT path, got: {other:?}"),
    }

    // Release the blocker's lock.
    blocker.execute_batch("ROLLBACK").unwrap();
    drop(blocker);

    // Rolled-back parts should not be visible.
    let parts = store
        .get_object_parts("b", "k", VersionId::from_u64(1))
        .unwrap();
    assert!(parts.is_empty(), "rolled-back parts should not be visible");

    // Connection should be usable — retry the same commit.
    store
        .commit_object_parts(std::slice::from_ref(&part))
        .unwrap();
    let parts = store
        .get_object_parts("b", "k", VersionId::from_u64(1))
        .unwrap();
    assert_eq!(parts.len(), 1);
}

// --- Multi-threaded concurrency tests ---

#[test]
fn mpu_threaded_upserts_different_parts() {
    use std::sync::{Arc, Barrier};

    let dir = test_util::tempdir();
    let pg_dir = dir.path().join("pg-0000");

    let setup = crate::PgStore::open(&pg_dir, 0).unwrap();
    create_upload(&setup, "uid-mt");
    drop(setup);

    let store1 = crate::PgStore::open(&pg_dir, 0).unwrap();
    store1
        .connection()
        .execute_batch("PRAGMA busy_timeout=10000")
        .unwrap();
    let store2 = crate::PgStore::open(&pg_dir, 0).unwrap();
    store2
        .connection()
        .execute_batch("PRAGMA busy_timeout=10000")
        .unwrap();

    let barrier = Arc::new(Barrier::new(2));

    let b1 = barrier.clone();
    let t1 = std::thread::spawn(move || {
        b1.wait();
        store1
            .upsert_multipart_part(&make_part("uid-mt", 1, 0))
            .unwrap();
    });

    let b2 = barrier.clone();
    let t2 = std::thread::spawn(move || {
        b2.wait();
        store2
            .upsert_multipart_part(&make_part("uid-mt", 2, 0))
            .unwrap();
    });

    t1.join().unwrap();
    t2.join().unwrap();

    // Both parts should exist.
    let verify = crate::PgStore::open(&pg_dir, 0).unwrap();
    let p1 = verify.get_multipart_part("uid-mt", 1).unwrap();
    let p2 = verify.get_multipart_part("uid-mt", 2).unwrap();
    assert_eq!(p1.part_number, 1);
    assert_eq!(p2.part_number, 2);
}

#[test]
fn mpu_threaded_state_transition_race() {
    use std::sync::{Arc, Barrier};

    let dir = test_util::tempdir();
    let pg_dir = dir.path().join("pg-0000");

    let setup = crate::PgStore::open(&pg_dir, 0).unwrap();
    create_upload(&setup, "uid-race-t");
    drop(setup);

    let store1 = crate::PgStore::open(&pg_dir, 0).unwrap();
    store1
        .connection()
        .execute_batch("PRAGMA busy_timeout=10000")
        .unwrap();
    let store2 = crate::PgStore::open(&pg_dir, 0).unwrap();
    store2
        .connection()
        .execute_batch("PRAGMA busy_timeout=10000")
        .unwrap();

    let barrier = Arc::new(Barrier::new(2));

    let b1 = barrier.clone();
    let t1 = std::thread::spawn(move || {
        b1.wait();
        store1.set_upload_state("uid-race-t", UploadState::Completing)
    });

    let b2 = barrier.clone();
    let t2 = std::thread::spawn(move || {
        b2.wait();
        store2.set_upload_state("uid-race-t", UploadState::Aborting)
    });

    let r1 = t1.join().unwrap();
    let r2 = t2.join().unwrap();

    // Exactly one should succeed, the other should fail with UploadNotInProgress.
    let (ok_count, err_result) = match (&r1, &r2) {
        (Ok(()), Err(e)) => (1, e),
        (Err(e), Ok(())) => (1, e),
        _ => panic!("expected exactly one success and one failure, got: r1={r1:?}, r2={r2:?}"),
    };
    assert_eq!(ok_count, 1);
    assert!(
        matches!(
            err_result,
            crate::error::MetadataError::UploadNotInProgress { .. }
        ),
        "loser should get UploadNotInProgress, got: {err_result:?}"
    );
}

#[test]
fn mpu_threaded_upsert_same_part_stress() {
    use std::sync::{Arc, Barrier};

    let n_threads: usize = 8;
    let dir = test_util::tempdir();
    let pg_dir = dir.path().join("pg-0000");

    let setup = crate::PgStore::open(&pg_dir, 0).unwrap();
    create_upload(&setup, "uid-stress");
    drop(setup);

    let barrier = Arc::new(Barrier::new(n_threads));

    let handles: Vec<_> = (0..n_threads)
        .map(|i| {
            let b = barrier.clone();
            let pg = pg_dir.clone();
            std::thread::spawn(move || {
                let store = crate::PgStore::open(&pg, 0).unwrap();
                store
                    .connection()
                    .execute_batch("PRAGMA busy_timeout=10000")
                    .unwrap();
                b.wait();
                store
                    .upsert_multipart_part(&make_part("uid-stress", 1, i as u32))
                    .unwrap();
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // Exactly one part should exist with a generation from one of the threads.
    let verify = crate::PgStore::open(&pg_dir, 0).unwrap();
    let part = verify.get_multipart_part("uid-stress", 1).unwrap();
    assert!(
        (part.generation as usize) < n_threads,
        "generation {} should be from one of the {} threads",
        part.generation,
        n_threads
    );

    let resp = verify
        .list_multipart_parts(&ListPartsReq {
            upload_id: "uid-stress".into(),
            part_number_marker: None,
            max_parts: 100,
        })
        .unwrap();
    assert_eq!(
        resp.parts.len(),
        1,
        "INSERT OR REPLACE should leave exactly one row"
    );
}

// --- Property-based tests ---

#[cfg(test)]
mod prop_tests {
    use super::*;
    use proptest::prelude::*;

    fn insert_keys(store: &dyn PgMetadataStore, bucket: &str, keys: &[String]) {
        for key in keys {
            let req = PutObjectReq::Live(PutLiveObjectReq {
                bucket: bucket.into(),
                key: key.clone().into(),
                version_id: VersionId::Null,
                generation_id: GenerationId::MIN,
                ec: EcShape { k: 4, m: 2 },
                size: 0,
                etag: ObjectEtag::SinglePart([0; 8]),
                layout: ObjectLayout::Standard,
                tags: None,
                metadata_blob: None,
            });
            store.put_object_meta(&req).unwrap();
        }
    }

    fn list_all_keys(
        store: &dyn PgMetadataStore,
        bucket: &str,
        prefix: Option<ObjectKey>,
        max_keys: u32,
    ) -> Vec<ObjectKey> {
        let mut all = Vec::new();
        let mut start_after: Option<ObjectKey> = None;
        for _ in 0..1000 {
            let resp = store
                .list_objects(&ListObjectsReq {
                    bucket: bucket.into(),
                    prefix: prefix.clone(),
                    start_after: start_after.clone(),
                    max_keys,
                })
                .unwrap();

            for w in resp.objects.windows(2) {
                assert!(w[0].key() < w[1].key());
            }

            all.extend(resp.objects.iter().map(|o| o.key().clone()));
            if !resp.is_truncated {
                break;
            }
            assert!(resp.next_start_after.is_some());
            start_after = resp.next_start_after;
        }
        all
    }

    proptest! {
        #[test]
        fn prop_metadata_pagination_roundtrip(
            keys in proptest::collection::vec(
                proptest::string::string_regex(r"[A-Za-z0-9._/-]{0,16}").unwrap(),
                0..=40
            ),
            max_keys in 1u32..=10,
        ) {
            let (_dir, store) = super::make_pg_store();
            insert_keys(&store, "bucket", &keys);

            let mut expected: Vec<ObjectKey> = keys.iter().map(|k| ObjectKey::from(k.as_str())).collect();
            expected.sort();
            expected.dedup();

            let got = list_all_keys(&store, "bucket", None, max_keys);
            prop_assert_eq!(got, expected);
        }

        #[test]
        fn prop_metadata_prefix_subset(
            keys in proptest::collection::vec(
                proptest::string::string_regex(r"[A-Za-z0-9._/-]{0,16}").unwrap(),
                0..=40
            ),
            prefix in proptest::string::string_regex(r"[A-Za-z0-9._/-]{0,8}").unwrap(),
            max_keys in 1u32..=10,
        ) {
            let (_dir, store) = super::make_pg_store();
            insert_keys(&store, "bucket", &keys);

            let mut expected: Vec<ObjectKey> = keys
                .iter()
                .filter(|k| k.starts_with(&prefix))
                .map(|k| ObjectKey::from(k.as_str()))
                .collect();
            expected.sort();
            expected.dedup();

            let got = list_all_keys(&store, "bucket", Some(ObjectKey::from(prefix)), max_keys);
            prop_assert_eq!(got, expected);
        }
    }
}

// ── Streaming upload session tests (PgStore) ─────────────────────────

#[test]
fn stream_upload_create_get_delete() {
    let (_dir, store) = make_pg_store();

    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: "sess-1".into(),
            bucket: "b".into(),
            key: "k".into(),
            target: StreamUploadTarget::PutObject,
        })
        .unwrap();

    let rec = store.get_stream_upload("sess-1").unwrap();
    assert_eq!(rec.session_id, "sess-1");
    assert_eq!(rec.bucket, "b");
    assert_eq!(rec.key, "k");
    assert_eq!(rec.target, StreamUploadTarget::PutObject);
    assert_eq!(rec.state, StreamUploadState::InProgress);

    store.delete_stream_upload("sess-1").unwrap();

    let err = store.get_stream_upload("sess-1").unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::StreamSessionNotFound { .. }
    ));
}

#[test]
fn stream_upload_state_transitions() {
    let (_dir, store) = make_pg_store();

    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: "sess-2".into(),
            bucket: "b".into(),
            key: "k".into(),
            target: StreamUploadTarget::PutObject,
        })
        .unwrap();

    // Transition to Completing
    store
        .set_stream_upload_state("sess-2", StreamUploadState::Completing)
        .unwrap();

    let rec = store.get_stream_upload("sess-2").unwrap();
    assert_eq!(rec.state, StreamUploadState::Completing);

    // Cannot transition again (not InProgress)
    let err = store
        .set_stream_upload_state("sess-2", StreamUploadState::Aborted)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::StreamSessionNotInProgress { .. }
    ));
}

#[test]
fn stream_upload_not_found() {
    let (_dir, store) = make_pg_store();

    let err = store.get_stream_upload("nonexistent").unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::StreamSessionNotFound { .. }
    ));

    let err = store
        .set_stream_upload_state("nonexistent", StreamUploadState::Aborted)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::StreamSessionNotFound { .. }
    ));
}

#[test]
fn stream_upload_upload_part_kind() {
    let (_dir, store) = make_pg_store();

    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: "sess-part".into(),
            bucket: "b".into(),
            key: "k".into(),
            target: StreamUploadTarget::UploadPart {
                upload_id: "mpu-123".into(),
                part_number: 3,
            },
        })
        .unwrap();

    let rec = store.get_stream_upload("sess-part").unwrap();
    assert_eq!(
        rec.target,
        StreamUploadTarget::UploadPart {
            upload_id: "mpu-123".into(),
            part_number: 3
        }
    );
}

#[test]
fn stream_segment_append_and_list() {
    let (_dir, store) = make_pg_store();

    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: "sess-chunks".into(),
            bucket: "b".into(),
            key: "k".into(),
            target: StreamUploadTarget::PutObject,
        })
        .unwrap();

    for i in 0..3u32 {
        store
            .append_stream_segment(&StreamUploadSegmentRecord {
                session_id: "sess-chunks".into(),
                segment_index: i,
                size: (i as u64 + 1) * 1000,
                segment_crc64: Some((i as u64) + 10),
                segment_okh: [i as u8; 16],
                segment_vid: GenerationId::new(42).unwrap(),
                shard_pg_id: i,
                ec_k: 4,
                ec_m: 2,
            })
            .unwrap();
    }

    let segments = store.list_stream_segments("sess-chunks").unwrap();
    assert_eq!(segments.len(), 3);
    assert_eq!(segments[0].segment_index, 0);
    assert_eq!(segments[0].size, 1000);
    assert_eq!(segments[1].segment_index, 1);
    assert_eq!(segments[1].size, 2000);
    assert_eq!(segments[2].segment_index, 2);
    assert_eq!(segments[2].size, 3000);
    assert_eq!(segments[0].segment_crc64, Some(10));
    assert_eq!(segments[2].segment_crc64, Some(12));
    assert_eq!(segments[0].segment_okh, [0u8; 16]);
    assert_eq!(segments[2].shard_pg_id, 2);
}

#[test]
fn stream_segment_cascade_delete() {
    let (_dir, store) = make_pg_store();

    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: "sess-cascade".into(),
            bucket: "b".into(),
            key: "k".into(),
            target: StreamUploadTarget::PutObject,
        })
        .unwrap();

    store
        .append_stream_segment(&StreamUploadSegmentRecord {
            session_id: "sess-cascade".into(),
            segment_index: 0,
            size: 4096,
            segment_crc64: None,
            segment_okh: [0xAA; 16],
            segment_vid: GenerationId::new(1).unwrap(),
            shard_pg_id: 0,
            ec_k: 4,
            ec_m: 2,
        })
        .unwrap();

    // Deleting session cascades to chunks
    store.delete_stream_upload("sess-cascade").unwrap();

    let segments = store.list_stream_segments("sess-cascade").unwrap();
    assert!(segments.is_empty());
}

#[test]
fn commit_stream_put_atomic() {
    let (_dir, store) = make_pg_store();

    // Create session and append chunks
    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: "sess-commit".into(),
            bucket: "b".into(),
            key: "k".into(),
            target: StreamUploadTarget::PutObject,
        })
        .unwrap();

    store
        .append_stream_segment(&StreamUploadSegmentRecord {
            session_id: "sess-commit".into(),
            segment_index: 0,
            size: 4_000_000,
            segment_crc64: None,
            segment_okh: [0x11; 16],
            segment_vid: GenerationId::new(1).unwrap(),
            shard_pg_id: 0,
            ec_k: 4,
            ec_m: 2,
        })
        .unwrap();

    store
        .append_stream_segment(&StreamUploadSegmentRecord {
            session_id: "sess-commit".into(),
            segment_index: 1,
            size: 2_000_000,
            segment_crc64: None,
            segment_okh: [0x22; 16],
            segment_vid: GenerationId::new(1).unwrap(),
            shard_pg_id: 1,
            ec_k: 4,
            ec_m: 2,
        })
        .unwrap();

    // Commit
    let obj = CommitStreamPutReq {
        bucket: "b".into(),
        key: "k".into(),
        version_id: VersionId::Null,
        generation_id: GenerationId::MIN,
        size: 6_000_000,
        etag_crc64: u64::from_le_bytes([0xAB; 8]),
        ec: EcShape { k: 4, m: 2 },
        tags: None,
        metadata_blob: None,
    };

    let committed_chunks = vec![
        ObjectSegmentRecord {
            bucket: "b".into(),
            key: "k".into(),
            version_id: VersionId::Null,
            segment_index: 0,
            size: 4_000_000,
            segment_crc64: Some(11),
            segment_okh: [0x11; 16],
            segment_vid: GenerationId::new(1).unwrap(),
            shard_pg_id: 0,
            ec_k: 4,
            ec_m: 2,
        },
        ObjectSegmentRecord {
            bucket: "b".into(),
            key: "k".into(),
            version_id: VersionId::Null,
            segment_index: 1,
            size: 2_000_000,
            segment_crc64: Some(22),
            segment_okh: [0x22; 16],
            segment_vid: GenerationId::new(1).unwrap(),
            shard_pg_id: 1,
            ec_k: 4,
            ec_m: 2,
        },
    ];

    store
        .commit_stream_put("sess-commit", &obj, &committed_chunks)
        .unwrap();

    // Object metadata is committed
    let record = store.get_object_meta("b", "k").unwrap();
    let live = record.as_live().unwrap();
    assert_eq!(live.size, 6_000_000);
    assert_eq!(live.layout, ObjectLayout::Standard);

    // Committed chunks are readable
    let chunks = store
        .get_object_segments("b", "k", VersionId::Null)
        .unwrap();
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].segment_index, 0);
    assert_eq!(chunks[0].size, 4_000_000);
    assert_eq!(chunks[0].segment_crc64, Some(11));
    assert_eq!(chunks[0].segment_okh, [0x11; 16]);
    assert_eq!(chunks[0].shard_pg_id, 0);
    assert_eq!(chunks[1].segment_index, 1);
    assert_eq!(chunks[1].size, 2_000_000);
    assert_eq!(chunks[1].segment_crc64, Some(22));
    assert_eq!(chunks[1].shard_pg_id, 1);

    // Staging rows are cleaned up
    let err = store.get_stream_upload("sess-commit").unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::StreamSessionNotFound { .. }
    ));
    let staging = store.list_stream_segments("sess-commit").unwrap();
    assert!(staging.is_empty());
}

#[test]
fn commit_stream_put_overwrite_unversioned() {
    let (_dir, store) = make_pg_store();

    // First write
    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: "s1".into(),
            bucket: "b".into(),
            key: "k".into(),
            target: StreamUploadTarget::PutObject,
        })
        .unwrap();
    store
        .commit_stream_put(
            "s1",
            &CommitStreamPutReq {
                bucket: "b".into(),
                key: "k".into(),
                version_id: VersionId::Null,
                generation_id: GenerationId::MIN,
                size: 100,
                etag_crc64: 1u64,
                ec: EcShape { k: 4, m: 2 },
                tags: None,
                metadata_blob: None,
            },
            &[ObjectSegmentRecord {
                bucket: "b".into(),
                key: "k".into(),
                version_id: VersionId::Null,
                segment_index: 0,
                size: 100,
                segment_crc64: None,
                segment_okh: [1; 16],
                segment_vid: GenerationId::new(1).unwrap(),
                shard_pg_id: 0,
                ec_k: 4,
                ec_m: 2,
            }],
        )
        .unwrap();

    // Second write overwrites
    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: "s2".into(),
            bucket: "b".into(),
            key: "k".into(),
            target: StreamUploadTarget::PutObject,
        })
        .unwrap();
    store
        .commit_stream_put(
            "s2",
            &CommitStreamPutReq {
                bucket: "b".into(),
                key: "k".into(),
                version_id: VersionId::Null,
                generation_id: GenerationId::MIN,
                size: 200,
                etag_crc64: 2u64,
                ec: EcShape { k: 4, m: 2 },
                tags: None,
                metadata_blob: None,
            },
            &[ObjectSegmentRecord {
                bucket: "b".into(),
                key: "k".into(),
                version_id: VersionId::Null,
                segment_index: 0,
                size: 200,
                segment_crc64: None,
                segment_okh: [2; 16],
                segment_vid: GenerationId::new(2).unwrap(),
                shard_pg_id: 1,
                ec_k: 4,
                ec_m: 2,
            }],
        )
        .unwrap();

    // Verify overwrite: new data
    let record = store.get_object_meta("b", "k").unwrap();
    assert_eq!(record.as_live().unwrap().size, 200);

    // Chunks replaced
    let chunks = store
        .get_object_segments("b", "k", VersionId::Null)
        .unwrap();
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].size, 200);
    assert_eq!(chunks[0].segment_okh, [2; 16]);
}

#[test]
fn put_object_with_segments_persists_manifest() {
    let (_dir, store) = make_pg_store();

    store
        .put_object_with_segments(
            &PutLiveObjectReq {
                bucket: "b".into(),
                key: "k".into(),
                version_id: VersionId::Null,
                generation_id: GenerationId::MIN,
                size: 6_000_000,
                etag: ObjectEtag::single_part(1),
                ec: EcShape { k: 4, m: 2 },
                layout: ObjectLayout::Standard,
                tags: None,
                metadata_blob: None,
            },
            &[
                ObjectSegmentRecord {
                    bucket: "b".into(),
                    key: "k".into(),
                    version_id: VersionId::Null,
                    segment_index: 0,
                    size: 4_000_000,
                    segment_crc64: None,
                    segment_okh: [0x11; 16],
                    segment_vid: GenerationId::MIN,
                    shard_pg_id: 0,
                    ec_k: 4,
                    ec_m: 2,
                },
                ObjectSegmentRecord {
                    bucket: "b".into(),
                    key: "k".into(),
                    version_id: VersionId::Null,
                    segment_index: 1,
                    size: 2_000_000,
                    segment_crc64: None,
                    segment_okh: [0x22; 16],
                    segment_vid: GenerationId::MIN,
                    shard_pg_id: 0,
                    ec_k: 4,
                    ec_m: 2,
                },
            ],
        )
        .unwrap();

    let record = store.get_object_meta("b", "k").unwrap();
    let live = record.as_live().unwrap();
    assert_eq!(live.size, 6_000_000);
    assert_eq!(live.layout, ObjectLayout::Standard);

    let segments = store
        .get_object_segments("b", "k", VersionId::Null)
        .unwrap();
    assert_eq!(segments.len(), 2);
    assert_eq!(segments[0].segment_okh, [0x11; 16]);
    assert_eq!(segments[1].segment_okh, [0x22; 16]);
}

#[test]
fn put_object_with_segments_overwrite_unversioned_replaces_manifest() {
    let (_dir, store) = make_pg_store();

    store
        .put_object_with_segments(
            &PutLiveObjectReq {
                bucket: "b".into(),
                key: "k".into(),
                version_id: VersionId::Null,
                generation_id: GenerationId::MIN,
                size: 100,
                etag: ObjectEtag::single_part(1),
                ec: EcShape { k: 4, m: 2 },
                layout: ObjectLayout::Standard,
                tags: None,
                metadata_blob: None,
            },
            &[ObjectSegmentRecord {
                bucket: "b".into(),
                key: "k".into(),
                version_id: VersionId::Null,
                segment_index: 0,
                size: 100,
                segment_crc64: None,
                segment_okh: [1; 16],
                segment_vid: GenerationId::MIN,
                shard_pg_id: 0,
                ec_k: 4,
                ec_m: 2,
            }],
        )
        .unwrap();

    store
        .put_object_with_segments(
            &PutLiveObjectReq {
                bucket: "b".into(),
                key: "k".into(),
                version_id: VersionId::Null,
                generation_id: GenerationId::new(2).unwrap(),
                size: 200,
                etag: ObjectEtag::single_part(2),
                ec: EcShape { k: 4, m: 2 },
                layout: ObjectLayout::Standard,
                tags: None,
                metadata_blob: None,
            },
            &[ObjectSegmentRecord {
                bucket: "b".into(),
                key: "k".into(),
                version_id: VersionId::Null,
                segment_index: 0,
                size: 200,
                segment_crc64: None,
                segment_okh: [2; 16],
                segment_vid: GenerationId::MIN,
                shard_pg_id: 1,
                ec_k: 4,
                ec_m: 2,
            }],
        )
        .unwrap();

    let record = store.get_object_meta("b", "k").unwrap();
    assert_eq!(record.as_live().unwrap().size, 200);

    let segments = store
        .get_object_segments("b", "k", VersionId::Null)
        .unwrap();
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].size, 200);
    assert_eq!(segments[0].segment_okh, [2; 16]);
}

#[test]
fn delete_object_segments_cleanup() {
    let (_dir, store) = make_pg_store();

    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: "s-del".into(),
            bucket: "b".into(),
            key: "k".into(),
            target: StreamUploadTarget::PutObject,
        })
        .unwrap();
    store
        .commit_stream_put(
            "s-del",
            &CommitStreamPutReq {
                bucket: "b".into(),
                key: "k".into(),
                version_id: VersionId::Null,
                generation_id: GenerationId::MIN,
                size: 100,
                etag_crc64: 1u64,
                ec: EcShape { k: 4, m: 2 },
                tags: None,
                metadata_blob: None,
            },
            &[ObjectSegmentRecord {
                bucket: "b".into(),
                key: "k".into(),
                version_id: VersionId::Null,
                segment_index: 0,
                size: 100,
                segment_crc64: None,
                segment_okh: [1; 16],
                segment_vid: GenerationId::new(1).unwrap(),
                shard_pg_id: 0,
                ec_k: 4,
                ec_m: 2,
            }],
        )
        .unwrap();

    // Delete committed chunks
    store
        .delete_object_segments("b", "k", VersionId::Null)
        .unwrap();
    let chunks = store
        .get_object_segments("b", "k", VersionId::Null)
        .unwrap();
    assert!(chunks.is_empty());

    // Idempotent
    store
        .delete_object_segments("b", "k", VersionId::Null)
        .unwrap();
}

#[test]
fn commit_stream_put_rejects_non_in_progress() {
    let (_dir, store) = make_pg_store();

    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: "sess-bad".into(),
            bucket: "b".into(),
            key: "k".into(),
            target: StreamUploadTarget::PutObject,
        })
        .unwrap();

    // Manually transition to Aborted
    store
        .set_stream_upload_state("sess-bad", StreamUploadState::Aborted)
        .unwrap();

    // commit_stream_put should fail with StreamSessionNotInProgress
    let err = store
        .commit_stream_put(
            "sess-bad",
            &CommitStreamPutReq {
                bucket: "b".into(),
                key: "k".into(),
                version_id: VersionId::Null,
                generation_id: GenerationId::MIN,
                size: 0,
                etag_crc64: 0u64,
                ec: EcShape { k: 4, m: 2 },
                tags: None,
                metadata_blob: None,
            },
            &[],
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::error::MetadataError::StreamSessionNotInProgress { .. }
        ),
        "expected StreamSessionNotInProgress, got: {err:?}"
    );
}

#[test]
fn multipart_part_segments_crud() {
    let (_dir, store) = make_pg_store();

    // Insert part segments directly (simulating committed state)
    let conn = store.connection();
    conn.execute(
        "INSERT INTO multipart_part_segments \
         (bucket, key, upload_id, version_id, part_number, segment_index, size, segment_okh, \
          segment_vid, shard_pg_id, ec_k, ec_m) \
         VALUES ('b', 'k', 'uid-1', 1, 1, 0, 4000000, X'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 10, 0, 4, 2)",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO multipart_part_segments \
         (bucket, key, upload_id, version_id, part_number, segment_index, size, segment_okh, \
          segment_vid, shard_pg_id, ec_k, ec_m) \
         VALUES ('b', 'k', 'uid-1', 1, 1, 1, 2000000, X'BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB', 10, 1, 4, 2)",
        [],
    )
    .unwrap();

    // Read back
    let chunks = store
        .get_multipart_part_segments("b", "k", VersionId::from_u64(1), 1)
        .unwrap();
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].segment_index, 0);
    assert_eq!(chunks[0].size, 4_000_000);
    assert_eq!(chunks[1].segment_index, 1);
    assert_eq!(chunks[1].size, 2_000_000);

    // Delete
    store
        .delete_multipart_part_segments("b", "k", VersionId::from_u64(1))
        .unwrap();
    let chunks = store
        .get_multipart_part_segments("b", "k", VersionId::from_u64(1), 1)
        .unwrap();
    assert!(chunks.is_empty());
}

#[test]
fn upsert_multipart_part_segments_replaces_prior_segments() {
    let (_dir, store) = make_pg_store();

    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: "mpu-1".into(),
            bucket: "b".into(),
            key: "k".into(),
            metadata_blob: vec![],
            owner_principal: None,
            checksum: None,
        })
        .unwrap();

    let make_part = |generation, size| MultipartPartRecord {
        upload_id: "mpu-1".into(),
        part_number: 1,
        generation,
        size,
        etag: vec![1],
        etag_kind: EtagKind::Crc64,
        part_okh: [0u8; 16],
        part_vid: GenerationId::MIN,
        ec_k: 4,
        ec_m: 2,
        last_modified: 1000,
        checksum: None,
    };
    let make_segment = |segment_index, size, fill| MultipartPartSegmentRecord {
        bucket: "b".into(),
        key: "k".into(),
        upload_id: "mpu-1".into(),
        version_id: MULTIPART_PART_SEGMENT_STAGING_VERSION_ID.to_u64(),
        part_number: 1,
        segment_index,
        size,
        segment_crc64: Some(u64::from(fill) + u64::from(segment_index)),
        segment_okh: [fill; 16],
        segment_vid: GenerationId::MIN,
        shard_pg_id: 0,
        ec_k: 4,
        ec_m: 2,
    };

    let (prev_gen, prev_segments) = store
        .upsert_multipart_part_segments(
            &make_part(0, 10),
            &[make_segment(0, 6, 0x11), make_segment(1, 4, 0x22)],
        )
        .unwrap();
    assert_eq!(prev_gen, None);
    assert!(prev_segments.is_empty());

    let (prev_gen, prev_segments) = store
        .upsert_multipart_part_segments(&make_part(1, 7), &[make_segment(0, 7, 0x33)])
        .unwrap();
    assert_eq!(prev_gen, Some(0));
    assert_eq!(prev_segments.len(), 2);
    assert_eq!(prev_segments[0].segment_crc64, Some(0x11));
    assert_eq!(prev_segments[1].segment_crc64, Some(0x23));
    assert_eq!(prev_segments[0].segment_okh, [0x11; 16]);
    assert_eq!(prev_segments[1].segment_okh, [0x22; 16]);

    let part = store.get_multipart_part("mpu-1", 1).unwrap();
    assert_eq!(part.generation, 1);
    assert_eq!(part.part_okh, [0u8; 16]);

    let segments = store
        .get_all_multipart_part_segments_for_upload("mpu-1")
        .unwrap();
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].segment_crc64, Some(0x33));
    assert_eq!(segments[0].segment_okh, [0x33; 16]);
}

#[test]
fn commit_stream_part_replaces_prior_chunks_on_reupload() {
    let (_dir, store) = make_pg_store();

    // Create the multipart upload first
    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: "mpu-1".into(),
            bucket: "b".into(),
            key: "k".into(),
            metadata_blob: vec![],
            owner_principal: None,
            checksum: None,
        })
        .unwrap();

    let make_part = |size: u64| MultipartPartRecord {
        upload_id: "mpu-1".into(),
        part_number: 1,
        generation: 0,
        size,
        etag: vec![1],
        etag_kind: EtagKind::Crc64,
        part_okh: [0xAA; 16],
        part_vid: GenerationId::new(1).unwrap(),
        ec_k: 4,
        ec_m: 2,
        last_modified: 1000,
        checksum: None,
    };

    // First upload: 3 chunks
    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: "sp-1".into(),
            bucket: "b".into(),
            key: "k".into(),
            target: StreamUploadTarget::UploadPart {
                upload_id: "mpu-1".into(),
                part_number: 1,
            },
        })
        .unwrap();

    let chunks_v1: Vec<MultipartPartSegmentRecord> = (0..3)
        .map(|i| MultipartPartSegmentRecord {
            bucket: "b".into(),
            key: "k".into(),
            upload_id: "mpu-1".into(),
            version_id: u64::MAX,
            part_number: 1,
            segment_index: i,
            size: 1000,
            segment_crc64: None,
            segment_okh: [0x11; 16],
            segment_vid: GenerationId::new(1).unwrap(),
            shard_pg_id: i,
            ec_k: 4,
            ec_m: 2,
        })
        .collect();

    store
        .commit_stream_part("sp-1", &make_part(3000), &chunks_v1)
        .unwrap();

    let chunks = store
        .get_multipart_part_segments("b", "k", VersionId::from_u64(u64::MAX), 1)
        .unwrap();
    assert_eq!(chunks.len(), 3);

    // Re-upload same part: only 1 chunk (fewer than before)
    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: "sp-2".into(),
            bucket: "b".into(),
            key: "k".into(),
            target: StreamUploadTarget::UploadPart {
                upload_id: "mpu-1".into(),
                part_number: 1,
            },
        })
        .unwrap();

    let chunks_v2 = vec![MultipartPartSegmentRecord {
        bucket: "b".into(),
        key: "k".into(),
        upload_id: "mpu-1".into(),
        version_id: u64::MAX,
        part_number: 1,
        segment_index: 0,
        size: 5000,
        segment_crc64: None,
        segment_okh: [0x22; 16],
        segment_vid: GenerationId::new(2).unwrap(),
        shard_pg_id: 0,
        ec_k: 4,
        ec_m: 2,
    }];

    store
        .commit_stream_part("sp-2", &make_part(5000), &chunks_v2)
        .unwrap();

    // Verify: only 1 chunk (stale rows deleted)
    let chunks = store
        .get_multipart_part_segments("b", "k", VersionId::from_u64(u64::MAX), 1)
        .unwrap();
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].size, 5000);
    assert_eq!(chunks[0].segment_okh, [0x22; 16]);
}

#[test]
fn commit_stream_put_rejects_wrong_kind() {
    let (_dir, store) = make_pg_store();

    // Create an UploadPart session
    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: "sess-wrong-kind".into(),
            bucket: "b".into(),
            key: "k".into(),
            target: StreamUploadTarget::UploadPart {
                upload_id: "mpu-x".into(),
                part_number: 1,
            },
        })
        .unwrap();

    // Try to commit_stream_put with it — should fail
    let err = store
        .commit_stream_put(
            "sess-wrong-kind",
            &CommitStreamPutReq {
                bucket: "b".into(),
                key: "k".into(),
                version_id: VersionId::Null,
                generation_id: GenerationId::MIN,
                size: 0,
                etag_crc64: 0u64,
                ec: EcShape { k: 4, m: 2 },
                tags: None,
                metadata_blob: None,
            },
            &[],
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::error::MetadataError::StreamSessionNotFound { .. }
        ),
        "expected StreamSessionNotFound for wrong kind, got: {err:?}"
    );
}

#[test]
fn commit_stream_put_rejects_wrong_bucket_key() {
    let (_dir, store) = make_pg_store();

    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: "sess-mismatch".into(),
            bucket: "b1".into(),
            key: "k1".into(),
            target: StreamUploadTarget::PutObject,
        })
        .unwrap();

    // Commit with different bucket/key
    let err = store
        .commit_stream_put(
            "sess-mismatch",
            &CommitStreamPutReq {
                bucket: "b2".into(),
                key: "k2".into(),
                version_id: VersionId::Null,
                generation_id: GenerationId::MIN,
                size: 0,
                etag_crc64: 0u64,
                ec: EcShape { k: 4, m: 2 },
                tags: None,
                metadata_blob: None,
            },
            &[],
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::error::MetadataError::StreamSessionNotFound { .. }
        ),
        "expected StreamSessionNotFound for wrong bucket/key, got: {err:?}"
    );
}

#[test]
fn commit_stream_part_rejects_wrong_upload_id() {
    let (_dir, store) = make_pg_store();

    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: "mpu-correct".into(),
            bucket: "b".into(),
            key: "k".into(),
            metadata_blob: vec![],
            owner_principal: None,
            checksum: None,
        })
        .unwrap();

    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: "sp-mismatch".into(),
            bucket: "b".into(),
            key: "k".into(),
            target: StreamUploadTarget::UploadPart {
                upload_id: "mpu-correct".into(),
                part_number: 1,
            },
        })
        .unwrap();

    // Commit with wrong upload_id in part record
    let err = store
        .commit_stream_part(
            "sp-mismatch",
            &MultipartPartRecord {
                upload_id: "mpu-WRONG".into(),
                part_number: 1,
                generation: 0,
                size: 100,
                etag: vec![1],
                etag_kind: EtagKind::Crc64,
                part_okh: [0xAA; 16],
                part_vid: GenerationId::new(1).unwrap(),
                ec_k: 4,
                ec_m: 2,
                last_modified: 1000,
                checksum: None,
            },
            &[],
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::error::MetadataError::StreamSessionNotFound { .. }
        ),
        "expected StreamSessionNotFound for wrong upload_id, got: {err:?}"
    );
}

#[test]
fn commit_stream_part_zero_chunks_clears_prior() {
    let (_dir, store) = make_pg_store();

    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: "mpu-zc".into(),
            bucket: "b".into(),
            key: "k".into(),
            metadata_blob: vec![],
            owner_principal: None,
            checksum: None,
        })
        .unwrap();

    // First upload: 2 chunks
    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: "sp-zc1".into(),
            bucket: "b".into(),
            key: "k".into(),
            target: StreamUploadTarget::UploadPart {
                upload_id: "mpu-zc".into(),
                part_number: 1,
            },
        })
        .unwrap();
    store
        .commit_stream_part(
            "sp-zc1",
            &MultipartPartRecord {
                upload_id: "mpu-zc".into(),
                part_number: 1,
                generation: 0,
                size: 2000,
                etag: vec![1],
                etag_kind: EtagKind::Crc64,
                part_okh: [0xAA; 16],
                part_vid: GenerationId::new(1).unwrap(),
                ec_k: 4,
                ec_m: 2,
                last_modified: 1000,
                checksum: None,
            },
            &[
                MultipartPartSegmentRecord {
                    bucket: "b".into(),
                    key: "k".into(),
                    upload_id: "mpu-zc".into(),
                    version_id: u64::MAX,
                    part_number: 1,
                    segment_index: 0,
                    size: 1000,
                    segment_crc64: None,
                    segment_okh: [0x11; 16],
                    segment_vid: GenerationId::new(1).unwrap(),
                    shard_pg_id: 0,
                    ec_k: 4,
                    ec_m: 2,
                },
                MultipartPartSegmentRecord {
                    bucket: "b".into(),
                    key: "k".into(),
                    upload_id: "mpu-zc".into(),
                    version_id: u64::MAX,
                    part_number: 1,
                    segment_index: 1,
                    size: 1000,
                    segment_crc64: None,
                    segment_okh: [0x22; 16],
                    segment_vid: GenerationId::new(1).unwrap(),
                    shard_pg_id: 1,
                    ec_k: 4,
                    ec_m: 2,
                },
            ],
        )
        .unwrap();

    assert_eq!(
        store
            .get_multipart_part_segments("b", "k", VersionId::from_u64(u64::MAX), 1)
            .unwrap()
            .len(),
        2
    );

    // Re-upload with zero chunks — must clear prior rows
    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: "sp-zc2".into(),
            bucket: "b".into(),
            key: "k".into(),
            target: StreamUploadTarget::UploadPart {
                upload_id: "mpu-zc".into(),
                part_number: 1,
            },
        })
        .unwrap();
    store
        .commit_stream_part(
            "sp-zc2",
            &MultipartPartRecord {
                upload_id: "mpu-zc".into(),
                part_number: 1,
                generation: 0,
                size: 0,
                etag: vec![2],
                etag_kind: EtagKind::Crc64,
                part_okh: [0xBB; 16],
                part_vid: GenerationId::new(2).unwrap(),
                ec_k: 4,
                ec_m: 2,
                last_modified: 2000,
                checksum: None,
            },
            &[], // zero chunks
        )
        .unwrap();

    let chunks = store
        .get_multipart_part_segments("b", "k", VersionId::from_u64(u64::MAX), 1)
        .unwrap();
    assert!(
        chunks.is_empty(),
        "stale chunks should be deleted on zero-chunk re-upload"
    );
}

#[test]
fn commit_stream_put_rejects_mismatched_chunk_target() {
    let (_dir, store) = make_pg_store();

    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: "sp-ct".into(),
            bucket: "b".into(),
            key: "k".into(),
            target: StreamUploadTarget::PutObject,
        })
        .unwrap();

    // Chunk with wrong bucket
    let err = store
        .commit_stream_put(
            "sp-ct",
            &CommitStreamPutReq {
                bucket: "b".into(),
                key: "k".into(),
                version_id: VersionId::Null,
                generation_id: GenerationId::MIN,
                size: 100,
                etag_crc64: 1u64,
                ec: EcShape { k: 4, m: 2 },
                tags: None,
                metadata_blob: None,
            },
            &[ObjectSegmentRecord {
                bucket: "WRONG".into(),
                key: "k".into(),
                version_id: VersionId::Null,
                segment_index: 0,
                size: 100,
                segment_crc64: None,
                segment_okh: [1; 16],
                segment_vid: GenerationId::new(1).unwrap(),
                shard_pg_id: 0,
                ec_k: 4,
                ec_m: 2,
            }],
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::error::MetadataError::StreamSessionNotFound { .. }
        ),
        "expected rejection for mismatched chunk bucket, got: {err:?}"
    );
}

#[test]
fn object_segments_reclaim_round_trip() {
    let (_dir, store) = make_pg_store();

    let generation_id = GenerationId::new(7).unwrap();
    let reclaim = ObjectSegmentsReclaimRecord {
        bucket: "b".into(),
        key: "k".into(),
        generation_id,
        created_at: 1234,
        segments: vec![
            ObjectSegmentsReclaimSegmentRecord {
                segment_index: 0,
                segment_okh: [0x11; 16],
                segment_vid: GenerationId::new(11).unwrap(),
                shard_pg_id: 1,
                ec: EcShape { k: 4, m: 2 },
            },
            ObjectSegmentsReclaimSegmentRecord {
                segment_index: 1,
                segment_okh: [0x22; 16],
                segment_vid: GenerationId::new(12).unwrap(),
                shard_pg_id: 2,
                ec: EcShape { k: 6, m: 3 },
            },
        ],
    };

    store.put_object_segments_reclaim(&reclaim).unwrap();

    let got = store
        .get_object_segments_reclaim("b", "k", generation_id)
        .unwrap()
        .expect("object segments reclaim should exist");
    assert_eq!(got, reclaim);

    store
        .delete_object_segments_reclaim("b", "k", generation_id)
        .unwrap();
    assert!(store
        .get_object_segments_reclaim("b", "k", generation_id)
        .unwrap()
        .is_none());
}

#[test]
fn next_generation_id_skips_object_segments_reclaim_generation() {
    let (_dir, store) = make_pg_store();

    store
        .put_object_segments_reclaim(&ObjectSegmentsReclaimRecord {
            bucket: "b".into(),
            key: "k".into(),
            generation_id: GenerationId::new(7).unwrap(),
            created_at: 1,
            segments: vec![ObjectSegmentsReclaimSegmentRecord {
                segment_index: 0,
                segment_okh: [0x33; 16],
                segment_vid: GenerationId::new(13).unwrap(),
                shard_pg_id: 0,
                ec: EcShape { k: 4, m: 2 },
            }],
        })
        .unwrap();

    assert_eq!(
        store.next_generation_id("b", "k").unwrap(),
        GenerationId::new(8).unwrap()
    );
}

#[test]
fn multipart_reclaim_round_trip() {
    let (_dir, store) = make_pg_store();

    let generation_id = GenerationId::new(9).unwrap();
    let reclaim = MultipartReclaimRecord {
        bucket: "b".into(),
        key: "k".into(),
        generation_id,
        created_at: 4321,
        parts: vec![
            MultipartReclaimPartRecord::ShardSet {
                part_number: 1,
                part_okh: [0x44; 16],
                part_vid: GenerationId::new(21).unwrap(),
                shard_pg_id: 3,
                ec: EcShape { k: 4, m: 2 },
            },
            MultipartReclaimPartRecord::Segments {
                part_number: 2,
                segments: vec![
                    MultipartReclaimPartSegmentRecord {
                        part_number: 2,
                        segment_index: 0,
                        segment_okh: [0x55; 16],
                        segment_vid: GenerationId::new(22).unwrap(),
                        shard_pg_id: 4,
                        ec: EcShape { k: 6, m: 3 },
                    },
                    MultipartReclaimPartSegmentRecord {
                        part_number: 2,
                        segment_index: 1,
                        segment_okh: [0x66; 16],
                        segment_vid: GenerationId::new(23).unwrap(),
                        shard_pg_id: 5,
                        ec: EcShape { k: 5, m: 2 },
                    },
                ],
            },
        ],
    };

    store.put_multipart_reclaim(&reclaim).unwrap();

    let got = store
        .get_multipart_reclaim("b", "k", generation_id)
        .unwrap()
        .expect("multipart reclaim should exist");
    assert_eq!(got, reclaim);

    store
        .delete_multipart_reclaim("b", "k", generation_id)
        .unwrap();
    assert!(store
        .get_multipart_reclaim("b", "k", generation_id)
        .unwrap()
        .is_none());
}

#[test]
fn next_generation_id_skips_multipart_reclaim_generation() {
    let (_dir, store) = make_pg_store();

    store
        .put_multipart_reclaim(&MultipartReclaimRecord {
            bucket: "b".into(),
            key: "k".into(),
            generation_id: GenerationId::new(11).unwrap(),
            created_at: 1,
            parts: vec![MultipartReclaimPartRecord::ShardSet {
                part_number: 1,
                part_okh: [0x77; 16],
                part_vid: GenerationId::new(24).unwrap(),
                shard_pg_id: 0,
                ec: EcShape { k: 4, m: 2 },
            }],
        })
        .unwrap();

    assert_eq!(
        store.next_generation_id("b", "k").unwrap(),
        GenerationId::new(12).unwrap()
    );
}

#[test]
fn get_bucket_payload_reclaim_root_returns_first_root() {
    let (_dir, store) = make_pg_store();

    store
        .put_multipart_reclaim(&MultipartReclaimRecord {
            bucket: "b".into(),
            key: "z".into(),
            generation_id: GenerationId::new(9).unwrap(),
            created_at: 1,
            parts: vec![MultipartReclaimPartRecord::ShardSet {
                part_number: 1,
                part_okh: [0x11; 16],
                part_vid: GenerationId::new(21).unwrap(),
                shard_pg_id: 0,
                ec: EcShape { k: 4, m: 2 },
            }],
        })
        .unwrap();
    store
        .put_simple_payload_reclaim(&SimplePayloadReclaimRecord {
            bucket: "b".into(),
            key: "a".into(),
            generation_id: GenerationId::new(3).unwrap(),
            ec: EcShape { k: 4, m: 2 },
            created_at: 1,
        })
        .unwrap();

    let root = store
        .get_bucket_payload_reclaim_root("b")
        .unwrap()
        .expect("bucket reclaim root should exist");
    assert_eq!(root.bucket, "b");
    assert_eq!(root.key, "a");
    assert_eq!(root.generation_id, GenerationId::new(3).unwrap());
}

#[test]
fn commit_stream_part_rejects_mismatched_chunk_part_number() {
    let (_dir, store) = make_pg_store();

    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: "mpu-cpc".into(),
            bucket: "b".into(),
            key: "k".into(),
            metadata_blob: vec![],
            owner_principal: None,
            checksum: None,
        })
        .unwrap();

    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: "sp-cpc".into(),
            bucket: "b".into(),
            key: "k".into(),
            target: StreamUploadTarget::UploadPart {
                upload_id: "mpu-cpc".into(),
                part_number: 1,
            },
        })
        .unwrap();

    // Chunk has part_number=99, but session is for part 1
    let err = store
        .commit_stream_part(
            "sp-cpc",
            &MultipartPartRecord {
                upload_id: "mpu-cpc".into(),
                part_number: 1,
                generation: 0,
                size: 100,
                etag: vec![1],
                etag_kind: EtagKind::Crc64,
                part_okh: [0xAA; 16],
                part_vid: GenerationId::new(1).unwrap(),
                ec_k: 4,
                ec_m: 2,
                last_modified: 1000,
                checksum: None,
            },
            &[MultipartPartSegmentRecord {
                bucket: "b".into(),
                key: "k".into(),
                upload_id: "mpu-cpc".into(),
                version_id: u64::MAX,
                part_number: 99, // wrong!
                segment_index: 0,
                size: 100,
                segment_crc64: None,
                segment_okh: [1; 16],
                segment_vid: GenerationId::new(1).unwrap(),
                shard_pg_id: 0,
                ec_k: 4,
                ec_m: 2,
            }],
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::error::MetadataError::StreamSessionNotFound { .. }
        ),
        "expected rejection for mismatched chunk part_number, got: {err:?}"
    );
}

#[test]
fn commit_stream_part_rejects_non_staging_chunk_version_id() {
    let (_dir, store) = make_pg_store();

    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: "mpu-vid".into(),
            bucket: "b".into(),
            key: "k".into(),
            metadata_blob: vec![],
            owner_principal: None,
            checksum: None,
        })
        .unwrap();

    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: "sp-vid".into(),
            bucket: "b".into(),
            key: "k".into(),
            target: StreamUploadTarget::UploadPart {
                upload_id: "mpu-vid".into(),
                part_number: 1,
            },
        })
        .unwrap();

    // Chunk has version_id=42 — must be PART_CHUNK_STAGING_VERSION_ID (u64::MAX) pre-CompleteMultipartUpload
    let err = store
        .commit_stream_part(
            "sp-vid",
            &MultipartPartRecord {
                upload_id: "mpu-vid".into(),
                part_number: 1,
                generation: 0,
                size: 100,
                etag: vec![1],
                etag_kind: EtagKind::Crc64,
                part_okh: [0xAA; 16],
                part_vid: GenerationId::new(1).unwrap(),
                ec_k: 4,
                ec_m: 2,
                last_modified: 1000,
                checksum: None,
            },
            &[MultipartPartSegmentRecord {
                bucket: "b".into(),
                key: "k".into(),
                upload_id: "mpu-vid".into(),
                version_id: 42, // wrong — must be u64::MAX (staging sentinel)
                part_number: 1,
                segment_index: 0,
                size: 100,
                segment_crc64: None,
                segment_okh: [1; 16],
                segment_vid: GenerationId::new(1).unwrap(),
                shard_pg_id: 0,
                ec_k: 4,
                ec_m: 2,
            }],
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::error::MetadataError::StreamSessionNotFound { .. }
        ),
        "expected rejection for non-staging chunk version_id, got: {err:?}"
    );
}

#[test]
fn malformed_segment_okh_returns_db_error() {
    let (_dir, store) = make_pg_store();

    // Insert a chunk with wrong-length okh directly via SQL
    let conn = store.connection();
    conn.execute(
        "INSERT INTO object_segments \
         (bucket, key, version_id, segment_index, size, segment_okh, segment_vid, \
          shard_pg_id, ec_k, ec_m) \
         VALUES ('b', 'k', 0, 0, 100, X'AABB', 1, 0, 4, 2)",
        [],
    )
    .unwrap();

    let err = store
        .get_object_segments("b", "k", VersionId::Null)
        .unwrap_err();
    assert!(
        matches!(err, crate::error::MetadataError::Db { .. }),
        "expected Db error for malformed okh, got: {err:?}"
    );
}

#[test]
fn malformed_multipart_segment_okh_returns_db_error() {
    let (_dir, store) = make_pg_store();

    // Insert a multipart part segment with wrong-length okh directly via SQL
    let conn = store.connection();
    conn.execute(
        "INSERT INTO multipart_part_segments \
         (bucket, key, upload_id, version_id, part_number, segment_index, size, segment_okh, \
          segment_vid, shard_pg_id, ec_k, ec_m) \
         VALUES ('b', 'k', 'mpu-bad', 0, 1, 0, 100, X'AABB', 1, 0, 4, 2)",
        [],
    )
    .unwrap();

    let err = store
        .get_all_multipart_part_segments_for_upload("mpu-bad")
        .unwrap_err();
    assert!(
        matches!(err, crate::error::MetadataError::Db { .. }),
        "expected Db error for malformed multipart segment okh, got: {err:?}"
    );
}
