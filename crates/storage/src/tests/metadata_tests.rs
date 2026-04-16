use super::{bucket_name, multipart_upload_id, object_key, stream_session_id};
use crate::traits::{PgMetadataStore, ShardStore};
use crate::types::*;
use std::num::NonZeroU64;

fn test_owner() -> OwnerIdentity {
    OwnerIdentity::from_principal("owner")
}

fn owner_identity(principal: &str) -> OwnerIdentity {
    OwnerIdentity::from_principal(principal)
}

fn sample_bucket_object_lock() -> BucketObjectLockConfig {
    BucketObjectLockConfig {
        enabled: true,
        default_retention: Some(ObjectLockDefaultRetention {
            mode: ObjectLockMode::Compliance,
            period: RetentionPeriod::days(30).unwrap(),
        }),
    }
}

fn sample_object_lock_state() -> ObjectLockState {
    ObjectLockState {
        retention: Some(ObjectRetention {
            retain_until_unix_seconds: 1_900_000_000,
            mode: ObjectLockMode::Governance,
        }),
        legal_hold: StoredLegalHoldStatus::On,
    }
}

/// Run the common metadata test suite against any PgMetadataStore implementation.
fn metadata_put_get_delete(store: &dyn PgMetadataStore) {
    let req = PutObjectReq::Live(PutLiveObjectReq {
        bucket: bucket_name("test-bucket"),
        key: object_key("test-key"),
        version_id: VersionId::Null,
        owner: test_owner(),
        acl_grants: AclGrants::default(),
        public_read: false,
        generation_id: GenerationId::MIN,
        ec: EcShape { k: 4, m: 2 },
        size: 1024,
        etag: ObjectEtag::SinglePart([0xAB, 0xCD, 0, 0, 0, 0, 0, 0]),
        layout: ObjectLayout::Standard,
        tags: None,
        metadata_blob: None,
        system_metadata_blob: None,
        object_lock: ObjectLockState::default(),
        encryption: ObjectEncryption::None,
    });

    // Put
    store.put_object_meta(&req).unwrap();

    // Get
    let obj = store
        .get_object_meta(&bucket_name("test-bucket"), &object_key("test-key"))
        .unwrap();
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
    assert_eq!(live.owner, test_owner());
    assert_eq!(live.ec.k, 4);
    assert_eq!(live.ec.m, 2);
    assert!(!obj.is_delete_marker());
    assert!(obj.last_modified() > 0);

    // Delete
    store
        .delete_object_meta(&bucket_name("test-bucket"), &object_key("test-key"))
        .unwrap();

    let err = store
        .get_object_meta(&bucket_name("test-bucket"), &object_key("test-key"))
        .unwrap_err();
    assert!(matches!(err, crate::MetadataError::ObjectNotFound));
}

fn metadata_put_overwrites(store: &dyn PgMetadataStore) {
    let req1 = PutObjectReq::Live(PutLiveObjectReq {
        bucket: bucket_name("bucket"),
        key: object_key("k"),
        version_id: VersionId::Null,
        owner: test_owner(),
        acl_grants: AclGrants::default(),
        public_read: false,
        generation_id: GenerationId::MIN,
        ec: EcShape { k: 4, m: 2 },
        size: 100,
        etag: ObjectEtag::SinglePart([1, 0, 0, 0, 0, 0, 0, 0]),
        layout: ObjectLayout::Standard,
        tags: None,
        metadata_blob: None,
        system_metadata_blob: None,
        object_lock: ObjectLockState::default(),
        encryption: ObjectEncryption::None,
    });
    store.put_object_meta(&req1).unwrap();

    let req2 = PutObjectReq::Live(PutLiveObjectReq {
        bucket: bucket_name("bucket"),
        key: object_key("k"),
        version_id: VersionId::Null,
        owner: test_owner(),
        acl_grants: AclGrants::default(),
        public_read: false,
        generation_id: GenerationId::MIN,
        ec: EcShape { k: 4, m: 2 },
        size: 200,
        etag: ObjectEtag::SinglePart([2, 0, 0, 0, 0, 0, 0, 0]),
        layout: ObjectLayout::Standard,
        tags: None,
        metadata_blob: None,
        system_metadata_blob: None,
        object_lock: ObjectLockState::default(),
        encryption: ObjectEncryption::None,
    });
    store.put_object_meta(&req2).unwrap();

    let obj = store
        .get_object_meta(&bucket_name("bucket"), &object_key("k"))
        .unwrap();
    let live = obj.as_live().unwrap();
    assert_eq!(live.size, 200);
    assert_eq!(live.etag, ObjectEtag::SinglePart([2, 0, 0, 0, 0, 0, 0, 0]));
}

fn metadata_get_nonexistent(store: &dyn PgMetadataStore) {
    let err = store
        .get_object_meta(&bucket_name("no-bucket"), &object_key("no-key"))
        .unwrap_err();
    assert!(matches!(err, crate::MetadataError::ObjectNotFound));
}

fn metadata_list_basic(store: &dyn PgMetadataStore) {
    for i in 0..5 {
        let req = PutObjectReq::Live(PutLiveObjectReq {
            bucket: bucket_name("list-bucket"),
            key: object_key(format!("obj-{i:02}")),
            version_id: VersionId::Null,
            owner: test_owner(),
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id: GenerationId::MIN,
            size: i * 100,
            etag: ObjectEtag::SinglePart([i as u8, 0, 0, 0, 0, 0, 0, 0]),
            ec: EcShape { k: 4, m: 2 },
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: None,
            system_metadata_blob: None,
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
        });
        store.put_object_meta(&req).unwrap();
    }

    let resp = store
        .list_objects(&ListObjectsReq {
            bucket: bucket_name("list-bucket"),
            prefix: None,
            start_after: None,
            start_at: None,
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
            bucket: bucket_name("prefix-bucket"),
            key: object_key(*key),
            version_id: VersionId::Null,
            owner: test_owner(),
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id: GenerationId::MIN,
            ec: EcShape { k: 4, m: 2 },
            size: 100,
            etag: ObjectEtag::SinglePart([0; 8]),
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: None,
            system_metadata_blob: None,
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
        });
        store.put_object_meta(&req).unwrap();
    }

    let resp = store
        .list_objects(&ListObjectsReq {
            bucket: bucket_name("prefix-bucket"),
            prefix: Some(object_key("photos/")),
            start_after: None,
            start_at: None,
            max_keys: 100,
        })
        .unwrap();

    assert_eq!(resp.objects.len(), 3);
    assert!(resp
        .objects
        .iter()
        .all(|o| o.key().as_str().starts_with("photos/")));
}

fn metadata_list_pagination(store: &dyn PgMetadataStore) {
    for i in 0..10 {
        let req = PutObjectReq::Live(PutLiveObjectReq {
            bucket: bucket_name("page-bucket"),
            key: object_key(format!("item-{i:02}")),
            version_id: VersionId::Null,
            owner: test_owner(),
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id: GenerationId::MIN,
            size: 0,
            etag: ObjectEtag::SinglePart([0; 8]),
            ec: EcShape { k: 4, m: 2 },
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: None,
            system_metadata_blob: None,
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
        });
        store.put_object_meta(&req).unwrap();
    }

    // First page: 3 items.
    let resp1 = store
        .list_objects(&ListObjectsReq {
            bucket: bucket_name("page-bucket"),
            prefix: None,
            start_after: None,
            start_at: None,
            max_keys: 3,
        })
        .unwrap();

    assert_eq!(resp1.objects.len(), 3);
    assert!(resp1.is_truncated);
    assert_eq!(resp1.objects[0].key(), "item-00");
    assert_eq!(resp1.objects[2].key(), "item-02");
    assert_eq!(resp1.next_start_after, Some(object_key("item-02")));

    // Second page.
    let resp2 = store
        .list_objects(&ListObjectsReq {
            bucket: bucket_name("page-bucket"),
            prefix: None,
            start_after: resp1.next_start_after,
            start_at: None,
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
                bucket: bucket_name("page-bucket"),
                prefix: None,
                start_after: start_after.clone(),
                start_at: None,
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
            bucket: bucket_name("empty-bucket"),
            prefix: None,
            start_after: None,
            start_at: None,
            max_keys: 100,
        })
        .unwrap();

    assert!(resp.objects.is_empty());
    assert!(!resp.is_truncated);
    assert!(resp.next_start_after.is_none());
}

fn metadata_empty_key(_store: &dyn PgMetadataStore) {
    let err = ObjectKey::try_from("").unwrap_err();
    assert!(matches!(err, ObjectKeyError::InvalidLength { length: 0 }));
}

fn metadata_long_key(store: &dyn PgMetadataStore) {
    // S3 allows keys up to 1024 bytes.
    let long_key = "x".repeat(1024);
    let req = PutObjectReq::Live(PutLiveObjectReq {
        bucket: bucket_name("bucket"),
        key: object_key(long_key.clone()),
        version_id: VersionId::Null,
        owner: test_owner(),
        acl_grants: AclGrants::default(),
        public_read: false,
        generation_id: GenerationId::MIN,
        ec: EcShape { k: 4, m: 2 },
        size: 0,
        etag: ObjectEtag::SinglePart([0; 8]),
        layout: ObjectLayout::Standard,
        tags: None,
        metadata_blob: None,
        system_metadata_blob: None,
        object_lock: ObjectLockState::default(),
        encryption: ObjectEncryption::None,
    });
    store.put_object_meta(&req).unwrap();

    let obj = store
        .get_object_meta(&bucket_name("bucket"), &object_key(&long_key))
        .unwrap();
    assert_eq!(*obj.key(), long_key);
}

fn metadata_zero_size_object(store: &dyn PgMetadataStore) {
    let req = PutObjectReq::Live(PutLiveObjectReq {
        bucket: bucket_name("bucket"),
        key: object_key("empty-obj"),
        version_id: VersionId::Null,
        owner: test_owner(),
        acl_grants: AclGrants::default(),
        public_read: false,
        generation_id: GenerationId::MIN,
        ec: EcShape { k: 4, m: 2 },
        size: 0,
        etag: ObjectEtag::SinglePart([0; 8]),
        layout: ObjectLayout::Standard,
        tags: None,
        metadata_blob: None,
        system_metadata_blob: None,
        object_lock: ObjectLockState::default(),
        encryption: ObjectEncryption::None,
    });
    store.put_object_meta(&req).unwrap();

    let obj = store
        .get_object_meta(&bucket_name("bucket"), &object_key("empty-obj"))
        .unwrap();
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
            &bucket_name("alpha"),
            "owner-1",
            &CanonicalUserId::from_principal("owner-1"),
            &AclGrants::default(),
            false,
            false,
        )
        .unwrap();
    store
        .create_bucket(
            &bucket_name("beta"),
            "owner-1",
            &CanonicalUserId::from_principal("owner-1"),
            &AclGrants::default(),
            true,
            false,
        )
        .unwrap();
    store
        .create_bucket(
            &bucket_name("gamma"),
            "owner-2",
            &CanonicalUserId::from_principal("owner-2"),
            &AclGrants::default(),
            false,
            false,
        )
        .unwrap();

    let err = store
        .create_bucket(
            &bucket_name("alpha"),
            "owner-1",
            &CanonicalUserId::from_principal("owner-1"),
            &AclGrants::default(),
            false,
            false,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::BucketAlreadyExists
    ));

    let beta = store.head_bucket(&bucket_name("beta")).unwrap();
    assert_eq!(beta.name, "beta");
    assert_eq!(beta.owner_principal, "owner-1");
    assert_eq!(
        beta.owner_canonical_id,
        CanonicalUserId::from_principal("owner-1")
    );
    assert!(beta.public_read);

    let owner1 = store
        .list_buckets(CanonicalUserId::from_principal("owner-1").as_str())
        .unwrap();
    assert_eq!(owner1.len(), 2);
    assert_eq!(owner1[0].name, "alpha");
    assert_eq!(owner1[1].name, "beta");

    store.delete_bucket(&bucket_name("alpha")).unwrap();
    let err = store.head_bucket(&bucket_name("alpha")).unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::BucketNotFound { .. }
    ));
}

#[test]
fn file_bucket_metadata_delete_nonexistent() {
    let (_dir, store) = make_pg_store();
    let err = store
        .delete_bucket(&bucket_name("no-such-bucket"))
        .unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::BucketNotFound { .. }
    ));
}

#[test]
fn delete_bucket_clears_completed_multipart_upload_records() {
    let (_dir, store) = make_pg_store();
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
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: multipart_upload_id("completed-upload"),
            bucket: bucket_name("bucket"),
            key: object_key("key"),
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

    let obj = CommitMultipartReq {
        bucket: bucket_name("bucket"),
        key: object_key("key"),
        version_id: VersionId::Null,
        owner: test_owner(),
        acl_grants: AclGrants::default(),
        public_read: false,
        generation_id: GenerationId::MIN,
        size: 1024,
        etag_crc64: [0xAA, 0, 0, 0, 0, 0, 0, 0],
        ec: EcShape { k: 4, m: 2 },
        tags: None,
        metadata_blob: Some(vec![].into()),
        system_metadata_blob: None,
        object_lock: ObjectLockState::default(),
        encryption: ObjectEncryption::None,
    };
    let parts = vec![ObjectPartRecord {
        bucket: bucket_name("bucket"),
        key: object_key("key"),
        version_id: VersionId::Null,
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
    }];
    store
        .complete_multipart_commit(&multipart_upload_id("completed-upload"), 1, &obj, &parts)
        .unwrap();

    assert!(store
        .get_completed_multipart_upload(&multipart_upload_id("completed-upload"))
        .unwrap()
        .is_some());

    store.delete_bucket(&bucket_name("bucket")).unwrap();

    assert!(store
        .get_completed_multipart_upload(&multipart_upload_id("completed-upload"))
        .unwrap()
        .is_none());
}

#[test]
fn file_bucket_metadata_versioning_transitions() {
    let (_dir, store) = make_pg_store();
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
    store
        .put_bucket_versioning(&bucket_name("bucket"), BucketVersioningState::Suspended)
        .unwrap();
    store
        .put_bucket_versioning(&bucket_name("bucket"), BucketVersioningState::Enabled)
        .unwrap();

    let err = store
        .put_bucket_versioning(&bucket_name("bucket"), BucketVersioningState::Disabled)
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
            &bucket_name("bucket"),
            "owner",
            &CanonicalUserId::from_principal("owner"),
            &AclGrants::default(),
            false,
            false,
        )
        .unwrap();
    store
        .put_bucket_versioning(&bucket_name("bucket"), BucketVersioningState::Disabled)
        .unwrap();
    assert_eq!(
        store
            .head_bucket(&bucket_name("bucket"))
            .unwrap()
            .versioning,
        BucketVersioningState::Disabled
    );
}

#[test]
fn file_bucket_metadata_config_roundtrip() {
    let (_dir, store) = make_pg_store();
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

    for kind in [BucketSubresourceKind::Cors, BucketSubresourceKind::Tagging] {
        let body = match kind {
            BucketSubresourceKind::Cors => "<Cors/>",
            BucketSubresourceKind::Tagging => "<Tagging/>",
            _ => unreachable!(),
        };
        store
            .put_bucket_subresource(
                &bucket_name("bucket"),
                PutBucketSubresource {
                    kind,
                    body,
                    aux: BucketSubresourceAux::None,
                },
            )
            .unwrap();
        assert_eq!(
            store
                .get_bucket_subresource(&bucket_name("bucket"), kind)
                .unwrap()
                .map(|stored| stored.body),
            Some(body.to_string())
        );
        store
            .delete_bucket_subresource(&bucket_name("bucket"), kind)
            .unwrap();
        assert_eq!(
            store
                .get_bucket_subresource(&bucket_name("bucket"), kind)
                .unwrap(),
            None
        );
    }

    let ownership_controls = BucketOwnershipControls {
        object_ownership: BucketObjectOwnership::BucketOwnerPreferred,
    };
    store
        .put_bucket_ownership_controls(&bucket_name("bucket"), ownership_controls)
        .unwrap();
    assert_eq!(
        store
            .get_bucket_ownership_controls(&bucket_name("bucket"))
            .unwrap(),
        Some(ownership_controls)
    );
    store
        .delete_bucket_ownership_controls(&bucket_name("bucket"))
        .unwrap();
    assert_eq!(
        store
            .get_bucket_ownership_controls(&bucket_name("bucket"))
            .unwrap(),
        None
    );

    store
        .put_bucket_subresource(
            &bucket_name("bucket"),
            PutBucketSubresource {
                kind: BucketSubresourceKind::Policy,
                body: "{\"Version\":\"2012-10-17\"}",
                aux: BucketSubresourceAux::policy(true),
            },
        )
        .unwrap();
    assert_eq!(
        store
            .get_bucket_subresource(&bucket_name("bucket"), BucketSubresourceKind::Policy)
            .unwrap()
            .map(|stored| stored.body),
        Some("{\"Version\":\"2012-10-17\"}".to_string())
    );
    assert!(
        store
            .head_bucket(&bucket_name("bucket"))
            .unwrap()
            .bucket_policy_present
    );
    assert!(
        store
            .head_bucket(&bucket_name("bucket"))
            .unwrap()
            .bucket_policy_public
    );
    assert_eq!(
        store
            .head_bucket(&bucket_name("bucket"))
            .unwrap()
            .bucket_policy_generation,
        1
    );
    store
        .delete_bucket_subresource(&bucket_name("bucket"), BucketSubresourceKind::Policy)
        .unwrap();
    assert_eq!(
        store
            .get_bucket_subresource(&bucket_name("bucket"), BucketSubresourceKind::Policy)
            .unwrap(),
        None
    );
    assert!(
        !store
            .head_bucket(&bucket_name("bucket"))
            .unwrap()
            .bucket_policy_present
    );
    assert!(
        !store
            .head_bucket(&bucket_name("bucket"))
            .unwrap()
            .bucket_policy_public
    );
    assert_eq!(
        store
            .head_bucket(&bucket_name("bucket"))
            .unwrap()
            .bucket_policy_generation,
        2
    );

    store
        .put_bucket_subresource(
            &bucket_name("bucket"),
            PutBucketSubresource {
                kind: BucketSubresourceKind::Lifecycle,
                body: "<LifecycleConfiguration/>",
                aux: BucketSubresourceAux::None,
            },
        )
        .unwrap();
    assert_eq!(
        store
            .get_bucket_subresource(&bucket_name("bucket"), BucketSubresourceKind::Lifecycle)
            .unwrap()
            .map(|stored| stored.body),
        Some("<LifecycleConfiguration/>".to_string())
    );
    assert!(
        store
            .head_bucket(&bucket_name("bucket"))
            .unwrap()
            .bucket_lifecycle_present
    );
    assert_eq!(
        store
            .head_bucket(&bucket_name("bucket"))
            .unwrap()
            .bucket_lifecycle_generation,
        1
    );
    store
        .put_bucket_subresource(
            &bucket_name("bucket"),
            PutBucketSubresource {
                kind: BucketSubresourceKind::Lifecycle,
                body: "<LifecycleConfiguration><Rule/></LifecycleConfiguration>",
                aux: BucketSubresourceAux::None,
            },
        )
        .unwrap();
    assert_eq!(
        store
            .get_bucket_subresource(&bucket_name("bucket"), BucketSubresourceKind::Lifecycle)
            .unwrap()
            .map(|stored| stored.body),
        Some("<LifecycleConfiguration><Rule/></LifecycleConfiguration>".to_string())
    );
    assert_eq!(
        store
            .head_bucket(&bucket_name("bucket"))
            .unwrap()
            .bucket_lifecycle_generation,
        2
    );
    store
        .delete_bucket_subresource(&bucket_name("bucket"), BucketSubresourceKind::Lifecycle)
        .unwrap();
    assert_eq!(
        store
            .get_bucket_subresource(&bucket_name("bucket"), BucketSubresourceKind::Lifecycle)
            .unwrap(),
        None
    );
    assert!(
        !store
            .head_bucket(&bucket_name("bucket"))
            .unwrap()
            .bucket_lifecycle_present
    );
    assert_eq!(
        store
            .head_bucket(&bucket_name("bucket"))
            .unwrap()
            .bucket_lifecycle_generation,
        3
    );

    let lifecycle_buckets = store.list_buckets_with_lifecycle().unwrap();
    assert!(lifecycle_buckets.is_empty());

    assert_eq!(
        store.get_bucket_encryption(&bucket_name("bucket")).unwrap(),
        BucketEncryptionConfig::default()
    );

    store
        .put_bucket_encryption(
            &bucket_name("bucket"),
            BucketEncryptionConfig {
                default_encryption: None,
                sse_c_blocked: true,
            },
        )
        .unwrap();
    assert_eq!(
        store.get_bucket_encryption(&bucket_name("bucket")).unwrap(),
        BucketEncryptionConfig {
            default_encryption: None,
            sse_c_blocked: true,
        }
    );
    assert!(
        store
            .head_bucket(&bucket_name("bucket"))
            .unwrap()
            .encryption
            .sse_c_blocked
    );
    store
        .put_bucket_encryption(&bucket_name("bucket"), BucketEncryptionConfig::default())
        .unwrap();
    assert_eq!(
        store.get_bucket_encryption(&bucket_name("bucket")).unwrap(),
        BucketEncryptionConfig::default()
    );

    store
        .put_bucket_acl(&bucket_name("bucket"), &AclGrants::default(), true, false)
        .unwrap();
    assert!(
        store
            .head_bucket(&bucket_name("bucket"))
            .unwrap()
            .public_read
    );
    store
        .put_bucket_acl(&bucket_name("bucket"), &AclGrants::default(), false, false)
        .unwrap();
    assert!(
        !store
            .head_bucket(&bucket_name("bucket"))
            .unwrap()
            .public_read
    );
}

#[test]
fn file_bucket_subresource_roundtrip() {
    let (_dir, store) = make_pg_store();
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
        .put_bucket_subresource(
            &bucket_name("bucket"),
            PutBucketSubresource {
                kind: BucketSubresourceKind::Cors,
                body: "<Cors/>",
                aux: BucketSubresourceAux::None,
            },
        )
        .unwrap();
    assert_eq!(
        store
            .get_bucket_subresource(&bucket_name("bucket"), BucketSubresourceKind::Cors)
            .unwrap(),
        Some(StoredBucketSubresource {
            body: "<Cors/>".to_string(),
            generation: Some(1),
            aux: BucketSubresourceAux::None,
        })
    );
    store
        .delete_bucket_subresource(&bucket_name("bucket"), BucketSubresourceKind::Cors)
        .unwrap();
    assert_eq!(
        store
            .get_bucket_subresource(&bucket_name("bucket"), BucketSubresourceKind::Cors)
            .unwrap(),
        None
    );

    store
        .put_bucket_subresource(
            &bucket_name("bucket"),
            PutBucketSubresource {
                kind: BucketSubresourceKind::Tagging,
                body: "<Tagging/>",
                aux: BucketSubresourceAux::None,
            },
        )
        .unwrap();
    assert_eq!(
        store
            .get_bucket_subresource(&bucket_name("bucket"), BucketSubresourceKind::Tagging)
            .unwrap(),
        Some(StoredBucketSubresource {
            body: "<Tagging/>".to_string(),
            generation: Some(1),
            aux: BucketSubresourceAux::None,
        })
    );

    let ownership_controls = BucketOwnershipControls {
        object_ownership: BucketObjectOwnership::BucketOwnerEnforced,
    };
    store
        .put_bucket_ownership_controls(&bucket_name("bucket"), ownership_controls)
        .unwrap();
    assert_eq!(
        store
            .get_bucket_ownership_controls(&bucket_name("bucket"))
            .unwrap(),
        Some(ownership_controls)
    );

    let public_access_block = PublicAccessBlockConfig {
        block_public_acls: true,
        ignore_public_acls: false,
        block_public_policy: true,
        restrict_public_buckets: false,
    };
    store
        .put_bucket_public_access_block(&bucket_name("bucket"), public_access_block)
        .unwrap();
    assert_eq!(
        store
            .get_bucket_public_access_block(&bucket_name("bucket"))
            .unwrap(),
        Some(public_access_block)
    );
    store
        .delete_bucket_public_access_block(&bucket_name("bucket"))
        .unwrap();
    assert_eq!(
        store
            .get_bucket_public_access_block(&bucket_name("bucket"))
            .unwrap(),
        None
    );

    store
        .put_bucket_subresource(
            &bucket_name("bucket"),
            PutBucketSubresource {
                kind: BucketSubresourceKind::Policy,
                body: "{\"Version\":\"2012-10-17\"}",
                aux: BucketSubresourceAux::policy(true),
            },
        )
        .unwrap();
    assert_eq!(
        store
            .get_bucket_subresource(&bucket_name("bucket"), BucketSubresourceKind::Policy)
            .unwrap(),
        Some(StoredBucketSubresource {
            body: "{\"Version\":\"2012-10-17\"}".to_string(),
            generation: Some(1),
            aux: BucketSubresourceAux::policy(true),
        })
    );

    store
        .put_bucket_subresource(
            &bucket_name("bucket"),
            PutBucketSubresource {
                kind: BucketSubresourceKind::Lifecycle,
                body: "<LifecycleConfiguration/>",
                aux: BucketSubresourceAux::None,
            },
        )
        .unwrap();
    assert_eq!(
        store
            .get_bucket_subresource(&bucket_name("bucket"), BucketSubresourceKind::Lifecycle)
            .unwrap(),
        Some(StoredBucketSubresource {
            body: "<LifecycleConfiguration/>".to_string(),
            generation: Some(1),
            aux: BucketSubresourceAux::None,
        })
    );
    store
        .delete_bucket_subresource(&bucket_name("bucket"), BucketSubresourceKind::Lifecycle)
        .unwrap();
    assert_eq!(
        store
            .get_bucket_subresource(&bucket_name("bucket"), BucketSubresourceKind::Lifecycle)
            .unwrap(),
        None
    );
    assert_eq!(
        store
            .head_bucket(&bucket_name("bucket"))
            .unwrap()
            .bucket_lifecycle_generation,
        2
    );
}

#[test]
fn file_bucket_subresource_rejects_aux_kind_mismatch() {
    let (_dir, store) = make_pg_store();
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

    let err = store
        .put_bucket_subresource(
            &bucket_name("bucket"),
            PutBucketSubresource {
                kind: BucketSubresourceKind::Cors,
                body: "<Cors/>",
                aux: BucketSubresourceAux::policy(true),
            },
        )
        .unwrap_err();
    assert!(matches!(err, crate::error::MetadataError::Db { .. }));

    let err = store
        .put_bucket_subresource(
            &bucket_name("bucket"),
            PutBucketSubresource {
                kind: BucketSubresourceKind::Policy,
                body: "{\"Version\":\"2012-10-17\"}",
                aux: BucketSubresourceAux::None,
            },
        )
        .unwrap_err();
    assert!(matches!(err, crate::error::MetadataError::Db { .. }));
}

#[test]
fn file_bucket_subresource_generations_bump_across_overwrite_and_delete() {
    let (_dir, store) = make_pg_store();
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
        .put_bucket_subresource(
            &bucket_name("bucket"),
            PutBucketSubresource {
                kind: BucketSubresourceKind::Cors,
                body: "<Cors/>",
                aux: BucketSubresourceAux::None,
            },
        )
        .unwrap();
    assert_eq!(
        store
            .get_bucket_subresource(&bucket_name("bucket"), BucketSubresourceKind::Cors)
            .unwrap()
            .unwrap()
            .generation,
        Some(1)
    );

    store
        .put_bucket_subresource(
            &bucket_name("bucket"),
            PutBucketSubresource {
                kind: BucketSubresourceKind::Cors,
                body: "<Cors><Rule/></Cors>",
                aux: BucketSubresourceAux::None,
            },
        )
        .unwrap();
    assert_eq!(
        store
            .get_bucket_subresource(&bucket_name("bucket"), BucketSubresourceKind::Cors)
            .unwrap()
            .unwrap(),
        StoredBucketSubresource {
            body: "<Cors><Rule/></Cors>".to_string(),
            generation: Some(2),
            aux: BucketSubresourceAux::None,
        }
    );

    store
        .delete_bucket_subresource(&bucket_name("bucket"), BucketSubresourceKind::Cors)
        .unwrap();
    assert_eq!(
        store
            .get_bucket_subresource(&bucket_name("bucket"), BucketSubresourceKind::Cors)
            .unwrap(),
        None
    );

    store
        .put_bucket_subresource(
            &bucket_name("bucket"),
            PutBucketSubresource {
                kind: BucketSubresourceKind::Cors,
                body: "<Cors><Rule>again</Rule></Cors>",
                aux: BucketSubresourceAux::None,
            },
        )
        .unwrap();
    assert_eq!(
        store
            .get_bucket_subresource(&bucket_name("bucket"), BucketSubresourceKind::Cors)
            .unwrap()
            .unwrap(),
        StoredBucketSubresource {
            body: "<Cors><Rule>again</Rule></Cors>".to_string(),
            generation: Some(4),
            aux: BucketSubresourceAux::None,
        }
    );
}

#[test]
fn delete_bucket_cascades_bucket_subresources() {
    let (_dir, store) = make_pg_store();
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
        .put_bucket_subresource(
            &bucket_name("bucket"),
            PutBucketSubresource {
                kind: BucketSubresourceKind::Policy,
                body: "{\"Version\":\"2012-10-17\"}",
                aux: BucketSubresourceAux::policy(true),
            },
        )
        .unwrap();
    assert!(store
        .get_bucket_subresource(&bucket_name("bucket"), BucketSubresourceKind::Policy)
        .unwrap()
        .is_some());

    store.delete_bucket(&bucket_name("bucket")).unwrap();

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
    assert_eq!(
        store
            .get_bucket_subresource(&bucket_name("bucket"), BucketSubresourceKind::Policy)
            .unwrap(),
        None
    );
}

#[test]
fn list_buckets_with_lifecycle_returns_only_active_lifecycle_buckets() {
    let (_dir, store) = make_pg_store();
    for bucket in ["alpha", "beta", "gamma"] {
        store
            .create_bucket(
                &bucket_name(bucket),
                "owner",
                &CanonicalUserId::from_principal("owner"),
                &AclGrants::default(),
                false,
                false,
            )
            .unwrap();
    }

    for (bucket, body) in [
        ("alpha", "<LifecycleConfiguration/>"),
        (
            "beta",
            "<LifecycleConfiguration><Rule/></LifecycleConfiguration>",
        ),
    ] {
        store
            .put_bucket_subresource(
                &bucket_name(bucket),
                PutBucketSubresource {
                    kind: BucketSubresourceKind::Lifecycle,
                    body,
                    aux: BucketSubresourceAux::None,
                },
            )
            .unwrap();
    }
    store
        .begin_bucket_write_drain(&bucket_name("beta"))
        .unwrap();
    store.mark_bucket_deleting(&bucket_name("beta")).unwrap();

    let buckets = store.list_buckets_with_lifecycle().unwrap();
    let names: Vec<String> = buckets
        .into_iter()
        .map(|bucket| bucket.name.to_string())
        .collect();
    assert_eq!(names, vec!["alpha"]);
}

#[test]
fn list_buckets_with_aborting_multipart_uploads_returns_distinct_bucket_names() {
    let (_dir, store) = make_pg_store();
    for bucket in ["alpha", "beta"] {
        store
            .create_bucket(
                &bucket_name(bucket),
                "owner",
                &CanonicalUserId::from_principal("owner"),
                &AclGrants::default(),
                false,
                false,
            )
            .unwrap();
    }

    for (upload_id, bucket, key) in [
        ("upload-a1", "alpha", "key-1"),
        ("upload-a2", "alpha", "key-2"),
        ("upload-b1", "beta", "key-1"),
    ] {
        store
            .create_multipart_upload(&CreateMultipartUploadReq {
                upload_id: multipart_upload_id(upload_id),
                bucket: bucket_name(bucket),
                key: object_key(key),
                owner: test_owner(),
                initiator: None,
                tags: None,
                metadata_blob: SerializedMetadataBlob::from(vec![]),
                system_metadata_blob: SerializedSystemMetadataBlob::from(vec![]),
                acl_grants: AclGrants::default(),
                public_read: false,
                object_lock: ObjectLockState::default(),
                checksum: None,
                encryption: ObjectEncryption::None,
            })
            .unwrap();
    }
    store
        .set_upload_state(&multipart_upload_id("upload-a1"), UploadState::Aborting)
        .unwrap();
    store
        .set_upload_state(&multipart_upload_id("upload-a2"), UploadState::Aborting)
        .unwrap();
    store
        .set_upload_state(&multipart_upload_id("upload-b1"), UploadState::Completing)
        .unwrap();

    let buckets = store
        .list_buckets_with_aborting_multipart_uploads()
        .unwrap();
    assert_eq!(buckets, vec![bucket_name("alpha")]);
}

#[test]
fn file_bucket_subresource_operations_on_nonexistent_bucket() {
    let (_dir, store) = make_pg_store();

    let err = store
        .put_bucket_subresource(
            &bucket_name("nope"),
            PutBucketSubresource {
                kind: BucketSubresourceKind::Cors,
                body: "<Cors/>",
                aux: BucketSubresourceAux::None,
            },
        )
        .unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::BucketNotFound { .. }
    ));

    let err = store
        .get_bucket_subresource(&bucket_name("nope"), BucketSubresourceKind::Tagging)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::BucketNotFound { .. }
    ));

    let err = store
        .get_bucket_subresource(&bucket_name("nope"), BucketSubresourceKind::Policy)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::BucketNotFound { .. }
    ));

    let err = store
        .delete_bucket_subresource(&bucket_name("nope"), BucketSubresourceKind::Lifecycle)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::BucketNotFound { .. }
    ));

    let err = store
        .put_bucket_encryption(
            &bucket_name("nope"),
            BucketEncryptionConfig {
                default_encryption: None,
                sse_c_blocked: true,
            },
        )
        .unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::BucketNotFound { .. }
    ));

    let err = store
        .delete_bucket_public_access_block(&bucket_name("nope"))
        .unwrap_err();
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
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            version_id: VersionId::Null,
            owner: test_owner(),
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id: GenerationId::MIN,
            ec: EcShape { k: 4, m: 2 },
            size: 100,
            etag: ObjectEtag::SinglePart([1, 2, 3, 0, 0, 0, 0, 0]),
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: None,
            system_metadata_blob: None,
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
        }))
        .unwrap();

    let obj = store
        .get_object_meta(&bucket_name("bucket"), &object_key("k"))
        .unwrap();
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
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            version_id: VersionId::Null,
            owner: test_owner(),
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id: GenerationId::MIN,
            ec: EcShape { k: 4, m: 2 },
            size: 100,
            etag: ObjectEtag::SinglePart([1, 2, 3, 0, 0, 0, 0, 0]),
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: None,
            system_metadata_blob: None,
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
        }))
        .unwrap();

    // Corrupting data_layout is blocked by DB CHECK constraints.
    let err = store
        .connection()
        .execute(
            "UPDATE objects SET data_layout = 99 WHERE bucket = 'bucket' AND key = 'k'",
            [],
        )
        .unwrap_err();
    assert!(
        matches!(err, rusqlite::Error::SqliteFailure(_, _)),
        "expected sqlite constraint failure, got: {err:?}"
    );

    // Record remains readable and unchanged.
    let obj = store
        .get_object_meta(&bucket_name("bucket"), &object_key("k"))
        .unwrap();
    assert_eq!(obj.as_live().unwrap().layout, ObjectLayout::Standard);
}

// --- Multipart metadata tests (PgStore only — needs SQL) ---

#[test]
fn mpu_create_and_get_upload() {
    let (_dir, store) = make_pg_store();
    let tags = "<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>";
    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: multipart_upload_id("uid-1"),
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            tags: Some(tags.into()),
            metadata_blob: vec![1, 2, 3].into(),
            system_metadata_blob: SerializedSystemMetadataBlob::default(),
            initiator: Some(owner_identity("alice")),

            owner: owner_identity("alice"),
            acl_grants: AclGrants::default(),
            public_read: false,
            object_lock: ObjectLockState::default(),
            checksum: None,
            encryption: ObjectEncryption::None,
        })
        .unwrap();

    let rec = store
        .get_multipart_upload(&multipart_upload_id("uid-1"))
        .unwrap();
    assert_eq!(rec.upload_id, multipart_upload_id("uid-1"));
    assert_eq!(rec.bucket, "bucket");
    assert_eq!(rec.key, "k");
    assert_eq!(rec.state, UploadState::InProgress);
    assert_eq!(rec.tags.as_deref(), Some(tags));
    assert_eq!(rec.metadata_blob, vec![1, 2, 3].into());
    assert_eq!(rec.initiator, Some(owner_identity("alice")));
    assert_eq!(rec.owner, owner_identity("alice"));
    assert!(rec.initiated_at > 0);
}

#[test]
fn mpu_create_upload_with_checksum_fields() {
    let (_dir, store) = make_pg_store();
    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: multipart_upload_id("uid-cksum"),
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
            checksum: Some(
                MultipartChecksumConfig::new(
                    ChecksumAlgorithm::Sha256,
                    Some(ChecksumType::Composite),
                )
                .unwrap(),
            ),
            encryption: ObjectEncryption::None,
        })
        .unwrap();

    let rec = store
        .get_multipart_upload(&multipart_upload_id("uid-cksum"))
        .unwrap();
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
            upload_id: multipart_upload_id("uid-no-cksum"),
            bucket: bucket_name("bucket"),
            key: object_key("k2"),
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
    let rec2 = store
        .get_multipart_upload(&multipart_upload_id("uid-no-cksum"))
        .unwrap();
    assert_eq!(rec2.checksum, None);
}

#[test]
fn mpu_part_checksum_round_trip() {
    let (_dir, store) = make_pg_store();
    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: multipart_upload_id("uid-pc"),
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
            checksum: Some(
                MultipartChecksumConfig::new(
                    ChecksumAlgorithm::Crc32,
                    Some(ChecksumType::FullObject),
                )
                .unwrap(),
            ),
            encryption: ObjectEncryption::None,
        })
        .unwrap();

    let checksum_bytes = ChecksumBytes::new([0xDE, 0xAD, 0xBE, 0xEF]).unwrap();
    store
        .upsert_multipart_part(&MultipartPartRecord {
            upload_id: multipart_upload_id("uid-pc"),
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

    let part = store
        .get_multipart_part(&multipart_upload_id("uid-pc"), 1)
        .unwrap();
    assert_eq!(part.checksum, Some(checksum_bytes));

    // None checksum round-trips
    store
        .upsert_multipart_part(&MultipartPartRecord {
            upload_id: multipart_upload_id("uid-pc"),
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

    let part2 = store
        .get_multipart_part(&multipart_upload_id("uid-pc"), 2)
        .unwrap();
    assert_eq!(part2.checksum, None);
}

#[test]
fn mpu_object_part_checksum_round_trip() {
    let (_dir, store) = make_pg_store();

    let checksum_bytes = ChecksumBytes::new([0x01, 0x02, 0x03, 0x04]).unwrap();
    store
        .commit_object_parts(&[
            ObjectPartRecord {
                bucket: bucket_name("bucket"),
                key: object_key("k"),
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
                bucket: bucket_name("bucket"),
                key: object_key("k"),
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
        .get_object_parts(
            &bucket_name("bucket"),
            &object_key("k"),
            VersionId::from_u64(1),
        )
        .unwrap();
    assert_eq!(committed.len(), 2);
    assert_eq!(committed[0].checksum, Some(checksum_bytes));
    assert_eq!(committed[1].checksum, None);
}

#[test]
fn mpu_complete_multipart_commit_preserves_checksums() {
    let (_dir, store) = make_pg_store();
    let tags = "<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>";

    // Create upload and object row (needed for complete_multipart_commit).
    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: multipart_upload_id("uid-cmc"),
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            tags: Some(tags.into()),
            metadata_blob: vec![].into(),
            system_metadata_blob: SerializedSystemMetadataBlob::default(),
            initiator: None,

            owner: test_owner(),
            acl_grants: AclGrants::default(),
            public_read: false,
            object_lock: ObjectLockState::default(),
            checksum: Some(
                MultipartChecksumConfig::new(
                    ChecksumAlgorithm::Sha256,
                    Some(ChecksumType::Composite),
                )
                .unwrap(),
            ),
            encryption: ObjectEncryption::None,
        })
        .unwrap();

    let obj = CommitMultipartReq {
        bucket: bucket_name("bucket"),
        key: object_key("k"),
        version_id: VersionId::Null,
        owner: test_owner(),
        acl_grants: AclGrants::default(),
        public_read: false,
        generation_id: GenerationId::MIN,
        size: 6 * 1024 * 1024,
        etag_crc64: [0xCC, 0, 0, 0, 0, 0, 0, 0],
        ec: EcShape { k: 4, m: 2 },
        tags: Some(tags.into()),
        metadata_blob: Some(vec![].into()),
        system_metadata_blob: None,
        object_lock: ObjectLockState::default(),
        encryption: ObjectEncryption::None,
    };

    let cksum = ChecksumBytes::new([0xDE, 0xAD]).unwrap();
    let parts = vec![
        ObjectPartRecord {
            bucket: bucket_name("bucket"),
            key: object_key("k"),
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
            bucket: bucket_name("bucket"),
            key: object_key("k"),
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
        .complete_multipart_commit(&multipart_upload_id("uid-cmc"), 1, &obj, &parts)
        .unwrap();

    let committed = store
        .get_object_parts(&bucket_name("bucket"), &object_key("k"), VersionId::Null)
        .unwrap();
    assert_eq!(committed.len(), 2);
    assert_eq!(committed[0].checksum, Some(cksum));
    assert_eq!(committed[1].checksum, None);
    let live = store
        .get_object_meta(&bucket_name("bucket"), &object_key("k"))
        .unwrap()
        .into_live()
        .unwrap();
    assert_eq!(live.tags.as_deref(), Some(tags));

    let completed = store
        .get_completed_multipart_upload(&multipart_upload_id("uid-cmc"))
        .unwrap()
        .expect("completed upload record");
    assert_eq!(completed.bucket.as_str(), "bucket");
    assert_eq!(completed.key.as_str(), "k");
    assert_eq!(completed.owner.principal, "owner");
}

#[test]
fn completed_multipart_tombstone_survives_null_version_overwrite() {
    let (_dir, store) = make_pg_store();
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

    let upload = CreateMultipartUploadReq {
        upload_id: multipart_upload_id("upload-1"),
        bucket: bucket_name("bucket"),
        key: object_key("key"),
        tags: None,
        metadata_blob: vec![].into(),
        system_metadata_blob: vec![].into(),
        initiator: Some(test_owner()),
        owner: test_owner(),
        acl_grants: AclGrants::default(),
        public_read: false,
        object_lock: ObjectLockState::default(),
        checksum: None,
        encryption: ObjectEncryption::None,
    };
    store.create_multipart_upload(&upload).unwrap();

    let obj = CommitMultipartReq {
        bucket: bucket_name("bucket"),
        key: object_key("key"),
        version_id: VersionId::Null,
        owner: test_owner(),
        acl_grants: AclGrants::default(),
        public_read: false,
        generation_id: GenerationId::MIN,
        size: 32,
        etag_crc64: [7, 0, 0, 0, 0, 0, 0, 0],
        ec: EcShape { k: 4, m: 2 },
        tags: None,
        metadata_blob: None,
        system_metadata_blob: None,
        object_lock: ObjectLockState::default(),
        encryption: ObjectEncryption::None,
    };
    let parts = vec![ObjectPartRecord {
        bucket: bucket_name("bucket"),
        key: object_key("key"),
        version_id: VersionId::Null,
        part_number: 1,
        size: 32,
        etag: vec![7; 8],
        etag_kind: EtagKind::Crc64,
        part_okh: [3; 16],
        part_vid: GenerationId::MIN,
        ec_k: 4,
        ec_m: 2,
        shard_pg_id: 0,
        checksum: None,
    }];
    store
        .complete_multipart_commit(&multipart_upload_id("upload-1"), 1, &obj, &parts)
        .unwrap();

    assert!(store
        .get_completed_multipart_upload(&multipart_upload_id("upload-1"))
        .unwrap()
        .is_some());

    store
        .put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
            bucket: bucket_name("bucket"),
            key: object_key("key"),
            version_id: VersionId::Null,
            owner: test_owner(),
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id: GenerationId::new(2).unwrap(),
            ec: EcShape { k: 4, m: 2 },
            size: 16,
            etag: ObjectEtag::SinglePart([9, 0, 0, 0, 0, 0, 0, 0]),
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: None,
            system_metadata_blob: None,
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
        }))
        .unwrap();

    assert!(store
        .get_completed_multipart_upload(&multipart_upload_id("upload-1"))
        .unwrap()
        .is_some());
}

#[test]
fn completed_multipart_tombstone_survives_object_version_delete() {
    let (_dir, store) = make_pg_store();
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

    let version_id = VersionId::Versioned(NonZeroU64::new(1).unwrap());
    let upload = CreateMultipartUploadReq {
        upload_id: multipart_upload_id("upload-versioned"),
        bucket: bucket_name("bucket"),
        key: object_key("key"),
        tags: None,
        metadata_blob: vec![].into(),
        system_metadata_blob: vec![].into(),
        initiator: Some(test_owner()),
        owner: test_owner(),
        acl_grants: AclGrants::default(),
        public_read: false,
        object_lock: ObjectLockState::default(),
        checksum: None,
        encryption: ObjectEncryption::None,
    };
    store.create_multipart_upload(&upload).unwrap();

    let obj = CommitMultipartReq {
        bucket: bucket_name("bucket"),
        key: object_key("key"),
        version_id,
        owner: test_owner(),
        acl_grants: AclGrants::default(),
        public_read: false,
        generation_id: GenerationId::MIN,
        size: 64,
        etag_crc64: [8, 0, 0, 0, 0, 0, 0, 0],
        ec: EcShape { k: 4, m: 2 },
        tags: None,
        metadata_blob: None,
        system_metadata_blob: None,
        object_lock: ObjectLockState::default(),
        encryption: ObjectEncryption::None,
    };
    let parts = vec![ObjectPartRecord {
        bucket: bucket_name("bucket"),
        key: object_key("key"),
        version_id,
        part_number: 1,
        size: 64,
        etag: vec![8; 8],
        etag_kind: EtagKind::Crc64,
        part_okh: [4; 16],
        part_vid: GenerationId::MIN,
        ec_k: 4,
        ec_m: 2,
        shard_pg_id: 0,
        checksum: None,
    }];
    store
        .complete_multipart_commit(&multipart_upload_id("upload-versioned"), 1, &obj, &parts)
        .unwrap();

    let completed = store
        .get_completed_multipart_upload(&multipart_upload_id("upload-versioned"))
        .unwrap()
        .expect("completed upload record");
    assert_eq!(completed.bucket.as_str(), "bucket");
    assert_eq!(completed.key.as_str(), "key");

    store
        .delete_object_version(&bucket_name("bucket"), &object_key("key"), version_id)
        .unwrap();

    assert!(store
        .get_completed_multipart_upload(&multipart_upload_id("upload-versioned"))
        .unwrap()
        .is_some());
}

#[test]
fn completed_multipart_upload_list_reports_global_completion_orders() {
    let (_dir, store) = make_pg_store();
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

    let complete_upload = |upload_id: &str, key: &str| {
        let upload_id = multipart_upload_id(upload_id);
        store
            .create_multipart_upload(&CreateMultipartUploadReq {
                upload_id: upload_id.clone(),
                bucket: bucket_name("bucket"),
                key: object_key(key),
                tags: None,
                metadata_blob: vec![].into(),
                system_metadata_blob: vec![].into(),
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
            .complete_multipart_commit(
                &upload_id,
                if upload_id == multipart_upload_id("z-first") {
                    1
                } else {
                    2
                },
                &CommitMultipartReq {
                    bucket: bucket_name("bucket"),
                    key: object_key(key),
                    version_id: VersionId::Null,
                    owner: test_owner(),
                    acl_grants: AclGrants::default(),
                    public_read: false,
                    generation_id: GenerationId::MIN,
                    size: 8,
                    etag_crc64: [1, 0, 0, 0, 0, 0, 0, 0],
                    ec: EcShape { k: 4, m: 2 },
                    tags: None,
                    metadata_blob: None,
                    system_metadata_blob: None,
                    object_lock: ObjectLockState::default(),
                    encryption: ObjectEncryption::None,
                },
                &[ObjectPartRecord {
                    bucket: bucket_name("bucket"),
                    key: object_key(key),
                    version_id: VersionId::Null,
                    part_number: 1,
                    size: 8,
                    etag: vec![1; 8],
                    etag_kind: EtagKind::Crc64,
                    part_okh: [1; 16],
                    part_vid: GenerationId::MIN,
                    ec_k: 4,
                    ec_m: 2,
                    shard_pg_id: 0,
                    checksum: None,
                }],
            )
            .unwrap();
    };

    crate::clock::with_time_override(1_700_000_000_000, || {
        complete_upload("z-first", "first");
        complete_upload("a-second", "second");
    });

    let uploads = store
        .list_completed_multipart_uploads_for_bucket("bucket")
        .unwrap();
    assert!(uploads.contains(&(multipart_upload_id("z-first"), 1)));
    assert!(uploads.contains(&(multipart_upload_id("a-second"), 2)));
}

#[test]
fn mpu_get_missing_upload_returns_no_such_upload() {
    let (_dir, store) = make_pg_store();
    let err = store
        .get_multipart_upload(&multipart_upload_id("nonexistent"))
        .unwrap_err();
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
            upload_id: multipart_upload_id("uid-2"),
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

    // InProgress -> Completing succeeds
    store
        .set_upload_state(&multipart_upload_id("uid-2"), UploadState::Completing)
        .unwrap();
    let rec = store
        .get_multipart_upload(&multipart_upload_id("uid-2"))
        .unwrap();
    assert_eq!(rec.state, UploadState::Completing);

    // Completing -> Aborting fails (not InProgress)
    let err = store
        .set_upload_state(&multipart_upload_id("uid-2"), UploadState::Aborting)
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
        .set_upload_state(&multipart_upload_id("nonexistent"), UploadState::Completing)
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
            upload_id: multipart_upload_id("uid-3"),
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

    // Add a part
    store
        .upsert_multipart_part(&MultipartPartRecord {
            upload_id: multipart_upload_id("uid-3"),
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
    store
        .delete_multipart_upload(&multipart_upload_id("uid-3"))
        .unwrap();

    assert!(matches!(
        store
            .get_multipart_upload(&multipart_upload_id("uid-3"))
            .unwrap_err(),
        crate::error::MetadataError::NoSuchUpload { .. }
    ));
}

#[test]
fn mpu_delete_missing_upload_returns_no_such_upload() {
    let (_dir, store) = make_pg_store();
    let err = store
        .delete_multipart_upload(&multipart_upload_id("nonexistent"))
        .unwrap_err();
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
            upload_id: multipart_upload_id("uid-4"),
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

    // First upload: no previous generation
    let prev = store
        .upsert_multipart_part(&MultipartPartRecord {
            upload_id: multipart_upload_id("uid-4"),
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
    let part = store
        .get_multipart_part(&multipart_upload_id("uid-4"), 1)
        .unwrap();
    assert_eq!(part.generation, 0);
    assert_eq!(part.size, 5 * 1024 * 1024);
    assert_eq!(part.etag, vec![0xBB]);
    assert_eq!(part.part_okh, [2u8; 16]);

    // Re-upload same part: returns previous generation
    let prev = store
        .upsert_multipart_part(&MultipartPartRecord {
            upload_id: multipart_upload_id("uid-4"),
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
    let part = store
        .get_multipart_part(&multipart_upload_id("uid-4"), 1)
        .unwrap();
    assert_eq!(part.generation, 1);
    assert_eq!(part.size, 6 * 1024 * 1024);
}

#[test]
fn mpu_list_parts_pagination() {
    let (_dir, store) = make_pg_store();
    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: multipart_upload_id("uid-5"),
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

    // Insert 5 parts
    for i in 1..=5 {
        store
            .upsert_multipart_part(&MultipartPartRecord {
                upload_id: multipart_upload_id("uid-5"),
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
            upload_id: multipart_upload_id("uid-5"),
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
            upload_id: multipart_upload_id("uid-5"),
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
            upload_id: multipart_upload_id("uid-5"),
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
                upload_id: multipart_upload_id(uid),
                bucket: bucket_name("bkt"),
                key: object_key(key),
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
    }

    // List first page (max 2)
    let resp = store
        .list_multipart_uploads(&ListMultipartUploadsReq {
            bucket: bucket_name("bkt"),
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
            bucket: bucket_name("bkt"),
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
                upload_id: multipart_upload_id(uid),
                bucket: bucket_name("bkt"),
                key: object_key(key),
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
    }

    let resp = store
        .list_multipart_uploads(&ListMultipartUploadsReq {
            bucket: bucket_name("bkt"),
            prefix: Some(object_key("photos/")),
            key_marker: None,
            upload_id_marker: None,
            max_uploads: 100,
        })
        .unwrap();
    assert_eq!(resp.uploads.len(), 2);
    assert!(resp
        .uploads
        .iter()
        .all(|u| u.key.as_str().starts_with("photos/")));
}

#[test]
fn mpu_list_uploads_same_key_multiple_upload_ids() {
    let (_dir, store) = make_pg_store();

    // Three uploads for the same key
    for uid in ["u-a", "u-b", "u-c"] {
        store
            .create_multipart_upload(&CreateMultipartUploadReq {
                upload_id: multipart_upload_id(uid),
                bucket: bucket_name("bkt"),
                key: object_key("same-key"),
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
    }

    // Page 1: max_uploads=2
    let resp = store
        .list_multipart_uploads(&ListMultipartUploadsReq {
            bucket: bucket_name("bkt"),
            prefix: None,
            key_marker: None,
            upload_id_marker: None,
            max_uploads: 2,
        })
        .unwrap();
    assert!(resp.is_truncated);
    assert_eq!(resp.uploads.len(), 2);
    // All same key, ordered by upload_id
    assert_eq!(resp.uploads[0].upload_id, multipart_upload_id("u-a"));
    assert_eq!(resp.uploads[1].upload_id, multipart_upload_id("u-b"));

    // Page 2: resume with markers
    let resp2 = store
        .list_multipart_uploads(&ListMultipartUploadsReq {
            bucket: bucket_name("bkt"),
            prefix: None,
            key_marker: resp.next_key_marker,
            upload_id_marker: resp.next_upload_id_marker,
            max_uploads: 2,
        })
        .unwrap();
    assert!(!resp2.is_truncated);
    assert_eq!(resp2.uploads.len(), 1);
    assert_eq!(resp2.uploads[0].upload_id, multipart_upload_id("u-c"));
}

#[test]
fn mpu_list_uploads_stale_marker_returns_remaining() {
    let (_dir, store) = make_pg_store();

    // Create 3 uploads for the same key.
    for uid in ["u-x", "u-y", "u-z"] {
        store
            .create_multipart_upload(&CreateMultipartUploadReq {
                upload_id: multipart_upload_id(uid),
                bucket: bucket_name("bkt"),
                key: object_key("key"),
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
    }

    // Delete the middle upload (simulating it being aborted between pages).
    store
        .delete_multipart_upload(&multipart_upload_id("u-y"))
        .unwrap();

    // Paginate using u-y as the marker — it no longer exists.
    // COALESCE to 0 means all remaining uploads for "key" are returned.
    let resp = store
        .list_multipart_uploads(&ListMultipartUploadsReq {
            bucket: bucket_name("bkt"),
            prefix: None,
            key_marker: Some(object_key("key")),
            upload_id_marker: Some(multipart_upload_id("u-y")),
            max_uploads: 10,
        })
        .unwrap();

    // u-x and u-z should both appear (safe re-return of u-x, plus u-z).
    // The stale marker must not cause u-z to be silently dropped.
    let ids: Vec<&str> = resp.uploads.iter().map(|u| u.upload_id.as_str()).collect();
    assert!(
        ids.contains(&multipart_upload_id("u-z").as_str()),
        "u-z must not be dropped; got: {ids:?}"
    );
}

#[test]
fn mpu_corrupted_part_okh_returns_error() {
    let (_dir, store) = make_pg_store();
    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: multipart_upload_id("uid-okh"),
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

    // Insert a part with valid okh
    store
        .upsert_multipart_part(&MultipartPartRecord {
            upload_id: multipart_upload_id("uid-okh"),
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
             WHERE upload_id = ?1 AND part_number = 1",
            [multipart_upload_id("uid-okh").into_string()],
        )
        .unwrap();

    // Reading should fail, not silently zero the okh
    let err = store
        .get_multipart_part(&multipart_upload_id("uid-okh"), 1)
        .unwrap_err();
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
            bucket: bucket_name("bucket"),
            key: object_key("k"),
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
             WHERE bucket = 'bucket' AND key = 'k' AND version_id = 1",
            [],
        )
        .unwrap();

    let err = store
        .get_object_parts(
            &bucket_name("bucket"),
            &object_key("k"),
            VersionId::from_u64(1),
        )
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
            upload_id: multipart_upload_id("uid-pnf"),
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

    let err = store
        .get_multipart_part(&multipart_upload_id("uid-pnf"), 42)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::error::MetadataError::PartNotFound {
                ref upload_id,
                part_number: 42
            } if upload_id == &multipart_upload_id("uid-pnf")
        ),
        "expected PartNotFound, got: {err:?}"
    );
}

#[test]
fn mpu_set_upload_state_rejects_in_progress_target() {
    let (_dir, store) = make_pg_store();
    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: multipart_upload_id("uid-ip"),
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

    // InProgress -> InProgress should be rejected, reporting actual state
    let err = store
        .set_upload_state(&multipart_upload_id("uid-ip"), UploadState::InProgress)
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
        .set_upload_state(&multipart_upload_id("uid-ip"), UploadState::Completing)
        .unwrap();
    let err = store
        .set_upload_state(&multipart_upload_id("uid-ip"), UploadState::InProgress)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::error::MetadataError::UploadNotInProgress { state: 1 }
        ),
        "expected UploadNotInProgress {{ state: 1 }}, got: {err:?}"
    );

    // Upload should still be Completing (InProgress target was rejected)
    let rec = store
        .get_multipart_upload(&multipart_upload_id("uid-ip"))
        .unwrap();
    assert_eq!(rec.state, UploadState::Completing);
}

#[test]
fn mpu_set_upload_state_in_progress_target_nonexistent_returns_no_such_upload() {
    let (_dir, store) = make_pg_store();
    let err = store
        .set_upload_state(&multipart_upload_id("nonexistent"), UploadState::InProgress)
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
            upload_id: multipart_upload_id("nonexistent"),
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
            upload_id: multipart_upload_id("nonexistent"),
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
        bucket: bucket_name("bucket"),
        key: object_key("k"),
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
        .get_object_parts(
            &bucket_name("bucket"),
            &object_key("k"),
            VersionId::from_u64(1),
        )
        .unwrap();
    assert_eq!(parts.len(), 1, "original commit should still be intact");
}

#[test]
fn mpu_commit_and_get_object_parts() {
    let (_dir, store) = make_pg_store();

    let parts = vec![
        ObjectPartRecord {
            bucket: bucket_name("bucket"),
            key: object_key("k"),
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
            bucket: bucket_name("bucket"),
            key: object_key("k"),
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
        .get_object_parts(
            &bucket_name("bucket"),
            &object_key("k"),
            VersionId::from_u64(1),
        )
        .unwrap();
    assert_eq!(committed.len(), 2);
    assert_eq!(committed[0].part_number, 1);
    assert_eq!(committed[0].size, 5 * 1024 * 1024);
    assert_eq!(committed[0].part_okh, [1u8; 16]);
    assert_eq!(committed[1].part_number, 2);
    assert_eq!(committed[1].size, 3 * 1024 * 1024);

    // Get for non-existent version returns empty
    let empty = store
        .get_object_parts(
            &bucket_name("bucket"),
            &object_key("k"),
            VersionId::from_u64(999),
        )
        .unwrap();
    assert!(empty.is_empty());
}

#[test]
fn get_object_parts_overlapping_range_returns_only_overlapping_parts() {
    let (_dir, store) = make_pg_store();

    let mib = 1024 * 1024;
    let parts = vec![
        ObjectPartRecord {
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            version_id: VersionId::from_u64(1),
            part_number: 1,
            size: 5 * mib,
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
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            version_id: VersionId::from_u64(1),
            part_number: 2,
            size: 3 * mib,
            etag: vec![0xBB],
            etag_kind: EtagKind::Crc64,
            part_okh: [2u8; 16],
            part_vid: GenerationId::new(2).unwrap(),
            ec_k: 4,
            ec_m: 2,
            shard_pg_id: 0,
            checksum: None,
        },
        ObjectPartRecord {
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            version_id: VersionId::from_u64(1),
            part_number: 3,
            size: 2 * mib,
            etag: vec![0xCC],
            etag_kind: EtagKind::Crc64,
            part_okh: [3u8; 16],
            part_vid: GenerationId::new(3).unwrap(),
            ec_k: 4,
            ec_m: 2,
            shard_pg_id: 0,
            checksum: None,
        },
    ];

    store.commit_object_parts(&parts).unwrap();

    let middle = store
        .get_object_parts_overlapping_range(
            &bucket_name("bucket"),
            &object_key("k"),
            VersionId::from_u64(1),
            5 * mib,
            5 * mib + 1,
        )
        .unwrap();
    assert_eq!(middle.len(), 1);
    assert_eq!(middle[0].part.part_number, 2);
    assert_eq!(middle[0].object_offset_start, 5 * mib);

    let boundary = store
        .get_object_parts_overlapping_range(
            &bucket_name("bucket"),
            &object_key("k"),
            VersionId::from_u64(1),
            5 * mib - 1,
            5 * mib + 1,
        )
        .unwrap();
    assert_eq!(boundary.len(), 2);
    assert_eq!(boundary[0].part.part_number, 1);
    assert_eq!(boundary[0].object_offset_start, 0);
    assert_eq!(boundary[1].part.part_number, 2);
    assert_eq!(boundary[1].object_offset_start, 5 * mib);
}

#[test]
fn mpu_delete_object_parts() {
    let (_dir, store) = make_pg_store();

    store
        .commit_object_parts(&[ObjectPartRecord {
            bucket: bucket_name("bucket"),
            key: object_key("k"),
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
        .delete_object_parts(
            &bucket_name("bucket"),
            &object_key("k"),
            VersionId::from_u64(1),
        )
        .unwrap();
    let parts = store
        .get_object_parts(
            &bucket_name("bucket"),
            &object_key("k"),
            VersionId::from_u64(1),
        )
        .unwrap();
    assert!(parts.is_empty());

    // Delete again is idempotent (no error)
    store
        .delete_object_parts(
            &bucket_name("bucket"),
            &object_key("k"),
            VersionId::from_u64(1),
        )
        .unwrap();
}

// --- Step 3b: Transaction rollback and concurrency hardening tests ---

/// Helper: create an upload with the given ID in the given store.
fn create_upload(store: &dyn PgMetadataStore, upload_id: &str) {
    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: multipart_upload_id(upload_id),
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
}

/// Helper: build a MultipartPartRecord for a given upload/part/generation.
fn make_part(upload_id: &str, part_number: u32, generation: u32) -> MultipartPartRecord {
    MultipartPartRecord {
        upload_id: multipart_upload_id(upload_id),
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
    let part = store
        .get_multipart_part(&multipart_upload_id("uid-a"), 1)
        .unwrap();
    assert_eq!(part.generation, 0);
}

#[test]
fn mpu_commit_partial_batch_failure_rolls_back_all() {
    let (_dir, store) = make_pg_store();

    // Commit part 1 for version 1.
    let part1 = ObjectPartRecord {
        bucket: bucket_name("bucket"),
        key: object_key("k"),
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
        .get_object_parts(
            &bucket_name("bucket"),
            &object_key("k"),
            VersionId::from_u64(1),
        )
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
    let part = store
        .get_multipart_part(&multipart_upload_id("uid-rapid"), 1)
        .unwrap();
    assert_eq!(part.generation, 9);

    // List should return exactly one part.
    let resp = store
        .list_multipart_parts(&ListPartsReq {
            upload_id: multipart_upload_id("uid-rapid"),
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
    let p1 = store1
        .get_multipart_part(&multipart_upload_id("uid-conc"), 1)
        .unwrap();
    let p2 = store1
        .get_multipart_part(&multipart_upload_id("uid-conc"), 2)
        .unwrap();
    assert_eq!(p1.part_number, 1);
    assert_eq!(p2.part_number, 2);

    let p1b = store2
        .get_multipart_part(&multipart_upload_id("uid-conc"), 1)
        .unwrap();
    let p2b = store2
        .get_multipart_part(&multipart_upload_id("uid-conc"), 2)
        .unwrap();
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
    let p1 = store1
        .get_multipart_part(&multipart_upload_id("uid-race"), 1)
        .unwrap();
    assert_eq!(p1.generation, 1);
    let p2 = store2
        .get_multipart_part(&multipart_upload_id("uid-race"), 1)
        .unwrap();
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
        .set_upload_state(&multipart_upload_id("uid-trans"), UploadState::Completing)
        .unwrap();

    // Store2 tries to transition to Aborting — should fail (no longer InProgress).
    let err = store2
        .set_upload_state(&multipart_upload_id("uid-trans"), UploadState::Aborting)
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
        bucket: bucket_name("bucket"),
        key: object_key("k"),
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
        .get_object_parts(
            &bucket_name("bucket"),
            &object_key("k"),
            VersionId::from_u64(1),
        )
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
        .get_object_parts(
            &bucket_name("bucket"),
            &object_key("k"),
            VersionId::from_u64(1),
        )
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
    store
        .delete_multipart_upload(&multipart_upload_id("uid-del"))
        .unwrap();

    // Upsert should now fail with NoSuchUpload (FK violation).
    let err = store
        .upsert_multipart_part(&make_part("uid-del", 2, 0))
        .unwrap_err();
    assert!(
        matches!(err, crate::error::MetadataError::NoSuchUpload { .. }),
        "expected NoSuchUpload after delete, got: {err:?}"
    );

    // Part from before delete should also be gone (CASCADE).
    let err = store
        .get_multipart_part(&multipart_upload_id("uid-del"), 1)
        .unwrap_err();
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
    let part = store
        .get_multipart_part(&multipart_upload_id("uid-after"), 1)
        .unwrap();
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
    let part = store
        .get_multipart_part(&multipart_upload_id("uid-prior"), 1)
        .unwrap();
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
        bucket: bucket_name("bucket"),
        key: object_key("k"),
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
        .get_object_parts(
            &bucket_name("bucket"),
            &object_key("k"),
            VersionId::from_u64(1),
        )
        .unwrap();
    assert!(parts.is_empty(), "rolled-back parts should not be visible");

    // Connection should be usable — retry the same commit.
    store
        .commit_object_parts(std::slice::from_ref(&part))
        .unwrap();
    let parts = store
        .get_object_parts(
            &bucket_name("bucket"),
            &object_key("k"),
            VersionId::from_u64(1),
        )
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
    let p1 = verify
        .get_multipart_part(&multipart_upload_id("uid-mt"), 1)
        .unwrap();
    let p2 = verify
        .get_multipart_part(&multipart_upload_id("uid-mt"), 2)
        .unwrap();
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
        store1.set_upload_state(&multipart_upload_id("uid-race-t"), UploadState::Completing)
    });

    let b2 = barrier.clone();
    let t2 = std::thread::spawn(move || {
        b2.wait();
        store2.set_upload_state(&multipart_upload_id("uid-race-t"), UploadState::Aborting)
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
    let part = verify
        .get_multipart_part(&multipart_upload_id("uid-stress"), 1)
        .unwrap();
    assert!(
        (part.generation as usize) < n_threads,
        "generation {} should be from one of the {} threads",
        part.generation,
        n_threads
    );

    let resp = verify
        .list_multipart_parts(&ListPartsReq {
            upload_id: multipart_upload_id("uid-stress"),
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
    use super::super::property_test_support::{
        object_snapshots_from_store, pagination_keys_strategy, pagination_page_size_strategy,
        render_trace, stateful_page_size_strategy, stateful_trace_strategy,
        version_snapshots_from_store, ModelOp, ObjectSnapshot, VersionSnapshot, VersionStateModel,
        PROP_TEST_BUCKET,
    };
    use super::*;
    use proptest::prelude::*;
    use proptest::test_runner::{TestCaseError, TestCaseResult};

    const UNPAGINATED_MAX_KEYS: u32 = 100;
    const FORCED_TIED_LAST_MODIFIED_MILLIS: i64 = 1;
    type LivePage = (Vec<ObjectSnapshot>, bool, Option<ObjectKey>);
    type VersionPage = (
        Vec<ObjectSnapshot>,
        bool,
        Option<ObjectKey>,
        Option<VersionId>,
    );

    fn insert_keys(store: &dyn PgMetadataStore, bucket: &str, keys: &[ObjectKey]) {
        for key in keys {
            let req = PutObjectReq::Live(PutLiveObjectReq {
                bucket: bucket_name(bucket),
                key: key.clone(),
                version_id: VersionId::Null,
                owner: test_owner(),
                acl_grants: AclGrants::default(),
                public_read: false,
                generation_id: GenerationId::MIN,
                ec: EcShape { k: 4, m: 2 },
                size: 0,
                etag: ObjectEtag::SinglePart([0; 8]),
                layout: ObjectLayout::Standard,
                tags: None,
                metadata_blob: None,
                system_metadata_blob: None,
                object_lock: ObjectLockState::default(),
                encryption: ObjectEncryption::None,
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
                    bucket: bucket_name(bucket),
                    prefix: prefix.clone(),
                    start_after: start_after.clone(),
                    start_at: None,
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

    fn make_property_store() -> (test_util::TempDir, crate::PgStore) {
        let (dir, store) = super::make_pg_store();
        store
            .create_bucket(
                &bucket_name(PROP_TEST_BUCKET),
                "owner",
                &CanonicalUserId::from_principal("owner"),
                &AclGrants::default(),
                false,
                false,
            )
            .unwrap();
        (dir, store)
    }

    fn live_put_req(key: &ObjectKey, version_id: VersionId, size: u64) -> PutObjectReq {
        PutObjectReq::Live(PutLiveObjectReq {
            bucket: bucket_name(PROP_TEST_BUCKET),
            key: key.clone(),
            version_id,
            owner: test_owner(),
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id: GenerationId::MIN,
            ec: EcShape { k: 4, m: 2 },
            size,
            etag: ObjectEtag::SinglePart([size as u8, 0, 0, 0, 0, 0, 0, 0]),
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: None,
            system_metadata_blob: None,
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
        })
    }

    fn delete_marker_req(key: &ObjectKey, version_id: VersionId) -> PutObjectReq {
        PutObjectReq::DeleteMarker(PutDeleteMarkerReq {
            bucket: bucket_name(PROP_TEST_BUCKET),
            key: key.clone(),
            version_id,
            owner: test_owner(),
        })
    }

    fn apply_op_to_store(store: &crate::PgStore, op: &ModelOp) {
        match op {
            ModelOp::SetVersioning(state) => {
                store
                    .put_bucket_versioning(&bucket_name(PROP_TEST_BUCKET), *state)
                    .unwrap();
            }
            ModelOp::PutLive {
                key,
                version_id,
                size,
            } => {
                store
                    .put_object_meta(&live_put_req(key, *version_id, *size))
                    .unwrap();
            }
            ModelOp::PutDeleteMarker { key, version_id } => {
                store
                    .put_object_meta(&delete_marker_req(key, *version_id))
                    .unwrap();
            }
            ModelOp::DeleteVersion { key, version_id } => {
                store
                    .delete_object_version(&bucket_name(PROP_TEST_BUCKET), key, *version_id)
                    .unwrap();
            }
        }
    }

    fn current_snapshot_from_store(
        store: &crate::PgStore,
        key: &ObjectKey,
    ) -> Result<Option<ObjectSnapshot>, TestCaseError> {
        match store.get_object_meta(&bucket_name(PROP_TEST_BUCKET), key) {
            Ok(object) => Ok(Some(ObjectSnapshot::from(&object))),
            Err(crate::MetadataError::ObjectNotFound) => Ok(None),
            Err(err) => Err(TestCaseError::fail(format!(
                "get_object_meta failed for key {key}: {err:?}"
            ))),
        }
    }

    fn list_live_snapshots(store: &crate::PgStore) -> Result<Vec<ObjectSnapshot>, TestCaseError> {
        match store.list_objects(&ListObjectsReq {
            bucket: bucket_name(PROP_TEST_BUCKET),
            prefix: None,
            start_after: None,
            start_at: None,
            max_keys: UNPAGINATED_MAX_KEYS,
        }) {
            Ok(resp) => {
                if resp.is_truncated {
                    return Err(TestCaseError::fail(format!(
                        "list_objects unexpectedly truncated with {} keys",
                        UNPAGINATED_MAX_KEYS
                    )));
                }
                Ok(object_snapshots_from_store(&resp.objects))
            }
            Err(err) => Err(TestCaseError::fail(format!("list_objects failed: {err:?}"))),
        }
    }

    fn list_version_snapshots(
        store: &crate::PgStore,
    ) -> Result<Vec<VersionSnapshot>, TestCaseError> {
        match store.list_object_versions(&ListObjectVersionsReq {
            bucket: bucket_name(PROP_TEST_BUCKET),
            prefix: None,
            key_marker: None,
            version_id_marker: None,
            max_keys: UNPAGINATED_MAX_KEYS,
        }) {
            Ok(resp) => {
                if resp.is_truncated {
                    return Err(TestCaseError::fail(format!(
                        "list_object_versions unexpectedly truncated with {} keys",
                        UNPAGINATED_MAX_KEYS
                    )));
                }
                Ok(version_snapshots_from_store(&resp.versions))
            }
            Err(err) => Err(TestCaseError::fail(format!(
                "list_object_versions failed: {err:?}"
            ))),
        }
    }

    fn list_live_snapshot_page(
        store: &crate::PgStore,
        start_after: Option<ObjectKey>,
        max_keys: u32,
    ) -> Result<LivePage, TestCaseError> {
        match store.list_objects(&ListObjectsReq {
            bucket: bucket_name(PROP_TEST_BUCKET),
            prefix: None,
            start_after,
            start_at: None,
            max_keys,
        }) {
            Ok(resp) => Ok((
                object_snapshots_from_store(&resp.objects),
                resp.is_truncated,
                resp.next_start_after,
            )),
            Err(err) => Err(TestCaseError::fail(format!(
                "paginated list_objects failed: {err:?}"
            ))),
        }
    }

    fn list_version_object_page(
        store: &crate::PgStore,
        key_marker: Option<ObjectKey>,
        version_id_marker: Option<VersionId>,
        max_keys: u32,
    ) -> Result<VersionPage, TestCaseError> {
        match store.list_object_versions(&ListObjectVersionsReq {
            bucket: bucket_name(PROP_TEST_BUCKET),
            prefix: None,
            key_marker,
            version_id_marker,
            max_keys,
        }) {
            Ok(resp) => Ok((
                object_snapshots_from_store(&resp.versions),
                resp.is_truncated,
                resp.next_key_marker,
                resp.next_version_id_marker,
            )),
            Err(err) => Err(TestCaseError::fail(format!(
                "paginated list_object_versions failed: {err:?}"
            ))),
        }
    }

    fn live_suffix_after_marker<'a>(
        expected: &'a [ObjectSnapshot],
        key: &ObjectKey,
    ) -> &'a [ObjectSnapshot] {
        let Some(index) = expected.iter().position(|object| object.key == *key) else {
            return &expected[expected.len()..];
        };
        &expected[index + 1..]
    }

    fn version_suffix_after_marker<'a>(
        expected: &'a [ObjectSnapshot],
        key: &ObjectKey,
        version_id: VersionId,
    ) -> &'a [ObjectSnapshot] {
        let Some(index) = expected
            .iter()
            .position(|version| version.key == *key && version.version_id == version_id)
        else {
            return &expected[expected.len()..];
        };
        &expected[index + 1..]
    }

    fn version_suffix_after_key<'a>(
        expected: &'a [ObjectSnapshot],
        key: &ObjectKey,
    ) -> &'a [ObjectSnapshot] {
        let mut index = 0usize;
        while index < expected.len() && expected[index].key <= *key {
            index += 1;
        }
        &expected[index..]
    }

    fn model_version_snapshots_for_key(
        model: &VersionStateModel,
        key: &ObjectKey,
    ) -> Vec<VersionSnapshot> {
        model
            .versions_for_key(key)
            .iter()
            .rev()
            .enumerate()
            .map(|(index, version)| VersionSnapshot {
                object: ObjectSnapshot {
                    key: key.clone(),
                    version_id: version.version_id,
                    kind: version.kind,
                    size: version.size,
                },
                is_latest: index == 0,
            })
            .collect()
    }

    fn assert_current_differential(
        store: &crate::PgStore,
        model: &VersionStateModel,
        keys: &[ObjectKey],
        context: &str,
    ) -> TestCaseResult {
        for key in keys {
            let actual = current_snapshot_from_store(store, key)?;
            let expected = model.current_snapshot(key);
            prop_assert_eq!(actual, expected, "{}", context);
        }
        Ok(())
    }

    fn assert_live_listing_differential(
        store: &crate::PgStore,
        model: &VersionStateModel,
        context: &str,
    ) -> TestCaseResult {
        let actual = list_live_snapshots(store)?;
        let expected = model.live_listing();
        prop_assert_eq!(actual, expected, "{}", context);
        Ok(())
    }

    fn assert_live_pagination_differential(
        store: &crate::PgStore,
        model: &VersionStateModel,
        page_size: u32,
        context: &str,
    ) -> TestCaseResult {
        let expected = model.live_listing();
        let mut paginated = Vec::new();
        let mut start_after: Option<ObjectKey> = None;
        let mut completed = false;

        for _ in 0..=expected.len() {
            let current_marker = start_after.clone();
            let (page, is_truncated, next_start_after) =
                list_live_snapshot_page(store, current_marker.clone(), page_size)?;

            prop_assert!(page.len() <= page_size as usize, "{}", context);
            if let Some(marker) = &current_marker {
                for object in &page {
                    prop_assert!(object.key > *marker, "{}", context);
                }
            }

            if is_truncated {
                let next_marker = next_start_after.clone().ok_or_else(|| {
                    TestCaseError::fail(format!(
                        "{context}\nmissing next_start_after for truncated page"
                    ))
                })?;
                let last_key = page
                    .last()
                    .map(|object| object.key.clone())
                    .ok_or_else(|| {
                        TestCaseError::fail(format!("{context}\ntruncated object page was empty"))
                    })?;
                prop_assert_eq!(&next_marker, &last_key, "{}", context);
                if let Some(previous_marker) = current_marker {
                    prop_assert!(next_marker > previous_marker, "{}", context);
                }
            } else {
                prop_assert!(next_start_after.is_none(), "{}", context);
            }

            paginated.extend(page);
            prop_assert!(paginated.len() <= expected.len(), "{}", context);
            prop_assert_eq!(
                paginated.as_slice(),
                &expected[..paginated.len()],
                "{}",
                context
            );

            if is_truncated {
                start_after = next_start_after;
            } else {
                completed = true;
                break;
            }
        }

        prop_assert!(completed, "{}", context);
        prop_assert_eq!(&paginated, &expected, "{}", context);

        for object in &expected {
            let (suffix, is_truncated, next_start_after) =
                list_live_snapshot_page(store, Some(object.key.clone()), UNPAGINATED_MAX_KEYS)?;
            prop_assert!(!is_truncated, "{}", context);
            prop_assert!(next_start_after.is_none(), "{}", context);
            prop_assert_eq!(
                suffix.as_slice(),
                live_suffix_after_marker(&expected, &object.key),
                "{}",
                context
            );
        }

        Ok(())
    }

    fn assert_version_listing_differential(
        store: &crate::PgStore,
        model: &VersionStateModel,
        keys: &[ObjectKey],
        context: &str,
    ) -> TestCaseResult {
        let actual = list_version_snapshots(store)?;
        let expected = model.version_listing();
        prop_assert_eq!(&actual, &expected, "{}", context);

        for key in keys {
            let per_key =
                match store.list_object_versions_for_key(&bucket_name(PROP_TEST_BUCKET), key) {
                    Ok(versions) => version_snapshots_from_store(&versions),
                    Err(err) => {
                        return Err(TestCaseError::fail(format!(
                            "{context}\nlist_object_versions_for_key failed for key {key}: {err:?}"
                        )))
                    }
                };
            let expected_per_key = model_version_snapshots_for_key(model, key);
            prop_assert_eq!(per_key, expected_per_key, "{}", context);
        }

        let latest_keys = actual
            .iter()
            .filter(|version| version.is_latest)
            .map(|version| version.object.key.clone())
            .collect::<Vec<_>>();
        let expected_latest_keys = expected
            .iter()
            .filter(|version| version.is_latest)
            .map(|version| version.object.key.clone())
            .collect::<Vec<_>>();
        prop_assert_eq!(latest_keys, expected_latest_keys, "{}", context);
        Ok(())
    }

    fn assert_version_pagination_differential(
        store: &crate::PgStore,
        model: &VersionStateModel,
        page_size: u32,
        context: &str,
    ) -> TestCaseResult {
        let expected = model
            .version_listing()
            .into_iter()
            .map(|version| version.object)
            .collect::<Vec<_>>();
        let mut paginated = Vec::new();
        let mut key_marker: Option<ObjectKey> = None;
        let mut version_id_marker: Option<VersionId> = None;
        let mut completed = false;

        for _ in 0..=expected.len() {
            let current_key_marker = key_marker.clone();
            let current_version_marker = version_id_marker;
            let (page, is_truncated, next_key_marker, next_version_id_marker) =
                list_version_object_page(
                    store,
                    current_key_marker.clone(),
                    current_version_marker,
                    page_size,
                )?;

            prop_assert!(page.len() <= page_size as usize, "{}", context);

            if is_truncated {
                let next_key = next_key_marker.clone().ok_or_else(|| {
                    TestCaseError::fail(format!(
                        "{context}\nmissing next_key_marker for truncated version page"
                    ))
                })?;
                let next_version = next_version_id_marker.ok_or_else(|| {
                    TestCaseError::fail(format!(
                        "{context}\nmissing next_version_id_marker for truncated version page"
                    ))
                })?;
                let last = page.last().ok_or_else(|| {
                    TestCaseError::fail(format!("{context}\ntruncated version page was empty"))
                })?;
                prop_assert_eq!(&next_key, &last.key, "{}", context);
                prop_assert_eq!(next_version, last.version_id, "{}", context);
                if let (Some(previous_key), Some(previous_version)) =
                    (current_key_marker, current_version_marker)
                {
                    prop_assert!(
                        next_key > previous_key
                            || (next_key == previous_key && next_version != previous_version),
                        "{}",
                        context
                    );
                }
            } else {
                prop_assert!(next_key_marker.is_none(), "{}", context);
                prop_assert!(next_version_id_marker.is_none(), "{}", context);
            }

            paginated.extend(page);
            prop_assert!(paginated.len() <= expected.len(), "{}", context);
            prop_assert_eq!(
                paginated.as_slice(),
                &expected[..paginated.len()],
                "{}",
                context
            );

            if is_truncated {
                key_marker = next_key_marker;
                version_id_marker = next_version_id_marker;
            } else {
                completed = true;
                break;
            }
        }

        prop_assert!(completed, "{}", context);
        prop_assert_eq!(&paginated, &expected, "{}", context);

        for version in &expected {
            let (suffix, is_truncated, next_key_marker, next_version_id_marker) =
                list_version_object_page(
                    store,
                    Some(version.key.clone()),
                    Some(version.version_id),
                    UNPAGINATED_MAX_KEYS,
                )?;
            prop_assert!(!is_truncated, "{}", context);
            prop_assert!(next_key_marker.is_none(), "{}", context);
            prop_assert!(next_version_id_marker.is_none(), "{}", context);
            prop_assert_eq!(
                suffix.as_slice(),
                version_suffix_after_marker(&expected, &version.key, version.version_id),
                "{}",
                context
            );
        }

        let mut seen_key: Option<&ObjectKey> = None;
        for version in &expected {
            if seen_key == Some(&version.key) {
                continue;
            }
            seen_key = Some(&version.key);

            let (suffix, is_truncated, next_key_marker, next_version_id_marker) =
                list_version_object_page(
                    store,
                    Some(version.key.clone()),
                    None,
                    UNPAGINATED_MAX_KEYS,
                )?;
            prop_assert!(!is_truncated, "{}", context);
            prop_assert!(next_key_marker.is_none(), "{}", context);
            prop_assert!(next_version_id_marker.is_none(), "{}", context);
            prop_assert_eq!(
                suffix.as_slice(),
                version_suffix_after_key(&expected, &version.key),
                "{}",
                context
            );
        }

        Ok(())
    }

    fn run_trace_with_check<F>(keys: &[ObjectKey], ops: &[ModelOp], mut check: F) -> TestCaseResult
    where
        F: FnMut(&crate::PgStore, &VersionStateModel, &[ObjectKey], &str) -> TestCaseResult,
    {
        let (_dir, store) = make_property_store();
        let mut model = VersionStateModel::new();
        let trace = render_trace(ops);

        let initial_context = format!("initial state\nfull trace:\n{trace}");
        check(&store, &model, keys, &initial_context)?;

        for (index, op) in ops.iter().enumerate() {
            apply_op_to_store(&store, op);
            prop_assert!(
                model.apply(op).is_ok(),
                "generated operation should satisfy model invariants at step {index}: {op}\nfull trace:\n{trace}"
            );
            let step_context = format!("after step {index}: {op}\nfull trace:\n{trace}");
            check(&store, &model, keys, &step_context)?;
        }

        Ok(())
    }

    fn materialize_trace(
        keys: &[ObjectKey],
        ops: &[ModelOp],
    ) -> Result<(test_util::TempDir, crate::PgStore, VersionStateModel), TestCaseError> {
        let (dir, store) = make_property_store();
        let mut model = VersionStateModel::new();
        let trace = render_trace(ops);

        for (index, op) in ops.iter().enumerate() {
            apply_op_to_store(&store, op);
            if let Err(err) = model.apply(op) {
                return Err(TestCaseError::fail(format!(
                    "generated operation should satisfy model invariants at step {index}: {op}\nerror: {err:?}\nfull trace:\n{trace}"
                )));
            }
        }

        for key in keys {
            let versions = model.versions_for_key(key);
            if versions.len() > 1 {
                store
                    .connection()
                    .execute(
                        "UPDATE objects SET last_modified = ?1 WHERE bucket = ?2 AND key = ?3",
                        rusqlite::params![
                            FORCED_TIED_LAST_MODIFIED_MILLIS,
                            PROP_TEST_BUCKET,
                            key.as_str()
                        ],
                    )
                    .map_err(|err| {
                        TestCaseError::fail(format!(
                            "forcing timestamp ties failed for key {key}: {err:?}\nfull trace:\n{trace}"
                        ))
                    })?;
            }
        }

        Ok((dir, store, model))
    }

    #[test]
    fn regression_empty_prefix_matches_all() {
        let (_dir, store) = super::make_pg_store();
        insert_keys(&store, "bucket", &[object_key("O")]);

        let got = list_all_keys(&store, "bucket", None, 1);
        assert_eq!(got, vec![object_key("O")]);
    }

    proptest! {
        #[test]
        fn prop_metadata_pagination_roundtrip(
            keys in pagination_keys_strategy(),
            max_keys in pagination_page_size_strategy(),
        ) {
            let (_dir, store) = super::make_pg_store();
            insert_keys(&store, "bucket", &keys);

            let mut expected = keys.clone();
            expected.sort();
            expected.dedup();
            let got = list_all_keys(&store, "bucket", None, max_keys);
            prop_assert_eq!(got, expected);
        }

        #[test]
        fn prop_metadata_prefix_subset(
            keys in pagination_keys_strategy(),
            prefix in proptest::string::string_regex(r"[A-Za-z0-9._/-]{0,8}").unwrap(),
            max_keys in pagination_page_size_strategy(),
        ) {
            let (_dir, store) = super::make_pg_store();
            insert_keys(&store, "bucket", &keys);

            let mut expected: Vec<ObjectKey> = keys
                .iter()
                .filter(|k| k.as_str().starts_with(&prefix))
                .cloned()
                .collect();
            expected.sort();
            expected.dedup();

            let prefix = if prefix.is_empty() {
                None
            } else {
                Some(object_key(prefix.clone()))
            };

            let got = list_all_keys(&store, "bucket", prefix, max_keys);
            prop_assert_eq!(got, expected);
        }

        #[test]
        fn prop_storage_current_version_selection(
            (keys, ops) in stateful_trace_strategy(),
        ) {
            run_trace_with_check(&keys, &ops, assert_current_differential)?;
        }

        #[test]
        fn prop_storage_live_listing_visibility(
            (keys, ops) in stateful_trace_strategy(),
        ) {
            run_trace_with_check(&keys, &ops, |store, model, _, context| {
                assert_live_listing_differential(store, model, context)
            })?;
        }

        #[test]
        fn prop_storage_version_listing_order_and_latest(
            (keys, ops) in stateful_trace_strategy(),
        ) {
            run_trace_with_check(&keys, &ops, assert_version_listing_differential)?;
        }

        #[test]
        fn prop_storage_timestamp_tie_invariance(
            (keys, ops) in stateful_trace_strategy(),
        ) {
            let trace = render_trace(&ops);

            let (_untied_dir, untied_store) = make_property_store();
            let mut untied_model = VersionStateModel::new();
            for (index, op) in ops.iter().enumerate() {
                apply_op_to_store(&untied_store, op);
                prop_assert!(
                    untied_model.apply(op).is_ok(),
                    "generated operation should satisfy model invariants at step {index}: {op}\nfull trace:\n{trace}"
                );
            }

            let (_tied_dir, tied_store, tied_model) = materialize_trace(&keys, &ops)?;
            prop_assert_eq!(&untied_model, &tied_model, "full trace:\n{}", trace);

            let context = format!("after forcing per-key timestamp ties\nfull trace:\n{trace}");
            assert_current_differential(&tied_store, &tied_model, &keys, &context)?;
            assert_live_listing_differential(&tied_store, &tied_model, &context)?;
            assert_version_listing_differential(&tied_store, &tied_model, &keys, &context)?;

            for key in &keys {
                let untied = current_snapshot_from_store(&untied_store, key)?;
                let tied = current_snapshot_from_store(&tied_store, key)?;
                prop_assert_eq!(tied, untied, "{}", context);
            }

            let untied_live = list_live_snapshots(&untied_store)?;
            let tied_live = list_live_snapshots(&tied_store)?;
            prop_assert_eq!(tied_live, untied_live, "{}", context);

            let untied_versions = list_version_snapshots(&untied_store)?;
            let tied_versions = list_version_snapshots(&tied_store)?;
            prop_assert_eq!(tied_versions, untied_versions, "{}", context);
        }

        #[test]
        fn prop_storage_live_listing_pagination_roundtrip(
            (keys, ops) in stateful_trace_strategy(),
            page_size in stateful_page_size_strategy(),
        ) {
            run_trace_with_check(&keys, &ops, |store, model, _, context| {
                assert_live_pagination_differential(store, model, page_size, context)
            })?;
        }

        #[test]
        fn prop_storage_version_listing_pagination_roundtrip(
            (keys, ops) in stateful_trace_strategy(),
            page_size in stateful_page_size_strategy(),
        ) {
            run_trace_with_check(&keys, &ops, |store, model, _, context| {
                assert_version_pagination_differential(store, model, page_size, context)
            })?;
        }
    }
}

// ── Streaming upload session tests (PgStore) ─────────────────────────

#[test]
fn stream_upload_create_get_delete() {
    let (_dir, store) = make_pg_store();

    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: stream_session_id("sess-1"),
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            target: StreamUploadTarget::PutObject,
            encryption: ObjectEncryption::None,
        })
        .unwrap();

    let rec = store.get_stream_upload(stream_session_id("sess-1").as_str()).unwrap();
    assert_eq!(rec.session_id, stream_session_id("sess-1"));
    assert_eq!(rec.bucket, "bucket");
    assert_eq!(rec.key, "k");
    assert_eq!(rec.target, StreamUploadTarget::PutObject);
    assert_eq!(rec.state, StreamUploadState::InProgress);

    store.delete_stream_upload(stream_session_id("sess-1").as_str()).unwrap();

    let err = store.get_stream_upload(stream_session_id("sess-1").as_str()).unwrap_err();
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
            session_id: stream_session_id("sess-2"),
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            target: StreamUploadTarget::PutObject,
            encryption: ObjectEncryption::None,
        })
        .unwrap();

    // Transition to Completing
    store
        .set_stream_upload_state(stream_session_id("sess-2").as_str(), StreamUploadState::Completing)
        .unwrap();

    let rec = store.get_stream_upload(stream_session_id("sess-2").as_str()).unwrap();
    assert_eq!(rec.state, StreamUploadState::Completing);

    // Cannot transition again (not InProgress)
    let err = store
        .set_stream_upload_state(stream_session_id("sess-2").as_str(), StreamUploadState::Aborted)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::StreamSessionNotInProgress { .. }
    ));
}

#[test]
fn stream_upload_not_found() {
    let (_dir, store) = make_pg_store();

    let err = store.get_stream_upload(stream_session_id("nonexistent").as_str()).unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::StreamSessionNotFound { .. }
    ));

    let err = store
        .set_stream_upload_state(stream_session_id("nonexistent").as_str(), StreamUploadState::Aborted)
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
            session_id: stream_session_id("sess-part"),
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            target: StreamUploadTarget::UploadPart {
                upload_id: multipart_upload_id("mpu-123"),
                part_number: 3,
            },
            encryption: ObjectEncryption::None,
        })
        .unwrap();

    let rec = store.get_stream_upload(stream_session_id("sess-part").as_str()).unwrap();
    assert_eq!(
        rec.target,
        StreamUploadTarget::UploadPart {
            upload_id: multipart_upload_id("mpu-123"),
            part_number: 3
        }
    );
}

#[test]
fn stream_upload_segment_vid_allocation_is_monotonic() {
    let (_dir, store) = make_pg_store();

    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: stream_session_id("sess-vid"),
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            target: StreamUploadTarget::PutObject,
            encryption: ObjectEncryption::None,
        })
        .unwrap();

    assert_eq!(
        store.allocate_stream_segment_vid(stream_session_id("sess-vid").as_str()).unwrap(),
        GenerationId::new(1).unwrap()
    );
    assert_eq!(
        store.allocate_stream_segment_vid(stream_session_id("sess-vid").as_str()).unwrap(),
        GenerationId::new(2).unwrap()
    );
    assert_eq!(
        store.allocate_stream_segment_vid(stream_session_id("sess-vid").as_str()).unwrap(),
        GenerationId::new(3).unwrap()
    );
}

#[test]
fn stream_upload_segment_vid_allocation_requires_in_progress_session() {
    let (_dir, store) = make_pg_store();

    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: stream_session_id("sess-vid-state"),
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            target: StreamUploadTarget::PutObject,
            encryption: ObjectEncryption::None,
        })
        .unwrap();
    store
        .set_stream_upload_state(stream_session_id("sess-vid-state").as_str(), StreamUploadState::Completing)
        .unwrap();

    let err = store
        .allocate_stream_segment_vid(stream_session_id("sess-vid-state").as_str())
        .unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::StreamSessionNotInProgress { .. }
    ));
}

#[test]
fn stream_segment_append_and_list() {
    let (_dir, store) = make_pg_store();

    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: stream_session_id("sess-segments"),
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            target: StreamUploadTarget::PutObject,
            encryption: ObjectEncryption::None,
        })
        .unwrap();

    for i in 0..3u32 {
        store
            .append_stream_segment(&StreamUploadSegmentRecord {
                session_id: stream_session_id("sess-segments"),
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

    let segments = store.list_stream_segments(stream_session_id("sess-segments").as_str()).unwrap();
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
fn stream_segment_publish_with_shards_same_pg_is_atomic() {
    let (_dir, store) = make_pg_store();

    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: stream_session_id("sess-publish"),
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            target: StreamUploadTarget::PutObject,
            encryption: ObjectEncryption::None,
        })
        .unwrap();

    let shard = ShardKey::new(&[0xA1; 16], 1, 0);
    let ack = store.write_shard(&shard, b"bbbb").unwrap();
    store
        .connection()
        .execute(
            "DELETE FROM shards WHERE shard_key = ?1",
            rusqlite::params![shard.as_bytes().as_slice()],
        )
        .unwrap();
    assert!(matches!(
        store.read_shard(&shard),
        Err(crate::StoreError::NotFound)
    ));

    let segment = StreamUploadSegmentRecord {
        session_id: stream_session_id("sess-publish"),
        segment_index: 0,
        size: 4,
        segment_crc64: Some(99),
        segment_okh: [0x44; 16],
        segment_vid: GenerationId::new(1).unwrap(),
        shard_pg_id: 0,
        ec_k: 4,
        ec_m: 2,
    };
    let shard_batch = [(&shard, ack)];

    store
        .register_written_shards_and_append_stream_segment(&shard_batch, &segment)
        .unwrap();

    let segments = store.list_stream_segments(stream_session_id("sess-publish").as_str()).unwrap();
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].segment_index, 0);
    assert_eq!(store.read_shard(&shard).unwrap().data, b"bbbb");
}

#[test]
fn stream_segment_cascade_delete() {
    let (_dir, store) = make_pg_store();

    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: stream_session_id("sess-cascade"),
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            target: StreamUploadTarget::PutObject,
            encryption: ObjectEncryption::None,
        })
        .unwrap();

    store
        .append_stream_segment(&StreamUploadSegmentRecord {
            session_id: stream_session_id("sess-cascade"),
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

    // Delete session cascades to segments
    store.delete_stream_upload(stream_session_id("sess-cascade").as_str()).unwrap();

    let segments = store.list_stream_segments(stream_session_id("sess-cascade").as_str()).unwrap();
    assert!(segments.is_empty());
}

#[test]
fn commit_stream_put_atomic() {
    let (_dir, store) = make_pg_store();

    // Create session and append segments
    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: stream_session_id("sess-commit"),
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            target: StreamUploadTarget::PutObject,
            encryption: ObjectEncryption::None,
        })
        .unwrap();

    store
        .append_stream_segment(&StreamUploadSegmentRecord {
            session_id: stream_session_id("sess-commit"),
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
            session_id: stream_session_id("sess-commit"),
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
        bucket: bucket_name("bucket"),
        key: object_key("k"),
        version_id: VersionId::Null,
        owner: test_owner(),
        acl_grants: AclGrants::default(),
        public_read: false,
        generation_id: GenerationId::MIN,
        size: 6_000_000,
        etag_crc64: u64::from_le_bytes([0xAB; 8]),
        ec: EcShape { k: 4, m: 2 },
        tags: None,
        metadata_blob: None,
        system_metadata_blob: None,
        object_lock: ObjectLockState::default(),
        encryption: ObjectEncryption::None,
    };

    let committed_segments = vec![
        ObjectSegmentRecord {
            bucket: bucket_name("bucket"),
            key: object_key("k"),
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
            bucket: bucket_name("bucket"),
            key: object_key("k"),
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
        .commit_stream_put(stream_session_id("sess-commit").as_str(), &obj, &committed_segments)
        .unwrap();

    // Object metadata is committed
    let record = store
        .get_object_meta(&bucket_name("bucket"), &object_key("k"))
        .unwrap();
    let live = record.as_live().unwrap();
    assert_eq!(live.size, 6_000_000);
    assert_eq!(live.layout, ObjectLayout::Standard);

    // Committed segments are readable
    let segments = store
        .get_object_segments(&bucket_name("bucket"), &object_key("k"), VersionId::Null)
        .unwrap();
    assert_eq!(segments.len(), 2);
    assert_eq!(segments[0].segment_index, 0);
    assert_eq!(segments[0].size, 4_000_000);
    assert_eq!(segments[0].segment_crc64, Some(11));
    assert_eq!(segments[0].segment_okh, [0x11; 16]);
    assert_eq!(segments[0].shard_pg_id, 0);
    assert_eq!(segments[1].segment_index, 1);
    assert_eq!(segments[1].size, 2_000_000);
    assert_eq!(segments[1].segment_crc64, Some(22));
    assert_eq!(segments[1].shard_pg_id, 1);

    // Staging rows are cleaned up
    let err = store.get_stream_upload(stream_session_id("sess-commit").as_str()).unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::StreamSessionNotFound { .. }
    ));
    let staging = store.list_stream_segments(stream_session_id("sess-commit").as_str()).unwrap();
    assert!(staging.is_empty());
}

#[test]
fn commit_stream_put_overwrite_unversioned() {
    let (_dir, store) = make_pg_store();

    // First write
    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: stream_session_id("s1"),
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            target: StreamUploadTarget::PutObject,
            encryption: ObjectEncryption::None,
        })
        .unwrap();
    store
        .commit_stream_put(
            stream_session_id("s1").as_str(),
            &CommitStreamPutReq {
                bucket: bucket_name("bucket"),
                key: object_key("k"),
                version_id: VersionId::Null,
                owner: test_owner(),
                acl_grants: AclGrants::default(),
                public_read: false,
                generation_id: GenerationId::MIN,
                size: 100,
                etag_crc64: 1u64,
                ec: EcShape { k: 4, m: 2 },
                tags: None,
                metadata_blob: None,
                system_metadata_blob: None,
                object_lock: ObjectLockState::default(),
                encryption: ObjectEncryption::None,
            },
            &[ObjectSegmentRecord {
                bucket: bucket_name("bucket"),
                key: object_key("k"),
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
            session_id: stream_session_id("s2"),
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            target: StreamUploadTarget::PutObject,
            encryption: ObjectEncryption::None,
        })
        .unwrap();
    store
        .commit_stream_put(
            stream_session_id("s2").as_str(),
            &CommitStreamPutReq {
                bucket: bucket_name("bucket"),
                key: object_key("k"),
                version_id: VersionId::Null,
                owner: test_owner(),
                acl_grants: AclGrants::default(),
                public_read: false,
                generation_id: GenerationId::MIN,
                size: 200,
                etag_crc64: 2u64,
                ec: EcShape { k: 4, m: 2 },
                tags: None,
                metadata_blob: None,
                system_metadata_blob: None,
                object_lock: ObjectLockState::default(),
                encryption: ObjectEncryption::None,
            },
            &[ObjectSegmentRecord {
                bucket: bucket_name("bucket"),
                key: object_key("k"),
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
    let record = store
        .get_object_meta(&bucket_name("bucket"), &object_key("k"))
        .unwrap();
    assert_eq!(record.as_live().unwrap().size, 200);

    // Segments replaced
    let segments = store
        .get_object_segments(&bucket_name("bucket"), &object_key("k"), VersionId::Null)
        .unwrap();
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].size, 200);
    assert_eq!(segments[0].segment_okh, [2; 16]);
}

#[test]
fn put_object_with_segments_persists_manifest() {
    let (_dir, store) = make_pg_store();

    store
        .put_object_with_segments(
            &PutLiveObjectReq {
                bucket: bucket_name("bucket"),
                key: object_key("k"),
                version_id: VersionId::Null,
                owner: test_owner(),
                acl_grants: AclGrants::default(),
                public_read: false,
                generation_id: GenerationId::MIN,
                size: 6_000_000,
                etag: ObjectEtag::single_part(1),
                ec: EcShape { k: 4, m: 2 },
                layout: ObjectLayout::Standard,
                tags: None,
                metadata_blob: None,
                system_metadata_blob: None,
                object_lock: ObjectLockState::default(),
                encryption: ObjectEncryption::None,
            },
            &[
                ObjectSegmentRecord {
                    bucket: bucket_name("bucket"),
                    key: object_key("k"),
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
                    bucket: bucket_name("bucket"),
                    key: object_key("k"),
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

    let record = store
        .get_object_meta(&bucket_name("bucket"), &object_key("k"))
        .unwrap();
    let live = record.as_live().unwrap();
    assert_eq!(live.size, 6_000_000);
    assert_eq!(live.layout, ObjectLayout::Standard);

    let segments = store
        .get_object_segments(&bucket_name("bucket"), &object_key("k"), VersionId::Null)
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
                bucket: bucket_name("bucket"),
                key: object_key("k"),
                version_id: VersionId::Null,
                owner: test_owner(),
                acl_grants: AclGrants::default(),
                public_read: false,
                generation_id: GenerationId::MIN,
                size: 100,
                etag: ObjectEtag::single_part(1),
                ec: EcShape { k: 4, m: 2 },
                layout: ObjectLayout::Standard,
                tags: None,
                metadata_blob: None,
                system_metadata_blob: None,
                object_lock: ObjectLockState::default(),
                encryption: ObjectEncryption::None,
            },
            &[ObjectSegmentRecord {
                bucket: bucket_name("bucket"),
                key: object_key("k"),
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
                bucket: bucket_name("bucket"),
                key: object_key("k"),
                version_id: VersionId::Null,
                owner: test_owner(),
                acl_grants: AclGrants::default(),
                public_read: false,
                generation_id: GenerationId::new(2).unwrap(),
                size: 200,
                etag: ObjectEtag::single_part(2),
                ec: EcShape { k: 4, m: 2 },
                layout: ObjectLayout::Standard,
                tags: None,
                metadata_blob: None,
                system_metadata_blob: None,
                object_lock: ObjectLockState::default(),
                encryption: ObjectEncryption::None,
            },
            &[ObjectSegmentRecord {
                bucket: bucket_name("bucket"),
                key: object_key("k"),
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

    let record = store
        .get_object_meta(&bucket_name("bucket"), &object_key("k"))
        .unwrap();
    assert_eq!(record.as_live().unwrap().size, 200);

    let segments = store
        .get_object_segments(&bucket_name("bucket"), &object_key("k"), VersionId::Null)
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
            session_id: stream_session_id("s-del"),
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            target: StreamUploadTarget::PutObject,
            encryption: ObjectEncryption::None,
        })
        .unwrap();
    store
        .commit_stream_put(
            stream_session_id("s-del").as_str(),
            &CommitStreamPutReq {
                bucket: bucket_name("bucket"),
                key: object_key("k"),
                version_id: VersionId::Null,
                owner: test_owner(),
                acl_grants: AclGrants::default(),
                public_read: false,
                generation_id: GenerationId::MIN,
                size: 100,
                etag_crc64: 1u64,
                ec: EcShape { k: 4, m: 2 },
                tags: None,
                metadata_blob: None,
                system_metadata_blob: None,
                object_lock: ObjectLockState::default(),
                encryption: ObjectEncryption::None,
            },
            &[ObjectSegmentRecord {
                bucket: bucket_name("bucket"),
                key: object_key("k"),
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

    // Delete committed segments
    store
        .delete_object_segments(&bucket_name("bucket"), &object_key("k"), VersionId::Null)
        .unwrap();
    let segments = store
        .get_object_segments(&bucket_name("bucket"), &object_key("k"), VersionId::Null)
        .unwrap();
    assert!(segments.is_empty());

    // Idempotent
    store
        .delete_object_segments(&bucket_name("bucket"), &object_key("k"), VersionId::Null)
        .unwrap();
}

#[test]
fn commit_stream_put_rejects_non_in_progress() {
    let (_dir, store) = make_pg_store();

    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: stream_session_id("sess-bad"),
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            target: StreamUploadTarget::PutObject,
            encryption: ObjectEncryption::None,
        })
        .unwrap();

    // Manually transition to Aborted
    store
        .set_stream_upload_state(stream_session_id("sess-bad").as_str(), StreamUploadState::Aborted)
        .unwrap();

    // commit_stream_put should fail with StreamSessionNotInProgress
    let err = store
        .commit_stream_put(
            stream_session_id("sess-bad").as_str(),
            &CommitStreamPutReq {
                bucket: bucket_name("bucket"),
                key: object_key("k"),
                version_id: VersionId::Null,
                owner: test_owner(),
                acl_grants: AclGrants::default(),
                public_read: false,
                generation_id: GenerationId::MIN,
                size: 0,
                etag_crc64: 0u64,
                ec: EcShape { k: 4, m: 2 },
                tags: None,
                metadata_blob: None,
                system_metadata_blob: None,
                object_lock: ObjectLockState::default(),
                encryption: ObjectEncryption::None,
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
         VALUES ('bucket', 'k', ?1, 1, 1, 0, 4000000, X'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 10, 0, 4, 2)",
        [multipart_upload_id("uid-1").into_string()],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO multipart_part_segments \
         (bucket, key, upload_id, version_id, part_number, segment_index, size, segment_okh, \
          segment_vid, shard_pg_id, ec_k, ec_m) \
         VALUES ('bucket', 'k', ?1, 1, 1, 1, 2000000, X'BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB', 10, 1, 4, 2)",
        [multipart_upload_id("uid-1").into_string()],
    )
    .unwrap();

    // Read back
    let segments = store
        .get_multipart_part_segments(
            &bucket_name("bucket"),
            &object_key("k"),
            VersionId::from_u64(1),
            1,
        )
        .unwrap();
    assert_eq!(segments.len(), 2);
    assert_eq!(segments[0].segment_index, 0);
    assert_eq!(segments[0].size, 4_000_000);
    assert_eq!(segments[1].segment_index, 1);
    assert_eq!(segments[1].size, 2_000_000);

    // Delete
    store
        .delete_multipart_part_segments(
            &bucket_name("bucket"),
            &object_key("k"),
            VersionId::from_u64(1),
        )
        .unwrap();
    let segments = store
        .get_multipart_part_segments(
            &bucket_name("bucket"),
            &object_key("k"),
            VersionId::from_u64(1),
            1,
        )
        .unwrap();
    assert!(segments.is_empty());
}

#[test]
fn upsert_multipart_part_segments_replaces_prior_segments() {
    let (_dir, store) = make_pg_store();

    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: multipart_upload_id("mpu-1"),
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

    let make_part = |generation, size| MultipartPartRecord {
        upload_id: multipart_upload_id("mpu-1"),
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
        bucket: bucket_name("bucket"),
        key: object_key("k"),
        upload_id: multipart_upload_id("mpu-1"),
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

    let part = store
        .get_multipart_part(&multipart_upload_id("mpu-1"), 1)
        .unwrap();
    assert_eq!(part.generation, 1);
    assert_eq!(part.part_okh, [0u8; 16]);

    let segments = store
        .get_all_multipart_part_segments_for_upload(&multipart_upload_id("mpu-1"))
        .unwrap();
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].segment_crc64, Some(0x33));
    assert_eq!(segments[0].segment_okh, [0x33; 16]);
}

#[test]
fn commit_stream_part_replaces_prior_segments_on_reupload() {
    let (_dir, store) = make_pg_store();

    // Create the multipart upload first
    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: multipart_upload_id("mpu-1"),
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

    let make_part = |size: u64| MultipartPartRecord {
        upload_id: multipart_upload_id("mpu-1"),
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

    // First upload: 3 segments
    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: stream_session_id("sp-1"),
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            target: StreamUploadTarget::UploadPart {
                upload_id: multipart_upload_id("mpu-1"),
                part_number: 1,
            },
            encryption: ObjectEncryption::None,
        })
        .unwrap();

    let segments_v1: Vec<MultipartPartSegmentRecord> = (0..3)
        .map(|i| MultipartPartSegmentRecord {
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            upload_id: multipart_upload_id("mpu-1"),
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

    let displaced_segments = store
        .commit_stream_part(stream_session_id("sp-1").as_str(), &make_part(3000), &segments_v1)
        .unwrap();
    assert!(
        displaced_segments.is_empty(),
        "first upload should not return displaced segments"
    );

    let segments = store
        .get_multipart_part_segments(
            &bucket_name("bucket"),
            &object_key("k"),
            VersionId::from_u64(u64::MAX),
            1,
        )
        .unwrap();
    assert_eq!(segments.len(), 3);

    // Re-upload same part: only 1 segment (fewer than before)
    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: stream_session_id("sp-2"),
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            target: StreamUploadTarget::UploadPart {
                upload_id: multipart_upload_id("mpu-1"),
                part_number: 1,
            },
            encryption: ObjectEncryption::None,
        })
        .unwrap();

    let segments_v2 = vec![MultipartPartSegmentRecord {
        bucket: bucket_name("bucket"),
        key: object_key("k"),
        upload_id: multipart_upload_id("mpu-1"),
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

    let displaced_segments = store
        .commit_stream_part(stream_session_id("sp-2").as_str(), &make_part(5000), &segments_v2)
        .unwrap();
    assert_eq!(displaced_segments.len(), 3);
    assert!(displaced_segments
        .iter()
        .all(|segment| segment.segment_okh == [0x11; 16]));
    assert!(displaced_segments
        .iter()
        .map(|segment| segment.segment_index)
        .eq(0..3));

    // Verify: only 1 segment (stale rows deleted)
    let segments = store
        .get_multipart_part_segments(
            &bucket_name("bucket"),
            &object_key("k"),
            VersionId::from_u64(u64::MAX),
            1,
        )
        .unwrap();
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].size, 5000);
    assert_eq!(segments[0].segment_okh, [0x22; 16]);
}

#[test]
fn commit_stream_put_rejects_wrong_kind() {
    let (_dir, store) = make_pg_store();

    // Create an UploadPart session
    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: stream_session_id("sess-wrong-kind"),
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            target: StreamUploadTarget::UploadPart {
                upload_id: multipart_upload_id("mpu-x"),
                part_number: 1,
            },
            encryption: ObjectEncryption::None,
        })
        .unwrap();

    // Try to commit_stream_put with it — should fail
    let err = store
        .commit_stream_put(
            stream_session_id("sess-wrong-kind").as_str(),
            &CommitStreamPutReq {
                bucket: bucket_name("bucket"),
                key: object_key("k"),
                version_id: VersionId::Null,
                owner: test_owner(),
                acl_grants: AclGrants::default(),
                public_read: false,
                generation_id: GenerationId::MIN,
                size: 0,
                etag_crc64: 0u64,
                ec: EcShape { k: 4, m: 2 },
                tags: None,
                metadata_blob: None,
                system_metadata_blob: None,
                object_lock: ObjectLockState::default(),
                encryption: ObjectEncryption::None,
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
            session_id: stream_session_id("sess-mismatch"),
            bucket: bucket_name("bucket-one"),
            key: object_key("k1"),
            target: StreamUploadTarget::PutObject,
            encryption: ObjectEncryption::None,
        })
        .unwrap();

    // Commit with different bucket/key
    let err = store
        .commit_stream_put(
            stream_session_id("sess-mismatch").as_str(),
            &CommitStreamPutReq {
                bucket: bucket_name("bucket-two"),
                key: object_key("k2"),
                version_id: VersionId::Null,
                owner: test_owner(),
                acl_grants: AclGrants::default(),
                public_read: false,
                generation_id: GenerationId::MIN,
                size: 0,
                etag_crc64: 0u64,
                ec: EcShape { k: 4, m: 2 },
                tags: None,
                metadata_blob: None,
                system_metadata_blob: None,
                object_lock: ObjectLockState::default(),
                encryption: ObjectEncryption::None,
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
            upload_id: multipart_upload_id("mpu-correct"),
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

    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: stream_session_id("sp-mismatch"),
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            target: StreamUploadTarget::UploadPart {
                upload_id: multipart_upload_id("mpu-correct"),
                part_number: 1,
            },
            encryption: ObjectEncryption::None,
        })
        .unwrap();

    // Commit with wrong upload_id in part record
    let err = store
        .commit_stream_part(
            stream_session_id("sp-mismatch").as_str(),
            &MultipartPartRecord {
                upload_id: multipart_upload_id("mpu-WRONG"),
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
fn commit_stream_part_zero_segments_clears_prior() {
    let (_dir, store) = make_pg_store();

    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: multipart_upload_id("mpu-zc"),
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

    // First upload: 2 segments
    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: stream_session_id("sp-zc1"),
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            target: StreamUploadTarget::UploadPart {
                upload_id: multipart_upload_id("mpu-zc"),
                part_number: 1,
            },
            encryption: ObjectEncryption::None,
        })
        .unwrap();
    store
        .commit_stream_part(
            stream_session_id("sp-zc1").as_str(),
            &MultipartPartRecord {
                upload_id: multipart_upload_id("mpu-zc"),
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
                    bucket: bucket_name("bucket"),
                    key: object_key("k"),
                    upload_id: multipart_upload_id("mpu-zc"),
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
                    bucket: bucket_name("bucket"),
                    key: object_key("k"),
                    upload_id: multipart_upload_id("mpu-zc"),
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
            .get_multipart_part_segments(
                &bucket_name("bucket"),
                &object_key("k"),
                VersionId::from_u64(u64::MAX),
                1,
            )
            .unwrap()
            .len(),
        2
    );

    // Re-upload with zero segments — must clear prior rows
    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: stream_session_id("sp-zc2"),
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            target: StreamUploadTarget::UploadPart {
                upload_id: multipart_upload_id("mpu-zc"),
                part_number: 1,
            },
            encryption: ObjectEncryption::None,
        })
        .unwrap();
    store
        .commit_stream_part(
            stream_session_id("sp-zc2").as_str(),
            &MultipartPartRecord {
                upload_id: multipart_upload_id("mpu-zc"),
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
            &[], // zero segments
        )
        .unwrap();

    let segments = store
        .get_multipart_part_segments(
            &bucket_name("bucket"),
            &object_key("k"),
            VersionId::from_u64(u64::MAX),
            1,
        )
        .unwrap();
    assert!(
        segments.is_empty(),
        "stale segments should be deleted on zero-segment re-upload"
    );
}

#[test]
fn commit_stream_put_rejects_mismatched_segment_target() {
    let (_dir, store) = make_pg_store();

    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: stream_session_id("sp-ct"),
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            target: StreamUploadTarget::PutObject,
            encryption: ObjectEncryption::None,
        })
        .unwrap();

    // Segment with wrong bucket
    let err = store
        .commit_stream_put(
            stream_session_id("sp-ct").as_str(),
            &CommitStreamPutReq {
                bucket: bucket_name("bucket"),
                key: object_key("k"),
                version_id: VersionId::Null,
                owner: test_owner(),
                acl_grants: AclGrants::default(),
                public_read: false,
                generation_id: GenerationId::MIN,
                size: 100,
                etag_crc64: 1u64,
                ec: EcShape { k: 4, m: 2 },
                tags: None,
                metadata_blob: None,
                system_metadata_blob: None,
                object_lock: ObjectLockState::default(),
                encryption: ObjectEncryption::None,
            },
            &[ObjectSegmentRecord {
                bucket: bucket_name("wrong-bucket"),
                key: object_key("k"),
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
        "expected rejection for mismatched segment bucket, got: {err:?}"
    );
}

#[test]
fn object_segments_reclaim_round_trip() {
    let (_dir, store) = make_pg_store();

    let generation_id = GenerationId::new(7).unwrap();
    let reclaim = ObjectSegmentsReclaimRecord {
        bucket: bucket_name("bucket"),
        key: object_key("k"),
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
        .get_object_segments_reclaim(&bucket_name("bucket"), &object_key("k"), generation_id)
        .unwrap()
        .expect("object segments reclaim should exist");
    assert_eq!(got, reclaim);

    store
        .delete_object_segments_reclaim(&bucket_name("bucket"), &object_key("k"), generation_id)
        .unwrap();
    assert!(store
        .get_object_segments_reclaim(&bucket_name("bucket"), &object_key("k"), generation_id)
        .unwrap()
        .is_none());
}

#[test]
fn next_generation_id_skips_object_segments_reclaim_generation() {
    let (_dir, store) = make_pg_store();

    store
        .put_object_segments_reclaim(&ObjectSegmentsReclaimRecord {
            bucket: bucket_name("bucket"),
            key: object_key("k"),
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
        store
            .next_generation_id(&bucket_name("bucket"), &object_key("k"))
            .unwrap(),
        GenerationId::new(8).unwrap()
    );
}

#[test]
fn multipart_reclaim_round_trip() {
    let (_dir, store) = make_pg_store();

    let generation_id = GenerationId::new(9).unwrap();
    let reclaim = MultipartReclaimRecord {
        bucket: bucket_name("bucket"),
        key: object_key("k"),
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
        .get_multipart_reclaim(&bucket_name("bucket"), &object_key("k"), generation_id)
        .unwrap()
        .expect("multipart reclaim should exist");
    assert_eq!(got, reclaim);

    store
        .delete_multipart_reclaim(&bucket_name("bucket"), &object_key("k"), generation_id)
        .unwrap();
    assert!(store
        .get_multipart_reclaim(&bucket_name("bucket"), &object_key("k"), generation_id)
        .unwrap()
        .is_none());
}

#[test]
fn next_generation_id_skips_multipart_reclaim_generation() {
    let (_dir, store) = make_pg_store();

    store
        .put_multipart_reclaim(&MultipartReclaimRecord {
            bucket: bucket_name("bucket"),
            key: object_key("k"),
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
        store
            .next_generation_id(&bucket_name("bucket"), &object_key("k"))
            .unwrap(),
        GenerationId::new(12).unwrap()
    );
}

#[test]
fn get_bucket_payload_reclaim_root_returns_first_root() {
    let (_dir, store) = make_pg_store();

    store
        .put_multipart_reclaim(&MultipartReclaimRecord {
            bucket: bucket_name("bucket"),
            key: object_key("z"),
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
            bucket: bucket_name("bucket"),
            key: object_key("a"),
            generation_id: GenerationId::new(3).unwrap(),
            ec: EcShape { k: 4, m: 2 },
            created_at: 1,
        })
        .unwrap();

    let root = store
        .get_bucket_payload_reclaim_root(&bucket_name("bucket"))
        .unwrap()
        .expect("bucket reclaim root should exist");
    assert_eq!(root.bucket, "bucket");
    assert_eq!(root.key, "a");
    assert_eq!(root.generation_id, GenerationId::new(3).unwrap());
}

#[test]
fn payload_reclaim_exists_checks_all_reclaim_tables() {
    let (_dir, store) = make_pg_store();

    let simple_generation = GenerationId::new(3).unwrap();
    let segments_generation = GenerationId::new(7).unwrap();
    let multipart_generation = GenerationId::new(11).unwrap();

    assert!(!store
        .payload_reclaim_exists(&bucket_name("bucket"), &object_key("k"), simple_generation)
        .unwrap());

    store
        .put_simple_payload_reclaim(&SimplePayloadReclaimRecord {
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            generation_id: simple_generation,
            ec: EcShape { k: 4, m: 2 },
            created_at: 1,
        })
        .unwrap();
    assert!(store
        .payload_reclaim_exists(&bucket_name("bucket"), &object_key("k"), simple_generation)
        .unwrap());
    store
        .delete_simple_payload_reclaim(&bucket_name("bucket"), &object_key("k"), simple_generation)
        .unwrap();

    store
        .put_object_segments_reclaim(&ObjectSegmentsReclaimRecord {
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            generation_id: segments_generation,
            created_at: 1,
            segments: vec![ObjectSegmentsReclaimSegmentRecord {
                segment_index: 0,
                segment_okh: [0x12; 16],
                segment_vid: GenerationId::new(21).unwrap(),
                shard_pg_id: 0,
                ec: EcShape { k: 4, m: 2 },
            }],
        })
        .unwrap();
    assert!(store
        .payload_reclaim_exists(
            &bucket_name("bucket"),
            &object_key("k"),
            segments_generation
        )
        .unwrap());
    store
        .delete_object_segments_reclaim(
            &bucket_name("bucket"),
            &object_key("k"),
            segments_generation,
        )
        .unwrap();

    store
        .put_multipart_reclaim(&MultipartReclaimRecord {
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            generation_id: multipart_generation,
            created_at: 1,
            parts: vec![MultipartReclaimPartRecord::ShardSet {
                part_number: 1,
                part_okh: [0x34; 16],
                part_vid: GenerationId::new(22).unwrap(),
                shard_pg_id: 0,
                ec: EcShape { k: 4, m: 2 },
            }],
        })
        .unwrap();
    assert!(store
        .payload_reclaim_exists(
            &bucket_name("bucket"),
            &object_key("k"),
            multipart_generation
        )
        .unwrap());
    store
        .delete_multipart_reclaim(
            &bucket_name("bucket"),
            &object_key("k"),
            multipart_generation,
        )
        .unwrap();

    assert!(!store
        .payload_reclaim_exists(
            &bucket_name("bucket"),
            &object_key("k"),
            multipart_generation
        )
        .unwrap());
}

#[test]
fn commit_stream_part_rejects_mismatched_segment_part_number() {
    let (_dir, store) = make_pg_store();

    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: multipart_upload_id("mpu-cpc"),
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

    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: stream_session_id("sp-cpc"),
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            target: StreamUploadTarget::UploadPart {
                upload_id: multipart_upload_id("mpu-cpc"),
                part_number: 1,
            },
            encryption: ObjectEncryption::None,
        })
        .unwrap();

    // Chunk has part_number=99, but session is for part 1
    let err = store
        .commit_stream_part(
            stream_session_id("sp-cpc").as_str(),
            &MultipartPartRecord {
                upload_id: multipart_upload_id("mpu-cpc"),
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
                bucket: bucket_name("bucket"),
                key: object_key("k"),
                upload_id: multipart_upload_id("mpu-cpc"),
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
        "expected rejection for mismatched segment part_number, got: {err:?}"
    );
}

#[test]
fn commit_stream_part_rejects_non_staging_segment_version_id() {
    let (_dir, store) = make_pg_store();

    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: multipart_upload_id("mpu-vid"),
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

    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: stream_session_id("sp-vid"),
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            target: StreamUploadTarget::UploadPart {
                upload_id: multipart_upload_id("mpu-vid"),
                part_number: 1,
            },
            encryption: ObjectEncryption::None,
        })
        .unwrap();

    // Segment has version_id=42 — must be PART_SEGMENT_STAGING_VERSION_ID (u64::MAX) pre-CompleteMultipartUpload
    let err = store
        .commit_stream_part(
            stream_session_id("sp-vid").as_str(),
            &MultipartPartRecord {
                upload_id: multipart_upload_id("mpu-vid"),
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
                bucket: bucket_name("bucket"),
                key: object_key("k"),
                upload_id: multipart_upload_id("mpu-vid"),
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
        "expected rejection for non-staging segment version_id, got: {err:?}"
    );
}

#[test]
fn malformed_segment_okh_returns_db_error() {
    let (_dir, store) = make_pg_store();

    // Insert a segment with wrong-length okh directly via SQL
    let conn = store.connection();
    conn.execute(
        "INSERT INTO object_segments \
         (bucket, key, version_id, segment_index, size, segment_okh, segment_vid, \
          shard_pg_id, ec_k, ec_m) \
         VALUES ('bucket', 'k', 0, 0, 100, X'AABB', 1, 0, 4, 2)",
        [],
    )
    .unwrap();

    let err = store
        .get_object_segments(&bucket_name("bucket"), &object_key("k"), VersionId::Null)
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
         VALUES ('bucket', 'k', ?1, 0, 1, 0, 100, X'AABB', 1, 0, 4, 2)",
        [multipart_upload_id("mpu-bad").into_string()],
    )
    .unwrap();

    let err = store
        .get_all_multipart_part_segments_for_upload(&multipart_upload_id("mpu-bad"))
        .unwrap_err();
    assert!(
        matches!(err, crate::error::MetadataError::Db { .. }),
        "expected Db error for malformed multipart segment okh, got: {err:?}"
    );
}

// ── get_object_version / delete_object_version ─────────────────────────

#[test]
fn get_object_version_null() {
    let (_dir, store) = make_pg_store();
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
            size: 100,
            etag: ObjectEtag::SinglePart([1, 0, 0, 0, 0, 0, 0, 0]),
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: None,
            system_metadata_blob: None,
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
        }))
        .unwrap();

    let obj = store
        .get_object_version(&bucket_name("bucket"), &object_key("k"), VersionId::Null)
        .unwrap();
    let live = obj.as_live().unwrap();
    assert_eq!(live.size, 100);
}

#[test]
fn get_object_version_versioned() {
    let (_dir, store) = make_pg_store();
    let vid = VersionId::Versioned(NonZeroU64::new(1).unwrap());
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
            size: 200,
            etag: ObjectEtag::SinglePart([2, 0, 0, 0, 0, 0, 0, 0]),
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: None,
            system_metadata_blob: None,
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
        }))
        .unwrap();

    let obj = store
        .get_object_version(&bucket_name("bucket"), &object_key("k"), vid)
        .unwrap();
    let live = obj.as_live().unwrap();
    assert_eq!(live.size, 200);
    assert_eq!(live.version_id, vid);
}

#[test]
fn get_object_version_not_found() {
    let (_dir, store) = make_pg_store();
    let err = store
        .get_object_version(&bucket_name("bucket"), &object_key("k"), VersionId::Null)
        .unwrap_err();
    assert!(matches!(err, crate::error::MetadataError::ObjectNotFound));
}

#[test]
fn delete_object_version_null() {
    let (_dir, store) = make_pg_store();
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
            size: 100,
            etag: ObjectEtag::SinglePart([1, 0, 0, 0, 0, 0, 0, 0]),
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: None,
            system_metadata_blob: None,
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
        }))
        .unwrap();

    store
        .delete_object_version(&bucket_name("bucket"), &object_key("k"), VersionId::Null)
        .unwrap();
    let err = store
        .get_object_version(&bucket_name("bucket"), &object_key("k"), VersionId::Null)
        .unwrap_err();
    assert!(matches!(err, crate::error::MetadataError::ObjectNotFound));
}

#[test]
fn delete_object_version_specific_leaves_others() {
    let (_dir, store) = make_pg_store();
    let vid1 = VersionId::Versioned(NonZeroU64::new(1).unwrap());
    let vid2 = VersionId::Versioned(NonZeroU64::new(2).unwrap());

    for (vid, size) in [(vid1, 100u64), (vid2, 200u64)] {
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
                size,
                etag: ObjectEtag::SinglePart([1, 0, 0, 0, 0, 0, 0, 0]),
                layout: ObjectLayout::Standard,
                tags: None,
                metadata_blob: None,
                system_metadata_blob: None,
                object_lock: ObjectLockState::default(),
                encryption: ObjectEncryption::None,
            }))
            .unwrap();
    }

    store
        .delete_object_version(&bucket_name("bucket"), &object_key("k"), vid1)
        .unwrap();

    // vid1 gone
    let err = store
        .get_object_version(&bucket_name("bucket"), &object_key("k"), vid1)
        .unwrap_err();
    assert!(matches!(err, crate::error::MetadataError::ObjectNotFound));
    // vid2 still exists
    let obj = store
        .get_object_version(&bucket_name("bucket"), &object_key("k"), vid2)
        .unwrap();
    assert_eq!(obj.as_live().unwrap().size, 200);
}

// ── list_object_versions ───────────────────────────────────────────────

#[test]
fn list_object_versions_basic() {
    let (_dir, store) = make_pg_store();
    let vid1 = VersionId::Versioned(NonZeroU64::new(1).unwrap());
    let vid2 = VersionId::Versioned(NonZeroU64::new(2).unwrap());

    for (vid, size) in [(vid1, 100u64), (vid2, 200)] {
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
                size,
                etag: ObjectEtag::SinglePart([1, 0, 0, 0, 0, 0, 0, 0]),
                layout: ObjectLayout::Standard,
                tags: None,
                metadata_blob: None,
                system_metadata_blob: None,
                object_lock: ObjectLockState::default(),
                encryption: ObjectEncryption::None,
            }))
            .unwrap();
    }

    let resp = store
        .list_object_versions(&ListObjectVersionsReq {
            bucket: bucket_name("bucket"),
            prefix: None,
            key_marker: None,
            version_id_marker: None,
            max_keys: 10,
        })
        .unwrap();

    assert_eq!(resp.versions.len(), 2);
    assert!(!resp.is_truncated);
}

#[test]
fn list_object_versions_pagination() {
    let (_dir, store) = make_pg_store();

    // Create 3 versions of the same key
    for i in 1..=3u64 {
        store
            .put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
                bucket: bucket_name("bucket"),
                key: object_key("k"),
                version_id: VersionId::Versioned(NonZeroU64::new(i).unwrap()),
                owner: test_owner(),
                acl_grants: AclGrants::default(),
                public_read: false,
                generation_id: GenerationId::MIN,
                ec: EcShape { k: 4, m: 2 },
                size: i * 100,
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

    // Page 1: max_keys=2
    let resp = store
        .list_object_versions(&ListObjectVersionsReq {
            bucket: bucket_name("bucket"),
            prefix: None,
            key_marker: None,
            version_id_marker: None,
            max_keys: 2,
        })
        .unwrap();
    assert_eq!(resp.versions.len(), 2);
    assert!(resp.is_truncated);
    assert!(resp.next_key_marker.is_some());
    assert!(resp.next_version_id_marker.is_some());

    // Page 2: use markers from page 1
    let resp2 = store
        .list_object_versions(&ListObjectVersionsReq {
            bucket: bucket_name("bucket"),
            prefix: None,
            key_marker: resp.next_key_marker,
            version_id_marker: resp.next_version_id_marker,
            max_keys: 2,
        })
        .unwrap();
    assert_eq!(resp2.versions.len(), 1);
    assert!(!resp2.is_truncated);
}

#[test]
fn list_object_versions_with_prefix() {
    let (_dir, store) = make_pg_store();

    for key in ["photos/a.jpg", "photos/b.jpg", "docs/c.txt"] {
        store
            .put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
                bucket: bucket_name("bucket"),
                key: object_key(key),
                version_id: VersionId::Null,
                owner: test_owner(),
                acl_grants: AclGrants::default(),
                public_read: false,
                generation_id: GenerationId::MIN,
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
    }

    let resp = store
        .list_object_versions(&ListObjectVersionsReq {
            bucket: bucket_name("bucket"),
            prefix: Some(object_key("photos/")),
            key_marker: None,
            version_id_marker: None,
            max_keys: 100,
        })
        .unwrap();
    assert_eq!(resp.versions.len(), 2);
}

#[test]
fn list_object_versions_includes_delete_markers() {
    let (_dir, store) = make_pg_store();
    let vid1 = VersionId::Versioned(NonZeroU64::new(1).unwrap());
    let vid2 = VersionId::Versioned(NonZeroU64::new(2).unwrap());

    store
        .put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            version_id: vid1,
            owner: test_owner(),
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id: GenerationId::MIN,
            ec: EcShape { k: 4, m: 2 },
            size: 100,
            etag: ObjectEtag::SinglePart([1, 0, 0, 0, 0, 0, 0, 0]),
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: None,
            system_metadata_blob: None,
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
        }))
        .unwrap();

    store
        .put_object_meta(&PutObjectReq::DeleteMarker(PutDeleteMarkerReq {
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            version_id: vid2,
            owner: test_owner(),
        }))
        .unwrap();

    let resp = store
        .list_object_versions(&ListObjectVersionsReq {
            bucket: bucket_name("bucket"),
            prefix: None,
            key_marker: None,
            version_id_marker: None,
            max_keys: 10,
        })
        .unwrap();
    assert_eq!(resp.versions.len(), 2);

    let has_live = resp.versions.iter().any(|v| !v.is_delete_marker());
    let has_dm = resp.versions.iter().any(|v| v.is_delete_marker());
    assert!(has_live);
    assert!(has_dm);
}

#[test]
fn suspended_null_live_version_stays_current_when_last_modified_ties() {
    let (_dir, store) = make_pg_store();
    let numbered = VersionId::Versioned(NonZeroU64::new(1).unwrap());

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
        .put_bucket_versioning(&bucket_name("bucket"), BucketVersioningState::Suspended)
        .unwrap();

    store
        .put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            version_id: numbered,
            owner: test_owner(),
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id: GenerationId::MIN,
            ec: EcShape { k: 4, m: 2 },
            size: 100,
            etag: ObjectEtag::SinglePart([1, 0, 0, 0, 0, 0, 0, 0]),
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: None,
            system_metadata_blob: None,
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
        }))
        .unwrap();
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
            size: 200,
            etag: ObjectEtag::SinglePart([2, 0, 0, 0, 0, 0, 0, 0]),
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: None,
            system_metadata_blob: None,
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
        }))
        .unwrap();
    store
        .connection()
        .execute(
            "UPDATE objects SET last_modified = ?1 WHERE bucket = ?2 AND key = ?3",
            rusqlite::params![1_i64, "bucket", "k"],
        )
        .unwrap();

    let current = store
        .get_object_meta(&bucket_name("bucket"), &object_key("k"))
        .unwrap();
    assert_eq!(current.version_id(), VersionId::Null);
    assert_eq!(current.as_live().unwrap().size, 200);

    let listed = store
        .list_objects(&ListObjectsReq {
            bucket: bucket_name("bucket"),
            prefix: None,
            start_after: None,
            start_at: None,
            max_keys: 10,
        })
        .unwrap();
    assert_eq!(listed.objects.len(), 1);
    assert_eq!(listed.objects[0].version_id(), VersionId::Null);
    assert_eq!(listed.objects[0].as_live().unwrap().size, 200);

    let versions = store
        .list_object_versions(&ListObjectVersionsReq {
            bucket: bucket_name("bucket"),
            prefix: None,
            key_marker: None,
            version_id_marker: None,
            max_keys: 10,
        })
        .unwrap();
    assert_eq!(versions.versions.len(), 2);
    assert_eq!(versions.versions[0].version_id(), VersionId::Null);
    assert_eq!(versions.versions[1].version_id(), numbered);
}

#[test]
fn suspended_null_delete_marker_stays_current_when_last_modified_ties() {
    let (_dir, store) = make_pg_store();
    let numbered = VersionId::Versioned(NonZeroU64::new(1).unwrap());

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
        .put_bucket_versioning(&bucket_name("bucket"), BucketVersioningState::Suspended)
        .unwrap();

    store
        .put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            version_id: numbered,
            owner: test_owner(),
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id: GenerationId::MIN,
            ec: EcShape { k: 4, m: 2 },
            size: 100,
            etag: ObjectEtag::SinglePart([1, 0, 0, 0, 0, 0, 0, 0]),
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: None,
            system_metadata_blob: None,
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
        }))
        .unwrap();
    store
        .put_object_meta(&PutObjectReq::DeleteMarker(PutDeleteMarkerReq {
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            version_id: VersionId::Null,
            owner: test_owner(),
        }))
        .unwrap();
    store
        .connection()
        .execute(
            "UPDATE objects SET last_modified = ?1 WHERE bucket = ?2 AND key = ?3",
            rusqlite::params![1_i64, "bucket", "k"],
        )
        .unwrap();

    let current = store
        .get_object_meta(&bucket_name("bucket"), &object_key("k"))
        .unwrap();
    assert_eq!(current.version_id(), VersionId::Null);
    assert!(current.is_delete_marker());

    let listed = store
        .list_objects(&ListObjectsReq {
            bucket: bucket_name("bucket"),
            prefix: None,
            start_after: None,
            start_at: None,
            max_keys: 10,
        })
        .unwrap();
    assert!(listed.objects.is_empty());

    let versions = store
        .list_object_versions(&ListObjectVersionsReq {
            bucket: bucket_name("bucket"),
            prefix: None,
            key_marker: None,
            version_id_marker: None,
            max_keys: 10,
        })
        .unwrap();
    assert_eq!(versions.versions.len(), 2);
    assert_eq!(versions.versions[0].version_id(), VersionId::Null);
    assert!(versions.versions[0].is_delete_marker());
    assert_eq!(versions.versions[1].version_id(), numbered);
    assert!(!versions.versions[1].is_delete_marker());
}

#[test]
fn put_object_meta_marks_displaced_live_version_noncurrent() {
    let (_dir, store) = make_pg_store();
    let older = VersionId::Versioned(NonZeroU64::new(1).unwrap());
    let current = VersionId::Versioned(NonZeroU64::new(2).unwrap());

    for version_id in [older, current] {
        store
            .put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
                bucket: bucket_name("bucket"),
                key: object_key("k"),
                version_id,
                owner: test_owner(),
                acl_grants: AclGrants::default(),
                public_read: false,
                generation_id: GenerationId::MIN,
                ec: EcShape { k: 4, m: 2 },
                size: 100,
                etag: ObjectEtag::SinglePart([version_id.to_u64() as u8, 0, 0, 0, 0, 0, 0, 0]),
                layout: ObjectLayout::Standard,
                tags: None,
                metadata_blob: None,
                system_metadata_blob: None,
                object_lock: ObjectLockState::default(),
                encryption: ObjectEncryption::None,
            }))
            .unwrap();
    }

    let current_record = store
        .get_object_version(&bucket_name("bucket"), &object_key("k"), current)
        .unwrap();
    let current_live = current_record.as_live().unwrap();
    let older_record = store
        .get_object_version(&bucket_name("bucket"), &object_key("k"), older)
        .unwrap();
    let older_live = older_record.as_live().unwrap();

    assert_eq!(current_live.became_noncurrent_at, None);
    assert_eq!(
        older_live.became_noncurrent_at,
        Some(current_live.last_modified)
    );
}

#[test]
fn put_delete_marker_marks_displaced_live_version_noncurrent() {
    let (_dir, store) = make_pg_store();
    let live_version = VersionId::Versioned(NonZeroU64::new(1).unwrap());
    let delete_marker = VersionId::Versioned(NonZeroU64::new(2).unwrap());

    store
        .put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            version_id: live_version,
            owner: test_owner(),
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id: GenerationId::MIN,
            ec: EcShape { k: 4, m: 2 },
            size: 100,
            etag: ObjectEtag::SinglePart([1, 0, 0, 0, 0, 0, 0, 0]),
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: None,
            system_metadata_blob: None,
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
        }))
        .unwrap();
    store
        .put_object_meta(&PutObjectReq::DeleteMarker(PutDeleteMarkerReq {
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            version_id: delete_marker,
            owner: test_owner(),
        }))
        .unwrap();

    let current = store
        .get_object_meta(&bucket_name("bucket"), &object_key("k"))
        .unwrap();
    assert!(current.is_delete_marker());

    let older_record = store
        .get_object_version(&bucket_name("bucket"), &object_key("k"), live_version)
        .unwrap();
    let older_live = older_record.as_live().unwrap();
    assert_eq!(
        older_live.became_noncurrent_at,
        Some(current.last_modified())
    );
}

#[test]
fn null_live_write_marks_displaced_numbered_version_noncurrent() {
    let (_dir, store) = make_pg_store();
    let numbered = VersionId::Versioned(NonZeroU64::new(1).unwrap());

    store
        .put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            version_id: numbered,
            owner: test_owner(),
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id: GenerationId::MIN,
            ec: EcShape { k: 4, m: 2 },
            size: 100,
            etag: ObjectEtag::SinglePart([1, 0, 0, 0, 0, 0, 0, 0]),
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: None,
            system_metadata_blob: None,
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
        }))
        .unwrap();
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
            size: 200,
            etag: ObjectEtag::SinglePart([2, 0, 0, 0, 0, 0, 0, 0]),
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: None,
            system_metadata_blob: None,
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
        }))
        .unwrap();

    let current = store
        .get_object_meta(&bucket_name("bucket"), &object_key("k"))
        .unwrap();
    assert_eq!(current.version_id(), VersionId::Null);

    let older_record = store
        .get_object_version(&bucket_name("bucket"), &object_key("k"), numbered)
        .unwrap();
    let older_live = older_record.as_live().unwrap();
    assert_eq!(
        older_live.became_noncurrent_at,
        Some(current.last_modified())
    );
}

#[test]
fn deleting_current_live_version_clears_revealed_live_noncurrent_timestamp() {
    let (_dir, store) = make_pg_store();
    let older = VersionId::Versioned(NonZeroU64::new(1).unwrap());
    let current = VersionId::Versioned(NonZeroU64::new(2).unwrap());

    for version_id in [older, current] {
        store
            .put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
                bucket: bucket_name("bucket"),
                key: object_key("k"),
                version_id,
                owner: test_owner(),
                acl_grants: AclGrants::default(),
                public_read: false,
                generation_id: GenerationId::MIN,
                ec: EcShape { k: 4, m: 2 },
                size: 100,
                etag: ObjectEtag::SinglePart([version_id.to_u64() as u8, 0, 0, 0, 0, 0, 0, 0]),
                layout: ObjectLayout::Standard,
                tags: None,
                metadata_blob: None,
                system_metadata_blob: None,
                object_lock: ObjectLockState::default(),
                encryption: ObjectEncryption::None,
            }))
            .unwrap();
    }

    let older_record = store
        .get_object_version(&bucket_name("bucket"), &object_key("k"), older)
        .unwrap();
    assert!(older_record
        .as_live()
        .unwrap()
        .became_noncurrent_at
        .is_some());

    store
        .delete_object_version(&bucket_name("bucket"), &object_key("k"), current)
        .unwrap();

    let revealed = store
        .get_object_meta(&bucket_name("bucket"), &object_key("k"))
        .unwrap();
    assert_eq!(revealed.version_id(), older);
    assert_eq!(revealed.as_live().unwrap().became_noncurrent_at, None);
}

#[test]
fn deleting_current_delete_marker_clears_revealed_live_noncurrent_timestamp() {
    let (_dir, store) = make_pg_store();
    let live_version = VersionId::Versioned(NonZeroU64::new(1).unwrap());
    let delete_marker = VersionId::Versioned(NonZeroU64::new(2).unwrap());

    store
        .put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            version_id: live_version,
            owner: test_owner(),
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id: GenerationId::MIN,
            ec: EcShape { k: 4, m: 2 },
            size: 100,
            etag: ObjectEtag::SinglePart([1, 0, 0, 0, 0, 0, 0, 0]),
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: None,
            system_metadata_blob: None,
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
        }))
        .unwrap();
    store
        .put_object_meta(&PutObjectReq::DeleteMarker(PutDeleteMarkerReq {
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            version_id: delete_marker,
            owner: test_owner(),
        }))
        .unwrap();

    let live_record = store
        .get_object_version(&bucket_name("bucket"), &object_key("k"), live_version)
        .unwrap();
    assert!(live_record
        .as_live()
        .unwrap()
        .became_noncurrent_at
        .is_some());

    store
        .delete_object_version(&bucket_name("bucket"), &object_key("k"), delete_marker)
        .unwrap();

    let revealed = store
        .get_object_meta(&bucket_name("bucket"), &object_key("k"))
        .unwrap();
    assert_eq!(revealed.version_id(), live_version);
    assert_eq!(revealed.as_live().unwrap().became_noncurrent_at, None);
}

// ── next_version_id ────────────────────────────────────────────────────

#[test]
fn next_version_id_increments() {
    let (_dir, store) = make_pg_store();

    // First call with no prior versions starts at 1.
    let v1 = store
        .next_version_id(&bucket_name("bucket"), &object_key("k"))
        .unwrap();
    assert!(matches!(v1, VersionId::Versioned(_)));

    // Write an object at v1 so the counter advances.
    store
        .put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            version_id: v1,
            owner: test_owner(),
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id: GenerationId::MIN,
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

    let v2 = store
        .next_version_id(&bucket_name("bucket"), &object_key("k"))
        .unwrap();
    assert!(matches!(v2, VersionId::Versioned(_)));
    assert_ne!(v1, v2);
}

#[test]
fn next_version_id_independent_per_key() {
    let (_dir, store) = make_pg_store();

    let v1 = store
        .next_version_id(&bucket_name("bucket"), &object_key("k1"))
        .unwrap();
    let v2 = store
        .next_version_id(&bucket_name("bucket"), &object_key("k2"))
        .unwrap();
    // Different keys should each get the first version ID
    assert_eq!(v1, v2);
}

// ── object tags ────────────────────────────────────────────────────────

#[test]
fn object_tags_round_trip() {
    let (_dir, store) = make_pg_store();
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
            size: 100,
            etag: ObjectEtag::SinglePart([1, 0, 0, 0, 0, 0, 0, 0]),
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: None,
            system_metadata_blob: None,
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
        }))
        .unwrap();

    // Initially no tags
    let tags = store
        .get_object_tags(&bucket_name("bucket"), &object_key("k"), VersionId::Null)
        .unwrap();
    assert!(tags.is_none());

    // Put tags
    store
        .put_object_tags(
            &bucket_name("bucket"),
            &object_key("k"),
            VersionId::Null,
            "<tags>env=prod</tags>",
        )
        .unwrap();
    let tags = store
        .get_object_tags(&bucket_name("bucket"), &object_key("k"), VersionId::Null)
        .unwrap();
    assert_eq!(tags.as_deref(), Some("<tags>env=prod</tags>"));

    // Overwrite tags
    store
        .put_object_tags(
            &bucket_name("bucket"),
            &object_key("k"),
            VersionId::Null,
            "<tags>env=staging</tags>",
        )
        .unwrap();
    let tags = store
        .get_object_tags(&bucket_name("bucket"), &object_key("k"), VersionId::Null)
        .unwrap();
    assert_eq!(tags.as_deref(), Some("<tags>env=staging</tags>"));

    // Delete tags
    store
        .delete_object_tags(&bucket_name("bucket"), &object_key("k"), VersionId::Null)
        .unwrap();
    let tags = store
        .get_object_tags(&bucket_name("bucket"), &object_key("k"), VersionId::Null)
        .unwrap();
    assert!(tags.is_none());
}

#[test]
fn object_tags_on_nonexistent_object() {
    let (_dir, store) = make_pg_store();
    let err = store
        .get_object_tags(&bucket_name("bucket"), &object_key("k"), VersionId::Null)
        .unwrap_err();
    assert!(matches!(err, crate::error::MetadataError::ObjectNotFound));
}

#[test]
fn object_tags_on_delete_marker() {
    let (_dir, store) = make_pg_store();
    let vid = VersionId::Versioned(NonZeroU64::new(1).unwrap());
    store
        .put_object_meta(&PutObjectReq::DeleteMarker(PutDeleteMarkerReq {
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            version_id: vid,
            owner: test_owner(),
        }))
        .unwrap();

    let err = store
        .put_object_tags(&bucket_name("bucket"), &object_key("k"), vid, "<tags/>")
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::error::MetadataError::MethodNotAllowedOnDeleteMarker
        ),
        "expected MethodNotAllowedOnDeleteMarker, got {err:?}"
    );

    let err = store
        .get_object_tags(&bucket_name("bucket"), &object_key("k"), vid)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::error::MetadataError::MethodNotAllowedOnDeleteMarker
        ),
        "expected MethodNotAllowedOnDeleteMarker, got {err:?}"
    );

    let err = store
        .delete_object_tags(&bucket_name("bucket"), &object_key("k"), vid)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::error::MetadataError::MethodNotAllowedOnDeleteMarker
        ),
        "expected MethodNotAllowedOnDeleteMarker, got {err:?}"
    );
}

// ── simple payload reclaim round-trip ──────────────────────────────────

#[test]
fn simple_payload_reclaim_round_trip() {
    let (_dir, store) = make_pg_store();
    let gen = GenerationId::new(5).unwrap();

    store
        .put_simple_payload_reclaim(&SimplePayloadReclaimRecord {
            bucket: bucket_name("bucket"),
            key: object_key("k"),
            generation_id: gen,
            ec: EcShape { k: 4, m: 2 },
            created_at: 12345,
        })
        .unwrap();

    let rec = store
        .get_simple_payload_reclaim(&bucket_name("bucket"), &object_key("k"), gen)
        .unwrap();
    assert!(rec.is_some());
    let rec = rec.unwrap();
    assert_eq!(rec.bucket.as_str(), "bucket");
    assert_eq!(rec.key.as_str(), "k");
    assert_eq!(rec.generation_id, gen);
    assert_eq!(rec.ec.k, 4);
    assert_eq!(rec.ec.m, 2);

    store
        .delete_simple_payload_reclaim(&bucket_name("bucket"), &object_key("k"), gen)
        .unwrap();
    let rec = store
        .get_simple_payload_reclaim(&bucket_name("bucket"), &object_key("k"), gen)
        .unwrap();
    assert!(rec.is_none());
}

#[test]
fn simple_payload_reclaim_delete_idempotent() {
    let (_dir, store) = make_pg_store();
    // Deleting a nonexistent reclaim should not error
    store
        .delete_simple_payload_reclaim(&bucket_name("bucket"), &object_key("k"), GenerationId::MIN)
        .unwrap();
}

// ── list_all_stream_uploads ────────────────────────────────────────────

#[test]
fn list_all_stream_uploads_empty() {
    let (_dir, store) = make_pg_store();
    let uploads = store.list_all_stream_uploads().unwrap();
    assert!(uploads.is_empty());
}

#[test]
fn list_all_stream_uploads_returns_sessions() {
    let (_dir, store) = make_pg_store();

    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: stream_session_id("s1"),
            bucket: bucket_name("bucket"),
            key: object_key("k1"),
            target: StreamUploadTarget::PutObject,
            encryption: ObjectEncryption::None,
        })
        .unwrap();
    store
        .create_stream_upload(&CreateStreamUploadReq {
            session_id: stream_session_id("s2"),
            bucket: bucket_name("bucket"),
            key: object_key("k2"),
            target: StreamUploadTarget::PutObject,
            encryption: ObjectEncryption::None,
        })
        .unwrap();

    let uploads = store.list_all_stream_uploads().unwrap();
    assert_eq!(uploads.len(), 2);

    let ids: Vec<&str> = uploads.iter().map(|u| u.session_id.as_str()).collect();
    let s1 = stream_session_id("s1");
    let s2 = stream_session_id("s2");
    assert!(ids.contains(&s1.as_str()));
    assert!(ids.contains(&s2.as_str()));
}

// ── delete_multipart_part_segments_by_upload_id ────────────────────────

#[test]
fn delete_multipart_part_segments_by_upload_id_cleans_up() {
    let (_dir, store) = make_pg_store();

    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: multipart_upload_id("mpu-seg"),
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

    // Upsert a part (needed before segments can be inserted).
    store
        .upsert_multipart_part(&MultipartPartRecord {
            upload_id: multipart_upload_id("mpu-seg"),
            part_number: 1,
            generation: 0,
            size: 100,
            etag: vec![0xAA],
            etag_kind: EtagKind::Crc64,
            part_okh: [
                0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD,
                0xEE, 0xFF,
            ],
            part_vid: GenerationId::MIN,
            ec_k: 4,
            ec_m: 2,
            last_modified: 0,
            checksum: None,
        })
        .unwrap();

    // Insert segments via SQL since upsert_multipart_part_segments may have
    // requirements we can work around here.
    let conn = store.connection();
    conn.execute(
        "INSERT INTO multipart_part_segments \
         (bucket, key, upload_id, version_id, part_number, segment_index, size, \
          segment_okh, segment_vid, shard_pg_id, ec_k, ec_m) \
         VALUES ('bucket', 'k', ?1, 0, 1, 0, 100, X'00112233445566778899AABBCCDDEEFF', 1, 0, 4, 2)",
        [multipart_upload_id("mpu-seg").into_string()],
    )
    .unwrap();

    // Verify segments exist
    let segs = store
        .get_all_multipart_part_segments_for_upload(&multipart_upload_id("mpu-seg"))
        .unwrap();
    assert_eq!(segs.len(), 1);

    // Delete segments by upload_id
    store
        .delete_multipart_part_segments_by_upload_id(&multipart_upload_id("mpu-seg"))
        .unwrap();

    let segs = store
        .get_all_multipart_part_segments_for_upload(&multipart_upload_id("mpu-seg"))
        .unwrap();
    assert!(segs.is_empty());
}

#[test]
fn delete_multipart_part_segments_by_upload_id_noop_on_missing() {
    let (_dir, store) = make_pg_store();
    // Should not error on nonexistent upload_id
    store
        .delete_multipart_part_segments_by_upload_id(&multipart_upload_id("nonexistent"))
        .unwrap();
}

// ── mark_bucket_deleting / head_bucket_raw ─────────────────────────────

#[test]
fn mark_bucket_deleting_and_head_bucket_raw() {
    let (_dir, store) = make_pg_store();

    store
        .create_bucket(
            &bucket_name("mybucket"),
            "owner",
            &CanonicalUserId::from_principal("owner"),
            &AclGrants::default(),
            false,
            false,
        )
        .unwrap();

    // head_bucket sees it
    let info = store.head_bucket(&bucket_name("mybucket")).unwrap();
    assert_eq!(info.state, BucketState::Active);

    // head_bucket_raw also sees it
    let raw = store.head_bucket_raw(&bucket_name("mybucket")).unwrap();
    assert_eq!(raw.state, BucketState::Active);
    assert!(!raw.write_reservations_blocked);
    assert_eq!(raw.active_write_reservations, 0);

    store
        .begin_bucket_write_drain(&bucket_name("mybucket"))
        .unwrap();

    // Mark as deleting
    store
        .mark_bucket_deleting(&bucket_name("mybucket"))
        .unwrap();

    // head_bucket should NOT find it anymore (filters Deleting)
    let err = store.head_bucket(&bucket_name("mybucket")).unwrap_err();
    assert!(
        matches!(err, crate::error::MetadataError::BucketNotFound { .. }),
        "expected BucketNotFound from head_bucket on Deleting bucket, got {err:?}"
    );

    // head_bucket_raw SHOULD still find it (includes Deleting)
    let raw = store.head_bucket_raw(&bucket_name("mybucket")).unwrap();
    assert_eq!(raw.state, BucketState::Deleting);
    assert!(raw.write_reservations_blocked);
    assert_eq!(raw.active_write_reservations, 0);
}

#[test]
fn mark_bucket_deleting_nonexistent() {
    let (_dir, store) = make_pg_store();
    let err = store
        .mark_bucket_deleting(&bucket_name("nope"))
        .unwrap_err();
    assert!(
        matches!(err, crate::error::MetadataError::BucketNotFound { .. }),
        "expected BucketNotFound, got {err:?}"
    );
}

#[test]
fn head_bucket_raw_nonexistent() {
    let (_dir, store) = make_pg_store();
    let err = store.head_bucket_raw(&bucket_name("nope")).unwrap_err();
    assert!(
        matches!(err, crate::error::MetadataError::BucketNotFound { .. }),
        "expected BucketNotFound, got {err:?}"
    );
}

#[test]
fn bucket_name_validation_rejects_invalid_inputs() {
    assert!(matches!(
        BucketName::try_from("BadBucket"),
        Err(BucketNameError::InvalidCharacterSet | BucketNameError::InvalidStartCharacter)
    ));
    assert!(matches!(
        BucketName::try_from("b"),
        Err(BucketNameError::InvalidLength { length: 1 })
    ));
}

#[test]
fn object_key_validation_rejects_invalid_inputs() {
    assert!(matches!(
        ObjectKey::try_from(""),
        Err(ObjectKeyError::InvalidLength { length: 0 })
    ));
    assert!(matches!(
        ObjectKey::try_from("\0"),
        Err(ObjectKeyError::ContainsNullByte)
    ));
}

#[test]
fn bucket_write_reservations_and_drain_round_trip() {
    let (_dir, store) = make_pg_store();
    store
        .create_bucket(
            &bucket_name("mybucket"),
            "owner",
            &CanonicalUserId::from_principal("owner"),
            &AclGrants::default(),
            false,
            false,
        )
        .unwrap();

    let reserved = store
        .acquire_bucket_write_reservation(&bucket_name("mybucket"))
        .unwrap();
    assert_eq!(reserved.state, BucketState::Active);
    assert_eq!(reserved.active_write_reservations, 1);
    assert!(!reserved.write_reservations_blocked);

    let raw = store.head_bucket_raw(&bucket_name("mybucket")).unwrap();
    assert_eq!(raw.active_write_reservations, 1);
    assert!(!raw.write_reservations_blocked);

    store
        .release_bucket_write_reservation(&bucket_name("mybucket"))
        .unwrap();
    let raw = store.head_bucket_raw(&bucket_name("mybucket")).unwrap();
    assert_eq!(raw.active_write_reservations, 0);

    store
        .begin_bucket_write_drain(&bucket_name("mybucket"))
        .unwrap();
    let raw = store.head_bucket_raw(&bucket_name("mybucket")).unwrap();
    assert!(raw.write_reservations_blocked);
    assert_eq!(raw.active_write_reservations, 0);

    let err = store
        .acquire_bucket_write_reservation(&bucket_name("mybucket"))
        .unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::BucketWriteDraining
    ));

    store
        .end_bucket_write_drain(&bucket_name("mybucket"))
        .unwrap();
    let raw = store.head_bucket_raw(&bucket_name("mybucket")).unwrap();
    assert!(!raw.write_reservations_blocked);
    assert_eq!(raw.active_write_reservations, 0);
}

#[test]
fn mark_bucket_deleting_requires_drained_reservations() {
    let (_dir, store) = make_pg_store();
    store
        .create_bucket(
            &bucket_name("mybucket"),
            "owner",
            &CanonicalUserId::from_principal("owner"),
            &AclGrants::default(),
            false,
            false,
        )
        .unwrap();

    let err = store
        .mark_bucket_deleting(&bucket_name("mybucket"))
        .unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::BucketNotFound { .. }
    ));

    store
        .begin_bucket_write_drain(&bucket_name("mybucket"))
        .unwrap();
    store
        .acquire_bucket_write_reservation(&bucket_name("mybucket"))
        .err();
    let reservation = store.head_bucket_raw(&bucket_name("mybucket")).unwrap();
    assert_eq!(reservation.active_write_reservations, 0);

    store
        .end_bucket_write_drain(&bucket_name("mybucket"))
        .unwrap();
    store
        .acquire_bucket_write_reservation(&bucket_name("mybucket"))
        .unwrap();
    store
        .begin_bucket_write_drain(&bucket_name("mybucket"))
        .unwrap();
    let err = store
        .mark_bucket_deleting(&bucket_name("mybucket"))
        .unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::BucketNotFound { .. }
    ));

    store
        .release_bucket_write_reservation(&bucket_name("mybucket"))
        .unwrap();
    store
        .mark_bucket_deleting(&bucket_name("mybucket"))
        .unwrap();
}

// ── put_bucket_versioning on nonexistent bucket ────────────────────────

#[test]
fn put_bucket_versioning_nonexistent_bucket() {
    let (_dir, store) = make_pg_store();
    let err = store
        .put_bucket_versioning(&bucket_name("nope"), BucketVersioningState::Enabled)
        .unwrap_err();
    assert!(
        matches!(err, crate::error::MetadataError::BucketNotFound { .. }),
        "expected BucketNotFound, got {err:?}"
    );
}

// ── complete_multipart_commit error: no such upload ────────────────────

#[test]
fn complete_multipart_commit_no_such_upload() {
    let (_dir, store) = make_pg_store();
    let obj = CommitMultipartReq {
        bucket: bucket_name("bucket"),
        key: object_key("k"),
        version_id: VersionId::Null,
        owner: test_owner(),
        acl_grants: AclGrants::default(),
        public_read: false,
        generation_id: GenerationId::MIN,
        size: 1024,
        etag_crc64: [0; 8],
        ec: EcShape { k: 4, m: 2 },
        tags: None,
        metadata_blob: None,
        system_metadata_blob: None,
        object_lock: ObjectLockState::default(),
        encryption: ObjectEncryption::None,
    };
    let parts = vec![ObjectPartRecord {
        bucket: bucket_name("bucket"),
        key: object_key("k"),
        version_id: VersionId::Null,
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
    }];
    // Should fail — upload "nonexistent" does not exist.
    let err = store
        .complete_multipart_commit(&multipart_upload_id("nonexistent"), 1, &obj, &parts)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::error::MetadataError::NoSuchUpload { .. }
                | crate::error::MetadataError::Db { .. }
        ),
        "expected NoSuchUpload or Db error for missing upload, got {err:?}"
    );
}

#[test]
fn bucket_object_lock_round_trip() {
    let (_dir, store) = make_pg_store();
    store
        .create_bucket(
            &bucket_name("mybucket"),
            "owner",
            &CanonicalUserId::from_principal("owner"),
            &AclGrants::default(),
            false,
            false,
        )
        .unwrap();
    store
        .put_bucket_versioning(&bucket_name("mybucket"), BucketVersioningState::Enabled)
        .unwrap();

    let config = sample_bucket_object_lock();
    store
        .put_bucket_object_lock(&bucket_name("mybucket"), config)
        .unwrap();

    let info = store.head_bucket_raw(&bucket_name("mybucket")).unwrap();
    assert_eq!(info.object_lock, config);
}

#[test]
fn bucket_object_lock_requires_enabled_versioning() {
    let (_dir, store) = make_pg_store();
    store
        .create_bucket(
            &bucket_name("mybucket"),
            "owner",
            &CanonicalUserId::from_principal("owner"),
            &AclGrants::default(),
            false,
            false,
        )
        .unwrap();

    let config = sample_bucket_object_lock();
    let err = store
        .put_bucket_object_lock(&bucket_name("mybucket"), config)
        .unwrap_err();
    assert!(matches!(err, crate::error::MetadataError::Db { .. }));

    store
        .put_bucket_versioning(&bucket_name("mybucket"), BucketVersioningState::Suspended)
        .unwrap();
    let err = store
        .put_bucket_object_lock(&bucket_name("mybucket"), config)
        .unwrap_err();
    assert!(matches!(err, crate::error::MetadataError::Db { .. }));
}

#[test]
fn bucket_object_lock_rejects_default_retention_without_enablement() {
    let (_dir, store) = make_pg_store();
    store
        .create_bucket(
            &bucket_name("mybucket"),
            "owner",
            &CanonicalUserId::from_principal("owner"),
            &AclGrants::default(),
            false,
            false,
        )
        .unwrap();

    let err = store
        .put_bucket_object_lock(
            &bucket_name("mybucket"),
            BucketObjectLockConfig {
                enabled: false,
                default_retention: sample_bucket_object_lock().default_retention,
            },
        )
        .unwrap_err();
    assert!(matches!(err, crate::error::MetadataError::Db { .. }));
}

#[test]
fn bucket_object_lock_cannot_be_disabled_or_suspended() {
    let (_dir, store) = make_pg_store();
    store
        .create_bucket(
            &bucket_name("mybucket"),
            "owner",
            &CanonicalUserId::from_principal("owner"),
            &AclGrants::default(),
            false,
            false,
        )
        .unwrap();
    store
        .put_bucket_versioning(&bucket_name("mybucket"), BucketVersioningState::Enabled)
        .unwrap();
    store
        .put_bucket_object_lock(&bucket_name("mybucket"), sample_bucket_object_lock())
        .unwrap();

    let err = store
        .put_bucket_versioning(&bucket_name("mybucket"), BucketVersioningState::Suspended)
        .unwrap_err();
    assert!(matches!(err, crate::error::MetadataError::Db { .. }));

    let err = store
        .put_bucket_object_lock(&bucket_name("mybucket"), BucketObjectLockConfig::default())
        .unwrap_err();
    assert!(matches!(err, crate::error::MetadataError::Db { .. }));
}

#[test]
fn live_object_object_lock_round_trip() {
    let (_dir, store) = make_pg_store();
    let object_lock = sample_object_lock_state();

    store
        .put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
            bucket: bucket_name("bucket"),
            key: object_key("key"),
            version_id: VersionId::Versioned(NonZeroU64::new(1).unwrap()),
            owner: test_owner(),
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id: GenerationId::MIN,
            size: 16,
            etag: ObjectEtag::SinglePart([7; 8]),
            ec: EcShape { k: 4, m: 2 },
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: None,
            system_metadata_blob: None,
            object_lock,
            encryption: ObjectEncryption::None,
        }))
        .unwrap();

    let record = store
        .get_object_version(
            &bucket_name("bucket"),
            &object_key("key"),
            VersionId::Versioned(NonZeroU64::new(1).unwrap()),
        )
        .unwrap();
    assert_eq!(record.as_live().unwrap().object_lock, object_lock);
}

#[test]
fn put_object_retention_and_legal_hold_round_trip() {
    let (_dir, store) = make_pg_store();
    let version_id = VersionId::Versioned(NonZeroU64::new(1).unwrap());
    store
        .put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
            bucket: bucket_name("bucket"),
            key: object_key("key"),
            version_id,
            owner: test_owner(),
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id: GenerationId::MIN,
            size: 16,
            etag: ObjectEtag::SinglePart([7; 8]),
            ec: EcShape { k: 4, m: 2 },
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: None,
            system_metadata_blob: None,
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
        }))
        .unwrap();

    let retention = ObjectRetention {
        mode: ObjectLockMode::Governance,
        retain_until_unix_seconds: 5_364_662_400,
    };
    store
        .put_object_retention(
            &bucket_name("bucket"),
            &object_key("key"),
            version_id,
            retention,
        )
        .unwrap();
    store
        .put_object_legal_hold(
            &bucket_name("bucket"),
            &object_key("key"),
            version_id,
            StoredLegalHoldStatus::On,
        )
        .unwrap();

    let record = store
        .get_object_version(&bucket_name("bucket"), &object_key("key"), version_id)
        .unwrap();
    let live = record.as_live().unwrap();
    assert_eq!(live.object_lock.retention, Some(retention));
    assert_eq!(live.object_lock.legal_hold, StoredLegalHoldStatus::On);
}

#[test]
fn put_object_retention_and_legal_hold_reject_delete_marker() {
    let (_dir, store) = make_pg_store();
    let version_id = VersionId::Versioned(NonZeroU64::new(1).unwrap());
    store
        .put_object_meta(&PutObjectReq::DeleteMarker(PutDeleteMarkerReq {
            bucket: bucket_name("bucket"),
            key: object_key("key"),
            version_id,
            owner: test_owner(),
        }))
        .unwrap();

    let err = store
        .put_object_retention(
            &bucket_name("bucket"),
            &object_key("key"),
            version_id,
            ObjectRetention {
                mode: ObjectLockMode::Governance,
                retain_until_unix_seconds: 5_364_662_400,
            },
        )
        .unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::MethodNotAllowedOnDeleteMarker
    ));

    let err = store
        .put_object_legal_hold(
            &bucket_name("bucket"),
            &object_key("key"),
            version_id,
            StoredLegalHoldStatus::Off,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        crate::error::MetadataError::MethodNotAllowedOnDeleteMarker
    ));
}

#[test]
fn multipart_upload_object_lock_round_trip_and_commit_copies_state() {
    let (_dir, store) = make_pg_store();
    let object_lock = sample_object_lock_state();

    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: multipart_upload_id("upload-1"),
            bucket: bucket_name("bucket"),
            key: object_key("key"),
            tags: None,
            metadata_blob: SerializedMetadataBlob::default(),
            system_metadata_blob: SerializedSystemMetadataBlob::default(),
            initiator: None,
            owner: test_owner(),
            acl_grants: AclGrants::default(),
            public_read: false,
            object_lock,
            checksum: None,
            encryption: ObjectEncryption::None,
        })
        .unwrap();

    let upload = store
        .get_multipart_upload(&multipart_upload_id("upload-1"))
        .unwrap();
    assert_eq!(upload.object_lock, object_lock);
    let uploads = store
        .list_multipart_uploads(&ListMultipartUploadsReq {
            bucket: bucket_name("bucket"),
            prefix: None,
            key_marker: None,
            upload_id_marker: None,
            max_uploads: 10,
        })
        .unwrap();
    assert_eq!(uploads.uploads[0].object_lock, object_lock);

    let obj = CommitMultipartReq {
        bucket: bucket_name("bucket"),
        key: object_key("key"),
        version_id: VersionId::Versioned(NonZeroU64::new(1).unwrap()),
        owner: test_owner(),
        acl_grants: AclGrants::default(),
        public_read: false,
        generation_id: GenerationId::MIN,
        size: 32,
        etag_crc64: [3; 8],
        ec: EcShape { k: 4, m: 2 },
        tags: None,
        metadata_blob: None,
        system_metadata_blob: None,
        object_lock: sample_object_lock_state(),
        encryption: ObjectEncryption::None,
    };
    let parts = vec![ObjectPartRecord {
        bucket: bucket_name("bucket"),
        key: object_key("key"),
        version_id: VersionId::Versioned(NonZeroU64::new(1).unwrap()),
        part_number: 1,
        size: 32,
        etag: vec![5; 8],
        etag_kind: EtagKind::Crc64,
        part_okh: [9; 16],
        part_vid: GenerationId::MIN,
        ec_k: 4,
        ec_m: 2,
        shard_pg_id: 0,
        checksum: None,
    }];

    store
        .complete_multipart_commit(&multipart_upload_id("upload-1"), 1, &obj, &parts)
        .unwrap();

    let committed = store
        .get_object_version(&bucket_name("bucket"), &object_key("key"), obj.version_id)
        .unwrap();
    assert_eq!(committed.as_live().unwrap().object_lock, object_lock);
}

#[test]
fn schema_rejects_delete_marker_with_object_lock_state() {
    let (_dir, store) = make_pg_store();
    let err = store
        .connection()
        .execute(
            "INSERT INTO objects \
             (bucket, key, version_id, generation_id, size, etag, etag_kind, last_modified, storage_class, ec_k, ec_m, status, data_layout, parts_count, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) \
             VALUES (?1, ?2, ?3, NULL, 0, zeroblob(0), 0, ?4, 0, 0, 0, 1, 0, NULL, NULL, NULL, 0, NULL, ?5, ?6, '', 0, 0, 1900000000, 2)",
            rusqlite::params![
                "bucket",
                "key",
                1i64,
                0i64,
                "owner",
                CanonicalUserId::from_principal("owner").as_str(),
            ],
        )
        .unwrap_err();
    assert!(matches!(
        err,
        rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error {
                code: rusqlite::ffi::ErrorCode::ConstraintViolation,
                ..
            },
            _
        )
    ));
}
