use crate::traits::PgMetadataStore;
use crate::types::*;

/// Run the common metadata test suite against any PgMetadataStore implementation.
fn metadata_put_get_delete(store: &dyn PgMetadataStore) {
    let req = PutObjectMetaReq {
        bucket: "test-bucket".to_string(),
        key: "test-key".to_string(),
        version_id: 0,
        status: 0,
        size: 1024,
        total_size: 0,
        etag: vec![0xAB, 0xCD],
        etag_kind: 0,
        ec_k: 4,
        ec_m: 2,
    };

    // Put
    store.put_object_meta(&req).unwrap();

    // Get
    let obj = store.get_object_meta("test-bucket", "test-key").unwrap();
    assert_eq!(obj.bucket, "test-bucket");
    assert_eq!(obj.key, "test-key");
    assert_eq!(obj.version_id, 0);
    assert_eq!(obj.size, 1024);
    assert_eq!(obj.etag, vec![0xAB, 0xCD]);
    assert_eq!(obj.etag_kind, 0);
    assert_eq!(obj.ec_k, 4);
    assert_eq!(obj.ec_m, 2);
    assert_eq!(obj.status, 0);
    assert!(obj.last_modified > 0);

    // Delete
    store.delete_object_meta("test-bucket", "test-key").unwrap();

    let err = store
        .get_object_meta("test-bucket", "test-key")
        .unwrap_err();
    assert!(matches!(err, crate::MetadataError::ObjectNotFound));
}

fn metadata_put_overwrites(store: &dyn PgMetadataStore) {
    let req1 = PutObjectMetaReq {
        bucket: "b".to_string(),
        key: "k".to_string(),
        version_id: 0,
        status: 0,
        size: 100,
        total_size: 0,
        etag: vec![1],
        etag_kind: 0,
        ec_k: 4,
        ec_m: 2,
    };
    store.put_object_meta(&req1).unwrap();

    let req2 = PutObjectMetaReq {
        bucket: "b".to_string(),
        key: "k".to_string(),
        version_id: 0,
        status: 0,
        size: 200,
        total_size: 0,
        etag: vec![2],
        etag_kind: 0,
        ec_k: 4,
        ec_m: 2,
    };
    store.put_object_meta(&req2).unwrap();

    let obj = store.get_object_meta("b", "k").unwrap();
    assert_eq!(obj.size, 200);
    assert_eq!(obj.etag, vec![2]);
}

fn metadata_get_nonexistent(store: &dyn PgMetadataStore) {
    let err = store.get_object_meta("no-bucket", "no-key").unwrap_err();
    assert!(matches!(err, crate::MetadataError::ObjectNotFound));
}

fn metadata_list_basic(store: &dyn PgMetadataStore) {
    for i in 0..5 {
        let req = PutObjectMetaReq {
            bucket: "list-bucket".to_string(),
            key: format!("obj-{i:02}"),
            version_id: 0,
            status: 0,
            size: i * 100,
            total_size: 0,
            etag: vec![i as u8],
            etag_kind: 0,
            ec_k: 4,
            ec_m: 2,
        };
        store.put_object_meta(&req).unwrap();
    }

    let resp = store
        .list_objects(&ListObjectsReq {
            bucket: "list-bucket".to_string(),
            prefix: None,
            start_after: None,
            max_keys: 100,
        })
        .unwrap();

    assert_eq!(resp.objects.len(), 5);
    assert!(!resp.is_truncated);
    // Verify ordering.
    for (i, obj) in resp.objects.iter().enumerate() {
        assert_eq!(obj.key, format!("obj-{i:02}"));
    }
}

fn metadata_list_with_prefix(store: &dyn PgMetadataStore) {
    for key in &["photos/a.jpg", "photos/b.jpg", "docs/c.txt", "photos/d.jpg"] {
        let req = PutObjectMetaReq {
            bucket: "prefix-bucket".to_string(),
            key: key.to_string(),
            version_id: 0,
            status: 0,
            size: 100,
            total_size: 0,
            etag: vec![0],
            etag_kind: 0,
            ec_k: 4,
            ec_m: 2,
        };
        store.put_object_meta(&req).unwrap();
    }

    let resp = store
        .list_objects(&ListObjectsReq {
            bucket: "prefix-bucket".to_string(),
            prefix: Some("photos/".to_string()),
            start_after: None,
            max_keys: 100,
        })
        .unwrap();

    assert_eq!(resp.objects.len(), 3);
    assert!(resp.objects.iter().all(|o| o.key.starts_with("photos/")));
}

fn metadata_list_pagination(store: &dyn PgMetadataStore) {
    for i in 0..10 {
        let req = PutObjectMetaReq {
            bucket: "page-bucket".to_string(),
            key: format!("item-{i:02}"),
            version_id: 0,
            status: 0,
            size: 0,
            total_size: 0,
            etag: vec![],
            etag_kind: 0,
            ec_k: 4,
            ec_m: 2,
        };
        store.put_object_meta(&req).unwrap();
    }

    // First page: 3 items.
    let resp1 = store
        .list_objects(&ListObjectsReq {
            bucket: "page-bucket".to_string(),
            prefix: None,
            start_after: None,
            max_keys: 3,
        })
        .unwrap();

    assert_eq!(resp1.objects.len(), 3);
    assert!(resp1.is_truncated);
    assert_eq!(resp1.objects[0].key, "item-00");
    assert_eq!(resp1.objects[2].key, "item-02");
    assert_eq!(resp1.next_start_after, Some("item-02".to_string()));

    // Second page.
    let resp2 = store
        .list_objects(&ListObjectsReq {
            bucket: "page-bucket".to_string(),
            prefix: None,
            start_after: resp1.next_start_after,
            max_keys: 3,
        })
        .unwrap();

    assert_eq!(resp2.objects.len(), 3);
    assert!(resp2.is_truncated);
    assert_eq!(resp2.objects[0].key, "item-03");

    // Continue until not truncated.
    let mut all_keys = Vec::new();
    all_keys.extend(resp1.objects.iter().map(|o| o.key.clone()));
    all_keys.extend(resp2.objects.iter().map(|o| o.key.clone()));

    let mut start_after = resp2.next_start_after;
    loop {
        let resp = store
            .list_objects(&ListObjectsReq {
                bucket: "page-bucket".to_string(),
                prefix: None,
                start_after: start_after.clone(),
                max_keys: 3,
            })
            .unwrap();

        all_keys.extend(resp.objects.iter().map(|o| o.key.clone()));
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
            bucket: "empty-bucket".to_string(),
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
    let req = PutObjectMetaReq {
        bucket: "b".to_string(),
        key: "".to_string(),
        version_id: 0,
        status: 0,
        size: 0,
        total_size: 0,
        etag: vec![],
        etag_kind: 0,
        ec_k: 4,
        ec_m: 2,
    };
    store.put_object_meta(&req).unwrap();

    let obj = store.get_object_meta("b", "").unwrap();
    assert_eq!(obj.key, "");
}

fn metadata_long_key(store: &dyn PgMetadataStore) {
    // S3 allows keys up to 1024 bytes.
    let long_key = "x".repeat(1024);
    let req = PutObjectMetaReq {
        bucket: "b".to_string(),
        key: long_key.clone(),
        version_id: 0,
        status: 0,
        size: 0,
        total_size: 0,
        etag: vec![],
        etag_kind: 0,
        ec_k: 4,
        ec_m: 2,
    };
    store.put_object_meta(&req).unwrap();

    let obj = store.get_object_meta("b", &long_key).unwrap();
    assert_eq!(obj.key, long_key);
}

fn metadata_zero_size_object(store: &dyn PgMetadataStore) {
    let req = PutObjectMetaReq {
        bucket: "b".to_string(),
        key: "empty-obj".to_string(),
        version_id: 0,
        status: 0,
        size: 0,
        total_size: 0,
        etag: vec![],
        etag_kind: 0,
        ec_k: 4,
        ec_m: 2,
    };
    store.put_object_meta(&req).unwrap();

    let obj = store.get_object_meta("b", "empty-obj").unwrap();
    assert_eq!(obj.size, 0);
}

// --- MemoryPgStore tests ---

#[test]
fn memory_metadata_put_get_delete() {
    let store = crate::MemoryPgStore::new();
    metadata_put_get_delete(&store);
}

#[test]
fn memory_metadata_put_overwrites() {
    let store = crate::MemoryPgStore::new();
    metadata_put_overwrites(&store);
}

#[test]
fn memory_metadata_get_nonexistent() {
    let store = crate::MemoryPgStore::new();
    metadata_get_nonexistent(&store);
}

#[test]
fn memory_metadata_list_basic() {
    let store = crate::MemoryPgStore::new();
    metadata_list_basic(&store);
}

#[test]
fn memory_metadata_list_with_prefix() {
    let store = crate::MemoryPgStore::new();
    metadata_list_with_prefix(&store);
}

#[test]
fn memory_metadata_list_pagination() {
    let store = crate::MemoryPgStore::new();
    metadata_list_pagination(&store);
}

#[test]
fn memory_metadata_list_empty_bucket() {
    let store = crate::MemoryPgStore::new();
    metadata_list_empty_bucket(&store);
}

#[test]
fn memory_metadata_empty_key() {
    let store = crate::MemoryPgStore::new();
    metadata_empty_key(&store);
}

#[test]
fn memory_metadata_long_key() {
    let store = crate::MemoryPgStore::new();
    metadata_long_key(&store);
}

#[test]
fn memory_metadata_zero_size() {
    let store = crate::MemoryPgStore::new();
    metadata_zero_size_object(&store);
}

// --- PgStore (filesystem) tests ---

fn make_pg_store() -> (tempfile::TempDir, crate::PgStore) {
    let dir = tempfile::tempdir().unwrap();
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

// --- DataLayout decode tests ---

#[test]
fn file_metadata_object_has_inline_legacy_layout() {
    let (_dir, store) = make_pg_store();
    store
        .put_object_meta(&PutObjectMetaReq {
            bucket: "b".to_string(),
            key: "k".to_string(),
            version_id: 0,
            status: 0,
            size: 100,
            total_size: 100,
            etag: vec![1, 2, 3],
            etag_kind: 0,
            ec_k: 4,
            ec_m: 2,
        })
        .unwrap();

    let obj = store.get_object_meta("b", "k").unwrap();
    assert_eq!(obj.data_layout, DataLayout::InlineLegacy);
    assert_eq!(obj.parts_count, None);
    assert_eq!(obj.metadata_blob, None);
}

#[test]
fn file_metadata_invalid_data_layout_returns_error() {
    let (_dir, store) = make_pg_store();

    // Insert a valid object first
    store
        .put_object_meta(&PutObjectMetaReq {
            bucket: "b".to_string(),
            key: "k".to_string(),
            version_id: 0,
            status: 0,
            size: 100,
            total_size: 100,
            etag: vec![1, 2, 3],
            etag_kind: 0,
            ec_k: 4,
            ec_m: 2,
        })
        .unwrap();

    // Corrupt the data_layout via raw SQL
    store
        .connection()
        .execute(
            "UPDATE objects SET data_layout = 99 WHERE bucket = 'b' AND key = 'k'",
            [],
        )
        .unwrap();

    // Reading should fail, not silently default to InlineLegacy
    let err = store.get_object_meta("b", "k").unwrap_err();
    assert!(
        matches!(err, crate::error::MetadataError::Db { .. }),
        "expected Db error for invalid data_layout, got: {err:?}"
    );
}

// --- Multipart metadata tests (PgStore only — needs SQL) ---

#[test]
fn mpu_create_and_get_upload() {
    let (_dir, store) = make_pg_store();
    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: "uid-1".to_string(),
            bucket: "b".to_string(),
            key: "k".to_string(),
            metadata_blob: vec![1, 2, 3],
            owner_principal: Some("alice".to_string()),
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
fn mpu_get_missing_upload_returns_no_such_upload() {
    let (_dir, store) = make_pg_store();
    let err = store.get_multipart_upload("nonexistent").unwrap_err();
    assert!(matches!(err, crate::error::MetadataError::NoSuchUpload));
}

#[test]
fn mpu_set_upload_state_transition() {
    let (_dir, store) = make_pg_store();
    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: "uid-2".to_string(),
            bucket: "b".to_string(),
            key: "k".to_string(),
            metadata_blob: vec![],
            owner_principal: None,
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
    assert!(matches!(err, crate::error::MetadataError::NoSuchUpload));
}

#[test]
fn mpu_delete_upload_cascades_parts() {
    let (_dir, store) = make_pg_store();
    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: "uid-3".to_string(),
            bucket: "b".to_string(),
            key: "k".to_string(),
            metadata_blob: vec![],
            owner_principal: None,
        })
        .unwrap();

    // Add a part
    store
        .upsert_multipart_part(&MultipartPartRecord {
            upload_id: "uid-3".to_string(),
            part_number: 1,
            generation: 0,
            size: 1024,
            etag: vec![0xAA],
            etag_kind: 0,
            part_okh: [1u8; 16],
            part_vid: 0,
            ec_k: 4,
            ec_m: 2,
            last_modified: 0,
        })
        .unwrap();

    // Delete upload — should cascade to parts
    store.delete_multipart_upload("uid-3").unwrap();

    assert!(matches!(
        store.get_multipart_upload("uid-3").unwrap_err(),
        crate::error::MetadataError::NoSuchUpload
    ));
}

#[test]
fn mpu_delete_missing_upload_returns_no_such_upload() {
    let (_dir, store) = make_pg_store();
    let err = store.delete_multipart_upload("nonexistent").unwrap_err();
    assert!(matches!(err, crate::error::MetadataError::NoSuchUpload));
}

#[test]
fn mpu_upsert_part_and_get() {
    let (_dir, store) = make_pg_store();
    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: "uid-4".to_string(),
            bucket: "b".to_string(),
            key: "k".to_string(),
            metadata_blob: vec![],
            owner_principal: None,
        })
        .unwrap();

    // First upload: no previous generation
    let prev = store
        .upsert_multipart_part(&MultipartPartRecord {
            upload_id: "uid-4".to_string(),
            part_number: 1,
            generation: 0,
            size: 5 * 1024 * 1024,
            etag: vec![0xBB],
            etag_kind: 0,
            part_okh: [2u8; 16],
            part_vid: 0,
            ec_k: 4,
            ec_m: 2,
            last_modified: 100,
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
            upload_id: "uid-4".to_string(),
            part_number: 1,
            generation: 1,
            size: 6 * 1024 * 1024,
            etag: vec![0xCC],
            etag_kind: 0,
            part_okh: [3u8; 16],
            part_vid: 1,
            ec_k: 4,
            ec_m: 2,
            last_modified: 200,
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
            upload_id: "uid-5".to_string(),
            bucket: "b".to_string(),
            key: "k".to_string(),
            metadata_blob: vec![],
            owner_principal: None,
        })
        .unwrap();

    // Insert 5 parts
    for i in 1..=5 {
        store
            .upsert_multipart_part(&MultipartPartRecord {
                upload_id: "uid-5".to_string(),
                part_number: i,
                generation: 0,
                size: 1024 * i as u64,
                etag: vec![i as u8],
                etag_kind: 0,
                part_okh: [i as u8; 16],
                part_vid: 0,
                ec_k: 4,
                ec_m: 2,
                last_modified: 0,
            })
            .unwrap();
    }

    // List first page (max 2)
    let resp = store
        .list_multipart_parts(&ListPartsReq {
            upload_id: "uid-5".to_string(),
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
            upload_id: "uid-5".to_string(),
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
            upload_id: "uid-5".to_string(),
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
                upload_id: uid.to_string(),
                bucket: "bkt".to_string(),
                key: key.to_string(),
                metadata_blob: vec![],
                owner_principal: None,
            })
            .unwrap();
    }

    // List first page (max 2)
    let resp = store
        .list_multipart_uploads(&ListMultipartUploadsReq {
            bucket: "bkt".to_string(),
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
            bucket: "bkt".to_string(),
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
                upload_id: uid.to_string(),
                bucket: "bkt".to_string(),
                key: key.to_string(),
                metadata_blob: vec![],
                owner_principal: None,
            })
            .unwrap();
    }

    let resp = store
        .list_multipart_uploads(&ListMultipartUploadsReq {
            bucket: "bkt".to_string(),
            prefix: Some("photos/".to_string()),
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
                upload_id: uid.to_string(),
                bucket: "bkt".to_string(),
                key: "same-key".to_string(),
                metadata_blob: vec![],
                owner_principal: None,
            })
            .unwrap();
    }

    // Page 1: max_uploads=2
    let resp = store
        .list_multipart_uploads(&ListMultipartUploadsReq {
            bucket: "bkt".to_string(),
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
            bucket: "bkt".to_string(),
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
fn mpu_corrupted_part_okh_returns_error() {
    let (_dir, store) = make_pg_store();
    store
        .create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: "uid-okh".to_string(),
            bucket: "b".to_string(),
            key: "k".to_string(),
            metadata_blob: vec![],
            owner_principal: None,
        })
        .unwrap();

    // Insert a part with valid okh
    store
        .upsert_multipart_part(&MultipartPartRecord {
            upload_id: "uid-okh".to_string(),
            part_number: 1,
            generation: 0,
            size: 1024,
            etag: vec![0xAA],
            etag_kind: 0,
            part_okh: [1u8; 16],
            part_vid: 0,
            ec_k: 4,
            ec_m: 2,
            last_modified: 0,
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
            bucket: "b".to_string(),
            key: "k".to_string(),
            version_id: 1,
            part_number: 1,
            size: 1024,
            etag: vec![0xAA],
            etag_kind: 0,
            part_okh: [1u8; 16],
            part_vid: 0,
            ec_k: 4,
            ec_m: 2,
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

    let err = store.get_object_parts("b", "k", 1).unwrap_err();
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
            upload_id: "uid-pnf".to_string(),
            bucket: "b".to_string(),
            key: "k".to_string(),
            metadata_blob: vec![],
            owner_principal: None,
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
            upload_id: "uid-ip".to_string(),
            bucket: "b".to_string(),
            key: "k".to_string(),
            metadata_blob: vec![],
            owner_principal: None,
        })
        .unwrap();

    // InProgress -> InProgress should be rejected, reporting actual state
    let err = store
        .set_upload_state("uid-ip", UploadState::InProgress)
        .unwrap_err();
    assert!(
        matches!(err, crate::error::MetadataError::UploadNotInProgress { state: 0 }),
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
        matches!(err, crate::error::MetadataError::UploadNotInProgress { state: 1 }),
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
        matches!(err, crate::error::MetadataError::NoSuchUpload),
        "expected NoSuchUpload for nonexistent upload with InProgress target, got: {err:?}"
    );
}

#[test]
fn mpu_upsert_part_nonexistent_upload_returns_no_such_upload() {
    let (_dir, store) = make_pg_store();
    let err = store
        .upsert_multipart_part(&MultipartPartRecord {
            upload_id: "nonexistent".to_string(),
            part_number: 1,
            generation: 0,
            size: 1024,
            etag: vec![0xAA],
            etag_kind: 0,
            part_okh: [1u8; 16],
            part_vid: 0,
            ec_k: 4,
            ec_m: 2,
            last_modified: 0,
        })
        .unwrap_err();
    assert!(
        matches!(err, crate::error::MetadataError::NoSuchUpload),
        "expected NoSuchUpload for FK violation, got: {err:?}"
    );
}

#[test]
fn mpu_list_parts_nonexistent_upload_returns_no_such_upload() {
    let (_dir, store) = make_pg_store();
    let err = store
        .list_multipart_parts(&ListPartsReq {
            upload_id: "nonexistent".to_string(),
            part_number_marker: None,
            max_parts: 10,
        })
        .unwrap_err();
    assert!(
        matches!(err, crate::error::MetadataError::NoSuchUpload),
        "expected NoSuchUpload for nonexistent upload, got: {err:?}"
    );
}

#[test]
fn mpu_commit_object_parts_rollback_on_duplicate() {
    let (_dir, store) = make_pg_store();

    let part = ObjectPartRecord {
        bucket: "b".to_string(),
        key: "k".to_string(),
        version_id: 1,
        part_number: 1,
        size: 1024,
        etag: vec![0xAA],
        etag_kind: 0,
        part_okh: [1u8; 16],
        part_vid: 0,
        ec_k: 4,
        ec_m: 2,
    };

    // First commit succeeds
    store.commit_object_parts(std::slice::from_ref(&part)).unwrap();

    // Second commit with same PK should fail (duplicate)
    let err = store.commit_object_parts(&[part]).unwrap_err();
    assert!(
        matches!(err, crate::error::MetadataError::Db { .. }),
        "expected Db error for duplicate PK, got: {err:?}"
    );

    // The connection should still be usable (no poisoned transaction)
    let parts = store.get_object_parts("b", "k", 1).unwrap();
    assert_eq!(parts.len(), 1, "original commit should still be intact");
}

#[test]
fn mpu_commit_and_get_object_parts() {
    let (_dir, store) = make_pg_store();

    let parts = vec![
        ObjectPartRecord {
            bucket: "b".to_string(),
            key: "k".to_string(),
            version_id: 1,
            part_number: 1,
            size: 5 * 1024 * 1024,
            etag: vec![0xAA],
            etag_kind: 0,
            part_okh: [1u8; 16],
            part_vid: 0,
            ec_k: 4,
            ec_m: 2,
        },
        ObjectPartRecord {
            bucket: "b".to_string(),
            key: "k".to_string(),
            version_id: 1,
            part_number: 2,
            size: 3 * 1024 * 1024,
            etag: vec![0xBB],
            etag_kind: 0,
            part_okh: [2u8; 16],
            part_vid: 1,
            ec_k: 4,
            ec_m: 2,
        },
    ];

    store.commit_object_parts(&parts).unwrap();

    let committed = store.get_object_parts("b", "k", 1).unwrap();
    assert_eq!(committed.len(), 2);
    assert_eq!(committed[0].part_number, 1);
    assert_eq!(committed[0].size, 5 * 1024 * 1024);
    assert_eq!(committed[0].part_okh, [1u8; 16]);
    assert_eq!(committed[1].part_number, 2);
    assert_eq!(committed[1].size, 3 * 1024 * 1024);

    // Get for non-existent version returns empty
    let empty = store.get_object_parts("b", "k", 999).unwrap();
    assert!(empty.is_empty());
}

#[test]
fn mpu_delete_object_parts() {
    let (_dir, store) = make_pg_store();

    store
        .commit_object_parts(&[ObjectPartRecord {
            bucket: "b".to_string(),
            key: "k".to_string(),
            version_id: 1,
            part_number: 1,
            size: 1024,
            etag: vec![0xAA],
            etag_kind: 0,
            part_okh: [1u8; 16],
            part_vid: 0,
            ec_k: 4,
            ec_m: 2,
        }])
        .unwrap();

    store.delete_object_parts("b", "k", 1).unwrap();
    let parts = store.get_object_parts("b", "k", 1).unwrap();
    assert!(parts.is_empty());

    // Delete again is idempotent (no error)
    store.delete_object_parts("b", "k", 1).unwrap();
}

// --- Property-based tests ---

#[cfg(test)]
mod prop_tests {
    use super::*;
    use proptest::prelude::*;

    fn insert_keys(store: &dyn PgMetadataStore, bucket: &str, keys: &[String]) {
        for key in keys {
            let req = PutObjectMetaReq {
                bucket: bucket.to_string(),
                key: key.clone(),
                version_id: 0,
                status: 0,
                size: 0,
                total_size: 0,
                etag: vec![],
                etag_kind: 0,
                ec_k: 4,
                ec_m: 2,
            };
            store.put_object_meta(&req).unwrap();
        }
    }

    fn list_all_keys(
        store: &dyn PgMetadataStore,
        bucket: &str,
        prefix: Option<String>,
        max_keys: u32,
    ) -> Vec<String> {
        let mut all = Vec::new();
        let mut start_after: Option<String> = None;
        for _ in 0..1000 {
            let resp = store
                .list_objects(&ListObjectsReq {
                    bucket: bucket.to_string(),
                    prefix: prefix.clone(),
                    start_after: start_after.clone(),
                    max_keys,
                })
                .unwrap();

            for w in resp.objects.windows(2) {
                assert!(w[0].key < w[1].key);
            }

            all.extend(resp.objects.iter().map(|o| o.key.clone()));
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
            let store = crate::MemoryPgStore::new();
            insert_keys(&store, "bucket", &keys);

            let mut expected = keys.clone();
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
            let store = crate::MemoryPgStore::new();
            insert_keys(&store, "bucket", &keys);

            let mut expected: Vec<String> = keys
                .iter()
                .filter(|k| k.starts_with(&prefix))
                .cloned()
                .collect();
            expected.sort();
            expected.dedup();

            let got = list_all_keys(&store, "bucket", Some(prefix), max_keys);
            prop_assert_eq!(got, expected);
        }
    }
}
