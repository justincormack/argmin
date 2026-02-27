use crate::traits::PgMetadataStore;
use crate::types::*;

/// Run the common metadata test suite against any PgMetadataStore implementation.
fn metadata_put_get_delete(store: &dyn PgMetadataStore) {
    let req = PutObjectMetaReq {
        bucket: "test-bucket".to_string(),
        key: "test-key".to_string(),
        version_id: "null".to_string(),
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
    assert_eq!(obj.version_id, "null");
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
        version_id: "null".to_string(),
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
        version_id: "null".to_string(),
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
            version_id: "null".to_string(),
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
            version_id: "null".to_string(),
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
            version_id: "null".to_string(),
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
        version_id: "null".to_string(),
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
        version_id: "null".to_string(),
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
        version_id: "null".to_string(),
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
                version_id: "null".to_string(),
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
