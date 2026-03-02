use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, Delete, ObjectIdentifier, VersioningConfiguration,
};
use s3_tests::{
    create_objects, create_objects_with_keys, delete_all_and_bucket, err_status, unique_bucket, CTX,
};

// ── Local helpers ───────────────────────────────────────────────────

fn make_object_id(key: &str) -> ObjectIdentifier {
    ObjectIdentifier::builder()
        .key(key)
        .build()
        .expect("build ObjectIdentifier")
}

fn make_delete_request(keys: &[&str], quiet: bool) -> Delete {
    let objects: Vec<ObjectIdentifier> = keys.iter().map(|k| make_object_id(k)).collect();
    Delete::builder()
        .set_objects(Some(objects))
        .quiet(quiet)
        .build()
        .expect("build Delete")
}

fn get_keys(objects: &[aws_sdk_s3::types::Object]) -> Vec<String> {
    objects
        .iter()
        .filter_map(|o| o.key().map(str::to_string))
        .collect()
}

// ── Multi-object delete ─────────────────────────────────────────────

#[test]
fn test_multi_object_delete() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects(client, "", 3).await;

        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let delete = make_delete_request(&key_refs, false);
        let resp = client
            .delete_objects()
            .bucket(&bucket)
            .delete(delete)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.deleted().len(), 3);
        assert!(resp.errors().is_empty());

        // Verify objects are gone via V1 list
        let list = client.list_objects().bucket(&bucket).send().await.unwrap();
        assert!(list.contents().is_empty());

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_multi_objectv2_delete() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects(client, "", 3).await;

        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let delete = make_delete_request(&key_refs, false);
        client
            .delete_objects()
            .bucket(&bucket)
            .delete(delete)
            .send()
            .await
            .unwrap();

        // Verify objects are gone via V2 list
        let list = client
            .list_objects_v2()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(list.key_count(), Some(0));
        assert!(list.contents().is_empty());

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_multi_object_delete_quiet() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects(client, "", 3).await;

        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let delete = make_delete_request(&key_refs, true);
        let resp = client
            .delete_objects()
            .bucket(&bucket)
            .delete(delete)
            .send()
            .await
            .unwrap();

        // Quiet mode: successfully deleted items not listed in response
        assert!(resp.deleted().is_empty());
        assert!(resp.errors().is_empty());

        // But objects should actually be deleted
        let list = client
            .list_objects_v2()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert!(list.contents().is_empty());

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_multi_object_delete_large() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects(client, "", 35).await;

        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let delete = make_delete_request(&key_refs, false);
        let resp = client
            .delete_objects()
            .bucket(&bucket)
            .delete(delete)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.deleted().len(), 35);
        assert!(resp.errors().is_empty());

        let list = client
            .list_objects_v2()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert!(list.contents().is_empty());

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_multi_object_delete_nonexistent_keys() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        // Delete keys that were never created — should succeed (idempotent)
        let delete = make_delete_request(&["nokey1", "nokey2", "nokey3"], false);
        let resp = client
            .delete_objects()
            .bucket(&bucket)
            .delete(delete)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.deleted().len(), 3);
        assert!(resp.errors().is_empty());

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_multi_object_delete_mixed() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, &["existing1", "existing2"]).await;

        // Delete mix of existing and nonexistent keys
        let delete = make_delete_request(&["existing1", "nonexistent", "existing2"], false);
        let resp = client
            .delete_objects()
            .bucket(&bucket)
            .delete(delete)
            .send()
            .await
            .unwrap();

        // All should be reported as deleted (including nonexistent)
        assert_eq!(resp.deleted().len(), 3);
        assert!(resp.errors().is_empty());

        // Verify existing objects are gone
        let list = client
            .list_objects_v2()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert!(list.contents().is_empty());

        // keys vec only has the originally created ones; bucket is already empty
        let _ = keys;
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_multi_object_delete_special_keys() {
    s3_tests::run(async {
        let client = CTX.client();
        let special = &["a/b/c", "hello world", "foo&bar", "key with spaces"];
        let (bucket, keys) = create_objects_with_keys(client, special).await;

        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let delete = make_delete_request(&key_refs, false);
        let resp = client
            .delete_objects()
            .bucket(&bucket)
            .delete(delete)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.deleted().len(), 4);
        assert!(resp.errors().is_empty());

        let list = client
            .list_objects_v2()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert!(list.contents().is_empty());

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_multi_object_delete_single() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, _keys) = create_objects_with_keys(client, &["only", "survivor"]).await;

        // Delete just one key via multi-delete API
        let delete = make_delete_request(&["only"], false);
        let resp = client
            .delete_objects()
            .bucket(&bucket)
            .delete(delete)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.deleted().len(), 1);
        assert!(resp.errors().is_empty());

        // "survivor" should still exist
        let list = client
            .list_objects_v2()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(get_keys(list.contents()), vec!["survivor"]);

        delete_all_and_bucket(client, &bucket, &["survivor".to_string()]).await;
    });
}

#[test]
fn test_multi_object_delete_verify_response() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, _keys) = create_objects_with_keys(client, &["alpha", "beta", "gamma"]).await;

        let delete = make_delete_request(&["alpha", "beta", "gamma"], false);
        let resp = client
            .delete_objects()
            .bucket(&bucket)
            .delete(delete)
            .send()
            .await
            .unwrap();

        // Verify response lists the correct deleted keys
        let mut deleted_keys: Vec<String> = resp
            .deleted()
            .iter()
            .filter_map(|d| d.key().map(str::to_string))
            .collect();
        deleted_keys.sort();
        assert_eq!(deleted_keys, vec!["alpha", "beta", "gamma"]);
        assert!(resp.errors().is_empty());

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_multi_object_delete_key_limit() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        // Build a request with >1000 keys (server limit)
        let key_strs: Vec<String> = (0..1001).map(|i| format!("key{}", i)).collect();
        let key_refs: Vec<&str> = key_strs.iter().map(|s| s.as_str()).collect();
        let delete = make_delete_request(&key_refs, false);

        let result = client
            .delete_objects()
            .bucket(&bucket)
            .delete(delete)
            .send()
            .await;
        assert_eq!(err_status(&result), 400);

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

// ── Single delete edge cases ────────────────────────────────────────

#[test]
fn test_object_delete_nonexistent_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();

        // Single delete from nonexistent bucket should error
        let result = client
            .delete_object()
            .bucket(&bucket)
            .key("somekey")
            .send()
            .await;
        assert!(result.is_err());
    });
}

#[test]
fn test_multi_object_delete_nonexistent_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();

        let delete = make_delete_request(&["key1", "key2"], false);
        let result = client
            .delete_objects()
            .bucket(&bucket)
            .delete(delete)
            .send()
            .await;
        assert!(result.is_err());
    });
}

#[test]
fn test_multi_objectv2_delete_key_limit() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        // Build a request with >1000 keys (server limit), verify via V2 list
        let key_strs: Vec<String> = (0..1001).map(|i| format!("key{}", i)).collect();
        let key_refs: Vec<&str> = key_strs.iter().map(|s| s.as_str()).collect();
        let delete = make_delete_request(&key_refs, false);

        let result = client
            .delete_objects()
            .bucket(&bucket)
            .delete(delete)
            .send()
            .await;
        assert_eq!(err_status(&result), 400);

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_object_delete_key_bucket_gone() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();

        // Try to delete an object from the now-deleted bucket
        let result = client
            .delete_object()
            .bucket(&bucket)
            .key("somekey")
            .send()
            .await;
        assert_eq!(err_status(&result), 404);
    });
}

// ── Versioning helpers ──────────────────────────────────────────────

async fn setup_versioned_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    client.create_bucket().bucket(&bucket).send().await.unwrap();
    client
        .put_bucket_versioning()
        .bucket(&bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    bucket
}

async fn create_multiple_versions(
    bucket: &str,
    key: &str,
    num: usize,
) -> (Vec<String>, Vec<String>) {
    let client = CTX.client();
    let mut version_ids = Vec::new();
    let mut contents = Vec::new();
    for i in 0..num {
        let body = format!("content-{}", i);
        let resp = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(body.clone().into_bytes()))
            .send()
            .await
            .unwrap();
        version_ids.push(resp.version_id().unwrap().to_string());
        contents.push(body);
    }
    (version_ids, contents)
}

/// Clean up a versioned bucket by deleting all versions and delete markers.
async fn cleanup_versioned_bucket(bucket: &str) {
    let client = CTX.client();
    // List all versions and delete markers, delete them all
    let resp = client
        .list_object_versions()
        .bucket(bucket)
        .send()
        .await
        .unwrap();
    for v in resp.versions() {
        client
            .delete_object()
            .bucket(bucket)
            .key(v.key().unwrap())
            .version_id(v.version_id().unwrap())
            .send()
            .await
            .unwrap();
    }
    for dm in resp.delete_markers() {
        client
            .delete_object()
            .bucket(bucket)
            .key(dm.key().unwrap())
            .version_id(dm.version_id().unwrap())
            .send()
            .await
            .unwrap();
    }
    client.delete_bucket().bucket(bucket).send().await.unwrap();
}

// ── Versioning + multi-object delete ────────────────────────────────

/// Batch-delete specific version IDs and verify they are removed.
#[test]
fn test_versioning_multi_object_delete() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "testobj";
        let num_versions = 5;

        let (version_ids, _contents) = create_multiple_versions(&bucket, key, num_versions).await;

        // Batch delete all versions by specifying their version IDs
        let objects: Vec<ObjectIdentifier> = version_ids
            .iter()
            .map(|vid| {
                ObjectIdentifier::builder()
                    .key(key)
                    .version_id(vid)
                    .build()
                    .unwrap()
            })
            .collect();
        let delete = Delete::builder()
            .set_objects(Some(objects))
            .quiet(false)
            .build()
            .unwrap();
        let resp = client
            .delete_objects()
            .bucket(&bucket)
            .delete(delete)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.deleted().len(), num_versions);
        assert!(resp.errors().is_empty());

        // Verify: no versions remain
        let list = client
            .list_object_versions()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert!(
            list.versions().is_empty(),
            "expected no versions after batch delete"
        );
        assert!(
            list.delete_markers().is_empty(),
            "expected no delete markers after batch delete"
        );

        // Idempotent: deleting the same version IDs again should succeed
        let objects2: Vec<ObjectIdentifier> = version_ids
            .iter()
            .map(|vid| {
                ObjectIdentifier::builder()
                    .key(key)
                    .version_id(vid)
                    .build()
                    .unwrap()
            })
            .collect();
        let delete2 = Delete::builder()
            .set_objects(Some(objects2))
            .quiet(false)
            .build()
            .unwrap();
        let resp2 = client
            .delete_objects()
            .bucket(&bucket)
            .delete(delete2)
            .send()
            .await
            .unwrap();
        assert_eq!(resp2.deleted().len(), num_versions);
        assert!(resp2.errors().is_empty());

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

/// Batch-delete versions plus a delete marker.
#[test]
fn test_versioning_multi_object_delete_with_marker() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "testobj";

        // Create 3 versions
        let (version_ids, _contents) = create_multiple_versions(&bucket, key, 3).await;

        // Create a delete marker by deleting without specifying versionId
        let del_resp = client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert!(del_resp.delete_marker().unwrap_or(false));
        let marker_vid = del_resp.version_id().unwrap().to_string();

        // Now batch-delete all versions + the delete marker
        let mut all_vids = version_ids.clone();
        all_vids.push(marker_vid);

        let objects: Vec<ObjectIdentifier> = all_vids
            .iter()
            .map(|vid| {
                ObjectIdentifier::builder()
                    .key(key)
                    .version_id(vid)
                    .build()
                    .unwrap()
            })
            .collect();
        let delete = Delete::builder()
            .set_objects(Some(objects))
            .quiet(false)
            .build()
            .unwrap();
        let resp = client
            .delete_objects()
            .bucket(&bucket)
            .delete(delete)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.deleted().len(), 4);
        assert!(resp.errors().is_empty());

        // The entry for the delete marker should have delete_marker=true
        let marker_entry = resp
            .deleted()
            .iter()
            .find(|d| d.delete_marker().unwrap_or(false));
        assert!(
            marker_entry.is_some(),
            "expected a delete marker entry in response"
        );

        // Verify: bucket should be completely clean
        let list = client
            .list_object_versions()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert!(list.versions().is_empty());
        assert!(list.delete_markers().is_empty());

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

/// Use delete_objects (without versionId) on a versioned bucket to create a delete marker.
#[test]
fn test_versioning_multi_object_delete_marker_create() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "testobj";

        // Put one version
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(b"data".to_vec()))
            .send()
            .await
            .unwrap();

        // Batch-delete WITHOUT specifying versionId → should create a delete marker
        let objects = vec![ObjectIdentifier::builder().key(key).build().unwrap()];
        let delete = Delete::builder()
            .set_objects(Some(objects))
            .quiet(false)
            .build()
            .unwrap();
        let resp = client
            .delete_objects()
            .bucket(&bucket)
            .delete(delete)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.deleted().len(), 1);
        assert!(resp.errors().is_empty());
        let d = &resp.deleted()[0];
        assert!(
            d.delete_marker().unwrap_or(false),
            "expected delete_marker=true when deleting without versionId in versioned bucket"
        );
        assert!(
            d.delete_marker_version_id().is_some(),
            "expected delete_marker_version_id in response"
        );

        // The object should now be inaccessible (404) via normal GET
        let get_result = client.get_object().bucket(&bucket).key(key).send().await;
        assert!(get_result.is_err());

        // But list_object_versions should show both the version and the delete marker
        let list = client
            .list_object_versions()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(
            list.versions().len(),
            1,
            "original version should still exist"
        );
        assert_eq!(list.delete_markers().len(), 1, "delete marker should exist");

        cleanup_versioned_bucket(&bucket).await;
    });
}

/// Batch-delete on a non-existent key in a versioned bucket creates a delete marker.
#[test]
fn test_versioning_multi_object_delete_nonexistent_creates_marker() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;

        // Batch-delete a key that was never created
        let objects = vec![ObjectIdentifier::builder()
            .key("never-existed")
            .build()
            .unwrap()];
        let delete = Delete::builder()
            .set_objects(Some(objects))
            .quiet(false)
            .build()
            .unwrap();
        let resp = client
            .delete_objects()
            .bucket(&bucket)
            .delete(delete)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.deleted().len(), 1);
        assert!(resp.errors().is_empty());
        let d = &resp.deleted()[0];
        assert!(
            d.delete_marker().unwrap_or(false),
            "expected delete_marker=true for nonexistent key in versioned bucket"
        );
        assert!(
            d.delete_marker_version_id().is_some(),
            "expected delete_marker_version_id for nonexistent key"
        );

        // Verify delete marker was actually created
        let list = client
            .list_object_versions()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert!(list.versions().is_empty());
        assert_eq!(list.delete_markers().len(), 1);
        assert_eq!(list.delete_markers()[0].key().unwrap(), "never-existed");

        cleanup_versioned_bucket(&bucket).await;
    });
}
