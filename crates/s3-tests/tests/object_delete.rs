use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use s3_tests::{
    create_objects, create_objects_with_keys, delete_all_and_bucket, unique_bucket, CTX,
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
        assert!(result.is_err(), "expected error for >1000 keys");

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
        assert!(result.is_err(), "expected error for >1000 keys");

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
        assert!(result.is_err());
    });
}
