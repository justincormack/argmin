use aws_sdk_s3::types::{
    BucketVersioningStatus, Delete, ObjectIdentifier, VersioningConfiguration,
};
use s3_tests::{
    assert_s3_err_code, create_objects, create_objects_with_keys, delete_all_and_bucket,
    delete_objects_with_md5, err_status, unique_bucket, SendRetryingOperationAborted, CTX,
};
use serde_json::json;

// ── Local helpers ───────────────────────────────────────────────────

fn make_object_id(key: &str) -> ObjectIdentifier {
    ObjectIdentifier::builder()
        .key(key)
        .build()
        .expect("build ObjectIdentifier")
}

fn make_object_id_with_etag(key: &str, etag: &str) -> ObjectIdentifier {
    ObjectIdentifier::builder()
        .key(key)
        .e_tag(etag)
        .build()
        .expect("build ObjectIdentifier")
}

fn make_object_id_with_version_and_etag(
    key: &str,
    version_id: &str,
    etag: &str,
) -> ObjectIdentifier {
    ObjectIdentifier::builder()
        .key(key)
        .version_id(version_id)
        .e_tag(etag)
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

fn object_resource(bucket: &str, key: &str) -> String {
    format!("arn:aws:s3:::{bucket}/{key}")
}

fn alt_policy_principal() -> serde_json::Value {
    json!({ "AWS": format!("arn:aws:iam::{}:root", CTX.alt_account_id()) })
}

fn object_tagging(key: &str, value: &str) -> aws_sdk_s3::types::Tagging {
    aws_sdk_s3::types::Tagging::builder()
        .tag_set(
            aws_sdk_s3::types::Tag::builder()
                .key(key)
                .value(value)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap()
}

async fn put_object(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    body: impl Into<Vec<u8>>,
) -> aws_sdk_s3::operation::put_object::PutObjectOutput {
    s3_tests::put_object_retrying_operation_aborted(client, bucket, key, body.into()).await
}

// ── Multi-object delete ─────────────────────────────────────────────

#[test]
fn test_multi_object_delete() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects(client, "", 3).await;

        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let delete = make_delete_request(&key_refs, false);
        let resp =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, delete).await;

        assert_eq!(resp.deleted().len(), 3);
        assert!(resp.errors().is_empty());

        // Verify objects are gone via V1 list
        let list = client
            .list_objects()
            .bucket(&bucket)
            .send_retrying_operation_aborted("list objects after multi-object delete")
            .await
            .unwrap();
        assert!(list.contents().is_empty());

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_bucket_policy_delete_object_existing_tag_condition_is_rejected() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let public_key = "public-delete";
        let private_key = "private-delete";
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        for key in [public_key, private_key] {
            put_object(client, &bucket, key, b"data").await;
        }

        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(public_key)
            .tagging(object_tagging("security", "public"))
            .send_retrying_operation_aborted("put object tagging during delete tests")
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(private_key)
            .tagging(object_tagging("security", "private"))
            .send_retrying_operation_aborted("put object tagging during delete tests")
            .await
            .unwrap();

        let result = client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:DeleteObject",
                        "Resource": [object_resource(&bucket, public_key), object_resource(&bucket, private_key)],
                        "Condition": {
                            "StringEquals": {
                                "s3:ExistingObjectTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send_retrying_operation_aborted("put bucket policy during delete tests")
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "MalformedPolicy");

        client
            .delete_object()
            .bucket(&bucket)
            .key(private_key)
            .send_retrying_operation_aborted("delete object during delete tests")
            .await
            .unwrap();
        client
            .delete_object()
            .bucket(&bucket)
            .key(public_key)
            .send_retrying_operation_aborted("delete object during delete tests")
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_bucket_policy_delete_object_version_existing_tag_condition_is_rejected() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let public_key = "public-version-delete";
        let private_key = "private-version-delete";
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_bucket_versioning()
            .bucket(&bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send_retrying_operation_aborted("put bucket versioning during delete tests")
            .await
            .unwrap();

        let public_version = put_object(client, &bucket, public_key, b"data")
            .await
            .version_id()
            .expect("expected version id")
            .to_string();
        let private_version = put_object(client, &bucket, private_key, b"data")
            .await
            .version_id()
            .expect("expected version id")
            .to_string();

        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(public_key)
            .version_id(&public_version)
            .tagging(object_tagging("security", "public"))
            .send_retrying_operation_aborted("put object tagging during delete tests")
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(private_key)
            .version_id(&private_version)
            .tagging(object_tagging("security", "private"))
            .send_retrying_operation_aborted("put object tagging during delete tests")
            .await
            .unwrap();

        let result = client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:DeleteObjectVersion",
                        "Resource": [object_resource(&bucket, public_key), object_resource(&bucket, private_key)],
                        "Condition": {
                            "StringEquals": {
                                "s3:ExistingObjectTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send_retrying_operation_aborted("put bucket policy during delete tests")
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "MalformedPolicy");

        cleanup_versioned_bucket(&bucket).await;
    });
}

#[test]
fn test_bucket_policy_delete_and_delete_tagging_existing_tag_condition_is_rejected() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let key = "mixed-delete-action";
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        put_object(client, &bucket, key, b"data").await;
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .tagging(object_tagging("security", "public"))
            .send_retrying_operation_aborted("put object tagging during delete tests")
            .await
            .unwrap();

        let result = client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": ["s3:DeleteObject", "s3:DeleteObjectTagging"],
                        "Resource": object_resource(&bucket, key),
                        "Condition": {
                            "StringEquals": {
                                "s3:ExistingObjectTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send_retrying_operation_aborted("put bucket policy during delete tests")
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "MalformedPolicy");

        delete_all_and_bucket(client, &bucket, &[key.to_string()]).await;
    });
}

#[test]
fn test_multi_objectv2_delete() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects(client, "", 3).await;

        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let delete = make_delete_request(&key_refs, false);
        s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, delete).await;

        // Verify objects are gone via V2 list
        let list = client
            .list_objects_v2()
            .bucket(&bucket)
            .send_retrying_operation_aborted("list objects during delete tests")
            .await
            .unwrap();
        assert_eq!(list.key_count(), Some(0));
        assert!(list.contents().is_empty());

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_multi_object_delete_quiet() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects(client, "", 3).await;

        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let delete = make_delete_request(&key_refs, true);
        let resp =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, delete).await;

        // Quiet mode: successfully deleted items not listed in response
        assert!(resp.deleted().is_empty());
        assert!(resp.errors().is_empty());

        // But objects should actually be deleted
        let list = client
            .list_objects_v2()
            .bucket(&bucket)
            .send_retrying_operation_aborted("list objects during delete tests")
            .await
            .unwrap();
        assert!(list.contents().is_empty());

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_multi_object_delete_large() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects(client, "", 35).await;

        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let delete = make_delete_request(&key_refs, false);
        let resp =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, delete).await;

        assert_eq!(resp.deleted().len(), 35);
        assert!(resp.errors().is_empty());

        let list = client
            .list_objects_v2()
            .bucket(&bucket)
            .send_retrying_operation_aborted("list objects during delete tests")
            .await
            .unwrap();
        assert!(list.contents().is_empty());

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_multi_object_delete_nonexistent_keys() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        // Delete keys that were never created — should succeed (idempotent)
        let delete = make_delete_request(&["nokey1", "nokey2", "nokey3"], false);
        let resp =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, delete).await;

        assert_eq!(resp.deleted().len(), 3);
        assert!(resp.errors().is_empty());

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_multi_object_delete_mixed() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, &["existing1", "existing2"]).await;

        // Delete mix of existing and nonexistent keys
        let delete = make_delete_request(&["existing1", "nonexistent", "existing2"], false);
        let resp =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, delete).await;

        // All should be reported as deleted (including nonexistent)
        assert_eq!(resp.deleted().len(), 3);
        assert!(resp.errors().is_empty());

        // Verify existing objects are gone
        let list = client
            .list_objects_v2()
            .bucket(&bucket)
            .send_retrying_operation_aborted("list objects during delete tests")
            .await
            .unwrap();
        assert!(list.contents().is_empty());

        // keys vec only has the originally created ones; bucket is already empty
        let _ = keys;
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
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
        let resp =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, delete).await;

        assert_eq!(resp.deleted().len(), 4);
        assert!(resp.errors().is_empty());

        let list = client
            .list_objects_v2()
            .bucket(&bucket)
            .send_retrying_operation_aborted("list objects during delete tests")
            .await
            .unwrap();
        assert!(list.contents().is_empty());

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_multi_object_delete_single() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, _keys) = create_objects_with_keys(client, &["only", "survivor"]).await;

        // Delete just one key via multi-delete API
        let delete = make_delete_request(&["only"], false);
        let resp =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, delete).await;

        assert_eq!(resp.deleted().len(), 1);
        assert!(resp.errors().is_empty());

        // "survivor" should still exist
        let list = client
            .list_objects_v2()
            .bucket(&bucket)
            .send_retrying_operation_aborted("list objects during delete tests")
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
        let resp =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, delete).await;

        // Verify response lists the correct deleted keys
        let mut deleted_keys: Vec<String> = resp
            .deleted()
            .iter()
            .filter_map(|d| d.key().map(str::to_string))
            .collect();
        deleted_keys.sort();
        assert_eq!(deleted_keys, vec!["alpha", "beta", "gamma"]);
        assert!(resp.errors().is_empty());

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_multi_object_delete_per_object_if_match() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let ok = put_object(client, &bucket, "ok", b"ok").await;
        put_object(client, &bucket, "stale", b"stale").await;

        let delete = Delete::builder()
            .set_objects(Some(vec![
                make_object_id_with_etag("ok", ok.e_tag().unwrap()),
                make_object_id_with_etag("stale", "\"0000000000000000\""),
            ]))
            .quiet(false)
            .build()
            .unwrap();
        let resp =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, delete).await;

        assert_eq!(resp.deleted().len(), 1);
        assert_eq!(resp.deleted()[0].key(), Some("ok"));
        assert_eq!(resp.errors().len(), 1);
        assert_eq!(resp.errors()[0].key(), Some("stale"));
        assert_eq!(resp.errors()[0].code(), Some("PreconditionFailed"));

        client
            .head_object()
            .bucket(&bucket)
            .key("stale")
            .send_retrying_operation_aborted("head object after conditional multi-delete")
            .await
            .unwrap();

        delete_all_and_bucket(client, &bucket, &["stale".to_string()]).await;
    });
}

#[test]
fn test_multi_object_delete_key_limit() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        // Build a request with >1000 keys (server limit)
        let key_strs: Vec<String> = (0..1001).map(|i| format!("key{}", i)).collect();
        let key_refs: Vec<&str> = key_strs.iter().map(|s| s.as_str()).collect();
        let delete = make_delete_request(&key_refs, false);

        let result = delete_objects_with_md5(client, &bucket, delete)
            .send()
            .await;
        assert_eq!(err_status(&result), 400);

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
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
            .send_retrying_operation_aborted("delete object during delete tests")
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
        let result = delete_objects_with_md5(client, &bucket, delete)
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
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        // Build a request with >1000 keys (server limit), verify via V2 list
        let key_strs: Vec<String> = (0..1001).map(|i| format!("key{}", i)).collect();
        let key_refs: Vec<&str> = key_strs.iter().map(|s| s.as_str()).collect();
        let delete = make_delete_request(&key_refs, false);

        let result = delete_objects_with_md5(client, &bucket, delete)
            .send()
            .await;
        assert_eq!(err_status(&result), 400);

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_object_delete_key_bucket_gone() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;

        // Try to delete an object from the now-deleted bucket
        let result = client
            .delete_object()
            .bucket(&bucket)
            .key("somekey")
            .send_retrying_operation_aborted("delete object during delete tests")
            .await;
        assert_eq!(err_status(&result), 404);
    });
}

// ── Versioning helpers ──────────────────────────────────────────────

async fn setup_versioned_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    client
        .put_bucket_versioning()
        .bucket(&bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send_retrying_operation_aborted("put bucket versioning during delete tests")
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
        let resp = put_object(client, bucket, key, body.clone().into_bytes()).await;
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
        .send_retrying_operation_aborted("list object versions during delete tests")
        .await
        .unwrap();
    for v in resp.versions() {
        client
            .delete_object()
            .bucket(bucket)
            .key(v.key().unwrap())
            .version_id(v.version_id().unwrap())
            .send_retrying_operation_aborted("delete object during delete tests")
            .await
            .unwrap();
    }
    for dm in resp.delete_markers() {
        client
            .delete_object()
            .bucket(bucket)
            .key(dm.key().unwrap())
            .version_id(dm.version_id().unwrap())
            .send_retrying_operation_aborted("delete object during delete tests")
            .await
            .unwrap();
    }
    s3_tests::delete_bucket_retrying_operation_aborted(client, bucket).await;
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
        let resp =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, delete).await;

        assert_eq!(resp.deleted().len(), num_versions);
        assert!(resp.errors().is_empty());

        // Verify: no versions remain
        let list = client
            .list_object_versions()
            .bucket(&bucket)
            .send_retrying_operation_aborted("list object versions during delete tests")
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
        let resp2 =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, delete2).await;
        assert_eq!(resp2.deleted().len(), num_versions);
        assert!(resp2.errors().is_empty());

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
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
            .send_retrying_operation_aborted("delete object during delete tests")
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
        let resp =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, delete).await;

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
            .send_retrying_operation_aborted("list object versions during delete tests")
            .await
            .unwrap();
        assert!(list.versions().is_empty());
        assert!(list.delete_markers().is_empty());

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
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
        put_object(client, &bucket, key, b"data").await;

        // Batch-delete WITHOUT specifying versionId → should create a delete marker
        let objects = vec![ObjectIdentifier::builder().key(key).build().unwrap()];
        let delete = Delete::builder()
            .set_objects(Some(objects))
            .quiet(false)
            .build()
            .unwrap();
        let resp =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, delete).await;

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
        let get_result = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("get object after versioned multi-object delete")
            .await;
        assert!(get_result.is_err());

        // But list_object_versions should show both the version and the delete marker
        let list = client
            .list_object_versions()
            .bucket(&bucket)
            .send_retrying_operation_aborted("list object versions during delete tests")
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

#[test]
fn test_versioning_multi_object_delete_current_if_match() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let put = put_object(client, &bucket, "obj", b"data").await;

        let bad_delete = Delete::builder()
            .set_objects(Some(vec![make_object_id_with_etag(
                "obj",
                "\"0000000000000000\"",
            )]))
            .quiet(false)
            .build()
            .unwrap();
        let bad_resp =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, bad_delete).await;
        assert!(bad_resp.deleted().is_empty());
        assert_eq!(bad_resp.errors().len(), 1);
        assert_eq!(bad_resp.errors()[0].key(), Some("obj"));
        assert_eq!(bad_resp.errors()[0].code(), Some("PreconditionFailed"));

        let delete = Delete::builder()
            .set_objects(Some(vec![make_object_id_with_etag(
                "obj",
                put.e_tag().unwrap(),
            )]))
            .quiet(false)
            .build()
            .unwrap();
        let resp =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, delete).await;

        assert_eq!(resp.deleted().len(), 1);
        let deleted = &resp.deleted()[0];
        assert_eq!(deleted.key(), Some("obj"));
        assert_eq!(deleted.delete_marker(), Some(true));
        assert!(deleted.delete_marker_version_id().is_some());

        let get_result = client
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("get object after versioned multi-object delete")
            .await;
        assert!(get_result.is_err());

        cleanup_versioned_bucket(&bucket).await;
    });
}

#[test]
fn test_versioning_multi_object_delete_version_id_with_etag_not_implemented() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "obj";

        let first = put_object(client, &bucket, key, b"v1").await;
        let first_version = first.version_id().unwrap().to_string();
        let first_etag = first.e_tag().unwrap().to_string();

        let second = put_object(client, &bucket, key, b"v2").await;
        let second_version = second.version_id().unwrap().to_string();

        // AWS does not support DeleteObjects entries that combine VersionId
        // and ETag on general-purpose buckets. Both matching and mismatching
        // ETags return per-object NotImplemented and leave the version intact.
        let bad_delete = Delete::builder()
            .set_objects(Some(vec![make_object_id_with_version_and_etag(
                key,
                &first_version,
                "\"0000000000000000\"",
            )]))
            .quiet(false)
            .build()
            .unwrap();
        let bad_resp =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, bad_delete).await;

        assert!(bad_resp.deleted().is_empty());
        assert_eq!(bad_resp.errors().len(), 1);
        assert_eq!(bad_resp.errors()[0].key(), Some(key));
        assert_eq!(
            bad_resp.errors()[0].version_id(),
            Some(first_version.as_str())
        );
        assert_eq!(bad_resp.errors()[0].code(), Some("NotImplemented"));

        let good_delete = Delete::builder()
            .set_objects(Some(vec![make_object_id_with_version_and_etag(
                key,
                &first_version,
                &first_etag,
            )]))
            .quiet(false)
            .build()
            .unwrap();
        let good_resp =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, good_delete).await;

        assert!(good_resp.deleted().is_empty());
        assert_eq!(good_resp.errors().len(), 1);
        assert_eq!(good_resp.errors()[0].key(), Some(key));
        assert_eq!(
            good_resp.errors()[0].version_id(),
            Some(first_version.as_str())
        );
        assert_eq!(good_resp.errors()[0].code(), Some("NotImplemented"));

        let first_get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&first_version)
            .send_retrying_operation_aborted("get first version after conditional multi-delete")
            .await
            .unwrap();
        let first_data = first_get.body.collect().await.unwrap().into_bytes();
        assert_eq!(&first_data[..], b"v1");

        let second_get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&second_version)
            .send_retrying_operation_aborted("get second version after conditional multi-delete")
            .await
            .unwrap();
        let data = second_get.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"v2");

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
        let resp =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, delete).await;

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
            .send_retrying_operation_aborted("list object versions during delete tests")
            .await
            .unwrap();
        assert!(list.versions().is_empty());
        assert_eq!(list.delete_markers().len(), 1);
        assert_eq!(list.delete_markers()[0].key().unwrap(), "never-existed");

        cleanup_versioned_bucket(&bucket).await;
    });
}
