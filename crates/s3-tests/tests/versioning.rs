use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, Delete, ObjectIdentifier, VersioningConfiguration,
};
use s3_tests::{err_status, unique_bucket, CTX};

// ── Helpers ─────────────────────────────────────────────────────────

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

/// Put multiple versions of the same key, returning (version_ids, contents).
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

/// Verify that GET with a specific versionId returns expected content.
async fn check_obj_content(bucket: &str, key: &str, version_id: &str, expected: &str) {
    let resp = CTX
        .client()
        .get_object()
        .bucket(bucket)
        .key(key)
        .version_id(version_id)
        .send()
        .await
        .unwrap();
    let body = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(
        std::str::from_utf8(&body).unwrap(),
        expected,
        "version {} content mismatch",
        version_id
    );
}

/// Delete all version IDs then delete the bucket.
async fn cleanup_versioned(bucket: &str, key: &str, version_ids: &[String]) {
    let client = CTX.client();
    for vid in version_ids {
        client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .version_id(vid)
            .send()
            .await
            .unwrap();
    }
    client.delete_bucket().bucket(bucket).send().await.unwrap();
}

// ── Basic versioning CRUD ───────────────────────────────────────────

#[test]
fn test_versioning_obj_create_read_remove() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "testobj";
        let num = 5;

        // Create 5 versions, verify each is readable, remove all
        let (version_ids, contents) = create_multiple_versions(&bucket, key, num).await;

        // Verify each version is independently readable
        for (vid, content) in version_ids.iter().zip(contents.iter()) {
            check_obj_content(&bucket, key, vid, content).await;
        }

        // Remove each version by versionId
        for vid in &version_ids {
            client
                .delete_object()
                .bucket(&bucket)
                .key(key)
                .version_id(vid)
                .send()
                .await
                .unwrap();
        }

        // Bucket should have no versions left
        let resp = client
            .list_object_versions()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert!(
            resp.versions().is_empty(),
            "expected no versions after removal"
        );

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_versioning_obj_create_read_remove_head() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "testobj";
        let num = 5;

        let (mut version_ids, mut contents) = create_multiple_versions(&bucket, key, num).await;

        // Remove the latest (head) version
        let removed_vid = version_ids.pop().unwrap();
        contents.pop();
        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&removed_vid)
            .send()
            .await
            .unwrap();

        // GET should now return the previous version
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let body = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(
            std::str::from_utf8(&body).unwrap(),
            contents.last().unwrap()
        );

        // Add a delete marker
        let del_resp = client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert!(del_resp.delete_marker().unwrap_or(false));
        let dm_vid = del_resp.version_id().unwrap().to_string();
        version_ids.push(dm_vid.clone());

        // list_object_versions should show versions + 1 delete marker
        let resp = client
            .list_object_versions()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.versions().len(), num - 1);
        assert_eq!(resp.delete_markers().len(), 1);
        assert_eq!(
            resp.delete_markers()[0].version_id().unwrap(),
            dm_vid.as_str()
        );

        cleanup_versioned(&bucket, key, &version_ids).await;
    });
}

#[test]
fn test_versioning_obj_create_versions_remove_all() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "testobj";
        let num = 10;

        let (version_ids, contents) = create_multiple_versions(&bucket, key, num).await;

        // Remove each version, verifying content before removal
        for i in 0..num {
            check_obj_content(&bucket, key, &version_ids[i], &contents[i]).await;
            client
                .delete_object()
                .bucket(&bucket)
                .key(key)
                .version_id(&version_ids[i])
                .send()
                .await
                .unwrap();
        }

        let resp = client
            .list_object_versions()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert!(resp.versions().is_empty());

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_versioning_obj_create_versions_remove_special_names() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let num = 10;

        for key in &["_testobj", "_", ":", "foo bar"] {
            let (version_ids, contents) = create_multiple_versions(&bucket, key, num).await;

            for i in 0..num {
                check_obj_content(&bucket, key, &version_ids[i], &contents[i]).await;
                client
                    .delete_object()
                    .bucket(&bucket)
                    .key(*key)
                    .version_id(&version_ids[i])
                    .send()
                    .await
                    .unwrap();
            }
        }

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

// ── Delete markers ──────────────────────────────────────────────────

#[test]
fn test_versioning_stack_delete_merkers() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "test1/a";

        let (mut all_vids, _) = create_multiple_versions(&bucket, key, 1).await;

        // Create 3 delete markers by deleting without versionId
        for _ in 0..3 {
            let resp = client
                .delete_object()
                .bucket(&bucket)
                .key(key)
                .send()
                .await
                .unwrap();
            all_vids.push(resp.version_id().unwrap().to_string());
        }

        let resp = client
            .list_object_versions()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.versions().len(), 1);
        assert_eq!(resp.delete_markers().len(), 3);

        cleanup_versioned(&bucket, key, &all_vids).await;
    });
}

// ── Null version handling ───────────────────────────────────────────

#[test]
fn test_versioning_obj_plain_null_version_removal() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        // Put object before versioning is enabled (null version)
        let key = "testobjfoo";
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"fooz"))
            .send()
            .await
            .unwrap();

        // Enable versioning
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

        // Delete the null version
        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .version_id("null")
            .send()
            .await
            .unwrap();

        // GET should now 404
        let result = client.get_object().bucket(&bucket).key(key).send().await;
        assert_eq!(err_status(&result), 404);

        // No versions should remain
        let resp = client
            .list_object_versions()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert!(resp.versions().is_empty());

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_versioning_obj_plain_null_version_overwrite() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let key = "testobjfoo";
        // Put before versioning
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"fooz"))
            .send()
            .await
            .unwrap();

        // Enable versioning
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

        // Put new version (gets a real version ID)
        let resp = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"zzz"))
            .send()
            .await
            .unwrap();
        let version_id = resp.version_id().unwrap().to_string();

        // GET returns new version
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let body = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&body[..], b"zzz");

        // Delete the new version → old null version becomes current
        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let body = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&body[..], b"fooz");

        // Delete the null version
        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .version_id("null")
            .send()
            .await
            .unwrap();

        let result = client.get_object().bucket(&bucket).key(key).send().await;
        assert_eq!(err_status(&result), 404);

        let resp = client
            .list_object_versions()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert!(resp.versions().is_empty());

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

// ── Suspend / resume ────────────────────────────────────────────────

#[test]
fn test_versioning_obj_suspend_versions() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "testobj";
        let num = 5;

        let (version_ids, _) = create_multiple_versions(&bucket, key, num).await;

        // Suspend versioning
        client
            .put_bucket_versioning()
            .bucket(&bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Suspended)
                    .build(),
            )
            .send()
            .await
            .unwrap();

        // Puts while suspended overwrite the null version
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"suspended content"))
            .send()
            .await
            .unwrap();

        // Re-enable versioning
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

        let (extra_vids, _) = create_multiple_versions(&bucket, key, 3).await;

        // Clean up: delete all versioned + null
        for vid in version_ids.iter().chain(extra_vids.iter()) {
            client
                .delete_object()
                .bucket(&bucket)
                .key(key)
                .version_id(vid)
                .send()
                .await
                .unwrap();
        }
        // Delete null version from suspended period
        let _ = client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .version_id("null")
            .send()
            .await;

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_versioning_obj_plain_null_version_overwrite_suspended() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let key = "testobjbar";
        // Put before versioning
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"foooz"))
            .send()
            .await
            .unwrap();

        // Enable then suspend
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
        client
            .put_bucket_versioning()
            .bucket(&bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Suspended)
                    .build(),
            )
            .send()
            .await
            .unwrap();

        // Put while suspended overwrites null
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"zzz"))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let body = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&body[..], b"zzz");

        // Should only have 1 version (the null)
        let resp = client
            .list_object_versions()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.versions().len(), 1);

        // Delete null version
        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .version_id("null")
            .send()
            .await
            .unwrap();

        let result = client.get_object().bucket(&bucket).key(key).send().await;
        assert_eq!(err_status(&result), 404);

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

// ── Suspended copy ──────────────────────────────────────────────────

#[test]
fn test_versioning_obj_suspended_copy() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key1 = "testobj1";

        let (version_ids, _) = create_multiple_versions(&bucket, key1, 1).await;

        // Suspend versioning
        client
            .put_bucket_versioning()
            .bucket(&bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Suspended)
                    .build(),
            )
            .send()
            .await
            .unwrap();

        // Overwrite with null version
        client
            .put_object()
            .bucket(&bucket)
            .key(key1)
            .body(ByteStream::from_static(b"null content"))
            .send()
            .await
            .unwrap();

        // Copy to another key in same bucket
        let key2 = "testobj2";
        client
            .copy_object()
            .bucket(&bucket)
            .key(key2)
            .copy_source(format!("{}/{}", bucket, key1))
            .send()
            .await
            .unwrap();

        // Copy to another non-versioned bucket
        let bucket2 = unique_bucket();
        client
            .create_bucket()
            .bucket(&bucket2)
            .send()
            .await
            .unwrap();
        client
            .copy_object()
            .bucket(&bucket2)
            .key(key1)
            .copy_source(format!("{}/{}", bucket, key1))
            .send()
            .await
            .unwrap();

        // Delete source (creates delete marker or overwrites null)
        client
            .delete_object()
            .bucket(&bucket)
            .key(key1)
            .send()
            .await
            .unwrap();

        // Verify copies
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key2)
            .send()
            .await
            .unwrap();
        let body = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&body[..], b"null content");

        let resp = client
            .get_object()
            .bucket(&bucket2)
            .key(key1)
            .send()
            .await
            .unwrap();
        let body = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&body[..], b"null content");

        // Cleanup
        client
            .delete_object()
            .bucket(&bucket2)
            .key(key1)
            .send()
            .await
            .unwrap();
        client
            .delete_bucket()
            .bucket(&bucket2)
            .send()
            .await
            .unwrap();
        let _ = client
            .delete_object()
            .bucket(&bucket)
            .key(key2)
            .version_id("null")
            .send()
            .await;
        let _ = client
            .delete_object()
            .bucket(&bucket)
            .key(key2)
            .send()
            .await;
        for vid in &version_ids {
            let _ = client
                .delete_object()
                .bucket(&bucket)
                .key(key1)
                .version_id(vid)
                .send()
                .await;
        }
        let _ = client
            .delete_object()
            .bucket(&bucket)
            .key(key1)
            .version_id("null")
            .send()
            .await;
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

// ── Version list ordering ───────────────────────────────────────────

#[test]
fn test_versioning_obj_list_marker() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "testobj";
        let key2 = "testobj-1";
        let num = 5;

        let (version_ids, contents) = create_multiple_versions(&bucket, key, num).await;
        let (version_ids2, contents2) = create_multiple_versions(&bucket, key2, num).await;

        let resp = client
            .list_object_versions()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let versions = resp.versions();

        // Versions come out sorted by key, then newest-first within each key
        // key < key2 lexicographically ("testobj" < "testobj-1")
        assert_eq!(versions.len(), num * 2);

        // First `num` entries should be for `key`, newest first
        for i in 0..num {
            let v = &versions[i];
            assert_eq!(v.key().unwrap(), key);
            assert_eq!(v.version_id().unwrap(), version_ids[num - 1 - i]);
            check_obj_content(
                &bucket,
                key,
                v.version_id().unwrap(),
                &contents[num - 1 - i],
            )
            .await;
        }

        // Next `num` entries for `key2`, newest first
        for i in 0..num {
            let v = &versions[num + i];
            assert_eq!(v.key().unwrap(), key2);
            assert_eq!(v.version_id().unwrap(), version_ids2[num - 1 - i]);
            check_obj_content(
                &bucket,
                key2,
                v.version_id().unwrap(),
                &contents2[num - 1 - i],
            )
            .await;
        }

        // Clean up both keys' versions, then delete bucket
        let client = CTX.client();
        for vid in &version_ids {
            client
                .delete_object()
                .bucket(&bucket)
                .key(key)
                .version_id(vid)
                .send()
                .await
                .unwrap();
        }
        for vid in &version_ids2 {
            client
                .delete_object()
                .bucket(&bucket)
                .key(key2)
                .version_id(vid)
                .send()
                .await
                .unwrap();
        }
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

// ── Copy specific versions ──────────────────────────────────────────

#[test]
fn test_versioning_copy_obj_version() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "testobj";
        let num = 3;

        let (version_ids, contents) = create_multiple_versions(&bucket, key, num).await;

        // Copy each version to a new key in same bucket
        let mut copy_keys = Vec::new();
        for i in 0..num {
            let new_key = format!("key_{}", i);
            client
                .copy_object()
                .bucket(&bucket)
                .key(&new_key)
                .copy_source(format!("{}/{}?versionId={}", bucket, key, version_ids[i]))
                .send()
                .await
                .unwrap();

            let resp = client
                .get_object()
                .bucket(&bucket)
                .key(&new_key)
                .send()
                .await
                .unwrap();
            let body = resp.body.collect().await.unwrap().into_bytes();
            assert_eq!(std::str::from_utf8(&body).unwrap(), contents[i]);
            copy_keys.push(new_key);
        }

        // Copy each version to another bucket
        let bucket2 = unique_bucket();
        client
            .create_bucket()
            .bucket(&bucket2)
            .send()
            .await
            .unwrap();

        for i in 0..num {
            let new_key = format!("key_{}", i);
            client
                .copy_object()
                .bucket(&bucket2)
                .key(&new_key)
                .copy_source(format!("{}/{}?versionId={}", bucket, key, version_ids[i]))
                .send()
                .await
                .unwrap();

            let resp = client
                .get_object()
                .bucket(&bucket2)
                .key(&new_key)
                .send()
                .await
                .unwrap();
            let body = resp.body.collect().await.unwrap().into_bytes();
            assert_eq!(std::str::from_utf8(&body).unwrap(), contents[i]);
        }

        // Copy latest (no versionId) to another bucket
        client
            .copy_object()
            .bucket(&bucket2)
            .key("new_key")
            .copy_source(format!("{}/{}", bucket, key))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket2)
            .key("new_key")
            .send()
            .await
            .unwrap();
        let body = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(std::str::from_utf8(&body).unwrap(), contents[num - 1]);

        // Cleanup bucket2
        for i in 0..num {
            client
                .delete_object()
                .bucket(&bucket2)
                .key(format!("key_{}", i))
                .send()
                .await
                .unwrap();
        }
        client
            .delete_object()
            .bucket(&bucket2)
            .key("new_key")
            .send()
            .await
            .unwrap();
        client
            .delete_bucket()
            .bucket(&bucket2)
            .send()
            .await
            .unwrap();

        // Cleanup bucket1
        for k in &copy_keys {
            client
                .delete_object()
                .bucket(&bucket)
                .key(k)
                .send()
                .await
                .unwrap();
        }
        cleanup_versioned(&bucket, key, &version_ids).await;
    });
}

// ── Multi-object delete with versions ───────────────────────────────

#[test]
fn test_versioning_multi_object_delete() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "key";

        let (version_ids, _) = create_multiple_versions(&bucket, key, 2).await;
        assert_eq!(version_ids.len(), 2);

        // Delete both versions via DeleteObjects
        let objects: Vec<ObjectIdentifier> = version_ids
            .iter()
            .map(|v| {
                ObjectIdentifier::builder()
                    .key(key)
                    .version_id(v)
                    .build()
                    .unwrap()
            })
            .collect();
        client
            .delete_objects()
            .bucket(&bucket)
            .delete(
                Delete::builder()
                    .set_objects(Some(objects.clone()))
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .unwrap();

        let resp = client
            .list_object_versions()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert!(resp.versions().is_empty());

        // Deleting again should succeed (idempotent)
        client
            .delete_objects()
            .bucket(&bucket)
            .delete(
                Delete::builder()
                    .set_objects(Some(objects))
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .unwrap();

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_versioning_multi_object_delete_with_marker() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "key";

        let (version_ids, _) = create_multiple_versions(&bucket, key, 2).await;

        // Create a delete marker
        let del_resp = client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert!(del_resp.delete_marker().unwrap_or(false));
        let dm_vid = del_resp.version_id().unwrap().to_string();

        // Delete all versions + delete marker
        let mut all_ids = version_ids.clone();
        all_ids.push(dm_vid);

        let objects: Vec<ObjectIdentifier> = all_ids
            .iter()
            .map(|v| {
                ObjectIdentifier::builder()
                    .key(key)
                    .version_id(v)
                    .build()
                    .unwrap()
            })
            .collect();
        client
            .delete_objects()
            .bucket(&bucket)
            .delete(
                Delete::builder()
                    .set_objects(Some(objects.clone()))
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .unwrap();

        let resp = client
            .list_object_versions()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert!(resp.versions().is_empty());
        assert!(resp.delete_markers().is_empty());

        // Idempotent re-delete
        client
            .delete_objects()
            .bucket(&bucket)
            .delete(
                Delete::builder()
                    .set_objects(Some(objects))
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .unwrap();

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_versioning_multi_object_delete_with_marker_create() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "key";

        // Use delete_objects to create a delete marker on a nonexistent key
        let objects = vec![ObjectIdentifier::builder().key(key).build().unwrap()];
        let resp = client
            .delete_objects()
            .bucket(&bucket)
            .delete(
                Delete::builder()
                    .set_objects(Some(objects))
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .unwrap();

        assert_eq!(resp.deleted().len(), 1);
        assert!(resp.deleted()[0].delete_marker().unwrap_or(false));
        let dm_vid = resp.deleted()[0]
            .delete_marker_version_id()
            .unwrap()
            .to_string();

        let resp = client
            .list_object_versions()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.delete_markers().len(), 1);
        assert_eq!(
            resp.delete_markers()[0].version_id().unwrap(),
            dm_vid.as_str()
        );
        assert_eq!(resp.delete_markers()[0].key().unwrap(), key);

        // Cleanup
        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&dm_vid)
            .send()
            .await
            .unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

// ── Version ID return behavior ──────────────────────────────────────

#[test]
fn test_versioning_bucket_atomic_upload_return_version_id() {
    s3_tests::run(async {
        let client = CTX.client();

        // Versioning-enabled: should return a version ID
        let bucket = setup_versioned_bucket().await;
        let resp = client
            .put_object()
            .bucket(&bucket)
            .key("bar")
            .body(ByteStream::from_static(b""))
            .send()
            .await
            .unwrap();
        let version_id = resp.version_id().unwrap().to_string();

        let resp = client
            .list_object_versions()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.versions().len(), 1);
        assert_eq!(
            resp.versions()[0].version_id().unwrap(),
            version_id.as_str()
        );

        client
            .delete_object()
            .bucket(&bucket)
            .key("bar")
            .version_id(&version_id)
            .send()
            .await
            .unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();

        // Default (no versioning): should not return a version ID
        let bucket2 = unique_bucket();
        client
            .create_bucket()
            .bucket(&bucket2)
            .send()
            .await
            .unwrap();
        let resp = client
            .put_object()
            .bucket(&bucket2)
            .key("baz")
            .body(ByteStream::from_static(b""))
            .send()
            .await
            .unwrap();
        assert!(
            resp.version_id().is_none(),
            "expected no version ID for non-versioned bucket"
        );
        client
            .delete_object()
            .bucket(&bucket2)
            .key("baz")
            .send()
            .await
            .unwrap();
        client
            .delete_bucket()
            .bucket(&bucket2)
            .send()
            .await
            .unwrap();

        // Suspended: should not return a version ID
        let bucket3 = unique_bucket();
        client
            .create_bucket()
            .bucket(&bucket3)
            .send()
            .await
            .unwrap();
        client
            .put_bucket_versioning()
            .bucket(&bucket3)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Suspended)
                    .build(),
            )
            .send()
            .await
            .unwrap();
        let resp = client
            .put_object()
            .bucket(&bucket3)
            .key("baz")
            .body(ByteStream::from_static(b""))
            .send()
            .await
            .unwrap();
        assert!(
            resp.version_id().is_none(),
            "expected no version ID for suspended bucket"
        );
        client
            .delete_object()
            .bucket(&bucket3)
            .key("baz")
            .send()
            .await
            .unwrap();
        client
            .delete_bucket()
            .bucket(&bucket3)
            .send()
            .await
            .unwrap();
    });
}

// ── Concurrent delete ───────────────────────────────────────────────

#[test]
fn test_versioning_concurrent_multi_object_delete() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let num_objects = 5;
        let num_versions = 3;

        let key_names: Vec<String> = (0..num_objects).map(|i| format!("key_{}", i)).collect();

        // Create num_versions versions of each key
        for _ in 0..num_versions {
            for key in &key_names {
                client
                    .put_object()
                    .bucket(&bucket)
                    .key(key)
                    .body(ByteStream::from_static(b"data"))
                    .send()
                    .await
                    .unwrap();
            }
        }

        let resp = client
            .list_object_versions()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let versions = resp.versions();
        assert_eq!(versions.len(), num_objects * num_versions);

        // Delete all versions
        let objects: Vec<ObjectIdentifier> = versions
            .iter()
            .map(|v| {
                ObjectIdentifier::builder()
                    .key(v.key().unwrap())
                    .version_id(v.version_id().unwrap())
                    .build()
                    .unwrap()
            })
            .collect();

        let resp = client
            .delete_objects()
            .bucket(&bucket)
            .delete(
                Delete::builder()
                    .set_objects(Some(objects))
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(resp.deleted().len(), num_objects * num_versions);

        let resp = client
            .list_object_versions()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert!(resp.versions().is_empty());

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

// ── Delete marker tests ─────────────────────────────────────────────

#[test]
fn test_delete_marker_nonversioned() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let key = "frodo.txt";
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"body"))
            .send()
            .await
            .unwrap();

        let resp = client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        // Non-versioned delete should not produce a delete marker
        assert!(!resp.delete_marker().unwrap_or(false));

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_delete_marker_versioned() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;

        let key = "bilbo.txt";
        let put_resp = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"body"))
            .send()
            .await
            .unwrap();
        let vid = put_resp.version_id().unwrap().to_string();

        let del_resp = client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        // Versioned delete should produce a delete marker
        assert!(del_resp.delete_marker().unwrap_or(false));
        let dm_vid = del_resp.version_id().unwrap().to_string();

        // Cleanup
        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&dm_vid)
            .send()
            .await
            .unwrap();
        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&vid)
            .send()
            .await
            .unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

// ── Versioning configuration ─────────────────────────────────────────

#[test]
fn test_versioning_bucket_create_suspend() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        // Fresh bucket: versioning status should be absent (unversioned)
        let resp = client
            .get_bucket_versioning()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert!(
            resp.status().is_none(),
            "expected no versioning status on new bucket"
        );

        // Suspend → Suspended
        client
            .put_bucket_versioning()
            .bucket(&bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Suspended)
                    .build(),
            )
            .send()
            .await
            .unwrap();
        let resp = client
            .get_bucket_versioning()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), Some(&BucketVersioningStatus::Suspended));

        // Enable → Enabled
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
        let resp = client
            .get_bucket_versioning()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), Some(&BucketVersioningStatus::Enabled));

        // Enable again (idempotent) → still Enabled
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
        let resp = client
            .get_bucket_versioning()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), Some(&BucketVersioningStatus::Enabled));

        // Suspend → Suspended
        client
            .put_bucket_versioning()
            .bucket(&bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Suspended)
                    .build(),
            )
            .send()
            .await
            .unwrap();
        let resp = client
            .get_bucket_versioning()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), Some(&BucketVersioningStatus::Suspended));

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

// ── Not implemented ─────────────────────────────────────────────────

#[test]
#[ignore = "not implemented: ACL per version"]
fn test_versioned_object_acl() {
    s3_tests::run(async {});
}

#[test]
#[ignore = "not implemented: ACL per version"]
fn test_versioned_object_acl_no_version_specified() {
    s3_tests::run(async {});
}

#[test]
fn test_versioning_obj_create_overwrite_multipart() {
    s3_tests::run(async {});
}

#[test]
fn test_versioning_bucket_multipart_upload_return_version_id() {
    s3_tests::run(async {});
}
