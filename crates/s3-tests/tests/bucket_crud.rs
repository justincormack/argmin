use std::time::{Duration, Instant};

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::VersioningConfiguration;
use s3_tests::{
    assert_s3_err_code, cleanup_versioned_bucket, ensure_distinct_s3_owners_or_skip, err_status,
    unique_bucket, CTX,
};

fn assert_canonical_owner_id(id: &str) {
    assert_eq!(
        id.len(),
        64,
        "expected 64-char canonical owner ID, got {id}"
    );
    assert!(
        id.bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
        "expected lowercase hex canonical owner ID, got {id}"
    );
}

async fn recreate_bucket_after_delete(client: &aws_sdk_s3::Client, bucket: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let result = client.create_bucket().bucket(bucket).send().await;
        match result {
            Ok(_) => return,
            Err(err) => {
                let debug = format!("{err:?}");
                if !debug.contains("BucketAlreadyExists") {
                    panic!("expected BucketAlreadyExists while waiting to recreate bucket, got {debug}");
                }
                if Instant::now() >= deadline {
                    panic!("bucket {bucket} was not reusable within 5s after delete: {debug}");
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

fn primary_account_id_or_skip(test_name: &str) -> Option<String> {
    match CTX.account_id() {
        Some(account_id) => Some(account_id.to_string()),
        None => {
            eprintln!("skipping {test_name}: S3_TEST_ACCOUNT_ID is not configured");
            None
        }
    }
}

// ── CreateBucket ─────────────────────────────────────────────────────

#[test]
fn test_bucket_create_delete() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();
        // Clean up
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_create_exists() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        // Verify bucket exists via HEAD
        client.head_bucket().bucket(&bucket).send().await.unwrap();

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_create_already_exists() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        // Creating the same bucket again by the same owner succeeds (idempotent,
        // matches AWS BucketAlreadyOwnedByYou behavior — returns 200).
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

// ── DeleteBucket ─────────────────────────────────────────────────────

#[test]
fn test_bucket_delete_notexist() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let result = client.delete_bucket().bucket(&bucket).send().await;
        assert_eq!(err_status(&result), 404);
    });
}

#[test]
fn test_bucket_delete_nonempty() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        client
            .put_object()
            .bucket(&bucket)
            .key("key")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        // Delete bucket should fail (not empty)
        let result = client.delete_bucket().bucket(&bucket).send().await;
        assert_eq!(err_status(&result), 409);

        // Clean up
        client
            .delete_object()
            .bucket(&bucket)
            .key("key")
            .send()
            .await
            .unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

/// Deleting a versioned bucket that contains only delete markers must fail
/// with 409 BucketNotEmpty. On AWS, delete_object on a versioned bucket
/// creates a delete marker rather than removing the object.
#[test]
fn test_bucket_delete_nonempty_delete_markers() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        // Enable versioning
        client
            .put_bucket_versioning()
            .bucket(&bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send()
            .await
            .unwrap();

        // Put an object, then delete it (creates a delete marker)
        client
            .put_object()
            .bucket(&bucket)
            .key("key")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();
        client
            .delete_object()
            .bucket(&bucket)
            .key("key")
            .send()
            .await
            .unwrap();

        // Bucket still has versions + delete marker; delete must fail
        let result = client.delete_bucket().bucket(&bucket).send().await;
        assert_eq!(err_status(&result), 409);

        // Clean up properly
        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_bucket_delete_then_recreate() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();

        client.create_bucket().bucket(&bucket).send().await.unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();

        // AWS documents that bucket removal can take time to finish, and
        // immediate same-name recreate may transiently return BucketAlreadyExists.
        recreate_bucket_after_delete(client, &bucket).await;
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

// ── HeadBucket ───────────────────────────────────────────────────────

#[test]
fn test_bucket_head() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        client.head_bucket().bucket(&bucket).send().await.unwrap();

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_head_expected_owner() {
    s3_tests::run(async {
        let Some(account_id) = primary_account_id_or_skip("test_bucket_head_expected_owner") else {
            return;
        };
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        client
            .head_bucket()
            .bucket(&bucket)
            .customize()
            .mutate_request({
                let account_id = account_id.clone();
                move |req| {
                    req.headers_mut()
                        .insert("x-amz-expected-bucket-owner", account_id.clone());
                }
            })
            .send()
            .await
            .unwrap();

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_head_wrong_expected_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let result = client
            .head_bucket()
            .bucket(&bucket)
            .customize()
            .mutate_request(|req| {
                req.headers_mut()
                    .insert("x-amz-expected-bucket-owner", "000000000000");
            })
            .send()
            .await;
        assert_eq!(err_status(&result), 403);

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_head_notexist() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let result = client.head_bucket().bucket(&bucket).send().await;
        assert!(result.is_err());
    });
}

// ── ListBuckets ──────────────────────────────────────────────────────

#[test]
fn test_buckets_list_empty() {
    s3_tests::run(async {
        // Note: this test may see buckets from other concurrent tests.
        // We just verify that list_buckets returns without error.
        let client = CTX.client();
        let _resp = client.list_buckets().send().await.unwrap();
    });
}

#[test]
fn test_buckets_list_contains_created() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let resp = client.list_buckets().send().await.unwrap();
        let names: Vec<&str> = resp.buckets().iter().filter_map(|b| b.name()).collect();
        assert!(
            names.contains(&bucket.as_str()),
            "expected bucket '{}' in list: {:?}",
            bucket,
            names
        );
        let owner = resp.owner().expect("expected owner in ListBuckets");
        let owner_id = owner.id().expect("expected owner ID in ListBuckets");
        assert_canonical_owner_id(owner_id);

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_list_objects_empty() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.key_count(), Some(0));
        assert!(resp.contents().is_empty());

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_list_objects_with_objects() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        for i in 0..3 {
            let key = format!("key{}", i);
            client
                .put_object()
                .bucket(&bucket)
                .key(&key)
                .body(ByteStream::from_static(b"content"))
                .send()
                .await
                .unwrap();
        }

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.key_count(), Some(3));

        let keys: Vec<&str> = resp.contents().iter().filter_map(|o| o.key()).collect();
        assert_eq!(keys, vec!["key0", "key1", "key2"]);

        // Clean up
        for i in 0..3 {
            client
                .delete_object()
                .bucket(&bucket)
                .key(format!("key{}", i))
                .send()
                .await
                .unwrap();
        }
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_list_objects_nonexistent_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let result = client.list_objects_v2().bucket(&bucket).send().await;
        assert!(result.is_err());
    });
}

// ── Extended HEAD / ACL / ownership ─────────────────────────────────

#[test]
fn test_bucket_head_extended() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        // HEAD should return without error and include standard headers
        client.head_bucket().bucket(&bucket).send().await.unwrap();

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_create_special_key_names() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        // Create objects with special key names
        let special_keys = &["foo/bar", "foo&bar", "foo bar", "foo+bar"];
        for key in special_keys {
            client
                .put_object()
                .bucket(&bucket)
                .key(*key)
                .body(ByteStream::from_static(b"data"))
                .send()
                .await
                .unwrap();
        }

        // Verify they all exist
        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.key_count(), Some(special_keys.len() as i32));

        // Clean up
        for key in special_keys {
            client
                .delete_object()
                .bucket(&bucket)
                .key(*key)
                .send()
                .await
                .unwrap();
        }
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_buckets_list_ctime() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let resp = client.list_buckets().send().await.unwrap();
        let found = resp
            .buckets()
            .iter()
            .find(|b| b.name() == Some(bucket.as_str()));
        assert!(found.is_some(), "bucket should be in listing");
        assert!(
            found.unwrap().creation_date().is_some(),
            "bucket should have creation date"
        );

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_create_exists_nonowner() {
    s3_tests::run(async {
        let client = CTX.client();
        if !CTX.has_alt_client() {
            eprintln!(
                "skipping test_bucket_create_exists_nonowner: alternate credentials are not configured"
            );
            return;
        }
        let alt_client = CTX.alt_client();
        if !ensure_distinct_s3_owners_or_skip(
            client,
            alt_client,
            "test_bucket_create_exists_nonowner",
        )
        .await
        {
            return;
        }

        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let result = alt_client.create_bucket().bucket(&bucket).send().await;
        assert_eq!(err_status(&result), 409);
        assert_s3_err_code(&result, "BucketAlreadyExists");

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
#[ignore = "not implemented: ACL grants"]
fn test_bucket_header_acl_grants() {
    s3_tests::run(async {});
}
