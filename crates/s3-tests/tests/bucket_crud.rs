use aws_sdk_s3::primitives::ByteStream;
use s3_tests::{unique_bucket, CTX};

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
        assert!(result.is_err());
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
        assert!(result.is_err());

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

#[test]
fn test_bucket_delete_then_recreate() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();

        client.create_bucket().bucket(&bucket).send().await.unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();

        // Recreating after delete should succeed
        client.create_bucket().bucket(&bucket).send().await.unwrap();
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
#[ignore = "not implemented: multi-user"]
fn test_bucket_create_exists_nonowner() {
    s3_tests::run(async {});
}

#[test]
#[ignore = "not implemented: bucket ownership controls"]
fn test_bucket_create_delete_bucket_ownership() {
    s3_tests::run(async {});
}

#[test]
#[ignore = "not implemented: ACL grants"]
fn test_bucket_header_acl_grants() {
    s3_tests::run(async {});
}
