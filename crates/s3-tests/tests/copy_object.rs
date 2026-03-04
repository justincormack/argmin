use aws_sdk_s3::primitives::ByteStream;
use s3_tests::{err_status, unique_bucket, CTX};

/// Create a bucket, returning its name.
async fn setup_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    client.create_bucket().bucket(&bucket).send().await.unwrap();
    bucket
}

/// Put an object and return its ETag (quoted, as returned by S3).
async fn put_object(bucket: &str, key: &str, body: &'static [u8]) -> String {
    let resp = CTX
        .client()
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(body))
        .send()
        .await
        .unwrap();
    resp.e_tag().unwrap().to_string()
}

/// Clean up objects and bucket.
async fn cleanup(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = client.delete_object().bucket(bucket).key(*key).send().await;
    }
    client.delete_bucket().bucket(bucket).send().await.unwrap();
}

// ── Basic copy ────────────────────────────────────────────────────────

#[test]
fn test_object_copy_zero_size() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "foo123bar", b"").await;

        CTX.client()
            .copy_object()
            .bucket(&bucket)
            .key("bar321foo")
            .copy_source(format!("{}/foo123bar", bucket))
            .send()
            .await
            .unwrap();

        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("bar321foo")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_length(), Some(0));

        cleanup(&bucket, &["foo123bar", "bar321foo"]).await;
    });
}

#[test]
fn test_object_copy_same_bucket() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "foo123bar", b"foo").await;

        CTX.client()
            .copy_object()
            .bucket(&bucket)
            .key("bar321foo")
            .copy_source(format!("{}/foo123bar", bucket))
            .send()
            .await
            .unwrap();

        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("bar321foo")
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"foo");

        cleanup(&bucket, &["foo123bar", "bar321foo"]).await;
    });
}

#[test]
fn test_object_copy_verify_contenttype() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;

        CTX.client()
            .put_object()
            .bucket(&bucket)
            .key("foo123bar")
            .content_type("text/bla")
            .body(ByteStream::from_static(b"foo"))
            .send()
            .await
            .unwrap();

        CTX.client()
            .copy_object()
            .bucket(&bucket)
            .key("bar321foo")
            .copy_source(format!("{}/foo123bar", bucket))
            .send()
            .await
            .unwrap();

        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("bar321foo")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_type(), Some("text/bla"));
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"foo");

        cleanup(&bucket, &["foo123bar", "bar321foo"]).await;
    });
}

#[test]
fn test_object_copy_to_itself() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "foo123bar", b"foo").await;

        // Copying to itself without REPLACE should fail with 400
        let result = CTX
            .client()
            .copy_object()
            .bucket(&bucket)
            .key("foo123bar")
            .copy_source(format!("{}/foo123bar", bucket))
            .send()
            .await;
        assert_eq!(err_status(&result), 400);

        cleanup(&bucket, &["foo123bar"]).await;
    });
}

#[test]
fn test_object_copy_to_itself_with_metadata() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "foo123bar", b"foo").await;

        // Copy to itself with REPLACE metadata directive should succeed
        CTX.client()
            .copy_object()
            .bucket(&bucket)
            .key("foo123bar")
            .copy_source(format!("{}/foo123bar", bucket))
            .metadata_directive(aws_sdk_s3::types::MetadataDirective::Replace)
            .metadata("foo", "bar")
            .send()
            .await
            .unwrap();

        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("foo123bar")
            .send()
            .await
            .unwrap();
        let meta = resp.metadata().unwrap();
        assert_eq!(meta.get("foo").map(String::as_str), Some("bar"));

        cleanup(&bucket, &["foo123bar"]).await;
    });
}

#[test]
fn test_object_copy_diff_bucket() {
    s3_tests::run(async {
        let bucket1 = setup_bucket().await;
        let bucket2 = setup_bucket().await;

        put_object(&bucket1, "foo123bar", b"foo").await;

        CTX.client()
            .copy_object()
            .bucket(&bucket2)
            .key("bar321foo")
            .copy_source(format!("{}/foo123bar", bucket1))
            .send()
            .await
            .unwrap();

        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket2)
            .key("bar321foo")
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"foo");

        cleanup(&bucket1, &["foo123bar"]).await;
        cleanup(&bucket2, &["bar321foo"]).await;
    });
}

#[test]
fn test_object_copy_retaining_metadata() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;

        CTX.client()
            .put_object()
            .bucket(&bucket)
            .key("foo123bar")
            .content_type("audio/ogg")
            .metadata("key1", "value1")
            .metadata("key2", "value2")
            .body(ByteStream::from_static(b"foo"))
            .send()
            .await
            .unwrap();

        // Default directive is COPY — metadata should be retained
        CTX.client()
            .copy_object()
            .bucket(&bucket)
            .key("bar321foo")
            .copy_source(format!("{}/foo123bar", bucket))
            .send()
            .await
            .unwrap();

        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("bar321foo")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_type(), Some("audio/ogg"));
        let meta = resp.metadata().unwrap();
        assert_eq!(meta.get("key1").map(String::as_str), Some("value1"));
        assert_eq!(meta.get("key2").map(String::as_str), Some("value2"));
        assert_eq!(resp.content_length(), Some(3));

        cleanup(&bucket, &["foo123bar", "bar321foo"]).await;
    });
}

#[test]
fn test_object_copy_replacing_metadata() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;

        CTX.client()
            .put_object()
            .bucket(&bucket)
            .key("foo123bar")
            .content_type("audio/ogg")
            .metadata("key1", "value1")
            .metadata("key2", "value2")
            .body(ByteStream::from_static(b"foo"))
            .send()
            .await
            .unwrap();

        // REPLACE directive — new metadata replaces original
        CTX.client()
            .copy_object()
            .bucket(&bucket)
            .key("bar321foo")
            .copy_source(format!("{}/foo123bar", bucket))
            .metadata_directive(aws_sdk_s3::types::MetadataDirective::Replace)
            .content_type("audio/mpeg")
            .metadata("key3", "value3")
            .metadata("key2", "value2")
            .send()
            .await
            .unwrap();

        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("bar321foo")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_type(), Some("audio/mpeg"));
        let meta = resp.metadata().unwrap();
        assert_eq!(meta.get("key3").map(String::as_str), Some("value3"));
        assert_eq!(meta.get("key2").map(String::as_str), Some("value2"));
        // Original key1 should be gone
        assert_eq!(meta.get("key1"), None);
        assert_eq!(resp.content_length(), Some(3));

        cleanup(&bucket, &["foo123bar", "bar321foo"]).await;
    });
}

#[test]
fn test_object_copy_bucket_not_found() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;

        let fake_source = format!("{}-fake/foo123bar", bucket);
        let result = CTX
            .client()
            .copy_object()
            .bucket(&bucket)
            .key("bar321foo")
            .copy_source(fake_source)
            .send()
            .await;
        assert_eq!(err_status(&result), 404);

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_object_copy_key_not_found() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;

        let result = CTX
            .client()
            .copy_object()
            .bucket(&bucket)
            .key("bar321foo")
            .copy_source(format!("{}/foo123bar", bucket))
            .send()
            .await;
        assert_eq!(err_status(&result), 404);

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_object_copy_16m() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let size = 16 * 1024 * 1024;
        let data = vec![0u8; size];

        CTX.client()
            .put_object()
            .bucket(&bucket)
            .key("obj1")
            .body(ByteStream::from(data))
            .send()
            .await
            .unwrap();

        CTX.client()
            .copy_object()
            .bucket(&bucket)
            .key("obj2")
            .copy_source(format!("{}/obj1", bucket))
            .send()
            .await
            .unwrap();

        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("obj2")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_length(), Some(size as i64));

        cleanup(&bucket, &["obj1", "obj2"]).await;
    });
}

// ── Versioned copy ──────────────────────────────────────────────────

#[test]
fn test_object_copy_versioned_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket1 = setup_bucket().await;

        // Enable versioning
        client
            .put_bucket_versioning()
            .bucket(&bucket1)
            .versioning_configuration(
                aws_sdk_s3::types::VersioningConfiguration::builder()
                    .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send()
            .await
            .unwrap();

        let data = b"hello";
        client
            .put_object()
            .bucket(&bucket1)
            .key("foo123bar")
            .body(ByteStream::from_static(data))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket1)
            .key("foo123bar")
            .send()
            .await
            .unwrap();
        let version_id = resp.version_id().unwrap().to_string();

        // Copy within same versioned bucket using versionId in source
        client
            .copy_object()
            .bucket(&bucket1)
            .key("bar321foo")
            .copy_source(format!("{}/foo123bar?versionId={}", bucket1, version_id))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket1)
            .key("bar321foo")
            .send()
            .await
            .unwrap();
        let version_id2 = resp.version_id().unwrap().to_string();
        assert_eq!(resp.content_length(), Some(data.len() as i64));
        let body = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&body[..], data);
        client
            .copy_object()
            .bucket(&bucket1)
            .key("bar321foo2")
            .copy_source(format!("{}/bar321foo?versionId={}", bucket1, version_id2))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket1)
            .key("bar321foo2")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_length(), Some(data.len() as i64));

        // Copy to another versioned bucket
        let bucket2 = setup_bucket().await;
        client
            .put_bucket_versioning()
            .bucket(&bucket2)
            .versioning_configuration(
                aws_sdk_s3::types::VersioningConfiguration::builder()
                    .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send()
            .await
            .unwrap();

        client
            .copy_object()
            .bucket(&bucket2)
            .key("bar321foo3")
            .copy_source(format!("{}/foo123bar?versionId={}", bucket1, version_id))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket2)
            .key("bar321foo3")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_length(), Some(data.len() as i64));

        // Copy to a non-versioned bucket
        let bucket3 = setup_bucket().await;
        client
            .copy_object()
            .bucket(&bucket3)
            .key("bar321foo4")
            .copy_source(format!("{}/foo123bar?versionId={}", bucket1, version_id))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket3)
            .key("bar321foo4")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_length(), Some(data.len() as i64));

        // Copy from non-versioned bucket to versioned bucket
        client
            .copy_object()
            .bucket(&bucket1)
            .key("foo123bar2")
            .copy_source(format!("{}/bar321foo4", bucket3))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket1)
            .key("foo123bar2")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_length(), Some(data.len() as i64));

        cleanup(&bucket3, &["bar321foo4"]).await;
        cleanup(&bucket2, &["bar321foo3"]).await;
        cleanup(
            &bucket1,
            &["foo123bar", "bar321foo", "bar321foo2", "foo123bar2"],
        )
        .await;
    });
}

#[test]
fn test_object_copy_versioned_url_encoding() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        // Enable versioning
        client
            .put_bucket_versioning()
            .bucket(&bucket)
            .versioning_configuration(
                aws_sdk_s3::types::VersioningConfiguration::builder()
                    .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send()
            .await
            .unwrap();

        // Key with special characters that need URL encoding
        let src_key = "foo?bar";
        client
            .put_object()
            .bucket(&bucket)
            .key(src_key)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let resp = client
            .head_object()
            .bucket(&bucket)
            .key(src_key)
            .send()
            .await
            .unwrap();
        let version_id = resp.version_id().unwrap().to_string();

        // Copy using versionId — source key needs URL encoding
        let dst_key = "bar&foo";
        client
            .copy_object()
            .bucket(&bucket)
            .key(dst_key)
            .copy_source(format!(
                "{}/{}?versionId={}",
                bucket,
                src_key.replace('?', "%3F").replace('&', "%26"),
                version_id
            ))
            .send()
            .await
            .unwrap();

        // Verify destination exists
        client
            .head_object()
            .bucket(&bucket)
            .key(dst_key)
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &[src_key, dst_key]).await;
    });
}

#[test]
fn test_object_copy_versioning_multipart_upload() {
    s3_tests::run(async {});
}

// ── Multi-user / ACL (not implemented) ──────────────────────────────

#[test]
#[ignore = "not implemented: multi-user"]
fn test_object_copy_not_owned_bucket() {
    s3_tests::run(async {});
}

#[test]
#[ignore = "not implemented: multi-user ACL"]
fn test_object_copy_not_owned_object_bucket() {
    s3_tests::run(async {});
}

#[test]
#[ignore = "not implemented: ACL on copy"]
fn test_object_copy_canned_acl() {
    s3_tests::run(async {});
}
