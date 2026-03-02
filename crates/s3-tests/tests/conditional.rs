use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::primitives::DateTime;
use s3_tests::{unique_bucket, CTX};

/// Create a bucket, returning its name.
async fn setup_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    client.create_bucket().bucket(&bucket).send().await.unwrap();
    bucket
}

/// Put an object and return its ETag (unquoted).
async fn put_object(bucket: &str, key: &str, body: &'static [u8]) -> String {
    let client = CTX.client();
    let resp = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(body))
        .send()
        .await
        .unwrap();
    resp.e_tag().unwrap().to_string()
}

/// Cleanup helper: delete object + bucket.
async fn cleanup(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = client.delete_object().bucket(bucket).key(*key).send().await;
    }
    client.delete_bucket().bucket(bucket).send().await.unwrap();
}

// ── GET If-Match ────────────────────────────────────────────────────────

#[test]
fn test_get_object_ifmatch_good() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let etag = put_object(&bucket, "obj", b"hello").await;

        let resp = CTX.client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .if_match(&etag)
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"hello");

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_get_object_ifmatch_failed() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "obj", b"hello").await;

        let result = CTX.client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .if_match("\"0000000000000000\"")
            .send()
            .await;
        assert!(result.is_err(), "expected 412 PreconditionFailed");

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_get_object_ifmatch_wildcard() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "obj", b"hello").await;

        let resp = CTX.client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .if_match("*")
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"hello");

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── GET If-None-Match ───────────────────────────────────────────────────

#[test]
fn test_get_object_ifnonematch_good() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "obj", b"hello").await;

        // Different etag → should succeed
        let resp = CTX.client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .if_none_match("\"0000000000000000\"")
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"hello");

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_get_object_ifnonematch_failed() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let etag = put_object(&bucket, "obj", b"hello").await;

        // Same etag → should return 304 Not Modified
        let result = CTX.client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .if_none_match(&etag)
            .send()
            .await;
        assert!(result.is_err(), "expected 304 NotModified");

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_get_object_ifnonematch_wildcard() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "obj", b"hello").await;

        // * matches any etag → should return 304
        let result = CTX.client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .if_none_match("*")
            .send()
            .await;
        assert!(result.is_err(), "expected 304 NotModified");

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── GET If-Modified-Since ───────────────────────────────────────────────

#[test]
fn test_get_object_ifmodifiedsince_good() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "obj", b"hello").await;

        // Use a date in the past → object was modified after → should succeed
        let past = DateTime::from_secs(0);
        let resp = CTX.client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .if_modified_since(past)
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"hello");

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_get_object_ifmodifiedsince_failed() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "obj", b"hello").await;

        // Use a date far in the future → object not modified since → 304
        let future = DateTime::from_secs(4_102_444_800); // 2100-01-01
        let result = CTX.client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .if_modified_since(future)
            .send()
            .await;
        assert!(result.is_err(), "expected 304 NotModified");

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── GET If-Unmodified-Since ─────────────────────────────────────────────

#[test]
fn test_get_object_ifunmodifiedsince_good() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "obj", b"hello").await;

        // Use a date far in the future → object was last modified before → should succeed
        let future = DateTime::from_secs(4_102_444_800);
        let resp = CTX.client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .if_unmodified_since(future)
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"hello");

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_get_object_ifunmodifiedsince_failed() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "obj", b"hello").await;

        // Use epoch → object modified after → 412
        let past = DateTime::from_secs(0);
        let result = CTX.client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .if_unmodified_since(past)
            .send()
            .await;
        assert!(result.is_err(), "expected 412 PreconditionFailed");

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── HEAD If-Match / If-None-Match ───────────────────────────────────────

#[test]
fn test_head_object_ifmatch_good() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let etag = put_object(&bucket, "obj", b"hello").await;

        let resp = CTX.client()
            .head_object()
            .bucket(&bucket)
            .key("obj")
            .if_match(&etag)
            .send()
            .await
            .unwrap();
        assert!(resp.e_tag().is_some());

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_head_object_ifmatch_failed() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "obj", b"hello").await;

        let result = CTX.client()
            .head_object()
            .bucket(&bucket)
            .key("obj")
            .if_match("\"0000000000000000\"")
            .send()
            .await;
        assert!(result.is_err(), "expected 412 PreconditionFailed");

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_head_object_ifnonematch_good() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "obj", b"hello").await;

        let resp = CTX.client()
            .head_object()
            .bucket(&bucket)
            .key("obj")
            .if_none_match("\"0000000000000000\"")
            .send()
            .await
            .unwrap();
        assert!(resp.e_tag().is_some());

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_head_object_ifnonematch_failed() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let etag = put_object(&bucket, "obj", b"hello").await;

        let result = CTX.client()
            .head_object()
            .bucket(&bucket)
            .key("obj")
            .if_none_match(&etag)
            .send()
            .await;
        assert!(result.is_err(), "expected 304 NotModified");

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── PUT If-None-Match: * (create-only) ──────────────────────────────────

#[test]
fn test_put_object_ifnonematch_star_create() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;

        // Object doesn't exist → should succeed
        CTX.client()
            .put_object()
            .bucket(&bucket)
            .key("new")
            .if_none_match("*")
            .body(ByteStream::from_static(b"created"))
            .send()
            .await
            .unwrap();

        let resp = CTX.client()
            .get_object()
            .bucket(&bucket)
            .key("new")
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"created");

        cleanup(&bucket, &["new"]).await;
    });
}

#[test]
fn test_put_object_ifnonematch_star_overwrite_fails() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "existing", b"original").await;

        // Object already exists → should fail with 412
        let result = CTX.client()
            .put_object()
            .bucket(&bucket)
            .key("existing")
            .if_none_match("*")
            .body(ByteStream::from_static(b"overwrite"))
            .send()
            .await;
        assert!(result.is_err(), "expected 412 PreconditionFailed");

        // Verify original content unchanged
        let resp = CTX.client()
            .get_object()
            .bucket(&bucket)
            .key("existing")
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"original");

        cleanup(&bucket, &["existing"]).await;
    });
}

// ── PUT If-Match (conditional overwrite) ────────────────────────────────

#[test]
fn test_put_object_ifmatch_good() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let etag = put_object(&bucket, "obj", b"v1").await;

        // Matching etag → should succeed
        CTX.client()
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .if_match(&etag)
            .body(ByteStream::from_static(b"v2"))
            .send()
            .await
            .unwrap();

        let resp = CTX.client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"v2");

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_put_object_ifmatch_failed() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "obj", b"v1").await;

        // Wrong etag → should fail with 412
        let result = CTX.client()
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .if_match("\"0000000000000000\"")
            .body(ByteStream::from_static(b"v2"))
            .send()
            .await;
        assert!(result.is_err(), "expected 412 PreconditionFailed");

        // Verify original content unchanged
        let resp = CTX.client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"v1");

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_put_object_ifmatch_nonexistent() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;

        // Object doesn't exist → If-Match cannot be satisfied → 412
        let result = CTX.client()
            .put_object()
            .bucket(&bucket)
            .key("nonexistent")
            .if_match("\"0000000000000000\"")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await;
        assert!(result.is_err(), "expected 412 PreconditionFailed");

        cleanup(&bucket, &[]).await;
    });
}

// ── DELETE If-Match ─────────────────────────────────────────────────────

#[test]
fn test_delete_object_ifmatch_good() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let etag = put_object(&bucket, "obj", b"hello").await;

        // Matching etag → delete should succeed
        CTX.client()
            .delete_object()
            .bucket(&bucket)
            .key("obj")
            .if_match(&etag)
            .send()
            .await
            .unwrap();

        // Verify deleted
        let result = CTX.client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send()
            .await;
        assert!(result.is_err());

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_delete_object_ifmatch_failed() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "obj", b"hello").await;

        // Wrong etag → delete should fail with 412
        let result = CTX.client()
            .delete_object()
            .bucket(&bucket)
            .key("obj")
            .if_match("\"0000000000000000\"")
            .send()
            .await;
        assert!(result.is_err(), "expected 412 PreconditionFailed");

        // Verify still exists
        let resp = CTX.client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"hello");

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── Copy source conditions ──────────────────────────────────────────────

#[test]
fn test_copy_object_source_ifmatch_good() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let etag = put_object(&bucket, "src", b"source data").await;

        CTX.client()
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src", bucket))
            .copy_source_if_match(&etag)
            .send()
            .await
            .unwrap();

        let resp = CTX.client()
            .get_object()
            .bucket(&bucket)
            .key("dst")
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"source data");

        cleanup(&bucket, &["src", "dst"]).await;
    });
}

#[test]
fn test_copy_object_source_ifmatch_failed() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "src", b"source data").await;

        let result = CTX.client()
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src", bucket))
            .copy_source_if_match("\"0000000000000000\"")
            .send()
            .await;
        assert!(result.is_err(), "expected 412 PreconditionFailed");

        cleanup(&bucket, &["src"]).await;
    });
}

#[test]
fn test_copy_object_source_ifnonematch_good() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "src", b"source data").await;

        // Different etag → copy should succeed
        CTX.client()
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src", bucket))
            .copy_source_if_none_match("\"0000000000000000\"")
            .send()
            .await
            .unwrap();

        let resp = CTX.client()
            .get_object()
            .bucket(&bucket)
            .key("dst")
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"source data");

        cleanup(&bucket, &["src", "dst"]).await;
    });
}

#[test]
fn test_copy_object_source_ifnonematch_failed() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let etag = put_object(&bucket, "src", b"source data").await;

        // Same etag → copy should fail with 412
        let result = CTX.client()
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src", bucket))
            .copy_source_if_none_match(&etag)
            .send()
            .await;
        assert!(result.is_err(), "expected 412 PreconditionFailed");

        cleanup(&bucket, &["src"]).await;
    });
}

#[test]
fn test_copy_object_source_ifmodifiedsince_good() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "src", b"source data").await;

        // Date in the past → object was modified after → should succeed
        let past = DateTime::from_secs(0);
        CTX.client()
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src", bucket))
            .copy_source_if_modified_since(past)
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &["src", "dst"]).await;
    });
}

#[test]
fn test_copy_object_source_ifmodifiedsince_failed() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "src", b"source data").await;

        // Date far in future → not modified since → 412
        let future = DateTime::from_secs(4_102_444_800);
        let result = CTX.client()
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src", bucket))
            .copy_source_if_modified_since(future)
            .send()
            .await;
        assert!(result.is_err(), "expected 412 PreconditionFailed");

        cleanup(&bucket, &["src"]).await;
    });
}

#[test]
fn test_copy_object_source_ifunmodifiedsince_good() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "src", b"source data").await;

        // Date far in future → object was last modified before → should succeed
        let future = DateTime::from_secs(4_102_444_800);
        CTX.client()
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src", bucket))
            .copy_source_if_unmodified_since(future)
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &["src", "dst"]).await;
    });
}

#[test]
fn test_copy_object_source_ifunmodifiedsince_failed() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "src", b"source data").await;

        // Date at epoch → object was modified after → 412
        let past = DateTime::from_secs(0);
        let result = CTX.client()
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src", bucket))
            .copy_source_if_unmodified_since(past)
            .send()
            .await;
        assert!(result.is_err(), "expected 412 PreconditionFailed");

        cleanup(&bucket, &["src"]).await;
    });
}
