use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::primitives::DateTime;
use aws_sdk_s3::types::{
    BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, VersioningConfiguration,
};
use s3_tests::{
    assert_s3_err_code, cleanup_versioned_bucket, copy_source_with_version, err_status,
    raw_object_with,
    shape::{assert_shape, error_response_headers, expected_error, shape},
    unique_bucket, SendRetryingOperationAborted, CTX,
};
use serde_json::json;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Create a bucket, returning its name.
async fn setup_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    bucket
}

async fn setup_bucket_allowing_policy() -> String {
    let client = CTX.client();
    let bucket = setup_bucket().await;
    s3_tests::disable_bucket_public_access_block(client, &bucket).await;
    bucket
}

/// Put an object and return its ETag (unquoted).
async fn put_object(bucket: &str, key: &str, body: &'static [u8]) -> String {
    let client = CTX.client();
    let resp = put_object_result_retrying_operation_aborted("put conditional setup object", || {
        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(body))
    })
    .await
    .unwrap();
    resp.e_tag().unwrap().to_string()
}

async fn put_object_result_retrying_operation_aborted(
    _context: &str,
    mut build: impl FnMut() -> aws_sdk_s3::operation::put_object::builders::PutObjectFluentBuilder,
) -> Result<
    aws_sdk_s3::operation::put_object::PutObjectOutput,
    aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::put_object::PutObjectError>,
> {
    s3_tests::retrying_operation_aborted_result(|| {
        let request = build();
        async move { request.send().await }
    })
    .await
}

/// Create a single-part multipart upload and return `(upload_id, part_etag)`.
async fn prepare_single_part_multipart_upload(
    bucket: &str,
    key: &str,
    body: &'static [u8],
) -> (String, String) {
    let client = CTX.client();
    let create = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send_retrying_operation_aborted("create conditional multipart upload")
        .await
        .unwrap();
    let upload_id = create.upload_id().unwrap().to_string();
    let part = s3_tests::retrying_operation_aborted("upload conditional multipart part", || {
        client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from_static(body))
            .send()
    })
    .await;
    (upload_id, part.e_tag().unwrap().to_string())
}

async fn assert_conditional_multipart_part_preserved(
    bucket: &str,
    key: &str,
    upload_id: &str,
    expected_etag: &str,
) {
    let output = CTX
        .client()
        .list_parts()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .send_retrying_operation_aborted("list parts after rejected conditional completion")
        .await
        .unwrap();
    assert_eq!(output.parts().len(), 1);
    assert_eq!(output.parts()[0].part_number(), Some(1));
    assert_eq!(output.parts()[0].e_tag(), Some(expected_etag));
}

/// Cleanup helper: delete object + bucket.
async fn cleanup(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = s3_tests::delete_object_retrying_operation_aborted(client, bucket, key).await;
    }
    s3_tests::delete_bucket_retrying_operation_aborted(client, bucket).await;
}

fn bucket_wildcard_resource(bucket: &str) -> String {
    format!("arn:aws:s3:::{bucket}/*")
}

fn alt_policy_principal() -> serde_json::Value {
    json!({ "AWS": format!("arn:aws:iam::{}:root", CTX.alt_account_id()) })
}

fn conditional_test_timeout() -> Duration {
    let timeout_secs: u64 = std::env::var("S3_TEST_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(30);
    Duration::from_secs(timeout_secs)
}

async fn put_bucket_policy_for_alt(bucket: &str, actions: serde_json::Value) {
    CTX.client()
        .put_bucket_policy()
        .bucket(bucket)
        .policy(
            json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": alt_policy_principal(),
                    "Action": actions,
                    "Resource": bucket_wildcard_resource(bucket)
                }],
            })
            .to_string(),
        )
        .send_retrying_operation_aborted("put conditional bucket policy")
        .await
        .unwrap();
}

async fn wait_for_alt_put_object(bucket: &str, key: &str) {
    let deadline = Instant::now() + conditional_test_timeout();

    loop {
        let result = put_object_result_retrying_operation_aborted(
            "put alt policy convergence object",
            || {
                CTX.alt_client()
                    .put_object()
                    .bucket(bucket)
                    .key(key)
                    .body(ByteStream::from_static(b"policy-convergence"))
            },
        )
        .await;
        if result.is_ok() {
            return;
        }
        let last_error = result
            .err()
            .map_or_else(|| "unknown error".to_string(), |error| format!("{error:?}"));
        if Instant::now() >= deadline {
            panic!("alt PutObject policy allow did not converge: {last_error}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn wait_for_alt_get_object(bucket: &str, key: &str) {
    let deadline = Instant::now() + conditional_test_timeout();

    loop {
        let result = CTX
            .alt_client()
            .get_object()
            .bucket(bucket)
            .key(key)
            .send_retrying_operation_aborted("get conditional object")
            .await;
        if result.is_ok() {
            return;
        }
        let last_error = result
            .err()
            .map_or_else(|| "unknown error".to_string(), |error| format!("{error:?}"));
        if Instant::now() >= deadline {
            panic!("alt GetObject policy allow did not converge: {last_error}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

// ── GET If-Match ────────────────────────────────────────────────────────

#[test]
fn test_get_object_ifmatch_good() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let etag = put_object(&bucket, "obj", b"hello").await;

        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .if_match(&etag)
            .send_retrying_operation_aborted("get conditional object")
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

        let result = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .if_match("\"0000000000000000\"")
            .send_retrying_operation_aborted("get conditional object")
            .await;
        assert_eq!(err_status(&result), 412);

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_get_object_ifmatch_wildcard() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "obj", b"hello").await;

        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .if_match("*")
            .send_retrying_operation_aborted("get conditional object")
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
        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .if_none_match("\"0000000000000000\"")
            .send_retrying_operation_aborted("get conditional object")
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
        let result = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .if_none_match(&etag)
            .send_retrying_operation_aborted("get conditional object")
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
        let result = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .if_none_match("*")
            .send_retrying_operation_aborted("get conditional object")
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
        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .if_modified_since(past)
            .send_retrying_operation_aborted("get conditional object")
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

        // Use the object's own last-modified time → not modified since → 304
        // (RFC 7232 §3.3: future dates must be ignored, so we use the actual timestamp)
        let head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("head conditional object")
            .await
            .unwrap();
        let last_modified = *head.last_modified().unwrap();
        let result = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .if_modified_since(last_modified)
            .send_retrying_operation_aborted("get conditional object")
            .await;
        assert_eq!(err_status(&result), 304);

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_get_object_ifmodifiedsince_future_ignored() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "obj", b"hello").await;

        // Per RFC 7232 §3.3: future dates must be ignored → returns 200
        let future = DateTime::from_secs(4_102_444_800); // 2100-01-01
        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .if_modified_since(future)
            .send_retrying_operation_aborted("get conditional object")
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"hello");

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
        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .if_unmodified_since(future)
            .send_retrying_operation_aborted("get conditional object")
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
        let result = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .if_unmodified_since(past)
            .send_retrying_operation_aborted("get conditional object")
            .await;
        assert!(result.is_err(), "expected 412 PreconditionFailed");

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_get_object_ifmatch_ignores_ifunmodifiedsince_when_etag_matches() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let etag = put_object(&bucket, "obj", b"hello").await;

        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .if_match(&etag)
            .if_unmodified_since(DateTime::from_secs(0))
            .send_retrying_operation_aborted("get conditional object")
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"hello");

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_get_object_ifnonematch_ignores_ifmodifiedsince_when_etag_differs() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "obj", b"hello").await;

        let head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("head conditional object")
            .await
            .unwrap();
        let last_modified = *head.last_modified().unwrap();

        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .if_none_match("\"0000000000000000\"")
            .if_modified_since(last_modified)
            .send_retrying_operation_aborted("get conditional object")
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"hello");

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── HEAD If-Match / If-None-Match ───────────────────────────────────────

#[test]
fn test_head_object_ifmatch_good() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let etag = put_object(&bucket, "obj", b"hello").await;

        let resp = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key("obj")
            .if_match(&etag)
            .send_retrying_operation_aborted("head conditional object")
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

        let result = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key("obj")
            .if_match("\"0000000000000000\"")
            .send_retrying_operation_aborted("head conditional object")
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

        let resp = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key("obj")
            .if_none_match("\"0000000000000000\"")
            .send_retrying_operation_aborted("head conditional object")
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

        let result = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key("obj")
            .if_none_match(&etag)
            .send_retrying_operation_aborted("head conditional object")
            .await;
        assert!(result.is_err(), "expected 304 NotModified");

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_head_object_ifmatch_ignores_ifunmodifiedsince_when_etag_matches() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let etag = put_object(&bucket, "obj", b"hello").await;

        let resp = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key("obj")
            .if_match(&etag)
            .if_unmodified_since(DateTime::from_secs(0))
            .send_retrying_operation_aborted("head conditional object")
            .await
            .unwrap();
        assert!(resp.e_tag().is_some());

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_head_object_ifnonematch_ignores_ifmodifiedsince_when_etag_differs() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "obj", b"hello").await;

        let head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("head conditional object")
            .await
            .unwrap();
        let last_modified = *head.last_modified().unwrap();

        let resp = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key("obj")
            .if_none_match("\"0000000000000000\"")
            .if_modified_since(last_modified)
            .send_retrying_operation_aborted("head conditional object")
            .await
            .unwrap();
        assert!(resp.e_tag().is_some());

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── PUT If-None-Match: * (create-only) ──────────────────────────────────

#[test]
fn test_put_object_ifnonmatch_nonexisted_good() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;

        // Object doesn't exist → should succeed
        put_object_result_retrying_operation_aborted("put conditional new object", || {
            CTX.client()
                .put_object()
                .bucket(&bucket)
                .key("new")
                .if_none_match("*")
                .body(ByteStream::from_static(b"created"))
        })
        .await
        .unwrap();

        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("new")
            .send_retrying_operation_aborted("get conditional object")
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"created");

        cleanup(&bucket, &["new"]).await;
    });
}

#[test]
fn test_put_object_ifnonmatch_overwrite_existed_failed() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "existing", b"original").await;

        // Object already exists → should fail with 412
        let result = put_object_result_retrying_operation_aborted(
            "put conditional existing object with if-none-match",
            || {
                CTX.client()
                    .put_object()
                    .bucket(&bucket)
                    .key("existing")
                    .if_none_match("*")
                    .body(ByteStream::from_static(b"overwrite"))
            },
        )
        .await;
        assert_eq!(err_status(&result), 412);

        // Verify original content unchanged
        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("existing")
            .send_retrying_operation_aborted("get conditional object")
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"original");

        cleanup(&bucket, &["existing"]).await;
    });
}

#[test]
fn test_put_object_ifnonmatch_requires_put_object_only() {
    s3_tests::run(async {
        let bucket = setup_bucket_allowing_policy().await;
        put_object(&bucket, "existing", b"original").await;
        put_bucket_policy_for_alt(&bucket, json!("s3:PutObject")).await;

        let allowed_key = "if-none-match-put-only-new";
        let deadline = Instant::now() + conditional_test_timeout();
        loop {
            let result = put_object_result_retrying_operation_aborted(
                "put alt conditional allowed object",
                || {
                    CTX.alt_client()
                        .put_object()
                        .bucket(&bucket)
                        .key(allowed_key)
                        .if_none_match("*")
                        .body(ByteStream::from_static(b"created"))
                },
            )
            .await;
            if result.is_ok() {
                break;
            }
            let last_error = result
                .err()
                .map_or_else(|| "unknown error".to_string(), |error| format!("{error:?}"));
            if Instant::now() >= deadline {
                panic!("alt conditional PutObject policy allow did not converge: {last_error}");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        let denied_read = CTX
            .alt_client()
            .get_object()
            .bucket(&bucket)
            .key("existing")
            .send_retrying_operation_aborted("get conditional object")
            .await;
        assert_eq!(err_status(&denied_read), 403);

        let existing_write = put_object_result_retrying_operation_aborted(
            "put alt conditional existing object with if-none-match",
            || {
                CTX.alt_client()
                    .put_object()
                    .bucket(&bucket)
                    .key("existing")
                    .if_none_match("*")
                    .body(ByteStream::from_static(b"overwrite"))
            },
        )
        .await;
        assert_eq!(err_status(&existing_write), 412);

        let owner_read = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("existing")
            .send_retrying_operation_aborted("get conditional object")
            .await
            .unwrap();
        let data = owner_read.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"original");

        cleanup(&bucket, &["existing", allowed_key]).await;
    });
}

// ── PUT If-Match (conditional overwrite) ────────────────────────────────

#[test]
fn test_put_object_ifmatch_good() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let etag = put_object(&bucket, "obj", b"v1").await;

        // Matching etag → should succeed
        put_object_result_retrying_operation_aborted(
            "put conditional object with if-match",
            || {
                CTX.client()
                    .put_object()
                    .bucket(&bucket)
                    .key("obj")
                    .if_match(&etag)
                    .body(ByteStream::from_static(b"v2"))
            },
        )
        .await
        .unwrap();

        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("get conditional object")
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
        let result = put_object_result_retrying_operation_aborted(
            "put conditional object with failing if-match",
            || {
                CTX.client()
                    .put_object()
                    .bucket(&bucket)
                    .key("obj")
                    .if_match("\"0000000000000000\"")
                    .body(ByteStream::from_static(b"v2"))
            },
        )
        .await;
        assert_eq!(err_status(&result), 412);

        // Verify original content unchanged
        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("get conditional object")
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"v1");

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_put_object_ifmatch_quoted_star_returns_not_implemented() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "obj", b"v1").await;

        let result = put_object_result_retrying_operation_aborted(
            "put conditional object with quoted-star if-match",
            || {
                CTX.client()
                    .put_object()
                    .bucket(&bucket)
                    .key("obj")
                    .if_match("\"*\"")
                    .body(ByteStream::from_static(b"v2"))
            },
        )
        .await;
        assert_eq!(err_status(&result), 501);

        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("get conditional object")
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"v1");

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_put_object_ifmatch_percent_encoded_star_is_literal_etag() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "obj", b"v1").await;

        let result = put_object_result_retrying_operation_aborted(
            "put conditional object with percent-encoded-star if-match",
            || {
                CTX.client()
                    .put_object()
                    .bucket(&bucket)
                    .key("obj")
                    .if_match("%2A")
                    .body(ByteStream::from_static(b"v2"))
            },
        )
        .await;
        assert_eq!(err_status(&result), 412);

        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("get conditional object")
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"v1");

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_put_object_ifmatch_requires_put_object_and_get_object() {
    s3_tests::run(async {
        let bucket = setup_bucket_allowing_policy().await;
        let etag = put_object(&bucket, "existing", b"original").await;
        put_bucket_policy_for_alt(&bucket, json!("s3:PutObject")).await;

        wait_for_alt_put_object(&bucket, "if-match-put-only-control").await;

        let missing_get = put_object_result_retrying_operation_aborted(
            "put alt conditional object without get permission",
            || {
                CTX.alt_client()
                    .put_object()
                    .bucket(&bucket)
                    .key("existing")
                    .if_match(&etag)
                    .body(ByteStream::from_static(b"without-get"))
            },
        )
        .await;
        assert_eq!(err_status(&missing_get), 403);

        put_bucket_policy_for_alt(&bucket, json!(["s3:PutObject", "s3:GetObject"])).await;
        wait_for_alt_get_object(&bucket, "existing").await;

        put_object_result_retrying_operation_aborted(
            "put alt conditional object with get permission",
            || {
                CTX.alt_client()
                    .put_object()
                    .bucket(&bucket)
                    .key("existing")
                    .if_match(&etag)
                    .body(ByteStream::from_static(b"with-get"))
            },
        )
        .await
        .unwrap();

        let overwritten = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("existing")
            .send_retrying_operation_aborted("get conditional object")
            .await
            .unwrap();
        let data = overwritten.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"with-get");

        cleanup(&bucket, &["existing", "if-match-put-only-control"]).await;
    });
}

#[test]
fn test_put_object_ifmatch_nonexisted_failed() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;

        // Object doesn't exist → If-Match fails with 404 NoSuchKey
        let result = put_object_result_retrying_operation_aborted(
            "put conditional missing object with if-match",
            || {
                CTX.client()
                    .put_object()
                    .bucket(&bucket)
                    .key("nonexistent")
                    .if_match("\"0000000000000000\"")
                    .body(ByteStream::from_static(b"data"))
            },
        )
        .await;
        assert_eq!(err_status(&result), 404);

        cleanup(&bucket, &[]).await;
    });
}

// ── CompleteMultipartUpload write conditions ─────────────────────────────

#[test]
fn test_complete_multipart_ifnonmatch_nonexisted_good() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let (upload_id, part_etag) =
            prepare_single_part_multipart_upload(&bucket, "obj", b"created").await;

        CTX.client()
            .complete_multipart_upload()
            .bucket(&bucket)
            .key("obj")
            .upload_id(&upload_id)
            .if_none_match("*")
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(&part_etag)
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send_retrying_operation_aborted("complete conditional multipart upload")
            .await
            .unwrap();

        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("get conditional object")
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"created");

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_complete_multipart_ifnonmatch_overwrite_existed_failed() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let original_etag = put_object(&bucket, "obj", b"original").await;
        let (upload_id, part_etag) =
            prepare_single_part_multipart_upload(&bucket, "obj", b"overwrite").await;

        let result = CTX
            .client()
            .complete_multipart_upload()
            .bucket(&bucket)
            .key("obj")
            .upload_id(&upload_id)
            .if_none_match("*")
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(&part_etag)
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send_retrying_operation_aborted("complete conditional multipart upload")
            .await;
        assert_eq!(err_status(&result), 412);
        assert_conditional_multipart_part_preserved(&bucket, "obj", &upload_id, &part_etag).await;

        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("get conditional object")
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"original");

        CTX.client()
            .complete_multipart_upload()
            .bucket(&bucket)
            .key("obj")
            .upload_id(&upload_id)
            .if_match(&original_etag)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(&part_etag)
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send_retrying_operation_aborted("retry conditional multipart upload")
            .await
            .unwrap();
        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("get corrected conditional object")
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"overwrite");

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_complete_multipart_ifmatch_good() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let etag = put_object(&bucket, "obj", b"v1").await;
        let (upload_id, part_etag) =
            prepare_single_part_multipart_upload(&bucket, "obj", b"v2").await;

        CTX.client()
            .complete_multipart_upload()
            .bucket(&bucket)
            .key("obj")
            .upload_id(&upload_id)
            .if_match(&etag)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(&part_etag)
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send_retrying_operation_aborted("complete conditional multipart upload")
            .await
            .unwrap();

        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("get conditional object")
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"v2");

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_complete_multipart_ifmatch_failed() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let original_etag = put_object(&bucket, "obj", b"v1").await;
        let (upload_id, part_etag) =
            prepare_single_part_multipart_upload(&bucket, "obj", b"v2").await;

        let result = CTX
            .client()
            .complete_multipart_upload()
            .bucket(&bucket)
            .key("obj")
            .upload_id(&upload_id)
            .if_match("\"0000000000000000\"")
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(&part_etag)
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send_retrying_operation_aborted("complete conditional multipart upload")
            .await;
        assert_eq!(err_status(&result), 412);
        assert_conditional_multipart_part_preserved(&bucket, "obj", &upload_id, &part_etag).await;

        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("get conditional object")
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"v1");

        CTX.client()
            .complete_multipart_upload()
            .bucket(&bucket)
            .key("obj")
            .upload_id(&upload_id)
            .if_match(&original_etag)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(&part_etag)
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send_retrying_operation_aborted("retry conditional multipart upload")
            .await
            .unwrap();
        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("get corrected conditional object")
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"v2");

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_complete_multipart_ifmatch_nonexisted_failed() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let (upload_id, part_etag) =
            prepare_single_part_multipart_upload(&bucket, "obj", b"created").await;

        let result = CTX
            .client()
            .complete_multipart_upload()
            .bucket(&bucket)
            .key("obj")
            .upload_id(&upload_id)
            .if_match("\"0000000000000000\"")
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(&part_etag)
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send_retrying_operation_aborted("complete conditional multipart upload")
            .await;
        assert_eq!(err_status(&result), 404);
        assert_conditional_multipart_part_preserved(&bucket, "obj", &upload_id, &part_etag).await;
        let get = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("get object after rejected conditional completion")
            .await;
        assert_eq!(err_status(&get), 404);

        CTX.client()
            .complete_multipart_upload()
            .bucket(&bucket)
            .key("obj")
            .upload_id(&upload_id)
            .if_none_match("*")
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(&part_etag)
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send_retrying_operation_aborted("retry conditional multipart upload")
            .await
            .unwrap();
        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("get corrected conditional object")
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"created");

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_complete_multipart_ifnonmatch_current_object_in_versioned_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        s3_tests::enable_bucket_versioning(client, &bucket).await;

        let first = put_object_result_retrying_operation_aborted(
            "put first versioned conditional object",
            || {
                client
                    .put_object()
                    .bucket(&bucket)
                    .key("obj")
                    .body(ByteStream::from_static(b"v1"))
            },
        )
        .await
        .unwrap();
        assert!(first.version_id().is_some());

        let second = put_object_result_retrying_operation_aborted(
            "put second versioned conditional object",
            || {
                client
                    .put_object()
                    .bucket(&bucket)
                    .key("obj")
                    .body(ByteStream::from_static(b"v2"))
            },
        )
        .await
        .unwrap();
        assert!(second.version_id().is_some());

        let (upload_id, part_etag) =
            prepare_single_part_multipart_upload(&bucket, "obj", b"v3").await;
        let result = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key("obj")
            .upload_id(&upload_id)
            .if_none_match("*")
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(&part_etag)
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send_retrying_operation_aborted("complete conditional multipart upload")
            .await;
        assert_eq!(err_status(&result), 412);
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("obj")
            .upload_id(&upload_id)
            .send_retrying_operation_aborted("abort conditional multipart upload")
            .await
            .unwrap();

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_complete_multipart_ifmatch_current_object_in_versioned_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        s3_tests::enable_bucket_versioning(client, &bucket).await;

        let first = put_object_result_retrying_operation_aborted(
            "put first versioned conditional object",
            || {
                client
                    .put_object()
                    .bucket(&bucket)
                    .key("obj")
                    .body(ByteStream::from_static(b"v1"))
            },
        )
        .await
        .unwrap();
        let first_etag = first.e_tag().unwrap().to_string();

        let second = put_object_result_retrying_operation_aborted(
            "put second versioned conditional object",
            || {
                client
                    .put_object()
                    .bucket(&bucket)
                    .key("obj")
                    .body(ByteStream::from_static(b"v2"))
            },
        )
        .await
        .unwrap();
        let second_etag = second.e_tag().unwrap().to_string();

        let (stale_upload_id, stale_part_etag) =
            prepare_single_part_multipart_upload(&bucket, "obj", b"stale").await;
        let stale = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key("obj")
            .upload_id(&stale_upload_id)
            .if_match(&first_etag)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(&stale_part_etag)
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send_retrying_operation_aborted("complete conditional multipart upload")
            .await;
        assert_eq!(err_status(&stale), 412);
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("obj")
            .upload_id(&stale_upload_id)
            .send_retrying_operation_aborted("abort conditional multipart upload")
            .await
            .unwrap();

        let (upload_id, part_etag) =
            prepare_single_part_multipart_upload(&bucket, "obj", b"v3").await;
        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key("obj")
            .upload_id(&upload_id)
            .if_match(&second_etag)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(&part_etag)
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send_retrying_operation_aborted("complete conditional multipart upload")
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("get conditional object")
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"v3");

        cleanup_versioned_bucket(client, &bucket).await;
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
            .send_retrying_operation_aborted("delete conditional object")
            .await
            .unwrap();

        // Verify deleted
        let result = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("get conditional object")
            .await;
        assert_eq!(err_status(&result), 404);

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_delete_object_ifmatch_failed() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "obj", b"hello").await;

        // Wrong etag → delete should fail with 412
        let result = CTX
            .client()
            .delete_object()
            .bucket(&bucket)
            .key("obj")
            .if_match("\"0000000000000000\"")
            .send_retrying_operation_aborted("delete conditional object")
            .await;
        assert!(result.is_err(), "expected 412 PreconditionFailed");

        // Verify still exists
        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("get conditional object")
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"hello");

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_delete_object_ifmatch_nonexistent_returns_no_such_key() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;

        let result = CTX
            .client()
            .delete_object()
            .bucket(&bucket)
            .key("nonexistent")
            .if_match("\"0000000000000000\"")
            .send_retrying_operation_aborted("delete conditional missing object")
            .await;
        assert_eq!(err_status(&result), 404);
        assert_s3_err_code(&result, "NoSuchKey");

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_delete_object_ifmatch_versioned_nonexistent_returns_no_such_key() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        s3_tests::enable_bucket_versioning(client, &bucket).await;

        let result = client
            .delete_object()
            .bucket(&bucket)
            .key("nonexistent")
            .if_match("\"0000000000000000\"")
            .send_retrying_operation_aborted("delete conditional missing versioned object")
            .await;
        assert_eq!(err_status(&result), 404);
        assert_s3_err_code(&result, "NoSuchKey");

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_delete_object_ifmatch_current_delete_marker_returns_no_such_key() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        s3_tests::enable_bucket_versioning(client, &bucket).await;

        put_object(&bucket, "obj", b"hello").await;
        client
            .delete_object()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("create current delete marker")
            .await
            .unwrap();

        let result = client
            .delete_object()
            .bucket(&bucket)
            .key("obj")
            .if_match("\"0000000000000000\"")
            .send_retrying_operation_aborted("delete conditional current delete marker")
            .await;
        assert_eq!(err_status(&result), 404);
        assert_s3_err_code(&result, "NoSuchKey");

        cleanup_versioned_bucket(client, &bucket).await;
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
            .send_retrying_operation_aborted("copy conditional object")
            .await
            .unwrap();

        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("dst")
            .send_retrying_operation_aborted("get conditional object")
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

        let result = CTX
            .client()
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src", bucket))
            .copy_source_if_match("\"0000000000000000\"")
            .send_retrying_operation_aborted("copy conditional object")
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
            .send_retrying_operation_aborted("copy conditional object")
            .await
            .unwrap();

        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("dst")
            .send_retrying_operation_aborted("get conditional object")
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
        let result = CTX
            .client()
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src", bucket))
            .copy_source_if_none_match(&etag)
            .send_retrying_operation_aborted("copy conditional object")
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
            .send_retrying_operation_aborted("copy conditional object")
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

        // Use the source object's own last-modified time → not modified since → 412
        // (RFC 7232 §3.3: future dates must be ignored, so we use the actual timestamp)
        let head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key("src")
            .send_retrying_operation_aborted("head conditional object")
            .await
            .unwrap();
        let last_modified = *head.last_modified().unwrap();
        let result = CTX
            .client()
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src", bucket))
            .copy_source_if_modified_since(last_modified)
            .send_retrying_operation_aborted("copy conditional object")
            .await;
        assert!(result.is_err(), "expected 412 PreconditionFailed");

        cleanup(&bucket, &["src"]).await;
    });
}

#[test]
fn test_copy_object_source_ifmodifiedsince_future_ignored() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "src", b"source data").await;

        // Per RFC 7232 §3.3: future dates must be ignored → copy succeeds
        let future = DateTime::from_secs(4_102_444_800); // 2100-01-01
        CTX.client()
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src", bucket))
            .copy_source_if_modified_since(future)
            .send_retrying_operation_aborted("copy conditional object")
            .await
            .unwrap();

        cleanup(&bucket, &["src", "dst"]).await;
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
            .send_retrying_operation_aborted("copy conditional object")
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
        let result = CTX
            .client()
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src", bucket))
            .copy_source_if_unmodified_since(past)
            .send_retrying_operation_aborted("copy conditional object")
            .await;
        assert!(result.is_err(), "expected 412 PreconditionFailed");

        cleanup(&bucket, &["src"]).await;
    });
}

#[test]
fn test_copy_source_combined_etag_date_precedence() {
    s3_tests::run(async {
        #[derive(Clone)]
        struct SourceConditionCase {
            name: &'static str,
            if_match: Option<String>,
            if_none_match: Option<String>,
            if_modified_since: Option<DateTime>,
            if_unmodified_since: Option<DateTime>,
            succeeds: bool,
        }

        let client = CTX.client();
        let bucket = setup_bucket().await;
        let source_etag = put_object(&bucket, "src", b"source data").await;
        let source_head = client
            .head_object()
            .bucket(&bucket)
            .key("src")
            .send_retrying_operation_aborted("head combined-condition copy source")
            .await
            .unwrap();
        let source_last_modified = *source_head.last_modified().unwrap();
        let past = DateTime::from_secs(0);
        let future = DateTime::from_secs(4_102_444_800);
        let wrong_etag = "\"0000000000000000\"".to_string();

        // AWS evaluates the ETag member of each documented pair when it is
        // present. The paired date neither rescues a failed ETag condition nor
        // defeats a successful one.
        let cases = [
            SourceConditionCase {
                name: "matching If-Match and passing If-Unmodified-Since",
                if_match: Some(source_etag.clone()),
                if_none_match: None,
                if_modified_since: None,
                if_unmodified_since: Some(future),
                succeeds: true,
            },
            SourceConditionCase {
                name: "matching If-Match and failing If-Unmodified-Since",
                if_match: Some(source_etag.clone()),
                if_none_match: None,
                if_modified_since: None,
                if_unmodified_since: Some(past),
                succeeds: true,
            },
            SourceConditionCase {
                name: "nonmatching If-Match and passing If-Unmodified-Since",
                if_match: Some(wrong_etag.clone()),
                if_none_match: None,
                if_modified_since: None,
                if_unmodified_since: Some(future),
                succeeds: false,
            },
            SourceConditionCase {
                name: "nonmatching If-Match and failing If-Unmodified-Since",
                if_match: Some(wrong_etag.clone()),
                if_none_match: None,
                if_modified_since: None,
                if_unmodified_since: Some(past),
                succeeds: false,
            },
            SourceConditionCase {
                name: "nonmatching If-None-Match and passing If-Modified-Since",
                if_match: None,
                if_none_match: Some(wrong_etag.clone()),
                if_modified_since: Some(past),
                if_unmodified_since: None,
                succeeds: true,
            },
            SourceConditionCase {
                name: "nonmatching If-None-Match and failing If-Modified-Since",
                if_match: None,
                if_none_match: Some(wrong_etag),
                if_modified_since: Some(source_last_modified),
                if_unmodified_since: None,
                succeeds: true,
            },
            SourceConditionCase {
                name: "matching If-None-Match and passing If-Modified-Since",
                if_match: None,
                if_none_match: Some(source_etag.clone()),
                if_modified_since: Some(past),
                if_unmodified_since: None,
                succeeds: false,
            },
            SourceConditionCase {
                name: "matching If-None-Match and failing If-Modified-Since",
                if_match: None,
                if_none_match: Some(source_etag.clone()),
                if_modified_since: Some(source_last_modified),
                if_unmodified_since: None,
                succeeds: false,
            },
        ];

        for (index, case) in cases.iter().enumerate() {
            let destination = format!("copy-dst-{index}");
            let request = client
                .copy_object()
                .bucket(&bucket)
                .key(&destination)
                .copy_source(format!("{bucket}/src"))
                .set_copy_source_if_match(case.if_match.clone())
                .set_copy_source_if_none_match(case.if_none_match.clone())
                .set_copy_source_if_modified_since(case.if_modified_since)
                .set_copy_source_if_unmodified_since(case.if_unmodified_since);
            let result = request.send().await;

            if case.succeeds {
                result.unwrap_or_else(|error| {
                    panic!("CopyObject {} unexpectedly failed: {error:?}", case.name)
                });
                let copied = client
                    .get_object()
                    .bucket(&bucket)
                    .key(&destination)
                    .send_retrying_operation_aborted("get combined-condition copied object")
                    .await
                    .unwrap();
                assert_eq!(copied.e_tag(), Some(source_etag.as_str()), "{}", case.name);
                let copied_data = copied.body.collect().await.unwrap().into_bytes();
                assert_eq!(&copied_data[..], b"source data", "{}", case.name);
            } else {
                assert_eq!(err_status(&result), 412, "CopyObject {}", case.name);
                assert_s3_err_code(&result, "PreconditionFailed");
                let head = client
                    .head_object()
                    .bucket(&bucket)
                    .key(&destination)
                    .send()
                    .await;
                assert_eq!(err_status(&head), 404, "CopyObject {}", case.name);
            }

            let upload_key = format!("upload-part-copy-dst-{index}");
            let upload = client
                .create_multipart_upload()
                .bucket(&bucket)
                .key(&upload_key)
                .send_retrying_operation_aborted("create combined-condition copy upload")
                .await
                .unwrap();
            let upload_id = upload.upload_id().unwrap();
            let upload_result = client
                .upload_part_copy()
                .bucket(&bucket)
                .key(&upload_key)
                .upload_id(upload_id)
                .part_number(1)
                .copy_source(format!("{bucket}/src"))
                .set_copy_source_if_match(case.if_match.clone())
                .set_copy_source_if_none_match(case.if_none_match.clone())
                .set_copy_source_if_modified_since(case.if_modified_since)
                .set_copy_source_if_unmodified_since(case.if_unmodified_since)
                .send()
                .await;

            if case.succeeds {
                let output = upload_result.unwrap_or_else(|error| {
                    panic!(
                        "UploadPartCopy {} unexpectedly failed: {error:?}",
                        case.name
                    )
                });
                assert_eq!(
                    output.copy_part_result().and_then(|part| part.e_tag()),
                    Some(source_etag.as_str()),
                    "{}",
                    case.name
                );
            } else {
                assert_eq!(
                    err_status(&upload_result),
                    412,
                    "UploadPartCopy {}",
                    case.name
                );
                assert_s3_err_code(&upload_result, "PreconditionFailed");
            }

            let parts = client
                .list_parts()
                .bucket(&bucket)
                .key(&upload_key)
                .upload_id(upload_id)
                .send_retrying_operation_aborted("list combined-condition copied parts")
                .await
                .unwrap();
            if case.succeeds {
                assert_eq!(parts.parts().len(), 1, "{}", case.name);
                assert_eq!(parts.parts()[0].e_tag(), Some(source_etag.as_str()));
            } else {
                assert!(parts.parts().is_empty(), "{}", case.name);
            }

            client
                .abort_multipart_upload()
                .bucket(&bucket)
                .key(&upload_key)
                .upload_id(upload_id)
                .send_retrying_operation_aborted("abort combined-condition copy upload")
                .await
                .unwrap();
        }

        let source = client
            .get_object()
            .bucket(&bucket)
            .key("src")
            .send_retrying_operation_aborted("get source after combined-condition copies")
            .await
            .unwrap();
        assert_eq!(source.e_tag(), Some(source_etag.as_str()));
        let source_data = source.body.collect().await.unwrap().into_bytes();
        assert_eq!(&source_data[..], b"source data");

        let copied_keys = (0..cases.len())
            .filter(|index| cases[*index].succeeds)
            .map(|index| format!("copy-dst-{index}"))
            .collect::<Vec<_>>();
        for key in copied_keys {
            s3_tests::delete_object_retrying_operation_aborted(client, &bucket, &key)
                .await
                .unwrap();
        }
        cleanup(&bucket, &["src"]).await;
    });
}

#[test]
fn test_copy_source_conditions_resolve_selected_source_version_first() {
    s3_tests::run(async {
        #[derive(Clone)]
        enum SourceCondition {
            Match(String),
            NoneMatch(String),
            ModifiedSince(DateTime),
            UnmodifiedSince(DateTime),
        }

        struct SourceStateCase {
            name: &'static str,
            copy_source: String,
            condition: SourceCondition,
            expected_error: Option<(u16, &'static str)>,
            expected_source_version: Option<String>,
        }

        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        s3_tests::enable_bucket_versioning(client, &bucket).await;

        let first = put_object_result_retrying_operation_aborted(
            "put first conditional copy source version",
            || {
                client
                    .put_object()
                    .bucket(&bucket)
                    .key("src")
                    .body(ByteStream::from_static(b"first source"))
            },
        )
        .await
        .unwrap();
        let first_etag = first.e_tag().unwrap().to_string();
        let first_version = first.version_id().unwrap().to_string();

        let second = put_object_result_retrying_operation_aborted(
            "put second conditional copy source version",
            || {
                client
                    .put_object()
                    .bucket(&bucket)
                    .key("src")
                    .body(ByteStream::from_static(b"second source"))
            },
        )
        .await
        .unwrap();
        let second_etag = second.e_tag().unwrap().to_string();
        let second_version = second.version_id().unwrap().to_string();

        let first_head = client
            .head_object()
            .bucket(&bucket)
            .key("src")
            .version_id(&first_version)
            .send_retrying_operation_aborted("head first conditional copy source version")
            .await
            .unwrap();
        let first_last_modified = *first_head.last_modified().unwrap();

        let marker = client
            .delete_object()
            .bucket(&bucket)
            .key("src")
            .send_retrying_operation_aborted("create conditional copy source delete marker")
            .await
            .unwrap();
        assert_eq!(marker.delete_marker(), Some(true));
        let marker_version = marker.version_id().unwrap().to_string();

        let wrong_etag = "\"0000000000000000\"".to_string();
        let past = DateTime::from_secs(0);
        let future = DateTime::from_secs(4_102_444_800);
        let missing_source = format!("{bucket}/missing");
        let current_marker_source = format!("{bucket}/src");
        let first_source = copy_source_with_version(&bucket, "src", &first_version);
        let marker_source = copy_source_with_version(&bucket, "src", &marker_version);

        let cases = vec![
            SourceStateCase {
                name: "missing source with If-Match",
                copy_source: missing_source.clone(),
                condition: SourceCondition::Match(wrong_etag.clone()),
                expected_error: Some((404, "NoSuchKey")),
                expected_source_version: None,
            },
            SourceStateCase {
                name: "missing source with If-None-Match",
                copy_source: missing_source.clone(),
                condition: SourceCondition::NoneMatch(wrong_etag.clone()),
                expected_error: Some((404, "NoSuchKey")),
                expected_source_version: None,
            },
            SourceStateCase {
                name: "missing source with If-Modified-Since",
                copy_source: missing_source.clone(),
                condition: SourceCondition::ModifiedSince(past),
                expected_error: Some((404, "NoSuchKey")),
                expected_source_version: None,
            },
            SourceStateCase {
                name: "missing source with If-Unmodified-Since",
                copy_source: missing_source,
                condition: SourceCondition::UnmodifiedSince(future),
                expected_error: Some((404, "NoSuchKey")),
                expected_source_version: None,
            },
            SourceStateCase {
                name: "current delete marker with If-Match",
                copy_source: current_marker_source.clone(),
                condition: SourceCondition::Match(first_etag.clone()),
                expected_error: Some((404, "NoSuchKey")),
                expected_source_version: None,
            },
            SourceStateCase {
                name: "current delete marker with If-None-Match",
                copy_source: current_marker_source.clone(),
                condition: SourceCondition::NoneMatch(wrong_etag.clone()),
                expected_error: Some((404, "NoSuchKey")),
                expected_source_version: None,
            },
            SourceStateCase {
                name: "current delete marker with If-Modified-Since",
                copy_source: current_marker_source.clone(),
                condition: SourceCondition::ModifiedSince(past),
                expected_error: Some((404, "NoSuchKey")),
                expected_source_version: None,
            },
            SourceStateCase {
                name: "current delete marker with If-Unmodified-Since",
                copy_source: current_marker_source,
                condition: SourceCondition::UnmodifiedSince(future),
                expected_error: Some((404, "NoSuchKey")),
                expected_source_version: None,
            },
            SourceStateCase {
                name: "explicit live version with matching If-Match",
                copy_source: first_source.clone(),
                condition: SourceCondition::Match(first_etag.clone()),
                expected_error: None,
                expected_source_version: Some(first_version.clone()),
            },
            SourceStateCase {
                name: "explicit live version with current-version If-Match",
                copy_source: first_source.clone(),
                condition: SourceCondition::Match(second_etag.clone()),
                expected_error: Some((412, "PreconditionFailed")),
                expected_source_version: None,
            },
            SourceStateCase {
                name: "explicit live version with nonmatching If-None-Match",
                copy_source: first_source.clone(),
                condition: SourceCondition::NoneMatch(second_etag.clone()),
                expected_error: None,
                expected_source_version: Some(first_version.clone()),
            },
            SourceStateCase {
                name: "explicit live version with matching If-None-Match",
                copy_source: first_source.clone(),
                condition: SourceCondition::NoneMatch(first_etag.clone()),
                expected_error: Some((412, "PreconditionFailed")),
                expected_source_version: None,
            },
            SourceStateCase {
                name: "explicit live version modified after epoch",
                copy_source: first_source.clone(),
                condition: SourceCondition::ModifiedSince(past),
                expected_error: None,
                expected_source_version: Some(first_version.clone()),
            },
            SourceStateCase {
                name: "explicit live version not modified after its timestamp",
                copy_source: first_source.clone(),
                condition: SourceCondition::ModifiedSince(first_last_modified),
                expected_error: Some((412, "PreconditionFailed")),
                expected_source_version: None,
            },
            SourceStateCase {
                name: "explicit live version unmodified before future",
                copy_source: first_source.clone(),
                condition: SourceCondition::UnmodifiedSince(future),
                expected_error: None,
                expected_source_version: Some(first_version.clone()),
            },
            SourceStateCase {
                name: "explicit live version not unmodified since epoch",
                copy_source: first_source,
                condition: SourceCondition::UnmodifiedSince(past),
                expected_error: Some((412, "PreconditionFailed")),
                expected_source_version: None,
            },
            SourceStateCase {
                name: "explicit delete marker with If-Match",
                copy_source: marker_source.clone(),
                condition: SourceCondition::Match(first_etag.clone()),
                expected_error: Some((400, "InvalidRequest")),
                expected_source_version: None,
            },
            SourceStateCase {
                name: "explicit delete marker with If-None-Match",
                copy_source: marker_source.clone(),
                condition: SourceCondition::NoneMatch(wrong_etag),
                expected_error: Some((400, "InvalidRequest")),
                expected_source_version: None,
            },
            SourceStateCase {
                name: "explicit delete marker with If-Modified-Since",
                copy_source: marker_source.clone(),
                condition: SourceCondition::ModifiedSince(past),
                expected_error: Some((400, "InvalidRequest")),
                expected_source_version: None,
            },
            SourceStateCase {
                name: "explicit delete marker with If-Unmodified-Since",
                copy_source: marker_source,
                condition: SourceCondition::UnmodifiedSince(future),
                expected_error: Some((400, "InvalidRequest")),
                expected_source_version: None,
            },
        ];

        for (index, case) in cases.iter().enumerate() {
            let destination = format!("copy-source-state-dst-{index}");
            let request = client
                .copy_object()
                .bucket(&bucket)
                .key(&destination)
                .copy_source(&case.copy_source);
            let request = match &case.condition {
                SourceCondition::Match(value) => request.copy_source_if_match(value),
                SourceCondition::NoneMatch(value) => request.copy_source_if_none_match(value),
                SourceCondition::ModifiedSince(value) => {
                    request.copy_source_if_modified_since(*value)
                }
                SourceCondition::UnmodifiedSince(value) => {
                    request.copy_source_if_unmodified_since(*value)
                }
            };
            let result = request.send().await;

            if let Some((status, code)) = case.expected_error {
                assert_eq!(err_status(&result), status, "CopyObject {}", case.name);
                assert_s3_err_code(&result, code);
                let head = client
                    .head_object()
                    .bucket(&bucket)
                    .key(&destination)
                    .send()
                    .await;
                assert_eq!(err_status(&head), 404, "CopyObject {}", case.name);
            } else {
                let output = result.unwrap_or_else(|error| {
                    panic!("CopyObject {} unexpectedly failed: {error:?}", case.name)
                });
                assert_eq!(
                    output.copy_source_version_id(),
                    case.expected_source_version.as_deref(),
                    "{}",
                    case.name
                );
                let copied = client
                    .get_object()
                    .bucket(&bucket)
                    .key(&destination)
                    .send_retrying_operation_aborted("get source-state conditional copy")
                    .await
                    .unwrap();
                assert_eq!(copied.e_tag(), Some(first_etag.as_str()), "{}", case.name);
                let copied_data = copied.body.collect().await.unwrap().into_bytes();
                assert_eq!(&copied_data[..], b"first source", "{}", case.name);
            }

            let upload_key = format!("upload-part-copy-source-state-dst-{index}");
            let upload = client
                .create_multipart_upload()
                .bucket(&bucket)
                .key(&upload_key)
                .send_retrying_operation_aborted("create source-state conditional copy upload")
                .await
                .unwrap();
            let upload_id = upload.upload_id().unwrap();
            let request = client
                .upload_part_copy()
                .bucket(&bucket)
                .key(&upload_key)
                .upload_id(upload_id)
                .part_number(1)
                .copy_source(&case.copy_source);
            let request = match &case.condition {
                SourceCondition::Match(value) => request.copy_source_if_match(value),
                SourceCondition::NoneMatch(value) => request.copy_source_if_none_match(value),
                SourceCondition::ModifiedSince(value) => {
                    request.copy_source_if_modified_since(*value)
                }
                SourceCondition::UnmodifiedSince(value) => {
                    request.copy_source_if_unmodified_since(*value)
                }
            };
            let upload_result = request.send().await;

            if let Some((status, code)) = case.expected_error {
                assert_eq!(
                    err_status(&upload_result),
                    status,
                    "UploadPartCopy {}",
                    case.name
                );
                assert_s3_err_code(&upload_result, code);
            } else {
                let output = upload_result.unwrap_or_else(|error| {
                    panic!(
                        "UploadPartCopy {} unexpectedly failed: {error:?}",
                        case.name
                    )
                });
                assert_eq!(
                    output.copy_source_version_id(),
                    case.expected_source_version.as_deref(),
                    "{}",
                    case.name
                );
                assert_eq!(
                    output.copy_part_result().and_then(|part| part.e_tag()),
                    Some(first_etag.as_str()),
                    "{}",
                    case.name
                );
            }

            let parts = client
                .list_parts()
                .bucket(&bucket)
                .key(&upload_key)
                .upload_id(upload_id)
                .send_retrying_operation_aborted("list source-state conditional copied parts")
                .await
                .unwrap();
            if case.expected_error.is_some() {
                assert!(parts.parts().is_empty(), "{}", case.name);
            } else {
                assert_eq!(parts.parts().len(), 1, "{}", case.name);
                assert_eq!(parts.parts()[0].e_tag(), Some(first_etag.as_str()));
            }
            client
                .abort_multipart_upload()
                .bucket(&bucket)
                .key(&upload_key)
                .upload_id(upload_id)
                .send_retrying_operation_aborted("abort source-state conditional copy upload")
                .await
                .unwrap();
        }

        let source_versions = client
            .list_object_versions()
            .bucket(&bucket)
            .prefix("src")
            .send_retrying_operation_aborted("list source after source-state conditional copies")
            .await
            .unwrap();
        assert_eq!(source_versions.versions().len(), 2);
        assert_eq!(source_versions.delete_markers().len(), 1);
        let retained_first = source_versions
            .versions()
            .iter()
            .find(|version| version.version_id() == Some(first_version.as_str()))
            .expect("first source version must remain listed");
        assert_eq!(retained_first.e_tag(), Some(first_etag.as_str()));
        assert_eq!(retained_first.is_latest(), Some(false));
        let retained_second = source_versions
            .versions()
            .iter()
            .find(|version| version.version_id() == Some(second_version.as_str()))
            .expect("second source version must remain listed");
        assert_eq!(retained_second.e_tag(), Some(second_etag.as_str()));
        assert_eq!(retained_second.is_latest(), Some(false));
        assert_eq!(
            source_versions.delete_markers()[0].version_id(),
            Some(marker_version.as_str())
        );
        assert_eq!(source_versions.delete_markers()[0].is_latest(), Some(true));

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_copy_source_version_header_follows_source_bucket_versioning() {
    s3_tests::run(async {
        async fn assert_source_version_headers(
            copy_source: String,
            destination_bucket: &str,
            label: &str,
            expected_source_version: Option<&str>,
            expected_etag: &str,
            expected_body: &[u8],
        ) {
            let client = CTX.client();
            let copy_key = format!("copy-source-version-{label}");
            let copy = client
                .copy_object()
                .bucket(destination_bucket)
                .key(&copy_key)
                .copy_source(&copy_source)
                .send()
                .await
                .unwrap_or_else(|error| panic!("{label} CopyObject failed: {error:?}"));
            assert_eq!(
                copy.copy_source_version_id(),
                expected_source_version,
                "{label} CopyObject"
            );
            let copied = client
                .get_object()
                .bucket(destination_bucket)
                .key(&copy_key)
                .send_retrying_operation_aborted("get source-version-header copied object")
                .await
                .unwrap();
            assert_eq!(copied.e_tag(), Some(expected_etag));
            let copied_data = copied.body.collect().await.unwrap().into_bytes();
            assert_eq!(&copied_data[..], expected_body);

            let upload_key = format!("upload-part-copy-source-version-{label}");
            let upload = client
                .create_multipart_upload()
                .bucket(destination_bucket)
                .key(&upload_key)
                .send_retrying_operation_aborted("create source-version-header copy upload")
                .await
                .unwrap();
            let upload_id = upload.upload_id().unwrap();
            let part = client
                .upload_part_copy()
                .bucket(destination_bucket)
                .key(&upload_key)
                .upload_id(upload_id)
                .part_number(1)
                .copy_source(&copy_source)
                .send()
                .await
                .unwrap_or_else(|error| panic!("{label} UploadPartCopy failed: {error:?}"));
            assert_eq!(
                part.copy_source_version_id(),
                expected_source_version,
                "{label} UploadPartCopy"
            );
            assert_eq!(
                part.copy_part_result().and_then(|result| result.e_tag()),
                Some(expected_etag)
            );
            client
                .abort_multipart_upload()
                .bucket(destination_bucket)
                .key(&upload_key)
                .upload_id(upload_id)
                .send_retrying_operation_aborted("abort source-version-header copy upload")
                .await
                .unwrap();
            s3_tests::delete_object_retrying_operation_aborted(
                client,
                destination_bucket,
                &copy_key,
            )
            .await
            .unwrap();
        }

        let client = CTX.client();
        let source_bucket = unique_bucket();
        let destination_bucket = unique_bucket();
        s3_tests::create_bucket(client, &source_bucket)
            .await
            .unwrap();
        s3_tests::create_bucket(client, &destination_bucket)
            .await
            .unwrap();

        let disabled_etag = put_object(&source_bucket, "disabled", b"disabled source").await;
        assert_source_version_headers(
            format!("{source_bucket}/disabled"),
            &destination_bucket,
            "disabled",
            None,
            &disabled_etag,
            b"disabled source",
        )
        .await;
        assert_source_version_headers(
            copy_source_with_version(&source_bucket, "disabled", "null"),
            &destination_bucket,
            "disabled-explicit-null",
            Some("null"),
            &disabled_etag,
            b"disabled source",
        )
        .await;

        s3_tests::enable_bucket_versioning(client, &source_bucket).await;
        let enabled = put_object_result_retrying_operation_aborted(
            "put enabled implicit copy source",
            || {
                client
                    .put_object()
                    .bucket(&source_bucket)
                    .key("enabled")
                    .body(ByteStream::from_static(b"enabled source"))
            },
        )
        .await
        .unwrap();
        let enabled_etag = enabled.e_tag().unwrap().to_string();
        let enabled_version = enabled.version_id().unwrap().to_string();
        assert_source_version_headers(
            format!("{source_bucket}/enabled"),
            &destination_bucket,
            "enabled",
            Some(&enabled_version),
            &enabled_etag,
            b"enabled source",
        )
        .await;

        client
            .put_bucket_versioning()
            .bucket(&source_bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Suspended)
                    .build(),
            )
            .send_retrying_operation_aborted("suspend implicit copy source bucket")
            .await
            .unwrap();
        let suspended = put_object_result_retrying_operation_aborted(
            "put suspended implicit copy source",
            || {
                client
                    .put_object()
                    .bucket(&source_bucket)
                    .key("suspended")
                    .body(ByteStream::from_static(b"suspended source"))
            },
        )
        .await
        .unwrap();
        assert_eq!(suspended.version_id(), None);
        let suspended_etag = suspended.e_tag().unwrap().to_string();
        assert_source_version_headers(
            format!("{source_bucket}/suspended"),
            &destination_bucket,
            "suspended",
            None,
            &suspended_etag,
            b"suspended source",
        )
        .await;
        assert_source_version_headers(
            copy_source_with_version(&source_bucket, "suspended", "null"),
            &destination_bucket,
            "suspended-explicit-null",
            Some("null"),
            &suspended_etag,
            b"suspended source",
        )
        .await;

        cleanup_versioned_bucket(client, &source_bucket).await;
        s3_tests::delete_bucket_retrying_operation_aborted(client, &destination_bucket).await;
    });
}

#[test]
fn test_copy_source_malformed_dates_are_ignored() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let source_etag = put_object(&bucket, "src", b"source data").await;

        for (index, header_name) in [
            "x-amz-copy-source-if-modified-since",
            "x-amz-copy-source-if-unmodified-since",
        ]
        .into_iter()
        .enumerate()
        {
            let destination = format!("malformed-date-copy-{index}");
            let copy_source = format!("{bucket}/src");
            let response = raw_object_with(
                "PUT",
                &bucket,
                &destination,
                b"",
                &[
                    ("x-amz-copy-source", copy_source.as_str()),
                    (header_name, "not-an-http-date"),
                ],
            );
            assert_eq!(
                response.status, 200,
                "CopyObject with malformed {header_name}: {response:?}"
            );
            let copied = client
                .get_object()
                .bucket(&bucket)
                .key(&destination)
                .send_retrying_operation_aborted("get malformed-date copied object")
                .await
                .unwrap();
            assert_eq!(copied.e_tag(), Some(source_etag.as_str()));
            let copied_data = copied.body.collect().await.unwrap().into_bytes();
            assert_eq!(&copied_data[..], b"source data");

            let upload_key = format!("malformed-date-part-copy-{index}");
            let upload = client
                .create_multipart_upload()
                .bucket(&bucket)
                .key(&upload_key)
                .send_retrying_operation_aborted("create malformed-date copy upload")
                .await
                .unwrap();
            let upload_id = upload.upload_id().unwrap();
            let upload_response = s3_tests::send_signed_request(
                "PUT",
                &format!(
                    "{}/{}/{}?partNumber=1&uploadId={upload_id}",
                    CTX.endpoint(),
                    bucket,
                    upload_key
                ),
                b"",
                [
                    ("x-amz-copy-source", copy_source.as_str()),
                    (header_name, "not-an-http-date"),
                ],
            );
            assert_eq!(
                upload_response.status, 200,
                "UploadPartCopy with malformed {header_name}: {upload_response:?}"
            );
            let parts = client
                .list_parts()
                .bucket(&bucket)
                .key(&upload_key)
                .upload_id(upload_id)
                .send_retrying_operation_aborted("list malformed-date copied part")
                .await
                .unwrap();
            assert_eq!(parts.parts().len(), 1);
            assert_eq!(parts.parts()[0].e_tag(), Some(source_etag.as_str()));
            client
                .abort_multipart_upload()
                .bucket(&bucket)
                .key(&upload_key)
                .upload_id(upload_id)
                .send_retrying_operation_aborted("abort malformed-date copy upload")
                .await
                .unwrap();
        }

        cleanup(
            &bucket,
            &["src", "malformed-date-copy-0", "malformed-date-copy-1"],
        )
        .await;
    });
}

#[test]
fn test_copy_object_racing_source_replacement_uses_one_object_version() {
    s3_tests::run(async {
        for attempt in 0..6 {
            let client = CTX.client();
            let bucket = setup_bucket().await;
            let source_key = "copy-object-race-replaced-source";
            let destination_key = "copy-object-race-replaced-destination";
            let original_body = vec![b'o'; 64 * 1024];
            let replacement_body = vec![b'r'; 64 * 1024];
            let original = put_object_result_retrying_operation_aborted(
                "put CopyObject replacement-race source",
                || {
                    client
                        .put_object()
                        .bucket(&bucket)
                        .key(source_key)
                        .content_type("application/x-original")
                        .metadata("source-state", "original")
                        .body(ByteStream::from(original_body.clone()))
                },
            )
            .await
            .unwrap();
            let original_etag = original.e_tag().unwrap().to_string();

            let barrier = Arc::new(tokio::sync::Barrier::new(2));
            let copy_client = client.clone();
            let copy_bucket = bucket.clone();
            let copy_barrier = Arc::clone(&barrier);
            let copy = tokio::spawn(async move {
                copy_barrier.wait().await;
                if attempt % 2 != 0 {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                copy_client
                    .copy_object()
                    .bucket(&copy_bucket)
                    .key(destination_key)
                    .copy_source(format!("{copy_bucket}/{source_key}"))
                    .send()
                    .await
            });
            let replacement_client = client.clone();
            let replacement_bucket = bucket.clone();
            let replacement_barrier = barrier;
            let replacement_body_for_task = replacement_body.clone();
            let replacement = tokio::spawn(async move {
                replacement_barrier.wait().await;
                if attempt % 2 == 0 {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                replacement_client
                    .put_object()
                    .bucket(replacement_bucket)
                    .key(source_key)
                    .content_type("application/x-replacement")
                    .metadata("source-state", "replacement")
                    .body(ByteStream::from(replacement_body_for_task))
                    .send()
                    .await
            });
            let (copy, replacement) = tokio::join!(copy, replacement);
            copy.unwrap()
                .unwrap_or_else(|error| panic!("CopyObject attempt {attempt}: {error:?}"));
            let replacement = replacement
                .unwrap()
                .unwrap_or_else(|error| panic!("source replacement attempt {attempt}: {error:?}"));
            let replacement_etag = replacement.e_tag().unwrap().to_string();
            assert_ne!(original_etag, replacement_etag);

            let copied = client
                .get_object()
                .bucket(&bucket)
                .key(destination_key)
                .send()
                .await
                .unwrap();
            let copied_etag = copied.e_tag().unwrap().to_string();
            assert!(
                copied_etag == original_etag || copied_etag == replacement_etag,
                "CopyObject attempt {attempt} returned an ETag from neither source version: {copied_etag}"
            );
            let expected_body = if copied_etag == original_etag {
                original_body.as_slice()
            } else {
                replacement_body.as_slice()
            };
            let (expected_content_type, expected_source_state) = if copied_etag == original_etag {
                ("application/x-original", "original")
            } else {
                ("application/x-replacement", "replacement")
            };
            assert_eq!(copied.content_type(), Some(expected_content_type));
            assert_eq!(
                copied
                    .metadata()
                    .and_then(|metadata| metadata.get("source-state"))
                    .map(String::as_str),
                Some(expected_source_state)
            );
            let copied_data = copied.body.collect().await.unwrap().into_bytes();
            assert_eq!(copied_data.as_ref(), expected_body);

            let current_source = client
                .get_object()
                .bucket(&bucket)
                .key(source_key)
                .send()
                .await
                .unwrap();
            assert_eq!(current_source.e_tag(), Some(replacement_etag.as_str()));
            assert_eq!(
                current_source.content_type(),
                Some("application/x-replacement")
            );
            assert_eq!(
                current_source
                    .metadata()
                    .and_then(|metadata| metadata.get("source-state"))
                    .map(String::as_str),
                Some("replacement")
            );
            assert_eq!(
                current_source
                    .body
                    .collect()
                    .await
                    .unwrap()
                    .into_bytes()
                    .as_ref(),
                replacement_body.as_slice()
            );

            cleanup(&bucket, &[source_key, destination_key]).await;
        }
    });
}

#[test]
fn test_copy_object_racing_source_delete_is_atomic() {
    s3_tests::run(async {
        for attempt in 0..6 {
            let client = CTX.client();
            let bucket = setup_bucket().await;
            let source_key = "copy-object-race-deleted-source";
            let destination_key = "copy-object-race-deleted-destination";
            let source_body = vec![b's'; 64 * 1024];
            let source = put_object_result_retrying_operation_aborted(
                "put CopyObject deletion-race source",
                || {
                    client
                        .put_object()
                        .bucket(&bucket)
                        .key(source_key)
                        .content_type("application/x-before-delete")
                        .metadata("source-state", "before-delete")
                        .body(ByteStream::from(source_body.clone()))
                },
            )
            .await
            .unwrap();
            let source_etag = source.e_tag().unwrap().to_string();

            let barrier = Arc::new(tokio::sync::Barrier::new(2));
            let copy_client = client.clone();
            let copy_bucket = bucket.clone();
            let copy_barrier = Arc::clone(&barrier);
            let copy = tokio::spawn(async move {
                copy_barrier.wait().await;
                if attempt % 2 != 0 {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                copy_client
                    .copy_object()
                    .bucket(&copy_bucket)
                    .key(destination_key)
                    .copy_source(format!("{copy_bucket}/{source_key}"))
                    .send()
                    .await
            });
            let delete_client = client.clone();
            let delete_bucket = bucket.clone();
            let delete = tokio::spawn(async move {
                barrier.wait().await;
                if attempt % 2 == 0 {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                delete_client
                    .delete_object()
                    .bucket(delete_bucket)
                    .key(source_key)
                    .send()
                    .await
            });
            let (copy, delete) = tokio::join!(copy, delete);
            let copy = copy.unwrap();
            delete
                .unwrap()
                .unwrap_or_else(|error| panic!("source delete attempt {attempt}: {error:?}"));

            let missing_source = client
                .get_object()
                .bucket(&bucket)
                .key(source_key)
                .send()
                .await;
            assert_eq!(err_status(&missing_source), 404);
            assert_s3_err_code(&missing_source, "NoSuchKey");

            match copy {
                Ok(_) => {
                    let copied = client
                        .get_object()
                        .bucket(&bucket)
                        .key(destination_key)
                        .send()
                        .await
                        .unwrap();
                    assert_eq!(copied.e_tag(), Some(source_etag.as_str()));
                    assert_eq!(copied.content_type(), Some("application/x-before-delete"));
                    assert_eq!(
                        copied
                            .metadata()
                            .and_then(|metadata| metadata.get("source-state"))
                            .map(String::as_str),
                        Some("before-delete")
                    );
                    assert_eq!(
                        copied.body.collect().await.unwrap().into_bytes().as_ref(),
                        source_body.as_slice()
                    );
                }
                Err(error) => {
                    assert_eq!(
                        error
                            .raw_response()
                            .map(|response| response.status().as_u16()),
                        Some(404)
                    );
                    assert_eq!(
                        error
                            .as_service_error()
                            .and_then(ProvideErrorMetadata::code),
                        Some("NoSuchKey")
                    );
                    let destination = client
                        .head_object()
                        .bucket(&bucket)
                        .key(destination_key)
                        .send()
                        .await;
                    assert_eq!(err_status(&destination), 404);
                }
            }

            cleanup(&bucket, &[source_key, destination_key]).await;
        }
    });
}

// ── PUT If-Match (additional Ceph tests) ────────────────────────────────

#[test]
fn test_put_object_if_match() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let etag = put_object(&bucket, "obj", b"data").await;

        // Basic If-Match PUT
        put_object_result_retrying_operation_aborted(
            "put conditional object with if-match",
            || {
                CTX.client()
                    .put_object()
                    .bucket(&bucket)
                    .key("obj")
                    .if_match(&etag)
                    .body(ByteStream::from_static(b"updated"))
            },
        )
        .await
        .unwrap();

        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("get conditional object")
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"updated");

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_put_object_ifmatch_overwrite_existed_good() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let etag = put_object(&bucket, "obj", b"original").await;

        // Overwrite existing object with matching etag
        put_object_result_retrying_operation_aborted(
            "put conditional overwrite object with if-match",
            || {
                CTX.client()
                    .put_object()
                    .bucket(&bucket)
                    .key("obj")
                    .if_match(&etag)
                    .body(ByteStream::from_static(b"overwritten"))
            },
        )
        .await
        .unwrap();

        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("get conditional object")
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"overwritten");

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── PUT unsupported conditional headers → 501 ──────────────────────────

#[test]
fn test_put_object_ifmatch_wildcard_not_implemented() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "obj", b"data").await;

        // If-Match: * on write → 501 NotImplemented
        let result = put_object_result_retrying_operation_aborted(
            "put conditional object with unsupported if-match wildcard",
            || {
                CTX.client()
                    .put_object()
                    .bucket(&bucket)
                    .key("obj")
                    .if_match("*")
                    .body(ByteStream::from_static(b"updated"))
            },
        )
        .await;
        assert_eq!(err_status(&result), 501);

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_put_object_ifnonematch_specific_not_implemented() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let etag = put_object(&bucket, "obj", b"data").await;

        // If-None-Match: <specific etag> on write → 501 NotImplemented
        let result = put_object_result_retrying_operation_aborted(
            "put conditional object with unsupported if-none-match etag",
            || {
                CTX.client()
                    .put_object()
                    .bucket(&bucket)
                    .key("obj")
                    .if_none_match(&etag)
                    .body(ByteStream::from_static(b"updated"))
            },
        )
        .await;
        assert_eq!(err_status(&result), 501);

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── PUT both If-Match and If-None-Match → rejected ──────────────────────

#[test]
fn test_put_object_both_ifmatch_and_ifnonematch_rejected() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let etag = put_object(&bucket, "obj", b"data").await;

        // Both If-Match: <etag> and If-None-Match: * on the same PUT → error
        let result = put_object_result_retrying_operation_aborted(
            "put conditional object with both conditional headers",
            || {
                CTX.client()
                    .put_object()
                    .bucket(&bucket)
                    .key("obj")
                    .if_match(&etag)
                    .if_none_match("*")
                    .body(ByteStream::from_static(b"updated"))
            },
        )
        .await;
        assert_eq!(err_status(&result), 501);

        // Verify original content unchanged
        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("get conditional object")
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"data");

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── Copy destination conditions ─────────────────────────────────────────

#[test]
fn test_copy_object_ifnonematch_star_nonexistent_destination_succeeds() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "src", b"source data").await;

        CTX.client()
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src", bucket))
            .if_none_match("*")
            .send_retrying_operation_aborted("copy create-only object to missing destination")
            .await
            .unwrap();

        let response = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("dst")
            .send_retrying_operation_aborted("get create-only copy destination")
            .await
            .unwrap();
        let data = response.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"source data");

        cleanup(&bucket, &["src", "dst"]).await;
    });
}

#[test]
fn test_copy_object_ifnonematch_star_existing_destination_fails_without_mutation() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "src", b"source data").await;
        let original_etag = put_object(&bucket, "dst", b"old destination").await;

        let result = CTX
            .client()
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src", bucket))
            .if_none_match("*")
            .send_retrying_operation_aborted("copy create-only object to existing destination")
            .await;
        assert_eq!(err_status(&result), 412);
        assert_s3_err_code(&result, "PreconditionFailed");

        let response = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("dst")
            .send_retrying_operation_aborted("get rejected create-only copy destination")
            .await
            .unwrap();
        assert_eq!(response.e_tag(), Some(original_etag.as_str()));
        let data = response.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"old destination");

        let source = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("src")
            .send_retrying_operation_aborted("get source after rejected create-only copy")
            .await
            .unwrap();
        let source_data = source.body.collect().await.unwrap().into_bytes();
        assert_eq!(&source_data[..], b"source data");

        cleanup(&bucket, &["src", "dst"]).await;
    });
}

#[test]
fn test_copy_object_destination_conditions_in_versioned_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        s3_tests::enable_bucket_versioning(client, &bucket).await;
        let source_etag = put_object(&bucket, "src", b"source data").await;
        let original = put_object_result_retrying_operation_aborted(
            "put original versioned copy destination",
            || {
                client
                    .put_object()
                    .bucket(&bucket)
                    .key("dst")
                    .body(ByteStream::from_static(b"old destination"))
            },
        )
        .await
        .unwrap();
        let original_etag = original.e_tag().unwrap().to_string();
        let original_version_id = original.version_id().unwrap().to_string();

        let rejected_existing = client
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src", bucket))
            .if_none_match("*")
            .send_retrying_operation_aborted(
                "copy create-only object over versioned live destination",
            )
            .await;
        assert_eq!(err_status(&rejected_existing), 412);
        assert_s3_err_code(&rejected_existing, "PreconditionFailed");

        let versions_after_rejection = client
            .list_object_versions()
            .bucket(&bucket)
            .prefix("dst")
            .send_retrying_operation_aborted("list versions after rejected conditional copy")
            .await
            .unwrap();
        assert_eq!(versions_after_rejection.versions().len(), 1);
        assert!(versions_after_rejection.delete_markers().is_empty());

        let marker = client
            .delete_object()
            .bucket(&bucket)
            .key("dst")
            .send_retrying_operation_aborted("create destination delete marker")
            .await
            .unwrap();
        assert_eq!(marker.delete_marker(), Some(true));
        let marker_version_id = marker.version_id().unwrap().to_string();

        let rejected_marker_match = client
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src", bucket))
            .if_match(&original_etag)
            .send_retrying_operation_aborted("copy if-match over current delete marker")
            .await;
        assert_eq!(err_status(&rejected_marker_match), 404);
        assert_s3_err_code(&rejected_marker_match, "NoSuchKey");

        let copied = client
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src", bucket))
            .if_none_match("*")
            .send_retrying_operation_aborted("copy create-only object over current delete marker")
            .await
            .unwrap();
        let copied_version_id = copied.version_id().unwrap().to_string();

        let response = client
            .get_object()
            .bucket(&bucket)
            .key("dst")
            .send_retrying_operation_aborted("get versioned conditional copy destination")
            .await
            .unwrap();
        assert_eq!(response.e_tag(), Some(source_etag.as_str()));
        let data = response.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"source data");

        let final_versions = client
            .list_object_versions()
            .bucket(&bucket)
            .prefix("dst")
            .send_retrying_operation_aborted("list versions after conditional copy")
            .await
            .unwrap();
        assert_eq!(final_versions.versions().len(), 2);
        assert_eq!(final_versions.delete_markers().len(), 1);
        let copied_version = final_versions
            .versions()
            .iter()
            .find(|version| version.version_id() == Some(copied_version_id.as_str()))
            .expect("copied destination version must remain listed");
        assert_eq!(copied_version.is_latest(), Some(true));
        let original_version = final_versions
            .versions()
            .iter()
            .find(|version| version.version_id() == Some(original_version_id.as_str()))
            .expect("original destination version must remain listed");
        assert_eq!(original_version.is_latest(), Some(false));
        let retained_marker = &final_versions.delete_markers()[0];
        assert_eq!(
            retained_marker.version_id(),
            Some(marker_version_id.as_str())
        );
        assert_eq!(retained_marker.is_latest(), Some(false));

        let original_response = client
            .get_object()
            .bucket(&bucket)
            .key("dst")
            .version_id(&original_version_id)
            .send_retrying_operation_aborted("get retained original copy destination version")
            .await
            .unwrap();
        assert_eq!(original_response.e_tag(), Some(original_etag.as_str()));
        let original_data = original_response.body.collect().await.unwrap().into_bytes();
        assert_eq!(&original_data[..], b"old destination");

        let source_response = client
            .get_object()
            .bucket(&bucket)
            .key("src")
            .send_retrying_operation_aborted("get source after versioned conditional copies")
            .await
            .unwrap();
        assert_eq!(source_response.e_tag(), Some(source_etag.as_str()));
        let source_data = source_response.body.collect().await.unwrap().into_bytes();
        assert_eq!(&source_data[..], b"source data");

        let match_original = put_object_result_retrying_operation_aborted(
            "put versioned if-match copy destination",
            || {
                client
                    .put_object()
                    .bucket(&bucket)
                    .key("dst-match")
                    .body(ByteStream::from_static(b"old match destination"))
            },
        )
        .await
        .unwrap();
        let match_original_etag = match_original.e_tag().unwrap().to_string();
        let match_original_version_id = match_original.version_id().unwrap().to_string();
        let matched = client
            .copy_object()
            .bucket(&bucket)
            .key("dst-match")
            .copy_source(format!("{}/src", bucket))
            .if_match(&match_original_etag)
            .send_retrying_operation_aborted("copy if-match over versioned live destination")
            .await
            .unwrap();
        let matched_version_id = matched.version_id().unwrap().to_string();
        assert_ne!(matched_version_id, match_original_version_id);

        let matched_versions = client
            .list_object_versions()
            .bucket(&bucket)
            .prefix("dst-match")
            .send_retrying_operation_aborted("list versioned if-match copy destination")
            .await
            .unwrap();
        assert_eq!(matched_versions.versions().len(), 2);
        assert!(matched_versions.delete_markers().is_empty());
        let matched_current = matched_versions
            .versions()
            .iter()
            .find(|version| version.version_id() == Some(matched_version_id.as_str()))
            .expect("matched copy version must remain listed");
        assert_eq!(matched_current.is_latest(), Some(true));
        assert_eq!(matched_current.e_tag(), Some(source_etag.as_str()));
        let matched_original = matched_versions
            .versions()
            .iter()
            .find(|version| version.version_id() == Some(match_original_version_id.as_str()))
            .expect("original matched destination version must remain listed");
        assert_eq!(matched_original.is_latest(), Some(false));
        assert_eq!(matched_original.e_tag(), Some(match_original_etag.as_str()));

        let matched_current_response = client
            .get_object()
            .bucket(&bucket)
            .key("dst-match")
            .send_retrying_operation_aborted("get versioned if-match copy destination")
            .await
            .unwrap();
        assert_eq!(
            matched_current_response.version_id(),
            Some(matched_version_id.as_str())
        );
        let matched_current_data = matched_current_response
            .body
            .collect()
            .await
            .unwrap()
            .into_bytes();
        assert_eq!(&matched_current_data[..], b"source data");

        let matched_original_response = client
            .get_object()
            .bucket(&bucket)
            .key("dst-match")
            .version_id(&match_original_version_id)
            .send_retrying_operation_aborted("get original versioned if-match destination")
            .await
            .unwrap();
        assert_eq!(
            matched_original_response.e_tag(),
            Some(match_original_etag.as_str())
        );
        let matched_original_data = matched_original_response
            .body
            .collect()
            .await
            .unwrap()
            .into_bytes();
        assert_eq!(&matched_original_data[..], b"old match destination");

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_copy_object_destination_conditions_in_suspended_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        s3_tests::enable_bucket_versioning(client, &bucket).await;
        let source_a_etag = put_object(&bucket, "src-a", b"source a").await;
        let source_b_etag = put_object(&bucket, "src-b", b"source b").await;
        let numbered = put_object_result_retrying_operation_aborted(
            "put numbered destination before suspending versioning",
            || {
                client
                    .put_object()
                    .bucket(&bucket)
                    .key("dst")
                    .body(ByteStream::from_static(b"numbered destination"))
            },
        )
        .await
        .unwrap();
        let numbered_etag = numbered.e_tag().unwrap().to_string();
        let numbered_version_id = numbered.version_id().unwrap().to_string();

        client
            .put_bucket_versioning()
            .bucket(&bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Suspended)
                    .build(),
            )
            .send_retrying_operation_aborted("suspend versioning for conditional copy")
            .await
            .unwrap();

        let null_live = put_object_result_retrying_operation_aborted(
            "put null destination after suspending versioning",
            || {
                client
                    .put_object()
                    .bucket(&bucket)
                    .key("dst")
                    .body(ByteStream::from_static(b"null destination"))
            },
        )
        .await
        .unwrap();
        assert_eq!(null_live.version_id(), None);
        let null_live_etag = null_live.e_tag().unwrap().to_string();

        let rejected_existing = client
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src-a", bucket))
            .if_none_match("*")
            .send_retrying_operation_aborted(
                "copy create-only object over suspended null live destination",
            )
            .await;
        assert_eq!(err_status(&rejected_existing), 412);
        assert_s3_err_code(&rejected_existing, "PreconditionFailed");

        let after_rejected_existing = client
            .list_object_versions()
            .bucket(&bucket)
            .prefix("dst")
            .send_retrying_operation_aborted(
                "list suspended destination after rejected create-only copy",
            )
            .await
            .unwrap();
        assert_eq!(after_rejected_existing.versions().len(), 2);
        assert!(after_rejected_existing.delete_markers().is_empty());
        let retained_null = after_rejected_existing
            .versions()
            .iter()
            .find(|version| version.version_id() == Some("null"))
            .expect("rejected copy must retain the null live destination");
        assert_eq!(retained_null.e_tag(), Some(null_live_etag.as_str()));
        assert_eq!(retained_null.is_latest(), Some(true));
        let retained_numbered = after_rejected_existing
            .versions()
            .iter()
            .find(|version| version.version_id() == Some(numbered_version_id.as_str()))
            .expect("rejected copy must retain the numbered destination");
        assert_eq!(retained_numbered.e_tag(), Some(numbered_etag.as_str()));
        assert_eq!(retained_numbered.is_latest(), Some(false));

        let marker = client
            .delete_object()
            .bucket(&bucket)
            .key("dst")
            .send_retrying_operation_aborted("replace suspended null live destination with marker")
            .await
            .unwrap();
        assert_eq!(marker.delete_marker(), Some(true));
        assert_eq!(marker.version_id(), Some("null"));

        let rejected_marker_match = client
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src-a", bucket))
            .if_match(&null_live_etag)
            .send_retrying_operation_aborted("copy if-match over suspended null delete marker")
            .await;
        assert_eq!(err_status(&rejected_marker_match), 404);
        assert_s3_err_code(&rejected_marker_match, "NoSuchKey");

        let after_rejected_marker_match = client
            .list_object_versions()
            .bucket(&bucket)
            .prefix("dst")
            .send_retrying_operation_aborted(
                "list suspended destination after rejected marker if-match copy",
            )
            .await
            .unwrap();
        assert_eq!(after_rejected_marker_match.versions().len(), 1);
        assert_eq!(after_rejected_marker_match.delete_markers().len(), 1);
        assert_eq!(
            after_rejected_marker_match.versions()[0].version_id(),
            Some(numbered_version_id.as_str())
        );
        assert_eq!(
            after_rejected_marker_match.versions()[0].is_latest(),
            Some(false)
        );
        assert_eq!(
            after_rejected_marker_match.delete_markers()[0].version_id(),
            Some("null")
        );
        assert_eq!(
            after_rejected_marker_match.delete_markers()[0].is_latest(),
            Some(true)
        );

        let copied = client
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src-a", bucket))
            .if_none_match("*")
            .send_retrying_operation_aborted(
                "copy create-only object over suspended null delete marker",
            )
            .await
            .unwrap();
        assert_eq!(copied.version_id(), Some("null"));

        let replaced = client
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src-b", bucket))
            .if_match(&source_a_etag)
            .send_retrying_operation_aborted("copy if-match over suspended null live destination")
            .await
            .unwrap();
        assert_eq!(replaced.version_id(), Some("null"));

        let current = client
            .get_object()
            .bucket(&bucket)
            .key("dst")
            .send_retrying_operation_aborted("get final suspended conditional copy destination")
            .await
            .unwrap();
        assert_eq!(current.version_id(), Some("null"));
        assert_eq!(current.e_tag(), Some(source_b_etag.as_str()));
        let current_data = current.body.collect().await.unwrap().into_bytes();
        assert_eq!(&current_data[..], b"source b");

        let ranged = client
            .get_object()
            .bucket(&bucket)
            .key("dst")
            .range("bytes=1-3")
            .send_retrying_operation_aborted("range get suspended null copy destination")
            .await
            .unwrap();
        assert_eq!(ranged.version_id(), Some("null"));
        let ranged_data = ranged.body.collect().await.unwrap().into_bytes();
        assert_eq!(&ranged_data[..], b"our");

        let part = client
            .get_object()
            .bucket(&bucket)
            .key("dst")
            .part_number(1)
            .send_retrying_operation_aborted("part get suspended null copy destination")
            .await
            .unwrap();
        assert_eq!(part.version_id(), Some("null"));
        assert_eq!(part.parts_count(), None);
        let part_data = part.body.collect().await.unwrap().into_bytes();
        assert_eq!(&part_data[..], b"source b");

        let head = client
            .head_object()
            .bucket(&bucket)
            .key("dst")
            .send_retrying_operation_aborted("head suspended null copy destination")
            .await
            .unwrap();
        assert_eq!(head.version_id(), Some("null"));

        let part_head = client
            .head_object()
            .bucket(&bucket)
            .key("dst")
            .part_number(1)
            .send_retrying_operation_aborted("part head suspended null copy destination")
            .await
            .unwrap();
        assert_eq!(part_head.version_id(), Some("null"));
        assert_eq!(part_head.parts_count(), None);

        let final_versions = client
            .list_object_versions()
            .bucket(&bucket)
            .prefix("dst")
            .send_retrying_operation_aborted("list final suspended conditional copy versions")
            .await
            .unwrap();
        assert_eq!(final_versions.versions().len(), 2);
        assert!(final_versions.delete_markers().is_empty());
        let final_null = final_versions
            .versions()
            .iter()
            .find(|version| version.version_id() == Some("null"))
            .expect("final copied null destination must remain listed");
        assert_eq!(final_null.e_tag(), Some(source_b_etag.as_str()));
        assert_eq!(final_null.is_latest(), Some(true));
        let final_numbered = final_versions
            .versions()
            .iter()
            .find(|version| version.version_id() == Some(numbered_version_id.as_str()))
            .expect("numbered destination must survive null replacements");
        assert_eq!(final_numbered.e_tag(), Some(numbered_etag.as_str()));
        assert_eq!(final_numbered.is_latest(), Some(false));

        let numbered_response = client
            .get_object()
            .bucket(&bucket)
            .key("dst")
            .version_id(&numbered_version_id)
            .send_retrying_operation_aborted("get retained numbered suspended destination")
            .await
            .unwrap();
        assert_eq!(numbered_response.e_tag(), Some(numbered_etag.as_str()));
        let numbered_data = numbered_response.body.collect().await.unwrap().into_bytes();
        assert_eq!(&numbered_data[..], b"numbered destination");

        for (key, expected_etag, expected_body) in [
            ("src-a", source_a_etag.as_str(), &b"source a"[..]),
            ("src-b", source_b_etag.as_str(), &b"source b"[..]),
        ] {
            let source = client
                .get_object()
                .bucket(&bucket)
                .key(key)
                .send_retrying_operation_aborted("get source after suspended conditional copies")
                .await
                .unwrap();
            assert_eq!(source.e_tag(), Some(expected_etag));
            let source_data = source.body.collect().await.unwrap().into_bytes();
            assert_eq!(&source_data[..], expected_body);
        }

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_copy_object_ifmatch_good() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "src", b"source data").await;
        let dst_etag = put_object(&bucket, "dst", b"old dst").await;

        // If-Match on destination with correct etag → should succeed
        CTX.client()
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src", bucket))
            .if_match(&dst_etag)
            .send_retrying_operation_aborted("copy conditional object")
            .await
            .unwrap();

        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("dst")
            .send_retrying_operation_aborted("get conditional object")
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"source data");

        cleanup(&bucket, &["src", "dst"]).await;
    });
}

#[test]
fn test_copy_object_ifmatch_failed() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "src", b"source data").await;
        put_object(&bucket, "dst", b"old dst").await;

        // If-Match on destination with wrong etag → should fail
        let result = CTX
            .client()
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src", bucket))
            .if_match("\"0000000000000000\"")
            .send_retrying_operation_aborted("copy conditional object")
            .await;
        assert_eq!(err_status(&result), 412);

        cleanup(&bucket, &["src", "dst"]).await;
    });
}

// ── Copy destination unsupported conditional headers → 501 ──────────────

#[test]
fn test_copy_object_ifmatch_wildcard_not_implemented() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "src", b"source data").await;
        put_object(&bucket, "dst", b"old dst").await;

        // If-Match: * on copy destination → 501 NotImplemented
        let result = CTX
            .client()
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src", bucket))
            .if_match("*")
            .send_retrying_operation_aborted("copy conditional object")
            .await;
        assert_eq!(err_status(&result), 501);

        cleanup(&bucket, &["src", "dst"]).await;
    });
}

#[test]
fn test_copy_object_ifmatch_quoted_star_not_implemented() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "src", b"source data").await;
        put_object(&bucket, "dst", b"old dst").await;

        let result = CTX
            .client()
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src", bucket))
            .if_match("\"*\"")
            .send_retrying_operation_aborted("copy conditional object")
            .await;
        assert_eq!(err_status(&result), 501);

        cleanup(&bucket, &["src", "dst"]).await;
    });
}

#[test]
fn test_copy_object_ifnonematch_specific_not_implemented() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "src", b"source data").await;
        let dst_etag = put_object(&bucket, "dst", b"old dst").await;

        // If-None-Match: <specific etag> on copy destination → 501 NotImplemented
        let result = CTX
            .client()
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src", bucket))
            .if_none_match(&dst_etag)
            .send_retrying_operation_aborted("copy conditional object")
            .await;
        assert_eq!(err_status(&result), 501);

        cleanup(&bucket, &["src", "dst"]).await;
    });
}

// ── DELETE If-Match (additional Ceph tests) ─────────────────────────────

#[test]
fn test_delete_object_if_match() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "obj", b"hello").await;

        // Delete with wrong If-Match → 412
        let result = CTX
            .client()
            .delete_object()
            .bucket(&bucket)
            .key("obj")
            .if_match("\"0000000000000000\"")
            .send_retrying_operation_aborted("delete conditional object")
            .await;
        assert_eq!(err_status(&result), 412);

        // Verify object still exists
        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("get conditional object")
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"hello");

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_delete_object_version_if_match_not_implemented() {
    s3_tests::run(async {
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
            .send_retrying_operation_aborted("put conditional bucket versioning")
            .await
            .unwrap();

        let put = put_object_result_retrying_operation_aborted(
            "put versioned object before delete-marker conditional get",
            || {
                client
                    .put_object()
                    .bucket(&bucket)
                    .key("obj")
                    .body(ByteStream::from_static(b"hello"))
            },
        )
        .await
        .unwrap();
        let version_id = put.version_id().unwrap().to_string();
        let etag = put.e_tag().unwrap().to_string();

        let bad = client
            .delete_object()
            .bucket(&bucket)
            .key("obj")
            .version_id(&version_id)
            .if_match("\"0000000000000000\"")
            .send_retrying_operation_aborted("delete conditional object")
            .await;
        assert_eq!(err_status(&bad), 501);

        let good = client
            .delete_object()
            .bucket(&bucket)
            .key("obj")
            .version_id(&version_id)
            .if_match(&etag)
            .send_retrying_operation_aborted("delete conditional object")
            .await;
        assert_eq!(err_status(&good), 501);

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .version_id(&version_id)
            .send_retrying_operation_aborted("get conditional object")
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"hello");

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

// ── Response shapes ─────────────────────────────────────────────────

/// Full response shapes for plain-object conditional requests: 304 from a
/// matching `If-None-Match` GET, 412 from a mismatching `If-Match` GET, and
/// 412 from `If-None-Match: *` PUT over an existing object. AWS names the
/// failing header in a `<Condition>` element.
#[test]
fn test_conditional_response_shapes() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "cond-shape.txt";
        let etag = put_object(&bucket, key, b"conditional-body").await;

        let not_modified = raw_object_with(
            "GET",
            &bucket,
            key,
            b"",
            &[("If-None-Match", etag.as_str())],
        );
        assert_shape(
            "GetObject If-None-Match 304",
            &not_modified,
            &shape()
                .status(304)
                .headers([
                    ("etag", etag.as_str()),
                    ("last-modified", "{http_date}"),
                    ("x-amz-request-id", "{request_id}"),
                    ("x-amz-id-2", "{host_id}"),
                ])
                .body_empty(),
        );

        let get_mismatch = raw_object_with(
            "GET",
            &bucket,
            key,
            b"",
            &[("If-Match", "\"00000000000000000000000000000000\"")],
        );
        assert_shape(
            "GetObject If-Match 412",
            &get_mismatch,
            &shape()
                .status(412)
                .headers(error_response_headers())
                .body(expected_error::precondition_failed("If-Match")),
        );

        let put_existing =
            raw_object_with("PUT", &bucket, key, b"overwrite", &[("If-None-Match", "*")]);
        assert_shape(
            "PutObject If-None-Match star 412",
            &put_existing,
            &shape()
                .status(412)
                .headers(error_response_headers())
                .body(expected_error::precondition_failed("If-None-Match")),
        );

        s3_tests::delete_object_retrying_operation_aborted(client, &bucket, key)
            .await
            .expect("delete conditional shape fixture");
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}
