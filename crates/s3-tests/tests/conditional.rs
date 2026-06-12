use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::primitives::DateTime;
use aws_sdk_s3::types::{
    BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, VersioningConfiguration,
};
use s3_tests::{
    cleanup_versioned_bucket, err_status, unique_bucket, SendRetryingOperationAborted, CTX,
};
use serde_json::json;

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

/// Cleanup helper: delete object + bucket.
async fn cleanup(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = s3_tests::delete_object_retrying_operation_aborted(client, bucket, *key).await;
    }
    s3_tests::delete_bucket_retrying_operation_aborted(client, bucket).await;
}

fn bucket_wildcard_resource(bucket: &str) -> String {
    format!("arn:aws:s3:::{bucket}/*")
}

fn alt_policy_principal() -> serde_json::Value {
    json!({ "AWS": format!("arn:aws:iam::{}:root", CTX.alt_account_id()) })
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
    for attempt in 0..20 {
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
        if attempt + 1 < 20 {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    }

    panic!("alt PutObject policy allow did not converge");
}

async fn wait_for_alt_get_object(bucket: &str, key: &str) {
    for attempt in 0..20 {
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
        if attempt + 1 < 20 {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    }

    panic!("alt GetObject policy allow did not converge");
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
        for attempt in 0..20 {
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
            if attempt + 1 == 20 {
                panic!("alt conditional PutObject policy allow did not converge");
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
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
        put_object(&bucket, "obj", b"original").await;
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
        CTX.client()
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("obj")
            .upload_id(&upload_id)
            .send_retrying_operation_aborted("abort conditional multipart upload")
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
        assert_eq!(&data[..], b"original");

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
        put_object(&bucket, "obj", b"v1").await;
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
        CTX.client()
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("obj")
            .upload_id(&upload_id)
            .send_retrying_operation_aborted("abort conditional multipart upload")
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
        assert_eq!(&data[..], b"v1");

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
        CTX.client()
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("obj")
            .upload_id(&upload_id)
            .send_retrying_operation_aborted("abort conditional multipart upload")
            .await
            .unwrap();

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_complete_multipart_ifnonmatch_current_object_in_versioned_bucket() {
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
