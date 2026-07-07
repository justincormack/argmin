use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};
use s3_tests::{
    cleanup_versioned_bucket, err_status, raw_object_query, raw_object_with,
    shape::{assert_shape, error_response_headers, expected_error, shape},
    unique_bucket, SendRetryingOperationAborted, CTX,
};

async fn setup_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    bucket
}

async fn cleanup(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = s3_tests::delete_object_retrying_operation_aborted(client, bucket, key).await;
    }
    s3_tests::delete_bucket_retrying_operation_aborted(client, bucket).await;
}

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
        .send_retrying_operation_aborted("enable range test bucket versioning")
        .await
        .unwrap();
    bucket
}

async fn put_object(
    bucket: &str,
    key: &str,
    body: &[u8],
) -> aws_sdk_s3::operation::put_object::PutObjectOutput {
    s3_tests::put_object_retrying_operation_aborted(CTX.client(), bucket, key, body.to_vec()).await
}

// ── Valid range variants ──────────────────────────────────────────────

#[test]
fn test_range_get_start_end() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"abcdefghijklmnopqrstuvwxyz";
        put_object(&bucket, "r", body).await;

        // bytes=0-4 → "abcde"
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("r")
            .range("bytes=0-4")
            .send_retrying_operation_aborted("get range object")
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"abcde");

        cleanup(&bucket, &["r"]).await;
    });
}

#[test]
fn test_range_get_from_start() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"abcdefghijklmnopqrstuvwxyz";
        put_object(&bucket, "r", body).await;

        // bytes=23- → "xyz" (last 3 bytes)
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("r")
            .range("bytes=23-")
            .send_retrying_operation_aborted("get range object")
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"xyz");

        cleanup(&bucket, &["r"]).await;
    });
}

#[test]
fn test_range_get_suffix() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"abcdefghijklmnopqrstuvwxyz";
        put_object(&bucket, "r", body).await;

        // bytes=-3 → last 3 bytes = "xyz"
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("r")
            .range("bytes=-3")
            .send_retrying_operation_aborted("get range object")
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"xyz");

        cleanup(&bucket, &["r"]).await;
    });
}

#[test]
fn test_range_get_suffix_exceeds_size() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"abcdefghijklmnopqrstuvwxyz"; // 26 bytes
        put_object(&bucket, "r", body).await;

        // bytes=-999 → suffix exceeds size, returns whole object
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("r")
            .range("bytes=-999")
            .send_retrying_operation_aborted("get range object")
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], body);

        cleanup(&bucket, &["r"]).await;
    });
}

#[test]
fn test_range_get_unsatisfiable() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"abcdefghijklmnopqrstuvwxyz"; // 26 bytes
        put_object(&bucket, "r", body).await;

        // bytes=100-200 → start past end of object → 416
        let result = client
            .get_object()
            .bucket(&bucket)
            .key("r")
            .range("bytes=100-200")
            .send_retrying_operation_aborted("get unsatisfiable range object")
            .await;
        assert_eq!(err_status(&result), 416);

        cleanup(&bucket, &["r"]).await;
    });
}

#[test]
fn test_range_get_from_start_unsatisfiable() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"abcdefghijklmnopqrstuvwxyz"; // 26 bytes
        put_object(&bucket, "r", body).await;

        // bytes=100- → start past end → 416
        let result = client
            .get_object()
            .bucket(&bucket)
            .key("r")
            .range("bytes=100-")
            .send_retrying_operation_aborted("get unsatisfiable range object")
            .await;
        assert_eq!(err_status(&result), 416);

        cleanup(&bucket, &["r"]).await;
    });
}

#[test]
fn test_range_get_if_match_returns_partial_content() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"abcdefghijklmnopqrstuvwxyz";
        let put = put_object(&bucket, "r", body).await;
        let etag = put.e_tag().unwrap().to_string();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("r")
            .range("bytes=0-4")
            .if_match(&etag)
            .send_retrying_operation_aborted("get range object")
            .await
            .unwrap();
        assert_eq!(resp.accept_ranges(), Some("bytes"));
        assert_eq!(resp.content_range(), Some("bytes 0-4/26"));
        assert_eq!(resp.content_length(), Some(5));
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"abcde");

        cleanup(&bucket, &["r"]).await;
    });
}

#[test]
fn test_range_get_if_none_match_returns_not_modified() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let put = put_object(&bucket, "r", b"abcdefghijklmnopqrstuvwxyz").await;
        let etag = put.e_tag().unwrap().to_string();

        let result = client
            .get_object()
            .bucket(&bucket)
            .key("r")
            .range("bytes=0-4")
            .if_none_match(&etag)
            .send_retrying_operation_aborted("get not-modified range object")
            .await;
        assert_eq!(err_status(&result), 304);

        cleanup(&bucket, &["r"]).await;
    });
}

#[test]
fn test_range_get_if_match_mismatch_returns_precondition_failed() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        put_object(&bucket, "r", b"abcdefghijklmnopqrstuvwxyz").await;

        let result = client
            .get_object()
            .bucket(&bucket)
            .key("r")
            .range("bytes=0-4")
            .if_match("\"0000000000000000\"")
            .send_retrying_operation_aborted("get range object with failing precondition")
            .await;
        assert_eq!(err_status(&result), 412);

        cleanup(&bucket, &["r"]).await;
    });
}

#[test]
fn test_range_get_specific_version_returns_expected_slice_and_version_id() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;

        let put_v1 = put_object(&bucket, "r", b"abcdefghijklmnopqrstuvwxyz").await;
        let v1 = put_v1.version_id().unwrap().to_string();

        put_object(&bucket, "r", b"0123456789abcdefghijklmnopqrstuvwxyz").await;

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("r")
            .version_id(&v1)
            .range("bytes=2-5")
            .send_retrying_operation_aborted("get range object version")
            .await
            .unwrap();
        assert_eq!(resp.version_id(), Some(v1.as_str()));
        assert_eq!(resp.content_range(), Some("bytes 2-5/26"));
        assert_eq!(resp.content_length(), Some(4));
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"cdef");

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

// ── Response shapes ─────────────────────────────────────────────────

#[test]
fn test_range_and_override_response_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        let key = "shape-range.txt";

        let put = raw_object_with(
            "PUT",
            &bucket,
            key,
            b"abcdefghij",
            &[("Content-Type", "application/octet-stream")],
        );
        assert_eq!(put.status, 200, "range fixture PUT failed: {put:?}");

        let range = raw_object_with("GET", &bucket, key, b"", &[("Range", "bytes=2-5")]);
        let range_captures = assert_shape(
            "GetObject range",
            &range,
            &shape()
                .status(206)
                .headers([
                    ("etag", "{etag}"),
                    ("last-modified", "{http_date}"),
                    ("accept-ranges", "bytes"),
                    ("content-range", "bytes 2-5/10"),
                    ("content-type", "application/octet-stream"),
                    ("x-amz-server-side-encryption", "AES256"),
                    ("content-length", "4"),
                    ("x-amz-request-id", "{request_id}"),
                    ("x-amz-id-2", "{host_id}"),
                ])
                .body("cdef"),
        );

        let overrides = raw_object_query(
            "GET",
            &bucket,
            key,
            "response-content-type=text%2Fhtml\
             &response-content-disposition=attachment%3B%20filename%3D%22override.txt%22",
        );
        let override_captures = assert_shape(
            "GetObject response overrides",
            &overrides,
            &shape()
                .status(200)
                .headers([
                    ("etag", "{etag}"),
                    ("last-modified", "{http_date}"),
                    ("accept-ranges", "bytes"),
                    ("content-type", "text/html"),
                    (
                        "content-disposition",
                        "attachment; filename=\"override.txt\"",
                    ),
                    ("x-amz-server-side-encryption", "AES256"),
                    ("content-length", "10"),
                    ("x-amz-request-id", "{request_id}"),
                    ("x-amz-id-2", "{host_id}"),
                ])
                .body("abcdefghij"),
        );
        assert_eq!(range_captures["etag"], override_captures["etag"]);

        s3_tests::delete_object_retrying_operation_aborted(client, &bucket, key)
            .await
            .expect("delete shape fixture");
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

/// Full error shape for a plain GET whose range starts past the end of the
/// object: 416 `InvalidRange` echoing the requested range and object size.
#[test]
fn test_get_object_invalid_range_error_shape() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "range-416.txt";

        let put = raw_object_with("PUT", &bucket, key, b"abcdefghij", &[]);
        assert_eq!(put.status, 200, "range fixture PUT failed: {put:?}");

        let response = raw_object_with("GET", &bucket, key, b"", &[("Range", "bytes=5000-6000")]);
        assert_shape(
            "GetObject unsatisfiable range",
            &response,
            &shape()
                .status(416)
                .headers(error_response_headers())
                .body(expected_error::invalid_range("bytes=5000-6000", 10)),
        );

        cleanup(&bucket, &[key]).await;
    });
}
