use s3_tests::{
    raw_anonymous, raw_anonymous_put,
    shape::{assert_shape, error_response_headers, expected_error, shape},
    unique_bucket, CTX,
};

/// Build an agent that returns all HTTP responses (including 4xx/5xx) as Ok.
fn agent() -> s3_tests::Agent {
    s3_tests::test_agent()
}

/// Create a private bucket (default), returning its name.
async fn setup_private_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    bucket
}

/// Cleanup helper.
async fn cleanup(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = s3_tests::delete_object_retrying_operation_aborted(client, bucket, key).await;
    }
    s3_tests::delete_bucket_retrying_operation_aborted(client, bucket).await;
}

// ── Anonymous GET object ────────────────────────────────────────────────
#[test]
fn test_anon_get_object_private_bucket_fail() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_private_bucket().await;

        s3_tests::put_object_retrying_operation_aborted(
            client,
            &bucket,
            "obj",
            b"private data".to_vec(),
        )
        .await;

        let response = raw_anonymous("GET", &bucket, "obj", None);
        assert_shape(
            "anonymous GetObject on private bucket",
            &response,
            &shape().status(403).headers(error_response_headers()).body(
                expected_error::with_host_id("AccessDenied", "Access Denied"),
            ),
        );

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── Anonymous HEAD object ───────────────────────────────────────────────
// ── Anonymous HEAD bucket ───────────────────────────────────────────────
#[test]
fn test_anon_head_bucket_private_fail() {
    s3_tests::run(async {
        let bucket = setup_private_bucket().await;

        let response = raw_anonymous("HEAD", &bucket, "", None);
        assert_shape(
            "anonymous HeadBucket private",
            &response,
            &shape()
                .status(403)
                .headers(error_response_headers())
                .header("x-amz-bucket-region", CTX.region())
                .body_empty(),
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_anon_bucket_surfaces_distinguish_private_and_nonexistent_like_aws() {
    s3_tests::run(async {
        // AWS does not hide bucket existence from anonymous bucket-surface reads:
        // an existing private bucket returns 403 AccessDenied, while a missing
        // bucket returns 404 NoSuchBucket. Keep this paired contract explicit so
        // future "bucket enumeration" reports do not push us away from AWS.
        let private_bucket = setup_private_bucket().await;
        let missing_bucket = unique_bucket();

        // Existing private bucket: 403 revealing the region (AWS attaches
        // x-amz-bucket-region to denied HeadBucket/ListObjects).
        assert_shape(
            "anonymous HeadBucket private",
            &raw_anonymous("HEAD", &private_bucket, "", None),
            &shape()
                .status(403)
                .headers(error_response_headers())
                .header("x-amz-bucket-region", CTX.region())
                .body_empty(),
        );
        assert_shape(
            "anonymous ListObjectsV2 private",
            &raw_anonymous("GET", &private_bucket, "", Some("list-type=2")),
            &shape()
                .status(403)
                .headers(error_response_headers())
                .header("x-amz-bucket-region", CTX.region())
                .body(expected_error::with_host_id(
                    "AccessDenied",
                    "Access Denied",
                )),
        );

        // Missing bucket: 404 with no region header.
        assert_shape(
            "anonymous HeadBucket missing",
            &raw_anonymous("HEAD", &missing_bucket, "", None),
            &shape()
                .status(404)
                .headers(error_response_headers())
                .body_empty(),
        );
        assert_shape(
            "anonymous ListObjectsV2 missing",
            &raw_anonymous("GET", &missing_bucket, "", Some("list-type=2")),
            &shape()
                .status(404)
                .headers(error_response_headers())
                .body(expected_error::no_such_bucket(&missing_bucket)),
        );

        cleanup(&private_bucket, &[]).await;
    });
}
// ── Anonymous ListObjects V1 ────────────────────────────────────────────
#[test]
fn test_anon_list_objects_v1_private_bucket_fail() {
    s3_tests::run(async {
        let bucket = setup_private_bucket().await;

        assert_shape(
            "anonymous ListObjectsV1 private",
            &raw_anonymous("GET", &bucket, "", None),
            &shape()
                .status(403)
                .headers(error_response_headers())
                .header("x-amz-bucket-region", CTX.region())
                .body(expected_error::with_host_id(
                    "AccessDenied",
                    "Access Denied",
                )),
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_anon_list_objects_v1_nonexistent_bucket_returns_404() {
    s3_tests::run(async {
        let bucket = unique_bucket();

        assert_shape(
            "anonymous ListObjectsV1 missing",
            &raw_anonymous("GET", &bucket, "", None),
            &shape()
                .status(404)
                .headers(error_response_headers())
                .body(expected_error::no_such_bucket(&bucket)),
        );
    });
}

// ── Anonymous ListObjects V2 ────────────────────────────────────────────
#[test]
fn test_anon_list_objects_v2_private_bucket_fail() {
    s3_tests::run(async {
        let bucket = setup_private_bucket().await;

        assert_shape(
            "anonymous ListObjectsV2 private",
            &raw_anonymous("GET", &bucket, "", Some("list-type=2")),
            &shape()
                .status(403)
                .headers(error_response_headers())
                .header("x-amz-bucket-region", CTX.region())
                .body(expected_error::with_host_id(
                    "AccessDenied",
                    "Access Denied",
                )),
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_anon_list_objects_v2_nonexistent_bucket_returns_404() {
    s3_tests::run(async {
        let bucket = unique_bucket();

        assert_shape(
            "anonymous ListObjectsV2 missing",
            &raw_anonymous("GET", &bucket, "", Some("list-type=2")),
            &shape()
                .status(404)
                .headers(error_response_headers())
                .body(expected_error::no_such_bucket(&bucket)),
        );
    });
}

// ── Anonymous PUT object (private bucket) ────────────────────────────

#[test]
fn test_object_anon_put() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_private_bucket().await;

        s3_tests::put_object_retrying_operation_aborted(client, &bucket, "foo", Vec::new()).await;

        // Object-scoped denial: no x-amz-bucket-region (AWS attaches it only
        // to denied HeadBucket/ListObjects).
        assert_shape(
            "anonymous PutObject private",
            &raw_anonymous_put(&bucket, "foo", b"foo"),
            &shape().status(403).headers(error_response_headers()).body(
                expected_error::with_host_id("AccessDenied", "Access Denied"),
            ),
        );

        cleanup(&bucket, &["foo"]).await;
    });
}
// ── Anonymous access to non-existent buckets ─────────────────────────
//
// AWS returns 404 NoSuchBucket for anonymous requests to non-existent
// buckets. This matches AWS behavior — bucket name secrecy is not relied
// upon for access control.

#[test]
fn test_anon_get_nonexistent_bucket_returns_404() {
    s3_tests::run(async {
        let url = format!(
            "{}/nonexistent-{}-{}/obj",
            CTX.endpoint(),
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let mut resp = agent().get(&url).call().expect("transport error");
        let status = resp.status().as_u16();
        let body = resp.body_mut().read_to_string().unwrap();
        assert_eq!(
            status, 404,
            "expected 404 for anon GET on nonexistent bucket, got {}",
            status
        );
        assert!(
            body.contains("<Code>NoSuchBucket</Code>"),
            "expected NoSuchBucket in body: {}",
            body
        );
    });
}

#[test]
fn test_anon_head_nonexistent_bucket_returns_404() {
    s3_tests::run(async {
        let url = format!(
            "{}/nonexistent-{}-{}",
            CTX.endpoint(),
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let resp = agent().head(&url).call().expect("transport error");
        assert_eq!(
            resp.status().as_u16(),
            404,
            "expected 404 for anon HEAD on nonexistent bucket, got {}",
            resp.status().as_u16()
        );
    });
}

#[test]
fn test_anon_put_nonexistent_bucket_returns_404() {
    s3_tests::run(async {
        let url = format!(
            "{}/nonexistent-{}-{}/obj",
            CTX.endpoint(),
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let mut resp = agent()
            .put(&url)
            .send(b"data" as &[u8])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body = resp.body_mut().read_to_string().unwrap();
        assert_eq!(
            status, 404,
            "expected 404 for anon PUT on nonexistent bucket, got {}",
            status
        );
        assert!(
            body.contains("<Code>NoSuchBucket</Code>"),
            "expected NoSuchBucket in body: {}",
            body
        );
    });
}
