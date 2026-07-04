use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::ObjectCannedAcl;
use s3_tests::CTX;

fn agent() -> s3_tests::Agent {
    s3_tests::test_agent()
}

fn assert_invalid_range_body_shape(body: &str, actual_object_size: u64) {
    assert!(body.contains("<Code>InvalidRange</Code>"), "body: {body}");
    assert!(
        body.contains("<Message>The requested range is not satisfiable</Message>"),
        "body: {body}"
    );
    assert!(
        body.contains(&format!(
            "<ActualObjectSize>{actual_object_size}</ActualObjectSize>"
        )),
        "body: {body}"
    );
    assert!(
        body.contains("<RequestId>"),
        "expected RequestId in body: {body}"
    );
    assert!(body.contains("<HostId>"), "expected HostId in body: {body}");
    assert!(
        !body.contains("<Resource>"),
        "expected no Resource element in body: {body}"
    );
}

async fn cleanup(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = s3_tests::delete_object_retrying_operation_aborted(client, bucket, key).await;
    }
    s3_tests::delete_bucket_retrying_operation_aborted(client, bucket).await;
}

/// Create a public bucket with a test object for anonymous range requests.
async fn setup_public_object() -> (String, String) {
    let (bucket, key, _etag) = setup_public_object_with_body(b"abcdefghijklmnopqrstuvwxyz").await;
    (bucket, key)
}

async fn setup_public_object_with_body(body: &'static [u8]) -> (String, String, String) {
    let client = CTX.client();
    let bucket = s3_tests::create_public_bucket(client).await;
    let key = "range-test";
    let put = s3_tests::retrying_operation_aborted("put public range object", || {
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .acl(ObjectCannedAcl::PublicRead)
            .body(ByteStream::from_static(body))
            .send()
    })
    .await;
    (bucket, key.to_string(), put.e_tag().unwrap().to_string())
}

#[test]
fn test_range_get_start_end_response_headers() {
    s3_tests::run(async {
        let (bucket, key, _) = setup_public_object_with_body(b"abcdefghijklmnopqrstuvwxyz").await;
        let url = format!("{}/{}/{}", CTX.endpoint(), bucket, key);
        let mut resp = agent()
            .get(&url)
            .header("Range", "bytes=0-4")
            .call()
            .expect("transport error");
        let data = resp.body_mut().read_to_vec().unwrap();
        assert_eq!(resp.status().as_u16(), 206);
        assert_eq!(
            resp.headers()
                .get("Content-Range")
                .map(|h| h.to_str().unwrap()),
            Some("bytes 0-4/26")
        );
        assert_eq!(
            resp.headers()
                .get("Content-Length")
                .map(|h| h.to_str().unwrap()),
            Some("5")
        );
        assert_eq!(
            resp.headers()
                .get("Accept-Ranges")
                .map(|h| h.to_str().unwrap()),
            Some("bytes")
        );
        assert_eq!(&data[..], b"abcde");

        cleanup(&bucket, &[&key]).await;
    });
}

#[test]
fn test_range_get_unsatisfiable_response_headers_and_body() {
    s3_tests::run(async {
        let (bucket, key, _) = setup_public_object_with_body(b"abcdefghijklmnopqrstuvwxyz").await;
        let url = format!("{}/{}/{}", CTX.endpoint(), bucket, key);
        let mut resp = agent()
            .get(&url)
            .header("Range", "bytes=100-200")
            .call()
            .expect("transport error");
        let body = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(resp.status().as_u16(), 416);
        assert!(resp.headers().get("Content-Range").is_none());
        assert_invalid_range_body_shape(&body, 26);

        cleanup(&bucket, &[&key]).await;
    });
}

// ── Malformed range headers (raw HTTP) ────────────────────────────────
//
// AWS S3 ignores malformed Range headers and returns 200 with the full
// object body. The exception is `bytes=-0` which is syntactically valid
// but semantically unsatisfiable (416).

/// Missing "bytes=" prefix — malformed, AWS ignores and returns full object.
#[test]
fn test_range_get_no_bytes_prefix() {
    s3_tests::run(async {
        let (bucket, key) = setup_public_object().await;
        let url = format!("{}/{}/{}", CTX.endpoint(), bucket, key);
        let resp = agent().get(&url).header("Range", "0-10").call().unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        cleanup(&bucket, &[&key]).await;
    });
}

/// Multi-range (comma-separated) — not supported by S3, AWS ignores
/// the header and returns the full object.
#[test]
fn test_range_get_multi_range() {
    s3_tests::run(async {
        let (bucket, key) = setup_public_object().await;
        let url = format!("{}/{}/{}", CTX.endpoint(), bucket, key);
        let resp = agent()
            .get(&url)
            .header("Range", "bytes=0-5, 10-15")
            .call()
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        cleanup(&bucket, &[&key]).await;
    });
}

/// Start > end is malformed — AWS ignores and returns full object.
#[test]
fn test_range_get_start_greater_than_end() {
    s3_tests::run(async {
        let (bucket, key) = setup_public_object().await;
        let url = format!("{}/{}/{}", CTX.endpoint(), bucket, key);
        let resp = agent()
            .get(&url)
            .header("Range", "bytes=10-5")
            .call()
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        cleanup(&bucket, &[&key]).await;
    });
}

/// "bytes=-" with nothing after the dash is malformed — AWS ignores
/// and returns full object.
#[test]
fn test_range_get_empty_range() {
    s3_tests::run(async {
        let (bucket, key) = setup_public_object().await;
        let url = format!("{}/{}/{}", CTX.endpoint(), bucket, key);
        let resp = agent().get(&url).header("Range", "bytes=-").call().unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        cleanup(&bucket, &[&key]).await;
    });
}

/// `bytes=-0` is syntactically valid (parses as suffix requesting the
/// last 0 bytes) but semantically unsatisfiable — AWS returns 416.
#[test]
fn test_range_get_zero_suffix() {
    s3_tests::run(async {
        let (bucket, key) = setup_public_object().await;
        let url = format!("{}/{}/{}", CTX.endpoint(), bucket, key);
        let resp = agent()
            .get(&url)
            .header("Range", "bytes=-0")
            .call()
            .unwrap();
        assert_eq!(resp.status().as_u16(), 416);
        cleanup(&bucket, &[&key]).await;
    });
}

/// Non-numeric start value — malformed, AWS ignores and returns full object.
#[test]
fn test_range_get_non_numeric_start() {
    s3_tests::run(async {
        let (bucket, key) = setup_public_object().await;
        let url = format!("{}/{}/{}", CTX.endpoint(), bucket, key);
        let resp = agent()
            .get(&url)
            .header("Range", "bytes=abc-10")
            .call()
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        cleanup(&bucket, &[&key]).await;
    });
}

/// Non-numeric end value — malformed, AWS ignores and returns full object.
#[test]
fn test_range_get_non_numeric_end() {
    s3_tests::run(async {
        let (bucket, key) = setup_public_object().await;
        let url = format!("{}/{}/{}", CTX.endpoint(), bucket, key);
        let resp = agent()
            .get(&url)
            .header("Range", "bytes=0-xyz")
            .call()
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        cleanup(&bucket, &[&key]).await;
    });
}

/// Non-numeric suffix value — malformed, AWS ignores and returns full object.
#[test]
fn test_range_get_non_numeric_suffix() {
    s3_tests::run(async {
        let (bucket, key) = setup_public_object().await;
        let url = format!("{}/{}/{}", CTX.endpoint(), bucket, key);
        let resp = agent()
            .get(&url)
            .header("Range", "bytes=-abc")
            .call()
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        cleanup(&bucket, &[&key]).await;
    });
}

/// Non-numeric open-ended start — malformed, AWS ignores and returns full object.
#[test]
fn test_range_get_non_numeric_from_start() {
    s3_tests::run(async {
        let (bucket, key) = setup_public_object().await;
        let url = format!("{}/{}/{}", CTX.endpoint(), bucket, key);
        let resp = agent()
            .get(&url)
            .header("Range", "bytes=abc-")
            .call()
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        cleanup(&bucket, &[&key]).await;
    });
}

/// Missing dash separator — malformed, AWS ignores and returns full object.
#[test]
fn test_range_get_no_dash() {
    s3_tests::run(async {
        let (bucket, key) = setup_public_object().await;
        let url = format!("{}/{}/{}", CTX.endpoint(), bucket, key);
        let resp = agent()
            .get(&url)
            .header("Range", "bytes=100")
            .call()
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        cleanup(&bucket, &[&key]).await;
    });
}

#[test]
fn test_range_get_malformed_header_returns_full_body_without_content_range() {
    s3_tests::run(async {
        let (bucket, key, _) = setup_public_object_with_body(b"abcdefghijklmnopqrstuvwxyz").await;
        let url = format!("{}/{}/{}", CTX.endpoint(), bucket, key);
        let mut resp = agent()
            .get(&url)
            .header("Range", "bytes=10-5")
            .call()
            .expect("transport error");
        let data = resp.body_mut().read_to_vec().unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        assert!(resp.headers().get("Content-Range").is_none());
        assert_eq!(
            resp.headers()
                .get("Accept-Ranges")
                .map(|h| h.to_str().unwrap()),
            Some("bytes")
        );
        assert_eq!(&data[..], b"abcdefghijklmnopqrstuvwxyz");

        cleanup(&bucket, &[&key]).await;
    });
}

#[test]
fn test_range_get_empty_object_is_unsatisfiable() {
    s3_tests::run(async {
        let (bucket, key, _) = setup_public_object_with_body(b"").await;
        let url = format!("{}/{}/{}", CTX.endpoint(), bucket, key);
        let mut resp = agent()
            .get(&url)
            .header("Range", "bytes=0-0")
            .call()
            .expect("transport error");
        let body = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(resp.status().as_u16(), 416);
        assert!(resp.headers().get("Content-Range").is_none());
        assert_invalid_range_body_shape(&body, 0);

        cleanup(&bucket, &[&key]).await;
    });
}
