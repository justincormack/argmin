use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketVersioningStatus, ObjectCannedAcl, VersioningConfiguration};
use s3_tests::{cleanup_versioned_bucket, err_status, unique_bucket, CTX};

async fn setup_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    client.create_bucket().bucket(&bucket).send().await.unwrap();
    bucket
}

async fn cleanup(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = client.delete_object().bucket(bucket).key(*key).send().await;
    }
    client.delete_bucket().bucket(bucket).send().await.unwrap();
}

fn agent() -> ureq::Agent {
    s3_tests::test_agent()
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
    let put = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .acl(ObjectCannedAcl::PublicRead)
        .body(ByteStream::from_static(body))
        .send()
        .await
        .unwrap();
    (bucket, key.to_string(), put.e_tag().unwrap().to_string())
}

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

// ── Valid range variants ──────────────────────────────────────────────

#[test]
fn test_range_get_start_end() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"abcdefghijklmnopqrstuvwxyz";
        client
            .put_object()
            .bucket(&bucket)
            .key("r")
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();

        // bytes=0-4 → "abcde"
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("r")
            .range("bytes=0-4")
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"abcde");

        cleanup(&bucket, &["r"]).await;
    });
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
fn test_range_get_from_start() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"abcdefghijklmnopqrstuvwxyz";
        client
            .put_object()
            .bucket(&bucket)
            .key("r")
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();

        // bytes=23- → "xyz" (last 3 bytes)
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("r")
            .range("bytes=23-")
            .send()
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
        client
            .put_object()
            .bucket(&bucket)
            .key("r")
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();

        // bytes=-3 → last 3 bytes = "xyz"
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("r")
            .range("bytes=-3")
            .send()
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
        client
            .put_object()
            .bucket(&bucket)
            .key("r")
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();

        // bytes=-999 → suffix exceeds size, returns whole object
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("r")
            .range("bytes=-999")
            .send()
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
        client
            .put_object()
            .bucket(&bucket)
            .key("r")
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();

        // bytes=100-200 → start past end of object → 416
        let result = client
            .get_object()
            .bucket(&bucket)
            .key("r")
            .range("bytes=100-200")
            .send()
            .await;
        assert_eq!(err_status(&result), 416);

        cleanup(&bucket, &["r"]).await;
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
        assert!(body.contains("<Code>InvalidRange</Code>"), "body: {body}");

        cleanup(&bucket, &[&key]).await;
    });
}

#[test]
fn test_range_get_from_start_unsatisfiable() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"abcdefghijklmnopqrstuvwxyz"; // 26 bytes
        client
            .put_object()
            .bucket(&bucket)
            .key("r")
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();

        // bytes=100- → start past end → 416
        let result = client
            .get_object()
            .bucket(&bucket)
            .key("r")
            .range("bytes=100-")
            .send()
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
        let put = client
            .put_object()
            .bucket(&bucket)
            .key("r")
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();
        let etag = put.e_tag().unwrap().to_string();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("r")
            .range("bytes=0-4")
            .if_match(&etag)
            .send()
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
        let put = client
            .put_object()
            .bucket(&bucket)
            .key("r")
            .body(ByteStream::from_static(b"abcdefghijklmnopqrstuvwxyz"))
            .send()
            .await
            .unwrap();
        let etag = put.e_tag().unwrap().to_string();

        let result = client
            .get_object()
            .bucket(&bucket)
            .key("r")
            .range("bytes=0-4")
            .if_none_match(&etag)
            .send()
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
        client
            .put_object()
            .bucket(&bucket)
            .key("r")
            .body(ByteStream::from_static(b"abcdefghijklmnopqrstuvwxyz"))
            .send()
            .await
            .unwrap();

        let result = client
            .get_object()
            .bucket(&bucket)
            .key("r")
            .range("bytes=0-4")
            .if_match("\"0000000000000000\"")
            .send()
            .await;
        assert_eq!(err_status(&result), 412);

        cleanup(&bucket, &["r"]).await;
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
        assert!(body.contains("<Code>InvalidRange</Code>"), "body: {body}");

        cleanup(&bucket, &[&key]).await;
    });
}

#[test]
fn test_range_get_specific_version_returns_expected_slice_and_version_id() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;

        let put_v1 = client
            .put_object()
            .bucket(&bucket)
            .key("r")
            .body(ByteStream::from_static(b"abcdefghijklmnopqrstuvwxyz"))
            .send()
            .await
            .unwrap();
        let v1 = put_v1.version_id().unwrap().to_string();

        client
            .put_object()
            .bucket(&bucket)
            .key("r")
            .body(ByteStream::from_static(
                b"0123456789abcdefghijklmnopqrstuvwxyz",
            ))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("r")
            .version_id(&v1)
            .range("bytes=2-5")
            .send()
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
