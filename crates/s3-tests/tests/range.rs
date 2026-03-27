use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::ObjectCannedAcl;
use s3_tests::{err_status, unique_bucket, CTX};

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
    let client = CTX.client();
    let bucket = s3_tests::create_public_bucket(client).await;
    let key = "range-test";
    let body = b"abcdefghijklmnopqrstuvwxyz"; // 26 bytes
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .acl(ObjectCannedAcl::PublicRead)
        .body(ByteStream::from_static(body))
        .send()
        .await
        .unwrap();
    (bucket, key.to_string())
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
