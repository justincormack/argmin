use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use ring::{digest, hmac};
use s3_tests::{create_public_bucket, sse_c_header_values, test_sse_c_key, unique_bucket, CTX};

/// Create a bucket, returning its name.
async fn setup_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    client.create_bucket().bucket(&bucket).send().await.unwrap();
    bucket
}

/// Build an agent that returns all HTTP responses (including 4xx/5xx) as Ok.
fn agent() -> ureq::Agent {
    s3_tests::test_agent()
}

fn endpoint_is_https() -> bool {
    CTX.endpoint().starts_with("https://")
}

fn require_https_endpoint() {
    assert!(
        endpoint_is_https(),
        "presigned SSE-C coverage requires an https:// endpoint; got {}",
        CTX.endpoint()
    );
}

macro_rules! with_presigned_headers {
    ($req:expr, $presigned:expr) => {{
        let mut req = $req;
        for (name, value) in $presigned.headers() {
            req = req.header(name, value);
        }
        req
    }};
}

// ── Manual presigned-URL helpers (for signed-payload tests) ─────────────

fn sha256_hex(data: &[u8]) -> String {
    let d = digest::digest(&digest::SHA256, data);
    d.as_ref().iter().map(|b| format!("{:02x}", b)).collect()
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> hmac::Tag {
    hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), data)
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

fn derive_signing_key(secret: &str, date: &str, region: &str, service: &str) -> hmac::Tag {
    let k_secret = format!("AWS4{}", secret);
    let k_date = hmac_sha256(k_secret.as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(k_date.as_ref(), region.as_bytes());
    let k_service = hmac_sha256(k_region.as_ref(), service.as_bytes());
    hmac_sha256(k_service.as_ref(), b"aws4_request")
}

/// URI-encode a value per AWS SigV4 rules (encode everything except unreserved chars).
fn uri_encode(value: &str) -> String {
    let mut out = String::new();
    for b in value.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => {
                out.push_str(&format!("%{:02X}", b));
            }
        }
    }
    out
}

/// Build a manually-signed presigned PUT URL with the given body hash.
fn presigned_put_url(
    endpoint: &str,
    bucket: &str,
    key: &str,
    body_hash: &str,
    access_key: &str,
    secret_key: &str,
    region: &str,
) -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let days = secs / 86400;
    let (year, month, day) = days_to_ymd(days);
    let time_of_day = secs % 86400;
    let hour = time_of_day / 3600;
    let minute = (time_of_day % 3600) / 60;
    let second = time_of_day % 60;
    let date_long = format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        year, month, day, hour, minute, second
    );
    let date_short = &date_long[..8];

    let path = format!("/{}/{}", bucket, key);
    let credential = format!("{}/{}/{}/s3/aws4_request", access_key, date_short, region);

    // Signed headers: host is always required; include x-amz-content-sha256
    // only when the payload is signed (not UNSIGNED-PAYLOAD).
    let host = endpoint
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    let (signed_headers, canonical_headers_str) = if body_hash == "UNSIGNED-PAYLOAD" {
        ("host".to_string(), format!("host:{}\n", host))
    } else {
        (
            "host;x-amz-content-sha256".to_string(),
            format!("host:{}\nx-amz-content-sha256:{}\n", host, body_hash),
        )
    };

    // Build canonical query string (sorted)
    let mut qs_parts = [
        format!("X-Amz-Algorithm={}", uri_encode("AWS4-HMAC-SHA256")),
        format!("X-Amz-Credential={}", uri_encode(&credential)),
        format!("X-Amz-Date={}", uri_encode(&date_long)),
        "X-Amz-Expires=900".to_string(),
        format!("X-Amz-SignedHeaders={}", uri_encode(&signed_headers)),
    ];
    qs_parts.sort();
    let canonical_qs = qs_parts.join("&");

    let canonical_request = format!(
        "PUT\n{}\n{}\n{}\n{}\n{}",
        path, canonical_qs, canonical_headers_str, signed_headers, body_hash
    );

    let canonical_hash = sha256_hex(canonical_request.as_bytes());
    let scope = format!("{}/{}/s3/aws4_request", date_short, region);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        date_long, scope, canonical_hash
    );

    let signing_key = derive_signing_key(secret_key, date_short, region, "s3");
    let signature = hmac_sha256(signing_key.as_ref(), string_to_sign.as_bytes());
    let sig_hex = hex_encode(signature.as_ref());

    format!(
        "{}{}?{}&X-Amz-Signature={}",
        endpoint, path, canonical_qs, sig_hex
    )
}

/// Convert days since Unix epoch to (year, month, day).
fn days_to_ymd(days: u64) -> (u64, u64, u64) {
    // Algorithm from http://howardhinnant.github.io/date_algorithms.html
    let z = days + 719468;
    let era = z / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

/// Cleanup helper.
async fn cleanup_with_client(client: &aws_sdk_s3::Client, bucket: &str, keys: &[&str]) {
    for key in keys {
        let _ = client.delete_object().bucket(bucket).key(*key).send().await;
    }
    client.delete_bucket().bucket(bucket).send().await.unwrap();
}

async fn cleanup(bucket: &str, keys: &[&str]) {
    cleanup_with_client(CTX.client(), bucket, keys).await;
}

// ── Presigned GET ───────────────────────────────────────────────────────

#[test]
fn test_presigned_get_object() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"presigned get content";

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();

        let presign_config = PresigningConfig::expires_in(Duration::from_secs(900)).unwrap();
        let presigned = client
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .presigned(presign_config)
            .await
            .unwrap();

        let mut resp = agent()
            .get(presigned.uri())
            .call()
            .expect("transport error");
        assert_eq!(resp.status().as_u16(), 200);
        let data = resp.body_mut().read_to_vec().unwrap();
        assert_eq!(&data[..], body);

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_presigned_get_object_nonexistent() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let presign_config = PresigningConfig::expires_in(Duration::from_secs(900)).unwrap();
        let presigned = client
            .get_object()
            .bucket(&bucket)
            .key("no-such-key")
            .presigned(presign_config)
            .await
            .unwrap();

        let mut resp = agent()
            .get(presigned.uri())
            .call()
            .expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(status, 404);

        cleanup(&bucket, &[]).await;
    });
}

// ── Presigned PUT ───────────────────────────────────────────────────────

#[test]
fn test_presigned_put_object() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"presigned put content";

        let presign_config = PresigningConfig::expires_in(Duration::from_secs(900)).unwrap();
        let presigned = client
            .put_object()
            .bucket(&bucket)
            .key("uploaded")
            .presigned(presign_config)
            .await
            .unwrap();

        let mut resp = agent()
            .put(presigned.uri())
            .send(&body[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(status, 200);

        // Verify via normal GET
        let get_resp = client
            .get_object()
            .bucket(&bucket)
            .key("uploaded")
            .send()
            .await
            .unwrap();
        let data = get_resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], body);

        cleanup(&bucket, &["uploaded"]).await;
    });
}

async fn assert_presigned_put_object_with_acl(client: &aws_sdk_s3::Client) {
    use aws_sdk_s3::types::{ObjectOwnership, OwnershipControls, OwnershipControlsRule};

    let bucket = unique_bucket();
    client.create_bucket().bucket(&bucket).send().await.unwrap();
    let ownership = OwnershipControls::builder()
        .rules(
            OwnershipControlsRule::builder()
                .object_ownership(ObjectOwnership::BucketOwnerPreferred)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    client
        .put_bucket_ownership_controls()
        .bucket(&bucket)
        .ownership_controls(ownership)
        .send()
        .await
        .unwrap();
    let body = b"hello world";

    let presign_config = PresigningConfig::expires_in(Duration::from_secs(900)).unwrap();
    let presigned = client
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .acl(aws_sdk_s3::types::ObjectCannedAcl::Private)
        .presigned(presign_config.clone())
        .await
        .unwrap();

    let mut resp = with_presigned_headers!(agent().put(presigned.uri()), presigned)
        .send(&body[..])
        .expect("transport error");
    let status = resp.status().as_u16();
    let response_body = resp.body_mut().read_to_string().unwrap_or_default();
    assert_eq!(
        status, 200,
        "expected 200 for presigned PUT with x-amz-acl, got {} body={}",
        status, response_body
    );

    let get_presigned = client
        .get_object()
        .bucket(&bucket)
        .key("foo")
        .presigned(presign_config)
        .await
        .unwrap();
    let mut get_resp = agent()
        .get(get_presigned.uri())
        .call()
        .expect("transport error");
    assert_eq!(get_resp.status().as_u16(), 200);
    let data = get_resp.body_mut().read_to_vec().unwrap();
    assert_eq!(&data[..], body);

    cleanup_with_client(client, &bucket, &["foo"]).await;
}

#[test]
fn test_presigned_sse_c_put_object() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"presigned sse-c put content";
        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);

        let presign_config = PresigningConfig::expires_in(Duration::from_secs(900)).unwrap();
        let presigned = client
            .put_object()
            .bucket(&bucket)
            .key("uploaded-sse-c")
            .sse_customer_algorithm("AES256")
            .sse_customer_key(key_b64.clone())
            .sse_customer_key_md5(key_md5_b64.clone())
            .presigned(presign_config)
            .await
            .unwrap();

        let mut resp = with_presigned_headers!(agent().put(presigned.uri()), presigned)
            .send(&body[..])
            .expect("transport error");
        assert_eq!(resp.status().as_u16(), 200);
        let alg = resp
            .headers()
            .get("x-amz-server-side-encryption-customer-algorithm")
            .map(|v| v.to_str().unwrap().to_string());
        let key_md5 = resp
            .headers()
            .get("x-amz-server-side-encryption-customer-key-md5")
            .map(|v| v.to_str().unwrap().to_string());
        let _ = resp.body_mut().read_to_string();
        assert_eq!(alg.as_deref(), Some("AES256"));
        assert_eq!(key_md5.as_deref(), Some(key_md5_b64.as_str()));

        let get_resp = client
            .get_object()
            .bucket(&bucket)
            .key("uploaded-sse-c")
            .sse_customer_algorithm("AES256")
            .sse_customer_key(key_b64.clone())
            .sse_customer_key_md5(key_md5_b64.clone())
            .send()
            .await
            .unwrap();
        let data = get_resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], body);

        cleanup(&bucket, &["uploaded-sse-c"]).await;
    });
}

#[test]
fn test_presigned_put_object_signed_payload() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"presigned put with signed payload";
        let body_hash = sha256_hex(body);

        let url = presigned_put_url(
            CTX.endpoint(),
            &bucket,
            "signed-body",
            &body_hash,
            CTX.access_key(),
            CTX.secret_key(),
            CTX.region(),
        );

        let mut resp = agent()
            .put(&url)
            .header("x-amz-content-sha256", &body_hash)
            .send(&body[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(
            status, 200,
            "expected 200 for signed-payload presigned PUT, got {}",
            status
        );

        // Verify via normal GET
        let get_resp = client
            .get_object()
            .bucket(&bucket)
            .key("signed-body")
            .send()
            .await
            .unwrap();
        let data = get_resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], body);

        cleanup(&bucket, &["signed-body"]).await;
    });
}

#[test]
fn test_presigned_put_object_signed_payload_mismatch() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let body = b"the body that was signed";
        let body_hash = sha256_hex(body);

        // Sign the URL for this specific body
        let url = presigned_put_url(
            CTX.endpoint(),
            &bucket,
            "signed-body",
            &body_hash,
            CTX.access_key(),
            CTX.secret_key(),
            CTX.region(),
        );

        // Send a different body — signature was for the original body
        let wrong_body = b"different body content";
        let mut resp = agent()
            .put(&url)
            .header("x-amz-content-sha256", &sha256_hex(wrong_body))
            .send(&wrong_body[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(
            status, 403,
            "expected 403 for mismatched body hash, got {}",
            status
        );

        cleanup(&bucket, &[]).await;
    });
}

// ── Presigned DELETE ────────────────────────────────────────────────────

#[test]
fn test_presigned_delete_object() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("todelete")
            .body(ByteStream::from_static(b"bye"))
            .send()
            .await
            .unwrap();

        let presign_config = PresigningConfig::expires_in(Duration::from_secs(900)).unwrap();
        let presigned = client
            .delete_object()
            .bucket(&bucket)
            .key("todelete")
            .presigned(presign_config)
            .await
            .unwrap();

        let mut resp = agent()
            .delete(presigned.uri())
            .call()
            .expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        // AWS DeleteObject returns 204 No Content
        assert_eq!(status, 204, "expected 204, got {}", status);

        // Verify deleted
        let result = client
            .get_object()
            .bucket(&bucket)
            .key("todelete")
            .send()
            .await;
        assert!(result.is_err());

        cleanup(&bucket, &[]).await;
    });
}

// ── Presigned HEAD ──────────────────────────────────────────────────────

#[test]
fn test_presigned_head_object() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"head test"))
            .send()
            .await
            .unwrap();

        let presign_config = PresigningConfig::expires_in(Duration::from_secs(900)).unwrap();
        let presigned = client
            .head_object()
            .bucket(&bucket)
            .key("obj")
            .presigned(presign_config)
            .await
            .unwrap();

        let mut resp = agent()
            .head(presigned.uri())
            .call()
            .expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(status, 200);

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_presigned_sse_c_get_object() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"presigned get sse-c content";
        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);

        client
            .put_object()
            .bucket(&bucket)
            .key("obj-sse-c")
            .sse_customer_algorithm("AES256")
            .sse_customer_key(key_b64.clone())
            .sse_customer_key_md5(key_md5_b64.clone())
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();

        let presign_config = PresigningConfig::expires_in(Duration::from_secs(900)).unwrap();
        let presigned = client
            .get_object()
            .bucket(&bucket)
            .key("obj-sse-c")
            .sse_customer_algorithm("AES256")
            .sse_customer_key(key_b64.clone())
            .sse_customer_key_md5(key_md5_b64.clone())
            .presigned(presign_config)
            .await
            .unwrap();

        let mut resp = with_presigned_headers!(agent().get(presigned.uri()), presigned)
            .call()
            .expect("transport error");
        assert_eq!(resp.status().as_u16(), 200);
        let alg = resp
            .headers()
            .get("x-amz-server-side-encryption-customer-algorithm")
            .map(|v| v.to_str().unwrap().to_string());
        let key_md5 = resp
            .headers()
            .get("x-amz-server-side-encryption-customer-key-md5")
            .map(|v| v.to_str().unwrap().to_string());
        let data = resp.body_mut().read_to_vec().unwrap();
        assert_eq!(alg.as_deref(), Some("AES256"));
        assert_eq!(key_md5.as_deref(), Some(key_md5_b64.as_str()));
        assert_eq!(&data[..], body);

        cleanup(&bucket, &["obj-sse-c"]).await;
    });
}

#[test]
fn test_presigned_sse_c_get_requires_signed_headers() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);

        client
            .put_object()
            .bucket(&bucket)
            .key("obj-sse-c-missing-headers")
            .sse_customer_algorithm("AES256")
            .sse_customer_key(key_b64.clone())
            .sse_customer_key_md5(key_md5_b64.clone())
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let presign_config = PresigningConfig::expires_in(Duration::from_secs(900)).unwrap();
        let presigned = client
            .get_object()
            .bucket(&bucket)
            .key("obj-sse-c-missing-headers")
            .sse_customer_algorithm("AES256")
            .sse_customer_key(key_b64.clone())
            .sse_customer_key_md5(key_md5_b64.clone())
            .presigned(presign_config)
            .await
            .unwrap();

        let mut resp = agent()
            .get(presigned.uri())
            .call()
            .expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(status, 403, "expected 403, got {}", status);

        cleanup(&bucket, &["obj-sse-c-missing-headers"]).await;
    });
}

#[test]
fn test_presigned_sse_c_head_object() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);

        client
            .put_object()
            .bucket(&bucket)
            .key("obj-head-sse-c")
            .sse_customer_algorithm("AES256")
            .sse_customer_key(key_b64.clone())
            .sse_customer_key_md5(key_md5_b64.clone())
            .body(ByteStream::from_static(b"head test"))
            .send()
            .await
            .unwrap();

        let presign_config = PresigningConfig::expires_in(Duration::from_secs(900)).unwrap();
        let presigned = client
            .head_object()
            .bucket(&bucket)
            .key("obj-head-sse-c")
            .sse_customer_algorithm("AES256")
            .sse_customer_key(key_b64.clone())
            .sse_customer_key_md5(key_md5_b64.clone())
            .presigned(presign_config)
            .await
            .unwrap();

        let mut resp = with_presigned_headers!(agent().head(presigned.uri()), presigned)
            .call()
            .expect("transport error");
        assert_eq!(resp.status().as_u16(), 200);
        let alg = resp
            .headers()
            .get("x-amz-server-side-encryption-customer-algorithm")
            .map(|v| v.to_str().unwrap().to_string());
        let key_md5 = resp
            .headers()
            .get("x-amz-server-side-encryption-customer-key-md5")
            .map(|v| v.to_str().unwrap().to_string());
        let _ = resp.body_mut().read_to_string();
        assert_eq!(alg.as_deref(), Some("AES256"));
        assert_eq!(key_md5.as_deref(), Some(key_md5_b64.as_str()));

        cleanup(&bucket, &["obj-head-sse-c"]).await;
    });
}

// ── Expired URL ─────────────────────────────────────────────────────────

#[test]
fn test_presigned_get_expired() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        // Generate a URL that expires in 1 second
        let presign_config = PresigningConfig::expires_in(Duration::from_secs(1)).unwrap();
        let presigned = client
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .presigned(presign_config)
            .await
            .unwrap();

        // Wait for it to expire
        std::thread::sleep(Duration::from_secs(2));

        let mut resp = agent()
            .get(presigned.uri())
            .call()
            .expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(status, 403, "expected 403 for expired URL, got {}", status);

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── Tampered signature ──────────────────────────────────────────────────

#[test]
fn test_presigned_get_bad_signature() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let presign_config = PresigningConfig::expires_in(Duration::from_secs(900)).unwrap();
        let presigned = client
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .presigned(presign_config)
            .await
            .unwrap();

        // Tamper with the signature
        let url = presigned.uri().to_string();
        let tampered = url.replace("X-Amz-Signature=", "X-Amz-Signature=0000000000000000");

        let mut resp = agent().get(&tampered).call().expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(
            status, 403,
            "expected 403 for bad signature, got {}",
            status
        );

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── Tampered query param (key changed) ──────────────────────────────────

#[test]
fn test_presigned_get_tampered_key() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("original")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let presign_config = PresigningConfig::expires_in(Duration::from_secs(900)).unwrap();
        let presigned = client
            .get_object()
            .bucket(&bucket)
            .key("original")
            .presigned(presign_config)
            .await
            .unwrap();

        // Change the key in the URL path from "original" to "different"
        let url = presigned.uri().to_string();
        let tampered = url.replace("/original?", "/different?");

        let mut resp = agent().get(&tampered).call().expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        // Signature covers the path; tampered key → SignatureDoesNotMatch
        assert_eq!(status, 403, "expected 403, got {}", status);

        cleanup(&bucket, &["original"]).await;
    });
}

// ── Wrong HTTP method ───────────────────────────────────────────────────

#[test]
fn test_presigned_wrong_method() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        // Generate a presigned GET URL
        let presign_config = PresigningConfig::expires_in(Duration::from_secs(900)).unwrap();
        let presigned = client
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .presigned(presign_config)
            .await
            .unwrap();

        // Use it with PUT instead of GET — signature was computed for GET
        let mut resp = agent()
            .put(presigned.uri())
            .send(b"overwrite attempt" as &[u8])
            .expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(status, 403, "expected 403 for wrong method, got {}", status);

        // Verify original content unchanged
        let get_resp = client
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send()
            .await
            .unwrap();
        let data = get_resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"data");

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── Missing signature param ─────────────────────────────────────────────

#[test]
fn test_presigned_missing_signature() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let presign_config = PresigningConfig::expires_in(Duration::from_secs(900)).unwrap();
        let presigned = client
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .presigned(presign_config)
            .await
            .unwrap();

        // Remove the X-Amz-Signature parameter
        let url = presigned.uri().to_string();
        let stripped: String = url
            .split('&')
            .filter(|p| !p.contains("X-Amz-Signature"))
            .collect::<Vec<_>>()
            .join("&");

        let mut resp = agent().get(&stripped).call().expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert!(
            status >= 400,
            "expected error for missing signature, got {}",
            status
        );

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── Presigned PUT then GET verifies round-trip ──────────────────────────

#[test]
fn test_presigned_put_get_round_trip() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"round-trip content via presigned URLs";

        // Presigned PUT
        let put_config = PresigningConfig::expires_in(Duration::from_secs(900)).unwrap();
        let put_presigned = client
            .put_object()
            .bucket(&bucket)
            .key("roundtrip")
            .presigned(put_config)
            .await
            .unwrap();

        let mut put_resp = agent()
            .put(put_presigned.uri())
            .send(&body[..])
            .expect("transport error");
        assert_eq!(put_resp.status().as_u16(), 200);
        let _ = put_resp.body_mut().read_to_string();

        // Presigned GET
        let get_config = PresigningConfig::expires_in(Duration::from_secs(900)).unwrap();
        let get_presigned = client
            .get_object()
            .bucket(&bucket)
            .key("roundtrip")
            .presigned(get_config)
            .await
            .unwrap();

        let mut get_resp = agent()
            .get(get_presigned.uri())
            .call()
            .expect("transport error");
        assert_eq!(get_resp.status().as_u16(), 200);
        let data = get_resp.body_mut().read_to_vec().unwrap();
        assert_eq!(&data[..], body);

        cleanup(&bucket, &["roundtrip"]).await;
    });
}

// ── Presigned GET with response overrides ───────────────────────────────

#[test]
fn test_presigned_get_response_content_type() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let presign_config = PresigningConfig::expires_in(Duration::from_secs(900)).unwrap();
        let presigned = client
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .response_content_type("application/pdf")
            .presigned(presign_config)
            .await
            .unwrap();

        let mut resp = agent()
            .get(presigned.uri())
            .call()
            .expect("transport error");
        assert_eq!(resp.status().as_u16(), 200);
        let ct = resp
            .headers()
            .get("Content-Type")
            .map(|v| v.to_str().unwrap().to_string());
        let _ = resp.body_mut().read_to_string();
        assert_eq!(ct.as_deref(), Some("application/pdf"));

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── X-Amz-Expires range tests ──────────────────────────────────────────

#[test]
fn test_object_raw_get_x_amz_expires_not_expired() {
    s3_tests::run(async {
        assert_object_raw_get_x_amz_expires_not_expired(CTX.client()).await;
    });
}

#[test]
fn test_object_raw_get_x_amz_expires_out_max_range() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        // Generate a valid presigned URL, then tamper X-Amz-Expires to exceed the
        // 604800-second (7-day) maximum. The SDK rejects >604800 client-side, so
        // we create a legal URL and replace the value.
        let presign_config = PresigningConfig::expires_in(Duration::from_secs(600)).unwrap();
        let presigned = client
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .presigned(presign_config)
            .await
            .unwrap();

        let tampered_url = presigned
            .uri()
            .replace("X-Amz-Expires=600", "X-Amz-Expires=604801");

        let mut resp = agent().get(&tampered_url).call().expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        // Presigned URL validation: expires > 604800 is an auth parameter error → 400
        assert_eq!(
            status, 400,
            "expected 400 for out-of-range expires, got {}",
            status
        );

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_object_raw_get_x_amz_expires_out_positive_range() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        // Manually construct a URL with a negative X-Amz-Expires
        let presign_config = PresigningConfig::expires_in(Duration::from_secs(600)).unwrap();
        let presigned = client
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .presigned(presign_config)
            .await
            .unwrap();

        // Replace the X-Amz-Expires value with a negative number
        let tampered_url = presigned
            .uri()
            .replace("X-Amz-Expires=600", "X-Amz-Expires=-1");

        let mut resp = agent().get(&tampered_url).call().expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        // Negative X-Amz-Expires is an auth parameter error → 400
        assert_eq!(
            status, 400,
            "expected 400 for negative expires, got {}",
            status
        );

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_object_raw_get_x_amz_expires_out_range_zero() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        // Construct a URL with X-Amz-Expires=0
        let presign_config = PresigningConfig::expires_in(Duration::from_secs(600)).unwrap();
        let presigned = client
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .presigned(presign_config)
            .await
            .unwrap();

        let tampered_url = presigned
            .uri()
            .replace("X-Amz-Expires=600", "X-Amz-Expires=0");

        let mut resp = agent().get(&tampered_url).call().expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        // Zero expires means immediately expired → auth expiry
        assert_eq!(status, 403, "expected 403 for zero expires, got {}", status);

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_object_raw_put_authenticated_expired() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        // Generate a presigned PUT URL that's already expired
        let presign_config = PresigningConfig::expires_in(Duration::from_secs(1)).unwrap();
        let presigned = client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .presigned(presign_config)
            .await
            .unwrap();

        // Wait for expiry
        std::thread::sleep(Duration::from_secs(2));

        let mut resp = agent()
            .put(presigned.uri())
            .send(b"data" as &[u8])
            .expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        // Expired presigned URL → auth expiry
        assert_eq!(status, 403, "expected 403 for expired PUT, got {}", status);

        cleanup(&bucket, &[]).await;
    });
}

// ── ACL / Tenant presigned (not implemented) ───────────────────────────

#[test]
fn test_object_presigned_put_object_with_acl() {
    s3_tests::run(async {
        assert_presigned_put_object_with_acl(CTX.client()).await;
    });
}

#[test]
fn test_object_presigned_put_object_with_acl_tenant() {
    s3_tests::run(async {
        assert_presigned_put_object_with_acl(CTX.alt_client()).await;
    });
}

async fn assert_object_raw_get_x_amz_expires_not_expired(client: &aws_sdk_s3::Client) {
    let bucket = create_public_bucket(client).await;
    client
        .put_object()
        .bucket(&bucket)
        .key("obj")
        .acl(aws_sdk_s3::types::ObjectCannedAcl::PublicRead)
        .body(ByteStream::from_static(b"data"))
        .send()
        .await
        .unwrap();

    let presign_config = PresigningConfig::expires_in(Duration::from_secs(600)).unwrap();
    let presigned = client
        .get_object()
        .bucket(&bucket)
        .key("obj")
        .presigned(presign_config)
        .await
        .unwrap();

    let mut options_resp = agent()
        .options(presigned.uri())
        .call()
        .expect("transport error");
    let options_status = options_resp.status().as_u16();
    let _ = options_resp.body_mut().read_to_string();
    assert_eq!(options_status, 400);

    let mut get_resp = agent()
        .get(presigned.uri())
        .call()
        .expect("transport error");
    assert_eq!(get_resp.status().as_u16(), 200);
    let data = get_resp.body_mut().read_to_vec().unwrap();
    assert_eq!(&data[..], b"data");

    cleanup_with_client(client, &bucket, &["obj"]).await;
}

#[test]
fn test_object_raw_get_x_amz_expires_not_expired_tenant() {
    s3_tests::run(async {
        assert_object_raw_get_x_amz_expires_not_expired(CTX.alt_client()).await;
    });
}
