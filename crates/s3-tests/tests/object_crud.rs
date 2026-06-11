use aws_sdk_s3::primitives::ByteStream;
use ring::{digest, hmac};
use s3_tests::{
    assert_s3_err_code, delete_all_and_bucket, err_status, send_signed_request, unique_bucket, CTX,
};
use std::time::{SystemTime, UNIX_EPOCH};

/// Create a bucket, returning its name. Tests are responsible for cleanup.
async fn setup_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    bucket
}

fn agent() -> s3_tests::Agent {
    s3_tests::test_agent()
}

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

fn days_to_ymd(days: u64) -> (u64, u64, u64) {
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

fn host() -> &'static str {
    CTX.endpoint()
        .trim_start_matches("http://")
        .trim_start_matches("https://")
}

struct SignedHeaders {
    authorization: String,
    amz_date: String,
    amz_content_sha256: String,
}

fn signed_put_with_content_encoding(bucket: &str, key: &str, body: &[u8], content_encoding: &str) {
    let path = format!("/{}/{}", bucket, key);
    let body_hash = sha256_hex(body);
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
    let host_val = host();
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let canonical_headers = format!(
        "host:{}\nx-amz-content-sha256:{}\nx-amz-date:{}\n",
        host_val, body_hash, date_long
    );
    let canonical_request = format!(
        "PUT\n{}\n\n{}\n{}\n{}",
        path, canonical_headers, signed_headers, body_hash
    );
    let canonical_hash = sha256_hex(canonical_request.as_bytes());
    let scope = format!("{}/{}/s3/aws4_request", date_short, CTX.region());
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        date_long, scope, canonical_hash
    );
    let signing_key = derive_signing_key(CTX.secret_key(), date_short, CTX.region(), "s3");
    let signature = hmac_sha256(signing_key.as_ref(), string_to_sign.as_bytes());
    let headers = SignedHeaders {
        authorization: format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            CTX.access_key(),
            scope,
            signed_headers,
            hex_encode(signature.as_ref())
        ),
        amz_date: date_long,
        amz_content_sha256: body_hash,
    };
    let url = format!("{}{}", CTX.endpoint(), path);
    let resp = agent()
        .put(&url)
        .header("Authorization", &headers.authorization)
        .header("x-amz-date", &headers.amz_date)
        .header("x-amz-content-sha256", &headers.amz_content_sha256)
        .header("Content-Encoding", content_encoding)
        .send(body)
        .expect("transport error");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "plain signed PUT with Content-Encoding should succeed"
    );
}

fn response_header(headers: &[(String, String)], name: &str) -> Option<String> {
    headers
        .iter()
        .find(|(header_name, _)| header_name.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.clone())
}

fn signed_put_with_header(
    bucket: &str,
    key: &str,
    body: &[u8],
    header_name: &str,
    header_value: &str,
) {
    let url = format!("{}/{bucket}/{key}", CTX.endpoint());
    let response = send_signed_request(
        "PUT",
        &url,
        body,
        vec![(header_name.to_string(), header_value.to_string())],
    );
    assert_eq!(
        response.status, 200,
        "PUT {header_name} should succeed, got {} body: {}",
        response.status, response.body
    );
}

fn signed_head(bucket: &str, key: &str) -> s3_tests::RawResponse {
    let url = format!("{}/{bucket}/{key}", CTX.endpoint());
    send_signed_request("HEAD", &url, b"", Vec::<(String, String)>::new())
}

// ── PutObject / GetObject basic ──────────────────────────────────────

#[test]
fn test_object_write_file() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"hello world";

        client
            .put_object()
            .bucket(&bucket)
            .key("testobj")
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("testobj")
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], body);

        client
            .delete_object()
            .bucket(&bucket)
            .key("testobj")
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_get_object_expected_bucket_owner() {
    s3_tests::run(async {
        let account_id = CTX.account_id().to_string();
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"hello expected owner";

        client
            .put_object()
            .bucket(&bucket)
            .key("testobj")
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("testobj")
            .customize()
            .mutate_request({
                let account_id = account_id.clone();
                move |req| {
                    req.headers_mut()
                        .insert("x-amz-expected-bucket-owner", account_id.clone());
                }
            })
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], body);

        client
            .delete_object()
            .bucket(&bucket)
            .key("testobj")
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_get_object_wrong_expected_bucket_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("testobj")
            .body(ByteStream::from_static(b"hello expected owner"))
            .send()
            .await
            .unwrap();

        let result = client
            .get_object()
            .bucket(&bucket)
            .key("testobj")
            .customize()
            .mutate_request(|req| {
                req.headers_mut()
                    .insert("x-amz-expected-bucket-owner", "000000000000");
            })
            .send()
            .await;
        assert_eq!(err_status(&result), 403);
        assert_s3_err_code(&result, "AccessDenied");

        client
            .delete_object()
            .bucket(&bucket)
            .key("testobj")
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_object_write_empty() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("empty")
            .body(ByteStream::from_static(b""))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("empty")
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert!(data.is_empty());

        client
            .delete_object()
            .bucket(&bucket)
            .key("empty")
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_object_write_overwrite() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"first"))
            .send()
            .await
            .unwrap();

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"second"))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"second");

        client
            .delete_object()
            .bucket(&bucket)
            .key("obj")
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── PutObject returns ETag ───────────────────────────────────────────

#[test]
fn test_object_write_check_etag() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let resp = client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let etag = resp.e_tag().expect("PutObject should return ETag");
        assert!(!etag.is_empty());

        client
            .delete_object()
            .bucket(&bucket)
            .key("obj")
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── GetObject nonexistent ────────────────────────────────────────────

#[test]
fn test_object_read_not_exist() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let result = client
            .get_object()
            .bucket(&bucket)
            .key("no-such-key")
            .send()
            .await;
        assert_eq!(err_status(&result), 404);

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_object_read_nonexistent_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let result = client.get_object().bucket(&bucket).key("key").send().await;
        assert!(result.is_err());
    });
}

// ── HeadObject ───────────────────────────────────────────────────────

#[test]
fn test_object_head_existing() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"head test content";

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();

        let resp = client
            .head_object()
            .bucket(&bucket)
            .key("obj")
            .send()
            .await
            .unwrap();

        assert_eq!(resp.content_length(), Some(body.len() as i64));
        assert!(resp.e_tag().is_some());

        client
            .delete_object()
            .bucket(&bucket)
            .key("obj")
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_object_head_nonexistent() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let result = client
            .head_object()
            .bucket(&bucket)
            .key("no-such-key")
            .send()
            .await;
        assert!(result.is_err());

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── DeleteObject ─────────────────────────────────────────────────────

#[test]
fn test_object_delete_existing() {
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

        client
            .delete_object()
            .bucket(&bucket)
            .key("todelete")
            .send()
            .await
            .unwrap();

        // Verify it's gone
        let result = client
            .get_object()
            .bucket(&bucket)
            .key("todelete")
            .send()
            .await;
        assert!(result.is_err());

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_object_delete_nonexistent() {
    s3_tests::run(async {
        // S3 returns 204 for deleting nonexistent objects (idempotent)
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .delete_object()
            .bucket(&bucket)
            .key("nonexistent")
            .send()
            .await
            .unwrap();

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── Content-Type ─────────────────────────────────────────────────────

#[test]
fn test_object_content_type() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("typed")
            .content_type("text/html")
            .body(ByteStream::from_static(b"<h1>hi</h1>"))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("typed")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_type(), Some("text/html"));

        client
            .delete_object()
            .bucket(&bucket)
            .key("typed")
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_object_default_content_type() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("noct")
            .body(ByteStream::from_static(b"binary"))
            .send()
            .await
            .unwrap();

        let resp = client
            .head_object()
            .bucket(&bucket)
            .key("noct")
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.content_type(),
            Some("application/octet-stream"),
            "unexpected content-type"
        );

        client
            .delete_object()
            .bucket(&bucket)
            .key("noct")
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── User Metadata (x-amz-meta-*) ────────────────────────────────────

#[test]
fn test_object_metadata_round_trip() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("meta")
            .metadata("color", "blue")
            .metadata("size", "42")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let resp = client
            .head_object()
            .bucket(&bucket)
            .key("meta")
            .send()
            .await
            .unwrap();

        let metadata = resp.metadata().unwrap();
        assert_eq!(metadata.get("color").map(|s| s.as_str()), Some("blue"));
        assert_eq!(metadata.get("size").map(|s| s.as_str()), Some("42"));

        client
            .delete_object()
            .bucket(&bucket)
            .key("meta")
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_object_metadata_in_get() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("meta2")
            .metadata("tag", "value")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("meta2")
            .send()
            .await
            .unwrap();

        let metadata = resp.metadata().unwrap();
        assert_eq!(metadata.get("tag").map(|s| s.as_str()), Some("value"));

        client
            .delete_object()
            .bucket(&bucket)
            .key("meta2")
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

/// Empty metadata value should be stored and retrieved as empty string.
#[test]
fn test_object_metadata_empty_value() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("meta-empty")
            .metadata("meta1", "")
            .body(ByteStream::from_static(b"bar"))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("meta-empty")
            .send()
            .await
            .unwrap();

        let metadata = resp.metadata().unwrap();
        assert_eq!(metadata.get("meta1").map(|s| s.as_str()), Some(""));

        client
            .delete_object()
            .bucket(&bucket)
            .key("meta-empty")
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

/// Overwriting metadata with empty value replaces the old value.
#[test]
fn test_object_metadata_overwrite_to_empty() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        // First put with a non-empty metadata value
        client
            .put_object()
            .bucket(&bucket)
            .key("foo")
            .metadata("meta1", "oldmeta")
            .body(ByteStream::from_static(b"bar"))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.metadata().unwrap().get("meta1").map(|s| s.as_str()),
            Some("oldmeta")
        );

        // Overwrite with empty metadata value
        client
            .put_object()
            .bucket(&bucket)
            .key("foo")
            .metadata("meta1", "")
            .body(ByteStream::from_static(b"bar"))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.metadata().unwrap().get("meta1").map(|s| s.as_str()),
            Some("")
        );

        client
            .delete_object()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

/// Re-putting an object without metadata clears all previous metadata.
#[test]
fn test_object_metadata_replaced_on_put() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        // Put with metadata
        client
            .put_object()
            .bucket(&bucket)
            .key("foo")
            .metadata("meta1", "bar")
            .body(ByteStream::from_static(b"bar"))
            .send()
            .await
            .unwrap();

        // Re-put same key without any metadata
        client
            .put_object()
            .bucket(&bucket)
            .key("foo")
            .body(ByteStream::from_static(b"bar"))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();

        // Metadata should be empty (or None)
        let metadata = resp.metadata();
        let is_empty = metadata.is_none() || metadata.unwrap().is_empty();
        assert!(
            is_empty,
            "metadata should be cleared on re-put without metadata"
        );

        client
            .delete_object()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

/// AWS accepts non-ASCII (unicode) metadata values.
#[test]
fn test_object_metadata_unicode_accepted() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let unicode_value = "Hello World\u{e9}"; // "Hello Worldé"
        client
            .put_object()
            .bucket(&bucket)
            .key("unicode-meta")
            .metadata("meta1", unicode_value)
            .body(ByteStream::from_static(b"bar"))
            .send()
            .await
            .unwrap();

        // Verify round-trip: both AWS and our server RFC 2047 encode
        // non-ASCII metadata values in response headers.
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("unicode-meta")
            .send()
            .await
            .unwrap();
        let meta = resp.metadata().unwrap();
        let returned = meta.get("meta1").unwrap();
        assert_eq!(
            returned, "=?UTF-8?Q?Hello_World=C3=83=C2=A9?=",
            "expected RFC 2047 Q-encoded value, got: {}",
            returned
        );

        client
            .delete_object()
            .bucket(&bucket)
            .key("unicode-meta")
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_object_metadata_too_large() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let oversized = "m".repeat(3000);

        let result = client
            .put_object()
            .bucket(&bucket)
            .key("metadata-too-large")
            .metadata("mint-test", oversized)
            .body(ByteStream::from_static(b""))
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "MetadataTooLarge");

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_object_system_metadata_too_large() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let oversized = "d".repeat(3000);

        let result = client
            .put_object()
            .bucket(&bucket)
            .key("system-metadata-too-large")
            .content_disposition(oversized)
            .body(ByteStream::from_static(b""))
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "MetadataTooLarge");

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_put_object_rejects_request_header_section_over_limit() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let url = format!("{}/{bucket}/header-section-too-large", CTX.endpoint());
        let response = send_signed_request(
            "PUT",
            &url,
            b"",
            vec![("x-test-padding".to_string(), "p".repeat(9000))],
        );

        assert_eq!(response.status, 400, "unexpected body: {}", response.body);
        assert!(
            response
                .body
                .contains("<Code>RequestHeaderSectionTooLarge</Code>"),
            "unexpected body: {}",
            response.body
        );

        let keys: Vec<String> = Vec::new();
        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

// ── ETag consistency ─────────────────────────────────────────────────

#[test]
fn test_object_etag_matches_head_and_get() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let put_resp = client
            .put_object()
            .bucket(&bucket)
            .key("etag")
            .body(ByteStream::from_static(b"etag test"))
            .send()
            .await
            .unwrap();
        let put_etag = put_resp.e_tag().unwrap().to_string();

        let head_resp = client
            .head_object()
            .bucket(&bucket)
            .key("etag")
            .send()
            .await
            .unwrap();
        let head_etag = head_resp.e_tag().unwrap().to_string();

        let get_resp = client
            .get_object()
            .bucket(&bucket)
            .key("etag")
            .send()
            .await
            .unwrap();
        let get_etag = get_resp.e_tag().unwrap().to_string();

        assert_eq!(put_etag, head_etag);
        assert_eq!(put_etag, get_etag);

        client
            .delete_object()
            .bucket(&bucket)
            .key("etag")
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── Same content produces same ETag ──────────────────────────────────

#[test]
fn test_object_same_content_same_etag() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"deterministic content";

        let resp1 = client
            .put_object()
            .bucket(&bucket)
            .key("obj1")
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();

        let resp2 = client
            .put_object()
            .bucket(&bucket)
            .key("obj2")
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();

        assert_eq!(resp1.e_tag(), resp2.e_tag());

        client
            .delete_object()
            .bucket(&bucket)
            .key("obj1")
            .send()
            .await
            .unwrap();
        client
            .delete_object()
            .bucket(&bucket)
            .key("obj2")
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── Special key names ────────────────────────────────────────────────

#[test]
fn test_object_key_with_slashes() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("a/b/c/d")
            .body(ByteStream::from_static(b"nested"))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("a/b/c/d")
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"nested");

        client
            .delete_object()
            .bucket(&bucket)
            .key("a/b/c/d")
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_object_key_with_spaces() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("hello world")
            .body(ByteStream::from_static(b"spaces"))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("hello world")
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"spaces");

        client
            .delete_object()
            .bucket(&bucket)
            .key("hello world")
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── Cache-Control ────────────────────────────────────────────────────

#[test]
fn test_object_write_cache_control() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("cached")
            .cache_control("max-age=3600")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let resp = client
            .head_object()
            .bucket(&bucket)
            .key("cached")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.cache_control(), Some("max-age=3600"));

        client
            .delete_object()
            .bucket(&bucket)
            .key("cached")
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_object_system_metadata_headers_round_trip_raw_values() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let cases = [
            ("Cache-Control", "max-age=60,private"),
            ("Content-Disposition", "attachment;filename=\"report.pdf\""),
            ("Content-Encoding", "gzip,br"),
            ("Content-Language", "en-US,fr-CA"),
            ("Content-Type", "text/plain;charset=utf-8"),
            ("Expires", "Mon, 15 Jan 2024 12:30:45 GMT"),
        ];
        let mut keys = Vec::with_capacity(cases.len());

        for (index, (header_name, header_value)) in cases.iter().enumerate() {
            let key = format!("raw-header-roundtrip-{index}");
            signed_put_with_header(&bucket, &key, b"data", header_name, header_value);

            let response = signed_head(&bucket, &key);
            assert_eq!(
                response.status, 200,
                "HEAD for {header_name} should succeed, got body: {}",
                response.body
            );
            assert_eq!(
                response_header(&response.headers, header_name),
                Some((*header_value).to_string()),
                "expected {header_name} to round-trip exactly"
            );

            keys.push(key);
        }

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

// ── Content-Disposition ──────────────────────────────────────────────

#[test]
fn test_object_content_disposition() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("file")
            .content_disposition("attachment; filename=\"report.pdf\"")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let resp = client
            .head_object()
            .bucket(&bucket)
            .key("file")
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.content_disposition(),
            Some("attachment; filename=\"report.pdf\"")
        );

        client
            .delete_object()
            .bucket(&bucket)
            .key("file")
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── Content-Encoding ─────────────────────────────────────────────────

#[test]
fn test_object_content_encoding() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("gzipped")
            .content_encoding("gzip")
            .body(ByteStream::from_static(b"compressed"))
            .send()
            .await
            .unwrap();

        let resp = client
            .head_object()
            .bucket(&bucket)
            .key("gzipped")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_encoding(), Some("gzip"));

        client
            .delete_object()
            .bucket(&bucket)
            .key("gzipped")
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── Content-Language ─────────────────────────────────────────────────

#[test]
fn test_object_content_language() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("french")
            .content_language("fr")
            .body(ByteStream::from_static(b"bonjour"))
            .send()
            .await
            .unwrap();

        let resp = client
            .head_object()
            .bucket(&bucket)
            .key("french")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_language(), Some("fr"));

        client
            .delete_object()
            .bucket(&bucket)
            .key("french")
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── HEAD zero-byte object ───────────────────────────────────────────

#[test]
fn test_object_head_zero_bytes() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("empty")
            .body(ByteStream::from_static(b""))
            .send()
            .await
            .unwrap();

        let resp = client
            .head_object()
            .bucket(&bucket)
            .key("empty")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_length(), Some(0));
        assert!(resp.e_tag().is_some());

        client
            .delete_object()
            .bucket(&bucket)
            .key("empty")
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── Read with unreadable key ────────────────────────────────────────

#[test]
fn test_object_read_unreadable() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "\u{2680}";

        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"data");

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── Expires header ──────────────────────────────────────────────────

#[test]
fn test_object_write_expires() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let expires = aws_sdk_s3::primitives::DateTime::from_secs(4_102_444_800); // 2100-01-01
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .expires(expires)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let resp = client
            .head_object()
            .bucket(&bucket)
            .key("obj")
            .send()
            .await
            .unwrap();
        assert!(resp.expires_string().is_some());

        client
            .delete_object()
            .bucket(&bucket)
            .key("obj")
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── Full lifecycle: write → read → update → read → delete ──────────

#[test]
fn test_object_write_read_update_read_delete() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        // Write
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"v1"))
            .send()
            .await
            .unwrap();

        // Read
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"v1");

        // Update
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"v2"))
            .send()
            .await
            .unwrap();

        // Read again
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"v2");

        // Delete
        client
            .delete_object()
            .bucket(&bucket)
            .key("obj")
            .send()
            .await
            .unwrap();

        // Verify gone
        let result = client.get_object().bucket(&bucket).key("obj").send().await;
        assert!(result.is_err());

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── Write to nonexistent bucket ─────────────────────────────────────

#[test]
fn test_object_write_to_nonexist_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();

        let result = client
            .put_object()
            .bucket(&bucket)
            .key("key")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await;
        assert_eq!(err_status(&result), 404);
    });
}

// ── Content-Encoding aws-chunked stripping ──────────────────────────

/// Port of Ceph test_object_content_encoding_aws_chunked.
/// Plain user-supplied Content-Encoding values are stored verbatim; the
/// transport-only aws-chunked token is stripped only for actual aws-chunked
/// streaming uploads.
#[test]
fn test_object_content_encoding_aws_chunked() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "ce-test";

        // 1. gzip only — returned as-is
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .content_encoding("gzip")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();
        let resp = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_encoding(), Some("gzip"));

        // 2. deflate, gzip — returned as-is
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .content_encoding("deflate, gzip")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();
        let resp = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_encoding(), Some("deflate, gzip"));

        // 3. gzip, aws-chunked — stored as-is for a plain non-streaming PUT.
        signed_put_with_content_encoding(&bucket, key, b"data", "gzip, aws-chunked");
        let resp = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_encoding(), Some("gzip, aws-chunked"));

        // 4. aws-chunked, gzip — stored as-is for a plain non-streaming PUT.
        signed_put_with_content_encoding(&bucket, key, b"data", "aws-chunked, gzip");
        let resp = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_encoding(), Some("aws-chunked, gzip"));

        // 5. aws-chunked only — stored as-is for a plain non-streaming PUT.
        signed_put_with_content_encoding(&bucket, key, b"data", "aws-chunked");
        let resp = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_encoding(), Some("aws-chunked"));

        // Cleanup
        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}
