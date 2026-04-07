use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, ChecksumAlgorithm, ChecksumMode, ChecksumType,
    CompletedMultipartUpload, CompletedPart, ObjectAttributes, VersioningConfiguration,
};
use ring::hmac;
use s3_tests::{
    assert_s3_err_code, cleanup_versioned_bucket, err_status, send_signed_request, unique_bucket,
    CTX,
};
use std::time::{SystemTime, UNIX_EPOCH};

// ── Helpers ─────────────────────────────────────────────────────────

async fn setup_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    bucket
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
        .send()
        .await
        .unwrap();
    bucket
}

async fn cleanup(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = client.delete_object().bucket(bucket).key(*key).send().await;
    }
    client.delete_bucket().bucket(bucket).send().await.unwrap();
}

fn response_header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(header_name, _)| header_name.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

fn assert_error_code(body: &str, code: &str) {
    let expected = format!("<Code>{}</Code>", code);
    assert!(
        body.contains(&expected),
        "expected {expected} in body: {body}",
    );
}

fn sha256_hex(data: &[u8]) -> String {
    let d = ring::digest::digest(&ring::digest::SHA256, data);
    d.as_ref().iter().map(|b| format!("{b:02x}")).collect()
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let k = hmac::Key::new(hmac::HMAC_SHA256, key);
    hmac::sign(&k, data).as_ref().to_vec()
}

fn format_amz_date(epoch_secs: u64) -> String {
    let days = epoch_secs / 86400;
    let tod = epoch_secs % 86400;
    let (y, m, d) = days_to_date(days as i64);
    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        y,
        m,
        d,
        tod / 3600,
        (tod % 3600) / 60,
        tod % 60,
    )
}

fn days_to_date(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

fn normalize_query(raw: &str) -> String {
    if raw.is_empty() {
        return String::new();
    }
    let mut pairs: Vec<(String, String)> = raw
        .split('&')
        .filter(|s| !s.is_empty())
        .map(|pair| {
            let mut parts = pair.splitn(2, '=');
            let key = parts.next().unwrap_or("").to_string();
            let val = parts.next().unwrap_or("").to_string();
            (key, val)
        })
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn send_signed_post(url_str: &str, body: &[u8], extra_headers: &[(&str, &str)]) -> (u16, String) {
    let agent = s3_tests::test_agent();
    let parsed = url::Url::parse(url_str).expect("parse URL");
    let path = parsed.path();
    let query = normalize_query(parsed.query().unwrap_or(""));

    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let dt = format_amz_date(secs);
    let date_stamp = &dt[..8];

    let access_key = CTX.access_key();
    let secret_key = CTX.secret_key();
    let region = CTX.region();
    let service = "s3";

    let host = parsed
        .host_str()
        .map(|h| {
            if let Some(port) = parsed.port() {
                format!("{h}:{port}")
            } else {
                h.to_string()
            }
        })
        .unwrap();

    let payload_hash = sha256_hex(body);

    let mut header_map: Vec<(String, String)> = vec![
        ("host".to_string(), host),
        ("x-amz-content-sha256".to_string(), payload_hash.clone()),
        ("x-amz-date".to_string(), dt.clone()),
    ];
    for &(k, v) in extra_headers {
        header_map.push((k.to_lowercase(), v.to_string()));
    }
    header_map.sort_by(|a, b| a.0.cmp(&b.0));

    let signed_headers = header_map
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical_headers: String = header_map
        .iter()
        .map(|(k, v)| format!("{k}:{v}\n"))
        .collect();
    let canonical_request =
        format!("POST\n{path}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}");

    let cr_hash = sha256_hex(canonical_request.as_bytes());
    let scope = format!("{date_stamp}/{region}/{service}/aws4_request");
    let string_to_sign = format!("AWS4-HMAC-SHA256\n{dt}\n{scope}\n{cr_hash}");

    let k_date = hmac_sha256(
        format!("AWS4{secret_key}").as_bytes(),
        date_stamp.as_bytes(),
    );
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    let signature: String = hmac_sha256(&k_signing, string_to_sign.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();

    let auth_header = format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{scope}, \
         SignedHeaders={signed_headers}, Signature={signature}"
    );

    let mut request = agent
        .post(url_str)
        .header("Authorization", &auth_header)
        .header("x-amz-date", &dt)
        .header("x-amz-content-sha256", &payload_hash);

    for &(k, v) in extra_headers {
        request = request.header(k, v);
    }

    let mut resp = request.send(body).expect("transport error");
    let status = resp.status().as_u16();
    let body_text = resp.body_mut().read_to_string().unwrap_or_default();
    (status, body_text)
}

/// 1024 bytes of 'A'.
fn body_1k() -> Vec<u8> {
    vec![b'A'; 1024]
}

const PART_SIZE: usize = 5 * 1024 * 1024;

/// SHA-256 of 1024 × 'A', base64-encoded.
/// Precomputed: sha256(b'A' * 1024) = 6ab7bc...
const SHA256_1K_A: &str = "arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0=";

/// CRC-64/NVME of 1024 × 'A', base64-encoded.
const CRC64NVME_1K_A: &str = "Qeh8oXvGiSo=";

/// MD5 of 1024 × 'A', base64-encoded.
const MD5_1K_A: &str = "1HsSe8LeLWh93ILaw1TEFQ==";

// ── test_object_checksum_sha256 ─────────────────────────────────────

#[test]
fn test_object_checksum_sha256() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "myobj";

        // PUT with valid SHA-256 checksum
        let resp = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(body_1k()))
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .checksum_sha256(SHA256_1K_A)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.checksum_sha256(), Some(SHA256_1K_A));

        // HEAD without ChecksumMode should NOT return the checksum
        let resp = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert!(
            resp.checksum_sha256().is_none(),
            "expected no checksum on plain HEAD"
        );

        // HEAD with ChecksumMode=ENABLED should return the checksum
        let resp = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.checksum_sha256(), Some(SHA256_1K_A));

        // PUT with bad checksum should fail with 400 BadDigest
        let result = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(body_1k()))
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .checksum_sha256("bad")
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidRequest");

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_object_content_md5() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "myobj";

        let resp = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(body_1k()))
            .content_md5(MD5_1K_A)
            .send()
            .await
            .unwrap();
        assert!(!resp.e_tag().unwrap_or_default().is_empty());

        let result = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(body_1k()))
            .content_md5("AAAAAAAAAAAAAAAAAAAAAA==")
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "BadDigest");

        let result = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(body_1k()))
            .content_md5("bad")
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidDigest");

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_delete_objects_content_md5_required() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "delete-md5-required";

        CTX.client()
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(body_1k()))
            .send()
            .await
            .unwrap();

        let url = format!("{}/{}?delete", CTX.endpoint(), bucket);
        let body = format!("<Delete><Object><Key>{key}</Key></Object></Delete>");
        let (status, body_text) = send_signed_post(&url, body.as_bytes(), &[]);
        assert_eq!(status, 400, "body: {body_text}");
        assert_error_code(&body_text, "InvalidRequest");

        cleanup(&bucket, &[key]).await;
    });
}

// ── test_object_checksum_crc32 ──────────────────────────────────────

/// CRC-32 of 1024 × 'A', base64-encoded.
const CRC32_1K_A: &str = "tzf7Gg==";

#[test]
fn test_object_checksum_crc32() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "myobj";

        // PUT with valid CRC-32 checksum
        let resp = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(body_1k()))
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .checksum_crc32(CRC32_1K_A)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.checksum_crc32(), Some(CRC32_1K_A));

        // GET with ChecksumMode should return the checksum
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.checksum_crc32(), Some(CRC32_1K_A));

        // PUT with bad checksum should fail
        let result = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(body_1k()))
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .checksum_crc32("AAAA/w==")
            .send()
            .await;
        assert_eq!(err_status(&result), 400);

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_upload_part_content_md5() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-md5";
        let body = body_1k();

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let part = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from(body.clone()))
            .content_md5(MD5_1K_A)
            .send()
            .await
            .unwrap();
        let part_etag = part.e_tag().unwrap().to_string();

        let completed = CompletedMultipartUpload::builder()
            .parts(
                CompletedPart::builder()
                    .part_number(1)
                    .e_tag(part_etag)
                    .build(),
            )
            .build();
        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(completed)
            .send()
            .await
            .unwrap();

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("multipart-md5-bad")
            .send()
            .await
            .unwrap();
        let bad_upload_id = create.upload_id().unwrap().to_string();

        let result = client
            .upload_part()
            .bucket(&bucket)
            .key("multipart-md5-bad")
            .upload_id(&bad_upload_id)
            .part_number(1)
            .body(ByteStream::from(body.clone()))
            .content_md5("AAAAAAAAAAAAAAAAAAAAAA==")
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "BadDigest");

        let result = client
            .upload_part()
            .bucket(&bucket)
            .key("multipart-md5-bad")
            .upload_id(&bad_upload_id)
            .part_number(1)
            .body(ByteStream::from(body))
            .content_md5("bad")
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidDigest");

        let _ = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("multipart-md5-bad")
            .upload_id(&bad_upload_id)
            .send()
            .await;

        cleanup(&bucket, &[key]).await;
    });
}

// ── test_object_checksum_crc32c ─────────────────────────────────────

#[test]
fn test_object_checksum_crc32c() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "myobj";

        // PUT with SDK-computed CRC-32C checksum (SDK computes when only
        // checksum_algorithm is set without an explicit value).
        let resp = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(body_1k()))
            .checksum_algorithm(ChecksumAlgorithm::Crc32C)
            .send()
            .await
            .unwrap();
        let crc32c_val = resp.checksum_crc32_c().unwrap();

        // GET with ChecksumMode should return the same checksum
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.checksum_crc32_c(), Some(crc32c_val));

        cleanup(&bucket, &[key]).await;
    });
}

// ── test_object_checksum_sha1 ───────────────────────────────────────

/// SHA-1 of 1024 × 'A', base64-encoded.
const SHA1_1K_A: &str = "dGw/TShsUx4GXor3bgrAhogxxrQ=";

#[test]
fn test_object_checksum_sha1() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "myobj";

        // PUT with valid SHA-1 checksum
        let resp = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(body_1k()))
            .checksum_algorithm(ChecksumAlgorithm::Sha1)
            .checksum_sha1(SHA1_1K_A)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.checksum_sha1(), Some(SHA1_1K_A));

        // GET with ChecksumMode should return the checksum
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.checksum_sha1(), Some(SHA1_1K_A));

        // PUT with bad checksum should fail
        let result = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(body_1k()))
            .checksum_algorithm(ChecksumAlgorithm::Sha1)
            .checksum_sha1("bad")
            .send()
            .await;
        assert_eq!(err_status(&result), 400);

        cleanup(&bucket, &[key]).await;
    });
}

// ── test_object_checksum_crc64nvme ──────────────────────────────────

#[test]
fn test_object_checksum_crc64nvme() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "myobj";

        // PUT with valid CRC-64/NVME checksum
        let resp = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(body_1k()))
            .checksum_algorithm(ChecksumAlgorithm::Crc64Nvme)
            .checksum_crc64_nvme(CRC64NVME_1K_A)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.checksum_crc64_nvme(), Some(CRC64NVME_1K_A));

        // HEAD without ChecksumMode should NOT return the checksum
        let resp = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert!(
            resp.checksum_crc64_nvme().is_none(),
            "expected no checksum on plain HEAD"
        );

        // HEAD with ChecksumMode=ENABLED should return the checksum
        let resp = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.checksum_crc64_nvme(), Some(CRC64NVME_1K_A));

        // PUT with bad checksum should fail with 400 BadDigest
        let result = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(body_1k()))
            .checksum_algorithm(ChecksumAlgorithm::Crc64Nvme)
            .checksum_crc64_nvme("bad")
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidRequest");

        cleanup(&bucket, &[key]).await;
    });
}

// ── test_post_object_upload_checksum — already in post_object.rs ────

// ── Multipart checksum tests ─────────────────────────────────────────

/// Helper: 3-part multipart upload with checksums.
///
/// Creates a multipart upload with the given checksum algorithm and type, uploads
/// 3 parts with pre-computed checksums, completes with the composite/combined
/// checksum, then verifies: CompleteMultipartUpload response, HeadObject with
/// ChecksumMode=ENABLED, and GetObjectAttributes Checksum.
///
/// Mirrors the Ceph `multipart_checksum_3parts_helper`.
struct MultipartChecksumTestCase {
    algo: ChecksumAlgorithm,
    cksum_type: ChecksumType,
    part1_cksum: &'static str,
    part2_cksum: &'static str,
    part3_cksum: &'static str,
    composite_cksum: &'static str,
}

/// Extract the checksum value from a response by algorithm.
fn get_cksum_from_complete(
    resp: &aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput,
    algo: &ChecksumAlgorithm,
) -> Option<String> {
    match algo {
        a if *a == ChecksumAlgorithm::Sha256 => resp.checksum_sha256().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Sha1 => resp.checksum_sha1().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Crc32 => resp.checksum_crc32().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Crc32C => resp.checksum_crc32_c().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Crc64Nvme => {
            resp.checksum_crc64_nvme().map(|s| s.to_string())
        }
        _ => None,
    }
}

fn get_cksum_from_head(
    resp: &aws_sdk_s3::operation::head_object::HeadObjectOutput,
    algo: &ChecksumAlgorithm,
) -> Option<String> {
    match algo {
        a if *a == ChecksumAlgorithm::Sha256 => resp.checksum_sha256().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Sha1 => resp.checksum_sha1().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Crc32 => resp.checksum_crc32().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Crc32C => resp.checksum_crc32_c().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Crc64Nvme => {
            resp.checksum_crc64_nvme().map(|s| s.to_string())
        }
        _ => None,
    }
}

fn get_cksum_from_checksum(
    cksum: &aws_sdk_s3::types::Checksum,
    algo: &ChecksumAlgorithm,
) -> Option<String> {
    match algo {
        a if *a == ChecksumAlgorithm::Sha256 => cksum.checksum_sha256().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Sha1 => cksum.checksum_sha1().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Crc32 => cksum.checksum_crc32().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Crc32C => cksum.checksum_crc32_c().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Crc64Nvme => {
            cksum.checksum_crc64_nvme().map(|s| s.to_string())
        }
        _ => None,
    }
}

/// Build a CompletedPart with the correct checksum field set for the given algorithm.
fn completed_part_with_checksum(
    etag: &str,
    part_number: i32,
    algo: &ChecksumAlgorithm,
    cksum: &str,
) -> CompletedPart {
    let mut b = CompletedPart::builder()
        .e_tag(etag)
        .part_number(part_number);
    if *algo == ChecksumAlgorithm::Sha256 {
        b = b.checksum_sha256(cksum);
    } else if *algo == ChecksumAlgorithm::Sha1 {
        b = b.checksum_sha1(cksum);
    } else if *algo == ChecksumAlgorithm::Crc32 {
        b = b.checksum_crc32(cksum);
    } else if *algo == ChecksumAlgorithm::Crc32C {
        b = b.checksum_crc32_c(cksum);
    } else if *algo == ChecksumAlgorithm::Crc64Nvme {
        b = b.checksum_crc64_nvme(cksum);
    }
    b.build()
}

/// Set the checksum value on an upload_part builder for the given algorithm.
fn upload_part_with_checksum(
    builder: aws_sdk_s3::operation::upload_part::builders::UploadPartFluentBuilder,
    algo: &ChecksumAlgorithm,
    cksum: &str,
) -> aws_sdk_s3::operation::upload_part::builders::UploadPartFluentBuilder {
    if *algo == ChecksumAlgorithm::Sha256 {
        builder.checksum_sha256(cksum)
    } else if *algo == ChecksumAlgorithm::Sha1 {
        builder.checksum_sha1(cksum)
    } else if *algo == ChecksumAlgorithm::Crc32 {
        builder.checksum_crc32(cksum)
    } else if *algo == ChecksumAlgorithm::Crc32C {
        builder.checksum_crc32_c(cksum)
    } else if *algo == ChecksumAlgorithm::Crc64Nvme {
        builder.checksum_crc64_nvme(cksum)
    } else {
        builder
    }
}

fn get_cksum_from_upload_part(
    resp: &aws_sdk_s3::operation::upload_part::UploadPartOutput,
    algo: &ChecksumAlgorithm,
) -> Option<String> {
    match algo {
        a if *a == ChecksumAlgorithm::Sha256 => resp.checksum_sha256().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Sha1 => resp.checksum_sha1().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Crc32 => resp.checksum_crc32().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Crc32C => resp.checksum_crc32_c().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Crc64Nvme => {
            resp.checksum_crc64_nvme().map(|s| s.to_string())
        }
        _ => None,
    }
}

async fn run_multipart_checksum_test(tc: &MultipartChecksumTestCase) {
    let client = CTX.client();
    let bucket = setup_bucket().await;
    let key = "mymultipart3";

    // CreateMultipartUpload with checksum algorithm + type
    let create = client
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .checksum_algorithm(tc.algo.clone())
        .checksum_type(tc.cksum_type.clone())
        .send()
        .await
        .unwrap();
    let upload_id = create.upload_id().unwrap();

    let parts_data: [(&[u8], &str); 3] = [
        (&[b'A'; PART_SIZE][..], tc.part1_cksum),
        (&[b'B'; PART_SIZE][..], tc.part2_cksum),
        (&[b'C'; PART_SIZE][..], tc.part3_cksum),
    ];

    let mut completed_parts = Vec::new();
    for (i, (data, cksum)) in parts_data.iter().enumerate() {
        let part_number = (i + 1) as i32;
        let builder = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(ByteStream::from(data.to_vec()))
            .checksum_algorithm(tc.algo.clone());
        let builder = upload_part_with_checksum(builder, &tc.algo, cksum);
        let resp = builder.send().await.unwrap();
        let returned_cksum = get_cksum_from_upload_part(&resp, &tc.algo)
            .expect("upload_part should return checksum");
        assert_eq!(returned_cksum, *cksum, "upload_part checksum mismatch");
        completed_parts.push(completed_part_with_checksum(
            resp.e_tag().unwrap(),
            part_number,
            &tc.algo,
            &returned_cksum,
        ));
    }

    // CompleteMultipartUpload with composite/combined checksum
    let complete_resp = client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(completed_parts))
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Verify response contains the checksum
    let resp_cksum = get_cksum_from_complete(&complete_resp, &tc.algo)
        .expect("complete response should contain checksum");
    assert_eq!(resp_cksum, tc.composite_cksum, "complete checksum mismatch");

    // HeadObject without ChecksumMode should NOT return the checksum
    let head_resp = client
        .head_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    assert!(
        get_cksum_from_head(&head_resp, &tc.algo).is_none(),
        "expected no checksum on plain HEAD"
    );

    // HeadObject with ChecksumMode=ENABLED should return the checksum + type
    let head_resp = client
        .head_object()
        .bucket(&bucket)
        .key(key)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .unwrap();
    let head_cksum =
        get_cksum_from_head(&head_resp, &tc.algo).expect("HEAD ENABLED should return checksum");
    assert_eq!(head_cksum, tc.composite_cksum, "HEAD checksum mismatch");
    assert_eq!(
        head_resp.checksum_type(),
        Some(&tc.cksum_type),
        "HEAD checksum type mismatch"
    );

    // GetObjectAttributes Checksum should include the checksum + type
    let attr_resp = client
        .get_object_attributes()
        .bucket(&bucket)
        .key(key)
        .object_attributes(ObjectAttributes::Checksum)
        .send()
        .await
        .unwrap();
    let cksum_info = attr_resp.checksum().expect("expected Checksum in response");
    let attr_cksum = get_cksum_from_checksum(cksum_info, &tc.algo)
        .expect("GetObjectAttributes should return checksum");
    // GetObjectAttributes returns the bare hash without the composite "-N" suffix;
    // the part count is conveyed by ChecksumType instead.
    let expected_bare = tc
        .composite_cksum
        .rfind('-')
        .and_then(|pos| {
            if tc.composite_cksum[pos + 1..]
                .bytes()
                .all(|b| b.is_ascii_digit())
                && !tc.composite_cksum[pos + 1..].is_empty()
            {
                Some(&tc.composite_cksum[..pos])
            } else {
                None
            }
        })
        .unwrap_or(tc.composite_cksum);
    assert_eq!(
        attr_cksum, expected_bare,
        "GetObjectAttributes checksum mismatch"
    );
    assert_eq!(
        cksum_info.checksum_type(),
        Some(&tc.cksum_type),
        "GetObjectAttributes checksum type mismatch"
    );

    cleanup(&bucket, &[key]).await;
}

#[test]
fn test_upload_part_uses_multipart_checksum_algorithm_without_part_checksum_header() {
    s3_tests::run(async {
        use base64::Engine;

        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "mpu-upload-part-implicit-checksum";
        let part_body = b"raw-upload-part-without-checksum-header";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .checksum_algorithm(ChecksumAlgorithm::Crc64Nvme)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let encoded_upload_id: String =
            url::form_urlencoded::byte_serialize(upload_id.as_bytes()).collect();
        let url = format!(
            "{}/{}/{}?partNumber=1&uploadId={encoded_upload_id}",
            CTX.endpoint(),
            bucket,
            key
        );
        let upload_part =
            send_signed_request("PUT", &url, part_body, std::iter::empty::<(&str, &str)>());
        assert_eq!(
            upload_part.status, 200,
            "upload part failed: {}",
            upload_part.body
        );

        let checksum_header = response_header(&upload_part.headers, "x-amz-checksum-crc64nvme")
            .expect("expected UploadPart checksum header");
        let expected_checksum = base64::engine::general_purpose::STANDARD
            .encode(checksum::crc64::checksum(part_body).to_be_bytes());
        assert_eq!(checksum_header, expected_checksum);

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &[]).await;
    });
}

// ── test_multipart_checksum_sha256 ───────────────────────────────────

/// Tests bad checksum rejection and missing part checksum rejection on
/// CompleteMultipartUpload, then a successful COMPOSITE SHA-256 upload.
#[test]
fn test_multipart_checksum_sha256() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        // -- bad object-level checksum rejected --
        let key = "mymultipart";
        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let resp = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(body_1k()))
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .checksum_sha256(SHA256_1K_A)
            .send()
            .await
            .unwrap();

        // CompleteMultipartUpload with wrong checksum should fail
        let result = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .checksum_sha256("bad")
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(resp.e_tag().unwrap())
                            .checksum_sha256(resp.checksum_sha256().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidRequest");

        // -- missing part checksum rejected --
        let key2 = "mymultipart2";
        let create2 = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key2)
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .send()
            .await
            .unwrap();
        let upload_id2 = create2.upload_id().unwrap();

        let resp2 = client
            .upload_part()
            .bucket(&bucket)
            .key(key2)
            .upload_id(upload_id2)
            .part_number(1)
            .body(ByteStream::from(body_1k()))
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .checksum_sha256(SHA256_1K_A)
            .send()
            .await
            .unwrap();

        // CompleteMultipartUpload without per-part checksum should fail.
        let result2 = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key2)
            .upload_id(upload_id2)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(resp2.e_tag().unwrap())
                            // no checksum_sha256 on the part
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await;
        assert_eq!(err_status(&result2), 400);
        assert_s3_err_code(&result2, "InvalidRequest");

        // -- successful COMPOSITE SHA-256 upload --
        let key3 = "mymultipart3";
        let create3 = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key3)
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .send()
            .await
            .unwrap();
        let upload_id3 = create3.upload_id().unwrap();

        let resp3 = client
            .upload_part()
            .bucket(&bucket)
            .key(key3)
            .upload_id(upload_id3)
            .part_number(1)
            .body(ByteStream::from(body_1k()))
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .checksum_sha256(SHA256_1K_A)
            .send()
            .await
            .unwrap();

        // COMPOSITE checksum = sha256(raw_part_checksum)-1
        let composite = "Ok6Cs5b96ux6+MWQkJO7UBT5sKPBeXBLwvj/hK89smg=-1";
        let complete_resp = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key3)
            .upload_id(upload_id3)
            .checksum_sha256(composite)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(resp3.e_tag().unwrap())
                            .checksum_sha256(resp3.checksum_sha256().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(complete_resp.checksum_sha256(), Some(composite));

        // HEAD with ChecksumMode=ENABLED
        let head = client
            .head_object()
            .bucket(&bucket)
            .key(key3)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
            .unwrap();
        assert_eq!(head.checksum_sha256(), Some(composite));

        // Abort the incomplete uploads, then clean up the completed key.
        let _ = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await;
        let _ = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key2)
            .upload_id(upload_id2)
            .send()
            .await;
        cleanup(&bucket, &[key2, key3]).await;
    });
}

// ── Multipart 3-part checksum helper tests ──────────────────────────

/// Pre-computed checksums for 5MB × 'A', 5MB × 'B', 5MB × 'C'.
/// Values from Ceph s3-tests (unittest_rgw_cksum).

#[test]
fn test_multipart_use_cksum_helper_sha256() {
    s3_tests::run(async {
        run_multipart_checksum_test(&MultipartChecksumTestCase {
            algo: ChecksumAlgorithm::Sha256,
            cksum_type: ChecksumType::Composite,
            part1_cksum: "275VF5loJr1YYawit0XSHREhkFXYkkPKGuoK0x9VKxI=",
            part2_cksum: "mrHwOfjTL5Zwfj74F05HOQGLdUb7E5szdCbxgUSq6NM=",
            part3_cksum: "Vw7oB/nKQ5xWb3hNgbyfkvDiivl+U+/Dft48nfJfDow=",
            composite_cksum: "uWBwpe1dxI4Vw8Gf0X9ynOdw/SS6VBzfWm9giiv1sf4=-3",
        })
        .await;
    });
}

#[test]
fn test_multipart_use_cksum_helper_crc64nvme() {
    s3_tests::run(async {
        run_multipart_checksum_test(&MultipartChecksumTestCase {
            algo: ChecksumAlgorithm::Crc64Nvme,
            cksum_type: ChecksumType::FullObject,
            part1_cksum: "L/E4WYn8v98=",
            part2_cksum: "xW1l19VobYM=",
            part3_cksum: "cK5MnNaWrW4=",
            composite_cksum: "i+6LR0y3eFo=",
        })
        .await;
    });
}

#[test]
fn test_multipart_use_cksum_helper_crc32() {
    s3_tests::run(async {
        run_multipart_checksum_test(&MultipartChecksumTestCase {
            algo: ChecksumAlgorithm::Crc32,
            cksum_type: ChecksumType::FullObject,
            part1_cksum: "JRTCyQ==",
            part2_cksum: "QoZTGg==",
            part3_cksum: "YAgjqw==",
            composite_cksum: "WgDhBQ==",
        })
        .await;
    });
}

#[test]
fn test_multipart_use_cksum_helper_crc32c() {
    s3_tests::run(async {
        run_multipart_checksum_test(&MultipartChecksumTestCase {
            algo: ChecksumAlgorithm::Crc32C,
            cksum_type: ChecksumType::FullObject,
            part1_cksum: "MDaLrw==",
            part2_cksum: "TH4EZg==",
            part3_cksum: "Z7mBIQ==",
            composite_cksum: "xU+Krw==",
        })
        .await;
    });
}

#[test]
fn test_multipart_use_cksum_helper_sha1() {
    s3_tests::run(async {
        run_multipart_checksum_test(&MultipartChecksumTestCase {
            algo: ChecksumAlgorithm::Sha1,
            cksum_type: ChecksumType::Composite,
            part1_cksum: "iIaTCGbm+vdVjNqIMF2S0T7ibMk=",
            part2_cksum: "LS/TJ32bAVKEwRu+sE3X7awh/lk=",
            part3_cksum: "6DDwovUaHwrKNXDMzOGbuvj9kxI=",
            composite_cksum: "sizjvY4eud3MrcHdZM3cQ/ol39o=-3",
        })
        .await;
    });
}

// ── test_get_object_part_with_checksum ────────────────────────────────
// Multipart CRC32 upload, then GET each part individually and verify
// data + per-part checksum header.

#[test]
fn test_get_object_part_with_checksum() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "partnum-cksum";

        // Create multipart upload with CRC32
        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let parts_data: Vec<Vec<u8>> = vec![
            vec![b'X'; PART_SIZE],
            vec![b'Y'; PART_SIZE],
            vec![b'Z'; 1024],
        ];

        // Upload parts with CRC32 checksums
        let mut completed_parts = Vec::new();
        let mut expected_checksums = Vec::new();
        for (i, data) in parts_data.iter().enumerate() {
            let part_number = (i + 1) as i32;
            let crc = checksum_crc32(data);
            let resp = client
                .upload_part()
                .bucket(&bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(part_number)
                .body(ByteStream::from(data.clone()))
                .checksum_algorithm(ChecksumAlgorithm::Crc32)
                .checksum_crc32(&crc)
                .send()
                .await
                .unwrap();
            let returned_crc = resp.checksum_crc32().unwrap().to_string();
            assert_eq!(returned_crc, crc);
            expected_checksums.push(crc.clone());
            completed_parts.push(
                CompletedPart::builder()
                    .e_tag(resp.e_tag().unwrap())
                    .part_number(part_number)
                    .checksum_crc32(&crc)
                    .build(),
            );
        }

        // Complete
        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(completed_parts))
                    .build(),
            )
            .send()
            .await
            .unwrap();

        // GET each part and verify data + checksum (no ChecksumMode needed)
        for (i, data) in parts_data.iter().enumerate() {
            let part_number = (i + 1) as i32;
            let resp = client
                .get_object()
                .bucket(&bucket)
                .key(key)
                .part_number(part_number)
                .send()
                .await
                .unwrap();

            // Verify parts_count
            assert_eq!(
                resp.parts_count(),
                Some(3),
                "parts_count for part {}",
                part_number
            );

            // Extract checksum before consuming body
            let got_crc = resp
                .checksum_crc32()
                .expect("expected per-part CRC32 checksum without ENABLED")
                .to_string();

            // Verify data matches
            let body = resp.body.collect().await.unwrap().into_bytes();
            assert_eq!(
                body.len(),
                data.len(),
                "body length mismatch for part {}",
                part_number
            );
            assert_eq!(
                &body[..],
                &data[..],
                "data mismatch for part {}",
                part_number
            );

            // Verify per-part CRC32 checksum
            assert_eq!(
                got_crc, expected_checksums[i],
                "checksum mismatch for part {}",
                part_number
            );
        }

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_list_object_versions_includes_full_object_checksum_type_for_single_part_put() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "versioned-single-part-checksum";

        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"hi"))
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .send()
            .await
            .unwrap();

        let versions = client
            .list_object_versions()
            .bucket(&bucket)
            .prefix(key)
            .send()
            .await
            .unwrap();
        let version = versions
            .versions()
            .iter()
            .find(|version| version.key() == Some(key))
            .expect("expected uploaded version");

        assert_eq!(
            version.checksum_algorithm(),
            &[ChecksumAlgorithm::Crc32],
            "ListObjectVersions checksum algorithm mismatch"
        );
        assert_eq!(
            version.checksum_type(),
            Some(&ChecksumType::FullObject),
            "ListObjectVersions checksum type mismatch"
        );

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

/// CRC32 of data, base64-encoded.
fn checksum_crc32(data: &[u8]) -> String {
    use base64::Engine;
    let crc = checksum::crc32::checksum(data);
    base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes())
}

// GetObjectAttributes checksum tests moved to object_attributes.rs
