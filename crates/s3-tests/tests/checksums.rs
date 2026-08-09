use aws_sdk_s3::operation::{RequestId, RequestIdExt};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, ChecksumAlgorithm, ChecksumMode, ChecksumType,
    CompletedMultipartUpload, CompletedPart, ObjectAttributes, VersioningConfiguration,
};
use checksum::ChecksumAlgorithm as LocalChecksumAlgorithm;
use ring::hmac;
use s3_tests::{
    assert_complete_multipart_sdk_error, assert_s3_err_code, cleanup_versioned_bucket, err_status,
    raw_object_with, retrying_operation_aborted, send_signed_request,
    shape::{assert_shape, error_response_headers, expected_error, shape},
    unique_bucket, SendRetryingOperationAborted, CTX,
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
        let _ = s3_tests::delete_object_retrying_operation_aborted(client, bucket, key).await;
    }
    s3_tests::delete_bucket_retrying_operation_aborted(client, bucket).await;
}

async fn assert_checksum_completion_preserved_upload(
    bucket: &str,
    key: &str,
    upload_id: &str,
    expected_etag: &str,
) {
    let parts = CTX
        .client()
        .list_parts()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .send()
        .await
        .unwrap();
    assert_eq!(parts.parts().len(), 1);
    assert_eq!(parts.parts()[0].part_number(), Some(1));
    assert_eq!(parts.parts()[0].e_tag(), Some(expected_etag));

    let object = CTX
        .client()
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await;
    assert_eq!(err_status(&object), 404);
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

fn assert_error_message(body: &str, message: &str) {
    let expected = format!("<Message>{}</Message>", message);
    assert!(
        body.contains(&expected),
        "expected {expected} in body: {body}",
    );
}

fn assert_error_argument_name(body: &str, argument_name: &str) {
    let expected = format!("<ArgumentName>{}</ArgumentName>", argument_name);
    assert!(
        body.contains(&expected),
        "expected {expected} in body: {body}",
    );
}

fn assert_error_argument_value(body: &str, argument_value: &str) {
    let expected = format!("<ArgumentValue>{}</ArgumentValue>", argument_value);
    assert!(
        body.contains(&expected),
        "expected {expected} in body: {body}",
    );
}

fn assert_auth_error_response_shape(body: &str) {
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

fn encode_base64(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn checksum_base64(algo: LocalChecksumAlgorithm, data: &[u8]) -> String {
    encode_base64(checksum::compute_checksum(algo, data).bytes())
}

async fn raw_put_object_with_checksum_headers(
    bucket: &str,
    key: &str,
    body: &[u8],
    headers: &[(&str, String)],
) -> s3_tests::RawResponse {
    let url = s3_tests::object_url(CTX.endpoint(), bucket, key, None);
    send_signed_request(
        "PUT",
        &url,
        body,
        headers.iter().map(|(k, v)| (*k, v.as_str())),
    )
}

fn raw_upload_part_url(bucket: &str, key: &str, upload_id: &str, part_number: u32) -> String {
    let encoded_upload_id: String =
        url::form_urlencoded::byte_serialize(upload_id.as_bytes()).collect();
    format!(
        "{}/{}/{}?partNumber={part_number}&uploadId={encoded_upload_id}",
        CTX.endpoint(),
        bucket,
        key
    )
}

async fn raw_put_object_with_duplicate_signed_checksum_headers(
    bucket: &str,
    key: &str,
    body: &[u8],
    checksum_header: &str,
    checksum_values: &[&str],
) -> s3_tests::RawResponse {
    assert!(checksum_values.len() > 1);
    let url = s3_tests::object_url(CTX.endpoint(), bucket, key, None);
    let parsed = url::Url::parse(&url).expect("parse object URL");
    let path = parsed.path();
    let query = normalize_query(parsed.query().unwrap_or(""));
    let agent = s3_tests::test_agent();

    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let amz_date = format_amz_date(secs);
    let date_stamp = &amz_date[..8];
    let payload_hash = sha256_hex(body);
    let host = parsed
        .host_str()
        .map(|host| {
            if let Some(port) = parsed.port() {
                format!("{host}:{port}")
            } else {
                host.to_string()
            }
        })
        .expect("object URL has host");
    let checksum_header = checksum_header.to_ascii_lowercase();

    let mut canonical_headers = [
        ("host".to_string(), host.clone()),
        ("x-amz-content-sha256".to_string(), payload_hash.clone()),
        ("x-amz-date".to_string(), amz_date.clone()),
        (checksum_header.clone(), checksum_values.join(",")),
    ];
    canonical_headers.sort_by(|a, b| a.0.cmp(&b.0));
    let signed_headers = canonical_headers
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical_headers_text = canonical_headers
        .iter()
        .map(|(name, value)| format!("{name}:{value}\n"))
        .collect::<String>();
    let canonical_request =
        format!("PUT\n{path}\n{query}\n{canonical_headers_text}\n{signed_headers}\n{payload_hash}");
    let credential_scope = format!("{date_stamp}/{}/s3/aws4_request", CTX.region());
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );
    let k_date = hmac_sha256(
        format!("AWS4{}", CTX.secret_key()).as_bytes(),
        date_stamp.as_bytes(),
    );
    let k_region = hmac_sha256(&k_date, CTX.region().as_bytes());
    let k_service = hmac_sha256(&k_region, b"s3");
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    let signature = hmac_sha256(&k_signing, string_to_sign.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}",
        CTX.access_key()
    );

    let mut request = agent
        .put(&url)
        .header("Authorization", &authorization)
        .header("x-amz-date", &amz_date)
        .header("x-amz-content-sha256", &payload_hash);
    for checksum_value in checksum_values {
        request = request.header(&checksum_header, *checksum_value);
    }
    let mut response = request.send(body).expect("transport error");
    let status = response.status().as_u16();
    let headers = response
        .headers()
        .iter()
        .map(|(k, v)| {
            (
                k.as_str().to_string(),
                v.to_str().unwrap_or_default().to_string(),
            )
        })
        .collect();
    let body = response.body_mut().read_to_string().unwrap_or_default();
    s3_tests::RawResponse {
        status,
        headers,
        body,
        body_read_error: None,
    }
}

async fn assert_stored_crc32_full_object_checksum(bucket: &str, key: &str, expected: &str) {
    let client = CTX.client();
    let head = client
        .head_object()
        .bucket(bucket)
        .key(key)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .unwrap();
    assert_eq!(head.checksum_crc32(), Some(expected));
    assert!(
        head.checksum_sha256().is_none(),
        "ignored x-amz-checksum-algorithm must not change stored checksum algorithm"
    );
    assert_eq!(head.checksum_type(), Some(&ChecksumType::FullObject));

    let attrs = client
        .get_object_attributes()
        .bucket(bucket)
        .key(key)
        .object_attributes(ObjectAttributes::Checksum)
        .send()
        .await
        .unwrap();
    let checksum = attrs.checksum().expect("expected Checksum attributes");
    assert_eq!(checksum.checksum_crc32(), Some(expected));
    assert!(
        checksum.checksum_sha256().is_none(),
        "ignored x-amz-checksum-algorithm must not change stored checksum attributes"
    );
    assert_eq!(checksum.checksum_type(), Some(&ChecksumType::FullObject));
}

async fn assert_stored_crc64nvme_full_object_checksum(bucket: &str, key: &str, expected: &str) {
    let client = CTX.client();
    let head = client
        .head_object()
        .bucket(bucket)
        .key(key)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .unwrap();
    assert_eq!(head.checksum_crc64_nvme(), Some(expected));
    assert_eq!(head.checksum_type(), Some(&ChecksumType::FullObject));

    let attrs = client
        .get_object_attributes()
        .bucket(bucket)
        .key(key)
        .object_attributes(ObjectAttributes::Checksum)
        .send()
        .await
        .unwrap();
    let checksum = attrs.checksum().expect("expected Checksum attributes");
    assert_eq!(checksum.checksum_crc64_nvme(), Some(expected));
    assert_eq!(checksum.checksum_type(), Some(&ChecksumType::FullObject));
}

async fn assert_stored_sha256_full_object_checksum(bucket: &str, key: &str, expected: &str) {
    let client = CTX.client();
    let head = client
        .head_object()
        .bucket(bucket)
        .key(key)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .unwrap();
    assert_eq!(head.checksum_sha256(), Some(expected));
    assert_eq!(head.checksum_type(), Some(&ChecksumType::FullObject));

    let attrs = client
        .get_object_attributes()
        .bucket(bucket)
        .key(key)
        .object_attributes(ObjectAttributes::Checksum)
        .send()
        .await
        .unwrap();
    let checksum = attrs.checksum().expect("expected Checksum attributes");
    assert_eq!(checksum.checksum_sha256(), Some(expected));
    assert_eq!(checksum.checksum_type(), Some(&ChecksumType::FullObject));
}

async fn assert_no_stored_sha256_checksum(bucket: &str, key: &str, context: &str) {
    let client = CTX.client();
    let head = client
        .head_object()
        .bucket(bucket)
        .key(key)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .unwrap();
    assert!(
        head.checksum_sha256().is_none(),
        "{context}: concrete or literal SHA256 checksum header must not be stored on HeadObject"
    );

    let attrs = client
        .get_object_attributes()
        .bucket(bucket)
        .key(key)
        .object_attributes(ObjectAttributes::Checksum)
        .send()
        .await
        .unwrap();
    if let Some(checksum) = attrs.checksum() {
        assert!(
            checksum.checksum_sha256().is_none(),
            "{context}: concrete or literal SHA256 checksum header must not be stored on GetObjectAttributes"
        );
    }
}

async fn complete_single_part_upload_without_checksum(
    bucket: &str,
    key: &str,
    upload_id: &str,
    body: Vec<u8>,
) {
    let client = CTX.client();
    let part = client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from(body))
        .send()
        .await
        .unwrap();
    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .e_tag(part.e_tag().unwrap())
                        .part_number(1)
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
}

fn composite_checksum_base64(algo: LocalChecksumAlgorithm, part_checksums: &[String]) -> String {
    use base64::Engine;

    let mut concat = Vec::new();
    for part_checksum in part_checksums {
        let raw = base64::engine::general_purpose::STANDARD
            .decode(part_checksum)
            .unwrap();
        concat.extend_from_slice(&raw);
    }
    format!(
        "{}-{}",
        checksum_base64(algo, &concat),
        part_checksums.len()
    )
}

fn bare_composite_checksum(value: &str) -> &str {
    value
        .rfind('-')
        .and_then(|pos| {
            if value[pos + 1..].bytes().all(|b| b.is_ascii_digit()) && !value[pos + 1..].is_empty()
            {
                Some(&value[..pos])
            } else {
                None
            }
        })
        .unwrap_or(value)
}

fn xml_text<'a>(body: &'a str, tag: &str) -> Option<&'a str> {
    let start_tag = format!("<{tag}>");
    let end_tag = format!("</{tag}>");
    let start = body.find(&start_tag)? + start_tag.len();
    let end = body[start..].find(&end_tag)? + start;
    Some(&body[start..end])
}

fn checksum_xml_element_name(algo: &ChecksumAlgorithm) -> &'static str {
    if *algo == ChecksumAlgorithm::Crc32 {
        "ChecksumCRC32"
    } else if *algo == ChecksumAlgorithm::Crc32C {
        "ChecksumCRC32C"
    } else if *algo == ChecksumAlgorithm::Sha1 {
        "ChecksumSHA1"
    } else if *algo == ChecksumAlgorithm::Sha256 {
        "ChecksumSHA256"
    } else if *algo == ChecksumAlgorithm::Crc64Nvme {
        "ChecksumCRC64NVME"
    } else if *algo == ChecksumAlgorithm::Md5 {
        "ChecksumMD5"
    } else if *algo == ChecksumAlgorithm::Sha512 {
        "ChecksumSHA512"
    } else if *algo == ChecksumAlgorithm::Xxhash64 {
        "ChecksumXXHASH64"
    } else if *algo == ChecksumAlgorithm::Xxhash3 {
        "ChecksumXXHASH3"
    } else if *algo == ChecksumAlgorithm::Xxhash128 {
        "ChecksumXXHASH128"
    } else {
        panic!("unsupported checksum algorithm: {algo:?}")
    }
}

fn checksum_header_name(algo: &ChecksumAlgorithm) -> &'static str {
    if *algo == ChecksumAlgorithm::Crc32 {
        "x-amz-checksum-crc32"
    } else if *algo == ChecksumAlgorithm::Crc32C {
        "x-amz-checksum-crc32c"
    } else if *algo == ChecksumAlgorithm::Sha1 {
        "x-amz-checksum-sha1"
    } else if *algo == ChecksumAlgorithm::Sha256 {
        "x-amz-checksum-sha256"
    } else if *algo == ChecksumAlgorithm::Crc64Nvme {
        "x-amz-checksum-crc64nvme"
    } else if *algo == ChecksumAlgorithm::Md5 {
        "x-amz-checksum-md5"
    } else if *algo == ChecksumAlgorithm::Sha512 {
        "x-amz-checksum-sha512"
    } else if *algo == ChecksumAlgorithm::Xxhash64 {
        "x-amz-checksum-xxhash64"
    } else if *algo == ChecksumAlgorithm::Xxhash3 {
        "x-amz-checksum-xxhash3"
    } else if *algo == ChecksumAlgorithm::Xxhash128 {
        "x-amz-checksum-xxhash128"
    } else {
        panic!("unsupported checksum algorithm: {algo:?}")
    }
}

fn multipart_complete_url(bucket: &str, key: &str, upload_id: &str) -> String {
    let encoded_upload_id: String =
        url::form_urlencoded::byte_serialize(upload_id.as_bytes()).collect();
    format!(
        "{}/{}/{}?uploadId={encoded_upload_id}",
        CTX.endpoint(),
        bucket,
        key
    )
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

const UNSUPPORTED_CHECKSUM_ALGORITHM_MESSAGE: &str = "Checksum algorithm provided is unsupported. Please try again with any of the valid types: [CRC32, CRC32C, CRC64NVME, MD5, SHA1, SHA256, SHA512, XXHASH128, XXHASH3, XXHASH64]";
const SDK_CHECKSUM_MISSING_VALUE_MESSAGE: &str =
    "x-amz-sdk-checksum-algorithm specified, but no corresponding x-amz-checksum-* or x-amz-trailer headers were found.";
const SDK_CHECKSUM_INVALID_VALUE_MESSAGE: &str =
    "Value for x-amz-sdk-checksum-algorithm header is invalid.";

#[test]
fn test_put_object_without_checksum_headers_defaults_crc64nvme() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "put-object-without-checksum-headers-defaults-crc64nvme";
        let body = b"put object default crc64nvme oracle";
        let expected = checksum_base64(LocalChecksumAlgorithm::Crc64nvme, body);
        let response = raw_put_object_with_checksum_headers(&bucket, key, body, &[]).await;
        assert_eq!(response.status, 200, "response: {response:?}");
        assert_eq!(
            response_header(&response.headers, "x-amz-checksum-crc64nvme"),
            Some(expected.as_str())
        );
        assert_eq!(
            response_header(&response.headers, "x-amz-checksum-type"),
            Some("FULL_OBJECT")
        );
        assert_stored_crc64nvme_full_object_checksum(&bucket, key, &expected).await;
        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_complete_multipart_without_checksum_headers_defaults_crc64nvme() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "mpu-without-checksum-headers-defaults-crc64nvme";
        let body = b"multipart default crc64nvme oracle";

        let create_url = s3_tests::object_url(CTX.endpoint(), &bucket, key, Some("uploads"));
        let create =
            send_signed_request("POST", &create_url, &[], std::iter::empty::<(&str, &str)>());
        assert_eq!(create.status, 200, "create response: {create:?}");
        assert_eq!(
            response_header(&create.headers, "x-amz-checksum-algorithm"),
            None
        );
        assert_eq!(
            response_header(&create.headers, "x-amz-checksum-type"),
            None
        );
        let upload_id = xml_text(&create.body, "UploadId").expect("UploadId in response");

        let part_url = raw_upload_part_url(&bucket, key, upload_id, 1);
        let part = send_signed_request("PUT", &part_url, body, std::iter::empty::<(&str, &str)>());
        assert_eq!(part.status, 200, "upload part response: {part:?}");
        let etag = response_header(&part.headers, "etag").expect("UploadPart ETag");

        let complete_url = multipart_complete_url(&bucket, key, upload_id);
        let complete_body = format!(
            "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
        );
        let expected = checksum_base64(LocalChecksumAlgorithm::Crc64nvme, body);
        let (status, response_body) =
            send_signed_post(&complete_url, complete_body.as_bytes(), &[]);
        assert_eq!(status, 200, "complete response body: {response_body}");
        assert!(
            response_body.contains(&format!(
                "<ChecksumCRC64NVME>{expected}</ChecksumCRC64NVME>"
            )),
            "CompleteMultipartUpload response missing default CRC64NVME checksum {expected}: {response_body}"
        );
        assert!(
            response_body.contains("<ChecksumType>FULL_OBJECT</ChecksumType>"),
            "CompleteMultipartUpload response missing FULL_OBJECT checksum type: {response_body}"
        );

        assert_stored_crc64nvme_full_object_checksum(&bucket, key, &expected).await;
        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_put_object_checksum_algorithm_lowercase_value() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "lowercase-checksum-algorithm";
        let body = b"checksum algorithm lowercase oracle";
        let crc32 = checksum_base64(LocalChecksumAlgorithm::Crc32, body);
        let response = raw_put_object_with_checksum_headers(
            &bucket,
            key,
            body,
            &[
                ("x-amz-checksum-algorithm", "crc32".to_string()),
                ("x-amz-checksum-crc32", crc32.clone()),
            ],
        )
        .await;
        assert_eq!(response.status, 200, "response: {response:?}");
        assert_eq!(
            response_header(&response.headers, "x-amz-checksum-crc32"),
            Some("iNxRQw==")
        );
        assert_eq!(
            response_header(&response.headers, "x-amz-checksum-type"),
            Some("FULL_OBJECT")
        );
        assert_stored_crc32_full_object_checksum(&bucket, key, &crc32).await;
        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_put_object_checksum_algorithm_mismatch() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "mismatched-checksum-algorithm";
        let body = b"checksum algorithm mismatch oracle";
        let crc32 = checksum_base64(LocalChecksumAlgorithm::Crc32, body);
        let response = raw_put_object_with_checksum_headers(
            &bucket,
            key,
            body,
            &[
                ("x-amz-checksum-algorithm", "SHA256".to_string()),
                ("x-amz-checksum-crc32", crc32.clone()),
            ],
        )
        .await;
        assert_eq!(response.status, 200, "response: {response:?}");
        assert_eq!(
            response_header(&response.headers, "x-amz-checksum-crc32"),
            Some("bo1dLQ==")
        );
        assert_eq!(
            response_header(&response.headers, "x-amz-checksum-type"),
            Some("FULL_OBJECT")
        );
        assert_stored_crc32_full_object_checksum(&bucket, key, &crc32).await;
        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_put_object_invalid_checksum_algorithm_with_value() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "invalid-checksum-algorithm-with-value";
        let body = b"invalid checksum algorithm with value oracle";
        let crc32 = checksum_base64(LocalChecksumAlgorithm::Crc32, body);
        let response = raw_put_object_with_checksum_headers(
            &bucket,
            key,
            body,
            &[
                ("x-amz-checksum-algorithm", "BOGUS".to_string()),
                ("x-amz-checksum-crc32", crc32.clone()),
            ],
        )
        .await;
        assert_eq!(response.status, 200, "response: {response:?}");
        assert_eq!(
            response_header(&response.headers, "x-amz-checksum-crc32"),
            Some("SN9Ohg==")
        );
        assert_eq!(
            response_header(&response.headers, "x-amz-checksum-type"),
            Some("FULL_OBJECT")
        );
        assert_stored_crc32_full_object_checksum(&bucket, key, &crc32).await;
        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_put_object_invalid_checksum_algorithm_without_value() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "invalid-checksum-algorithm-without-value";
        let body = b"invalid checksum algorithm without value oracle";
        let expected = checksum_base64(LocalChecksumAlgorithm::Crc64nvme, body);
        let response = raw_put_object_with_checksum_headers(
            &bucket,
            key,
            body,
            &[("x-amz-checksum-algorithm", "BOGUS".to_string())],
        )
        .await;
        assert_eq!(response.status, 200, "response: {response:?}");
        assert_eq!(
            response_header(&response.headers, "x-amz-checksum-crc64nvme"),
            Some(expected.as_str())
        );
        assert_eq!(
            response_header(&response.headers, "x-amz-checksum-type"),
            Some("FULL_OBJECT")
        );
        assert_stored_crc64nvme_full_object_checksum(&bucket, key, &expected).await;
        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_put_object_checksum_algorithm_literal_only_not_stored_as_checksum() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "checksum-algorithm-literal-only-not-stored";
        let body = b"checksum algorithm literal only oracle";
        let response = raw_put_object_with_checksum_headers(
            &bucket,
            key,
            body,
            &[("x-amz-checksum-algorithm", "SHA256".to_string())],
        )
        .await;
        assert_eq!(response.status, 200, "response: {response:?}");
        assert_no_stored_sha256_checksum(&bucket, key, "PutObject x-amz-checksum-algorithm").await;
        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_put_object_invalid_checksum_type_without_value_is_ignored() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "invalid-checksum-type-without-value";
        let body = b"invalid checksum type without value oracle";
        let expected = checksum_base64(LocalChecksumAlgorithm::Crc64nvme, body);
        let response = raw_put_object_with_checksum_headers(
            &bucket,
            key,
            body,
            &[("x-amz-checksum-type", "BOGUS".to_string())],
        )
        .await;
        assert_eq!(response.status, 200, "response: {response:?}");
        assert_eq!(
            response_header(&response.headers, "x-amz-checksum-crc64nvme"),
            Some(expected.as_str())
        );
        assert_eq!(
            response_header(&response.headers, "x-amz-checksum-type"),
            Some("FULL_OBJECT")
        );
        assert_stored_crc64nvme_full_object_checksum(&bucket, key, &expected).await;
        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_put_object_invalid_checksum_type_with_value_is_ignored() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "invalid-checksum-type-with-value";
        let body = b"invalid checksum type with value oracle";
        let crc32 = checksum_base64(LocalChecksumAlgorithm::Crc32, body);
        let response = raw_put_object_with_checksum_headers(
            &bucket,
            key,
            body,
            &[
                ("x-amz-checksum-type", "BOGUS".to_string()),
                ("x-amz-checksum-crc32", crc32.clone()),
            ],
        )
        .await;
        assert_eq!(response.status, 200, "response: {response:?}");
        assert_eq!(
            response_header(&response.headers, "x-amz-checksum-crc32"),
            Some(crc32.as_str())
        );
        assert_eq!(
            response_header(&response.headers, "x-amz-checksum-type"),
            Some("FULL_OBJECT")
        );
        assert_stored_crc32_full_object_checksum(&bucket, key, &crc32).await;
        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_put_object_composite_checksum_type_with_value_is_full_object() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "composite-checksum-type-with-value";
        let body = b"composite checksum type on put object oracle";
        let crc32 = checksum_base64(LocalChecksumAlgorithm::Crc32, body);
        let response = raw_put_object_with_checksum_headers(
            &bucket,
            key,
            body,
            &[
                ("x-amz-checksum-type", "COMPOSITE".to_string()),
                ("x-amz-checksum-crc32", crc32.clone()),
            ],
        )
        .await;
        assert_eq!(response.status, 200, "response: {response:?}");
        assert_eq!(
            response_header(&response.headers, "x-amz-checksum-crc32"),
            Some(crc32.as_str())
        );
        assert_eq!(
            response_header(&response.headers, "x-amz-checksum-type"),
            Some("FULL_OBJECT")
        );
        assert_stored_crc32_full_object_checksum(&bucket, key, &crc32).await;
        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_put_object_full_object_checksum_type_with_value_is_full_object() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "full-object-checksum-type-with-value";
        let body = b"full object checksum type on put object oracle";
        let crc32 = checksum_base64(LocalChecksumAlgorithm::Crc32, body);
        let response = raw_put_object_with_checksum_headers(
            &bucket,
            key,
            body,
            &[
                ("x-amz-checksum-type", "FULL_OBJECT".to_string()),
                ("x-amz-checksum-crc32", crc32.clone()),
            ],
        )
        .await;
        assert_eq!(response.status, 200, "response: {response:?}");
        assert_eq!(
            response_header(&response.headers, "x-amz-checksum-crc32"),
            Some(crc32.as_str())
        );
        assert_eq!(
            response_header(&response.headers, "x-amz-checksum-type"),
            Some("FULL_OBJECT")
        );
        assert_stored_crc32_full_object_checksum(&bucket, key, &crc32).await;
        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_put_object_composite_checksum_type_with_crc64nvme_is_full_object() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "composite-checksum-type-with-crc64nvme";
        let body = b"composite checksum type with crc64nvme put object oracle";
        let crc64 = checksum_base64(LocalChecksumAlgorithm::Crc64nvme, body);
        let response = raw_put_object_with_checksum_headers(
            &bucket,
            key,
            body,
            &[
                ("x-amz-checksum-type", "COMPOSITE".to_string()),
                ("x-amz-checksum-crc64nvme", crc64.clone()),
            ],
        )
        .await;
        assert_eq!(response.status, 200, "response: {response:?}");
        assert_eq!(
            response_header(&response.headers, "x-amz-checksum-crc64nvme"),
            Some(crc64.as_str())
        );
        assert_eq!(
            response_header(&response.headers, "x-amz-checksum-type"),
            Some("FULL_OBJECT")
        );
        assert_stored_crc64nvme_full_object_checksum(&bucket, key, &crc64).await;
        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_put_object_composite_checksum_type_with_sha256_is_full_object() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "composite-checksum-type-with-sha256";
        let body = b"composite checksum type with sha256 put object oracle";
        let sha256 = checksum_base64(LocalChecksumAlgorithm::Sha256, body);
        let response = raw_put_object_with_checksum_headers(
            &bucket,
            key,
            body,
            &[
                ("x-amz-checksum-type", "COMPOSITE".to_string()),
                ("x-amz-checksum-sha256", sha256.clone()),
            ],
        )
        .await;
        assert_eq!(response.status, 200, "response: {response:?}");
        assert_eq!(
            response_header(&response.headers, "x-amz-checksum-sha256"),
            Some(sha256.as_str())
        );
        assert_eq!(
            response_header(&response.headers, "x-amz-checksum-type"),
            Some("FULL_OBJECT")
        );
        assert_stored_sha256_full_object_checksum(&bucket, key, &sha256).await;
        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_put_object_full_object_checksum_type_with_sha256_is_full_object() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "full-object-checksum-type-with-sha256";
        let body = b"full object checksum type with sha256 put object oracle";
        let sha256 = checksum_base64(LocalChecksumAlgorithm::Sha256, body);
        let response = raw_put_object_with_checksum_headers(
            &bucket,
            key,
            body,
            &[
                ("x-amz-checksum-type", "FULL_OBJECT".to_string()),
                ("x-amz-checksum-sha256", sha256.clone()),
            ],
        )
        .await;
        assert_eq!(response.status, 200, "response: {response:?}");
        assert_eq!(
            response_header(&response.headers, "x-amz-checksum-sha256"),
            Some(sha256.as_str())
        );
        assert_eq!(
            response_header(&response.headers, "x-amz-checksum-type"),
            Some("FULL_OBJECT")
        );
        assert_stored_sha256_full_object_checksum(&bucket, key, &sha256).await;
        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_put_object_duplicate_checksum_header() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "duplicate-checksum-header";
        let body = b"duplicate checksum header oracle";
        let crc32 = checksum_base64(LocalChecksumAlgorithm::Crc32, body);
        let response = raw_put_object_with_duplicate_signed_checksum_headers(
            &bucket,
            key,
            body,
            "x-amz-checksum-crc32",
            &[&crc32, &crc32],
        )
        .await;
        assert_eq!(response.status, 400, "response: {response:?}");
        assert_error_code(&response.body, "InvalidArgument");
        assert_error_message(&response.body, "Only one value may be specified.");
        assert_error_argument_name(&response.body, "x-amz-checksum-crc32");
        assert_error_argument_value(&response.body, &crc32);
        assert_auth_error_response_shape(&response.body);
        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_put_object_duplicate_checksum_header_reports_second_value() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "duplicate-checksum-header-distinct-values";
        let body = b"duplicate checksum header distinct value oracle";
        let first_crc32 = checksum_base64(LocalChecksumAlgorithm::Crc32, body);
        let second_crc32 = checksum_base64(LocalChecksumAlgorithm::Crc32, b"different checksum");
        assert_ne!(first_crc32, second_crc32);
        let response = raw_put_object_with_duplicate_signed_checksum_headers(
            &bucket,
            key,
            body,
            "x-amz-checksum-crc32",
            &[&first_crc32, &second_crc32],
        )
        .await;
        assert_eq!(response.status, 400, "response: {response:?}");
        assert_error_code(&response.body, "InvalidArgument");
        assert_error_message(&response.body, "Only one value may be specified.");
        assert_error_argument_name(&response.body, "x-amz-checksum-crc32");
        assert_error_argument_value(&response.body, &second_crc32);
        assert_auth_error_response_shape(&response.body);
        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_put_object_duplicate_checksum_header_reports_first_duplicate_value() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "duplicate-checksum-header-three-values";
        let body = b"duplicate checksum header three value oracle";
        let first_crc32 = checksum_base64(LocalChecksumAlgorithm::Crc32, body);
        let second_crc32 = checksum_base64(LocalChecksumAlgorithm::Crc32, b"second checksum");
        let third_crc32 = checksum_base64(LocalChecksumAlgorithm::Crc32, b"third checksum");
        assert_ne!(first_crc32, second_crc32);
        assert_ne!(first_crc32, third_crc32);
        assert_ne!(second_crc32, third_crc32);
        let response = raw_put_object_with_duplicate_signed_checksum_headers(
            &bucket,
            key,
            body,
            "x-amz-checksum-crc32",
            &[&first_crc32, &second_crc32, &third_crc32],
        )
        .await;
        assert_eq!(response.status, 400, "response: {response:?}");
        assert_error_code(&response.body, "InvalidArgument");
        assert_error_message(&response.body, "Only one value may be specified.");
        assert_error_argument_name(&response.body, "x-amz-checksum-crc32");
        assert_error_argument_value(&response.body, &second_crc32);
        assert_auth_error_response_shape(&response.body);
        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_put_object_sdk_checksum_algorithm_with_matching_value() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "sdk-checksum-algorithm-with-matching-value";
        let body = b"sdk checksum algorithm with matching value oracle";
        let crc32 = checksum_base64(LocalChecksumAlgorithm::Crc32, body);
        let response = raw_put_object_with_checksum_headers(
            &bucket,
            key,
            body,
            &[
                ("x-amz-sdk-checksum-algorithm", "CRC32".to_string()),
                ("x-amz-checksum-crc32", crc32),
            ],
        )
        .await;
        assert_eq!(response.status, 200, "response: {response:?}");
        assert_eq!(
            response_header(&response.headers, "x-amz-checksum-crc32"),
            Some("23SgOw==")
        );
        assert_eq!(
            response_header(&response.headers, "x-amz-checksum-type"),
            Some("FULL_OBJECT")
        );
        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_put_object_sdk_checksum_algorithm_lowercase_value() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "sdk-checksum-algorithm-lowercase-value";
        let body = b"sdk checksum algorithm lowercase value oracle";
        let crc32 = checksum_base64(LocalChecksumAlgorithm::Crc32, body);
        let response = raw_put_object_with_checksum_headers(
            &bucket,
            key,
            body,
            &[
                ("x-amz-sdk-checksum-algorithm", "crc32".to_string()),
                ("x-amz-checksum-crc32", crc32),
            ],
        )
        .await;
        assert_eq!(response.status, 200, "response: {response:?}");
        assert_eq!(
            response_header(&response.headers, "x-amz-checksum-crc32"),
            Some("0R8J1Q==")
        );
        assert_eq!(
            response_header(&response.headers, "x-amz-checksum-type"),
            Some("FULL_OBJECT")
        );
        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_put_object_sdk_checksum_algorithm_mismatched_value() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "sdk-checksum-algorithm-mismatched-value";
        let body = b"sdk checksum algorithm mismatched value oracle";
        let crc32 = checksum_base64(LocalChecksumAlgorithm::Crc32, body);
        let response = raw_put_object_with_checksum_headers(
            &bucket,
            key,
            body,
            &[
                ("x-amz-sdk-checksum-algorithm", "SHA256".to_string()),
                ("x-amz-checksum-crc32", crc32),
            ],
        )
        .await;
        assert_eq!(response.status, 400, "response: {response:?}");
        assert_error_code(&response.body, "InvalidRequest");
        assert_error_message(&response.body, SDK_CHECKSUM_INVALID_VALUE_MESSAGE);
        assert_auth_error_response_shape(&response.body);
        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_put_object_invalid_sdk_checksum_algorithm_with_value() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "invalid-sdk-checksum-algorithm-with-value";
        let body = b"invalid sdk checksum algorithm with value oracle";
        let crc32 = checksum_base64(LocalChecksumAlgorithm::Crc32, body);
        let response = raw_put_object_with_checksum_headers(
            &bucket,
            key,
            body,
            &[
                ("x-amz-sdk-checksum-algorithm", "BOGUS".to_string()),
                ("x-amz-checksum-crc32", crc32),
            ],
        )
        .await;
        assert_eq!(response.status, 400, "response: {response:?}");
        assert_error_code(&response.body, "InvalidRequest");
        assert_error_message(&response.body, SDK_CHECKSUM_INVALID_VALUE_MESSAGE);
        assert_auth_error_response_shape(&response.body);
        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_put_object_sdk_checksum_algorithm_without_value() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "sdk-checksum-algorithm-without-value";
        let body = b"sdk checksum algorithm without value oracle";
        let response = raw_put_object_with_checksum_headers(
            &bucket,
            key,
            body,
            &[("x-amz-sdk-checksum-algorithm", "CRC32".to_string())],
        )
        .await;
        assert_eq!(response.status, 400, "response: {response:?}");
        assert_error_code(&response.body, "InvalidRequest");
        assert_error_message(&response.body, SDK_CHECKSUM_MISSING_VALUE_MESSAGE);
        assert_auth_error_response_shape(&response.body);
        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_put_object_invalid_sdk_checksum_algorithm_without_value() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "invalid-sdk-checksum-algorithm-without-value";
        let body = b"invalid sdk checksum algorithm without value oracle";
        let response = raw_put_object_with_checksum_headers(
            &bucket,
            key,
            body,
            &[("x-amz-sdk-checksum-algorithm", "BOGUS".to_string())],
        )
        .await;
        assert_eq!(response.status, 400, "response: {response:?}");
        assert_error_code(&response.body, "InvalidRequest");
        assert_error_message(&response.body, SDK_CHECKSUM_MISSING_VALUE_MESSAGE);
        assert_auth_error_response_shape(&response.body);
        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_create_multipart_checksum_algorithm_lowercase() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "create-multipart-lowercase-checksum-algorithm";
        let url = s3_tests::object_url(CTX.endpoint(), &bucket, key, Some("uploads"));
        let response =
            send_signed_request("POST", &url, &[], [("x-amz-checksum-algorithm", "crc32")]);
        if response.status == 200 {
            let upload_id = xml_text(&response.body, "UploadId").expect("UploadId in response");
            CTX.client()
                .abort_multipart_upload()
                .bucket(&bucket)
                .key(key)
                .upload_id(upload_id)
                .send()
                .await
                .unwrap();
        }
        assert_eq!(response.status, 200, "response: {response:?}");
        assert_eq!(
            response_header(&response.headers, "x-amz-checksum-algorithm"),
            Some("CRC32")
        );
        assert_eq!(
            response_header(&response.headers, "x-amz-checksum-type"),
            Some("COMPOSITE")
        );
        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_create_multipart_invalid_checksum_algorithm() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "create-multipart-invalid-checksum-algorithm";
        let url = s3_tests::object_url(CTX.endpoint(), &bucket, key, Some("uploads"));
        let response =
            send_signed_request("POST", &url, &[], [("x-amz-checksum-algorithm", "BOGUS")]);
        if response.status == 200 {
            let upload_id = xml_text(&response.body, "UploadId").expect("UploadId in response");
            CTX.client()
                .abort_multipart_upload()
                .bucket(&bucket)
                .key(key)
                .upload_id(upload_id)
                .send()
                .await
                .unwrap();
        }
        assert_eq!(response.status, 400, "response: {response:?}");
        assert_error_code(&response.body, "InvalidRequest");
        assert!(
            response
                .body
                .contains(UNSUPPORTED_CHECKSUM_ALGORITHM_MESSAGE),
            "body: {}",
            response.body
        );
        assert_auth_error_response_shape(&response.body);
        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_create_multipart_invalid_checksum_type_error_shape() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "create-multipart-invalid-checksum-type-shape";
        let url = s3_tests::object_url(CTX.endpoint(), &bucket, key, Some("uploads"));
        let response = send_signed_request(
            "POST",
            &url,
            &[],
            [
                ("x-amz-checksum-algorithm", "CRC32"),
                ("x-amz-checksum-type", "INVALID"),
            ],
        );
        assert_shape(
            "CreateMultipartUpload invalid checksum type",
            &response,
            &shape().status(400).headers(error_response_headers()).body(
                expected_error::with_host_id(
                    "InvalidRequest",
                    "Value for x-amz-checksum-type header is invalid.",
                ),
            ),
        );
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_create_multipart_checksum_type_without_algorithm_error_shape() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "create-multipart-type-without-algorithm-shape";
        let url = s3_tests::object_url(CTX.endpoint(), &bucket, key, Some("uploads"));
        let response =
            send_signed_request("POST", &url, &[], [("x-amz-checksum-type", "COMPOSITE")]);
        assert_shape(
            "CreateMultipartUpload checksum type without algorithm",
            &response,
            &shape().status(400).headers(error_response_headers()).body(
                expected_error::with_host_id(
                    "InvalidRequest",
                    "The x-amz-checksum-type header can only be used with the x-amz-checksum-algorithm header.",
                ),
            ),
        );
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_create_multipart_crc64nvme_composite_error_shape() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "create-multipart-crc64nvme-composite-shape";
        let url = s3_tests::object_url(CTX.endpoint(), &bucket, key, Some("uploads"));
        let response = send_signed_request(
            "POST",
            &url,
            &[],
            [
                ("x-amz-checksum-algorithm", "CRC64NVME"),
                ("x-amz-checksum-type", "COMPOSITE"),
            ],
        );
        assert_shape(
            "CreateMultipartUpload CRC64NVME COMPOSITE",
            &response,
            &shape()
                .status(400)
                .headers(error_response_headers())
                .body(expected_error::with_host_id(
                "InvalidRequest",
                "The COMPOSITE checksum type cannot be used with the crc64nvme checksum algorithm.",
            )),
        );
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_create_multipart_sha256_full_object_error_shape() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "create-multipart-sha256-full-object-shape";
        let url = s3_tests::object_url(CTX.endpoint(), &bucket, key, Some("uploads"));
        let response = send_signed_request(
            "POST",
            &url,
            &[],
            [
                ("x-amz-checksum-algorithm", "SHA256"),
                ("x-amz-checksum-type", "FULL_OBJECT"),
            ],
        );
        assert_shape(
            "CreateMultipartUpload SHA256 FULL_OBJECT",
            &response,
            &shape()
                .status(400)
                .headers(error_response_headers())
                .body(expected_error::with_host_id(
                "InvalidRequest",
                "The FULL_OBJECT checksum type cannot be used with the sha256 checksum algorithm.",
            )),
        );
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_create_multipart_concrete_checksum_header_without_algorithm_is_ignored() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "create-multipart-concrete-checksum-without-algorithm";
        let url = s3_tests::object_url(CTX.endpoint(), &bucket, key, Some("uploads"));
        let response = send_signed_request(
            "POST",
            &url,
            &[],
            [("x-amz-checksum-sha256", "not-even-base64")],
        );
        assert_eq!(response.status, 200, "response: {response:?}");
        assert_eq!(
            response_header(&response.headers, "x-amz-checksum-algorithm"),
            None
        );
        assert_eq!(
            response_header(&response.headers, "x-amz-checksum-type"),
            None
        );
        let upload_id = xml_text(&response.body, "UploadId").expect("UploadId in response");
        complete_single_part_upload_without_checksum(
            &bucket,
            key,
            upload_id,
            vec![b'A'; PART_SIZE],
        )
        .await;
        assert_no_stored_sha256_checksum(
            &bucket,
            key,
            "CreateMultipartUpload concrete checksum without algorithm",
        )
        .await;
        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_create_multipart_concrete_checksum_header_does_not_override_algorithm() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "create-multipart-concrete-checksum-with-algorithm";
        let url = s3_tests::object_url(CTX.endpoint(), &bucket, key, Some("uploads"));
        let response = send_signed_request(
            "POST",
            &url,
            &[],
            [
                ("x-amz-checksum-algorithm", "CRC32"),
                ("x-amz-checksum-sha256", "not-even-base64"),
            ],
        );
        assert_eq!(response.status, 200, "response: {response:?}");
        assert_eq!(
            response_header(&response.headers, "x-amz-checksum-algorithm"),
            Some("CRC32")
        );
        assert_eq!(
            response_header(&response.headers, "x-amz-checksum-type"),
            Some("COMPOSITE")
        );
        let upload_id = xml_text(&response.body, "UploadId").expect("UploadId in response");
        CTX.client()
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_copy_object_replace_checksum_algorithm_lowercase() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let source_key = "copy-source-lowercase-checksum-algorithm";
        let destination_key = "copy-destination-lowercase-checksum-algorithm";
        client
            .put_object()
            .bucket(&bucket)
            .key(source_key)
            .body(ByteStream::from_static(
                b"copy checksum algorithm lowercase oracle",
            ))
            .send()
            .await
            .unwrap();
        let url = s3_tests::object_url(CTX.endpoint(), &bucket, destination_key, None);
        let copy_source = format!("/{bucket}/{source_key}");
        let response = send_signed_request(
            "PUT",
            &url,
            &[],
            [
                ("x-amz-copy-source", copy_source.as_str()),
                ("x-amz-metadata-directive", "REPLACE"),
                ("x-amz-checksum-algorithm", "crc32"),
            ],
        );
        assert_eq!(response.status, 200, "response: {response:?}");
        assert_eq!(xml_text(&response.body, "ChecksumCRC32"), Some("mYg1AA=="));
        assert_eq!(
            xml_text(&response.body, "ChecksumType"),
            Some("FULL_OBJECT")
        );
        cleanup(&bucket, &[source_key, destination_key]).await;
    });
}

#[test]
fn test_copy_object_replace_invalid_checksum_algorithm() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let source_key = "copy-source-invalid-checksum-algorithm";
        let destination_key = "copy-destination-invalid-checksum-algorithm";
        client
            .put_object()
            .bucket(&bucket)
            .key(source_key)
            .body(ByteStream::from_static(
                b"copy checksum algorithm invalid oracle",
            ))
            .send()
            .await
            .unwrap();
        let url = s3_tests::object_url(CTX.endpoint(), &bucket, destination_key, None);
        let copy_source = format!("/{bucket}/{source_key}");
        let response = send_signed_request(
            "PUT",
            &url,
            &[],
            [
                ("x-amz-copy-source", copy_source.as_str()),
                ("x-amz-metadata-directive", "REPLACE"),
                ("x-amz-checksum-algorithm", "BOGUS"),
            ],
        );
        assert_eq!(response.status, 400, "response: {response:?}");
        assert_error_code(&response.body, "InvalidRequest");
        assert!(
            response
                .body
                .contains(UNSUPPORTED_CHECKSUM_ALGORITHM_MESSAGE),
            "body: {}",
            response.body
        );
        assert_auth_error_response_shape(&response.body);
        cleanup(&bucket, &[source_key, destination_key]).await;
    });
}

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
        a if *a == ChecksumAlgorithm::Md5 => resp.checksum_md5().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Sha512 => resp.checksum_sha512().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Xxhash64 => resp.checksum_xxhash64().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Xxhash3 => resp.checksum_xxhash3().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Xxhash128 => resp.checksum_xxhash128().map(|s| s.to_string()),
        _ => None,
    }
}

fn get_cksum_from_put(
    resp: &aws_sdk_s3::operation::put_object::PutObjectOutput,
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
        a if *a == ChecksumAlgorithm::Md5 => resp.checksum_md5().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Sha512 => resp.checksum_sha512().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Xxhash64 => resp.checksum_xxhash64().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Xxhash3 => resp.checksum_xxhash3().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Xxhash128 => resp.checksum_xxhash128().map(|s| s.to_string()),
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
        a if *a == ChecksumAlgorithm::Md5 => resp.checksum_md5().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Sha512 => resp.checksum_sha512().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Xxhash64 => resp.checksum_xxhash64().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Xxhash3 => resp.checksum_xxhash3().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Xxhash128 => resp.checksum_xxhash128().map(|s| s.to_string()),
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
        a if *a == ChecksumAlgorithm::Md5 => cksum.checksum_md5().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Sha512 => cksum.checksum_sha512().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Xxhash64 => cksum.checksum_xxhash64().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Xxhash3 => cksum.checksum_xxhash3().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Xxhash128 => {
            cksum.checksum_xxhash128().map(|s| s.to_string())
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
    } else if *algo == ChecksumAlgorithm::Md5 {
        b = b.checksum_md5(cksum);
    } else if *algo == ChecksumAlgorithm::Sha512 {
        b = b.checksum_sha512(cksum);
    } else if *algo == ChecksumAlgorithm::Xxhash64 {
        b = b.checksum_xxhash64(cksum);
    } else if *algo == ChecksumAlgorithm::Xxhash3 {
        b = b.checksum_xxhash3(cksum);
    } else if *algo == ChecksumAlgorithm::Xxhash128 {
        b = b.checksum_xxhash128(cksum);
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
    } else if *algo == ChecksumAlgorithm::Md5 {
        builder.checksum_md5(cksum)
    } else if *algo == ChecksumAlgorithm::Sha512 {
        builder.checksum_sha512(cksum)
    } else if *algo == ChecksumAlgorithm::Xxhash64 {
        builder.checksum_xxhash64(cksum)
    } else if *algo == ChecksumAlgorithm::Xxhash3 {
        builder.checksum_xxhash3(cksum)
    } else if *algo == ChecksumAlgorithm::Xxhash128 {
        builder.checksum_xxhash128(cksum)
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
        a if *a == ChecksumAlgorithm::Md5 => resp.checksum_md5().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Sha512 => resp.checksum_sha512().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Xxhash64 => resp.checksum_xxhash64().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Xxhash3 => resp.checksum_xxhash3().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Xxhash128 => resp.checksum_xxhash128().map(|s| s.to_string()),
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
        .send_retrying_operation_aborted("create checksum multipart upload")
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
        let resp = retrying_operation_aborted("upload checksum multipart part", || {
            let builder = client
                .upload_part()
                .bucket(&bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(part_number)
                .body(ByteStream::from(data.to_vec()))
                .checksum_algorithm(tc.algo.clone());
            let builder = upload_part_with_checksum(builder, &tc.algo, cksum);
            async move { builder.send().await }
        })
        .await;
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
    let expected_bare = bare_composite_checksum(tc.composite_cksum);
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
fn test_complete_multipart_legacy_object_checksum_without_create_algorithm_is_ignored() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let part_body = b"legacy object checksum header";
        let cases = [
            (
                "crc32",
                ChecksumAlgorithm::Crc32,
                LocalChecksumAlgorithm::Crc32,
            ),
            (
                "crc32c",
                ChecksumAlgorithm::Crc32C,
                LocalChecksumAlgorithm::Crc32c,
            ),
            (
                "sha1",
                ChecksumAlgorithm::Sha1,
                LocalChecksumAlgorithm::Sha1,
            ),
            (
                "sha256",
                ChecksumAlgorithm::Sha256,
                LocalChecksumAlgorithm::Sha256,
            ),
        ];
        let mut keys = Vec::new();

        for (name, aws_algo, local_algo) in cases {
            let key = format!("mpu-complete-legacy-checksum-without-create-{name}");
            keys.push(key.clone());
            let create = client
                .create_multipart_upload()
                .bucket(&bucket)
                .key(&key)
                .send()
                .await
                .unwrap();
            let upload_id = create.upload_id().unwrap().to_string();

            let part = client
                .upload_part()
                .bucket(&bucket)
                .key(&key)
                .upload_id(&upload_id)
                .part_number(1)
                .body(ByteStream::from(part_body.to_vec()))
                .send()
                .await
                .unwrap();
            let etag = part.e_tag().unwrap();

            let url = multipart_complete_url(&bucket, &key, &upload_id);
            let body = format!(
                "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
            );
            let checksum = checksum_base64(local_algo, part_body);
            let (status, body_text) = send_signed_post(
                &url,
                body.as_bytes(),
                &[(checksum_header_name(&aws_algo), checksum.as_str())],
            );
            assert_eq!(status, 200, "body: {body_text}");

            let head = client
                .head_object()
                .bucket(&bucket)
                .key(&key)
                .checksum_mode(ChecksumMode::Enabled)
                .send()
                .await
                .unwrap();
            assert!(
                get_cksum_from_head(&head, &aws_algo).is_none(),
                "unconfigured legacy complete checksum header must not be stored for {name}"
            );
        }

        let key_refs = keys.iter().map(String::as_str).collect::<Vec<_>>();
        cleanup(&bucket, &key_refs).await;
    });
}

#[test]
fn test_complete_multipart_new_object_checksum_without_create_algorithm_rejected() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let part_body = b"new object checksum header";
        let cases = [
            ("md5", ChecksumAlgorithm::Md5, LocalChecksumAlgorithm::Md5),
            (
                "sha512",
                ChecksumAlgorithm::Sha512,
                LocalChecksumAlgorithm::Sha512,
            ),
            (
                "xxhash64",
                ChecksumAlgorithm::Xxhash64,
                LocalChecksumAlgorithm::XxHash64,
            ),
            (
                "xxhash3",
                ChecksumAlgorithm::Xxhash3,
                LocalChecksumAlgorithm::XxHash3,
            ),
            (
                "xxhash128",
                ChecksumAlgorithm::Xxhash128,
                LocalChecksumAlgorithm::XxHash128,
            ),
        ];

        for (name, aws_algo, local_algo) in cases {
            let key = format!("mpu-complete-new-checksum-without-create-{name}");
            let create = client
                .create_multipart_upload()
                .bucket(&bucket)
                .key(&key)
                .send()
                .await
                .unwrap();
            let upload_id = create.upload_id().unwrap().to_string();

            let part = client
                .upload_part()
                .bucket(&bucket)
                .key(&key)
                .upload_id(&upload_id)
                .part_number(1)
                .body(ByteStream::from(part_body.to_vec()))
                .send()
                .await
                .unwrap();
            let etag = part.e_tag().unwrap();

            let url = multipart_complete_url(&bucket, &key, &upload_id);
            let body = format!(
                "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
            );
            let checksum = checksum_base64(local_algo, part_body);
            let (status, body_text) = send_signed_post(
                &url,
                body.as_bytes(),
                &[(checksum_header_name(&aws_algo), checksum.as_str())],
            );
            assert!(
                matches!(status, 200 | 400),
                "CompleteMultipartUpload InvalidRequest used status {status}, expected 400 or embedded-error status 200: {body_text}"
            );
            assert_error_code(&body_text, "InvalidRequest");
            assert_error_message(
                &body_text,
                &format!(
                    "Checksum Type mismatch occurred, expected checksum Type: null, actual checksum Type: {name}"
                ),
            );
            assert_auth_error_response_shape(&body_text);

            let _ = client
                .abort_multipart_upload()
                .bucket(&bucket)
                .key(&key)
                .upload_id(&upload_id)
                .send()
                .await;
        }
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_complete_multipart_unconfigured_crc64nvme_checksum_is_stored() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "mpu-complete-crc64nvme-checksum-without-create";
        let part_body = b"crc64nvme object checksum header";

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
            .body(ByteStream::from(part_body.to_vec()))
            .send()
            .await
            .unwrap();
        let etag = part.e_tag().unwrap();

        let url = multipart_complete_url(&bucket, key, &upload_id);
        let body = format!(
            "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
        );
        let expected = checksum_base64(LocalChecksumAlgorithm::Crc64nvme, part_body);
        let (status, body_text) = send_signed_post(
            &url,
            body.as_bytes(),
            &[(
                checksum_header_name(&ChecksumAlgorithm::Crc64Nvme),
                expected.as_str(),
            )],
        );
        assert_eq!(status, 200, "body: {body_text}");
        assert!(
            body_text.contains(&format!(
                "<ChecksumCRC64NVME>{expected}</ChecksumCRC64NVME>"
            )),
            "CompleteMultipartUpload response missing CRC64NVME checksum {expected}: {body_text}"
        );
        assert!(
            body_text.contains("<ChecksumType>FULL_OBJECT</ChecksumType>"),
            "CompleteMultipartUpload response missing FULL_OBJECT type: {body_text}"
        );

        let head = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
            .unwrap();
        assert_eq!(
            get_cksum_from_head(&head, &ChecksumAlgorithm::Crc64Nvme),
            Some(expected.clone()),
            "HeadObject checksum mismatch"
        );
        assert_eq!(head.checksum_type(), Some(&ChecksumType::FullObject));

        let attrs = client
            .get_object_attributes()
            .bucket(&bucket)
            .key(key)
            .object_attributes(ObjectAttributes::Checksum)
            .send()
            .await
            .unwrap();
        let checksum = attrs.checksum().expect("expected Checksum attributes");
        assert_eq!(
            get_cksum_from_checksum(checksum, &ChecksumAlgorithm::Crc64Nvme),
            Some(expected),
            "GetObjectAttributes checksum mismatch"
        );
        assert_eq!(checksum.checksum_type(), Some(&ChecksumType::FullObject));

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_complete_multipart_unconfigured_crc64nvme_mismatch_is_computed() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "mpu-complete-crc64nvme-checksum-without-create-mismatch";
        let part_body = b"crc64nvme object checksum mismatch";

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
            .body(ByteStream::from(part_body.to_vec()))
            .send()
            .await
            .unwrap();
        let etag = part.e_tag().unwrap();

        let url = multipart_complete_url(&bucket, key, &upload_id);
        let body = format!(
            "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
        );
        let wrong_checksum = encode_base64(&[0u8; 8]);
        let expected = checksum_base64(LocalChecksumAlgorithm::Crc64nvme, part_body);
        assert_ne!(wrong_checksum, expected);
        let (status, body_text) = send_signed_post(
            &url,
            body.as_bytes(),
            &[(
                checksum_header_name(&ChecksumAlgorithm::Crc64Nvme),
                wrong_checksum.as_str(),
            )],
        );
        assert_eq!(status, 200, "body: {body_text}");
        assert!(
            body_text.contains(&format!(
                "<ChecksumCRC64NVME>{expected}</ChecksumCRC64NVME>"
            )),
            "CompleteMultipartUpload response missing computed CRC64NVME checksum {expected}: {body_text}"
        );
        assert!(
            body_text.contains("<ChecksumType>FULL_OBJECT</ChecksumType>"),
            "CompleteMultipartUpload response missing FULL_OBJECT type: {body_text}"
        );

        let head = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
            .unwrap();
        assert_eq!(
            get_cksum_from_head(&head, &ChecksumAlgorithm::Crc64Nvme),
            Some(expected),
            "HeadObject checksum mismatch"
        );
        assert_eq!(head.checksum_type(), Some(&ChecksumType::FullObject));

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_complete_multipart_new_part_checksum_without_stored_checksum_is_invalid_part() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "mpu-complete-new-part-checksum-without-create";
        let part_body = b"new part checksum element";

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
            .body(ByteStream::from(part_body.to_vec()))
            .send()
            .await
            .unwrap();
        let etag = part.e_tag().unwrap();
        let sha512 = checksum_base64(LocalChecksumAlgorithm::Sha512, part_body);

        let url = multipart_complete_url(&bucket, key, &upload_id);
        let body = format!(
            "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag><ChecksumSHA512>{sha512}</ChecksumSHA512></Part></CompleteMultipartUpload>"
        );
        let (status, body_text) = send_signed_post(&url, body.as_bytes(), &[]);
        assert!(
            status == 200 || status == 400,
            "unexpected status {status}, body: {body_text}"
        );
        assert_error_code(&body_text, "InvalidPart");

        let _ = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await;
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_upload_part_new_checksum_without_create_algorithm_is_accepted() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "mpu-upload-part-new-checksum-without-create";
        let part_body = b"new upload part checksum header";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();
        let sha512 = checksum_base64(LocalChecksumAlgorithm::Sha512, part_body);

        let resp = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from(part_body.to_vec()))
            .checksum_algorithm(ChecksumAlgorithm::Sha512)
            .checksum_sha512(&sha512)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.checksum_sha512(), Some(sha512.as_str()));

        let _ = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await;
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_complete_multipart_new_part_checksum_without_create_algorithm_is_accepted_but_not_stored() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "mpu-complete-new-upload-part-checksum-without-create";
        let part_body = b"new upload part checksum then complete";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();
        let sha512 = checksum_base64(LocalChecksumAlgorithm::Sha512, part_body);

        let part = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from(part_body.to_vec()))
            .checksum_algorithm(ChecksumAlgorithm::Sha512)
            .checksum_sha512(&sha512)
            .send()
            .await
            .unwrap();
        assert_eq!(part.checksum_sha512(), Some(sha512.as_str()));

        let result = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(completed_part_with_checksum(
                        part.e_tag().unwrap(),
                        1,
                        &ChecksumAlgorithm::Sha512,
                        &sha512,
                    ))
                    .build(),
            )
            .send()
            .await;
        assert_complete_multipart_sdk_error(&result, 400, "InvalidPart");

        let _ = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await;
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_multipart_new_composite_checksum_algorithms_round_trip() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let cases = [
            ("md5", ChecksumAlgorithm::Md5, LocalChecksumAlgorithm::Md5),
            (
                "sha512",
                ChecksumAlgorithm::Sha512,
                LocalChecksumAlgorithm::Sha512,
            ),
            (
                "xxhash64",
                ChecksumAlgorithm::Xxhash64,
                LocalChecksumAlgorithm::XxHash64,
            ),
            (
                "xxhash3",
                ChecksumAlgorithm::Xxhash3,
                LocalChecksumAlgorithm::XxHash3,
            ),
            (
                "xxhash128",
                ChecksumAlgorithm::Xxhash128,
                LocalChecksumAlgorithm::XxHash128,
            ),
        ];
        let parts_data = [
            vec![b'A'; PART_SIZE],
            vec![b'B'; PART_SIZE],
            b"tail part for new checksum algorithms".to_vec(),
        ];
        let mut keys = Vec::new();

        for (name, aws_algo, local_algo) in cases {
            let key = format!("new-composite-mpu-{name}");
            keys.push(key.clone());

            let create = client
                .create_multipart_upload()
                .bucket(&bucket)
                .key(&key)
                .checksum_algorithm(aws_algo.clone())
                .checksum_type(ChecksumType::Composite)
                .send_retrying_operation_aborted("create new checksum multipart upload")
                .await
                .unwrap_or_else(|err| panic!("CreateMultipartUpload failed for {name}: {err:?}"));
            let upload_id = create.upload_id().unwrap();

            let mut completed_parts = Vec::new();
            let mut raw_completed_parts = Vec::new();
            let mut part_checksums = Vec::new();
            for (idx, data) in parts_data.iter().enumerate() {
                let part_number = (idx + 1) as i32;
                let checksum = checksum_base64(local_algo, data);
                let resp = retrying_operation_aborted("upload new checksum multipart part", || {
                    let builder = client
                        .upload_part()
                        .bucket(&bucket)
                        .key(&key)
                        .upload_id(upload_id)
                        .part_number(part_number)
                        .body(ByteStream::from(data.clone()))
                        .checksum_algorithm(aws_algo.clone());
                    let builder = upload_part_with_checksum(builder, &aws_algo, &checksum);
                    async move { builder.send().await }
                })
                .await;
                let returned_checksum = get_cksum_from_upload_part(&resp, &aws_algo)
                    .expect("UploadPart should echo the checksum");
                assert_eq!(returned_checksum, checksum);
                part_checksums.push(returned_checksum.clone());
                let etag = resp.e_tag().unwrap().to_string();
                raw_completed_parts.push(format!(
                    "<Part><PartNumber>{part_number}</PartNumber><ETag>{etag}</ETag><{elem}>{returned_checksum}</{elem}></Part>",
                    elem = checksum_xml_element_name(&aws_algo),
                ));
                completed_parts.push(completed_part_with_checksum(
                    &etag,
                    part_number,
                    &aws_algo,
                    &returned_checksum,
                ));
            }

            let expected = composite_checksum_base64(local_algo, &part_checksums);
            if matches!(
                local_algo,
                LocalChecksumAlgorithm::XxHash64
                    | LocalChecksumAlgorithm::XxHash3
                    | LocalChecksumAlgorithm::XxHash128
            ) {
                let body = format!(
                    "<CompleteMultipartUpload>{}</CompleteMultipartUpload>",
                    raw_completed_parts.join("")
                );
                let (status, body_text) = send_signed_post(
                    &multipart_complete_url(&bucket, &key, upload_id),
                    body.as_bytes(),
                    &[],
                );
                assert_eq!(
                    status, 200,
                    "CompleteMultipartUpload failed for {name}: {body_text}"
                );
                let elem = checksum_xml_element_name(&aws_algo);
                assert!(
                    body_text.contains(&format!("<{elem}>{expected}</{elem}>")),
                    "CompleteMultipartUpload response missing {name} checksum {expected}: {body_text}"
                );
                assert!(
                    body_text.contains("<ChecksumType>COMPOSITE</ChecksumType>"),
                    "CompleteMultipartUpload response missing COMPOSITE type for {name}: {body_text}"
                );
            } else {
                let complete = client
                    .complete_multipart_upload()
                    .bucket(&bucket)
                    .key(&key)
                    .upload_id(upload_id)
                    .multipart_upload(
                        CompletedMultipartUpload::builder()
                            .set_parts(Some(completed_parts))
                            .build(),
                    )
                    .send()
                    .await
                    .unwrap_or_else(|err| {
                        panic!("CompleteMultipartUpload failed for {name}: {err:?}")
                    });
                assert_eq!(
                    get_cksum_from_complete(&complete, &aws_algo),
                    Some(expected.clone()),
                    "CompleteMultipartUpload checksum mismatch for {name}"
                );
                assert_eq!(complete.checksum_type(), Some(&ChecksumType::Composite));
            }

            let head = client
                .head_object()
                .bucket(&bucket)
                .key(&key)
                .checksum_mode(ChecksumMode::Enabled)
                .send()
                .await
                .unwrap();
            assert_eq!(
                get_cksum_from_head(&head, &aws_algo),
                Some(expected.clone()),
                "HeadObject checksum mismatch for {name}"
            );
            assert_eq!(head.checksum_type(), Some(&ChecksumType::Composite));

            let attrs = client
                .get_object_attributes()
                .bucket(&bucket)
                .key(&key)
                .object_attributes(ObjectAttributes::Checksum)
                .send()
                .await
                .unwrap();
            let checksum = attrs.checksum().expect("expected Checksum attributes");
            assert_eq!(
                get_cksum_from_checksum(checksum, &aws_algo),
                Some(bare_composite_checksum(&expected).to_string()),
                "GetObjectAttributes checksum mismatch for {name}"
            );
            assert_eq!(checksum.checksum_type(), Some(&ChecksumType::Composite));
        }

        let key_refs = keys.iter().map(String::as_str).collect::<Vec<_>>();
        cleanup(&bucket, &key_refs).await;
    });
}

#[test]
fn test_complete_multipart_configured_new_object_checksum_header_mismatch_rejected() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "mpu-configured-new-object-checksum-mismatch";
        let part_body = b"configured new object checksum mismatch";
        let part_checksum = checksum_base64(LocalChecksumAlgorithm::Sha512, part_body);

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .checksum_algorithm(ChecksumAlgorithm::Sha512)
            .checksum_type(ChecksumType::Composite)
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
            .body(ByteStream::from(part_body.to_vec()))
            .checksum_algorithm(ChecksumAlgorithm::Sha512)
            .checksum_sha512(&part_checksum)
            .send()
            .await
            .unwrap();
        let wrong_object_checksum = encode_base64(&[0u8; 64]);

        let result = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .checksum_sha512(&wrong_object_checksum)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(completed_part_with_checksum(
                        part.e_tag().unwrap(),
                        1,
                        &ChecksumAlgorithm::Sha512,
                        &part_checksum,
                    ))
                    .build(),
            )
            .send()
            .await;
        assert_complete_multipart_sdk_error(&result, 400, "BadDigest");

        let _ = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await;
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_object_checksum_xxhash_vectors_round_trip() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"xxhash wire compatibility vector";
        let cases = [
            (
                "xxhash64",
                ChecksumAlgorithm::Xxhash64,
                LocalChecksumAlgorithm::XxHash64,
            ),
            (
                "xxhash3",
                ChecksumAlgorithm::Xxhash3,
                LocalChecksumAlgorithm::XxHash3,
            ),
            (
                "xxhash128",
                ChecksumAlgorithm::Xxhash128,
                LocalChecksumAlgorithm::XxHash128,
            ),
        ];
        let mut keys = Vec::new();

        for (name, aws_algo, local_algo) in cases {
            let key = format!("object-checksum-{name}");
            let expected = checksum_base64(local_algo, body);
            let put = match aws_algo {
                ChecksumAlgorithm::Xxhash64 => {
                    client
                        .put_object()
                        .bucket(&bucket)
                        .key(&key)
                        .body(ByteStream::from(body.to_vec()))
                        .checksum_algorithm(aws_algo.clone())
                        .checksum_xxhash64(&expected)
                        .send()
                        .await
                }
                ChecksumAlgorithm::Xxhash3 => {
                    client
                        .put_object()
                        .bucket(&bucket)
                        .key(&key)
                        .body(ByteStream::from(body.to_vec()))
                        .checksum_algorithm(aws_algo.clone())
                        .checksum_xxhash3(&expected)
                        .send()
                        .await
                }
                ChecksumAlgorithm::Xxhash128 => {
                    client
                        .put_object()
                        .bucket(&bucket)
                        .key(&key)
                        .body(ByteStream::from(body.to_vec()))
                        .checksum_algorithm(aws_algo.clone())
                        .checksum_xxhash128(&expected)
                        .send()
                        .await
                }
                _ => unreachable!(),
            }
            .unwrap();
            assert_eq!(get_cksum_from_put(&put, &aws_algo), Some(expected.clone()));

            let head = client
                .head_object()
                .bucket(&bucket)
                .key(&key)
                .checksum_mode(ChecksumMode::Enabled)
                .send()
                .await
                .unwrap();
            assert_eq!(get_cksum_from_head(&head, &aws_algo), Some(expected));
            keys.push(key);
        }

        let refs = keys.iter().map(String::as_str).collect::<Vec<_>>();
        cleanup(&bucket, &refs).await;
    });
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

#[test]
fn test_upload_part_copy_uses_multipart_checksum_algorithm() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let src_key = "mpu-copy-checksum-source";
        let dst_key = "mpu-copy-checksum-destination";
        let source_body = vec![b'C'; PART_SIZE];
        let expected_checksum = checksum_crc32(&source_body);

        client
            .put_object()
            .bucket(&bucket)
            .key(src_key)
            .body(ByteStream::from(source_body))
            .send()
            .await
            .unwrap();

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .checksum_type(ChecksumType::FullObject)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let copy = client
            .upload_part_copy()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .part_number(1)
            .copy_source(format!("{bucket}/{src_key}"))
            .send()
            .await
            .unwrap();
        let copied_part = copy.copy_part_result().expect("expected CopyPartResult");
        let copied_checksum = copied_part
            .checksum_crc32()
            .expect("expected UploadPartCopy CRC32 checksum");
        assert_eq!(copied_checksum, expected_checksum);

        let complete = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(copied_part.e_tag().unwrap())
                            .part_number(1)
                            .checksum_crc32(copied_checksum)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(complete.checksum_crc32(), Some(expected_checksum.as_str()));
        assert_eq!(complete.checksum_type(), Some(&ChecksumType::FullObject));

        let head = client
            .head_object()
            .bucket(&bucket)
            .key(dst_key)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
            .unwrap();
        assert_eq!(head.checksum_crc32(), Some(expected_checksum.as_str()));
        assert_eq!(head.checksum_type(), Some(&ChecksumType::FullObject));

        cleanup(&bucket, &[src_key, dst_key]).await;
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
        // COMPOSITE checksum = sha256(raw_part_checksum)-1
        let composite = "Ok6Cs5b96ux6+MWQkJO7UBT5sKPBeXBLwvj/hK89smg=-1";

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

        // CompleteMultipartUpload with a malformed checksum should fail before digest comparison.
        let malformed_result = client
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
        assert_complete_multipart_sdk_error(&malformed_result, 400, "InvalidRequest");

        // CompleteMultipartUpload with a validly encoded but wrong checksum should fail.
        let wrong_sha256 = encode_base64(&[0u8; 32]);
        let result = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .checksum_sha256(wrong_sha256)
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
        assert_complete_multipart_sdk_error(&result, 400, "BadDigest");
        assert_checksum_completion_preserved_upload(&bucket, key, upload_id, resp.e_tag().unwrap())
            .await;

        let corrected = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .checksum_sha256(composite)
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
            .await
            .unwrap();
        assert_eq!(corrected.checksum_sha256(), Some(composite));

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
        assert_complete_multipart_sdk_error(&result2, 400, "InvalidRequest");
        assert_checksum_completion_preserved_upload(
            &bucket,
            key2,
            upload_id2,
            resp2.e_tag().unwrap(),
        )
        .await;

        let corrected2 = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key2)
            .upload_id(upload_id2)
            .checksum_sha256(composite)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(resp2.e_tag().unwrap())
                            .checksum_sha256(resp2.checksum_sha256().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(corrected2.checksum_sha256(), Some(composite));

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

        cleanup(&bucket, &[key, key2, key3]).await;
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

            let parts_count = resp.parts_count();
            let request_id = resp.request_id().map(str::to_owned);
            let extended_request_id = resp.extended_request_id().map(str::to_owned);

            // Verify data matches
            let body = match resp.body.collect().await {
                Ok(body) => body.into_bytes(),
                Err(error) => {
                    panic!(
                        "GetObject part body collection failed: bucket={bucket:?} key={key:?} part_number={part_number} expected_body_len={} parts_count={parts_count:?} request_id={request_id:?} extended_request_id={extended_request_id:?} checksum_crc32={got_crc:?} error={error:?}",
                        data.len(),
                    );
                }
            };
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

// ── Response shapes ─────────────────────────────────────────────────

#[test]
fn test_put_object_bad_inline_checksum_error_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let response = raw_object_with(
            "PUT",
            &bucket,
            "shape-bad-inline-checksum.txt",
            b"hello streaming checksum",
            &[("x-amz-checksum-crc32", "AAAA/w==")],
        );
        assert_shape(
            "PutObject bad inline checksum",
            &response,
            &shape().status(400).headers(error_response_headers()).body(
                expected_error::with_host_id(
                    "BadDigest",
                    "The CRC32 you specified did not match the calculated checksum.",
                ),
            ),
        );

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_checksum_mode_object_response_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        let key = "shape-checksum-mode.txt";

        raw_object_with(
            "PUT",
            &bucket,
            key,
            b"checksum-mode-body",
            &[("Content-Type", "application/octet-stream")],
        );

        let get_head_headers = [
            ("etag", "{etag}"),
            ("last-modified", "{http_date}"),
            ("accept-ranges", "bytes"),
            ("content-type", "application/octet-stream"),
            ("x-amz-checksum-crc64nvme", "dmtRIDonanA="),
            ("x-amz-checksum-type", "FULL_OBJECT"),
            ("x-amz-server-side-encryption", "AES256"),
            ("content-length", "18"),
            ("x-amz-request-id", "{request_id}"),
            ("x-amz-id-2", "{host_id}"),
        ];
        let get = raw_object_with(
            "GET",
            &bucket,
            key,
            b"",
            &[("x-amz-checksum-mode", "ENABLED")],
        );
        let get_captures = assert_shape(
            "GetObject checksum mode",
            &get,
            &shape()
                .status(200)
                .headers(get_head_headers)
                .body("checksum-mode-body"),
        );
        let head = raw_object_with(
            "HEAD",
            &bucket,
            key,
            b"",
            &[("x-amz-checksum-mode", "ENABLED")],
        );
        let head_captures = assert_shape(
            "HeadObject checksum mode",
            &head,
            &shape().status(200).headers(get_head_headers).body_empty(),
        );
        assert_eq!(get_captures["etag"], head_captures["etag"]);

        s3_tests::delete_object_retrying_operation_aborted(client, &bucket, key)
            .await
            .expect("delete shape fixture");
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}
