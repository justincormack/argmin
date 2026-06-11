/// Integration tests for malformed XML request bodies.
///
/// These cover the error paths in XML parsers (parse_delete_objects_xml,
/// parse_versioning_config_xml, parse_cors_config_xml, etc.) that are not
/// exercised by the AWS SDK, which always sends well-formed XML.
///
/// Each test sends a raw HTTP request with a deliberately broken XML body
/// and asserts the expected error response.
use ring::hmac;
use s3_tests::{unique_bucket, CTX};
use std::time::{SystemTime, UNIX_EPOCH};

// ── Helpers ─────────────────────────────────────────────────────────────

fn agent() -> s3_tests::Agent {
    s3_tests::test_agent()
}

async fn setup_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    bucket
}

async fn cleanup(bucket: &str) {
    let client = CTX.client();
    s3_tests::delete_bucket_retrying_operation_aborted(client, bucket).await;
}

fn assert_error_code(body: &str, code: &str) {
    let expected = format!("<Code>{}</Code>", code);
    assert!(
        body.contains(&expected),
        "expected {expected} in body: {body}",
    );
}

// ── SigV4 signing ──────────────────────────────────────────────────────

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

/// Normalize query string for SigV4: bare keys become "key=", then sort.
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

/// Send a signed request and return (status, body).
fn send_signed(
    method: &str,
    url_str: &str,
    body: &[u8],
    extra_headers: &[(&str, &str)],
) -> (u16, String) {
    let a = agent();
    let parsed = url::Url::parse(url_str).expect("parse URL");
    let path = parsed.path();
    let raw_query = parsed.query().unwrap_or("");
    let query = normalize_query(raw_query);

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

    let signed_headers: String = header_map
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical_headers: String = header_map
        .iter()
        .map(|(k, v)| format!("{k}:{v}\n"))
        .collect();
    let canonical_request =
        format!("{method}\n{path}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}");

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

    // PUT and POST accept a body; build with send(body).
    let mut request = match method {
        "PUT" => a.put(url_str),
        "POST" => a.post(url_str),
        _ => panic!("unsupported method: {method}"),
    }
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

/// Compute base64-encoded CRC32 checksum for AWS x-amz-checksum-crc32 header.
fn crc32_b64(data: &[u8]) -> String {
    let crc = checksum::crc32::checksum(data);
    base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        crc.to_be_bytes(),
    )
}

fn md5_b64(data: &[u8]) -> String {
    use md5_legacy::Digest;

    let digest = md5_legacy::Md5::digest(data);
    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &digest[..])
}

/// Convenience: send signed PUT with CRC32 checksum (required by AWS for some APIs).
fn signed_put_with_checksum(url: &str, body: &[u8], extra: &[(&str, &str)]) -> (u16, String) {
    let cksum = crc32_b64(body);
    let mut headers: Vec<(&str, &str)> = extra.to_vec();
    headers.push(("x-amz-checksum-crc32", &cksum));
    send_signed("PUT", url, body, &headers)
}

/// Convenience: send signed POST with checksums required by DeleteObjects.
fn signed_post_with_checksum(url: &str, body: &[u8], extra: &[(&str, &str)]) -> (u16, String) {
    let cksum = crc32_b64(body);
    let md5 = md5_b64(body);
    let mut headers: Vec<(&str, &str)> = extra.to_vec();
    headers.push(("x-amz-checksum-crc32", &cksum));
    headers.push(("content-md5", &md5));
    send_signed("POST", url, body, &headers)
}

/// Convenience: send signed POST.
fn signed_post(url: &str, body: &[u8], extra: &[(&str, &str)]) -> (u16, String) {
    send_signed("POST", url, body, extra)
}

// ── parse_delete_objects_xml error paths ─────────────────────────────

/// Malformed delete-objects XML with unclosed <Object> element.
#[test]
fn test_delete_objects_malformed_xml() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let url = format!("{}/{}?delete", CTX.endpoint(), bucket);
        let body = b"<Delete><Object><Key>k</Key></Delete>";
        let (status, body_text) = signed_post_with_checksum(&url, body, &[]);
        assert_eq!(status, 400, "body: {body_text}");
        assert_error_code(&body_text, "MalformedXML");
        cleanup(&bucket).await;
    });
}

/// Delete-objects XML with <Object> but missing <Key>.
#[test]
fn test_delete_objects_missing_key() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let url = format!("{}/{}?delete", CTX.endpoint(), bucket);
        let body = b"<Delete><Object><NotKey>x</NotKey></Object></Delete>";
        let (status, body_text) = signed_post_with_checksum(&url, body, &[]);
        assert_eq!(status, 400, "body: {body_text}");
        assert_error_code(&body_text, "MalformedXML");
        cleanup(&bucket).await;
    });
}

#[test]
fn test_delete_objects_oversized_key_rejected() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let url = format!("{}/{}?delete", CTX.endpoint(), bucket);
        let key = "a".repeat(1025);
        let body = format!("<Delete><Object><Key>{key}</Key></Object></Delete>");
        let (status, body_text) = signed_post_with_checksum(&url, body.as_bytes(), &[]);
        assert_eq!(status, 400, "body: {body_text}");
        assert_error_code(&body_text, "KeyTooLongError");
        assert!(body_text.contains("<Message>Your key is too long</Message>"));
        assert!(body_text.contains("<Size>1025</Size>"));
        assert!(body_text.contains("<MaxSizeAllowed>1024</MaxSizeAllowed>"));
        cleanup(&bucket).await;
    });
}

// ── parse_versioning_config_xml error paths ──────────────────────────

/// Versioning config XML missing the <Status> element.
#[test]
fn test_put_versioning_missing_status() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let url = format!("{}/{}?versioning", CTX.endpoint(), bucket);
        let body = b"<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"></VersioningConfiguration>";
        let (status, body_text) = signed_put_with_checksum(&url, body, &[]);
        assert_eq!(status, 400, "body: {body_text}");
        assert_error_code(&body_text, "IllegalVersioningConfigurationException");
        cleanup(&bucket).await;
    });
}

/// Versioning config XML with invalid status value.
#[test]
fn test_put_versioning_invalid_status() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let url = format!("{}/{}?versioning", CTX.endpoint(), bucket);
        let body = b"<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Status>Invalid</Status></VersioningConfiguration>";
        let (status, body_text) = signed_put_with_checksum(&url, body, &[]);
        assert_eq!(status, 400, "body: {body_text}");
        assert_error_code(&body_text, "MalformedXML");
        cleanup(&bucket).await;
    });
}

// ── parse_cors_config_xml error paths ───────────────────────────────

/// CORS config with unclosed <CORSRule>.
#[test]
fn test_put_cors_unclosed_rule() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let url = format!("{}/{}?cors", CTX.endpoint(), bucket);
        let body =
            b"<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin></CORSConfiguration>";
        let (status, body_text) = signed_put_with_checksum(&url, body, &[]);
        assert_eq!(status, 400, "body: {body_text}");
        assert_error_code(&body_text, "MalformedXML");
        cleanup(&bucket).await;
    });
}

/// CORS config with a rule that has no AllowedOrigin.
#[test]
fn test_put_cors_missing_origin() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let url = format!("{}/{}?cors", CTX.endpoint(), bucket);
        let body = b"<CORSConfiguration>\
            <CORSRule><AllowedMethod>GET</AllowedMethod></CORSRule>\
            </CORSConfiguration>";
        let (status, body_text) = signed_put_with_checksum(&url, body, &[]);
        assert_eq!(status, 400, "body: {body_text}");
        assert_error_code(&body_text, "MalformedXML");
        cleanup(&bucket).await;
    });
}

/// CORS config with a rule that has no AllowedMethod.
#[test]
fn test_put_cors_missing_method() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let url = format!("{}/{}?cors", CTX.endpoint(), bucket);
        let body = b"<CORSConfiguration>\
            <CORSRule><AllowedOrigin>*</AllowedOrigin></CORSRule>\
            </CORSConfiguration>";
        let (status, body_text) = signed_put_with_checksum(&url, body, &[]);
        assert_eq!(status, 400, "body: {body_text}");
        assert_error_code(&body_text, "MalformedXML");
        cleanup(&bucket).await;
    });
}

/// CORS config with no rules at all.
#[test]
fn test_put_cors_empty_config() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let url = format!("{}/{}?cors", CTX.endpoint(), bucket);
        let body = b"<CORSConfiguration></CORSConfiguration>";
        let (status, body_text) = signed_put_with_checksum(&url, body, &[]);
        assert_eq!(status, 400, "body: {body_text}");
        assert_error_code(&body_text, "MalformedXML");
        cleanup(&bucket).await;
    });
}

/// CORS config missing <CORSConfiguration> wrapper entirely.
#[test]
fn test_put_cors_missing_wrapper() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let url = format!("{}/{}?cors", CTX.endpoint(), bucket);
        let body = b"<CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule>";
        let (status, body_text) = signed_put_with_checksum(&url, body, &[]);
        assert_eq!(status, 400, "body: {body_text}");
        assert_error_code(&body_text, "MalformedXML");
        cleanup(&bucket).await;
    });
}

// ── parse_tagging_xml error paths ────────────────────────────────────

/// Tagging XML missing <Tagging> wrapper.
#[test]
fn test_put_bucket_tagging_malformed_xml() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let url = format!("{}/{}?tagging", CTX.endpoint(), bucket);
        let body = b"<TagSet><Tag><Key>k</Key><Value>v</Value></Tag></TagSet>";
        let (status, body_text) = signed_put_with_checksum(&url, body, &[]);
        assert_eq!(status, 400, "body: {body_text}");
        assert_error_code(&body_text, "MalformedXML");
        cleanup(&bucket).await;
    });
}

// ── parse_public_access_block_xml error paths ────────────────────────

/// Public access block XML missing the wrapper element.
#[test]
fn test_put_public_access_block_malformed_xml() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let url = format!("{}/{}?publicAccessBlock", CTX.endpoint(), bucket);
        let body = b"<BlockPublicAcls>true</BlockPublicAcls>";
        let (status, body_text) = signed_put_with_checksum(&url, body, &[]);
        assert_eq!(status, 400, "body: {body_text}");
        assert_error_code(&body_text, "MalformedXML");
        cleanup(&bucket).await;
    });
}

// ── parse_ownership_controls_xml error paths ─────────────────────────

/// Ownership controls missing <OwnershipControls> wrapper.
#[test]
fn test_put_ownership_controls_missing_wrapper() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let url = format!("{}/{}?ownershipControls", CTX.endpoint(), bucket);
        let body = b"<Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule>";
        let (status, body_text) = signed_put_with_checksum(&url, body, &[]);
        assert_eq!(status, 400, "body: {body_text}");
        assert_error_code(&body_text, "MalformedXML");
        cleanup(&bucket).await;
    });
}

/// Ownership controls missing <Rule>.
#[test]
fn test_put_ownership_controls_missing_rule() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let url = format!("{}/{}?ownershipControls", CTX.endpoint(), bucket);
        let body = b"<OwnershipControls></OwnershipControls>";
        let (status, body_text) = signed_put_with_checksum(&url, body, &[]);
        assert_eq!(status, 400, "body: {body_text}");
        assert_error_code(&body_text, "MalformedXML");
        cleanup(&bucket).await;
    });
}

/// Ownership controls missing <ObjectOwnership>.
#[test]
fn test_put_ownership_controls_missing_value() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let url = format!("{}/{}?ownershipControls", CTX.endpoint(), bucket);
        let body = b"<OwnershipControls><Rule></Rule></OwnershipControls>";
        let (status, body_text) = signed_put_with_checksum(&url, body, &[]);
        assert_eq!(status, 400, "body: {body_text}");
        assert_error_code(&body_text, "MalformedXML");
        cleanup(&bucket).await;
    });
}

// ── parse_complete_multipart_upload_xml error paths ──────────────────

/// Complete multipart upload with malformed XML (no <Part>).
#[test]
fn test_complete_multipart_malformed_xml() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "mpu-malformed";

        // Start a real multipart upload to get a valid upload ID.
        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let url = format!(
            "{}/{}/{}?uploadId={}",
            CTX.endpoint(),
            bucket,
            key,
            upload_id
        );
        let body = b"<CompleteMultipartUpload>not a part</CompleteMultipartUpload>";
        let (status, body_text) = signed_post(&url, body, &[]);
        assert_eq!(status, 400, "body: {body_text}");
        assert_error_code(&body_text, "MalformedXML");

        // Abort the upload to clean up.
        let _ = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await;
        cleanup(&bucket).await;
    });
}

/// Complete multipart upload with <Part> but missing <PartNumber>.
#[test]
fn test_complete_multipart_missing_part_number() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "mpu-no-pn";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let url = format!(
            "{}/{}/{}?uploadId={}",
            CTX.endpoint(),
            bucket,
            key,
            upload_id
        );
        let body = b"<CompleteMultipartUpload>\
            <Part><ETag>\"abc\"</ETag></Part>\
            </CompleteMultipartUpload>";
        let (status, body_text) = signed_post(&url, body, &[]);
        assert_eq!(status, 400, "body: {body_text}");
        assert_error_code(&body_text, "MalformedXML");

        let _ = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await;
        cleanup(&bucket).await;
    });
}

// ── CORS MaxAgeSeconds parse error ───────────────────────────────────

/// CORS config with non-numeric MaxAgeSeconds.
#[test]
fn test_put_cors_invalid_max_age() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let url = format!("{}/{}?cors", CTX.endpoint(), bucket);
        let body = b"<CORSConfiguration>\
            <CORSRule>\
            <AllowedOrigin>*</AllowedOrigin>\
            <AllowedMethod>GET</AllowedMethod>\
            <MaxAgeSeconds>abc</MaxAgeSeconds>\
            </CORSRule>\
            </CORSConfiguration>";
        let (status, body_text) = signed_put_with_checksum(&url, body, &[]);
        assert_eq!(status, 400, "body: {body_text}");
        assert_error_code(&body_text, "MalformedXML");
        cleanup(&bucket).await;
    });
}

// ── CORS invalid method error ───────────────────────────────────────

/// CORS config with an invalid HTTP method.
#[test]
fn test_put_cors_invalid_method() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let url = format!("{}/{}?cors", CTX.endpoint(), bucket);
        let body = b"<CORSConfiguration>\
            <CORSRule>\
            <AllowedOrigin>*</AllowedOrigin>\
            <AllowedMethod>PATCH</AllowedMethod>\
            </CORSRule>\
            </CORSConfiguration>";
        let (status, body_text) = signed_put_with_checksum(&url, body, &[]);
        assert_eq!(status, 400, "body: {body_text}");
        assert_error_code(&body_text, "InvalidRequest");
        cleanup(&bucket).await;
    });
}

// ── Non-UTF-8 body error paths ──────────────────────────────────────

/// Delete-objects with non-UTF-8 body bytes.
#[test]
fn test_delete_objects_non_utf8() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let url = format!("{}/{}?delete", CTX.endpoint(), bucket);
        let body: &[u8] = &[0xFF, 0xFE, 0x80, 0x81];
        let (status, body_text) = signed_post_with_checksum(&url, body, &[]);
        assert_eq!(status, 400, "body: {body_text}");
        assert_error_code(&body_text, "MalformedXML");
        cleanup(&bucket).await;
    });
}

/// Versioning config with non-UTF-8 body bytes.
#[test]
fn test_put_versioning_non_utf8() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let url = format!("{}/{}?versioning", CTX.endpoint(), bucket);
        let body: &[u8] = &[0xFF, 0xFE, 0x80, 0x81];
        let (status, body_text) = signed_put_with_checksum(&url, body, &[]);
        assert_eq!(status, 400, "body: {body_text}");
        assert_error_code(&body_text, "MalformedXML");
        cleanup(&bucket).await;
    });
}

/// CORS config with non-UTF-8 body bytes.
#[test]
fn test_put_cors_non_utf8() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let url = format!("{}/{}?cors", CTX.endpoint(), bucket);
        let body: &[u8] = &[0xFF, 0xFE, 0x80, 0x81];
        let (status, body_text) = signed_put_with_checksum(&url, body, &[]);
        assert_eq!(status, 400, "body: {body_text}");
        assert_error_code(&body_text, "MalformedXML");
        cleanup(&bucket).await;
    });
}

/// Tagging XML with non-UTF-8 body bytes.
#[test]
fn test_put_tagging_non_utf8() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let url = format!("{}/{}?tagging", CTX.endpoint(), bucket);
        let body: &[u8] = &[0xFF, 0xFE, 0x80, 0x81];
        let (status, body_text) = signed_put_with_checksum(&url, body, &[]);
        assert_eq!(status, 400, "body: {body_text}");
        assert_error_code(&body_text, "MalformedXML");
        cleanup(&bucket).await;
    });
}

/// Public access block with non-UTF-8 body bytes.
#[test]
fn test_put_public_access_block_non_utf8() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let url = format!("{}/{}?publicAccessBlock", CTX.endpoint(), bucket);
        let body: &[u8] = &[0xFF, 0xFE, 0x80, 0x81];
        let (status, body_text) = signed_put_with_checksum(&url, body, &[]);
        assert_eq!(status, 400, "body: {body_text}");
        assert_error_code(&body_text, "MalformedXML");
        cleanup(&bucket).await;
    });
}

/// Ownership controls with non-UTF-8 body bytes.
#[test]
fn test_put_ownership_controls_non_utf8() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let url = format!("{}/{}?ownershipControls", CTX.endpoint(), bucket);
        let body: &[u8] = &[0xFF, 0xFE, 0x80, 0x81];
        let (status, body_text) = signed_put_with_checksum(&url, body, &[]);
        assert_eq!(status, 400, "body: {body_text}");
        assert_error_code(&body_text, "MalformedXML");
        cleanup(&bucket).await;
    });
}

/// Complete multipart upload with non-UTF-8 body bytes.
#[test]
fn test_complete_multipart_non_utf8() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "mpu-utf8";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let url = format!(
            "{}/{}/{}?uploadId={}",
            CTX.endpoint(),
            bucket,
            key,
            upload_id
        );
        let body: &[u8] = &[0xFF, 0xFE, 0x80, 0x81];
        let (status, body_text) = signed_post(&url, body, &[]);
        assert_eq!(status, 400, "body: {body_text}");
        assert_error_code(&body_text, "MalformedXML");

        let _ = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await;
        cleanup(&bucket).await;
    });
}

// ── Tagging header with valid percent-encoding but invalid UTF-8 ────

/// PutObject with x-amz-tagging where percent-decoded bytes are not valid UTF-8.
#[test]
fn test_put_object_tagging_header_non_utf8_decoded() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "tag-nonutf8";
        let url = format!("{}/{}/{}", CTX.endpoint(), bucket, key);
        let body = b"hello";
        // %FF decodes to byte 0xFF which is not valid UTF-8
        let (status, body_text) = send_signed("PUT", &url, body, &[("x-amz-tagging", "foo=%FF")]);
        assert_eq!(status, 400, "body: {body_text}");
        assert_error_code(&body_text, "InvalidTag");
        cleanup(&bucket).await;
    });
}

/// Complete multipart upload with non-numeric PartNumber.
#[test]
fn test_complete_multipart_invalid_part_number() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "mpu-badpn";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let url = format!(
            "{}/{}/{}?uploadId={}",
            CTX.endpoint(),
            bucket,
            key,
            upload_id
        );
        let body = b"<CompleteMultipartUpload>\
            <Part><PartNumber>abc</PartNumber><ETag>\"x\"</ETag></Part>\
            </CompleteMultipartUpload>";
        let (status, body_text) = signed_post(&url, body, &[]);
        assert_eq!(status, 400, "body: {body_text}");
        assert_error_code(&body_text, "MalformedXML");

        let _ = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await;
        cleanup(&bucket).await;
    });
}

// ── percent_decode_tag / decode_hex_pair error paths ─────────────────

/// PutObject with x-amz-tagging containing invalid percent-encoding.
/// This exercises percent_decode_tag and decode_hex_pair.
#[test]
fn test_put_object_tagging_header_bad_percent_encoding() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "tag-pct";
        let url = format!("{}/{}/{}", CTX.endpoint(), bucket, key);
        let body = b"hello";
        // %ZZ is not valid hex
        let (status, body_text) = send_signed("PUT", &url, body, &[("x-amz-tagging", "foo=%ZZ")]);
        assert_eq!(status, 400, "body: {body_text}");
        assert_error_code(&body_text, "InvalidArgument");
        // Clean up: key was not created, just delete bucket
        cleanup(&bucket).await;
    });
}

/// PutObject with x-amz-tagging containing truncated percent-encoding.
#[test]
fn test_put_object_tagging_header_truncated_percent() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "tag-trunc";
        let url = format!("{}/{}/{}", CTX.endpoint(), bucket, key);
        let body = b"hello";
        // %A is incomplete (needs two hex digits)
        let (status, body_text) = send_signed("PUT", &url, body, &[("x-amz-tagging", "foo=%A")]);
        assert_eq!(status, 400, "body: {body_text}");
        assert_error_code(&body_text, "InvalidArgument");
        cleanup(&bucket).await;
    });
}
