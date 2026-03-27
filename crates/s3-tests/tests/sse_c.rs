use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{ChecksumAlgorithm, ChecksumMode};
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use ring::hmac;
use s3_tests::{
    assert_s3_err_code, err_status, sse_c_header_values, test_sse_c_key, unique_bucket, CTX,
};
use std::time::{SystemTime, UNIX_EPOCH};

macro_rules! with_sse_c_headers {
    ($op:expr, $key_b64:expr, $key_md5_b64:expr) => {{
        $op.customize().mutate_request({
            let key_b64 = $key_b64.clone();
            let key_md5_b64 = $key_md5_b64.clone();
            move |req| {
                req.headers_mut()
                    .insert("x-amz-server-side-encryption-customer-algorithm", "AES256");
                req.headers_mut()
                    .insert("x-amz-server-side-encryption-customer-key", key_b64.clone());
                req.headers_mut().insert(
                    "x-amz-server-side-encryption-customer-key-md5",
                    key_md5_b64.clone(),
                );
            }
        })
    }};
}

macro_rules! with_sse_c_copy_headers {
    ($op:expr, $src_key_b64:expr, $src_key_md5_b64:expr, $dst_key_b64:expr, $dst_key_md5_b64:expr) => {{
        $op.customize().mutate_request({
            let src_key_b64 = $src_key_b64.clone();
            let src_key_md5_b64 = $src_key_md5_b64.clone();
            let dst_key_b64 = $dst_key_b64.clone();
            let dst_key_md5_b64 = $dst_key_md5_b64.clone();
            move |req| {
                req.headers_mut()
                    .insert("x-amz-server-side-encryption-customer-algorithm", "AES256");
                req.headers_mut().insert(
                    "x-amz-server-side-encryption-customer-key",
                    dst_key_b64.clone(),
                );
                req.headers_mut().insert(
                    "x-amz-server-side-encryption-customer-key-md5",
                    dst_key_md5_b64.clone(),
                );
                req.headers_mut().insert(
                    "x-amz-copy-source-server-side-encryption-customer-algorithm",
                    "AES256",
                );
                req.headers_mut().insert(
                    "x-amz-copy-source-server-side-encryption-customer-key",
                    src_key_b64.clone(),
                );
                req.headers_mut().insert(
                    "x-amz-copy-source-server-side-encryption-customer-key-md5",
                    src_key_md5_b64.clone(),
                );
            }
        })
    }};
}

async fn cleanup(bucket: &str, key: &str) {
    let client = CTX.client();
    let _ = client.delete_object().bucket(bucket).key(key).send().await;
    client.delete_bucket().bucket(bucket).send().await.unwrap();
}

async fn cleanup_multipart(bucket: &str, key: &str, upload_id: &str) {
    let client = CTX.client();
    let _ = client
        .abort_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .send()
        .await;
    cleanup(bucket, key).await;
}

fn agent() -> ureq::Agent {
    ureq::Agent::config_builder()
        .http_status_as_error(false)
        .build()
        .new_agent()
}

fn sha256_hex(data: &[u8]) -> String {
    let d = ring::digest::digest(&ring::digest::SHA256, data);
    d.as_ref().iter().map(|b| format!("{b:02x}")).collect()
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let k = hmac::Key::new(hmac::HMAC_SHA256, key);
    hmac::sign(&k, data).as_ref().to_vec()
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

fn signed_put(url_str: &str, body: &[u8], extra_headers: &[(&str, &str)]) -> (u16, String) {
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
        header_map.push((k.to_ascii_lowercase(), v.to_string()));
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
        format!("PUT\n{path}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}");
    let cr_hash = sha256_hex(canonical_request.as_bytes());
    let scope = format!("{date_stamp}/{region}/s3/aws4_request");
    let string_to_sign = format!("AWS4-HMAC-SHA256\n{dt}\n{scope}\n{cr_hash}");

    let k_date = hmac_sha256(
        format!("AWS4{secret_key}").as_bytes(),
        date_stamp.as_bytes(),
    );
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, b"s3");
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    let signature: String = hmac_sha256(&k_signing, string_to_sign.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();

    let auth_header = format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
    );

    let mut request = agent()
        .put(url_str)
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

fn xml_tag<'a>(body: &'a str, tag: &str) -> Option<&'a str> {
    let start = format!("<{tag}>");
    let end = format!("</{tag}>");
    let start_idx = body.find(&start)? + start.len();
    let end_idx = body[start_idx..].find(&end)? + start_idx;
    Some(&body[start_idx..end_idx])
}

#[test]
fn test_sse_c_put_get_head_round_trip() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&key);
        let body = b"hello sse-c".to_vec();

        with_sse_c_headers!(
            client
                .put_object()
                .bucket(&bucket)
                .key("obj")
                .body(ByteStream::from(body.clone())),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();

        let head = with_sse_c_headers!(
            client.head_object().bucket(&bucket).key("obj"),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        assert_eq!(head.content_length(), Some(body.len() as i64));
        assert!(head.e_tag().is_some());
        assert_eq!(head.sse_customer_algorithm(), Some("AES256"));
        assert_eq!(head.sse_customer_key_md5(), Some(key_md5_b64.as_str()));

        let get = with_sse_c_headers!(
            client.get_object().bucket(&bucket).key("obj"),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        assert_eq!(get.sse_customer_algorithm(), Some("AES256"));
        assert_eq!(get.sse_customer_key_md5(), Some(key_md5_b64.as_str()));
        assert_eq!(
            get.body.collect().await.unwrap().into_bytes().as_ref(),
            body.as_slice()
        );

        cleanup(&bucket, "obj").await;
    });
}

#[test]
fn test_sse_c_get_requires_headers() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&key);

        with_sse_c_headers!(
            client
                .put_object()
                .bucket(&bucket)
                .key("obj")
                .body(ByteStream::from_static(b"secret")),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();

        let result = client.get_object().bucket(&bucket).key("obj").send().await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidRequest");

        cleanup(&bucket, "obj").await;
    });
}

#[test]
fn test_sse_c_head_requires_headers() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&key);

        with_sse_c_headers!(
            client
                .put_object()
                .bucket(&bucket)
                .key("obj")
                .body(ByteStream::from_static(b"secret")),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();

        let result = client.head_object().bucket(&bucket).key("obj").send().await;
        assert_eq!(err_status(&result), 400);

        cleanup(&bucket, "obj").await;
    });
}

#[test]
fn test_sse_c_get_rejects_wrong_key() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&key);
        let wrong_key = [42u8; 32];
        let (wrong_key_b64, wrong_key_md5_b64) = sse_c_header_values(&wrong_key);

        with_sse_c_headers!(
            client
                .put_object()
                .bucket(&bucket)
                .key("obj")
                .body(ByteStream::from_static(b"secret")),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();

        let result = with_sse_c_headers!(
            client.get_object().bucket(&bucket).key("obj"),
            wrong_key_b64,
            wrong_key_md5_b64
        )
        .send()
        .await;
        assert_eq!(err_status(&result), 403);
        assert_s3_err_code(&result, "AccessDenied");

        cleanup(&bucket, "obj").await;
    });
}

#[test]
fn test_sse_c_head_checksum_mode_uses_customer_key() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&key);
        let checksum_sha256 = "arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0=";

        with_sse_c_headers!(
            client
                .put_object()
                .bucket(&bucket)
                .key("obj")
                .body(ByteStream::from(vec![b'A'; 1024]))
                .checksum_algorithm(ChecksumAlgorithm::Sha256)
                .checksum_sha256(checksum_sha256),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();

        let head = with_sse_c_headers!(
            client
                .head_object()
                .bucket(&bucket)
                .key("obj")
                .checksum_mode(ChecksumMode::Enabled),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        assert_eq!(head.checksum_sha256(), Some(checksum_sha256));
        assert_eq!(head.sse_customer_algorithm(), Some("AES256"));
        assert_eq!(head.sse_customer_key_md5(), Some(key_md5_b64.as_str()));

        cleanup(&bucket, "obj").await;
    });
}

#[test]
fn test_sse_c_multipart_round_trip() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&key);
        let wrong_key = [42u8; 32];
        let (wrong_key_b64, wrong_key_md5_b64) = sse_c_header_values(&wrong_key);
        let body = b"hello multipart sse-c".to_vec();

        let create = with_sse_c_headers!(
            client.create_multipart_upload().bucket(&bucket).key("obj"),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let part = with_sse_c_headers!(
            client
                .upload_part()
                .bucket(&bucket)
                .key("obj")
                .upload_id(&upload_id)
                .part_number(1)
                .body(ByteStream::from(body.clone())),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        let etag = part.e_tag().unwrap().to_string();

        with_sse_c_headers!(
            client
                .complete_multipart_upload()
                .bucket(&bucket)
                .key("obj")
                .upload_id(&upload_id)
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .parts(CompletedPart::builder().e_tag(etag).part_number(1).build())
                        .build()
                ),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();

        let head = with_sse_c_headers!(
            client.head_object().bucket(&bucket).key("obj"),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        assert_eq!(head.content_length(), Some(body.len() as i64));
        assert_eq!(head.sse_customer_algorithm(), Some("AES256"));
        assert_eq!(head.sse_customer_key_md5(), Some(key_md5_b64.as_str()));

        let get = with_sse_c_headers!(
            client.get_object().bucket(&bucket).key("obj"),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        assert_eq!(get.sse_customer_algorithm(), Some("AES256"));
        assert_eq!(get.sse_customer_key_md5(), Some(key_md5_b64.as_str()));
        assert_eq!(
            get.body.collect().await.unwrap().into_bytes().as_ref(),
            body.as_slice()
        );

        let missing_headers = client.get_object().bucket(&bucket).key("obj").send().await;
        assert_eq!(err_status(&missing_headers), 400);
        assert_s3_err_code(&missing_headers, "InvalidRequest");

        let wrong_key_result = with_sse_c_headers!(
            client.get_object().bucket(&bucket).key("obj"),
            wrong_key_b64,
            wrong_key_md5_b64
        )
        .send()
        .await;
        assert_eq!(err_status(&wrong_key_result), 403);
        assert_s3_err_code(&wrong_key_result, "AccessDenied");

        cleanup(&bucket, "obj").await;
    });
}

#[test]
fn test_sse_c_upload_part_requires_headers() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&key);
        let create = with_sse_c_headers!(
            client.create_multipart_upload().bucket(&bucket).key("obj"),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let result = client
            .upload_part()
            .bucket(&bucket)
            .key("obj")
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from_static(b"secret"))
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidRequest");

        cleanup_multipart(&bucket, "obj", &upload_id).await;
    });
}

#[test]
fn test_sse_c_copy_object_round_trip() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let src_key = test_sse_c_key();
        let (src_key_b64, src_key_md5_b64) = sse_c_header_values(&src_key);
        let dst_key = [7u8; 32];
        let (dst_key_b64, dst_key_md5_b64) = sse_c_header_values(&dst_key);
        let body = b"hello sse-c copy".to_vec();

        with_sse_c_headers!(
            client
                .put_object()
                .bucket(&bucket)
                .key("src")
                .body(ByteStream::from(body.clone())),
            src_key_b64,
            src_key_md5_b64
        )
        .send()
        .await
        .unwrap();

        with_sse_c_copy_headers!(
            client
                .copy_object()
                .bucket(&bucket)
                .key("dst")
                .copy_source(format!("{}/src", bucket)),
            src_key_b64,
            src_key_md5_b64,
            dst_key_b64,
            dst_key_md5_b64
        )
        .send()
        .await
        .unwrap();

        let head = with_sse_c_headers!(
            client.head_object().bucket(&bucket).key("dst"),
            dst_key_b64,
            dst_key_md5_b64
        )
        .send()
        .await
        .unwrap();
        assert_eq!(head.content_length(), Some(body.len() as i64));

        let get = with_sse_c_headers!(
            client.get_object().bucket(&bucket).key("dst"),
            dst_key_b64,
            dst_key_md5_b64
        )
        .send()
        .await
        .unwrap();
        assert_eq!(
            get.body.collect().await.unwrap().into_bytes().as_ref(),
            body.as_slice()
        );

        let client = CTX.client();
        let _ = client
            .delete_object()
            .bucket(&bucket)
            .key("src")
            .send()
            .await;
        let _ = client
            .delete_object()
            .bucket(&bucket)
            .key("dst")
            .send()
            .await;
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_sse_c_copy_object_requires_source_headers() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&key);

        with_sse_c_headers!(
            client
                .put_object()
                .bucket(&bucket)
                .key("src")
                .body(ByteStream::from_static(b"secret-copy")),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();

        let result = client
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src", bucket))
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidRequest");

        let client = CTX.client();
        let _ = client
            .delete_object()
            .bucket(&bucket)
            .key("src")
            .send()
            .await;
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_sse_c_upload_part_copy_round_trip() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let src_key = test_sse_c_key();
        let (src_key_b64, src_key_md5_b64) = sse_c_header_values(&src_key);
        let dst_key = [9u8; 32];
        let (dst_key_b64, dst_key_md5_b64) = sse_c_header_values(&dst_key);
        let body = b"hello multipart copy sse-c".to_vec();

        with_sse_c_headers!(
            client
                .put_object()
                .bucket(&bucket)
                .key("src")
                .body(ByteStream::from(body.clone())),
            src_key_b64,
            src_key_md5_b64
        )
        .send()
        .await
        .unwrap();

        let create = with_sse_c_headers!(
            client.create_multipart_upload().bucket(&bucket).key("dst"),
            dst_key_b64,
            dst_key_md5_b64
        )
        .send()
        .await
        .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let url = format!(
            "{}/{}/dst?partNumber=1&uploadId={}",
            CTX.endpoint(),
            bucket,
            upload_id
        );
        let copy_source = format!("{}/src", bucket);
        let headers = [
            ("x-amz-copy-source", copy_source.as_str()),
            (
                "x-amz-copy-source-server-side-encryption-customer-algorithm",
                "AES256",
            ),
            (
                "x-amz-copy-source-server-side-encryption-customer-key",
                src_key_b64.as_str(),
            ),
            (
                "x-amz-copy-source-server-side-encryption-customer-key-md5",
                src_key_md5_b64.as_str(),
            ),
            ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
            (
                "x-amz-server-side-encryption-customer-key",
                dst_key_b64.as_str(),
            ),
            (
                "x-amz-server-side-encryption-customer-key-md5",
                dst_key_md5_b64.as_str(),
            ),
        ];
        let (status, body_text) = signed_put(&url, &[], &headers);
        assert_eq!(status, 200, "body: {body_text}");
        let etag = xml_tag(&body_text, "ETag")
            .expect("copy part response etag")
            .replace("&quot;", "\"")
            .to_string();

        with_sse_c_headers!(
            client
                .complete_multipart_upload()
                .bucket(&bucket)
                .key("dst")
                .upload_id(&upload_id)
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .parts(CompletedPart::builder().e_tag(etag).part_number(1).build())
                        .build(),
                ),
            dst_key_b64,
            dst_key_md5_b64
        )
        .send()
        .await
        .unwrap();

        let get = with_sse_c_headers!(
            client.get_object().bucket(&bucket).key("dst"),
            dst_key_b64,
            dst_key_md5_b64
        )
        .send()
        .await
        .unwrap();
        assert_eq!(
            get.body.collect().await.unwrap().into_bytes().as_ref(),
            body.as_slice()
        );

        let client = CTX.client();
        let _ = client
            .delete_object()
            .bucket(&bucket)
            .key("src")
            .send()
            .await;
        let _ = client
            .delete_object()
            .bucket(&bucket)
            .key("dst")
            .send()
            .await;
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_sse_c_upload_part_copy_requires_source_headers() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let src_key = test_sse_c_key();
        let (src_key_b64, src_key_md5_b64) = sse_c_header_values(&src_key);
        let dst_key = [9u8; 32];
        let (dst_key_b64, dst_key_md5_b64) = sse_c_header_values(&dst_key);

        with_sse_c_headers!(
            client
                .put_object()
                .bucket(&bucket)
                .key("src")
                .body(ByteStream::from_static(b"secret-copy-part")),
            src_key_b64,
            src_key_md5_b64
        )
        .send()
        .await
        .unwrap();

        let create = with_sse_c_headers!(
            client.create_multipart_upload().bucket(&bucket).key("dst"),
            dst_key_b64,
            dst_key_md5_b64
        )
        .send()
        .await
        .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let result = with_sse_c_headers!(
            client
                .upload_part_copy()
                .bucket(&bucket)
                .key("dst")
                .upload_id(&upload_id)
                .part_number(1)
                .copy_source(format!("{}/src", bucket)),
            dst_key_b64,
            dst_key_md5_b64
        )
        .send()
        .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidRequest");

        let client = CTX.client();
        let _ = client
            .delete_object()
            .bucket(&bucket)
            .key("src")
            .send()
            .await;
        let _ = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("dst")
            .upload_id(&upload_id)
            .send()
            .await;
        let _ = client
            .delete_object()
            .bucket(&bucket)
            .key("dst")
            .send()
            .await;
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}
