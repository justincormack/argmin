use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, ChecksumAlgorithm, ChecksumMode, ObjectAttributes,
    VersioningConfiguration,
};
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use ring::hmac;
use s3_tests::{
    assert_s3_err_code, create_bucket_with_sse_c_enabled, delete_all_and_bucket, err_status,
    raw_object_with,
    shape::{assert_shape, error_response_headers, expected_error, shape},
    sse_c_header_values, test_sse_c_key, unique_bucket, SendRetryingOperationAborted, CTX,
};
use std::time::{SystemTime, UNIX_EPOCH};

const MULTIPART_MIN_PART_SIZE: usize = 5 * 1024 * 1024;
// Matches the server's internal segment boundary where SSE-C ciphertext adds
// one 16-byte authentication tag and previously exposed a length-accounting
// regression.
const SSE_C_SEGMENT_BOUNDARY_SIZE: usize = 8 * 1024 * 1024;

const fn mib(size: usize) -> usize {
    size * 1024 * 1024
}

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

macro_rules! sse_c_single_part_round_trip_tests {
    ($( $name:ident => ($label:expr, $size:expr, $seed:expr), )* ) => {
        $(
            #[test]
            fn $name() {
                require_https_endpoint();
                s3_tests::run(async {
                    assert_sse_c_put_get_head_round_trip_size($size, $seed, $label).await;
                });
            }
        )*
    };
}

macro_rules! sse_c_multipart_round_trip_tests {
    ($( $name:ident => ($label:expr, $size:expr, $seed:expr), )* ) => {
        $(
            #[test]
            fn $name() {
                require_https_endpoint();
                s3_tests::run(async {
                    assert_sse_c_multipart_round_trip_size($size, $seed, $label).await;
                });
            }
        )*
    };
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

async fn put_sse_c_object_retrying_operation_aborted(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    body: Vec<u8>,
    key_b64: &str,
    key_md5_b64: &str,
) -> aws_sdk_s3::operation::put_object::PutObjectOutput {
    let key_b64 = key_b64.to_owned();
    let key_md5_b64 = key_md5_b64.to_owned();
    s3_tests::retrying_operation_aborted("put SSE-C object", || {
        let body = body.clone();
        let key_b64 = key_b64.clone();
        let key_md5_b64 = key_md5_b64.clone();
        async move {
            with_sse_c_headers!(
                client
                    .put_object()
                    .bucket(bucket)
                    .key(key)
                    .body(ByteStream::from(body)),
                key_b64,
                key_md5_b64
            )
            .send()
            .await
        }
    })
    .await
}

async fn put_sse_c_object_with_sha256_retrying_operation_aborted(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    body: Vec<u8>,
    key_b64: &str,
    key_md5_b64: &str,
    checksum_sha256: &str,
) -> aws_sdk_s3::operation::put_object::PutObjectOutput {
    let key_b64 = key_b64.to_owned();
    let key_md5_b64 = key_md5_b64.to_owned();
    let checksum_sha256 = checksum_sha256.to_owned();
    s3_tests::retrying_operation_aborted("put SSE-C object with checksum", || {
        let body = body.clone();
        let key_b64 = key_b64.clone();
        let key_md5_b64 = key_md5_b64.clone();
        let checksum_sha256 = checksum_sha256.clone();
        async move {
            with_sse_c_headers!(
                client
                    .put_object()
                    .bucket(bucket)
                    .key(key)
                    .body(ByteStream::from(body))
                    .checksum_algorithm(ChecksumAlgorithm::Sha256)
                    .checksum_sha256(checksum_sha256),
                key_b64,
                key_md5_b64
            )
            .send()
            .await
        }
    })
    .await
}

async fn cleanup(bucket: &str, key: &str) {
    let client = CTX.client();
    let _ = s3_tests::delete_object_retrying_operation_aborted(client, bucket, key).await;

    for _ in 0..10 {
        let uploads = match client
            .list_multipart_uploads()
            .bucket(bucket)
            .send_retrying_operation_aborted("list multipart uploads during SSE-C cleanup")
            .await
        {
            Ok(uploads) => uploads,
            Err(err)
                if err.as_service_error().and_then(ProvideErrorMetadata::code)
                    == Some("NoSuchBucket") =>
            {
                return;
            }
            Err(err) => panic!("list multipart uploads during SSE-C cleanup: {err:?}"),
        };
        for upload in uploads.uploads() {
            let _ = client
                .abort_multipart_upload()
                .bucket(bucket)
                .key(upload.key().unwrap_or_default())
                .upload_id(upload.upload_id().unwrap_or_default())
                .send_retrying_operation_aborted("abort SSE-C multipart upload during cleanup")
                .await;
        }

        match client
            .delete_bucket()
            .bucket(bucket)
            .send_retrying_operation_aborted("delete SSE-C bucket during cleanup")
            .await
        {
            Ok(_) => return,
            Err(err) => {
                if err.as_service_error().and_then(ProvideErrorMetadata::code)
                    == Some("NoSuchBucket")
                {
                    return;
                }
                let raw = format!("{err:?}");
                if raw.contains("OperationAborted") || raw.contains("BucketNotEmpty") {
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    continue;
                }
                panic!("delete_bucket failed unexpectedly: {raw}");
            }
        }
    }

    s3_tests::delete_bucket_retrying_operation_aborted(client, bucket).await;
}

async fn cleanup_versioned(bucket: &str, key: &str, version_ids: &[String]) {
    let client = CTX.client();
    for version_id in version_ids {
        client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .version_id(version_id)
            .send()
            .await
            .unwrap();
    }
    s3_tests::delete_bucket_retrying_operation_aborted(client, bucket).await;
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

fn agent() -> s3_tests::Agent {
    s3_tests::test_agent()
}

fn endpoint_is_https() -> bool {
    CTX.endpoint().starts_with("https://")
}

fn require_https_endpoint() {
    assert!(
        endpoint_is_https(),
        "SSE-C coverage requires an https:// endpoint; got {}",
        CTX.endpoint()
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

fn patterned_bytes(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| seed.wrapping_add((i % 251) as u8))
        .collect()
}

async fn setup_sse_c_object(bucket: &str, key: &str, body: Vec<u8>) -> (String, String, Vec<u8>) {
    let client = CTX.client();
    s3_tests::create_bucket_with_sse_c_enabled(client, bucket)
        .await
        .unwrap();

    let customer_key = test_sse_c_key();
    let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);
    put_sse_c_object_retrying_operation_aborted(
        client,
        bucket,
        key,
        body.clone(),
        &key_b64,
        &key_md5_b64,
    )
    .await;

    (key_b64, key_md5_b64, body)
}

async fn assert_sse_c_put_get_head_round_trip_size(size: usize, seed: u8, label: &str) {
    let client = CTX.client();
    let bucket = unique_bucket();
    let object_key = format!("obj-{label}");
    s3_tests::create_bucket_with_sse_c_enabled(client, &bucket)
        .await
        .unwrap();

    let key = test_sse_c_key();
    let (key_b64, key_md5_b64) = sse_c_header_values(&key);
    let body = patterned_bytes(size, seed);

    put_sse_c_object_retrying_operation_aborted(
        client,
        &bucket,
        &object_key,
        body.clone(),
        &key_b64,
        &key_md5_b64,
    )
    .await;

    let head = with_sse_c_headers!(
        client.head_object().bucket(&bucket).key(&object_key),
        key_b64,
        key_md5_b64
    )
    .send()
    .await
    .unwrap();
    assert_eq!(
        head.content_length(),
        Some(body.len() as i64),
        "HEAD content length mismatch for {label}",
    );
    assert_eq!(
        head.sse_customer_algorithm(),
        Some("AES256"),
        "HEAD SSE-C algorithm mismatch for {label}",
    );
    assert_eq!(
        head.sse_customer_key_md5(),
        Some(key_md5_b64.as_str()),
        "HEAD SSE-C key MD5 mismatch for {label}",
    );

    let get = with_sse_c_headers!(
        client.get_object().bucket(&bucket).key(&object_key),
        key_b64,
        key_md5_b64
    )
    .send()
    .await
    .unwrap();
    assert_eq!(
        get.sse_customer_algorithm(),
        Some("AES256"),
        "GET SSE-C algorithm mismatch for {label}",
    );
    assert_eq!(
        get.sse_customer_key_md5(),
        Some(key_md5_b64.as_str()),
        "GET SSE-C key MD5 mismatch for {label}",
    );
    assert_eq!(
        get.body.collect().await.unwrap().into_bytes().as_ref(),
        body.as_slice(),
        "GET body mismatch for {label}",
    );

    cleanup(&bucket, &object_key).await;
}

async fn assert_sse_c_multipart_round_trip_size(size: usize, seed: u8, label: &str) {
    let client = CTX.client();
    let bucket = unique_bucket();
    let object_key = format!("obj-{label}");
    s3_tests::create_bucket_with_sse_c_enabled(client, &bucket)
        .await
        .unwrap();

    let key = test_sse_c_key();
    let (key_b64, key_md5_b64) = sse_c_header_values(&key);
    let body = patterned_bytes(size, seed);

    let create = with_sse_c_headers!(
        client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(&object_key),
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
            .key(&object_key)
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
            .key(&object_key)
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
        client.head_object().bucket(&bucket).key(&object_key),
        key_b64,
        key_md5_b64
    )
    .send()
    .await
    .unwrap();
    assert_eq!(
        head.content_length(),
        Some(body.len() as i64),
        "multipart HEAD content length mismatch for {label}",
    );
    assert_eq!(
        head.sse_customer_algorithm(),
        Some("AES256"),
        "multipart HEAD SSE-C algorithm mismatch for {label}",
    );
    assert_eq!(
        head.sse_customer_key_md5(),
        Some(key_md5_b64.as_str()),
        "multipart HEAD SSE-C key MD5 mismatch for {label}",
    );

    let get = with_sse_c_headers!(
        client.get_object().bucket(&bucket).key(&object_key),
        key_b64,
        key_md5_b64
    )
    .send()
    .await
    .unwrap();
    assert_eq!(
        get.sse_customer_algorithm(),
        Some("AES256"),
        "multipart GET SSE-C algorithm mismatch for {label}",
    );
    assert_eq!(
        get.sse_customer_key_md5(),
        Some(key_md5_b64.as_str()),
        "multipart GET SSE-C key MD5 mismatch for {label}",
    );
    assert_eq!(
        get.body.collect().await.unwrap().into_bytes().as_ref(),
        body.as_slice(),
        "multipart GET body mismatch for {label}",
    );

    cleanup(&bucket, &object_key).await;
}

#[test]
fn test_sse_c_put_get_head_round_trip() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_with_sse_c_enabled(client, &bucket)
            .await
            .unwrap();

        let key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&key);
        let body = b"hello sse-c".to_vec();

        put_sse_c_object_retrying_operation_aborted(
            client,
            &bucket,
            "obj",
            body.clone(),
            &key_b64,
            &key_md5_b64,
        )
        .await;

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

sse_c_single_part_round_trip_tests! {
    test_sse_c_put_get_head_round_trip_empty => ("empty", 0, 0x11),
    test_sse_c_put_get_head_round_trip_one_mib => ("one-mib", mib(1), 0x13),
    test_sse_c_put_get_head_round_trip_four_mib => ("four-mib", mib(4), 0x17),
    test_sse_c_put_get_head_round_trip_segment_minus_one => (
        "segment-minus-one",
        SSE_C_SEGMENT_BOUNDARY_SIZE - 1,
        0x1d
    ),
    test_sse_c_put_get_head_round_trip_segment_boundary => (
        "segment-boundary",
        SSE_C_SEGMENT_BOUNDARY_SIZE,
        0x23
    ),
    test_sse_c_put_get_head_round_trip_segment_plus_one => (
        "segment-plus-one",
        SSE_C_SEGMENT_BOUNDARY_SIZE + 1,
        0x29
    ),
    test_sse_c_put_get_head_round_trip_ten_mib => ("ten-mib", mib(10), 0x2f),
    test_sse_c_put_get_head_round_trip_two_segments => ("two-segments", mib(16), 0x35),
    test_sse_c_put_get_head_round_trip_irregular_nine_mib_plus => (
        "irregular-nine-mib-plus",
        mib(9) + 12_345,
        0x3b
    ),
}

#[test]
fn test_sse_c_range_get_single_part_headers_and_body() {
    require_https_endpoint();
    s3_tests::run(async {
        let bucket = unique_bucket();
        let (key_b64, key_md5_b64, body) = setup_sse_c_object(
            &bucket,
            "obj",
            patterned_bytes(SSE_C_SEGMENT_BOUNDARY_SIZE + 257, 0x44),
        )
        .await;

        let start = SSE_C_SEGMENT_BOUNDARY_SIZE - 32;
        let end = SSE_C_SEGMENT_BOUNDARY_SIZE + 32;
        let resp = with_sse_c_headers!(
            CTX.client()
                .get_object()
                .bucket(&bucket)
                .key("obj")
                .range(format!("bytes={start}-{end}")),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();

        assert_eq!(resp.accept_ranges(), Some("bytes"));
        assert_eq!(
            resp.content_range(),
            Some(format!("bytes {start}-{end}/{}", body.len()).as_str())
        );
        assert_eq!(resp.content_length(), Some((end - start + 1) as i64));
        assert_eq!(resp.sse_customer_algorithm(), Some("AES256"));
        assert_eq!(resp.sse_customer_key_md5(), Some(key_md5_b64.as_str()));
        let got = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(got.as_ref(), &body[start..=end]);

        cleanup(&bucket, "obj").await;
    });
}

#[test]
fn test_sse_c_range_get_unsatisfiable_returns_invalid_range() {
    require_https_endpoint();
    s3_tests::run(async {
        let bucket = unique_bucket();
        let (key_b64, key_md5_b64, _) =
            setup_sse_c_object(&bucket, "obj", patterned_bytes(26, 0x21)).await;

        let result = with_sse_c_headers!(
            CTX.client()
                .get_object()
                .bucket(&bucket)
                .key("obj")
                .range("bytes=100-200"),
            key_b64,
            key_md5_b64
        )
        .send()
        .await;
        assert_eq!(err_status(&result), 416);
        assert_s3_err_code(&result, "InvalidRange");

        cleanup(&bucket, "obj").await;
    });
}

#[test]
fn test_sse_c_range_get_empty_object_is_unsatisfiable() {
    require_https_endpoint();
    s3_tests::run(async {
        let bucket = unique_bucket();
        let (key_b64, key_md5_b64, _) = setup_sse_c_object(&bucket, "obj", Vec::new()).await;

        let result = with_sse_c_headers!(
            CTX.client()
                .get_object()
                .bucket(&bucket)
                .key("obj")
                .range("bytes=0-0"),
            key_b64,
            key_md5_b64
        )
        .send()
        .await;
        assert_eq!(err_status(&result), 416);
        assert_s3_err_code(&result, "InvalidRange");

        cleanup(&bucket, "obj").await;
    });
}

#[test]
fn test_sse_c_range_get_malformed_header_returns_full_object() {
    require_https_endpoint();
    s3_tests::run(async {
        let bucket = unique_bucket();
        let (key_b64, key_md5_b64, body) =
            setup_sse_c_object(&bucket, "obj", patterned_bytes(64, 0x55)).await;

        let resp = with_sse_c_headers!(
            CTX.client()
                .get_object()
                .bucket(&bucket)
                .key("obj")
                .range("bytes=10-5"),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        assert_eq!(resp.content_range(), None);
        assert_eq!(resp.content_length(), Some(body.len() as i64));
        assert_eq!(resp.accept_ranges(), Some("bytes"));
        let got = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(got.as_ref(), body.as_slice());

        cleanup(&bucket, "obj").await;
    });
}

#[test]
fn test_sse_c_range_get_if_match_returns_partial_content() {
    require_https_endpoint();
    s3_tests::run(async {
        let bucket = unique_bucket();
        let (key_b64, key_md5_b64, body) =
            setup_sse_c_object(&bucket, "obj", patterned_bytes(128, 0x66)).await;

        let head = with_sse_c_headers!(
            CTX.client().head_object().bucket(&bucket).key("obj"),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        let etag = head.e_tag().unwrap().to_string();

        let resp = with_sse_c_headers!(
            CTX.client()
                .get_object()
                .bucket(&bucket)
                .key("obj")
                .range("bytes=8-31")
                .if_match(&etag),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        assert_eq!(resp.content_range(), Some("bytes 8-31/128"));
        let got = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(got.as_ref(), &body[8..=31]);

        cleanup(&bucket, "obj").await;
    });
}

#[test]
fn test_sse_c_range_get_if_none_match_returns_not_modified() {
    require_https_endpoint();
    s3_tests::run(async {
        let bucket = unique_bucket();
        let (key_b64, key_md5_b64, _) =
            setup_sse_c_object(&bucket, "obj", patterned_bytes(128, 0x77)).await;

        let head = with_sse_c_headers!(
            CTX.client().head_object().bucket(&bucket).key("obj"),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        let etag = head.e_tag().unwrap().to_string();

        let result = with_sse_c_headers!(
            CTX.client()
                .get_object()
                .bucket(&bucket)
                .key("obj")
                .range("bytes=8-31")
                .if_none_match(&etag),
            key_b64,
            key_md5_b64
        )
        .send()
        .await;
        assert_eq!(err_status(&result), 304);

        cleanup(&bucket, "obj").await;
    });
}

#[test]
fn test_sse_c_range_get_on_versioned_object_returns_requested_version() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_with_sse_c_enabled(client, &bucket)
            .await
            .unwrap();
        client
            .put_bucket_versioning()
            .bucket(&bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send_retrying_operation_aborted("enable SSE-C range versioning")
            .await
            .unwrap();

        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);
        let first = patterned_bytes(96, 0x81);
        let second = patterned_bytes(96, 0x91);

        let put_v1 = put_sse_c_object_retrying_operation_aborted(
            client,
            &bucket,
            "obj",
            first.clone(),
            &key_b64,
            &key_md5_b64,
        )
        .await;
        let v1 = put_v1.version_id().unwrap().to_string();

        let put_v2 = put_sse_c_object_retrying_operation_aborted(
            client,
            &bucket,
            "obj",
            second,
            &key_b64,
            &key_md5_b64,
        )
        .await;
        let v2 = put_v2.version_id().unwrap().to_string();

        let resp = with_sse_c_headers!(
            client
                .get_object()
                .bucket(&bucket)
                .key("obj")
                .version_id(&v1)
                .range("bytes=10-19"),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        assert_eq!(resp.version_id(), Some(v1.as_str()));
        assert_eq!(resp.content_range(), Some("bytes 10-19/96"));
        let got = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(got.as_ref(), &first[10..=19]);

        cleanup_versioned(&bucket, "obj", &[v2, v1]).await;
    });
}

#[test]
fn test_sse_c_range_get_and_part_number_rejected_together() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_with_sse_c_enabled(client, &bucket)
            .await
            .unwrap();

        let key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&key);
        let part1 = patterned_bytes(MULTIPART_MIN_PART_SIZE, 0x33);
        let part2 = patterned_bytes(1024, 0x44);

        let create = with_sse_c_headers!(
            client.create_multipart_upload().bucket(&bucket).key("obj"),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let mut completed = Vec::new();
        for (part_number, data) in [(1, part1), (2, part2)] {
            let part = with_sse_c_headers!(
                client
                    .upload_part()
                    .bucket(&bucket)
                    .key("obj")
                    .upload_id(&upload_id)
                    .part_number(part_number)
                    .body(ByteStream::from(data)),
                key_b64,
                key_md5_b64
            )
            .send()
            .await
            .unwrap();
            completed.push(
                CompletedPart::builder()
                    .e_tag(part.e_tag().unwrap())
                    .part_number(part_number)
                    .build(),
            );
        }

        with_sse_c_headers!(
            client
                .complete_multipart_upload()
                .bucket(&bucket)
                .key("obj")
                .upload_id(&upload_id)
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .set_parts(Some(completed))
                        .build()
                ),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();

        let result = with_sse_c_headers!(
            client
                .get_object()
                .bucket(&bucket)
                .key("obj")
                .part_number(2)
                .range("bytes=0-1"),
            key_b64,
            key_md5_b64
        )
        .send()
        .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidRequest");

        cleanup(&bucket, "obj").await;
    });
}

#[test]
fn test_sse_c_get_requires_headers() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_with_sse_c_enabled(client, &bucket)
            .await
            .unwrap();

        let key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&key);

        put_sse_c_object_retrying_operation_aborted(
            client,
            &bucket,
            "obj",
            b"secret".to_vec(),
            &key_b64,
            &key_md5_b64,
        )
        .await;

        let result = client
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("get SSE-C object without headers")
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidRequest");

        cleanup(&bucket, "obj").await;
    });
}

#[test]
fn test_sse_c_head_requires_headers() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_with_sse_c_enabled(client, &bucket)
            .await
            .unwrap();

        let key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&key);

        put_sse_c_object_retrying_operation_aborted(
            client,
            &bucket,
            "obj",
            b"secret".to_vec(),
            &key_b64,
            &key_md5_b64,
        )
        .await;

        let result = client
            .head_object()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("head SSE-C object without headers")
            .await;
        assert_eq!(err_status(&result), 400);

        cleanup(&bucket, "obj").await;
    });
}

#[test]
fn test_sse_c_put_rejects_invalid_key_md5() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_with_sse_c_enabled(client, &bucket)
            .await
            .unwrap();

        let key = test_sse_c_key();
        let (key_b64, _) = sse_c_header_values(&key);
        let result = client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .sse_customer_algorithm("AES256")
            .sse_customer_key(key_b64)
            .sse_customer_key_md5("AAAAAAAAAAAAAAAAAAAAAA==")
            .body(ByteStream::from_static(b"secret"))
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidArgument");

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_sse_c_put_invalid_key_md5_argument_name_matches_aws() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_with_sse_c_enabled(client, &bucket)
            .await
            .unwrap();

        let key = test_sse_c_key();
        let (key_b64, _) = sse_c_header_values(&key);
        let url = format!("{}/{}/obj", CTX.endpoint(), bucket);
        let headers = [
            ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
            (
                "x-amz-server-side-encryption-customer-key",
                key_b64.as_str(),
            ),
            (
                "x-amz-server-side-encryption-customer-key-md5",
                "AAAAAAAAAAAAAAAAAAAAAA==",
            ),
        ];
        let (status, body_text) = signed_put(&url, b"secret", &headers);
        assert_eq!(status, 400, "body: {body_text}");
        assert_eq!(xml_tag(&body_text, "Code"), Some("InvalidArgument"));
        assert_eq!(
            xml_tag(&body_text, "ArgumentName"),
            Some("x-amz-server-side-encryption")
        );

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_sse_c_put_requires_key_md5_header() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_with_sse_c_enabled(client, &bucket)
            .await
            .unwrap();

        let key = test_sse_c_key();
        let (key_b64, _) = sse_c_header_values(&key);
        let result = client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .sse_customer_algorithm("AES256")
            .sse_customer_key(key_b64)
            .body(ByteStream::from_static(b"secret"))
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidArgument");

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_sse_c_put_requires_key_header() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_with_sse_c_enabled(client, &bucket)
            .await
            .unwrap();

        let result = client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .sse_customer_algorithm("AES256")
            .body(ByteStream::from_static(b"secret"))
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidArgument");

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_sse_c_put_rejects_key_without_algorithm() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_with_sse_c_enabled(client, &bucket)
            .await
            .unwrap();

        let key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&key);
        let result = client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .sse_customer_key(key_b64)
            .sse_customer_key_md5(key_md5_b64)
            .body(ByteStream::from_static(b"secret"))
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidArgument");

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_sse_c_get_rejects_wrong_key() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_with_sse_c_enabled(client, &bucket)
            .await
            .unwrap();

        let key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&key);
        let wrong_key = [42u8; 32];
        let (wrong_key_b64, wrong_key_md5_b64) = sse_c_header_values(&wrong_key);

        put_sse_c_object_retrying_operation_aborted(
            client,
            &bucket,
            "obj",
            b"secret".to_vec(),
            &key_b64,
            &key_md5_b64,
        )
        .await;

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
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_with_sse_c_enabled(client, &bucket)
            .await
            .unwrap();

        let key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&key);
        let checksum_sha256 = "arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0=";

        put_sse_c_object_with_sha256_retrying_operation_aborted(
            client,
            &bucket,
            "obj",
            vec![b'A'; 1024],
            &key_b64,
            &key_md5_b64,
            checksum_sha256,
        )
        .await;

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
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_with_sse_c_enabled(client, &bucket)
            .await
            .unwrap();

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

        let missing_headers = client
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("get SSE-C object without headers")
            .await;
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

sse_c_multipart_round_trip_tests! {
    test_sse_c_multipart_round_trip_one_mib => ("one-mib", mib(1), 0x41),
    test_sse_c_multipart_round_trip_four_mib => ("four-mib", mib(4), 0x47),
    test_sse_c_multipart_round_trip_min_part_size => (
        "multipart-min-part-size",
        MULTIPART_MIN_PART_SIZE,
        0x4d
    ),
    test_sse_c_multipart_round_trip_segment_minus_one => (
        "segment-minus-one",
        SSE_C_SEGMENT_BOUNDARY_SIZE - 1,
        0x53
    ),
    test_sse_c_multipart_round_trip_segment_boundary => (
        "segment-boundary",
        SSE_C_SEGMENT_BOUNDARY_SIZE,
        0x61
    ),
    test_sse_c_multipart_round_trip_segment_plus_one => (
        "segment-plus-one",
        SSE_C_SEGMENT_BOUNDARY_SIZE + 1,
        0x67
    ),
    test_sse_c_multipart_round_trip_ten_mib => ("ten-mib", mib(10), 0x6d),
    test_sse_c_multipart_round_trip_two_segments => ("two-segments", mib(16), 0x73),
    test_sse_c_multipart_round_trip_irregular_nine_mib_plus => (
        "irregular-nine-mib-plus",
        mib(9) + 12_345,
        0x79
    ),
}

#[test]
fn test_sse_c_multipart_range_read() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_with_sse_c_enabled(client, &bucket)
            .await
            .unwrap();

        let key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&key);
        let part_sizes = [
            MULTIPART_MIN_PART_SIZE + 1,
            MULTIPART_MIN_PART_SIZE + 3,
            1024 * 1024,
        ];
        let parts_data = [
            patterned_bytes(part_sizes[0], 0x10),
            patterned_bytes(part_sizes[1], 0x40),
            patterned_bytes(part_sizes[2], 0x70),
        ];
        let full_body: Vec<u8> = parts_data.iter().flatten().copied().collect();

        let create = with_sse_c_headers!(
            client.create_multipart_upload().bucket(&bucket).key("obj"),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let mut completed = Vec::new();
        for (idx, data) in parts_data.iter().enumerate() {
            let part = with_sse_c_headers!(
                client
                    .upload_part()
                    .bucket(&bucket)
                    .key("obj")
                    .upload_id(&upload_id)
                    .part_number((idx + 1) as i32)
                    .body(ByteStream::from(data.clone())),
                key_b64,
                key_md5_b64
            )
            .send()
            .await
            .unwrap();
            completed.push(
                CompletedPart::builder()
                    .e_tag(part.e_tag().unwrap())
                    .part_number((idx + 1) as i32)
                    .build(),
            );
        }

        with_sse_c_headers!(
            client
                .complete_multipart_upload()
                .bucket(&bucket)
                .key("obj")
                .upload_id(&upload_id)
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .set_parts(Some(completed))
                        .build()
                ),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();

        let ranges = [
            (0usize, 1023usize),
            (MULTIPART_MIN_PART_SIZE - 8, MULTIPART_MIN_PART_SIZE + 16),
            (
                part_sizes[0] + part_sizes[1] - 12,
                part_sizes[0] + part_sizes[1] + 12,
            ),
            (full_body.len() - 4096, full_body.len() - 1),
        ];
        for (start, end) in ranges {
            let resp = with_sse_c_headers!(
                client
                    .get_object()
                    .bucket(&bucket)
                    .key("obj")
                    .range(format!("bytes={start}-{end}")),
                key_b64,
                key_md5_b64
            )
            .send()
            .await
            .unwrap();
            let body = resp.body.collect().await.unwrap().into_bytes();
            assert_eq!(
                body.as_ref(),
                &full_body[start..=end],
                "range {start}-{end} mismatch"
            );
        }

        cleanup(&bucket, "obj").await;
    });
}

#[test]
fn test_sse_c_multipart_get_part() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_with_sse_c_enabled(client, &bucket)
            .await
            .unwrap();

        let key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&key);
        let part_sizes = [
            MULTIPART_MIN_PART_SIZE,
            MULTIPART_MIN_PART_SIZE,
            MULTIPART_MIN_PART_SIZE,
            1024 * 1024,
        ];
        let parts_data = [
            patterned_bytes(part_sizes[0], b'A'),
            patterned_bytes(part_sizes[1], b'B'),
            patterned_bytes(part_sizes[2], b'C'),
            patterned_bytes(part_sizes[3], b'D'),
        ];

        let create = with_sse_c_headers!(
            client.create_multipart_upload().bucket(&bucket).key("obj"),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let mut completed = Vec::new();
        for (idx, data) in parts_data.iter().enumerate() {
            let part_number = (idx + 1) as i32;
            let part = with_sse_c_headers!(
                client
                    .upload_part()
                    .bucket(&bucket)
                    .key("obj")
                    .upload_id(&upload_id)
                    .part_number(part_number)
                    .body(ByteStream::from(data.clone())),
                key_b64,
                key_md5_b64
            )
            .send()
            .await
            .unwrap();
            completed.push(
                CompletedPart::builder()
                    .e_tag(part.e_tag().unwrap())
                    .part_number(part_number)
                    .build(),
            );
        }

        let complete = with_sse_c_headers!(
            client
                .complete_multipart_upload()
                .bucket(&bucket)
                .key("obj")
                .upload_id(&upload_id)
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .set_parts(Some(completed))
                        .build()
                ),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        let etag = complete.e_tag().unwrap().to_string();
        let part_count = part_sizes.len() as i32;

        for (idx, data) in parts_data.iter().enumerate() {
            let pn = (idx + 1) as i32;

            let head = with_sse_c_headers!(
                client
                    .head_object()
                    .bucket(&bucket)
                    .key("obj")
                    .part_number(pn),
                key_b64,
                key_md5_b64
            )
            .send()
            .await
            .unwrap();
            assert_eq!(head.parts_count(), Some(part_count));
            assert_eq!(head.e_tag().unwrap(), etag);
            assert_eq!(head.content_length(), Some(data.len() as i64));

            let get = with_sse_c_headers!(
                client
                    .get_object()
                    .bucket(&bucket)
                    .key("obj")
                    .part_number(pn),
                key_b64,
                key_md5_b64
            )
            .send()
            .await
            .unwrap();
            assert_eq!(get.parts_count(), Some(part_count));
            assert_eq!(get.e_tag().unwrap(), etag);
            assert_eq!(get.content_length(), Some(data.len() as i64));
            let body = get.body.collect().await.unwrap().into_bytes();
            assert_eq!(body.as_ref(), data.as_slice());
        }

        let get = with_sse_c_headers!(
            client
                .get_object()
                .bucket(&bucket)
                .key("obj")
                .part_number(part_count + 1),
            key_b64,
            key_md5_b64
        )
        .send()
        .await;
        assert_eq!(err_status(&get), 416);

        let head = with_sse_c_headers!(
            client
                .head_object()
                .bucket(&bucket)
                .key("obj")
                .part_number(part_count + 1),
            key_b64,
            key_md5_b64
        )
        .send()
        .await;
        assert_eq!(err_status(&head), 416);

        cleanup(&bucket, "obj").await;
    });
}

#[test]
fn test_sse_c_non_multipart_get_part() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_with_sse_c_enabled(client, &bucket)
            .await
            .unwrap();

        let key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&key);
        let body = b"body".to_vec();

        let put = put_sse_c_object_retrying_operation_aborted(
            client,
            &bucket,
            "obj",
            body.clone(),
            &key_b64,
            &key_md5_b64,
        )
        .await;
        let etag = put.e_tag().unwrap().to_string();

        let get = with_sse_c_headers!(
            client
                .get_object()
                .bucket(&bucket)
                .key("obj")
                .part_number(2),
            key_b64,
            key_md5_b64
        )
        .send()
        .await;
        assert_eq!(err_status(&get), 416);

        let head = with_sse_c_headers!(
            client
                .head_object()
                .bucket(&bucket)
                .key("obj")
                .part_number(2),
            key_b64,
            key_md5_b64
        )
        .send()
        .await;
        assert_eq!(err_status(&head), 416);

        let head = with_sse_c_headers!(
            client
                .head_object()
                .bucket(&bucket)
                .key("obj")
                .part_number(1),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        assert_eq!(head.e_tag().unwrap(), etag);
        assert_eq!(head.content_length(), Some(body.len() as i64));

        let get = with_sse_c_headers!(
            client
                .get_object()
                .bucket(&bucket)
                .key("obj")
                .part_number(1),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        assert_eq!(get.e_tag().unwrap(), etag);
        let got = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(got.as_ref(), body.as_slice());

        cleanup(&bucket, "obj").await;
    });
}

#[test]
fn test_sse_c_upload_part_requires_headers() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_with_sse_c_enabled(client, &bucket)
            .await
            .unwrap();

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
fn test_sse_c_upload_part_rejects_wrong_key() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_with_sse_c_enabled(client, &bucket)
            .await
            .unwrap();

        let key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&key);
        let wrong_key = [42u8; 32];
        let (wrong_key_b64, wrong_key_md5_b64) = sse_c_header_values(&wrong_key);
        let create = with_sse_c_headers!(
            client.create_multipart_upload().bucket(&bucket).key("obj"),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let result = with_sse_c_headers!(
            client
                .upload_part()
                .bucket(&bucket)
                .key("obj")
                .upload_id(&upload_id)
                .part_number(1)
                .body(ByteStream::from_static(b"secret")),
            wrong_key_b64,
            wrong_key_md5_b64
        )
        .send()
        .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidRequest");

        cleanup_multipart(&bucket, "obj", &upload_id).await;
    });
}

#[test]
fn test_sse_c_upload_part_rejects_invalid_key_md5() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_with_sse_c_enabled(client, &bucket)
            .await
            .unwrap();

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
            .sse_customer_algorithm("AES256")
            .sse_customer_key(key_b64)
            .sse_customer_key_md5("AAAAAAAAAAAAAAAAAAAAAA==")
            .body(ByteStream::from_static(b"secret"))
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidArgument");

        cleanup_multipart(&bucket, "obj", &upload_id).await;
    });
}

#[test]
fn test_sse_c_complete_multipart_allows_missing_headers() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_with_sse_c_enabled(client, &bucket)
            .await
            .unwrap();

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

        let part = with_sse_c_headers!(
            client
                .upload_part()
                .bucket(&bucket)
                .key("obj")
                .upload_id(&upload_id)
                .part_number(1)
                .body(ByteStream::from_static(b"secret")),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();

        let result = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key("obj")
            .upload_id(&upload_id)
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
            .await;
        result.unwrap();

        cleanup(&bucket, "obj").await;
    });
}

#[test]
fn test_sse_c_complete_multipart_checksum_requires_headers() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_with_sse_c_enabled(client, &bucket)
            .await
            .unwrap();

        let key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&key);
        let create = with_sse_c_headers!(
            client
                .create_multipart_upload()
                .bucket(&bucket)
                .key("obj")
                .checksum_algorithm(ChecksumAlgorithm::Sha256),
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
                .body(ByteStream::from(vec![b'A'; 1024]))
                .checksum_algorithm(ChecksumAlgorithm::Sha256)
                .checksum_sha256("arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0="),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();

        let result = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key("obj")
            .upload_id(&upload_id)
            .checksum_sha256("Ok6Cs5b96ux6+MWQkJO7UBT5sKPBeXBLwvj/hK89smg=-1")
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(part.e_tag().unwrap())
                            .checksum_sha256(part.checksum_sha256().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidRequest");

        cleanup_multipart(&bucket, "obj", &upload_id).await;
    });
}

#[test]
fn test_sse_c_complete_multipart_checksum_round_trip() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_with_sse_c_enabled(client, &bucket)
            .await
            .unwrap();

        let key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&key);
        let part_checksum = "arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0=";
        let object_checksum = "Ok6Cs5b96ux6+MWQkJO7UBT5sKPBeXBLwvj/hK89smg=";
        let object_checksum_claim = "Ok6Cs5b96ux6+MWQkJO7UBT5sKPBeXBLwvj/hK89smg=-1";
        let create = with_sse_c_headers!(
            client
                .create_multipart_upload()
                .bucket(&bucket)
                .key("obj")
                .checksum_algorithm(ChecksumAlgorithm::Sha256),
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
                .body(ByteStream::from(vec![b'A'; 1024]))
                .checksum_algorithm(ChecksumAlgorithm::Sha256)
                .checksum_sha256(part_checksum),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();

        with_sse_c_headers!(
            client
                .complete_multipart_upload()
                .bucket(&bucket)
                .key("obj")
                .upload_id(&upload_id)
                .checksum_sha256(object_checksum_claim)
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .parts(
                            CompletedPart::builder()
                                .e_tag(part.e_tag().unwrap())
                                .checksum_sha256(part.checksum_sha256().unwrap())
                                .part_number(1)
                                .build(),
                        )
                        .build(),
                ),
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
        assert_eq!(head.checksum_sha256(), Some(object_checksum_claim));
        assert_eq!(
            head.checksum_type(),
            Some(&aws_sdk_s3::types::ChecksumType::Composite)
        );

        let attrs = with_sse_c_headers!(
            client
                .get_object_attributes()
                .bucket(&bucket)
                .key("obj")
                .object_attributes(ObjectAttributes::Checksum),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        let checksum = attrs.checksum().expect("expected checksum");
        assert_eq!(checksum.checksum_sha256(), Some(object_checksum));
        assert_eq!(
            checksum.checksum_type(),
            Some(&aws_sdk_s3::types::ChecksumType::Composite)
        );

        cleanup(&bucket, "obj").await;
    });
}

#[test]
fn test_sse_c_complete_multipart_rejects_wrong_key() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_with_sse_c_enabled(client, &bucket)
            .await
            .unwrap();

        let key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&key);
        let wrong_key = [42u8; 32];
        let (wrong_key_b64, wrong_key_md5_b64) = sse_c_header_values(&wrong_key);
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
                .body(ByteStream::from_static(b"secret")),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();

        let result = with_sse_c_headers!(
            client
                .complete_multipart_upload()
                .bucket(&bucket)
                .key("obj")
                .upload_id(&upload_id)
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .parts(
                            CompletedPart::builder()
                                .e_tag(part.e_tag().unwrap())
                                .part_number(1)
                                .build(),
                        )
                        .build(),
                ),
            wrong_key_b64,
            wrong_key_md5_b64
        )
        .send()
        .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidRequest");

        cleanup_multipart(&bucket, "obj", &upload_id).await;
    });
}

#[test]
fn test_sse_c_copy_object_round_trip() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_with_sse_c_enabled(client, &bucket)
            .await
            .unwrap();

        let src_key = test_sse_c_key();
        let (src_key_b64, src_key_md5_b64) = sse_c_header_values(&src_key);
        let dst_key = [7u8; 32];
        let (dst_key_b64, dst_key_md5_b64) = sse_c_header_values(&dst_key);
        let body = b"hello sse-c copy".to_vec();

        put_sse_c_object_retrying_operation_aborted(
            client,
            &bucket,
            "src",
            body.clone(),
            &src_key_b64,
            &src_key_md5_b64,
        )
        .await;

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
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_sse_c_copy_object_requires_source_headers() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_with_sse_c_enabled(client, &bucket)
            .await
            .unwrap();

        let key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&key);

        put_sse_c_object_retrying_operation_aborted(
            client,
            &bucket,
            "src",
            b"secret-copy".to_vec(),
            &key_b64,
            &key_md5_b64,
        )
        .await;

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
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_sse_c_copy_object_invalid_source_key_md5_argument_name_matches_aws() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_with_sse_c_enabled(client, &bucket)
            .await
            .unwrap();

        let key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&key);

        put_sse_c_object_retrying_operation_aborted(
            client,
            &bucket,
            "src",
            b"secret-copy".to_vec(),
            &key_b64,
            &key_md5_b64,
        )
        .await;

        let url = format!("{}/{}/dst", CTX.endpoint(), bucket);
        let copy_source = format!("{}/src", bucket);
        let headers = [
            ("x-amz-copy-source", copy_source.as_str()),
            (
                "x-amz-copy-source-server-side-encryption-customer-algorithm",
                "AES256",
            ),
            (
                "x-amz-copy-source-server-side-encryption-customer-key",
                key_b64.as_str(),
            ),
            (
                "x-amz-copy-source-server-side-encryption-customer-key-md5",
                "AAAAAAAAAAAAAAAAAAAAAA==",
            ),
        ];
        let (status, body_text) = signed_put(&url, &[], &headers);
        assert_eq!(status, 400, "body: {body_text}");
        assert_eq!(xml_tag(&body_text, "Code"), Some("InvalidArgument"));
        assert_eq!(
            xml_tag(&body_text, "ArgumentName"),
            Some("x-amz-server-side-encryption")
        );

        cleanup(&bucket, "src").await;
    });
}

#[test]
fn test_sse_c_upload_part_copy_round_trip() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_with_sse_c_enabled(client, &bucket)
            .await
            .unwrap();

        let src_key = test_sse_c_key();
        let (src_key_b64, src_key_md5_b64) = sse_c_header_values(&src_key);
        let dst_key = [9u8; 32];
        let (dst_key_b64, dst_key_md5_b64) = sse_c_header_values(&dst_key);
        let body = b"hello multipart copy sse-c".to_vec();

        put_sse_c_object_retrying_operation_aborted(
            client,
            &bucket,
            "src",
            body.clone(),
            &src_key_b64,
            &src_key_md5_b64,
        )
        .await;

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
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_sse_c_upload_part_copy_requires_source_headers() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_with_sse_c_enabled(client, &bucket)
            .await
            .unwrap();

        let src_key = test_sse_c_key();
        let (src_key_b64, src_key_md5_b64) = sse_c_header_values(&src_key);
        let dst_key = [9u8; 32];
        let (dst_key_b64, dst_key_md5_b64) = sse_c_header_values(&dst_key);

        put_sse_c_object_retrying_operation_aborted(
            client,
            &bucket,
            "src",
            b"secret-copy-part".to_vec(),
            &src_key_b64,
            &src_key_md5_b64,
        )
        .await;

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
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_sse_c_upload_part_copy_rejects_wrong_destination_key() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_with_sse_c_enabled(client, &bucket)
            .await
            .unwrap();

        let src_key = test_sse_c_key();
        let (src_key_b64, src_key_md5_b64) = sse_c_header_values(&src_key);
        let dst_key = [9u8; 32];
        let (dst_key_b64, dst_key_md5_b64) = sse_c_header_values(&dst_key);
        let wrong_dst_key = [11u8; 32];
        let (wrong_dst_key_b64, wrong_dst_key_md5_b64) = sse_c_header_values(&wrong_dst_key);

        put_sse_c_object_retrying_operation_aborted(
            client,
            &bucket,
            "src",
            b"secret-copy-part".to_vec(),
            &src_key_b64,
            &src_key_md5_b64,
        )
        .await;

        let create = with_sse_c_headers!(
            client.create_multipart_upload().bucket(&bucket).key("dst"),
            dst_key_b64,
            dst_key_md5_b64
        )
        .send()
        .await
        .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let result = with_sse_c_copy_headers!(
            client
                .upload_part_copy()
                .bucket(&bucket)
                .key("dst")
                .upload_id(&upload_id)
                .part_number(1)
                .copy_source(format!("{}/src", bucket)),
            src_key_b64,
            src_key_md5_b64,
            wrong_dst_key_b64,
            wrong_dst_key_md5_b64
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
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── Response shapes ─────────────────────────────────────────────────

fn sse_c_request_headers<'a>(key_b64: &'a str, key_md5_b64: &'a str) -> [(&'a str, &'a str); 3] {
    [
        ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
        ("x-amz-server-side-encryption-customer-key", key_b64),
        ("x-amz-server-side-encryption-customer-key-md5", key_md5_b64),
    ]
}

#[test]
fn test_sse_c_blocked_by_default_error_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        let key = "shape-sse-c-blocked-by-default.txt";
        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);

        let response = raw_object_with(
            "PUT",
            &bucket,
            key,
            b"secret",
            &sse_c_request_headers(&key_b64, &key_md5_b64),
        );
        // The caller principal differs per endpoint (account ID locally, the
        // caller ARN on AWS); everything else in the message is fixed.
        assert_shape(
            "PutObject SSE-C blocked by default encryption",
            &response,
            &shape()
                .status(403)
                .headers(error_response_headers())
                .body(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>AccessDenied</Code>\
                     <Message>User: {any} is not authorized to perform: s3:PutObject on \
                     resource: \"arn:aws:s3:::{bucket}/{key}\" because this bucket has blocked \
                     upload requests that specify Server Side Encryption with Customer provided \
                     keys (SSE-C). Please specify a different server-side encryption \
                     type.</Message><RequestId>{request_id}</RequestId>\
                     <HostId>{host_id}</HostId></Error>",
                )
                .sub("bucket", bucket.as_str())
                .sub("key", key),
        );

        delete_all_and_bucket(client, &bucket, &[]).await;
    });
}

#[test]
fn test_sse_c_put_head_response_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        create_bucket_with_sse_c_enabled(client, &bucket)
            .await
            .expect("create SSE-C enabled bucket");
        let key = "shape-sse-c-enabled.txt";
        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);

        let put = raw_object_with(
            "PUT",
            &bucket,
            key,
            b"secret",
            &sse_c_request_headers(&key_b64, &key_md5_b64),
        );
        let put_captures = assert_shape(
            "PutObject SSE-C shape",
            &put,
            &shape()
                .status(200)
                .headers([
                    ("etag", "{etag}"),
                    ("x-amz-checksum-crc64nvme", "57Cg+N2YpVM="),
                    ("x-amz-checksum-type", "FULL_OBJECT"),
                    ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
                    (
                        "x-amz-server-side-encryption-customer-key-md5",
                        key_md5_b64.as_str(),
                    ),
                    ("x-amz-request-id", "{request_id}"),
                    ("x-amz-id-2", "{host_id}"),
                    ("content-length", "0"),
                ])
                .body_empty(),
        );

        let head = raw_object_with(
            "HEAD",
            &bucket,
            key,
            b"",
            &sse_c_request_headers(&key_b64, &key_md5_b64),
        );
        let head_captures = assert_shape(
            "HeadObject SSE-C shape",
            &head,
            &shape()
                .status(200)
                .headers([
                    ("etag", "{etag}"),
                    ("content-length", "6"),
                    ("last-modified", "{http_date}"),
                    ("accept-ranges", "bytes"),
                    ("content-type", "binary/octet-stream"),
                    ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
                    (
                        "x-amz-server-side-encryption-customer-key-md5",
                        key_md5_b64.as_str(),
                    ),
                    ("x-amz-request-id", "{request_id}"),
                    ("x-amz-id-2", "{host_id}"),
                ])
                .body_empty(),
        );
        assert_eq!(put_captures["etag"], head_captures["etag"]);

        delete_all_and_bucket(client, &bucket, &[key.to_string()]).await;
    });
}

#[test]
fn test_sse_c_missing_key_md5_error_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        create_bucket_with_sse_c_enabled(client, &bucket)
            .await
            .expect("create SSE-C enabled bucket");
        let customer_key = test_sse_c_key();
        let (key_b64, _) = sse_c_header_values(&customer_key);

        let response = raw_object_with(
            "PUT",
            &bucket,
            "shape-sse-c-missing-key-md5.txt",
            b"secret",
            &[
                ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
                (
                    "x-amz-server-side-encryption-customer-key",
                    key_b64.as_str(),
                ),
            ],
        );
        assert_shape(
            "PutObject SSE-C missing key MD5",
            &response,
            &shape().status(400).headers(error_response_headers()).body(
                expected_error::invalid_argument(
                    "Requests specifying Server Side Encryption with Customer provided keys \
                     must provide the client calculated MD5 of the secret key.",
                    "x-amz-server-side-encryption",
                ),
            ),
        );

        delete_all_and_bucket(client, &bucket, &[]).await;
    });
}

#[test]
fn test_sse_c_missing_key_error_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        create_bucket_with_sse_c_enabled(client, &bucket)
            .await
            .expect("create SSE-C enabled bucket");
        let customer_key = test_sse_c_key();
        let (_, key_md5_b64) = sse_c_header_values(&customer_key);

        let response = raw_object_with(
            "PUT",
            &bucket,
            "shape-sse-c-missing-key.txt",
            b"secret",
            &[
                ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
                (
                    "x-amz-server-side-encryption-customer-key-md5",
                    key_md5_b64.as_str(),
                ),
            ],
        );
        assert_shape(
            "PutObject SSE-C missing key",
            &response,
            &shape().status(400).headers(error_response_headers()).body(
                expected_error::invalid_argument(
                    "Requests specifying Server Side Encryption with Customer provided keys \
                     must provide an appropriate secret key.",
                    "x-amz-server-side-encryption",
                ),
            ),
        );

        delete_all_and_bucket(client, &bucket, &[]).await;
    });
}

#[test]
fn test_sse_c_wrong_algorithm_error_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        create_bucket_with_sse_c_enabled(client, &bucket)
            .await
            .expect("create SSE-C enabled bucket");
        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);

        let response = raw_object_with(
            "PUT",
            &bucket,
            "shape-sse-c-wrong-algorithm.txt",
            b"secret",
            &[
                ("x-amz-server-side-encryption-customer-algorithm", "aws:kms"),
                (
                    "x-amz-server-side-encryption-customer-key",
                    key_b64.as_str(),
                ),
                (
                    "x-amz-server-side-encryption-customer-key-md5",
                    key_md5_b64.as_str(),
                ),
            ],
        );
        assert_shape(
            "PutObject SSE-C wrong algorithm",
            &response,
            &shape().status(400).headers(error_response_headers()).body(
                expected_error::invalid_encryption_algorithm(
                    "The Encryption request you specified is not valid. Supported value: \
                     AES256.",
                    "aws:kms",
                ),
            ),
        );

        delete_all_and_bucket(client, &bucket, &[]).await;
    });
}
