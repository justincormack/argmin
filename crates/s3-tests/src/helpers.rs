use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::LazyLock;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_sdk_s3::client::customize::CustomizableOperation;
use aws_sdk_s3::operation::delete_objects::builders::DeleteObjectsFluentBuilder;
use aws_sdk_s3::operation::delete_objects::{DeleteObjectsError, DeleteObjectsOutput};
use aws_sdk_s3::operation::put_bucket_lifecycle_configuration::builders::PutBucketLifecycleConfigurationFluentBuilder;
use aws_sdk_s3::operation::put_bucket_lifecycle_configuration::{
    PutBucketLifecycleConfigurationError, PutBucketLifecycleConfigurationOutput,
};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketLifecycleConfiguration, Delete};
use aws_sdk_s3::Client;
use base64::Engine;
use md5_legacy::Digest;
use ring::hmac;

use crate::{test_agent, CTX};

static BUCKET_COUNTER: AtomicU64 = AtomicU64::new(0);

const TEST_SSE_C_KEY_BYTES: [u8; 32] = *b"abcdefghijklmnopqrstuvwxyzABCDEF";

/// Bucket name prefix, configurable via `S3_TEST_BUCKET_PREFIX`.
/// Defaults to `"test"`.
static BUCKET_PREFIX: LazyLock<String> = LazyLock::new(|| {
    if std::env::var("S3_TEST_ENDPOINT").is_ok() {
        std::env::var("S3_TEST_BUCKET_PREFIX").expect(
            "S3_TEST_BUCKET_PREFIX required with S3_TEST_ENDPOINT; use a dedicated prefix such as claude-s3- that matches the test IAM policy",
        )
    } else {
        std::env::var("S3_TEST_BUCKET_PREFIX").unwrap_or_else(|_| "test".to_string())
    }
});

/// Return the bucket prefix (from `S3_TEST_BUCKET_PREFIX` or `"test"`).
pub fn bucket_prefix() -> &'static str {
    &BUCKET_PREFIX
}

/// Fixed 32-byte customer key for SSE-C integration tests.
pub fn test_sse_c_key() -> [u8; 32] {
    TEST_SSE_C_KEY_BYTES
}

/// Return base64-encoded SSE-C key and key MD5 header values.
pub fn sse_c_header_values(key: &[u8; 32]) -> (String, String) {
    let key_b64 = base64::engine::general_purpose::STANDARD.encode(key);
    let md5 = md5_legacy::Md5::digest(key);
    let key_md5_b64 = base64::engine::general_purpose::STANDARD.encode(&md5[..]);
    (key_b64, key_md5_b64)
}

/// Generate a unique bucket name for a test.
///
/// Uses a monotonic counter + process ID to avoid collisions between
/// parallel test runs and between tests within the same run.
/// The prefix is configurable via `S3_TEST_BUCKET_PREFIX` (default `"test"`).
pub fn unique_bucket() -> String {
    let n = BUCKET_COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    format!("{}-{}-{}-{}", bucket_prefix(), pid, n, timestamp_millis())
}

/// Configure bucket-level Public Access Block to allow public ACL and policy tests.
///
/// This only affects the bucket-level setting. Account-level or org-level block
/// public access can still override this configuration.
pub async fn disable_bucket_public_access_block(client: &Client, bucket: &str) {
    use aws_sdk_s3::types::PublicAccessBlockConfiguration;

    let pab = PublicAccessBlockConfiguration::builder()
        .block_public_acls(false)
        .ignore_public_acls(false)
        .block_public_policy(false)
        .restrict_public_buckets(false)
        .build();
    client
        .put_public_access_block()
        .bucket(bucket)
        .public_access_block_configuration(pab)
        .send()
        .await
        .expect("disable bucket public access block");
}

fn timestamp_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Create a bucket and populate it with `n` objects named "key0", "key1", ...
///
/// Returns the bucket name and the list of keys.
pub async fn create_objects(client: &Client, prefix: &str, n: usize) -> (String, Vec<String>) {
    let bucket = unique_bucket();
    client
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("create bucket");

    let mut keys = Vec::with_capacity(n);
    for i in 0..n {
        let key = format!("{}key{}", prefix, i);
        client
            .put_object()
            .bucket(&bucket)
            .key(&key)
            .body(ByteStream::from_static(b"content"))
            .send()
            .await
            .expect("put object");
        keys.push(key);
    }
    (bucket, keys)
}

/// Create a bucket and populate it with objects whose keys are the given strings.
///
/// Each object body is `b"content"`. Returns `(bucket_name, keys_as_owned_strings)`.
pub async fn create_objects_with_keys(client: &Client, keys: &[&str]) -> (String, Vec<String>) {
    let bucket = unique_bucket();
    client
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("create bucket");

    let mut owned_keys = Vec::with_capacity(keys.len());
    for key in keys {
        client
            .put_object()
            .bucket(&bucket)
            .key(*key)
            .body(ByteStream::from_static(b"content"))
            .send()
            .await
            .expect("put object");
        owned_keys.push((*key).to_string());
    }
    (bucket, owned_keys)
}

/// Create a public-read bucket.
///
/// Disables bucket-level BlockPublicAccess, sets ObjectOwnership to
/// BucketOwnerPreferred, then applies the public-read ACL. Note that
/// account-level BlockPublicAccess (if enabled) can still override
/// bucket-level settings and cause these calls to fail.
pub async fn create_public_bucket(client: &Client) -> String {
    use aws_sdk_s3::types::{BucketCannedAcl, ObjectOwnership, OwnershipControlsRule};

    let bucket = unique_bucket();

    // 1. Create the bucket (private, default ownership)
    client
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("create bucket");

    // 2. Disable BlockPublicAccess on this bucket
    disable_bucket_public_access_block(client, &bucket).await;

    // 3. Set ownership to BucketOwnerPreferred (required to use canned ACLs)
    let ownership_rule = OwnershipControlsRule::builder()
        .object_ownership(ObjectOwnership::BucketOwnerPreferred)
        .build()
        .unwrap();
    let ownership = aws_sdk_s3::types::OwnershipControls::builder()
        .rules(ownership_rule)
        .build()
        .unwrap();
    client
        .put_bucket_ownership_controls()
        .bucket(&bucket)
        .ownership_controls(ownership)
        .send()
        .await
        .expect("set ownership controls");

    // 4. Apply public-read ACL
    client
        .put_bucket_acl()
        .bucket(&bucket)
        .acl(BucketCannedAcl::PublicRead)
        .send()
        .await
        .expect("set public-read ACL");

    bucket
}

/// Create a public-read-write bucket.
///
/// Disables bucket-level BlockPublicAccess, sets ObjectOwnership to
/// BucketOwnerPreferred, then applies the public-read-write ACL.
pub async fn create_public_write_bucket(client: &Client) -> String {
    use aws_sdk_s3::types::{BucketCannedAcl, ObjectOwnership, OwnershipControlsRule};

    let bucket = unique_bucket();

    client
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("create bucket");

    disable_bucket_public_access_block(client, &bucket).await;

    let ownership_rule = OwnershipControlsRule::builder()
        .object_ownership(ObjectOwnership::BucketOwnerPreferred)
        .build()
        .unwrap();
    let ownership = aws_sdk_s3::types::OwnershipControls::builder()
        .rules(ownership_rule)
        .build()
        .unwrap();
    client
        .put_bucket_ownership_controls()
        .bucket(&bucket)
        .ownership_controls(ownership)
        .send()
        .await
        .expect("set ownership controls");

    client
        .put_bucket_acl()
        .bucket(&bucket)
        .acl(BucketCannedAcl::PublicReadWrite)
        .send()
        .await
        .expect("set public-read-write ACL");

    // Issue one read-back pass after the control-plane writes. This is not a
    // convergence loop; it just gives AWS a moment to settle before tests make
    // anonymous data-plane requests against the bucket.
    client
        .get_public_access_block()
        .bucket(&bucket)
        .send()
        .await
        .expect("read public access block");
    client
        .get_bucket_ownership_controls()
        .bucket(&bucket)
        .send()
        .await
        .expect("read ownership controls");
    client
        .get_bucket_acl()
        .bucket(&bucket)
        .send()
        .await
        .expect("read bucket ACL");

    bucket
}

/// Delete all listed keys from the bucket, then delete the bucket itself.
pub async fn delete_all_and_bucket(client: &Client, bucket: &str, keys: &[String]) {
    for key in keys {
        client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .expect("delete object");
    }
    client
        .delete_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("delete bucket");
}

/// Build a DeleteObjects request that sets the required Content-MD5 header from
/// the serialized XML body before signing.
pub fn delete_objects_with_md5(
    client: &Client,
    bucket: &str,
    delete: Delete,
) -> CustomizableOperation<DeleteObjectsOutput, DeleteObjectsError, DeleteObjectsFluentBuilder> {
    client
        .delete_objects()
        .bucket(bucket)
        .delete(delete)
        .customize()
        .mutate_request(|req| {
            let body = req
                .body()
                .bytes()
                .expect("DeleteObjects body must be in-memory");
            let digest = md5_legacy::Md5::digest(body);
            let content_md5 = base64::engine::general_purpose::STANDARD.encode(&digest[..]);
            req.headers_mut().insert("content-md5", content_md5);
        })
}

/// Minimal response data for raw signed HTTP test requests.
pub struct RawResponse {
    pub status: u16,
    pub body: String,
}

/// Build a `Content-MD5` header pair for the request body.
pub fn content_md5_header(body: &[u8]) -> (String, String) {
    ("Content-MD5".to_string(), md5_b64(body))
}

/// Build a CRC32-based SDK checksum header set for the request body.
pub fn sdk_checksum_headers(body: &[u8]) -> Vec<(String, String)> {
    vec![
        ("x-amz-checksum-algorithm".to_string(), "CRC32".to_string()),
        ("x-amz-checksum-crc32".to_string(), crc32_b64(body)),
    ]
}

/// Send a raw signed S3 request, bypassing SDK auto-checksum behavior.
pub fn send_signed_request<K, V, I>(
    method: &str,
    url_str: &str,
    body: &[u8],
    extra_headers: I,
) -> RawResponse
where
    K: AsRef<str>,
    V: AsRef<str>,
    I: IntoIterator<Item = (K, V)>,
{
    let agent = test_agent();
    let parsed = url::Url::parse(url_str).expect("parse URL");
    let path = parsed.path();
    let query = normalize_query(parsed.query().unwrap_or(""));

    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let amz_date = format_amz_date(secs);
    let date_stamp = &amz_date[..8];

    let host = parsed
        .host_str()
        .map(|host| {
            if let Some(port) = parsed.port() {
                format!("{host}:{port}")
            } else {
                host.to_string()
            }
        })
        .expect("URL host");
    let payload_hash = sha256_hex(body);

    let mut request_headers: Vec<(String, String)> = vec![
        ("host".to_string(), host),
        ("x-amz-content-sha256".to_string(), payload_hash.clone()),
        ("x-amz-date".to_string(), amz_date.clone()),
    ];
    for (name, value) in extra_headers {
        request_headers.push((name.as_ref().to_lowercase(), value.as_ref().to_string()));
    }
    request_headers.sort_by(|a, b| a.0.cmp(&b.0));

    let signed_headers = request_headers
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical_headers: String = request_headers
        .iter()
        .map(|(name, value)| format!("{name}:{value}\n"))
        .collect();
    let canonical_request =
        format!("{method}\n{path}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}");

    let scope = format!("{date_stamp}/{}/s3/aws4_request", CTX.region());
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );
    let k_date = hmac_sha256(
        format!("AWS4{}", CTX.secret_key()).as_bytes(),
        date_stamp.as_bytes(),
    );
    let k_region = hmac_sha256(&k_date, CTX.region().as_bytes());
    let k_service = hmac_sha256(&k_region, b"s3");
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    let signature: String = hmac_sha256(&k_signing, string_to_sign.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        CTX.access_key()
    );

    const MAX_SLOWDOWN_RETRIES: u32 = 4;
    let mut attempt = 0;
    loop {
        let mut response = if method == "HEAD" {
            let mut request = agent
                .head(url_str)
                .header("Authorization", &authorization)
                .header("x-amz-date", &amz_date)
                .header("x-amz-content-sha256", &payload_hash);
            for (name, value) in &request_headers {
                if name == "host" || name == "x-amz-content-sha256" || name == "x-amz-date" {
                    continue;
                }
                request = request.header(name, value);
            }
            request.call().expect("raw HEAD transport error")
        } else {
            let mut request = match method {
                "PUT" => agent.put(url_str),
                "POST" => agent.post(url_str),
                other => panic!("unsupported method: {other}"),
            }
            .header("Authorization", &authorization)
            .header("x-amz-date", &amz_date)
            .header("x-amz-content-sha256", &payload_hash);
            for (name, value) in &request_headers {
                if name == "host" || name == "x-amz-content-sha256" || name == "x-amz-date" {
                    continue;
                }
                request = request.header(name, value);
            }
            request.send(body).expect("raw request transport error")
        };
        let status = response.status().as_u16();
        let body_text = response.body_mut().read_to_string().unwrap_or_default();
        if status == 503
            && body_text.contains("<Code>SlowDown</Code>")
            && attempt < MAX_SLOWDOWN_RETRIES
        {
            let backoff_ms = 200u64 << attempt;
            thread::sleep(Duration::from_millis(backoff_ms));
            attempt += 1;
            continue;
        }
        return RawResponse {
            status,
            body: body_text,
        };
    }
}

fn md5_b64(data: &[u8]) -> String {
    let digest = md5_legacy::Md5::digest(data);
    base64::engine::general_purpose::STANDARD.encode(&digest[..])
}

fn crc32_b64(data: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(checksum::crc32::checksum(data).to_be_bytes())
}

fn sha256_hex(data: &[u8]) -> String {
    let digest = ring::digest::digest(&ring::digest::SHA256, data);
    digest
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let key = hmac::Key::new(hmac::HMAC_SHA256, key);
    hmac::sign(&key, data).as_ref().to_vec()
}

fn format_amz_date(epoch_secs: u64) -> String {
    let days = epoch_secs / 86_400;
    let time_of_day = epoch_secs % 86_400;
    let (year, month, day) = days_to_date(days as i64);
    format!(
        "{year:04}{month:02}{day:02}T{:02}{:02}{:02}Z",
        time_of_day / 3_600,
        (time_of_day % 3_600) / 60,
        time_of_day % 60,
    )
}

fn days_to_date(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = (z - era * 146_097) as u32;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era as i64 + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = if month_prime < 10 {
        month_prime + 3
    } else {
        month_prime - 9
    };
    let year = if month <= 2 { year + 1 } else { year };
    (year, month, day)
}

fn normalize_query(raw: &str) -> String {
    if raw.is_empty() {
        return String::new();
    }
    let mut pairs: Vec<(String, String)> = raw
        .split('&')
        .filter(|segment| !segment.is_empty())
        .map(|segment| {
            let mut parts = segment.splitn(2, '=');
            let key = parts.next().unwrap_or("").to_string();
            let value = parts.next().unwrap_or("").to_string();
            (key, value)
        })
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// Build a PutBucketLifecycleConfiguration request that sets the required
/// Content-MD5 header from the serialized XML body before signing.
pub fn put_bucket_lifecycle_with_md5(
    client: &Client,
    bucket: &str,
    lifecycle_configuration: BucketLifecycleConfiguration,
) -> CustomizableOperation<
    PutBucketLifecycleConfigurationOutput,
    PutBucketLifecycleConfigurationError,
    PutBucketLifecycleConfigurationFluentBuilder,
> {
    client
        .put_bucket_lifecycle_configuration()
        .bucket(bucket)
        .lifecycle_configuration(lifecycle_configuration)
        .customize()
        .mutate_request(|req| {
            let body = req
                .body()
                .bytes()
                .expect("PutBucketLifecycleConfiguration body must be in-memory");
            let digest = md5_legacy::Md5::digest(body);
            let content_md5 = base64::engine::general_purpose::STANDARD.encode(&digest[..]);
            req.headers_mut().insert("content-md5", content_md5);
        })
}

/// Build an `x-amz-copy-source` value for a specific object version.
///
/// `source_key` should already be URL-encoded if it contains reserved path
/// characters. The `version_id` query component is always percent-encoded.
pub fn copy_source_with_version(bucket: &str, source_key: &str, version_id: &str) -> String {
    let encoded_version_id: String =
        url::form_urlencoded::byte_serialize(version_id.as_bytes()).collect();
    format!("{bucket}/{source_key}?versionId={encoded_version_id}")
}

/// Delete all object versions and delete markers in a bucket, then delete the bucket.
///
/// This is needed for versioned buckets on AWS where simple delete_object creates
/// delete markers rather than removing objects.
pub async fn cleanup_versioned_bucket(client: &Client, bucket: &str) {
    loop {
        let resp = client
            .list_object_versions()
            .bucket(bucket)
            .send()
            .await
            .expect("list object versions");

        let mut objects: Vec<aws_sdk_s3::types::ObjectIdentifier> = Vec::new();

        for v in resp.versions() {
            objects.push(
                aws_sdk_s3::types::ObjectIdentifier::builder()
                    .key(v.key().unwrap_or_default())
                    .version_id(v.version_id().unwrap_or_default())
                    .build()
                    .unwrap(),
            );
        }
        for m in resp.delete_markers() {
            objects.push(
                aws_sdk_s3::types::ObjectIdentifier::builder()
                    .key(m.key().unwrap_or_default())
                    .version_id(m.version_id().unwrap_or_default())
                    .build()
                    .unwrap(),
            );
        }

        if objects.is_empty() {
            break;
        }

        let delete = aws_sdk_s3::types::Delete::builder()
            .set_objects(Some(objects))
            .quiet(true)
            .build()
            .unwrap();
        delete_objects_with_md5(client, bucket, delete)
            .send()
            .await
            .expect("delete objects");
    }

    client
        .delete_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("delete bucket");
}

/// Assert that an S3 SDK error contains the expected error code string.
pub fn assert_s3_err_code<T, E: std::fmt::Debug>(
    result: &Result<T, aws_sdk_s3::error::SdkError<E>>,
    expected_code: &str,
) {
    match result {
        Ok(_) => panic!("expected error with code {}, got Ok", expected_code),
        Err(e) => {
            let msg = format!("{:?}", e);
            assert!(
                msg.contains(expected_code),
                "expected error code '{}' in error: {}",
                expected_code,
                msg
            );
        }
    }
}

/// Extract the HTTP status code from an S3 SDK error.
///
/// Panics if the result is `Ok` or if the error has no raw HTTP response.
pub fn err_status<T, E: std::fmt::Debug>(
    result: &Result<T, aws_sdk_s3::error::SdkError<E>>,
) -> u16 {
    match result {
        Ok(_) => panic!("expected error, got Ok"),
        Err(sdk_err) => sdk_err
            .raw_response()
            .map(|r| r.status().as_u16())
            .unwrap_or_else(|| panic!("error has no raw HTTP response: {:?}", sdk_err)),
    }
}
