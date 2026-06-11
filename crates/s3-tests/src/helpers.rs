use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::LazyLock;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use auth::canonical::canonical_query_string;
use aws_sdk_s3::client::customize::CustomizableOperation;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::operation::delete_objects::builders::DeleteObjectsFluentBuilder;
use aws_sdk_s3::operation::delete_objects::{DeleteObjectsError, DeleteObjectsOutput};
use aws_sdk_s3::operation::put_bucket_lifecycle_configuration::builders::PutBucketLifecycleConfigurationFluentBuilder;
use aws_sdk_s3::operation::put_bucket_lifecycle_configuration::{
    PutBucketLifecycleConfigurationError, PutBucketLifecycleConfigurationOutput,
};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BlockedEncryptionTypes, BucketCannedAcl, BucketLifecycleConfiguration,
    BucketLocationConstraint, CreateBucketConfiguration, Delete, EncryptionType, ObjectOwnership,
    ServerSideEncryption, ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration,
    ServerSideEncryptionRule,
};
use aws_sdk_s3::Client;
use base64::Engine;
use md5_legacy::Digest;
use ring::hmac;

use crate::CTX;

static BUCKET_COUNTER: AtomicU64 = AtomicU64::new(0);
static BUCKET_NAMESPACE: LazyLock<u64> = LazyLock::new(|| {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    now ^ ((std::process::id() as u64) << 32)
});

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
    format!("{}{pid}-{:016x}-{n}", bucket_prefix(), *BUCKET_NAMESPACE)
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
    wait_for_bucket_public_access_block_disabled(client, bucket).await;
}

async fn create_test_bucket(client: &Client, bucket: &str) {
    let mut request = client.create_bucket().bucket(bucket);
    if CTX.region() != "us-east-1" {
        request = request.create_bucket_configuration(
            CreateBucketConfiguration::builder()
                .location_constraint(BucketLocationConstraint::from(CTX.region()))
                .build(),
        );
    }
    match request.send().await {
        Ok(_) => {}
        Err(err) if is_create_bucket_lost_success_retry(&err) => {
            verify_bucket_exists_after_create_conflict(client, bucket, "create bucket").await;
        }
        Err(err) => panic!("create bucket: {err:?}"),
    }
}

async fn create_test_bucket_with_ownership(
    client: &Client,
    bucket: &str,
    ownership: ObjectOwnership,
) {
    let mut request = client
        .create_bucket()
        .bucket(bucket)
        .object_ownership(ownership.clone());
    if CTX.region() != "us-east-1" {
        request = request.create_bucket_configuration(
            CreateBucketConfiguration::builder()
                .location_constraint(BucketLocationConstraint::from(CTX.region()))
                .build(),
        );
    }
    match request.send().await {
        Ok(_) => {}
        Err(err) if is_create_bucket_lost_success_retry(&err) => {
            verify_bucket_exists_after_create_conflict(
                client,
                bucket,
                "create bucket with ownership",
            )
            .await;
        }
        Err(err) => panic!("create bucket with ownership: {err:?}"),
    }
    wait_for_bucket_ownership_controls(client, bucket, ownership).await;
}

fn is_create_bucket_lost_success_retry(
    err: &aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::create_bucket::CreateBucketError>,
) -> bool {
    s3_error_code(err) == Some("BucketAlreadyOwnedByYou")
}

fn is_bucket_already_absent<E: ProvideErrorMetadata>(err: &aws_sdk_s3::error::SdkError<E>) -> bool {
    s3_error_code(err) == Some("NoSuchBucket")
}

fn s3_error_code<E: ProvideErrorMetadata>(err: &aws_sdk_s3::error::SdkError<E>) -> Option<&str> {
    err.as_service_error().and_then(ProvideErrorMetadata::code)
}

async fn verify_bucket_exists_after_create_conflict(client: &Client, bucket: &str, context: &str) {
    client
        .head_bucket()
        .bucket(bucket)
        .send()
        .await
        .unwrap_or_else(|head_err| {
            panic!("{context}: BucketAlreadyOwnedByYou but HeadBucket failed for {bucket}: {head_err:?}");
        });
}

async fn wait_for_bucket_ownership_controls(
    client: &Client,
    bucket: &str,
    expected: ObjectOwnership,
) {
    const MAX_ATTEMPTS: usize = 20;

    for attempt in 0..MAX_ATTEMPTS {
        let result = client
            .get_bucket_ownership_controls()
            .bucket(bucket)
            .send()
            .await;
        if let Ok(resp) = result {
            if resp.ownership_controls().map(|controls| {
                controls
                    .rules()
                    .iter()
                    .any(|rule| rule.object_ownership() == &expected)
            }) == Some(true)
            {
                return;
            }
        }
        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }
        panic!("ObjectOwnership={expected:?} did not converge for {bucket}");
    }
}

/// Create a bucket through the default S3 ownership path.
///
/// Current AWS behavior and this server's frontend default new buckets to
/// `BucketOwnerEnforced` when `x-amz-object-ownership` is absent. Use this
/// helper when a test intentionally exercises the modern BOE path.
pub async fn create_boe_bucket(client: &Client) -> String {
    let bucket = unique_bucket();
    create_test_bucket(client, &bucket).await;
    bucket
}

/// Create a bucket with an explicit object ownership mode.
pub async fn create_bucket_with_ownership(client: &Client, ownership: ObjectOwnership) -> String {
    let bucket = unique_bucket();
    create_test_bucket_with_ownership(client, &bucket, ownership).await;
    bucket
}

/// Create a bucket in legacy ACL mode and disable bucket-level public access block.
///
/// This helper is for tests that intentionally exercise ACL authorization. It
/// accepts `ObjectWriter` and `BucketOwnerPreferred`, but rejects BOE because
/// BOE disables ACL writes by design.
pub async fn create_acl_enabled_bucket(client: &Client, ownership: ObjectOwnership) -> String {
    assert!(
        matches!(
            ownership,
            ObjectOwnership::ObjectWriter | ObjectOwnership::BucketOwnerPreferred
        ),
        "ACL-enabled test buckets must use ObjectWriter or BucketOwnerPreferred"
    );
    let bucket = create_bucket_with_ownership(client, ownership).await;
    disable_bucket_public_access_block(client, &bucket).await;
    bucket
}

async fn wait_for_bucket_public_access_block_disabled(client: &Client, bucket: &str) {
    const MAX_ATTEMPTS: usize = 20;

    for attempt in 0..MAX_ATTEMPTS {
        let result = client.get_public_access_block().bucket(bucket).send().await;
        if let Ok(resp) = result {
            if let Some(config) = resp.public_access_block_configuration() {
                if config.block_public_acls() == Some(false)
                    && config.ignore_public_acls() == Some(false)
                    && config.block_public_policy() == Some(false)
                    && config.restrict_public_buckets() == Some(false)
                {
                    return;
                }
            }
        }
        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }
        panic!("bucket public access block disablement did not converge for {bucket}");
    }
}

fn sse_c_enabled_bucket_encryption() -> ServerSideEncryptionConfiguration {
    let default = ServerSideEncryptionByDefault::builder()
        .sse_algorithm(ServerSideEncryption::Aes256)
        .build()
        .unwrap();
    ServerSideEncryptionConfiguration::builder()
        .rules(
            ServerSideEncryptionRule::builder()
                .apply_server_side_encryption_by_default(default)
                .blocked_encryption_types(
                    BlockedEncryptionTypes::builder()
                        .encryption_type(EncryptionType::None)
                        .build(),
                )
                .build(),
        )
        .build()
        .unwrap()
}

fn bucket_encryption_blocks_sse_c(config: &ServerSideEncryptionConfiguration) -> Option<bool> {
    let rule = config.rules().first()?;
    let blocked = rule
        .blocked_encryption_types()
        .map(|types| {
            types
                .encryption_type()
                .iter()
                .map(|value| value.as_str())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    Some(blocked.contains(&"SSE-C"))
}

async fn wait_for_bucket_sse_c_enabled(client: &Client, bucket: &str) {
    const MAX_ATTEMPTS: usize = 20;

    for attempt in 0..MAX_ATTEMPTS {
        let result = client.get_bucket_encryption().bucket(bucket).send().await;
        if let Ok(resp) = result {
            if let Some(config) = resp.server_side_encryption_configuration() {
                if bucket_encryption_blocks_sse_c(config) == Some(false) {
                    return;
                }
            }
        }
        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }
        panic!("bucket SSE-C enablement did not converge for {bucket}");
    }
}

/// Explicitly allow SSE-C on a bucket.
///
/// AWS now disables SSE-C by default on new buckets until
/// `PutBucketEncryption` sets `BlockedEncryptionTypes = NONE`. External SSE-C
/// fixtures use this helper so their positive-path coverage stays focused on
/// SSE-C behavior rather than bucket-default gating.
pub async fn enable_bucket_sse_c(client: &Client, bucket: &str) {
    client
        .put_bucket_encryption()
        .bucket(bucket)
        .server_side_encryption_configuration(sse_c_enabled_bucket_encryption())
        .send()
        .await
        .expect("enable bucket SSE-C");
    wait_for_bucket_sse_c_enabled(client, bucket).await;
}

/// Create a bucket and explicitly allow SSE-C on it.
pub async fn create_bucket_with_sse_c_enabled(
    client: &Client,
    bucket: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    create_test_bucket(client, bucket).await;
    enable_bucket_sse_c(client, bucket).await;
    Ok(())
}

/// Create a bucket and populate it with `n` objects named "key0", "key1", ...
///
/// Returns the bucket name and the list of keys.
pub async fn create_objects(client: &Client, prefix: &str, n: usize) -> (String, Vec<String>) {
    let bucket = unique_bucket();
    create_test_bucket(client, &bucket).await;

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
    create_test_bucket(client, &bucket).await;

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
    let bucket = create_acl_enabled_bucket(client, ObjectOwnership::BucketOwnerPreferred).await;

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
    let bucket = create_acl_enabled_bucket(client, ObjectOwnership::BucketOwnerPreferred).await;

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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: String,
    pub body_read_error: Option<String>,
}

#[derive(Clone, Copy, Debug)]
pub struct SignedRequestCredentials<'a> {
    pub access_key: &'a str,
    pub secret_key: &'a str,
    pub region: &'a str,
    pub tls_ca_pem: Option<&'a [u8]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresignedRequest {
    uri: String,
    headers: Vec<(String, String)>,
}

impl PresignedRequest {
    pub fn uri(&self) -> &str {
        &self.uri
    }

    pub fn headers(&self) -> impl Iterator<Item = (&str, &str)> {
        self.headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
    }
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

pub fn object_url(endpoint: &str, bucket: &str, key: &str, query: Option<&str>) -> String {
    let encoded_key = auth::canonical::uri_encode_path(key);
    match query {
        Some(query) => format!("{endpoint}/{bucket}/{encoded_key}?{query}"),
        None => format!("{endpoint}/{bucket}/{encoded_key}"),
    }
}

pub fn presign_url<K, V, I>(
    method: &str,
    url_str: &str,
    expires: Duration,
    extra_headers: I,
    payload_hash: Option<&str>,
) -> PresignedRequest
where
    K: AsRef<str>,
    V: AsRef<str>,
    I: IntoIterator<Item = (K, V)>,
{
    presign_url_with_credentials(
        method,
        url_str,
        expires,
        extra_headers,
        payload_hash,
        SignedRequestCredentials {
            access_key: CTX.access_key(),
            secret_key: CTX.secret_key(),
            region: CTX.region(),
            tls_ca_pem: CTX.tls_ca_pem(),
        },
    )
}

pub fn presign_url_with_credentials<K, V, I>(
    method: &str,
    url_str: &str,
    expires: Duration,
    extra_headers: I,
    payload_hash: Option<&str>,
    credentials: SignedRequestCredentials<'_>,
) -> PresignedRequest
where
    K: AsRef<str>,
    V: AsRef<str>,
    I: IntoIterator<Item = (K, V)>,
{
    let parsed = url::Url::parse(url_str).expect("parse URL");
    let path = parsed.path();
    let base_query = parsed.query().unwrap_or("");
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

    let payload_hash = payload_hash.unwrap_or("UNSIGNED-PAYLOAD").to_string();
    let mut request_headers = vec![("host".to_string(), host)];
    if payload_hash != "UNSIGNED-PAYLOAD" {
        request_headers.push(("x-amz-content-sha256".to_string(), payload_hash.clone()));
    }
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
    let credential = format!(
        "{}/{}/{}/s3/aws4_request",
        credentials.access_key, date_stamp, credentials.region
    );
    let mut raw_query_parts = Vec::new();
    if !base_query.is_empty() {
        raw_query_parts.push(base_query.to_string());
    }
    raw_query_parts.push("X-Amz-Algorithm=AWS4-HMAC-SHA256".to_string());
    raw_query_parts.push(format!("X-Amz-Credential={credential}"));
    raw_query_parts.push(format!("X-Amz-Date={amz_date}"));
    raw_query_parts.push(format!("X-Amz-Expires={}", expires.as_secs()));
    raw_query_parts.push(format!("X-Amz-SignedHeaders={signed_headers}"));
    let canonical_query = normalize_query(&raw_query_parts.join("&"));
    let canonical_request = format!(
        "{method}\n{path}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    );
    let scope = format!("{date_stamp}/{}/s3/aws4_request", credentials.region);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );
    let signing_key = derive_signing_key(credentials.secret_key, date_stamp, credentials.region);
    let signature = hmac_sha256(&signing_key, string_to_sign.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let uri = format!(
        "{}{}?{}&X-Amz-Signature={signature}",
        parsed.origin().ascii_serialization(),
        path,
        canonical_query
    );
    let headers = request_headers
        .into_iter()
        .filter(|(name, _)| name != "host")
        .collect();

    PresignedRequest { uri, headers }
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
    send_signed_request_for_service_with_credentials(
        method,
        url_str,
        body,
        extra_headers,
        "s3",
        SignedRequestCredentials {
            access_key: CTX.access_key(),
            secret_key: CTX.secret_key(),
            region: CTX.region(),
            tls_ca_pem: CTX.tls_ca_pem(),
        },
    )
}

/// Send a raw signed S3 request and preserve response metadata if the response
/// body races with an early server-side connection close.
pub fn send_signed_request_allow_response_body_error<K, V, I>(
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
    send_signed_request_to_endpoint_for_service_with_credentials_inner(
        method,
        url_str,
        url_str,
        body,
        extra_headers,
        "s3",
        SignedRequestCredentials {
            access_key: CTX.access_key(),
            secret_key: CTX.secret_key(),
            region: CTX.region(),
            tls_ca_pem: CTX.tls_ca_pem(),
        },
        true,
    )
}

/// Send a raw signed S3 request using explicit endpoint credentials.
pub fn send_signed_request_with_credentials<K, V, I>(
    method: &str,
    url_str: &str,
    body: &[u8],
    extra_headers: I,
    credentials: SignedRequestCredentials<'_>,
) -> RawResponse
where
    K: AsRef<str>,
    V: AsRef<str>,
    I: IntoIterator<Item = (K, V)>,
{
    send_signed_request_for_service_with_credentials(
        method,
        url_str,
        body,
        extra_headers,
        "s3",
        credentials,
    )
}

/// Send a raw signed request using an explicit SigV4 service name.
pub fn send_signed_request_for_service_with_credentials<K, V, I>(
    method: &str,
    url_str: &str,
    body: &[u8],
    extra_headers: I,
    service: &str,
    credentials: SignedRequestCredentials<'_>,
) -> RawResponse
where
    K: AsRef<str>,
    V: AsRef<str>,
    I: IntoIterator<Item = (K, V)>,
{
    send_signed_request_to_endpoint_for_service_with_credentials(
        method,
        url_str,
        url_str,
        body,
        extra_headers,
        service,
        credentials,
    )
}

/// Send a raw signed request to one endpoint while signing and sending a
/// distinct `Host` header.
pub fn send_signed_request_to_endpoint_for_service_with_credentials<K, V, I>(
    method: &str,
    connect_url_str: &str,
    signed_url_str: &str,
    body: &[u8],
    extra_headers: I,
    service: &str,
    credentials: SignedRequestCredentials<'_>,
) -> RawResponse
where
    K: AsRef<str>,
    V: AsRef<str>,
    I: IntoIterator<Item = (K, V)>,
{
    send_signed_request_to_endpoint_for_service_with_credentials_inner(
        method,
        connect_url_str,
        signed_url_str,
        body,
        extra_headers,
        service,
        credentials,
        false,
    )
}

#[allow(clippy::too_many_arguments)]
fn send_signed_request_to_endpoint_for_service_with_credentials_inner<K, V, I>(
    method: &str,
    connect_url_str: &str,
    signed_url_str: &str,
    body: &[u8],
    extra_headers: I,
    service: &str,
    credentials: SignedRequestCredentials<'_>,
    allow_response_body_error: bool,
) -> RawResponse
where
    K: AsRef<str>,
    V: AsRef<str>,
    I: IntoIterator<Item = (K, V)>,
{
    let parsed = url::Url::parse(connect_url_str).expect("parse URL");
    let signed_parsed = url::Url::parse(signed_url_str).expect("parse signed URL");
    let endpoint = parsed.origin().ascii_serialization();
    let agent = crate::build_test_agent(
        &endpoint,
        credentials.tls_ca_pem,
        crate::configured_test_timeout(),
    );
    let path = signed_parsed.path();
    let query = normalize_query(signed_parsed.query().unwrap_or(""));

    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let amz_date = format_amz_date(secs);
    let date_stamp = &amz_date[..8];

    let host = signed_parsed
        .host_str()
        .map(|host| {
            if let Some(port) = signed_parsed.port() {
                format!("{host}:{port}")
            } else {
                host.to_string()
            }
        })
        .expect("URL host");
    let host_header = host.clone();
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

    let scope = format!("{date_stamp}/{}/{service}/aws4_request", credentials.region);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );
    let signing_key = derive_signing_key_with_service(
        credentials.secret_key,
        date_stamp,
        credentials.region,
        service,
    );
    let signature: String = hmac_sha256(&signing_key, string_to_sign.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        credentials.access_key
    );

    const MAX_SLOWDOWN_RETRIES: u32 = 4;
    const MAX_TRANSPORT_RETRIES: u32 = 3;
    let mut attempt = 0;
    loop {
        let response_result = if method == "HEAD" {
            let mut request = agent
                .head(connect_url_str)
                .header("Authorization", &authorization)
                .header("x-amz-date", &amz_date)
                .header("Host", &host_header)
                .header("x-amz-content-sha256", &payload_hash);
            for (name, value) in &request_headers {
                if name == "host" || name == "x-amz-content-sha256" || name == "x-amz-date" {
                    continue;
                }
                request = request.header(name, value);
            }
            request.call()
        } else if method == "GET" {
            let mut request = agent
                .get(connect_url_str)
                .header("Authorization", &authorization)
                .header("x-amz-date", &amz_date)
                .header("Host", &host_header)
                .header("x-amz-content-sha256", &payload_hash);
            for (name, value) in &request_headers {
                if name == "host" || name == "x-amz-content-sha256" || name == "x-amz-date" {
                    continue;
                }
                request = request.header(name, value);
            }
            request.call()
        } else if method == "DELETE" {
            let mut request = agent
                .delete(connect_url_str)
                .header("Authorization", &authorization)
                .header("x-amz-date", &amz_date)
                .header("Host", &host_header)
                .header("x-amz-content-sha256", &payload_hash);
            for (name, value) in &request_headers {
                if name == "host" || name == "x-amz-content-sha256" || name == "x-amz-date" {
                    continue;
                }
                request = request.header(name, value);
            }
            request.call()
        } else {
            let mut request = match method {
                "PUT" => agent.put(connect_url_str),
                "POST" => agent.post(connect_url_str),
                other => panic!("unsupported method: {other}"),
            }
            .header("Authorization", &authorization)
            .header("x-amz-date", &amz_date)
            .header("Host", &host_header)
            .header("x-amz-content-sha256", &payload_hash);
            for (name, value) in &request_headers {
                if name == "host" || name == "x-amz-content-sha256" || name == "x-amz-date" {
                    continue;
                }
                request = request.header(name, value);
            }
            if allow_response_body_error {
                request.send_allow_response_body_error(body)
            } else {
                request.send(body)
            }
        };
        let mut response = match response_result {
            Ok(response) => response,
            Err(_err) if attempt < MAX_TRANSPORT_RETRIES => {
                let backoff_ms = 100u64 << attempt;
                thread::sleep(Duration::from_millis(backoff_ms));
                attempt += 1;
                continue;
            }
            Err(err) => panic!("raw request transport error: {err}"),
        };
        let status = response.status().as_u16();
        let body_read_error = response.body_read_error().map(ToOwned::to_owned);
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
        let headers = response
            .headers()
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_string(),
                    value
                        .to_str()
                        .expect("response header is valid utf-8")
                        .to_string(),
                )
            })
            .collect();
        return RawResponse {
            status,
            headers,
            body: body_text,
            body_read_error,
        };
    }
}

fn derive_signing_key_with_service(
    secret_key: &str,
    date_stamp: &str,
    region: &str,
    service: &str,
) -> Vec<u8> {
    let k_date = hmac_sha256(
        format!("AWS4{secret_key}").as_bytes(),
        date_stamp.as_bytes(),
    );
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
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

fn derive_signing_key(secret_key: &str, date_stamp: &str, region: &str) -> Vec<u8> {
    let k_date = hmac_sha256(
        format!("AWS4{secret_key}").as_bytes(),
        date_stamp.as_bytes(),
    );
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, b"s3");
    hmac_sha256(&k_service, b"aws4_request")
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
    canonical_query_string(raw)
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
/// The source key path is percent-encoded with `/` preserved as a path
/// separator. The `version_id` query component is always percent-encoded.
pub fn copy_source_with_version(bucket: &str, source_key: &str, version_id: &str) -> String {
    let encoded_key = auth::canonical::uri_encode_path(source_key);
    let encoded_version_id: String =
        url::form_urlencoded::byte_serialize(version_id.as_bytes()).collect();
    format!("{bucket}/{encoded_key}?versionId={encoded_version_id}")
}

/// Delete all object versions and delete markers in a bucket, then delete the bucket.
///
/// This is needed for versioned buckets on AWS where simple delete_object creates
/// delete markers rather than removing objects.
pub async fn cleanup_versioned_bucket(client: &Client, bucket: &str) {
    loop {
        let resp = match client.list_object_versions().bucket(bucket).send().await {
            Ok(resp) => resp,
            Err(err) if is_bucket_already_absent(&err) => return,
            Err(err) => panic!("list object versions: {err:?}"),
        };

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

        for chunk in objects.chunks(25) {
            let mut last_error = None;
            for _ in 0..5 {
                let delete = aws_sdk_s3::types::Delete::builder()
                    .set_objects(Some(chunk.to_vec()))
                    .quiet(true)
                    .build()
                    .unwrap();
                match delete_objects_with_md5(client, bucket, delete).send().await {
                    Ok(_) => {
                        last_error = None;
                        break;
                    }
                    Err(err) if is_bucket_already_absent(&err) => return,
                    Err(err) => {
                        last_error = Some(err);
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                }
            }
            if let Some(err) = last_error {
                panic!("delete objects: {err:?}");
            }
        }
    }

    match client.delete_bucket().bucket(bucket).send().await {
        Ok(_) => {}
        Err(err) if is_bucket_already_absent(&err) => {}
        Err(err) => panic!("delete bucket: {err:?}"),
    }
}

/// Assert that an S3 SDK error contains the expected error code string.
pub fn assert_s3_err_code<T, E: std::fmt::Debug>(
    result: &Result<T, aws_sdk_s3::error::SdkError<E>>,
    expected_code: &str,
) {
    match result {
        Ok(_) => panic!("expected error with code {}, got Ok", expected_code),
        Err(e) => {
            if expected_code == "AccessDenied"
                && e.raw_response().map(|r| r.status().as_u16()) == Some(403)
            {
                return;
            }
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

/// Return true for SDK errors caused by the server closing an in-flight request body.
///
/// Some tests intentionally race a streaming request against a server-side state
/// transition that can reject the request after the client has started writing.
/// In that shape there may be no S3 error response for the SDK to expose.
pub fn is_sdk_stream_disconnect<E: std::fmt::Debug>(err: &aws_sdk_s3::error::SdkError<E>) -> bool {
    if err.raw_response().is_some() {
        return false;
    }
    let message = format!("{err:?}");
    message.contains("DispatchFailure")
        && (message.contains("BodyWrite")
            || message.contains("BrokenPipe")
            || message.contains("Broken pipe")
            || message.contains("Connection reset")
            || message.contains("IncompleteMessage"))
}

/// Return true for SDK errors caused by a streaming request being rejected
/// while the client is still writing, accepting either a pure dispatch
/// disconnect or a response whose status arrived before the error body failed.
pub fn is_sdk_stream_disconnect_or_status<E: std::fmt::Debug>(
    err: &aws_sdk_s3::error::SdkError<E>,
    expected_status: u16,
) -> bool {
    if is_sdk_stream_disconnect(err) {
        return true;
    }
    if err.raw_response().map(|r| r.status().as_u16()) != Some(expected_status) {
        return false;
    }
    let message = format!("{err:?}");
    message.contains("ResponseError")
        && (message.contains("hyper::Error(Body")
            || message.contains("connection error")
            || message.contains("IncompleteMessage")
            || message.contains("incomplete message"))
}
