use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::LazyLock;

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
