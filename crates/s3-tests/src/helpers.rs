use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::LazyLock;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;

static BUCKET_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Bucket name prefix, configurable via `S3_TEST_BUCKET_PREFIX`.
/// Defaults to `"test"`.
static BUCKET_PREFIX: LazyLock<String> = LazyLock::new(|| {
    std::env::var("S3_TEST_BUCKET_PREFIX").unwrap_or_else(|_| "test".to_string())
});

/// Return the bucket prefix (from `S3_TEST_BUCKET_PREFIX` or `"test"`).
pub fn bucket_prefix() -> &'static str {
    &BUCKET_PREFIX
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
        client
            .delete_objects()
            .bucket(bucket)
            .delete(delete)
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
