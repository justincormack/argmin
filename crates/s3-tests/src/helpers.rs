use std::sync::atomic::{AtomicU64, Ordering};

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;

static BUCKET_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generate a unique bucket name for a test.
///
/// Uses a monotonic counter + process ID to avoid collisions between
/// parallel test runs and between tests within the same run.
pub fn unique_bucket() -> String {
    let n = BUCKET_COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    format!("test-{}-{}-{}", pid, n, timestamp_millis())
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
pub async fn create_objects(
    client: &Client,
    prefix: &str,
    n: usize,
) -> (String, Vec<String>) {
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
