//! Atomic read/write integration tests (Ceph group 32).
//!
//! Verifies that S3 object writes are atomic from a reader's perspective:
//! concurrent readers always see either the complete old object or the
//! complete new object, never a mix.

use aws_sdk_s3::primitives::ByteStream;
use s3_tests::{
    delete_bucket_retrying_operation_aborted, err_status,
    get_object_body_retrying_operation_aborted, put_object_retrying_operation_aborted,
    retrying_operation_aborted_result, unique_bucket, SendRetryingOperationAborted, CTX,
};

const ONE_MIB: usize = 1024 * 1024;
const FOUR_MIB: usize = 4 * ONE_MIB;
const EIGHT_MIB: usize = 8 * ONE_MIB;
const TEN_MIB: usize = 10 * ONE_MIB;

async fn setup_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    bucket
}

async fn cleanup(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        client
            .delete_object()
            .bucket(bucket)
            .key(*key)
            .send_retrying_operation_aborted("delete object during atomic cleanup")
            .await
            .unwrap_or_else(|err| panic!("delete object during atomic cleanup: {err:?}"));
    }
    delete_bucket_retrying_operation_aborted(client, bucket).await;
}

fn make_body(ch: u8, size: usize) -> Vec<u8> {
    vec![ch; size]
}

async fn get_body_result(bucket: &str, key: &str) -> Result<Vec<u8>, String> {
    let resp = CTX
        .client()
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(|err| format!("{err:?}"))?;
    resp.body
        .collect()
        .await
        .map(|body| body.into_bytes().to_vec())
        .map_err(|err| format!("{err:?}"))
}

/// Assert that every byte in `data` is the same value (no mixing).
fn assert_uniform(data: &[u8], expected_len: usize) {
    assert_eq!(data.len(), expected_len, "unexpected body length");
    assert!(!data.is_empty(), "body is empty");
    let first = data[0];
    assert!(
        data.iter().all(|&b| b == first),
        "body is not uniform: expected all bytes to be 0x{:02x}, found mixed content",
        first,
    );
}

#[derive(Debug)]
struct AtomicAttemptError {
    context: &'static str,
    message: String,
}

impl AtomicAttemptError {
    fn new(context: &'static str, message: String) -> Self {
        Self { context, message }
    }
}

async fn atomic_read_case(size: usize) {
    run_atomic_case(|| atomic_read_attempt(size)).await;
}

async fn run_atomic_case<F, Fut>(attempt_fn: F)
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<(), AtomicAttemptError>>,
{
    if let Err(err) = attempt_fn().await {
        panic!("atomic case failed during {}: {}", err.context, err.message);
    }
}

async fn atomic_read_attempt(size: usize) -> Result<(), AtomicAttemptError> {
    let client = CTX.client();
    let bucket = setup_bucket().await;
    let key = "atomic-read";

    let result = async {
        retrying_operation_aborted_result(|| {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from(make_body(b'A', size)))
                .send()
        })
        .await
        .map_err(|err| AtomicAttemptError::new("initial put", format!("{err:?}")))?;

        let bucket2 = bucket.clone();
        let write_task = tokio::spawn(async move {
            retrying_operation_aborted_result(|| {
                CTX.client()
                    .put_object()
                    .bucket(&bucket2)
                    .key("atomic-read")
                    .body(ByteStream::from(make_body(b'B', size)))
                    .send()
            })
            .await
            .map_err(|err| format!("{err:?}"))?;
            Ok::<(), String>(())
        });

        let bucket3 = bucket.clone();
        let read_task = tokio::spawn(async move { get_body_result(&bucket3, "atomic-read").await });

        let (write_result, read_result) = tokio::join!(write_task, read_task);
        write_result
            .map_err(|err| AtomicAttemptError::new("concurrent put task", err.to_string()))?
            .map_err(|err| AtomicAttemptError::new("concurrent put", err))?;
        let body = read_result
            .map_err(|err| AtomicAttemptError::new("concurrent get task", err.to_string()))?
            .map_err(|err| AtomicAttemptError::new("concurrent get", err))?;

        assert_uniform(&body, size);

        let final_body = get_body_result(&bucket, key)
            .await
            .map_err(|err| AtomicAttemptError::new("final get", err))?;
        assert_uniform(&final_body, size);
        assert_eq!(final_body[0], b'B');

        Ok(())
    }
    .await;

    cleanup(&bucket, &[key]).await;
    result
}

async fn atomic_write_case(size: usize) {
    run_atomic_case(|| atomic_write_attempt(size)).await;
}

async fn atomic_write_attempt(size: usize) -> Result<(), AtomicAttemptError> {
    let client = CTX.client();
    let bucket = setup_bucket().await;
    let key = "atomic-write";

    let result = async {
        retrying_operation_aborted_result(|| {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from(make_body(b'A', size)))
                .send()
        })
        .await
        .map_err(|err| AtomicAttemptError::new("first put", format!("{err:?}")))?;

        let body = get_body_result(&bucket, key)
            .await
            .map_err(|err| AtomicAttemptError::new("first get", err))?;
        assert_uniform(&body, size);
        assert_eq!(body[0], b'A');

        retrying_operation_aborted_result(|| {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from(make_body(b'B', size)))
                .send()
        })
        .await
        .map_err(|err| AtomicAttemptError::new("second put", format!("{err:?}")))?;

        let body = get_body_result(&bucket, key)
            .await
            .map_err(|err| AtomicAttemptError::new("second get", err))?;
        assert_uniform(&body, size);
        assert_eq!(body[0], b'B');

        Ok(())
    }
    .await;

    cleanup(&bucket, &[key]).await;
    result
}

async fn atomic_dual_write_case(size: usize) {
    run_atomic_case(|| atomic_dual_write_attempt(size)).await;
}

async fn atomic_dual_write_attempt(size: usize) -> Result<(), AtomicAttemptError> {
    let client = CTX.client();
    let bucket = setup_bucket().await;
    let key = "atomic-dual-write";

    let result = async {
        retrying_operation_aborted_result(|| {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from(make_body(b'X', size)))
                .send()
        })
        .await
        .map_err(|err| AtomicAttemptError::new("initial put", format!("{err:?}")))?;

        let bucket2 = bucket.clone();
        let write_a = tokio::spawn(async move {
            retrying_operation_aborted_result(|| {
                CTX.client()
                    .put_object()
                    .bucket(&bucket2)
                    .key("atomic-dual-write")
                    .body(ByteStream::from(make_body(b'A', size)))
                    .send()
            })
            .await
            .map_err(|err| format!("{err:?}"))
        });

        let bucket3 = bucket.clone();
        let write_b = tokio::spawn(async move {
            retrying_operation_aborted_result(|| {
                CTX.client()
                    .put_object()
                    .bucket(&bucket3)
                    .key("atomic-dual-write")
                    .body(ByteStream::from(make_body(b'B', size)))
                    .send()
            })
            .await
            .map_err(|err| format!("{err:?}"))
        });

        let (a, b) = tokio::join!(write_a, write_b);
        a.map_err(|err| AtomicAttemptError::new("concurrent put A task", err.to_string()))?
            .map_err(|err| AtomicAttemptError::new("concurrent put A", err))?;
        b.map_err(|err| AtomicAttemptError::new("concurrent put B task", err.to_string()))?
            .map_err(|err| AtomicAttemptError::new("concurrent put B", err))?;

        let body = get_body_result(&bucket, key)
            .await
            .map_err(|err| AtomicAttemptError::new("final get", err))?;
        assert_uniform(&body, size);
        assert!(
            body[0] == b'A' || body[0] == b'B',
            "expected all 'A' or all 'B', got 0x{:02x}",
            body[0],
        );

        Ok(())
    }
    .await;

    cleanup(&bucket, &[key]).await;
    result
}

macro_rules! atomic_size_test {
    ($name:ident, $helper:ident, $size:expr) => {
        #[test]
        fn $name() {
            s3_tests::run(async {
                $helper($size).await;
            });
        }
    };
}

// Concurrent read during write sees either the old or new object, never mixed.
// Matches Ceph: `*_1mb`, `*_4mb`, `*_8mb`; extends coverage with `*_10mb`.
atomic_size_test!(test_atomic_read_1mb, atomic_read_case, ONE_MIB);
atomic_size_test!(test_atomic_read_4mb, atomic_read_case, FOUR_MIB);
atomic_size_test!(test_atomic_read_8mb, atomic_read_case, EIGHT_MIB);
atomic_size_test!(test_atomic_read_10mb, atomic_read_case, TEN_MIB);

// Sequential overwrite produces a clean, complete replacement.
// Matches Ceph: `*_1mb`, `*_4mb`, `*_8mb`; extends coverage with `*_10mb`.
atomic_size_test!(test_atomic_write_1mb, atomic_write_case, ONE_MIB);
atomic_size_test!(test_atomic_write_4mb, atomic_write_case, FOUR_MIB);
atomic_size_test!(test_atomic_write_8mb, atomic_write_case, EIGHT_MIB);
atomic_size_test!(test_atomic_write_10mb, atomic_write_case, TEN_MIB);

// Two concurrent writes: final object is entirely one or the other.
// Matches Ceph: `*_1mb`, `*_4mb`, `*_8mb`; extends coverage with `*_10mb`.
atomic_size_test!(test_atomic_dual_write_1mb, atomic_dual_write_case, ONE_MIB);
atomic_size_test!(test_atomic_dual_write_4mb, atomic_dual_write_case, FOUR_MIB);
atomic_size_test!(
    test_atomic_dual_write_8mb,
    atomic_dual_write_case,
    EIGHT_MIB
);
atomic_size_test!(test_atomic_dual_write_10mb, atomic_dual_write_case, TEN_MIB);

async fn atomic_conditional_write_attempt() -> Result<(), AtomicAttemptError> {
    let client = CTX.client();
    let bucket = setup_bucket().await;
    let key = "atomic-cond-write";

    let result = async {
        let resp = retrying_operation_aborted_result(|| {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from(make_body(b'A', ONE_MIB)))
                .send()
        })
        .await
        .map_err(|err| AtomicAttemptError::new("initial conditional put", format!("{err:?}")))?;
        let etag_a = resp.e_tag().unwrap().to_string();

        retrying_operation_aborted_result(|| {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .if_match(&etag_a)
                .body(ByteStream::from(make_body(b'B', ONE_MIB)))
                .send()
        })
        .await
        .map_err(|err| AtomicAttemptError::new("conditional overwrite put", format!("{err:?}")))?;

        let body = get_body_result(&bucket, key)
            .await
            .map_err(|err| AtomicAttemptError::new("conditional final get", err))?;
        assert_uniform(&body, ONE_MIB);
        assert_eq!(body[0], b'B');

        Ok(())
    }
    .await;

    cleanup(&bucket, &[key]).await;
    result
}

/// Conditional overwrite with if_match(<etag>) succeeds atomically.
///
/// Matches Ceph: test_atomic_conditional_write_1mb
#[test]
fn test_atomic_conditional_write() {
    s3_tests::run(async {
        run_atomic_case(atomic_conditional_write_attempt).await;
    });
}

async fn atomic_dual_conditional_write_attempt() -> Result<(), AtomicAttemptError> {
    let client = CTX.client();
    let bucket = setup_bucket().await;
    let key = "atomic-dual-cond";

    let result = async {
        let resp = retrying_operation_aborted_result(|| {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from(make_body(b'A', ONE_MIB)))
                .send()
        })
        .await
        .map_err(|err| AtomicAttemptError::new("initial stale-etag put", format!("{err:?}")))?;
        let etag_a = resp.e_tag().unwrap().to_string();

        retrying_operation_aborted_result(|| {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from(make_body(b'B', ONE_MIB)))
                .send()
        })
        .await
        .map_err(|err| AtomicAttemptError::new("stale-etag overwrite put", format!("{err:?}")))?;

        let stale_result = retrying_operation_aborted_result(|| {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .if_match(&etag_a)
                .body(ByteStream::from(make_body(b'C', ONE_MIB)))
                .send()
        })
        .await;
        match stale_result {
            Ok(_) => {
                return Err(AtomicAttemptError::new(
                    "stale conditional put",
                    "expected HTTP 412, got success".to_string(),
                ));
            }
            Err(err)
                if err
                    .raw_response()
                    .map(|response| response.status().as_u16())
                    == Some(412) => {}
            Err(err) => {
                return Err(AtomicAttemptError::new(
                    "stale conditional put",
                    format!("{err:?}"),
                ));
            }
        }

        let body = get_body_result(&bucket, key)
            .await
            .map_err(|err| AtomicAttemptError::new("stale-etag final get", err))?;
        assert_uniform(&body, ONE_MIB);
        assert_eq!(body[0], b'B');

        Ok(())
    }
    .await;

    cleanup(&bucket, &[key]).await;
    result
}

/// Conditional overwrite with a stale etag fails with 412 PreconditionFailed.
///
/// Matches Ceph: test_atomic_dual_conditional_write_1mb
#[test]
fn test_atomic_dual_conditional_write() {
    s3_tests::run(async {
        run_atomic_case(atomic_dual_conditional_write_attempt).await;
    });
}

/// Writing to a deleted bucket returns 404 NoSuchBucket.
///
/// Matches Ceph: test_atomic_write_bucket_gone
#[test]
fn test_atomic_write_bucket_gone() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();

        // Create then immediately delete the bucket
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;

        // PUT to the gone bucket → 404
        let result = client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from(make_body(b'A', ONE_MIB)))
            .send()
            .await;
        assert_eq!(err_status(&result), 404);
    });
}

/// A pre-existing object remains readable while a multipart upload to
/// the same key is in progress, and survives abort of that upload.
///
/// Matches Ceph: test_atomic_multipart_upload_write
#[test]
fn test_atomic_multipart_upload_write() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "foo";

        // Put a pre-existing object
        put_object_retrying_operation_aborted(client, &bucket, key, b"bar".to_vec()).await;

        // Start a multipart upload to the same key
        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("start atomic multipart upload")
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        // Original object is still readable during in-progress MPU
        let body = get_object_body_retrying_operation_aborted(
            client,
            &bucket,
            key,
            None,
            "get object during atomic multipart test",
        )
        .await;
        assert_eq!(&body[..], b"bar");

        // Abort the multipart upload
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send_retrying_operation_aborted("abort atomic multipart upload")
            .await
            .unwrap();

        // Original object still intact after abort
        let body = get_object_body_retrying_operation_aborted(
            client,
            &bucket,
            key,
            None,
            "get object during atomic multipart test",
        )
        .await;
        assert_eq!(&body[..], b"bar");

        cleanup(&bucket, &[key]).await;
    });
}
