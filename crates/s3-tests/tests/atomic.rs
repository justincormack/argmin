//! Atomic read/write integration tests (Ceph group 32).
//!
//! Verifies that S3 object writes are atomic from a reader's perspective:
//! concurrent readers always see either the complete old object or the
//! complete new object, never a mix.

use aws_sdk_s3::primitives::ByteStream;
use s3_tests::{err_status, unique_bucket, CTX};

const SIZE: usize = 1024 * 1024; // 1 MB

async fn setup_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    client.create_bucket().bucket(&bucket).send().await.unwrap();
    bucket
}

async fn cleanup(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = client.delete_object().bucket(bucket).key(*key).send().await;
    }
    client.delete_bucket().bucket(bucket).send().await.unwrap();
}

fn make_body(ch: u8, size: usize) -> Vec<u8> {
    vec![ch; size]
}

async fn get_body(bucket: &str, key: &str) -> Vec<u8> {
    let resp = CTX
        .client()
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    resp.body.collect().await.unwrap().into_bytes().to_vec()
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

/// Concurrent read during write sees either the old or new object, never mixed.
///
/// Matches Ceph: test_atomic_read_1mb / test_atomic_read_4mb / test_atomic_read_8mb
#[test]
fn test_atomic_read() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "atomic-read";

        // Initial write: 1 MB of 'A'
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(make_body(b'A', SIZE)))
            .send()
            .await
            .unwrap();

        // Concurrent overwrite + read
        let bucket2 = bucket.clone();
        let write_task = tokio::spawn(async move {
            CTX.client()
                .put_object()
                .bucket(&bucket2)
                .key("atomic-read")
                .body(ByteStream::from(make_body(b'B', SIZE)))
                .send()
                .await
                .unwrap();
        });

        let bucket3 = bucket.clone();
        let read_task = tokio::spawn(async move { get_body(&bucket3, "atomic-read").await });

        let (write_result, read_result) = tokio::join!(write_task, read_task);
        write_result.unwrap();
        let body = read_result.unwrap();

        // The read must see either all 'A' or all 'B', never a mix
        assert_uniform(&body, SIZE);

        // After the write completes, the object must be all 'B'
        let final_body = get_body(&bucket, key).await;
        assert_uniform(&final_body, SIZE);
        assert_eq!(final_body[0], b'B');

        cleanup(&bucket, &[key]).await;
    });
}

/// Sequential overwrite produces a clean, complete replacement.
///
/// Matches Ceph: test_atomic_write_1mb / test_atomic_write_4mb / test_atomic_write_8mb
#[test]
fn test_atomic_write() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "atomic-write";

        // Write 'A', read back
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(make_body(b'A', SIZE)))
            .send()
            .await
            .unwrap();
        let body = get_body(&bucket, key).await;
        assert_uniform(&body, SIZE);
        assert_eq!(body[0], b'A');

        // Overwrite with 'B', read back
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(make_body(b'B', SIZE)))
            .send()
            .await
            .unwrap();
        let body = get_body(&bucket, key).await;
        assert_uniform(&body, SIZE);
        assert_eq!(body[0], b'B');

        cleanup(&bucket, &[key]).await;
    });
}

/// Two concurrent writes — final object is entirely one or the other.
///
/// Matches Ceph: test_atomic_dual_write_1mb / test_atomic_dual_write_4mb / test_atomic_dual_write_8mb
#[test]
fn test_atomic_dual_write() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "atomic-dual-write";

        // Seed the object
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(make_body(b'X', SIZE)))
            .send()
            .await
            .unwrap();

        // Two concurrent overwrites
        let bucket2 = bucket.clone();
        let write_a = tokio::spawn(async move {
            CTX.client()
                .put_object()
                .bucket(&bucket2)
                .key("atomic-dual-write")
                .body(ByteStream::from(make_body(b'A', SIZE)))
                .send()
                .await
                .unwrap();
        });

        let bucket3 = bucket.clone();
        let write_b = tokio::spawn(async move {
            CTX.client()
                .put_object()
                .bucket(&bucket3)
                .key("atomic-dual-write")
                .body(ByteStream::from(make_body(b'B', SIZE)))
                .send()
                .await
                .unwrap();
        });

        let (a, b) = tokio::join!(write_a, write_b);
        a.unwrap();
        b.unwrap();

        // Must be entirely 'A' or entirely 'B'
        let body = get_body(&bucket, key).await;
        assert_uniform(&body, SIZE);
        assert!(
            body[0] == b'A' || body[0] == b'B',
            "expected all 'A' or all 'B', got 0x{:02x}",
            body[0],
        );

        cleanup(&bucket, &[key]).await;
    });
}

/// Conditional overwrite with if_match(<etag>) succeeds atomically.
///
/// Matches Ceph: test_atomic_conditional_write_1mb
#[test]
fn test_atomic_conditional_write() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "atomic-cond-write";

        // Write 'A', capture etag
        let resp = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(make_body(b'A', SIZE)))
            .send()
            .await
            .unwrap();
        let etag_a = resp.e_tag().unwrap().to_string();

        // Conditional overwrite with if_match(<etag>) — must succeed
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .if_match(&etag_a)
            .body(ByteStream::from(make_body(b'B', SIZE)))
            .send()
            .await
            .unwrap();

        let body = get_body(&bucket, key).await;
        assert_uniform(&body, SIZE);
        assert_eq!(body[0], b'B');

        cleanup(&bucket, &[key]).await;
    });
}

/// Conditional overwrite with a stale etag fails with 412 PreconditionFailed.
///
/// Matches Ceph: test_atomic_dual_conditional_write_1mb
#[test]
fn test_atomic_dual_conditional_write() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "atomic-dual-cond";

        // Write 'A', capture etag
        let resp = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(make_body(b'A', SIZE)))
            .send()
            .await
            .unwrap();
        let etag_a = resp.e_tag().unwrap().to_string();

        // Unconditional overwrite with 'B' (changes the etag)
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(make_body(b'B', SIZE)))
            .send()
            .await
            .unwrap();

        // Conditional overwrite with stale etag → must fail
        let result = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .if_match(&etag_a)
            .body(ByteStream::from(make_body(b'C', SIZE)))
            .send()
            .await;
        assert_eq!(err_status(&result), 412);

        // Object must still be all 'B'
        let body = get_body(&bucket, key).await;
        assert_uniform(&body, SIZE);
        assert_eq!(body[0], b'B');

        cleanup(&bucket, &[key]).await;
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
        client.create_bucket().bucket(&bucket).send().await.unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();

        // PUT to the gone bucket → 404
        let result = client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from(make_body(b'A', SIZE)))
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
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"bar"))
            .send()
            .await
            .unwrap();

        // Start a multipart upload to the same key
        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        // Original object is still readable during in-progress MPU
        let get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let body = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(&body[..], b"bar");

        // Abort the multipart upload
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await
            .unwrap();

        // Original object still intact after abort
        let get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let body = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(&body[..], b"bar");

        cleanup(&bucket, &[key]).await;
    });
}
