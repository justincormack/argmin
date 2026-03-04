//! Multipart upload integration tests.
//!
//! Tests the full multipart upload lifecycle through the S3 HTTP API:
//! CreateMultipartUpload, UploadPart, CompleteMultipartUpload,
//! AbortMultipartUpload, ListMultipartUploads, ListParts.

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::CompletedMultipartUpload;
use aws_sdk_s3::types::CompletedPart;
use s3_tests::{assert_s3_err_code, unique_bucket, CTX};

const PART_SIZE: usize = 5 * 1024 * 1024; // 5 MB minimum part size

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

/// Helper: create multipart upload, upload parts, complete, return (etag, version_id).
async fn do_multipart_upload(
    bucket: &str,
    key: &str,
    parts_data: &[Vec<u8>],
) -> String {
    let client = CTX.client();

    let create = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    let upload_id = create.upload_id().unwrap();

    let mut completed_parts = Vec::new();
    for (i, data) in parts_data.iter().enumerate() {
        let part_number = (i + 1) as i32;
        let resp = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(ByteStream::from(data.clone()))
            .send()
            .await
            .unwrap();
        completed_parts.push(
            CompletedPart::builder()
                .e_tag(resp.e_tag().unwrap())
                .part_number(part_number)
                .build(),
        );
    }

    let complete = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(completed_parts))
                .build(),
        )
        .send()
        .await
        .unwrap();
    complete.e_tag().unwrap().to_string()
}

// ── Basic lifecycle ─────────────────────────────────────────────────

#[test]
fn test_multipart_upload_basic() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-basic";

        let part1 = vec![b'a'; PART_SIZE];
        let part2 = vec![b'b'; 1024]; // last part can be < 5MB

        let etag = do_multipart_upload(&bucket, key, &[part1.clone(), part2.clone()]).await;
        assert!(!etag.is_empty());

        // Verify the object is readable and has correct content
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(data.len(), PART_SIZE + 1024);
        assert!(data[..PART_SIZE].iter().all(|&b| b == b'a'));
        assert!(data[PART_SIZE..].iter().all(|&b| b == b'b'));

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_multipart_upload_single_part() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-single";

        // Single part (last part exempt from min size)
        let part = vec![b'x'; 256];
        do_multipart_upload(&bucket, key, &[part.clone()]).await;

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], &part[..]);

        cleanup(&bucket, &[key]).await;
    });
}

// ── Abort ───────────────────────────────────────────────────────────

#[test]
fn test_multipart_upload_abort() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-abort";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        // Upload a part
        client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![0u8; 1024]))
            .send()
            .await
            .unwrap();

        // Abort the upload
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await
            .unwrap();

        // The object should not exist
        let result = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await;
        assert!(result.is_err());

        cleanup(&bucket, &[]).await;
    });
}

// ── ListMultipartUploads ────────────────────────────────────────────

#[test]
fn test_list_multipart_uploads_empty() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let resp = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert!(resp.uploads().is_empty());

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_list_multipart_uploads_active() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        // Create two uploads
        let create1 = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("key1")
            .send()
            .await
            .unwrap();
        let uid1 = create1.upload_id().unwrap().to_string();

        let create2 = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("key2")
            .send()
            .await
            .unwrap();
        let uid2 = create2.upload_id().unwrap().to_string();

        let resp = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let uploads = resp.uploads();
        assert_eq!(uploads.len(), 2);

        let keys: Vec<&str> = uploads.iter().map(|u| u.key().unwrap()).collect();
        assert!(keys.contains(&"key1"));
        assert!(keys.contains(&"key2"));

        // Abort both
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("key1")
            .upload_id(&uid1)
            .send()
            .await
            .unwrap();
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("key2")
            .upload_id(&uid2)
            .send()
            .await
            .unwrap();

        // Should be empty now
        let resp = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert!(resp.uploads().is_empty());

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_list_multipart_uploads_prefix() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let c1 = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("photos/a.jpg")
            .send()
            .await
            .unwrap();
        let uid1 = c1.upload_id().unwrap().to_string();

        let c2 = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("docs/b.txt")
            .send()
            .await
            .unwrap();
        let uid2 = c2.upload_id().unwrap().to_string();

        let resp = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .prefix("photos/")
            .send()
            .await
            .unwrap();
        let uploads = resp.uploads();
        assert_eq!(uploads.len(), 1);
        assert_eq!(uploads[0].key().unwrap(), "photos/a.jpg");

        // Cleanup
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("photos/a.jpg")
            .upload_id(&uid1)
            .send()
            .await
            .unwrap();
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("docs/b.txt")
            .upload_id(&uid2)
            .send()
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

// ── ListParts ───────────────────────────────────────────────────────

#[test]
fn test_list_parts() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "list-parts-key";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        // Upload two parts
        client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![b'a'; PART_SIZE]))
            .send()
            .await
            .unwrap();

        client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(2)
            .body(ByteStream::from(vec![b'b'; 1024]))
            .send()
            .await
            .unwrap();

        let resp = client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await
            .unwrap();
        let parts = resp.parts();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].part_number().unwrap(), 1);
        assert_eq!(parts[0].size().unwrap(), PART_SIZE as i64);
        assert_eq!(parts[1].part_number().unwrap(), 2);
        assert_eq!(parts[1].size().unwrap(), 1024);

        // Abort to clean up
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

// ── Error cases ─────────────────────────────────────────────────────

#[test]
fn test_complete_multipart_no_such_upload() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let result = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key("nokey")
            .upload_id("nonexistent-upload-id")
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag("\"abc\"")
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await;
        assert!(result.is_err());

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_abort_multipart_no_such_upload() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let result = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("nokey")
            .upload_id("nonexistent-upload-id")
            .send()
            .await;
        // Our server returns NoSuchUpload for nonexistent upload IDs.
        s3_tests::assert_s3_err_code(&result, "NoSuchUpload");

        cleanup(&bucket, &[]).await;
    });
}

// ── Part size validation ────────────────────────────────────────────

#[test]
fn test_multipart_part_too_small() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "part-too-small";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        // Upload two parts, first one too small (< 5MB)
        let resp1 = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![0u8; 100]))
            .send()
            .await
            .unwrap();

        let resp2 = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(2)
            .body(ByteStream::from(vec![0u8; 100]))
            .send()
            .await
            .unwrap();

        // CompleteMultipartUpload should fail with EntityTooSmall
        let result = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(resp1.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .parts(
                        CompletedPart::builder()
                            .e_tag(resp2.e_tag().unwrap())
                            .part_number(2)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await;
        assert!(result.is_err());

        // Abort to clean up
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

// ── Overwrite existing object ───────────────────────────────────────

#[test]
fn test_multipart_overwrites_existing_object() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "overwrite-me";

        // Put a regular object first
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"original"))
            .send()
            .await
            .unwrap();

        // Overwrite with multipart
        let new_data = vec![b'z'; 512];
        do_multipart_upload(&bucket, key, &[new_data.clone()]).await;

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], &new_data[..]);

        cleanup(&bucket, &[key]).await;
    });
}

// ── HeadObject on multipart object ──────────────────────────────────

#[test]
fn test_multipart_head_object() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-head";

        let part1 = vec![b'h'; PART_SIZE];
        let part2 = vec![b'i'; 2048];
        do_multipart_upload(&bucket, key, &[part1, part2]).await;

        let resp = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_length().unwrap(), (PART_SIZE + 2048) as i64);

        cleanup(&bucket, &[key]).await;
    });
}

// ── Range read on multipart object ──────────────────────────────────

#[test]
fn test_multipart_range_read() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-range";

        let part1 = vec![b'A'; PART_SIZE];
        let part2 = vec![b'B'; 2048];
        do_multipart_upload(&bucket, key, &[part1, part2]).await;

        // Read across the part boundary
        let start = PART_SIZE - 10;
        let end = PART_SIZE + 9;
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .range(format!("bytes={}-{}", start, end))
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(data.len(), 20);
        assert!(data[..10].iter().all(|&b| b == b'A'));
        assert!(data[10..].iter().all(|&b| b == b'B'));

        cleanup(&bucket, &[key]).await;
    });
}

// ── Multiple concurrent uploads for same key ────────────────────────

#[test]
fn test_multipart_concurrent_uploads_same_key() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "concurrent";

        // Start two uploads for the same key
        let c1 = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let uid1 = c1.upload_id().unwrap().to_string();

        let c2 = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let uid2 = c2.upload_id().unwrap().to_string();
        assert_ne!(uid1, uid2);

        // Both should appear in listing
        let resp = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.uploads().len(), 2);

        // Complete the first, abort the second
        let r1 = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&uid1)
            .part_number(1)
            .body(ByteStream::from(vec![b'1'; 100]))
            .send()
            .await
            .unwrap();

        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&uid1)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(r1.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .unwrap();

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&uid2)
            .send()
            .await
            .unwrap();

        // Only the completed upload's object should exist
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(data.len(), 100);
        assert!(data.iter().all(|&b| b == b'1'));

        cleanup(&bucket, &[key]).await;
    });
}

// ── Part overwrite (re-upload same part number) ─────────────────────

#[test]
fn test_multipart_part_overwrite() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "part-overwrite";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        // Upload part 1 with data 'a'
        client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![b'a'; 256]))
            .send()
            .await
            .unwrap();

        // Re-upload part 1 with data 'b' — should replace
        let resp = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![b'b'; 512]))
            .send()
            .await
            .unwrap();

        // Complete with the second upload's ETag
        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(resp.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .unwrap();

        let get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let data = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(data.len(), 512);
        assert!(data.iter().all(|&b| b == b'b'));

        cleanup(&bucket, &[key]).await;
    });
}

// ── Completion validation (Ceph parity) ─────────────────────────────

/// Ceph: test_multipart_upload_empty — completing with no parts should fail.
#[test]
fn test_multipart_complete_empty_parts() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "empty-complete";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let result = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(CompletedMultipartUpload::builder().build())
            .send()
            .await;
        assert!(result.is_err());

        // Abort to clean up
        let _ = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await;
        cleanup(&bucket, &[]).await;
    });
}

/// Ceph: test_multipart_upload_incorrect_etag — wrong ETag should fail with InvalidPart.
#[test]
fn test_multipart_complete_incorrect_etag() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "wrong-etag";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![0u8; 256]))
            .send()
            .await
            .unwrap();

        // Complete with a fabricated ETag
        let result = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag("\"ffffffffffffffff\"")
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await;
        assert_s3_err_code(&result, "InvalidPart");

        let _ = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await;
        cleanup(&bucket, &[]).await;
    });
}

/// Ceph: test_multipart_upload_missing_part — referencing an unuploaded part number.
#[test]
fn test_multipart_complete_missing_part() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "missing-part";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        // Upload part 1
        let resp = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![0u8; 256]))
            .send()
            .await
            .unwrap();

        // Complete referencing part 9999 (never uploaded)
        let result = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(resp.e_tag().unwrap())
                            .part_number(9999)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await;
        assert_s3_err_code(&result, "InvalidPart");

        let _ = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await;
        cleanup(&bucket, &[]).await;
    });
}

/// Ceph: test_multipart_upload — metadata and content-type survive multipart.
#[test]
fn test_multipart_metadata_preserved() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "meta-preserved";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .content_type("application/octet-stream")
            .metadata("testkey", "testvalue")
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let resp = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![b'm'; 128]))
            .send()
            .await
            .unwrap();

        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(resp.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .unwrap();

        let head = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(head.content_type().unwrap(), "application/octet-stream");
        assert_eq!(
            head.metadata().unwrap().get("testkey").unwrap(),
            "testvalue"
        );

        cleanup(&bucket, &[key]).await;
    });
}

/// Parts must be in strictly ascending order.
#[test]
fn test_multipart_complete_invalid_order() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "invalid-order";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let r1 = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![b'a'; PART_SIZE]))
            .send()
            .await
            .unwrap();

        let r2 = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(2)
            .body(ByteStream::from(vec![b'b'; 256]))
            .send()
            .await
            .unwrap();

        // Complete with parts in reverse order (2, 1)
        let result = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(r2.e_tag().unwrap())
                            .part_number(2)
                            .build(),
                    )
                    .parts(
                        CompletedPart::builder()
                            .e_tag(r1.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await;
        assert_s3_err_code(&result, "InvalidPartOrder");

        let _ = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await;
        cleanup(&bucket, &[]).await;
    });
}

/// Multipart ETag is a composite format: "hex-N".
#[test]
fn test_multipart_composite_etag() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "composite-etag";

        let part_data = vec![b'e'; 256];
        let etag = do_multipart_upload(&bucket, key, &[part_data]).await;
        // Multipart ETags have the format "hex-N" where N is part count
        assert!(etag.contains("-1"), "expected composite ETag with -1 suffix, got: {etag}");

        cleanup(&bucket, &[key]).await;
    });
}

// ── Ceph parity: resend part ────────────────────────────────────────

/// Re-uploading a part before completion replaces the previous upload.
///
/// Matches Ceph: test_multipart_upload_resend_part
#[test]
fn test_multipart_upload_resend_part() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "resend-part";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        // Upload part 1 with data 'A'
        let data_a = vec![b'A'; PART_SIZE];
        let _resp_a = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(data_a))
            .send()
            .await
            .unwrap();

        // Re-upload part 1 with data 'B' (replaces the first upload)
        let data_b = vec![b'B'; PART_SIZE];
        let resp_b = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(data_b.clone()))
            .send()
            .await
            .unwrap();

        // Complete with the second ETag
        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(resp_b.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .unwrap();

        // Verify the content is from the second upload
        let get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let body = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.len(), PART_SIZE);
        assert!(body.iter().all(|&b| b == b'B'));

        cleanup(&bucket, &[key]).await;
    });
}

// ── Ceph parity: multiple sizes ─────────────────────────────────────

/// Multipart upload with various total sizes, covering all boundary
/// variants from the Ceph test.
///
/// Matches Ceph: test_multipart_upload_multiple_sizes
#[test]
fn test_multipart_upload_multiple_sizes() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multi-sizes";
        let mb = 1024 * 1024;
        let kb = 1024;

        // Helper: upload with given total size split into 5MB parts + remainder
        async fn upload_and_check(
            client: &aws_sdk_s3::Client,
            bucket: &str,
            key: &str,
            total: usize,
        ) {
            let part_size = 5 * 1024 * 1024;
            let mut parts = Vec::new();
            let mut remaining = total;
            while remaining > 0 {
                let sz = remaining.min(part_size);
                parts.push(vec![b'x'; sz]);
                remaining -= sz;
            }
            do_multipart_upload(bucket, key, &parts).await;
            let head = client
                .head_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await
                .unwrap();
            assert_eq!(
                head.content_length(),
                Some(total as i64),
                "size mismatch for {total} byte upload"
            );
        }

        // Ceph sizes: 5MB, 5MB+100KB, 5MB+600KB, 10MB+100KB, 10MB+600KB, 10MB
        for size in [
            5 * mb,
            5 * mb + 100 * kb,
            5 * mb + 600 * kb,
            10 * mb + 100 * kb,
            10 * mb + 600 * kb,
            10 * mb,
        ] {
            upload_and_check(client, &bucket, key, size).await;
        }

        cleanup(&bucket, &[key]).await;
    });
}

// ── PartNumber GET semantics ────────────────────────────────────────

#[test]
#[ignore = "not implemented: PartNumber GET query parameter"]
fn test_multipart_get_part() {
    s3_tests::run(async {});
}

#[test]
#[ignore = "not implemented: PartNumber GET query parameter"]
fn test_non_multipart_get_part() {
    s3_tests::run(async {});
}

// ── UploadPartCopy (not implemented) ────────────────────────────────

#[test]
#[ignore = "not implemented: UploadPartCopy"]
fn test_multipart_copy_small() {
    s3_tests::run(async {});
}

#[test]
#[ignore = "not implemented: UploadPartCopy"]
fn test_multipart_copy_without_range() {
    s3_tests::run(async {});
}

#[test]
#[ignore = "not implemented: UploadPartCopy"]
fn test_multipart_copy_invalid_range() {
    s3_tests::run(async {});
}

#[test]
#[ignore = "not implemented: UploadPartCopy"]
fn test_multipart_copy_improper_range() {
    s3_tests::run(async {});
}

#[test]
#[ignore = "not implemented: UploadPartCopy"]
fn test_multipart_copy_special_names() {
    s3_tests::run(async {});
}

#[test]
#[ignore = "not implemented: UploadPartCopy"]
fn test_multipart_copy_versioned() {
    s3_tests::run(async {});
}

#[test]
#[ignore = "not implemented: UploadPartCopy"]
fn test_multipart_copy_multiple_sizes() {
    s3_tests::run(async {});
}

#[test]
#[ignore = "not implemented: UploadPartCopy"]
fn test_upload_part_copy_percent_encoded_key() {
    s3_tests::run(async {});
}

// ── Multi-user (not implemented) ────────────────────────────────────

#[test]
#[ignore = "not implemented: multi-user"]
fn test_list_multipart_upload_owner() {
    s3_tests::run(async {});
}
