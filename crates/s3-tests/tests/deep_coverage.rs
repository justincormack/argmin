//! Integration tests targeting pg_store.rs code paths with low coverage.
//!
//! These tests exercise segment storage, large object handling, versioned
//! tagging, list-versions pagination, bucket deletion edge cases, multipart
//! abort cleanup, and part re-upload through the full S3 HTTP API.

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, Tag, Tagging,
    VersioningConfiguration,
};
use s3_tests::{
    assert_s3_err_code, cleanup_versioned_bucket, unique_bucket, SendRetryingOperationAborted, CTX,
};

const PART_SIZE: usize = 5 * 1024 * 1024; // 5 MB minimum part size

async fn setup_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    bucket
}

async fn setup_versioned_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    client
        .put_bucket_versioning()
        .bucket(&bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    bucket
}

async fn cleanup(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = s3_tests::delete_object_retrying_operation_aborted(client, bucket, *key).await;
    }
    s3_tests::delete_bucket_retrying_operation_aborted(client, bucket).await;
}

fn tag(key: &str, value: &str) -> Tag {
    Tag::builder().key(key).value(value).build().unwrap()
}

fn tagging(tags: Vec<Tag>) -> Tagging {
    Tagging::builder().set_tag_set(Some(tags)).build().unwrap()
}

// ── 1. Large object round-trip (multi-segment) ────────────────────────

/// PUT an object larger than one internal segment, GET it back, verify
/// byte-for-byte. Exercises streaming PutObject finalization with multiple
/// segments and multi-segment shard reads on GET.
#[test]
fn test_large_object_round_trip_multisegment() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "large-8mb";

        let size = (8 * 1024 * 1024) + 123;
        let data: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();

        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(data.clone()))
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
        assert_eq!(
            get.content_length(),
            Some(size as i64),
            "content-length mismatch"
        );
        let got = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(got.len(), data.len(), "body length mismatch");
        assert_eq!(&got[..], &data[..], "body data mismatch");

        cleanup(&bucket, &[key]).await;
    });
}

// ── 2. Large object overwrite ──────────────────────────────────────────

/// PUT an object larger than one internal segment, overwrite it with different
/// data of the same size, and verify GET returns the new bytes.
/// Exercises segment reclaim for multi-segment overwrites.
#[test]
fn test_large_object_overwrite() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "large-overwrite";

        let size = (8 * 1024 * 1024) + 123;
        let data_v1: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        let data_v2: Vec<u8> = (0..size).map(|i| ((i + 128) % 251) as u8).collect();
        assert_ne!(&data_v1[..32], &data_v2[..32], "test data should differ");

        // Write v1.
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(data_v1))
            .send()
            .await
            .unwrap();

        // Overwrite with v2.
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(data_v2.clone()))
            .send()
            .await
            .unwrap();

        // GET should return v2.
        let get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let got = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(got.len(), data_v2.len(), "body length mismatch");
        assert_eq!(&got[..], &data_v2[..], "body should be v2 data");

        cleanup(&bucket, &[key]).await;
    });
}

// ── 3. Large multipart parts (multi-segment per part) ──────────────────

/// Multipart upload with parts larger than one internal segment. Each part spans
/// multiple internal segments, exercising commit_stream_part with multiple
/// segments per part.
#[test]
fn test_multipart_large_parts() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "mpu-large-parts";

        let part_size = (8 * 1024 * 1024) + 123;
        let part1: Vec<u8> = (0..part_size).map(|i| (i % 251) as u8).collect();
        let part2: Vec<u8> = (0..part_size).map(|i| (i % 239) as u8).collect();

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let resp1 = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from(part1.clone()))
            .send()
            .await
            .unwrap();
        let resp2 = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(2)
            .body(ByteStream::from(part2.clone()))
            .send()
            .await
            .unwrap();

        let completed = CompletedMultipartUpload::builder()
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
            .build();

        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(completed)
            .send()
            .await
            .unwrap();

        // Read back full object.
        let get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let got = get.body.collect().await.unwrap().into_bytes();
        let expected_size = part1.len() + part2.len();
        assert_eq!(got.len(), expected_size, "body length mismatch");
        assert_eq!(&got[..part1.len()], &part1[..], "part 1 data mismatch");
        assert_eq!(&got[part1.len()..], &part2[..], "part 2 data mismatch");

        cleanup(&bucket, &[key]).await;
    });
}

// ── 4. Versioned object tags ───────────────────────────────────────────

/// Enable versioning, create two versions, tag version 1 only, verify
/// version 2 has no tags, delete version 1, verify its tags are gone.
#[test]
fn test_versioned_object_tags() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "tagged-versions";

        // Create version 1.
        let v1 = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"v1"))
            .send()
            .await
            .unwrap();
        let vid1 = v1.version_id().unwrap().to_string();

        // Create version 2.
        let v2 = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"v2"))
            .send()
            .await
            .unwrap();
        let vid2 = v2.version_id().unwrap().to_string();

        // Tag version 1 only (non-current version).
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .version_id(&vid1)
            .tagging(tagging(vec![tag("env", "prod")]))
            .send()
            .await
            .unwrap();

        // Version 1 has tags.
        let tags1 = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(key)
            .version_id(&vid1)
            .send()
            .await
            .unwrap();
        assert_eq!(tags1.tag_set().len(), 1);
        assert_eq!(tags1.tag_set()[0].key(), "env");

        // Version 2 has no tags.
        let tags2 = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(key)
            .version_id(&vid2)
            .send()
            .await
            .unwrap();
        assert!(
            tags2.tag_set().is_empty(),
            "version 2 should have no tags, got {:?}",
            tags2.tag_set()
        );

        // Delete version 1 — its tags should also be gone.
        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&vid1)
            .send()
            .await
            .unwrap();

        // Getting tags for deleted version should fail (NoSuchVersion).
        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(key)
            .version_id(&vid1)
            .send()
            .await;
        assert_s3_err_code(&result, "NoSuchVersion");

        // Version 2 still accessible.
        let get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&vid2)
            .send()
            .await
            .unwrap();
        let body = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(&body[..], b"v2");

        cleanup_versioned_bucket(CTX.client(), &bucket).await;
    });
}

// ── 5. Delete marker tag rejection ─────────────────────────────────────

/// PutObjectTagging on a delete marker should return 405
/// MethodNotAllowed.
#[test]
fn test_delete_marker_tag_rejection() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "delete-marker-tags";

        // Create an object, then delete it to create a delete marker.
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let del = client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let dm_vid = del.version_id().unwrap().to_string();

        // Attempt to tag the delete marker.
        let result = client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .version_id(&dm_vid)
            .tagging(tagging(vec![tag("env", "prod")]))
            .send()
            .await;
        assert_s3_err_code(&result, "MethodNotAllowed");

        cleanup_versioned_bucket(CTX.client(), &bucket).await;
    });
}

// ── 6. ListObjectVersions pagination ───────────────────────────────────

/// Multiple keys with multiple versions each, paginate using both
/// key-marker and version-id-marker. Exercises the SQL branch
/// (key > ? OR (key = ? AND version_id < ?)).
#[test]
fn test_list_object_versions_full_pagination() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;

        // Create 3 keys, each with 3 versions = 9 total version entries.
        for key in ["a", "b", "c"] {
            for body in [b"v1" as &[u8], b"v2", b"v3"] {
                client
                    .put_object()
                    .bucket(&bucket)
                    .key(key)
                    .body(ByteStream::from_static(body))
                    .send()
                    .await
                    .unwrap();
            }
        }

        // Paginate with max_keys=2 and collect all versions.
        let mut all_entries = Vec::new();
        let mut key_marker: Option<String> = None;
        let mut vid_marker: Option<String> = None;
        let mut pages = 0;

        loop {
            let mut req = client.list_object_versions().bucket(&bucket).max_keys(2);

            if let Some(ref km) = key_marker {
                req = req.key_marker(km);
            }
            if let Some(ref vm) = vid_marker {
                req = req.version_id_marker(vm);
            }

            let resp = req
                .send_retrying_operation_aborted(
                    "list object versions during deep coverage pagination",
                )
                .await
                .unwrap();
            pages += 1;

            for v in resp.versions() {
                all_entries.push((
                    v.key().unwrap().to_string(),
                    v.version_id().unwrap().to_string(),
                ));
            }

            if resp.is_truncated() != Some(true) {
                break;
            }
            key_marker = resp.next_key_marker().map(|s| s.to_string());
            vid_marker = resp.next_version_id_marker().map(|s| s.to_string());

            // Safety: don't loop forever.
            assert!(pages < 20, "too many pages");
        }

        assert_eq!(
            all_entries.len(),
            9,
            "expected 9 versions, got {}",
            all_entries.len()
        );
        assert!(
            pages >= 5,
            "expected at least 5 pages with max_keys=2 for 9 entries, got {pages}"
        );

        // Verify all keys are present.
        let a_count = all_entries.iter().filter(|(k, _)| k == "a").count();
        let b_count = all_entries.iter().filter(|(k, _)| k == "b").count();
        let c_count = all_entries.iter().filter(|(k, _)| k == "c").count();
        assert_eq!(a_count, 3);
        assert_eq!(b_count, 3);
        assert_eq!(c_count, 3);

        // No duplicate (key, version_id) pairs.
        let mut pairs: Vec<(String, String)> = all_entries.clone();
        pairs.sort();
        let before = pairs.len();
        pairs.dedup();
        assert_eq!(
            pairs.len(),
            before,
            "duplicate (key, version_id) pairs found"
        );

        cleanup_versioned_bucket(CTX.client(), &bucket).await;
    });
}

// ── 7. Delete non-empty versioned bucket ───────────────────────────────

/// Attempt to delete a bucket that still contains versioned objects and
/// delete markers. Should fail with BucketNotEmpty.
#[test]
fn test_delete_nonempty_versioned_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;

        // Create an object and a delete marker.
        client
            .put_object()
            .bucket(&bucket)
            .key("still-here")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();
        // Simple delete creates a delete marker but doesn't remove the version.
        client
            .delete_object()
            .bucket(&bucket)
            .key("still-here")
            .send_retrying_operation_aborted("create deep coverage delete marker")
            .await
            .unwrap();

        // Bucket has a live version + a delete marker — not empty.
        let result = client
            .delete_bucket()
            .bucket(&bucket)
            .send_retrying_operation_aborted("delete nonempty deep coverage bucket")
            .await;
        assert_s3_err_code(&result, "BucketNotEmpty");

        // Clean up properly.
        cleanup_versioned_bucket(CTX.client(), &bucket).await;
    });
}

// ── 8. Multipart abort with parts cleanup ──────────────────────────────

/// Create MPU, upload several parts, abort, verify ListParts shows empty
/// for that upload, and a fresh upload to the same key completes cleanly.
#[test]
fn test_multipart_abort_parts_cleanup() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "mpu-abort-parts";

        // Create and populate.
        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        for pn in 1..=3 {
            client
                .upload_part()
                .bucket(&bucket)
                .key(key)
                .upload_id(&upload_id)
                .part_number(pn)
                .body(ByteStream::from(vec![pn as u8; PART_SIZE]))
                .send()
                .await
                .unwrap();
        }

        // Verify parts are listed.
        let parts = client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();
        assert_eq!(parts.parts().len(), 3);

        // Abort.
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();

        // ListParts for aborted upload should fail.
        let result = client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await;
        assert!(result.is_err(), "ListParts should fail for aborted upload");

        // ListMultipartUploads should be empty.
        let uploads = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert!(
            uploads.uploads().is_empty(),
            "no uploads should remain after abort"
        );

        // A fresh upload to the same key should work.
        let create2 = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let uid2 = create2.upload_id().unwrap().to_string();

        let r = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&uid2)
            .part_number(1)
            .body(ByteStream::from(vec![0x42u8; PART_SIZE]))
            .send()
            .await
            .unwrap();

        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&uid2)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(r.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .unwrap();

        // Verify the object.
        let get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let got = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(got.len(), PART_SIZE);

        cleanup(&bucket, &[key]).await;
    });
}

// ── 9. Part re-upload (generation overwrite) ───────────────────────────

/// Upload part 1 twice (re-upload), then complete. The completed object
/// should contain only the latest part data. Exercises generation
/// overwrite in upsert_multipart_part.
#[test]
fn test_multipart_part_reupload() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "mpu-reupload";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        // Upload part 1 with pattern A.
        let data_a: Vec<u8> = vec![0xAA; PART_SIZE];
        client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from(data_a))
            .send()
            .await
            .unwrap();

        // Re-upload part 1 with pattern B.
        let data_b: Vec<u8> = vec![0xBB; PART_SIZE];
        let resp_b = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from(data_b.clone()))
            .send()
            .await
            .unwrap();

        // Complete using the ETag from the second upload.
        let completed = CompletedMultipartUpload::builder()
            .parts(
                CompletedPart::builder()
                    .e_tag(resp_b.e_tag().unwrap())
                    .part_number(1)
                    .build(),
            )
            .build();

        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(completed)
            .send()
            .await
            .unwrap();

        // Verify the object contains pattern B, not A.
        let get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let got = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(got.len(), PART_SIZE, "body length mismatch");
        assert!(
            got.iter().all(|&b| b == 0xBB),
            "body should be pattern B (0xBB), first byte is {:#04x}",
            got[0]
        );

        cleanup(&bucket, &[key]).await;
    });
}

// ── 10. ListObjectVersions with key-marker only (no version-id-marker) ──

/// Paginate ListObjectVersions using only key-marker (no version-id-marker).
/// Exercises the SQL branch `key > ?` instead of `(key > ? OR (key = ? AND version_id < ?))`.
#[test]
fn test_list_object_versions_key_marker_only() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;

        // Create 3 keys with 2 versions each = 6 total entries.
        for key in ["alpha", "beta", "gamma"] {
            for body in [b"v1" as &[u8], b"v2"] {
                client
                    .put_object()
                    .bucket(&bucket)
                    .key(key)
                    .body(ByteStream::from_static(body))
                    .send()
                    .await
                    .unwrap();
            }
        }

        // Page 1: get first 2 entries.
        let resp1 = client
            .list_object_versions()
            .bucket(&bucket)
            .max_keys(2)
            .send()
            .await
            .unwrap();
        assert_eq!(resp1.versions().len(), 2);
        assert_eq!(resp1.is_truncated(), Some(true));

        // Page 2: use only key-marker (no version-id-marker).
        // This should skip past the key-marker and return subsequent entries.
        let key_marker = resp1.next_key_marker().unwrap().to_string();
        let resp2 = client
            .list_object_versions()
            .bucket(&bucket)
            .max_keys(2)
            .key_marker(&key_marker)
            .send()
            .await
            .unwrap();
        assert_eq!(resp2.versions().len(), 2);

        // Entries from page 2 should all have keys > key_marker.
        for v in resp2.versions() {
            assert!(
                v.key().unwrap() > key_marker.as_str(),
                "expected key > {key_marker}, got {}",
                v.key().unwrap()
            );
        }

        cleanup_versioned_bucket(CTX.client(), &bucket).await;
    });
}

// ── 11. ListMultipartUploads with key-marker only (no upload-id-marker) ─

/// Paginate ListMultipartUploads using only key-marker (no upload-id-marker).
/// Exercises the SQL branch `key > ?` instead of the compound cursor branch.
#[test]
fn test_list_multipart_uploads_key_marker_only() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        // Create 4 multipart uploads on different keys.
        let mut upload_ids = Vec::new();
        for key in ["mpu-a", "mpu-b", "mpu-c", "mpu-d"] {
            let create = client
                .create_multipart_upload()
                .bucket(&bucket)
                .key(key)
                .send()
                .await
                .unwrap();
            upload_ids.push((key.to_string(), create.upload_id().unwrap().to_string()));
        }

        // Page 1: get first 2 uploads.
        let resp1 = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .max_uploads(2)
            .send()
            .await
            .unwrap();
        assert_eq!(resp1.uploads().len(), 2);
        assert_eq!(resp1.is_truncated(), Some(true));

        // Page 2: use only key-marker (no upload-id-marker).
        let key_marker = resp1.next_key_marker().unwrap().to_string();
        let resp2 = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .max_uploads(2)
            .key_marker(&key_marker)
            .send()
            .await
            .unwrap();
        assert_eq!(resp2.uploads().len(), 2);

        // Entries from page 2 should all have keys > key_marker.
        for u in resp2.uploads() {
            assert!(
                u.key().unwrap() > key_marker.as_str(),
                "expected key > {key_marker}, got {}",
                u.key().unwrap()
            );
        }

        // Cleanup: abort all uploads.
        for (key, uid) in &upload_ids {
            client
                .abort_multipart_upload()
                .bucket(&bucket)
                .key(key)
                .upload_id(uid)
                .send()
                .await
                .unwrap();
        }
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── 12. PutObjectTagging on nonexistent key ─────────────────────────────

/// PutObjectTagging on a key that doesn't exist at all should return NoSuchKey.
/// (Distinct from delete marker, which returns MethodNotAllowed.)
#[test]
fn test_put_tagging_nonexistent_key() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let result = client
            .put_object_tagging()
            .bucket(&bucket)
            .key("does-not-exist")
            .tagging(tagging(vec![tag("env", "test")]))
            .send()
            .await;
        assert_s3_err_code(&result, "NoSuchKey");

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── 13. DeleteObjectTagging on nonexistent key ──────────────────────────

/// DeleteObjectTagging on a key that doesn't exist should return NoSuchKey.
#[test]
fn test_delete_tagging_nonexistent_key() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let result = client
            .delete_object_tagging()
            .bucket(&bucket)
            .key("does-not-exist")
            .send()
            .await;
        assert_s3_err_code(&result, "NoSuchKey");

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── 14. Versioned streaming PutObject (large object on versioned bucket) ─

/// PUT an 8 MiB object on a versioned bucket. Exercises streaming PutObject
/// finalization on versioned buckets rather than the unversioned overwrite
/// path.
#[test]
fn test_large_put_versioned_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "large-versioned";

        let size = 8 * 1024 * 1024;
        let data_v1: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        let data_v2: Vec<u8> = (0..size).map(|i| ((i + 128) % 251) as u8).collect();

        // Write v1.
        let r1 = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(data_v1.clone()))
            .send()
            .await
            .unwrap();
        let vid1 = r1.version_id().unwrap().to_string();

        // Write v2.
        let r2 = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(data_v2.clone()))
            .send()
            .await
            .unwrap();
        let vid2 = r2.version_id().unwrap().to_string();
        assert_ne!(vid1, vid2, "versions should differ");

        // GET latest should return v2.
        let get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let got = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(got.len(), data_v2.len());
        assert_eq!(&got[..], &data_v2[..]);

        // GET v1 by version-id should return v1.
        let get_v1 = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&vid1)
            .send()
            .await
            .unwrap();
        let got_v1 = get_v1.body.collect().await.unwrap().into_bytes();
        assert_eq!(got_v1.len(), data_v1.len());
        assert_eq!(&got_v1[..], &data_v1[..]);

        cleanup_versioned_bucket(CTX.client(), &bucket).await;
    });
}

// ── 15. Versioned multipart CompleteMultipartUpload ──────────────────────

/// Complete a multipart upload on a versioned bucket. Exercises the versioned
/// SQL branch in complete_multipart_commit (`INSERT INTO` vs `INSERT OR REPLACE`).
#[test]
fn test_multipart_complete_versioned_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "mpu-versioned";

        // First: put a simple object to create version 1.
        let r1 = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"simple-v1"))
            .send()
            .await
            .unwrap();
        let vid1 = r1.version_id().unwrap().to_string();

        // Second: multipart upload to create version 2.
        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let part_data: Vec<u8> = vec![0xCC; PART_SIZE];
        let resp = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from(part_data.clone()))
            .send()
            .await
            .unwrap();

        let complete_resp = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
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
        let vid2 = complete_resp.version_id().unwrap().to_string();
        assert_ne!(vid1, vid2, "MPU should create a new version");

        // GET latest should return multipart data.
        let get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let got = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(got.len(), PART_SIZE);
        assert!(got.iter().all(|&b| b == 0xCC));

        // GET v1 should return simple data.
        let get_v1 = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&vid1)
            .send()
            .await
            .unwrap();
        let got_v1 = get_v1.body.collect().await.unwrap().into_bytes();
        assert_eq!(&got_v1[..], b"simple-v1");

        cleanup_versioned_bucket(CTX.client(), &bucket).await;
    });
}
