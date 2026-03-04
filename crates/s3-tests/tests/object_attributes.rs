use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, ChecksumAlgorithm, CompletedMultipartUpload, CompletedPart,
    ObjectAttributes, StorageClass, VersioningConfiguration,
};
use s3_tests::{unique_bucket, CTX};

const PART_SIZE: usize = 5 * 1024 * 1024; // 5 MB minimum part size

/// Cleanup helper: delete all given keys then the bucket.
async fn cleanup(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = client.delete_object().bucket(bucket).key(*key).send().await;
    }
    client.delete_bucket().bucket(bucket).send().await.unwrap();
}

/// Cleanup helper for versioned buckets: delete each specific version then
/// delete the bucket.
async fn cleanup_versioned(bucket: &str, key: &str, version_ids: &[String]) {
    let client = CTX.client();
    for version_id in version_ids {
        let _ = client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .version_id(version_id)
            .send()
            .await;
    }
    client.delete_bucket().bucket(bucket).send().await.unwrap();
}

// ── test_get_object_attributes ──────────────────────────────────────

/// Basic GetObjectAttributes: put a small object, request all attributes,
/// verify ETag, ObjectSize, StorageClass. ObjectParts and VersionId absent.
#[test]
fn test_get_object_attributes() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let put_resp = client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(aws_sdk_s3::primitives::ByteStream::from_static(b"foo"))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object_attributes()
            .bucket(&bucket)
            .key("obj")
            .object_attributes(ObjectAttributes::Etag)
            .object_attributes(ObjectAttributes::Checksum)
            .object_attributes(ObjectAttributes::ObjectParts)
            .object_attributes(ObjectAttributes::StorageClass)
            .object_attributes(ObjectAttributes::ObjectSize)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.object_size(), Some(3));
        // ETag should match the put response (unquoted in the attributes response)
        let put_etag = put_resp.e_tag().unwrap().trim_matches('"');
        let got_etag = resp.e_tag().unwrap().trim_matches('"');
        assert_eq!(got_etag, put_etag);
        assert_eq!(resp.storage_class().unwrap(), &StorageClass::Standard);
        // No delete marker or version ID for unversioned bucket
        assert!(resp.delete_marker().is_none());
        assert!(resp.version_id().is_none());
        // No ObjectParts for non-multipart object
        assert!(resp.object_parts().is_none());

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── test_get_versioned_object_attributes ─────────────────────────────

/// Enable versioning, put two versions of same key, verify VersionId is
/// returned and we can fetch a specific old version.
#[test]
fn test_get_versioned_object_attributes() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        // Enable versioning
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

        // Put version 1
        let put1_resp = client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(aws_sdk_s3::primitives::ByteStream::from_static(b"foo"))
            .send()
            .await
            .unwrap();
        let v1_etag = put1_resp.e_tag().unwrap().trim_matches('"');
        let v1_id = put1_resp.version_id().unwrap().to_string();

        // GetObjectAttributes without versionId should return current object
        let resp = client
            .get_object_attributes()
            .bucket(&bucket)
            .key("obj")
            .object_attributes(ObjectAttributes::ObjectSize)
            .object_attributes(ObjectAttributes::Etag)
            .object_attributes(ObjectAttributes::Checksum)
            .object_attributes(ObjectAttributes::ObjectParts)
            .object_attributes(ObjectAttributes::StorageClass)
            .send()
            .await
            .unwrap();

        assert!(resp.delete_marker().is_none());
        assert_eq!(resp.version_id(), Some(v1_id.as_str()));
        assert_eq!(resp.object_size(), Some(3));
        assert_eq!(resp.e_tag().unwrap().trim_matches('"'), v1_etag);
        assert_eq!(resp.storage_class(), Some(&StorageClass::Standard));
        assert!(resp.object_parts().is_none());

        // Put a new current version
        let put2_resp = client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(aws_sdk_s3::primitives::ByteStream::from_static(b"foo"))
            .send()
            .await
            .unwrap();
        let v2_id = put2_resp.version_id().unwrap().to_string();

        // Ask for the original version again
        let old_version = client
            .get_object_attributes()
            .bucket(&bucket)
            .key("obj")
            .version_id(&v1_id)
            .object_attributes(ObjectAttributes::ObjectSize)
            .object_attributes(ObjectAttributes::Etag)
            .object_attributes(ObjectAttributes::Checksum)
            .object_attributes(ObjectAttributes::ObjectParts)
            .object_attributes(ObjectAttributes::StorageClass)
            .send()
            .await
            .unwrap();

        assert!(old_version.delete_marker().is_none());
        assert_eq!(old_version.version_id(), Some(v1_id.as_str()));
        assert_eq!(old_version.object_size(), Some(3));
        assert_eq!(old_version.e_tag().unwrap().trim_matches('"'), v1_etag);
        assert_eq!(old_version.storage_class(), Some(&StorageClass::Standard));
        assert!(old_version.object_parts().is_none());

        cleanup_versioned(&bucket, "obj", &[v1_id, v2_id]).await;
    });
}

// ── test_get_checksum_object_attributes ──────────────────────────────

/// Put an object with a SHA-256 checksum, verify it appears in
/// GetObjectAttributes response.
#[test]
fn test_get_checksum_object_attributes() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let checksum_sha256 = "arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0=";
        let put_resp = client
            .put_object()
            .bucket(&bucket)
            .key("myobj")
            .body(aws_sdk_s3::primitives::ByteStream::from(vec![b'A'; 1024]))
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .checksum_sha256(checksum_sha256)
            .send()
            .await
            .unwrap();
        assert_eq!(put_resp.checksum_sha256(), Some(checksum_sha256));

        let resp = client
            .get_object_attributes()
            .bucket(&bucket)
            .key("myobj")
            .object_attributes(ObjectAttributes::Etag)
            .object_attributes(ObjectAttributes::Checksum)
            .object_attributes(ObjectAttributes::ObjectParts)
            .object_attributes(ObjectAttributes::StorageClass)
            .object_attributes(ObjectAttributes::ObjectSize)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.object_size(), Some(1024));
        let put_etag = put_resp.e_tag().unwrap().trim_matches('"');
        let got_etag = resp.e_tag().unwrap().trim_matches('"');
        assert_eq!(got_etag, put_etag);
        assert_eq!(resp.storage_class().unwrap(), &StorageClass::Standard);

        // Verify checksum
        let checksum = resp.checksum().expect("expected Checksum in response");
        assert_eq!(
            checksum.checksum_sha256().unwrap(),
            checksum_sha256,
            "SHA-256 checksum mismatch"
        );
        assert!(resp.object_parts().is_none());

        cleanup(&bucket, &["myobj"]).await;
    });
}

// ── Ignored tests (need encryption) ──────────────────────────────────

#[test]
#[ignore = "not implemented: SSE-C encryption"]
fn test_get_sse_c_encrypted_object_attributes() {
    s3_tests::run(async {});
}

/// Helper: create multipart upload, upload parts, complete, return etag.
async fn do_multipart_upload(bucket: &str, key: &str, parts_data: &[Vec<u8>]) -> String {
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

/// Two-part multipart upload, verify ObjectParts TotalPartsCount, part sizes,
/// and part numbers.
#[test]
fn test_get_multipart_object_attributes() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let part1 = vec![b'a'; PART_SIZE];
        let part2 = vec![b'b'; 1024];
        do_multipart_upload(&bucket, "mpu", &[part1, part2]).await;

        let resp = client
            .get_object_attributes()
            .bucket(&bucket)
            .key("mpu")
            .object_attributes(ObjectAttributes::ObjectParts)
            .object_attributes(ObjectAttributes::ObjectSize)
            .send()
            .await
            .unwrap();

        let parts_info = resp.object_parts().expect("expected ObjectParts");
        assert_eq!(parts_info.total_parts_count(), Some(2));
        let parts = parts_info.parts();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].part_number(), Some(1));
        assert_eq!(parts[0].size(), Some(PART_SIZE as i64));
        assert_eq!(parts[1].part_number(), Some(2));
        assert_eq!(parts[1].size(), Some(1024));
        assert_eq!(parts_info.is_truncated(), Some(false));

        assert_eq!(resp.object_size(), Some((PART_SIZE + 1024) as i64));

        cleanup(&bucket, &["mpu"]).await;
    });
}

/// Single-part multipart upload, verify TotalPartsCount=1.
#[test]
fn test_get_single_multipart_object_attributes() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let part1 = vec![b'x'; PART_SIZE];
        do_multipart_upload(&bucket, "mpu-single", &[part1]).await;

        let resp = client
            .get_object_attributes()
            .bucket(&bucket)
            .key("mpu-single")
            .object_attributes(ObjectAttributes::ObjectParts)
            .send()
            .await
            .unwrap();

        let parts_info = resp.object_parts().expect("expected ObjectParts");
        assert_eq!(parts_info.total_parts_count(), Some(1));
        assert_eq!(parts_info.parts().len(), 1);
        assert_eq!(parts_info.parts()[0].part_number(), Some(1));
        assert_eq!(parts_info.parts()[0].size(), Some(PART_SIZE as i64));

        cleanup(&bucket, &["mpu-single"]).await;
    });
}

/// Three-part multipart upload with pagination: max_parts(1), verify truncation
/// and NextPartNumberMarker, then fetch next page.
#[test]
fn test_get_paginated_multipart_object_attributes() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let parts: Vec<Vec<u8>> = vec![
            vec![b'a'; PART_SIZE],
            vec![b'b'; PART_SIZE],
            vec![b'c'; 1024],
        ];
        do_multipart_upload(&bucket, "mpu-page", &parts).await;

        // Page 1: max_parts=1
        let resp = client
            .get_object_attributes()
            .bucket(&bucket)
            .key("mpu-page")
            .object_attributes(ObjectAttributes::ObjectParts)
            .max_parts(1)
            .send()
            .await
            .unwrap();

        let p1 = resp.object_parts().expect("expected ObjectParts page 1");
        assert_eq!(p1.total_parts_count(), Some(3));
        assert_eq!(p1.is_truncated(), Some(true));
        assert_eq!(p1.parts().len(), 1);
        assert_eq!(p1.parts()[0].part_number(), Some(1));
        let next_marker = p1
            .next_part_number_marker()
            .expect("expected NextPartNumberMarker");

        // Page 2: use marker from page 1, max_parts=1
        let resp2 = client
            .get_object_attributes()
            .bucket(&bucket)
            .key("mpu-page")
            .object_attributes(ObjectAttributes::ObjectParts)
            .max_parts(1)
            .part_number_marker(next_marker)
            .send()
            .await
            .unwrap();

        let p2 = resp2.object_parts().expect("expected ObjectParts page 2");
        assert_eq!(p2.total_parts_count(), Some(3));
        assert_eq!(p2.is_truncated(), Some(true));
        assert_eq!(p2.parts().len(), 1);
        assert_eq!(p2.parts()[0].part_number(), Some(2));

        // Page 3: last page
        let next_marker2 = p2
            .next_part_number_marker()
            .expect("expected NextPartNumberMarker page 2");
        let resp3 = client
            .get_object_attributes()
            .bucket(&bucket)
            .key("mpu-page")
            .object_attributes(ObjectAttributes::ObjectParts)
            .max_parts(1)
            .part_number_marker(next_marker2)
            .send()
            .await
            .unwrap();

        let p3 = resp3.object_parts().expect("expected ObjectParts page 3");
        assert_eq!(p3.is_truncated(), Some(false));
        assert_eq!(p3.parts().len(), 1);
        assert_eq!(p3.parts()[0].part_number(), Some(3));
        assert!(p3.next_part_number_marker().is_none());

        cleanup(&bucket, &["mpu-page"]).await;
    });
}

/// max_parts=0 should return IsTruncated=true, empty parts list,
/// TotalPartsCount with the real count, and a NextPartNumberMarker so
/// the caller knows where pagination would start.
#[test]
fn test_get_zero_max_parts_object_attributes() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let part1 = vec![b'a'; PART_SIZE];
        let part2 = vec![b'b'; 1024];
        do_multipart_upload(&bucket, "mpu-zero", &[part1, part2]).await;

        let resp = client
            .get_object_attributes()
            .bucket(&bucket)
            .key("mpu-zero")
            .object_attributes(ObjectAttributes::ObjectParts)
            .max_parts(0)
            .send()
            .await
            .unwrap();

        let parts_info = resp.object_parts().expect("expected ObjectParts");
        assert_eq!(parts_info.total_parts_count(), Some(2));
        assert_eq!(parts_info.is_truncated(), Some(true));
        assert!(parts_info.parts().is_empty());
        // NextPartNumberMarker must be present so callers can advance
        assert!(parts_info.next_part_number_marker().is_some());

        cleanup(&bucket, &["mpu-zero"]).await;
    });
}

/// Multipart upload with CRC32 checksums, verify per-part checksums and
/// object-level checksum in GetObjectAttributes response.
#[test]
fn test_get_multipart_checksum_object_attributes() {
    use aws_sdk_s3::types::ChecksumMode;

    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let key = "mpu-cksum";
        let part1_data = vec![b'X'; PART_SIZE];
        let part2_data = vec![b'Y'; 1024];

        // CreateMultipartUpload with CRC32
        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        // Upload parts with CRC32 checksums (server computes them)
        let resp1 = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(part1_data))
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .send()
            .await
            .unwrap();
        let cksum1 = resp1.checksum_crc32().unwrap().to_string();

        let resp2 = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(2)
            .body(ByteStream::from(part2_data))
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .send()
            .await
            .unwrap();
        let cksum2 = resp2.checksum_crc32().unwrap().to_string();

        // Complete
        let _complete = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(resp1.e_tag().unwrap())
                            .checksum_crc32(&cksum1)
                            .part_number(1)
                            .build(),
                    )
                    .parts(
                        CompletedPart::builder()
                            .e_tag(resp2.e_tag().unwrap())
                            .checksum_crc32(&cksum2)
                            .part_number(2)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .unwrap();

        // GetObjectAttributes: Checksum should show object-level checksum + type
        let resp = client
            .get_object_attributes()
            .bucket(&bucket)
            .key(key)
            .object_attributes(ObjectAttributes::Checksum)
            .object_attributes(ObjectAttributes::ObjectParts)
            .send()
            .await
            .unwrap();

        let checksum = resp.checksum().expect("expected Checksum");
        assert!(
            checksum.checksum_crc32().is_some(),
            "expected CRC32 in Checksum"
        );
        assert_eq!(
            checksum.checksum_type(),
            Some(&aws_sdk_s3::types::ChecksumType::FullObject),
            "expected FULL_OBJECT type"
        );

        // ObjectParts should include per-part checksums
        let parts_info = resp.object_parts().expect("expected ObjectParts");
        assert_eq!(parts_info.total_parts_count(), Some(2));
        let parts = parts_info.parts();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].checksum_crc32(), Some(cksum1.as_str()));
        assert_eq!(parts[1].checksum_crc32(), Some(cksum2.as_str()));

        // HeadObject with ChecksumMode=ENABLED should also return checksum + type
        let head = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
            .unwrap();
        assert!(head.checksum_crc32().is_some());
        assert_eq!(
            head.checksum_type(),
            Some(&aws_sdk_s3::types::ChecksumType::FullObject)
        );

        cleanup(&bucket, &[key]).await;
    });
}
