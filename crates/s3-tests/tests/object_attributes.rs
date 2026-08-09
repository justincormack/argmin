// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, ChecksumAlgorithm, CompletedMultipartUpload, CompletedPart,
    ObjectAttributes, StorageClass, VersioningConfiguration,
};
use s3_tests::{
    assert_s3_err_code, err_status, send_signed_request,
    shape::{
        assert_shape, error_response_headers, expected_error, id_headers, shape, xml_tag_text,
    },
    sse_c_header_values, test_sse_c_key, unique_bucket, SendRetryingOperationAborted, CTX,
};

const PART_SIZE: usize = 5 * 1024 * 1024; // 5 MB minimum part size

fn endpoint_is_https() -> bool {
    CTX.endpoint().starts_with("https://")
}

fn require_https_endpoint() {
    assert!(
        endpoint_is_https(),
        "object attributes SSE-C coverage requires an https:// endpoint; got {}",
        CTX.endpoint()
    );
}

async fn setup_sse_c_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    s3_tests::create_bucket_with_sse_c_enabled(client, &bucket)
        .await
        .unwrap();
    bucket
}

macro_rules! with_sse_c_headers {
    ($op:expr, $key_b64:expr, $key_md5_b64:expr) => {{
        $op.customize().mutate_request({
            let key_b64 = $key_b64.clone();
            let key_md5_b64 = $key_md5_b64.clone();
            move |req| {
                req.headers_mut()
                    .insert("x-amz-server-side-encryption-customer-algorithm", "AES256");
                req.headers_mut()
                    .insert("x-amz-server-side-encryption-customer-key", key_b64.clone());
                req.headers_mut().insert(
                    "x-amz-server-side-encryption-customer-key-md5",
                    key_md5_b64.clone(),
                );
            }
        })
    }};
}

/// Cleanup helper: delete all given keys then the bucket.
async fn cleanup(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = s3_tests::delete_object_retrying_operation_aborted(client, bucket, key).await;
    }
    s3_tests::delete_bucket_retrying_operation_aborted(client, bucket).await;
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
    s3_tests::delete_bucket_retrying_operation_aborted(client, bucket).await;
}

// ── test_get_object_attributes ──────────────────────────────────────

/// Basic GetObjectAttributes: put a small object, request all attributes,
/// verify ETag, ObjectSize, StorageClass. ObjectParts and VersionId absent.
#[test]
fn test_get_object_attributes() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

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
        s3_tests::create_bucket(client, &bucket).await.unwrap();

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
        s3_tests::create_bucket(client, &bucket).await.unwrap();

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

#[test]
fn test_get_sse_c_encrypted_object_attributes() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_sse_c_bucket().await;

        let key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&key);
        let body = b"encrypted-object-attributes".to_vec();

        with_sse_c_headers!(
            client
                .put_object()
                .bucket(&bucket)
                .key("obj")
                .body(ByteStream::from(body.clone())),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();

        let resp = with_sse_c_headers!(
            client
                .get_object_attributes()
                .bucket(&bucket)
                .key("obj")
                .object_attributes(ObjectAttributes::Etag)
                .object_attributes(ObjectAttributes::ObjectSize),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();

        assert_eq!(resp.object_size(), Some(body.len() as i64));
        assert!(resp.e_tag().is_some());
        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_get_sse_c_object_attributes_requires_headers() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_sse_c_bucket().await;

        let key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&key);

        with_sse_c_headers!(
            client
                .put_object()
                .bucket(&bucket)
                .key("obj")
                .body(ByteStream::from_static(b"encrypted-object-attributes")),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();

        let result = client
            .get_object_attributes()
            .bucket(&bucket)
            .key("obj")
            .object_attributes(ObjectAttributes::Etag)
            .object_attributes(ObjectAttributes::ObjectSize)
            .send()
            .await;

        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidRequest");
        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_get_sse_c_object_attributes_rejects_wrong_key() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_sse_c_bucket().await;

        let key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&key);
        let wrong_key = [42u8; 32];
        let (wrong_key_b64, wrong_key_md5_b64) = sse_c_header_values(&wrong_key);

        with_sse_c_headers!(
            client
                .put_object()
                .bucket(&bucket)
                .key("obj")
                .body(ByteStream::from_static(b"encrypted-object-attributes")),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();

        let result = with_sse_c_headers!(
            client
                .get_object_attributes()
                .bucket(&bucket)
                .key("obj")
                .object_attributes(ObjectAttributes::Etag)
                .object_attributes(ObjectAttributes::ObjectSize),
            wrong_key_b64,
            wrong_key_md5_b64
        )
        .send()
        .await;

        assert_eq!(err_status(&result), 403);
        assert_s3_err_code(&result, "AccessDenied");
        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_get_sse_c_checksum_object_attributes() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_sse_c_bucket().await;

        let key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&key);
        let checksum_sha256 = "arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0=";

        with_sse_c_headers!(
            client
                .put_object()
                .bucket(&bucket)
                .key("obj")
                .body(ByteStream::from(vec![b'A'; 1024]))
                .checksum_algorithm(ChecksumAlgorithm::Sha256)
                .checksum_sha256(checksum_sha256),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();

        let resp = with_sse_c_headers!(
            client
                .get_object_attributes()
                .bucket(&bucket)
                .key("obj")
                .object_attributes(ObjectAttributes::Checksum)
                .object_attributes(ObjectAttributes::ObjectSize),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();

        assert_eq!(resp.object_size(), Some(1024));
        let checksum = resp.checksum().expect("expected checksum");
        assert_eq!(checksum.checksum_sha256(), Some(checksum_sha256));

        cleanup(&bucket, &["obj"]).await;
    });
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
        s3_tests::create_bucket(client, &bucket).await.unwrap();

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

        // Non-checksummed multipart: AWS only returns PartsCount, no Part elements
        let parts_info = resp.object_parts().expect("expected ObjectParts");
        assert_eq!(parts_info.total_parts_count(), Some(2));

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
        s3_tests::create_bucket(client, &bucket).await.unwrap();

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

        // Non-checksummed multipart: AWS only returns PartsCount
        let parts_info = resp.object_parts().expect("expected ObjectParts");
        assert_eq!(parts_info.total_parts_count(), Some(1));

        cleanup(&bucket, &["mpu-single"]).await;
    });
}

/// Three-part checksummed multipart upload with pagination: max_parts(1),
/// verify truncation and NextPartNumberMarker, then fetch next page.
/// Uses CRC32 checksums so AWS returns full ObjectParts detail.
#[test]
fn test_get_paginated_multipart_object_attributes() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let key = "mpu-page";
        let parts_data: Vec<Vec<u8>> = vec![
            vec![b'a'; PART_SIZE],
            vec![b'b'; PART_SIZE],
            vec![b'c'; 1024],
        ];

        // Create checksummed multipart upload
        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let mut completed_parts = Vec::new();
        for (i, data) in parts_data.iter().enumerate() {
            let part_number = (i + 1) as i32;
            let resp = client
                .upload_part()
                .bucket(&bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(part_number)
                .body(ByteStream::from(data.clone()))
                .checksum_algorithm(ChecksumAlgorithm::Crc32)
                .send()
                .await
                .unwrap();
            completed_parts.push(
                CompletedPart::builder()
                    .e_tag(resp.e_tag().unwrap())
                    .checksum_crc32(resp.checksum_crc32().unwrap())
                    .part_number(part_number)
                    .build(),
            );
        }

        client
            .complete_multipart_upload()
            .bucket(&bucket)
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

        // Page 1: max_parts=1
        let resp = client
            .get_object_attributes()
            .bucket(&bucket)
            .key(key)
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
            .key(key)
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
            .key(key)
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
        assert!(p3.next_part_number_marker().is_some());

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_get_object_attributes_max_parts_above_list_limit_is_preserved() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let key = "mpu-max-parts-clamp";
        let parts_data = [vec![b'a'; PART_SIZE], vec![b'b'; 1024]];

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let mut completed_parts = Vec::new();
        for (i, data) in parts_data.iter().enumerate() {
            let part_number = (i + 1) as i32;
            let resp = client
                .upload_part()
                .bucket(&bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(part_number)
                .body(ByteStream::from(data.clone()))
                .checksum_algorithm(ChecksumAlgorithm::Crc32)
                .send()
                .await
                .unwrap();
            completed_parts.push(
                CompletedPart::builder()
                    .e_tag(resp.e_tag().unwrap())
                    .checksum_crc32(resp.checksum_crc32().unwrap())
                    .part_number(part_number)
                    .build(),
            );
        }

        client
            .complete_multipart_upload()
            .bucket(&bucket)
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

        let url = format!("{}/{bucket}/{key}?attributes", CTX.endpoint());
        let response = send_signed_request(
            "GET",
            &url,
            b"",
            [
                ("x-amz-object-attributes", "ObjectParts"),
                ("x-amz-max-parts", "5000"),
            ],
        );

        assert_eq!(response.status, 200, "unexpected body: {}", response.body);
        assert!(
            response.body.contains("<MaxParts>5000</MaxParts>"),
            "unexpected body: {}",
            response.body
        );

        cleanup(&bucket, &[key]).await;
    });
}

/// max_parts=0 should return IsTruncated=false, empty parts list,
/// and TotalPartsCount with the real count.
/// Uses CRC32 checksums so AWS returns full ObjectParts detail.
#[test]
fn test_get_zero_max_parts_object_attributes() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let key = "mpu-zero";
        let parts_data = [vec![b'a'; PART_SIZE], vec![b'b'; 1024]];

        // Create checksummed multipart upload
        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let mut completed_parts = Vec::new();
        for (i, data) in parts_data.iter().enumerate() {
            let part_number = (i + 1) as i32;
            let resp = client
                .upload_part()
                .bucket(&bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(part_number)
                .body(ByteStream::from(data.clone()))
                .checksum_algorithm(ChecksumAlgorithm::Crc32)
                .send()
                .await
                .unwrap();
            completed_parts.push(
                CompletedPart::builder()
                    .e_tag(resp.e_tag().unwrap())
                    .checksum_crc32(resp.checksum_crc32().unwrap())
                    .part_number(part_number)
                    .build(),
            );
        }

        client
            .complete_multipart_upload()
            .bucket(&bucket)
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

        let resp = client
            .get_object_attributes()
            .bucket(&bucket)
            .key(key)
            .object_attributes(ObjectAttributes::ObjectParts)
            .max_parts(0)
            .send()
            .await
            .unwrap();

        let parts_info = resp.object_parts().expect("expected ObjectParts");
        assert_eq!(parts_info.total_parts_count(), Some(2));
        assert_eq!(parts_info.is_truncated(), Some(false));
        assert!(parts_info.parts().is_empty());
        assert!(parts_info.next_part_number_marker().is_some());

        cleanup(&bucket, &[key]).await;
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
        s3_tests::create_bucket(client, &bucket).await.unwrap();

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
            Some(&aws_sdk_s3::types::ChecksumType::Composite),
            "expected COMPOSITE type"
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
            Some(&aws_sdk_s3::types::ChecksumType::Composite)
        );

        cleanup(&bucket, &[key]).await;
    });
}

// ── ChecksumType default and validation tests ─────────────────────────

/// Helper: create a checksummed multipart upload and complete it.
/// Returns the CompleteMultipartUpload response.
async fn do_checksummed_multipart_upload(
    bucket: &str,
    key: &str,
    algo: ChecksumAlgorithm,
    checksum_type: Option<aws_sdk_s3::types::ChecksumType>,
) -> aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput {
    let client = CTX.client();
    let mut create = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .checksum_algorithm(algo.clone());
    if let Some(ct) = checksum_type {
        create = create.checksum_type(ct);
    }
    let create_resp = create
        .send_retrying_operation_aborted("create multipart upload for object attributes")
        .await
        .unwrap();
    let upload_id = create_resp.upload_id().unwrap();

    let part_data = vec![b'Z'; PART_SIZE];
    let resp = client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from(part_data))
        .checksum_algorithm(algo.clone())
        .send()
        .await
        .unwrap();

    let mut part_builder = CompletedPart::builder()
        .e_tag(resp.e_tag().unwrap())
        .part_number(1);

    // Attach the per-part checksum
    match algo {
        ChecksumAlgorithm::Crc32 => {
            part_builder = part_builder.checksum_crc32(resp.checksum_crc32().unwrap());
        }
        ChecksumAlgorithm::Crc32C => {
            part_builder = part_builder.checksum_crc32_c(resp.checksum_crc32_c().unwrap());
        }
        ChecksumAlgorithm::Sha256 => {
            part_builder = part_builder.checksum_sha256(resp.checksum_sha256().unwrap());
        }
        ChecksumAlgorithm::Sha1 => {
            part_builder = part_builder.checksum_sha1(resp.checksum_sha1().unwrap());
        }
        ChecksumAlgorithm::Crc64Nvme => {
            part_builder = part_builder.checksum_crc64_nvme(resp.checksum_crc64_nvme().unwrap());
        }
        _ => {}
    }

    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(part_builder.build())
                .build(),
        )
        .send()
        .await
        .unwrap()
}

/// CRC32 multipart with no explicit checksum_type defaults to COMPOSITE.
#[test]
fn test_multipart_crc32_default_composite() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let key = "mpu-crc32-default";
        let complete =
            do_checksummed_multipart_upload(&bucket, key, ChecksumAlgorithm::Crc32, None).await;

        // Composite checksums have a -N suffix (e.g. "AAAAAA==-1")
        let cksum = complete.checksum_crc32().expect("expected CRC32 checksum");
        assert!(
            cksum.contains('-'),
            "expected composite checksum with -N suffix, got: {cksum}"
        );

        let resp = client
            .get_object_attributes()
            .bucket(&bucket)
            .key(key)
            .object_attributes(ObjectAttributes::Checksum)
            .send()
            .await
            .unwrap();
        let checksum = resp.checksum().expect("expected Checksum");
        assert_eq!(
            checksum.checksum_type(),
            Some(&aws_sdk_s3::types::ChecksumType::Composite)
        );

        cleanup(&bucket, &[key]).await;
    });
}

/// CRC32C multipart with no explicit checksum_type defaults to COMPOSITE.
#[test]
fn test_multipart_crc32c_default_composite() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let key = "mpu-crc32c-default";
        let complete =
            do_checksummed_multipart_upload(&bucket, key, ChecksumAlgorithm::Crc32C, None).await;

        let cksum = complete
            .checksum_crc32_c()
            .expect("expected CRC32C checksum");
        assert!(
            cksum.contains('-'),
            "expected composite checksum with -N suffix, got: {cksum}"
        );

        let resp = client
            .get_object_attributes()
            .bucket(&bucket)
            .key(key)
            .object_attributes(ObjectAttributes::Checksum)
            .send()
            .await
            .unwrap();
        let checksum = resp.checksum().expect("expected Checksum");
        assert_eq!(
            checksum.checksum_type(),
            Some(&aws_sdk_s3::types::ChecksumType::Composite)
        );

        cleanup(&bucket, &[key]).await;
    });
}

/// SHA256 multipart with no explicit checksum_type defaults to COMPOSITE.
#[test]
fn test_multipart_sha256_default_composite() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let key = "mpu-sha256-default";
        let complete =
            do_checksummed_multipart_upload(&bucket, key, ChecksumAlgorithm::Sha256, None).await;

        let cksum = complete
            .checksum_sha256()
            .expect("expected SHA256 checksum");
        assert!(
            cksum.contains('-'),
            "expected composite checksum with -N suffix, got: {cksum}"
        );

        let resp = client
            .get_object_attributes()
            .bucket(&bucket)
            .key(key)
            .object_attributes(ObjectAttributes::Checksum)
            .send()
            .await
            .unwrap();
        let checksum = resp.checksum().expect("expected Checksum");
        assert_eq!(
            checksum.checksum_type(),
            Some(&aws_sdk_s3::types::ChecksumType::Composite)
        );

        cleanup(&bucket, &[key]).await;
    });
}

/// CRC64NVME multipart defaults to FULL_OBJECT (only supported type).
#[test]
fn test_multipart_crc64nvme_default_full_object() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let key = "mpu-crc64nvme-default";
        let _complete =
            do_checksummed_multipart_upload(&bucket, key, ChecksumAlgorithm::Crc64Nvme, None).await;

        let resp = client
            .get_object_attributes()
            .bucket(&bucket)
            .key(key)
            .object_attributes(ObjectAttributes::Checksum)
            .send()
            .await
            .unwrap();
        let checksum = resp.checksum().expect("expected Checksum");
        assert_eq!(
            checksum.checksum_type(),
            Some(&aws_sdk_s3::types::ChecksumType::FullObject)
        );

        cleanup(&bucket, &[key]).await;
    });
}

/// CRC64NVME with explicit COMPOSITE should be rejected.
#[test]
fn test_multipart_crc64nvme_composite_rejected() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let result = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("mpu-crc64nvme-composite")
            .checksum_algorithm(ChecksumAlgorithm::Crc64Nvme)
            .checksum_type(aws_sdk_s3::types::ChecksumType::Composite)
            .send()
            .await;
        assert!(result.is_err(), "expected error for CRC64NVME + COMPOSITE");

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

/// CRC32 with explicit FULL_OBJECT should be accepted.
#[test]
fn test_multipart_crc32_explicit_full_object() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let key = "mpu-crc32-full-object";
        let _complete = do_checksummed_multipart_upload(
            &bucket,
            key,
            ChecksumAlgorithm::Crc32,
            Some(aws_sdk_s3::types::ChecksumType::FullObject),
        )
        .await;

        let resp = client
            .get_object_attributes()
            .bucket(&bucket)
            .key(key)
            .object_attributes(ObjectAttributes::Checksum)
            .send()
            .await
            .unwrap();
        let checksum = resp.checksum().expect("expected Checksum");
        assert_eq!(
            checksum.checksum_type(),
            Some(&aws_sdk_s3::types::ChecksumType::FullObject)
        );

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_get_object_attributes_multipart_response_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        let key = "shape-object-attributes.txt";

        let create = send_signed_request(
            "POST",
            &format!("{}/{}/{}?uploads=", CTX.endpoint(), bucket, key),
            b"",
            [
                ("Content-Type", "application/octet-stream"),
                ("x-amz-checksum-algorithm", "CRC32"),
            ],
        );
        let upload_id = xml_tag_text(&create.body, "UploadId")
            .expect("create upload for attributes fixture")
            .to_string();
        let big = vec![b'B'; 5 * 1024 * 1024];
        let mut etags = vec![];
        let mut checksums = vec![];
        for (part_number, body) in [(1u32, big.as_slice()), (2, &b"attributes-second-part"[..])] {
            let crc32 = s3_tests::sdk_checksum_headers(body)
                .into_iter()
                .find(|(header, _)| header == "x-amz-checksum-crc32")
                .map(|(_, value)| value)
                .expect("crc32 checksum header");
            let part = send_signed_request(
                "PUT",
                &format!(
                    "{}/{}/{}?partNumber={}&uploadId={}",
                    CTX.endpoint(),
                    bucket,
                    key,
                    part_number,
                    upload_id
                ),
                body,
                [("x-amz-checksum-crc32", crc32.as_str())],
            );
            let etag = s3_tests::shape::response_header_value(&part, "etag")
                .expect("part etag")
                .to_string();
            etags.push(etag);
            checksums.push(crc32);
        }
        let complete_body = format!(
            "<CompleteMultipartUpload>\
             <Part><PartNumber>1</PartNumber><ETag>{}</ETag>\
             <ChecksumCRC32>{}</ChecksumCRC32></Part>\
             <Part><PartNumber>2</PartNumber><ETag>{}</ETag>\
             <ChecksumCRC32>{}</ChecksumCRC32></Part>\
             </CompleteMultipartUpload>",
            etags[0], checksums[0], etags[1], checksums[1]
        );
        let complete = send_signed_request(
            "POST",
            &format!(
                "{}/{}/{}?uploadId={}",
                CTX.endpoint(),
                bucket,
                key,
                upload_id
            ),
            complete_body.as_bytes(),
            [("Content-Type", "application/xml")],
        );
        assert_eq!(complete.status, 200, "complete fixture: {complete:?}");

        // Composite CRC32 attributes for a two-part upload: every checksum
        // value is deterministic for the fixed part bodies.
        let response = send_signed_request(
            "GET",
            &format!("{}/{}/{}?attributes=", CTX.endpoint(), bucket, key),
            b"",
            [(
                "x-amz-object-attributes",
                "Checksum,ObjectParts,ObjectSize,StorageClass",
            )],
        );
        assert_shape(
            "GetObjectAttributes multipart shape",
            &response,
            &shape()
                .status(200)
                .headers(id_headers())
                .header("last-modified", "{http_date}")
                .body(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <GetObjectAttributesResponse \
                     xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Checksum>\
                     <ChecksumCRC32>1wbLhg==</ChecksumCRC32>\
                     <ChecksumType>COMPOSITE</ChecksumType></Checksum><ObjectParts>\
                     <PartsCount>2</PartsCount><PartNumberMarker>0</PartNumberMarker>\
                     <NextPartNumberMarker>2</NextPartNumberMarker><MaxParts>1000</MaxParts>\
                     <IsTruncated>false</IsTruncated><Part><PartNumber>1</PartNumber>\
                     <Size>5242880</Size><ChecksumCRC32>QoZTGg==</ChecksumCRC32></Part>\
                     <Part><PartNumber>2</PartNumber><Size>22</Size>\
                     <ChecksumCRC32>d7wqew==</ChecksumCRC32></Part></ObjectParts>\
                     <StorageClass>STANDARD</StorageClass><ObjectSize>5242902</ObjectSize>\
                     </GetObjectAttributesResponse>",
                ),
        );

        s3_tests::delete_object_retrying_operation_aborted(client, &bucket, key)
            .await
            .expect("delete attributes shape fixture");
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_get_object_attributes_rejects_managed_encryption_request_headers() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        let key = "attributes-managed-encryption-read-headers.txt";

        let put = send_signed_request(
            "PUT",
            &format!("{}/{}/{}", CTX.endpoint(), bucket, key),
            b"attributes encrypted by default",
            std::iter::empty::<(&str, &str)>(),
        );
        assert_eq!(put.status, 200, "attributes fixture PUT failed: {put:?}");

        let invalid_sse_body = expected_error::invalid_argument_with_value(
            "x-amz-server-side-encryption header is not supported for this operation.",
            "x-amz-server-side-encryption",
            "AES256",
        );
        let sse_response = send_signed_request(
            "GET",
            &format!("{}/{}/{}?attributes=", CTX.endpoint(), bucket, key),
            b"",
            [
                ("x-amz-object-attributes", "ETag,ObjectSize"),
                ("x-amz-server-side-encryption", "AES256"),
            ],
        );
        assert_shape(
            "GetObjectAttributes rejects x-amz-server-side-encryption request header",
            &sse_response,
            &shape()
                .status(400)
                .headers(error_response_headers())
                .body(invalid_sse_body.as_str()),
        );

        let invalid_kms_key_body = expected_error::invalid_argument_no_decl(
            "Server Side Encryption with AWS KMS managed key requires HTTP header x-amz-server-side-encryption : aws:kms",
            "x-amz-server-side-encryption",
        );
        let kms_key_response = send_signed_request(
            "GET",
            &format!("{}/{}/{}?attributes=", CTX.endpoint(), bucket, key),
            b"",
            [
                ("x-amz-object-attributes", "ETag,ObjectSize"),
                (
                    "x-amz-server-side-encryption-aws-kms-key-id",
                    "arn:aws:kms:us-east-1:111122223333:key/example",
                ),
            ],
        );
        assert_shape(
            "GetObjectAttributes rejects x-amz-server-side-encryption-aws-kms-key-id request header",
            &kms_key_response,
            &shape()
                .status(400)
                .headers(error_response_headers())
                .body(invalid_kms_key_body.as_str()),
        );

        s3_tests::delete_object_retrying_operation_aborted(client, &bucket, key)
            .await
            .expect("delete attributes managed-encryption fixture");
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}
