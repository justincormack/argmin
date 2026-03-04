use aws_sdk_s3::types::{
    BucketVersioningStatus, ChecksumAlgorithm, ObjectAttributes, StorageClass,
    VersioningConfiguration,
};
use s3_tests::{unique_bucket, CTX};

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

#[test]
#[ignore = "not implemented: GetObjectAttributes ObjectParts"]
fn test_get_multipart_object_attributes() {
    s3_tests::run(async {});
}

#[test]
#[ignore = "not implemented: GetObjectAttributes ObjectParts"]
fn test_get_single_multipart_object_attributes() {
    s3_tests::run(async {});
}

#[test]
#[ignore = "not implemented: GetObjectAttributes ObjectParts"]
fn test_get_paginated_multipart_object_attributes() {
    s3_tests::run(async {});
}

#[test]
#[ignore = "not implemented: GetObjectAttributes ObjectParts"]
fn test_get_multipart_checksum_object_attributes() {
    s3_tests::run(async {});
}
