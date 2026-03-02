use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::ChecksumAlgorithm;
use s3_tests::{err_status, unique_bucket, CTX};

// ── Helpers ─────────────────────────────────────────────────────────

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

/// 1024 bytes of 'A'.
fn body_1k() -> Vec<u8> {
    vec![b'A'; 1024]
}

/// SHA-256 of 1024 × 'A', base64-encoded.
/// Precomputed: sha256(b'A' * 1024) = 6ab7bc...
const SHA256_1K_A: &str = "arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0=";

/// CRC-64/NVME of 1024 × 'A', base64-encoded.
const CRC64NVME_1K_A: &str = "Qeh8oXvGiSo=";

// ── test_object_checksum_sha256 ─────────────────────────────────────

#[test]
fn test_object_checksum_sha256() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "myobj";

        // PUT with valid SHA-256 checksum
        let resp = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(body_1k()))
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .checksum_sha256(SHA256_1K_A)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.checksum_sha256(), Some(SHA256_1K_A));

        // HEAD without ChecksumMode should NOT return the checksum
        let resp = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert!(
            resp.checksum_sha256().is_none(),
            "expected no checksum on plain HEAD"
        );

        // HEAD with ChecksumMode=ENABLED should return the checksum
        let resp = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.checksum_sha256(), Some(SHA256_1K_A));

        // PUT with bad checksum should fail with 400 BadDigest
        let result = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(body_1k()))
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .checksum_sha256("bad")
            .send()
            .await;
        assert_eq!(err_status(&result), 400);

        cleanup(&bucket, &[key]).await;
    });
}

// ── test_object_checksum_crc64nvme ──────────────────────────────────

#[test]
fn test_object_checksum_crc64nvme() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "myobj";

        // PUT with valid CRC-64/NVME checksum
        let resp = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(body_1k()))
            .checksum_algorithm(ChecksumAlgorithm::Crc64Nvme)
            .checksum_crc64_nvme(CRC64NVME_1K_A)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.checksum_crc64_nvme(), Some(CRC64NVME_1K_A));

        // HEAD without ChecksumMode should NOT return the checksum
        let resp = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert!(
            resp.checksum_crc64_nvme().is_none(),
            "expected no checksum on plain HEAD"
        );

        // HEAD with ChecksumMode=ENABLED should return the checksum
        let resp = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.checksum_crc64_nvme(), Some(CRC64NVME_1K_A));

        // PUT with bad checksum should fail with 400 BadDigest
        let result = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(body_1k()))
            .checksum_algorithm(ChecksumAlgorithm::Crc64Nvme)
            .checksum_crc64_nvme("bad")
            .send()
            .await;
        assert_eq!(err_status(&result), 400);

        cleanup(&bucket, &[key]).await;
    });
}

// ── test_post_object_upload_checksum — already in post_object.rs ────

// ── Multipart checksum tests (not implemented) ──────────────────────

#[test]
#[ignore = "not implemented: multipart upload"]
fn test_multipart_checksum_sha256() {
    s3_tests::run(async {});
}

#[test]
#[ignore = "not implemented: multipart upload"]
fn test_multipart_use_cksum_helper_sha256() {
    s3_tests::run(async {});
}

#[test]
#[ignore = "not implemented: multipart upload"]
fn test_multipart_use_cksum_helper_crc64nvme() {
    s3_tests::run(async {});
}

#[test]
#[ignore = "not implemented: multipart upload"]
fn test_multipart_use_cksum_helper_crc32() {
    s3_tests::run(async {});
}

#[test]
#[ignore = "not implemented: multipart upload"]
fn test_multipart_use_cksum_helper_crc32c() {
    s3_tests::run(async {});
}

#[test]
#[ignore = "not implemented: multipart upload"]
fn test_multipart_use_cksum_helper_sha1() {
    s3_tests::run(async {});
}

// ── GetObjectAttributes checksum tests (not implemented) ────────────

#[test]
#[ignore = "not implemented: GetObjectAttributes"]
fn test_get_checksum_object_attributes() {
    s3_tests::run(async {});
}

#[test]
#[ignore = "not implemented: GetObjectAttributes + multipart"]
fn test_get_multipart_checksum_object_attributes() {
    s3_tests::run(async {});
}
