/// Integration tests for SDK-driven streaming uploads.
///
/// These tests use the actual AWS SDK (`aws-sdk-s3`) client to make streaming
/// uploads via `ByteStream::from_path()` and auto-checksum, verifying the
/// server matches the exact wire format the SDK produces.
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::ChecksumAlgorithm;
use s3_tests::{unique_bucket, CTX};

// ── Helpers ─────────────────────────────────────────────────────────────

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

// ── Tests ───────────────────────────────────────────────────────────────

/// PUT via ByteStream::from_path (no checksum).
/// Triggers STREAMING-AWS4-HMAC-SHA256-PAYLOAD.
#[test]
fn test_sdk_put_from_file() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "streaming-file";

        let data = b"hello from file stream";
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut tmp.as_file(), data).unwrap();

        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_path(tmp.path()).await.unwrap())
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
        let got = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(&got[..], data);

        cleanup(&bucket, &[key]).await;
    });
}

/// PUT via ByteStream::from_path + checksum_algorithm(Crc32).
/// Triggers STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER.
#[test]
fn test_sdk_put_from_file_with_trailing_crc32() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "streaming-file-crc32";

        let data = b"file stream with trailing crc32 checksum";
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut tmp.as_file(), data).unwrap();

        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_path(tmp.path()).await.unwrap())
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .send()
            .await
            .unwrap();

        // Verify data roundtrip.
        let get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let got = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(&got[..], data);

        // Verify checksum was stored.
        let head = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
            .send()
            .await
            .unwrap();
        assert!(
            head.checksum_crc32().is_some(),
            "expected CRC32 checksum on HEAD"
        );

        cleanup(&bucket, &[key]).await;
    });
}

/// PUT via ByteStream::from(Vec) + checksum_algorithm(Crc32).
/// Should trigger streaming trailer mode for in-memory bodies with auto-checksum.
#[test]
fn test_sdk_put_inmemory_auto_checksum() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "streaming-inmem-crc32";

        let data = b"in-memory body with auto crc32";

        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(data.to_vec()))
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .send()
            .await
            .unwrap();

        // Verify data roundtrip.
        let get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let got = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(&got[..], &data[..]);

        // Verify checksum was stored.
        let head = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
            .send()
            .await
            .unwrap();
        assert!(
            head.checksum_crc32().is_some(),
            "expected CRC32 checksum on HEAD"
        );

        cleanup(&bucket, &[key]).await;
    });
}

/// PUT via ByteStream::from_path + checksum_algorithm(Sha256).
/// Verifies SHA-256 trailing checksum path.
#[test]
fn test_sdk_put_from_file_with_trailing_sha256() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "streaming-file-sha256";

        let data = b"file stream with trailing sha256 checksum";
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut tmp.as_file(), data).unwrap();

        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_path(tmp.path()).await.unwrap())
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .send()
            .await
            .unwrap();

        // Verify data roundtrip.
        let get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let got = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(&got[..], data);

        // Verify checksum was stored.
        let head = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
            .send()
            .await
            .unwrap();
        assert!(
            head.checksum_sha256().is_some(),
            "expected SHA256 checksum on HEAD"
        );

        cleanup(&bucket, &[key]).await;
    });
}

/// Large file (1 MB) via ByteStream::from_path.
/// Exercises multi-chunk streaming (SDK splits into multiple aws-chunked chunks).
#[test]
fn test_sdk_put_large_file() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "streaming-large";

        // 1 MB of pattern data.
        let data: Vec<u8> = (0..1_048_576).map(|i| (i % 251) as u8).collect();
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut tmp.as_file(), &data).unwrap();

        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_path(tmp.path()).await.unwrap())
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
        let got = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(got.len(), data.len(), "body length mismatch");
        assert_eq!(&got[..], &data[..], "body data mismatch");

        cleanup(&bucket, &[key]).await;
    });
}

/// Large file (1 MB) via ByteStream::from_path + checksum_algorithm(Crc32).
/// Exercises multi-chunk streaming with trailing checksum (SDK splits into
/// multiple aws-chunked chunks and appends a CRC32 trailer).
#[test]
fn test_sdk_put_large_file_with_trailing_crc32() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "streaming-large-crc32";

        // 1 MB of pattern data — triggers multi-chunk.
        let data: Vec<u8> = (0..1_048_576).map(|i| (i % 251) as u8).collect();
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut tmp.as_file(), &data).unwrap();

        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_path(tmp.path()).await.unwrap())
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .send()
            .await
            .unwrap();

        // Verify data roundtrip.
        let get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let got = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(got.len(), data.len(), "body length mismatch");
        assert_eq!(&got[..], &data[..], "body data mismatch");

        // Verify checksum was stored.
        let head = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
            .send()
            .await
            .unwrap();
        assert!(
            head.checksum_crc32().is_some(),
            "expected CRC32 checksum on HEAD"
        );

        cleanup(&bucket, &[key]).await;
    });
}

/// Multipart upload using ByteStream::from_path for UploadPart.
/// Verifies streaming works for multipart operations.
#[test]
fn test_sdk_multipart_from_file() {
    s3_tests::run(async {
        use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};

        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "streaming-multipart";

        // Two 5 MB parts.
        let part1_data: Vec<u8> = vec![b'A'; 5 * 1024 * 1024];
        let part2_data: Vec<u8> = vec![b'B'; 5 * 1024 * 1024];

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        // Upload part 1 from file.
        let tmp1 = tempfile::NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut tmp1.as_file(), &part1_data).unwrap();
        let resp1 = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from_path(tmp1.path()).await.unwrap())
            .send()
            .await
            .unwrap();

        // Upload part 2 from file.
        let tmp2 = tempfile::NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut tmp2.as_file(), &part2_data).unwrap();
        let resp2 = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(2)
            .body(ByteStream::from_path(tmp2.path()).await.unwrap())
            .send()
            .await
            .unwrap();

        // Complete.
        client
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
            .await
            .unwrap();

        // Verify data roundtrip.
        let get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let got = get.body.collect().await.unwrap().into_bytes();
        let expected_len = part1_data.len() + part2_data.len();
        assert_eq!(got.len(), expected_len, "body length mismatch");
        assert_eq!(&got[..part1_data.len()], &part1_data[..]);
        assert_eq!(&got[part1_data.len()..], &part2_data[..]);

        cleanup(&bucket, &[key]).await;
    });
}
