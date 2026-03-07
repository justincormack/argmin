use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    ChecksumAlgorithm, ChecksumMode, ChecksumType, CompletedMultipartUpload, CompletedPart,
    ObjectAttributes,
};
use s3_tests::{assert_s3_err_code, err_status, unique_bucket, CTX};

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

const PART_SIZE: usize = 5 * 1024 * 1024;

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
        assert_s3_err_code(&result, "InvalidRequest");

        cleanup(&bucket, &[key]).await;
    });
}

// ── test_object_checksum_crc32 ──────────────────────────────────────

/// CRC-32 of 1024 × 'A', base64-encoded.
const CRC32_1K_A: &str = "tzf7Gg==";

#[test]
fn test_object_checksum_crc32() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "myobj";

        // PUT with valid CRC-32 checksum
        let resp = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(body_1k()))
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .checksum_crc32(CRC32_1K_A)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.checksum_crc32(), Some(CRC32_1K_A));

        // GET with ChecksumMode should return the checksum
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.checksum_crc32(), Some(CRC32_1K_A));

        // PUT with bad checksum should fail
        let result = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(body_1k()))
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .checksum_crc32("AAAA/w==")
            .send()
            .await;
        assert_eq!(err_status(&result), 400);

        cleanup(&bucket, &[key]).await;
    });
}

// ── test_object_checksum_crc32c ─────────────────────────────────────

#[test]
fn test_object_checksum_crc32c() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "myobj";

        // PUT with SDK-computed CRC-32C checksum (SDK computes when only
        // checksum_algorithm is set without an explicit value).
        let resp = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(body_1k()))
            .checksum_algorithm(ChecksumAlgorithm::Crc32C)
            .send()
            .await
            .unwrap();
        let crc32c_val = resp.checksum_crc32_c().unwrap();

        // GET with ChecksumMode should return the same checksum
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.checksum_crc32_c(), Some(crc32c_val.as_ref()));

        cleanup(&bucket, &[key]).await;
    });
}

// ── test_object_checksum_sha1 ───────────────────────────────────────

/// SHA-1 of 1024 × 'A', base64-encoded.
const SHA1_1K_A: &str = "dGw/TShsUx4GXor3bgrAhogxxrQ=";

#[test]
fn test_object_checksum_sha1() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "myobj";

        // PUT with valid SHA-1 checksum
        let resp = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(body_1k()))
            .checksum_algorithm(ChecksumAlgorithm::Sha1)
            .checksum_sha1(SHA1_1K_A)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.checksum_sha1(), Some(SHA1_1K_A));

        // GET with ChecksumMode should return the checksum
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.checksum_sha1(), Some(SHA1_1K_A));

        // PUT with bad checksum should fail
        let result = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(body_1k()))
            .checksum_algorithm(ChecksumAlgorithm::Sha1)
            .checksum_sha1("bad")
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
        assert_s3_err_code(&result, "InvalidRequest");

        cleanup(&bucket, &[key]).await;
    });
}

// ── test_post_object_upload_checksum — already in post_object.rs ────

// ── Multipart checksum tests ─────────────────────────────────────────

/// Helper: 3-part multipart upload with checksums.
///
/// Creates a multipart upload with the given checksum algorithm and type, uploads
/// 3 parts with pre-computed checksums, completes with the composite/combined
/// checksum, then verifies: CompleteMultipartUpload response, HeadObject with
/// ChecksumMode=ENABLED, and GetObjectAttributes Checksum.
///
/// Mirrors the Ceph `multipart_checksum_3parts_helper`.
struct MultipartChecksumTestCase {
    algo: ChecksumAlgorithm,
    cksum_type: ChecksumType,
    part1_cksum: &'static str,
    part2_cksum: &'static str,
    part3_cksum: &'static str,
    composite_cksum: &'static str,
}

/// Extract the checksum value from a response by algorithm.
fn get_cksum_from_complete(
    resp: &aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput,
    algo: &ChecksumAlgorithm,
) -> Option<String> {
    match algo {
        a if *a == ChecksumAlgorithm::Sha256 => resp.checksum_sha256().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Sha1 => resp.checksum_sha1().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Crc32 => resp.checksum_crc32().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Crc32C => resp.checksum_crc32_c().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Crc64Nvme => {
            resp.checksum_crc64_nvme().map(|s| s.to_string())
        }
        _ => None,
    }
}

fn get_cksum_from_head(
    resp: &aws_sdk_s3::operation::head_object::HeadObjectOutput,
    algo: &ChecksumAlgorithm,
) -> Option<String> {
    match algo {
        a if *a == ChecksumAlgorithm::Sha256 => resp.checksum_sha256().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Sha1 => resp.checksum_sha1().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Crc32 => resp.checksum_crc32().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Crc32C => resp.checksum_crc32_c().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Crc64Nvme => {
            resp.checksum_crc64_nvme().map(|s| s.to_string())
        }
        _ => None,
    }
}

fn get_cksum_from_checksum(
    cksum: &aws_sdk_s3::types::Checksum,
    algo: &ChecksumAlgorithm,
) -> Option<String> {
    match algo {
        a if *a == ChecksumAlgorithm::Sha256 => cksum.checksum_sha256().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Sha1 => cksum.checksum_sha1().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Crc32 => cksum.checksum_crc32().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Crc32C => cksum.checksum_crc32_c().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Crc64Nvme => {
            cksum.checksum_crc64_nvme().map(|s| s.to_string())
        }
        _ => None,
    }
}

/// Build a CompletedPart with the correct checksum field set for the given algorithm.
fn completed_part_with_checksum(
    etag: &str,
    part_number: i32,
    algo: &ChecksumAlgorithm,
    cksum: &str,
) -> CompletedPart {
    let mut b = CompletedPart::builder()
        .e_tag(etag)
        .part_number(part_number);
    if *algo == ChecksumAlgorithm::Sha256 {
        b = b.checksum_sha256(cksum);
    } else if *algo == ChecksumAlgorithm::Sha1 {
        b = b.checksum_sha1(cksum);
    } else if *algo == ChecksumAlgorithm::Crc32 {
        b = b.checksum_crc32(cksum);
    } else if *algo == ChecksumAlgorithm::Crc32C {
        b = b.checksum_crc32_c(cksum);
    } else if *algo == ChecksumAlgorithm::Crc64Nvme {
        b = b.checksum_crc64_nvme(cksum);
    }
    b.build()
}

/// Set the checksum value on an upload_part builder for the given algorithm.
fn upload_part_with_checksum(
    builder: aws_sdk_s3::operation::upload_part::builders::UploadPartFluentBuilder,
    algo: &ChecksumAlgorithm,
    cksum: &str,
) -> aws_sdk_s3::operation::upload_part::builders::UploadPartFluentBuilder {
    if *algo == ChecksumAlgorithm::Sha256 {
        builder.checksum_sha256(cksum)
    } else if *algo == ChecksumAlgorithm::Sha1 {
        builder.checksum_sha1(cksum)
    } else if *algo == ChecksumAlgorithm::Crc32 {
        builder.checksum_crc32(cksum)
    } else if *algo == ChecksumAlgorithm::Crc32C {
        builder.checksum_crc32_c(cksum)
    } else if *algo == ChecksumAlgorithm::Crc64Nvme {
        builder.checksum_crc64_nvme(cksum)
    } else {
        builder
    }
}

fn get_cksum_from_upload_part(
    resp: &aws_sdk_s3::operation::upload_part::UploadPartOutput,
    algo: &ChecksumAlgorithm,
) -> Option<String> {
    match algo {
        a if *a == ChecksumAlgorithm::Sha256 => resp.checksum_sha256().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Sha1 => resp.checksum_sha1().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Crc32 => resp.checksum_crc32().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Crc32C => resp.checksum_crc32_c().map(|s| s.to_string()),
        a if *a == ChecksumAlgorithm::Crc64Nvme => {
            resp.checksum_crc64_nvme().map(|s| s.to_string())
        }
        _ => None,
    }
}

async fn run_multipart_checksum_test(tc: &MultipartChecksumTestCase) {
    let client = CTX.client();
    let bucket = setup_bucket().await;
    let key = "mymultipart3";

    // CreateMultipartUpload with checksum algorithm + type
    let create = client
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .checksum_algorithm(tc.algo.clone())
        .checksum_type(tc.cksum_type.clone())
        .send()
        .await
        .unwrap();
    let upload_id = create.upload_id().unwrap();

    let parts_data: [(&[u8], &str); 3] = [
        (&[b'A'; PART_SIZE][..], tc.part1_cksum),
        (&[b'B'; PART_SIZE][..], tc.part2_cksum),
        (&[b'C'; PART_SIZE][..], tc.part3_cksum),
    ];

    let mut completed_parts = Vec::new();
    for (i, (data, cksum)) in parts_data.iter().enumerate() {
        let part_number = (i + 1) as i32;
        let builder = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(ByteStream::from(data.to_vec()))
            .checksum_algorithm(tc.algo.clone());
        let builder = upload_part_with_checksum(builder, &tc.algo, cksum);
        let resp = builder.send().await.unwrap();
        let returned_cksum = get_cksum_from_upload_part(&resp, &tc.algo)
            .expect("upload_part should return checksum");
        assert_eq!(returned_cksum, *cksum, "upload_part checksum mismatch");
        completed_parts.push(completed_part_with_checksum(
            resp.e_tag().unwrap(),
            part_number,
            &tc.algo,
            &returned_cksum,
        ));
    }

    // CompleteMultipartUpload with composite/combined checksum
    let complete_resp = client
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

    // Verify response contains the checksum
    let resp_cksum = get_cksum_from_complete(&complete_resp, &tc.algo)
        .expect("complete response should contain checksum");
    assert_eq!(resp_cksum, tc.composite_cksum, "complete checksum mismatch");

    // HeadObject without ChecksumMode should NOT return the checksum
    let head_resp = client
        .head_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    assert!(
        get_cksum_from_head(&head_resp, &tc.algo).is_none(),
        "expected no checksum on plain HEAD"
    );

    // HeadObject with ChecksumMode=ENABLED should return the checksum + type
    let head_resp = client
        .head_object()
        .bucket(&bucket)
        .key(key)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .unwrap();
    let head_cksum =
        get_cksum_from_head(&head_resp, &tc.algo).expect("HEAD ENABLED should return checksum");
    assert_eq!(head_cksum, tc.composite_cksum, "HEAD checksum mismatch");
    assert_eq!(
        head_resp.checksum_type(),
        Some(&tc.cksum_type),
        "HEAD checksum type mismatch"
    );

    // GetObjectAttributes Checksum should include the checksum + type
    let attr_resp = client
        .get_object_attributes()
        .bucket(&bucket)
        .key(key)
        .object_attributes(ObjectAttributes::Checksum)
        .send()
        .await
        .unwrap();
    let cksum_info = attr_resp.checksum().expect("expected Checksum in response");
    let attr_cksum = get_cksum_from_checksum(cksum_info, &tc.algo)
        .expect("GetObjectAttributes should return checksum");
    // GetObjectAttributes returns the bare hash without the composite "-N" suffix;
    // the part count is conveyed by ChecksumType instead.
    let expected_bare = tc
        .composite_cksum
        .rfind('-')
        .and_then(|pos| {
            if tc.composite_cksum[pos + 1..]
                .bytes()
                .all(|b| b.is_ascii_digit())
                && !tc.composite_cksum[pos + 1..].is_empty()
            {
                Some(&tc.composite_cksum[..pos])
            } else {
                None
            }
        })
        .unwrap_or(tc.composite_cksum);
    assert_eq!(
        attr_cksum, expected_bare,
        "GetObjectAttributes checksum mismatch"
    );
    assert_eq!(
        cksum_info.checksum_type(),
        Some(&tc.cksum_type),
        "GetObjectAttributes checksum type mismatch"
    );

    cleanup(&bucket, &[key]).await;
}

// ── test_multipart_checksum_sha256 ───────────────────────────────────

/// Tests bad checksum rejection and missing part checksum rejection on
/// CompleteMultipartUpload, then a successful COMPOSITE SHA-256 upload.
#[test]
fn test_multipart_checksum_sha256() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        // -- bad object-level checksum rejected --
        let key = "mymultipart";
        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
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
            .body(ByteStream::from(body_1k()))
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .checksum_sha256(SHA256_1K_A)
            .send()
            .await
            .unwrap();

        // CompleteMultipartUpload with wrong checksum should fail
        let result = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .checksum_sha256("bad")
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(resp.e_tag().unwrap())
                            .checksum_sha256(resp.checksum_sha256().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidRequest");

        // -- missing part checksum rejected --
        let key2 = "mymultipart2";
        let create2 = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key2)
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .send()
            .await
            .unwrap();
        let upload_id2 = create2.upload_id().unwrap();

        let resp2 = client
            .upload_part()
            .bucket(&bucket)
            .key(key2)
            .upload_id(upload_id2)
            .part_number(1)
            .body(ByteStream::from(body_1k()))
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .checksum_sha256(SHA256_1K_A)
            .send()
            .await
            .unwrap();

        // CompleteMultipartUpload without per-part checksum should fail
        let result2 = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key2)
            .upload_id(upload_id2)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(resp2.e_tag().unwrap())
                            // no checksum_sha256 on the part
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await;
        assert_eq!(err_status(&result2), 400);

        // -- successful COMPOSITE SHA-256 upload --
        let key3 = "mymultipart3";
        let create3 = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key3)
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .send()
            .await
            .unwrap();
        let upload_id3 = create3.upload_id().unwrap();

        let resp3 = client
            .upload_part()
            .bucket(&bucket)
            .key(key3)
            .upload_id(upload_id3)
            .part_number(1)
            .body(ByteStream::from(body_1k()))
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .checksum_sha256(SHA256_1K_A)
            .send()
            .await
            .unwrap();

        // COMPOSITE checksum = sha256(raw_part_checksum)-1
        let composite = "Ok6Cs5b96ux6+MWQkJO7UBT5sKPBeXBLwvj/hK89smg=-1";
        let complete_resp = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key3)
            .upload_id(upload_id3)
            .checksum_sha256(composite)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(resp3.e_tag().unwrap())
                            .checksum_sha256(resp3.checksum_sha256().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(complete_resp.checksum_sha256(), Some(composite));

        // HEAD with ChecksumMode=ENABLED
        let head = client
            .head_object()
            .bucket(&bucket)
            .key(key3)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
            .unwrap();
        assert_eq!(head.checksum_sha256(), Some(composite));

        // Abort the incomplete uploads, then clean up the completed key.
        let _ = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await;
        let _ = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key2)
            .upload_id(upload_id2)
            .send()
            .await;
        cleanup(&bucket, &[key3]).await;
    });
}

// ── Multipart 3-part checksum helper tests ──────────────────────────

/// Pre-computed checksums for 5MB × 'A', 5MB × 'B', 5MB × 'C'.
/// Values from Ceph s3-tests (unittest_rgw_cksum).

#[test]
fn test_multipart_use_cksum_helper_sha256() {
    s3_tests::run(async {
        run_multipart_checksum_test(&MultipartChecksumTestCase {
            algo: ChecksumAlgorithm::Sha256,
            cksum_type: ChecksumType::Composite,
            part1_cksum: "275VF5loJr1YYawit0XSHREhkFXYkkPKGuoK0x9VKxI=",
            part2_cksum: "mrHwOfjTL5Zwfj74F05HOQGLdUb7E5szdCbxgUSq6NM=",
            part3_cksum: "Vw7oB/nKQ5xWb3hNgbyfkvDiivl+U+/Dft48nfJfDow=",
            composite_cksum: "uWBwpe1dxI4Vw8Gf0X9ynOdw/SS6VBzfWm9giiv1sf4=-3",
        })
        .await;
    });
}

#[test]
fn test_multipart_use_cksum_helper_crc64nvme() {
    s3_tests::run(async {
        run_multipart_checksum_test(&MultipartChecksumTestCase {
            algo: ChecksumAlgorithm::Crc64Nvme,
            cksum_type: ChecksumType::FullObject,
            part1_cksum: "L/E4WYn8v98=",
            part2_cksum: "xW1l19VobYM=",
            part3_cksum: "cK5MnNaWrW4=",
            composite_cksum: "i+6LR0y3eFo=",
        })
        .await;
    });
}

#[test]
fn test_multipart_use_cksum_helper_crc32() {
    s3_tests::run(async {
        run_multipart_checksum_test(&MultipartChecksumTestCase {
            algo: ChecksumAlgorithm::Crc32,
            cksum_type: ChecksumType::FullObject,
            part1_cksum: "JRTCyQ==",
            part2_cksum: "QoZTGg==",
            part3_cksum: "YAgjqw==",
            composite_cksum: "WgDhBQ==",
        })
        .await;
    });
}

#[test]
fn test_multipart_use_cksum_helper_crc32c() {
    s3_tests::run(async {
        run_multipart_checksum_test(&MultipartChecksumTestCase {
            algo: ChecksumAlgorithm::Crc32C,
            cksum_type: ChecksumType::FullObject,
            part1_cksum: "MDaLrw==",
            part2_cksum: "TH4EZg==",
            part3_cksum: "Z7mBIQ==",
            composite_cksum: "xU+Krw==",
        })
        .await;
    });
}

#[test]
fn test_multipart_use_cksum_helper_sha1() {
    s3_tests::run(async {
        run_multipart_checksum_test(&MultipartChecksumTestCase {
            algo: ChecksumAlgorithm::Sha1,
            cksum_type: ChecksumType::Composite,
            part1_cksum: "iIaTCGbm+vdVjNqIMF2S0T7ibMk=",
            part2_cksum: "LS/TJ32bAVKEwRu+sE3X7awh/lk=",
            part3_cksum: "6DDwovUaHwrKNXDMzOGbuvj9kxI=",
            composite_cksum: "sizjvY4eud3MrcHdZM3cQ/ol39o=-3",
        })
        .await;
    });
}

// ── test_get_object_part_with_checksum ────────────────────────────────
// Multipart CRC32 upload, then GET each part individually and verify
// data + per-part checksum header.

#[test]
fn test_get_object_part_with_checksum() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "partnum-cksum";

        // Create multipart upload with CRC32
        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let parts_data: Vec<Vec<u8>> = vec![
            vec![b'X'; PART_SIZE],
            vec![b'Y'; PART_SIZE],
            vec![b'Z'; 1024],
        ];

        // Upload parts with CRC32 checksums
        let mut completed_parts = Vec::new();
        let mut expected_checksums = Vec::new();
        for (i, data) in parts_data.iter().enumerate() {
            let part_number = (i + 1) as i32;
            let crc = checksum_crc32(data);
            let resp = client
                .upload_part()
                .bucket(&bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(part_number)
                .body(ByteStream::from(data.clone()))
                .checksum_algorithm(ChecksumAlgorithm::Crc32)
                .checksum_crc32(&crc)
                .send()
                .await
                .unwrap();
            let returned_crc = resp.checksum_crc32().unwrap().to_string();
            assert_eq!(returned_crc, crc);
            expected_checksums.push(crc.clone());
            completed_parts.push(
                CompletedPart::builder()
                    .e_tag(resp.e_tag().unwrap())
                    .part_number(part_number)
                    .checksum_crc32(&crc)
                    .build(),
            );
        }

        // Complete
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

        // GET each part and verify data + checksum (no ChecksumMode needed)
        for (i, data) in parts_data.iter().enumerate() {
            let part_number = (i + 1) as i32;
            let resp = client
                .get_object()
                .bucket(&bucket)
                .key(key)
                .part_number(part_number)
                .send()
                .await
                .unwrap();

            // Verify parts_count
            assert_eq!(
                resp.parts_count(),
                Some(3),
                "parts_count for part {}",
                part_number
            );

            // Extract checksum before consuming body
            let got_crc = resp
                .checksum_crc32()
                .expect("expected per-part CRC32 checksum without ENABLED")
                .to_string();

            // Verify data matches
            let body = resp.body.collect().await.unwrap().into_bytes();
            assert_eq!(
                body.len(),
                data.len(),
                "body length mismatch for part {}",
                part_number
            );
            assert_eq!(
                &body[..],
                &data[..],
                "data mismatch for part {}",
                part_number
            );

            // Verify per-part CRC32 checksum
            assert_eq!(
                got_crc, expected_checksums[i],
                "checksum mismatch for part {}",
                part_number
            );
        }

        cleanup(&bucket, &[key]).await;
    });
}

/// CRC32 of data, base64-encoded.
fn checksum_crc32(data: &[u8]) -> String {
    use base64::Engine;
    let crc = checksum::crc32::checksum(data);
    base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes())
}

// GetObjectAttributes checksum tests moved to object_attributes.rs
