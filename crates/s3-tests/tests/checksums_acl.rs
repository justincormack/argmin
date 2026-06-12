use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    ChecksumAlgorithm, ChecksumMode, ChecksumType, CompletedMultipartUpload, CompletedPart,
    ObjectAttributes, ObjectOwnership,
};
use s3_tests::{create_acl_enabled_bucket, SendRetryingOperationAborted, CTX};

const PART_SIZE: usize = 5 * 1024 * 1024;
const SHA256_1K_A: &str = "arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0=";

fn body_1k() -> Vec<u8> {
    vec![b'A'; 1024]
}

async fn setup_acl_bucket() -> String {
    create_acl_enabled_bucket(CTX.client(), ObjectOwnership::ObjectWriter).await
}

async fn cleanup(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = s3_tests::delete_object_retrying_operation_aborted(client, bucket, key).await;
    }
    s3_tests::delete_bucket_retrying_operation_aborted(client, bucket).await;
}

fn completed_part_with_crc32(etag: &str, part_number: i32, checksum: &str) -> CompletedPart {
    CompletedPart::builder()
        .e_tag(etag)
        .part_number(part_number)
        .checksum_crc32(checksum)
        .build()
}

#[test]
fn test_object_checksum_acl_sha256_put_head() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_acl_bucket().await;
        let key = "acl-object-checksum";

        let put = s3_tests::retrying_operation_aborted("put ACL checksum object", || {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from(body_1k()))
                .checksum_algorithm(ChecksumAlgorithm::Sha256)
                .checksum_sha256(SHA256_1K_A)
                .send()
        })
        .await;
        assert_eq!(put.checksum_sha256(), Some(SHA256_1K_A));

        let plain_head = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("head ACL checksum object without checksum mode")
            .await
            .unwrap();
        assert!(
            plain_head.checksum_sha256().is_none(),
            "expected no checksum on plain HEAD"
        );

        let checksum_head = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .checksum_mode(ChecksumMode::Enabled)
            .send_retrying_operation_aborted("head ACL checksum object with checksum mode")
            .await
            .unwrap();
        assert_eq!(checksum_head.checksum_sha256(), Some(SHA256_1K_A));

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_multipart_checksum_acl_crc32_round_trip() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_acl_bucket().await;
        let key = "acl-multipart-checksum";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .checksum_type(ChecksumType::FullObject)
            .send_retrying_operation_aborted("create ACL checksum multipart upload")
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let parts_data: [(&[u8], &str); 3] = [
            (&[b'A'; PART_SIZE][..], "JRTCyQ=="),
            (&[b'B'; PART_SIZE][..], "QoZTGg=="),
            (&[b'C'; PART_SIZE][..], "YAgjqw=="),
        ];

        let mut completed_parts = Vec::new();
        for (i, (data, checksum)) in parts_data.iter().enumerate() {
            let part_number = (i + 1) as i32;
            let body = data.to_vec();
            let part =
                s3_tests::retrying_operation_aborted("upload ACL checksum multipart part", || {
                    client
                        .upload_part()
                        .bucket(&bucket)
                        .key(key)
                        .upload_id(upload_id)
                        .part_number(part_number)
                        .body(ByteStream::from(body.clone()))
                        .checksum_algorithm(ChecksumAlgorithm::Crc32)
                        .checksum_crc32(*checksum)
                        .send()
                })
                .await;
            assert_eq!(part.checksum_crc32(), Some(*checksum));
            completed_parts.push(completed_part_with_crc32(
                part.e_tag().unwrap(),
                part_number,
                checksum,
            ));
        }

        let complete = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(completed_parts))
                    .build(),
            )
            .send_retrying_operation_aborted("complete ACL checksum multipart upload")
            .await
            .unwrap();
        assert_eq!(complete.checksum_crc32(), Some("WgDhBQ=="));

        let head = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .checksum_mode(ChecksumMode::Enabled)
            .send_retrying_operation_aborted("head completed ACL checksum multipart object")
            .await
            .unwrap();
        assert_eq!(head.checksum_crc32(), Some("WgDhBQ=="));
        assert_eq!(head.checksum_type(), Some(&ChecksumType::FullObject));

        let attributes = client
            .get_object_attributes()
            .bucket(&bucket)
            .key(key)
            .object_attributes(ObjectAttributes::Checksum)
            .send_retrying_operation_aborted("get ACL checksum object attributes")
            .await
            .unwrap();
        let checksum = attributes.checksum().expect("expected checksum");
        assert_eq!(checksum.checksum_crc32(), Some("WgDhBQ=="));
        assert_eq!(checksum.checksum_type(), Some(&ChecksumType::FullObject));

        cleanup(&bucket, &[key]).await;
    });
}
