use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use s3_tests::{assert_s3_err_code, err_status, unique_bucket, SendRetryingOperationAborted, CTX};

fn owner_root_client() -> &'static aws_sdk_s3::Client {
    CTX.require_owner_root_client()
}

fn constrained_client() -> &'static aws_sdk_s3::Client {
    CTX.require_second_client()
}

async fn create_standard_bucket(client: &aws_sdk_s3::Client) -> String {
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    bucket
}

async fn get_object_body(client: &aws_sdk_s3::Client, bucket: &str, key: &str) -> Vec<u8> {
    client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send_retrying_operation_aborted("get object body in constrained write test")
        .await
        .unwrap()
        .body
        .collect()
        .await
        .unwrap()
        .into_bytes()
        .to_vec()
}

async fn cleanup_plain_bucket(
    root_client: &aws_sdk_s3::Client,
    constrained_client: &aws_sdk_s3::Client,
    bucket: &str,
    keys: &[&str],
) {
    for client in [root_client, constrained_client] {
        for key in keys {
            let _ = s3_tests::delete_object_retrying_operation_aborted(client, bucket, key).await;
        }
    }

    for client in [root_client, constrained_client] {
        if client
            .delete_bucket()
            .bucket(bucket)
            .send_retrying_operation_aborted("delete bucket during constrained write cleanup")
            .await
            .is_ok()
        {
            return;
        }
    }

    panic!("bucket cleanup delete failed for {bucket}");
}

#[test]
fn test_same_account_constrained_user_cannot_put_object() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let limited_client = constrained_client();
        let bucket = create_standard_bucket(root_client).await;

        let result = limited_client
            .put_object()
            .bucket(&bucket)
            .key("limited-write")
            .body(ByteStream::from_static(b"limited"))
            .send()
            .await;

        assert_eq!(err_status(&result), 403);
        assert_s3_err_code(&result, "AccessDenied");

        cleanup_plain_bucket(root_client, limited_client, &bucket, &[]).await;
    });
}

#[test]
fn test_same_account_constrained_user_cannot_overwrite_object() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let limited_client = constrained_client();
        let bucket = create_standard_bucket(root_client).await;

        root_client
            .put_object()
            .bucket(&bucket)
            .key("root-object")
            .body(ByteStream::from_static(b"root"))
            .send()
            .await
            .unwrap();

        let result = limited_client
            .put_object()
            .bucket(&bucket)
            .key("root-object")
            .body(ByteStream::from_static(b"limited"))
            .send()
            .await;

        assert_eq!(err_status(&result), 403);
        assert_s3_err_code(&result, "AccessDenied");
        let body = get_object_body(root_client, &bucket, "root-object").await;
        assert_eq!(body.as_slice(), b"root");

        cleanup_plain_bucket(root_client, limited_client, &bucket, &["root-object"]).await;
    });
}

#[test]
fn test_same_account_constrained_user_cannot_create_multipart_upload() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let limited_client = constrained_client();
        let bucket = create_standard_bucket(root_client).await;

        let result = limited_client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("limited-multipart")
            .send()
            .await;

        assert_eq!(err_status(&result), 403);
        assert_s3_err_code(&result, "AccessDenied");

        cleanup_plain_bucket(root_client, limited_client, &bucket, &[]).await;
    });
}

#[test]
fn test_same_account_constrained_user_cannot_upload_part() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let limited_client = constrained_client();
        let bucket = create_standard_bucket(root_client).await;
        let key = "root-created-multipart";

        let create = root_client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let result = limited_client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from_static(b"limited-part"))
            .send()
            .await;

        assert_eq!(err_status(&result), 403);
        assert_s3_err_code(&result, "AccessDenied");

        root_client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await
            .unwrap();
        cleanup_plain_bucket(root_client, limited_client, &bucket, &[]).await;
    });
}

#[test]
fn test_same_account_constrained_user_cannot_complete_multipart_upload() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let limited_client = constrained_client();
        let bucket = create_standard_bucket(root_client).await;
        let key = "root-complete-multipart";

        let create = root_client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();
        let part = root_client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from_static(b"root-part"))
            .send()
            .await
            .unwrap();

        let result = limited_client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .part_number(1)
                            .e_tag(part.e_tag().unwrap())
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await;

        assert_eq!(err_status(&result), 403);
        assert_s3_err_code(&result, "AccessDenied");

        root_client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await
            .unwrap();
        cleanup_plain_bucket(root_client, limited_client, &bucket, &[]).await;
    });
}
