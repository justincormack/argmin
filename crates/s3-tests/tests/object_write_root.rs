use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use s3_tests::{unique_bucket, CTX};

fn owner_root_client() -> &'static aws_sdk_s3::Client {
    CTX.require_owner_root_client()
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
        .send()
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
    non_root_client: &aws_sdk_s3::Client,
    bucket: &str,
    keys: &[&str],
) {
    for client in [root_client, non_root_client] {
        for key in keys {
            let _ = client.delete_object().bucket(bucket).key(*key).send().await;
        }
    }

    for client in [root_client, non_root_client] {
        if client.delete_bucket().bucket(bucket).send().await.is_ok() {
            return;
        }
    }

    panic!("bucket cleanup delete failed for {bucket}");
}

#[test]
fn test_same_account_root_put_object_into_non_root_bucket() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let client = CTX.client();
        let bucket = create_standard_bucket(client).await;

        root_client
            .put_object()
            .bucket(&bucket)
            .key("root-write")
            .body(ByteStream::from_static(b"root-body"))
            .send()
            .await
            .unwrap();

        let body = get_object_body(client, &bucket, "root-write").await;
        assert_eq!(body.as_slice(), b"root-body");

        cleanup_plain_bucket(root_client, client, &bucket, &["root-write"]).await;
    });
}

#[test]
fn test_same_account_non_root_put_object_into_root_bucket() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let client = CTX.client();
        let bucket = create_standard_bucket(root_client).await;

        client
            .put_object()
            .bucket(&bucket)
            .key("non-root-write")
            .body(ByteStream::from_static(b"non-root-body"))
            .send()
            .await
            .unwrap();

        let body = get_object_body(root_client, &bucket, "non-root-write").await;
        assert_eq!(body.as_slice(), b"non-root-body");

        cleanup_plain_bucket(root_client, client, &bucket, &["non-root-write"]).await;
    });
}

#[test]
fn test_same_account_root_and_non_root_can_overwrite_each_others_objects() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let client = CTX.client();
        let bucket = create_standard_bucket(client).await;

        client
            .put_object()
            .bucket(&bucket)
            .key("cross-overwrite")
            .body(ByteStream::from_static(b"non-root-initial"))
            .send()
            .await
            .unwrap();
        root_client
            .put_object()
            .bucket(&bucket)
            .key("cross-overwrite")
            .body(ByteStream::from_static(b"root-overwrite"))
            .send()
            .await
            .unwrap();
        let root_overwrite_body = get_object_body(client, &bucket, "cross-overwrite").await;
        assert_eq!(root_overwrite_body.as_slice(), b"root-overwrite");

        root_client
            .put_object()
            .bucket(&bucket)
            .key("root-initial")
            .body(ByteStream::from_static(b"root-initial"))
            .send()
            .await
            .unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key("root-initial")
            .body(ByteStream::from_static(b"non-root-overwrite"))
            .send()
            .await
            .unwrap();
        let non_root_overwrite_body = get_object_body(root_client, &bucket, "root-initial").await;
        assert_eq!(non_root_overwrite_body.as_slice(), b"non-root-overwrite");

        cleanup_plain_bucket(
            root_client,
            client,
            &bucket,
            &["cross-overwrite", "root-initial"],
        )
        .await;
    });
}

#[test]
fn test_same_account_non_root_can_finish_root_multipart_upload() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let client = CTX.client();
        let bucket = create_standard_bucket(root_client).await;
        let key = "root-created-multipart";

        let create = root_client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let current_upload_id = create.upload_id().unwrap().to_string();

        let part = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&current_upload_id)
            .part_number(1)
            .body(ByteStream::from_static(b"non-root-part"))
            .send()
            .await
            .unwrap();

        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&current_upload_id)
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
            .await
            .unwrap();

        let body = get_object_body(root_client, &bucket, key).await;
        assert_eq!(body.as_slice(), b"non-root-part");

        cleanup_plain_bucket(root_client, client, &bucket, &[key]).await;
    });
}

#[test]
fn test_same_account_root_can_finish_non_root_multipart_upload() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let client = CTX.client();
        let bucket = create_standard_bucket(client).await;
        let key = "non-root-created-multipart";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let current_upload_id = create.upload_id().unwrap().to_string();

        let part = root_client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&current_upload_id)
            .part_number(1)
            .body(ByteStream::from_static(b"root-part"))
            .send()
            .await
            .unwrap();

        root_client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&current_upload_id)
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
            .await
            .unwrap();

        let body = get_object_body(client, &bucket, key).await;
        assert_eq!(body.as_slice(), b"root-part");

        cleanup_plain_bucket(root_client, client, &bucket, &[key]).await;
    });
}
