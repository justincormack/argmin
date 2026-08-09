// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    CompletedMultipartUpload, CompletedPart, MetadataDirective, ObjectOwnership,
};
use s3_tests::{
    create_acl_enabled_bucket, is_retryable_operation_contention, SendRetryingOperationAborted, CTX,
};
use std::time::Duration;

const WEBSITE_REDIRECT_ACL_OPERATION_ATTEMPTS: usize = 20;

async fn put_object_retrying_operation_aborted(
    bucket: &str,
    key: &str,
    redirect: Option<&str>,
    body: Vec<u8>,
) -> aws_sdk_s3::operation::put_object::PutObjectOutput {
    for attempt in 0..WEBSITE_REDIRECT_ACL_OPERATION_ATTEMPTS {
        let mut put = CTX
            .client()
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(body.clone()));
        if let Some(redirect) = redirect {
            put = put.website_redirect_location(redirect);
        }
        match put.send().await {
            Ok(output) => return output,
            Err(err)
                if is_retryable_operation_contention(&err)
                    && attempt + 1 < WEBSITE_REDIRECT_ACL_OPERATION_ATTEMPTS =>
            {
                tokio::time::sleep(Duration::from_millis(10 * (attempt as u64 + 1))).await;
            }
            Err(err) => panic!("put object during website redirect ACL test: {err:?}"),
        }
    }
    unreachable!("put object retry loop must return on final attempt");
}

async fn upload_part_retrying_operation_aborted(
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    body: Vec<u8>,
) -> aws_sdk_s3::operation::upload_part::UploadPartOutput {
    for attempt in 0..WEBSITE_REDIRECT_ACL_OPERATION_ATTEMPTS {
        match CTX
            .client()
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(ByteStream::from(body.clone()))
            .send()
            .await
        {
            Ok(output) => return output,
            Err(err)
                if is_retryable_operation_contention(&err)
                    && attempt + 1 < WEBSITE_REDIRECT_ACL_OPERATION_ATTEMPTS =>
            {
                tokio::time::sleep(Duration::from_millis(10 * (attempt as u64 + 1))).await;
            }
            Err(err) => panic!("upload part during website redirect ACL test: {err:?}"),
        }
    }
    unreachable!("upload part retry loop must return on final attempt");
}

async fn setup_acl_bucket() -> String {
    create_acl_enabled_bucket(CTX.client(), ObjectOwnership::ObjectWriter).await
}

async fn cleanup(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = client
            .delete_object()
            .bucket(bucket)
            .key(*key)
            .send_retrying_operation_aborted("delete object during website redirect ACL cleanup")
            .await;
    }
    s3_tests::delete_bucket_retrying_operation_aborted(client, bucket).await;
}

async fn complete_single_part_multipart_upload_with_redirect(
    bucket: &str,
    key: &str,
    redirect: &str,
) {
    let create = CTX
        .client()
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .website_redirect_location(redirect)
        .send_retrying_operation_aborted(
            "create multipart upload during website redirect ACL setup",
        )
        .await
        .unwrap();
    let upload_id = create.upload_id().unwrap().to_string();

    let part = upload_part_retrying_operation_aborted(
        bucket,
        key,
        &upload_id,
        1,
        b"multipart-body".to_vec(),
    )
    .await;

    CTX.client()
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
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
        .send_retrying_operation_aborted(
            "complete multipart upload during website redirect ACL setup",
        )
        .await
        .unwrap();
}

#[test]
fn test_website_redirect_acl_put_object_round_trip() {
    s3_tests::run(async {
        let bucket = setup_acl_bucket().await;
        let key = "acl-put-redirect";
        let redirect = "/docs/acl-put.html";

        put_object_retrying_operation_aborted(
            &bucket,
            key,
            Some(redirect),
            b"redirect-body".to_vec(),
        )
        .await;

        let head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during website redirect test")
            .await
            .unwrap();
        assert_eq!(head.website_redirect_location(), Some(redirect));

        let get = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during website redirect test")
            .await
            .unwrap();
        assert_eq!(get.website_redirect_location(), Some(redirect));
        let body = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(&body[..], b"redirect-body");

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_website_redirect_acl_copy_object_explicit_redirect() {
    s3_tests::run(async {
        let bucket = setup_acl_bucket().await;
        let src_key = "acl-copy-src";
        let dst_key = "acl-copy-dst";
        let redirect = "/docs/acl-copy.html";

        put_object_retrying_operation_aborted(
            &bucket,
            src_key,
            Some("/docs/source.html"),
            b"copy-body".to_vec(),
        )
        .await;

        CTX.client()
            .copy_object()
            .bucket(&bucket)
            .key(dst_key)
            .copy_source(format!("{bucket}/{src_key}"))
            .metadata_directive(MetadataDirective::Copy)
            .website_redirect_location(redirect)
            .send_retrying_operation_aborted("copy object during website redirect ACL test")
            .await
            .unwrap();

        let src_head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key(src_key)
            .send_retrying_operation_aborted("S3 operation during website redirect test")
            .await
            .unwrap();
        assert_eq!(
            src_head.website_redirect_location(),
            Some("/docs/source.html")
        );

        let dst_head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key(dst_key)
            .send_retrying_operation_aborted("S3 operation during website redirect test")
            .await
            .unwrap();
        assert_eq!(dst_head.website_redirect_location(), Some(redirect));

        cleanup(&bucket, &[src_key, dst_key]).await;
    });
}

#[test]
fn test_website_redirect_acl_multipart_persists_from_initiation() {
    s3_tests::run(async {
        let bucket = setup_acl_bucket().await;
        let key = "acl-multipart-redirect";
        let redirect = "/docs/acl-multipart.html";

        complete_single_part_multipart_upload_with_redirect(&bucket, key, redirect).await;

        let head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during website redirect test")
            .await
            .unwrap();
        assert_eq!(head.website_redirect_location(), Some(redirect));

        cleanup(&bucket, &[key]).await;
    });
}
