use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    CompletedMultipartUpload, CompletedPart, MetadataDirective, ObjectOwnership,
};
use s3_tests::{create_acl_enabled_bucket, CTX};

async fn setup_acl_bucket() -> String {
    create_acl_enabled_bucket(CTX.client(), ObjectOwnership::ObjectWriter).await
}

async fn cleanup(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = client.delete_object().bucket(bucket).key(*key).send().await;
    }
    client.delete_bucket().bucket(bucket).send().await.unwrap();
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
        .send()
        .await
        .unwrap();
    let upload_id = create.upload_id().unwrap().to_string();

    let part = CTX
        .client()
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"multipart-body"))
        .send()
        .await
        .unwrap();

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
        .send()
        .await
        .unwrap();
}

#[test]
fn test_website_redirect_acl_put_object_round_trip() {
    s3_tests::run(async {
        let bucket = setup_acl_bucket().await;
        let key = "acl-put-redirect";
        let redirect = "/docs/acl-put.html";

        CTX.client()
            .put_object()
            .bucket(&bucket)
            .key(key)
            .website_redirect_location(redirect)
            .body(ByteStream::from_static(b"redirect-body"))
            .send()
            .await
            .unwrap();

        let head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(head.website_redirect_location(), Some(redirect));

        let get = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
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

        CTX.client()
            .put_object()
            .bucket(&bucket)
            .key(src_key)
            .website_redirect_location("/docs/source.html")
            .body(ByteStream::from_static(b"copy-body"))
            .send()
            .await
            .unwrap();

        CTX.client()
            .copy_object()
            .bucket(&bucket)
            .key(dst_key)
            .copy_source(format!("{bucket}/{src_key}"))
            .metadata_directive(MetadataDirective::Copy)
            .website_redirect_location(redirect)
            .send()
            .await
            .unwrap();

        let src_head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key(src_key)
            .send()
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
            .send()
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
            .send()
            .await
            .unwrap();
        assert_eq!(head.website_redirect_location(), Some(redirect));

        cleanup(&bucket, &[key]).await;
    });
}
