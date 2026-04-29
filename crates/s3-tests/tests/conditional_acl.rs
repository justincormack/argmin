use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::ObjectOwnership;
use s3_tests::{assert_s3_err_code, create_acl_enabled_bucket, err_status, CTX};

async fn setup_acl_bucket() -> String {
    create_acl_enabled_bucket(CTX.client(), ObjectOwnership::ObjectWriter).await
}

async fn put_object(bucket: &str, key: &str, body: &'static [u8]) -> String {
    CTX.client()
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(body))
        .send()
        .await
        .unwrap()
        .e_tag()
        .unwrap()
        .to_string()
}

async fn cleanup(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = client.delete_object().bucket(bucket).key(*key).send().await;
    }
    client.delete_bucket().bucket(bucket).send().await.unwrap();
}

#[test]
fn test_conditional_acl_get_if_match() {
    s3_tests::run(async {
        let bucket = setup_acl_bucket().await;
        let key = "acl-get-if-match";
        let etag = put_object(&bucket, key, b"hello").await;

        let object = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key(key)
            .if_match(&etag)
            .send()
            .await
            .unwrap();
        let body = object.body.collect().await.unwrap().into_bytes();
        assert_eq!(&body[..], b"hello");

        let denied = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key(key)
            .if_match("\"0000000000000000\"")
            .send()
            .await;
        assert_eq!(err_status(&denied), 412);
        assert_s3_err_code(&denied, "PreconditionFailed");

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_conditional_acl_head_if_none_match() {
    s3_tests::run(async {
        let bucket = setup_acl_bucket().await;
        let key = "acl-head-if-none-match";
        let etag = put_object(&bucket, key, b"hello").await;

        let present = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key(key)
            .if_none_match("\"0000000000000000\"")
            .send()
            .await
            .unwrap();
        assert!(present.e_tag().is_some());

        let not_modified = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key(key)
            .if_none_match(etag)
            .send()
            .await;
        assert_eq!(err_status(&not_modified), 304);

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_conditional_acl_put_if_match_and_if_none_match() {
    s3_tests::run(async {
        let bucket = setup_acl_bucket().await;
        let key = "acl-put-conditional";
        let etag = put_object(&bucket, key, b"v1").await;

        CTX.client()
            .put_object()
            .bucket(&bucket)
            .key(key)
            .if_match(&etag)
            .body(ByteStream::from_static(b"v2"))
            .send()
            .await
            .unwrap();
        let object = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let body = object.body.collect().await.unwrap().into_bytes();
        assert_eq!(&body[..], b"v2");

        let overwrite = CTX
            .client()
            .put_object()
            .bucket(&bucket)
            .key(key)
            .if_none_match("*")
            .body(ByteStream::from_static(b"v3"))
            .send()
            .await;
        assert_eq!(err_status(&overwrite), 412);
        assert_s3_err_code(&overwrite, "PreconditionFailed");

        let create_key = "acl-put-create-only";
        CTX.client()
            .put_object()
            .bucket(&bucket)
            .key(create_key)
            .if_none_match("*")
            .body(ByteStream::from_static(b"created"))
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &[key, create_key]).await;
    });
}
