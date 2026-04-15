use aws_sdk_s3::primitives::ByteStream;
use s3_http_tests::{create_bucket, run, unique_bucket, CTX};
use s3_tests::{assert_s3_err_code, err_status, sse_c_header_values, test_sse_c_key};

#[test]
fn test_sse_c_put_requires_https() {
    run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        create_bucket(client, &bucket).await.unwrap();

        let key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&key);
        let result = client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .sse_customer_algorithm("AES256")
            .sse_customer_key(key_b64)
            .sse_customer_key_md5(key_md5_b64)
            .body(ByteStream::from_static(b"secret"))
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidArgument");

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_sse_c_get_and_head_require_https() {
    run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        create_bucket(client, &bucket).await.unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"plain"))
            .send()
            .await
            .unwrap();

        let key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&key);

        let head = client
            .head_object()
            .bucket(&bucket)
            .key("obj")
            .sse_customer_algorithm("AES256")
            .sse_customer_key(key_b64.clone())
            .sse_customer_key_md5(key_md5_b64.clone())
            .send()
            .await;
        assert_eq!(err_status(&head), 400);

        let get = client
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .sse_customer_algorithm("AES256")
            .sse_customer_key(key_b64)
            .sse_customer_key_md5(key_md5_b64)
            .send()
            .await;
        assert_eq!(err_status(&get), 400);

        client
            .delete_object()
            .bucket(&bucket)
            .key("obj")
            .send()
            .await
            .unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_plain_http_without_sse_c_still_works() {
    run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        create_bucket(client, &bucket).await.unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"plain-http"))
            .send()
            .await
            .unwrap();

        let get = client
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send()
            .await
            .unwrap();
        assert_eq!(
            get.body.collect().await.unwrap().into_bytes().as_ref(),
            b"plain-http"
        );

        client
            .delete_object()
            .bucket(&bucket)
            .key("obj")
            .send()
            .await
            .unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}
