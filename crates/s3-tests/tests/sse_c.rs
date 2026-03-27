use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use s3_tests::{
    assert_s3_err_code, err_status, sse_c_header_values, test_sse_c_key, unique_bucket, CTX,
};

macro_rules! with_sse_c_headers {
    ($op:expr, $key_b64:expr, $key_md5_b64:expr) => {{
        $op.customize().mutate_request({
            let key_b64 = $key_b64.clone();
            let key_md5_b64 = $key_md5_b64.clone();
            move |req| {
                req.headers_mut()
                    .insert("x-amz-server-side-encryption-customer-algorithm", "AES256");
                req.headers_mut()
                    .insert("x-amz-server-side-encryption-customer-key", key_b64.clone());
                req.headers_mut().insert(
                    "x-amz-server-side-encryption-customer-key-md5",
                    key_md5_b64.clone(),
                );
            }
        })
    }};
}

async fn cleanup(bucket: &str, key: &str) {
    let client = CTX.client();
    let _ = client.delete_object().bucket(bucket).key(key).send().await;
    client.delete_bucket().bucket(bucket).send().await.unwrap();
}

async fn cleanup_multipart(bucket: &str, key: &str, upload_id: &str) {
    let client = CTX.client();
    let _ = client
        .abort_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .send()
        .await;
    cleanup(bucket, key).await;
}

#[test]
fn test_sse_c_put_get_head_round_trip() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&key);
        let body = b"hello sse-c".to_vec();

        with_sse_c_headers!(
            client
                .put_object()
                .bucket(&bucket)
                .key("obj")
                .body(ByteStream::from(body.clone())),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();

        let head = with_sse_c_headers!(
            client.head_object().bucket(&bucket).key("obj"),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        assert_eq!(head.content_length(), Some(body.len() as i64));
        assert!(head.e_tag().is_some());
        assert_eq!(head.sse_customer_algorithm(), Some("AES256"));
        assert_eq!(head.sse_customer_key_md5(), Some(key_md5_b64.as_str()));

        let get = with_sse_c_headers!(
            client.get_object().bucket(&bucket).key("obj"),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        assert_eq!(get.sse_customer_algorithm(), Some("AES256"));
        assert_eq!(get.sse_customer_key_md5(), Some(key_md5_b64.as_str()));
        assert_eq!(
            get.body.collect().await.unwrap().into_bytes().as_ref(),
            body.as_slice()
        );

        cleanup(&bucket, "obj").await;
    });
}

#[test]
fn test_sse_c_get_requires_headers() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&key);

        with_sse_c_headers!(
            client
                .put_object()
                .bucket(&bucket)
                .key("obj")
                .body(ByteStream::from_static(b"secret")),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();

        let result = client.get_object().bucket(&bucket).key("obj").send().await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidRequest");

        cleanup(&bucket, "obj").await;
    });
}

#[test]
fn test_sse_c_head_requires_headers() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&key);

        with_sse_c_headers!(
            client
                .put_object()
                .bucket(&bucket)
                .key("obj")
                .body(ByteStream::from_static(b"secret")),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();

        let result = client.head_object().bucket(&bucket).key("obj").send().await;
        assert_eq!(err_status(&result), 400);

        cleanup(&bucket, "obj").await;
    });
}

#[test]
fn test_sse_c_get_rejects_wrong_key() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&key);
        let wrong_key = [42u8; 32];
        let (wrong_key_b64, wrong_key_md5_b64) = sse_c_header_values(&wrong_key);

        with_sse_c_headers!(
            client
                .put_object()
                .bucket(&bucket)
                .key("obj")
                .body(ByteStream::from_static(b"secret")),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();

        let result = with_sse_c_headers!(
            client.get_object().bucket(&bucket).key("obj"),
            wrong_key_b64,
            wrong_key_md5_b64
        )
        .send()
        .await;
        assert_eq!(err_status(&result), 403);
        assert_s3_err_code(&result, "AccessDenied");

        cleanup(&bucket, "obj").await;
    });
}

#[test]
fn test_sse_c_multipart_round_trip() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&key);
        let wrong_key = [42u8; 32];
        let (wrong_key_b64, wrong_key_md5_b64) = sse_c_header_values(&wrong_key);
        let body = b"hello multipart sse-c".to_vec();

        let create = with_sse_c_headers!(
            client.create_multipart_upload().bucket(&bucket).key("obj"),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let part = with_sse_c_headers!(
            client
                .upload_part()
                .bucket(&bucket)
                .key("obj")
                .upload_id(&upload_id)
                .part_number(1)
                .body(ByteStream::from(body.clone())),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        let etag = part.e_tag().unwrap().to_string();

        with_sse_c_headers!(
            client
                .complete_multipart_upload()
                .bucket(&bucket)
                .key("obj")
                .upload_id(&upload_id)
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .parts(CompletedPart::builder().e_tag(etag).part_number(1).build())
                        .build()
                ),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();

        let head = with_sse_c_headers!(
            client.head_object().bucket(&bucket).key("obj"),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        assert_eq!(head.content_length(), Some(body.len() as i64));
        assert_eq!(head.sse_customer_algorithm(), Some("AES256"));
        assert_eq!(head.sse_customer_key_md5(), Some(key_md5_b64.as_str()));

        let get = with_sse_c_headers!(
            client.get_object().bucket(&bucket).key("obj"),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        assert_eq!(get.sse_customer_algorithm(), Some("AES256"));
        assert_eq!(get.sse_customer_key_md5(), Some(key_md5_b64.as_str()));
        assert_eq!(
            get.body.collect().await.unwrap().into_bytes().as_ref(),
            body.as_slice()
        );

        let missing_headers = client.get_object().bucket(&bucket).key("obj").send().await;
        assert_eq!(err_status(&missing_headers), 400);
        assert_s3_err_code(&missing_headers, "InvalidRequest");

        let wrong_key_result = with_sse_c_headers!(
            client.get_object().bucket(&bucket).key("obj"),
            wrong_key_b64,
            wrong_key_md5_b64
        )
        .send()
        .await;
        assert_eq!(err_status(&wrong_key_result), 403);
        assert_s3_err_code(&wrong_key_result, "AccessDenied");

        cleanup(&bucket, "obj").await;
    });
}

#[test]
fn test_sse_c_upload_part_requires_headers() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&key);
        let create = with_sse_c_headers!(
            client.create_multipart_upload().bucket(&bucket).key("obj"),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let result = client
            .upload_part()
            .bucket(&bucket)
            .key("obj")
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from_static(b"secret"))
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidRequest");

        cleanup_multipart(&bucket, "obj", &upload_id).await;
    });
}
