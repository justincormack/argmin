use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart, ObjectOwnership};
use s3_tests::{
    create_acl_enabled_bucket, enable_bucket_sse_c, sse_c_header_values, test_sse_c_key,
    SendRetryingOperationAborted, CTX,
};

const MULTIPART_MIN_PART_SIZE: usize = 5 * 1024 * 1024;

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

fn require_https_endpoint() {
    assert!(
        CTX.endpoint().starts_with("https://"),
        "SSE-C coverage requires an https:// endpoint; got {}",
        CTX.endpoint()
    );
}

fn patterned_bytes(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| seed.wrapping_add((i % 251) as u8))
        .collect()
}

async fn create_acl_sse_c_bucket() -> String {
    let client = CTX.client();
    let bucket = create_acl_enabled_bucket(client, ObjectOwnership::ObjectWriter).await;
    enable_bucket_sse_c(client, &bucket).await;
    bucket
}

async fn cleanup(bucket: &str, key: &str) {
    let client = CTX.client();
    let _ = client
        .delete_object()
        .bucket(bucket)
        .key(key)
        .send_retrying_operation_aborted("delete SSE-C ACL cleanup object")
        .await;
    s3_tests::delete_bucket_retrying_operation_aborted(client, bucket).await;
}

#[test]
fn test_sse_c_acl_mode_put_get_head_round_trip() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_acl_sse_c_bucket().await;
        let key = "acl-sse-c-single";
        let body = patterned_bytes(1024, 0x41);
        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);

        with_sse_c_headers!(
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from(body.clone())),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();

        let head = with_sse_c_headers!(
            client.head_object().bucket(&bucket).key(key),
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
            client.get_object().bucket(&bucket).key(key),
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

        cleanup(&bucket, key).await;
    });
}

#[test]
fn test_sse_c_acl_mode_range_get() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_acl_sse_c_bucket().await;
        let key = "acl-sse-c-range";
        let body = patterned_bytes(4096, 0x52);
        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);

        with_sse_c_headers!(
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from(body.clone())),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();

        let get = with_sse_c_headers!(
            client
                .get_object()
                .bucket(&bucket)
                .key(key)
                .range("bytes=10-29"),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        assert_eq!(get.content_range(), Some("bytes 10-29/4096"));
        assert_eq!(
            get.body.collect().await.unwrap().into_bytes().as_ref(),
            &body[10..30]
        );

        cleanup(&bucket, key).await;
    });
}

#[test]
fn test_sse_c_acl_mode_multipart_round_trip() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_acl_sse_c_bucket().await;
        let key = "acl-sse-c-multipart";
        let body = patterned_bytes(MULTIPART_MIN_PART_SIZE, 0x63);
        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);

        let create = with_sse_c_headers!(
            client.create_multipart_upload().bucket(&bucket).key(key),
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
                .key(key)
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
                .key(key)
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

        let get = with_sse_c_headers!(
            client.get_object().bucket(&bucket).key(key),
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

        cleanup(&bucket, key).await;
    });
}
