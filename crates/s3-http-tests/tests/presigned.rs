use std::time::Duration;

use aws_sdk_s3::presigning::PresigningConfig;
use s3_http_tests::{run, test_agent, unique_bucket, CTX};
use s3_tests::{sse_c_header_values, test_sse_c_key};

macro_rules! with_presigned_headers {
    ($req:expr, $presigned:expr) => {{
        let mut req = $req;
        for (name, value) in $presigned.headers() {
            req = req.header(name, value);
        }
        req
    }};
}

#[test]
fn test_presigned_sse_c_put_requires_https() {
    run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let body = b"presigned insecure sse-c put";
        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);

        let presign_config = PresigningConfig::expires_in(Duration::from_secs(900)).unwrap();
        let presigned = client
            .put_object()
            .bucket(&bucket)
            .key("uploaded-sse-c-http")
            .sse_customer_algorithm("AES256")
            .sse_customer_key(key_b64)
            .sse_customer_key_md5(key_md5_b64)
            .presigned(presign_config)
            .await
            .unwrap();

        let mut resp = with_presigned_headers!(test_agent().put(presigned.uri()), presigned)
            .send(&body[..])
            .expect("transport error");
        assert_eq!(resp.status().as_u16(), 400);
        let body = resp.body_mut().read_to_string().unwrap_or_default();
        assert!(
            body.contains("<Code>InvalidArgument</Code>"),
            "expected InvalidArgument, got {body}"
        );

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}
