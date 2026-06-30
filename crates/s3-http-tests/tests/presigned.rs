use std::time::Duration;

use s3_http_tests::{create_bucket, run, test_agent, unique_bucket, CTX};
use s3_tests::{
    delete_bucket_retrying_operation_aborted, object_url, presign_url_with_credentials,
    sse_c_header_values, test_sse_c_key, SignedRequestCredentials,
};

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
        create_bucket(client, &bucket).await.unwrap();

        let body = b"presigned insecure sse-c put";
        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);

        let presigned = presign_url_with_credentials(
            "PUT",
            &object_url(CTX.endpoint(), &bucket, "uploaded-sse-c-http", None),
            Duration::from_secs(900),
            [
                ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
                (
                    "x-amz-server-side-encryption-customer-key",
                    key_b64.as_str(),
                ),
                (
                    "x-amz-server-side-encryption-customer-key-md5",
                    key_md5_b64.as_str(),
                ),
            ],
            None,
            SignedRequestCredentials {
                access_key: CTX.access_key(),
                secret_key: CTX.secret_key(),
                region: CTX.region(),
                tls_ca_pem: None,
            },
        );

        let mut resp = with_presigned_headers!(test_agent().put(presigned.uri()), presigned)
            .send(&body[..])
            .expect("transport error");
        assert_eq!(resp.status().as_u16(), 400);
        let body = resp.body_mut().read_to_string().unwrap_or_default();
        assert!(
            body.contains("<Code>InvalidArgument</Code>"),
            "expected InvalidArgument, got {body}"
        );

        delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}
