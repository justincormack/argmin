use s3_http_tests::{create_bucket, run, unique_bucket, CTX};
use s3_tests::{
    post_object_to_test_endpoint, sigv4_post_sse_c_fields_for_credentials, test_sse_c_key,
};

fn assert_error_code(body: &str, code: &str) {
    let expected = format!("<Code>{code}</Code>");
    assert!(
        body.contains(&expected),
        "expected {expected} in body, got {body}"
    );
}

#[test]
fn test_post_object_sse_c_requires_https() {
    run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        create_bucket(client, &bucket).await.unwrap();

        let key = "post-sse-c-http";
        let file_data = b"insecure post sse-c";
        let customer_key = test_sse_c_key();
        let fields = sigv4_post_sse_c_fields_for_credentials(
            CTX.access_key(),
            CTX.secret_key(),
            CTX.region(),
            &bucket,
            key,
            &customer_key,
        );
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let (status, body) = post_object_to_test_endpoint(
            CTX.endpoint(),
            None,
            &bucket,
            &field_refs,
            file_data,
            "test.txt",
        );
        assert_eq!(status, 400, "expected 400, got {} body={}", status, body);
        assert_error_code(&body, "InvalidArgument");

        let _ = client.delete_object().bucket(&bucket).key(key).send().await;
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}
