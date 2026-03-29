use s3_tests::{
    build_client_with_ca, post_object_to_test_endpoint, sigv4_post_sse_c_fields_for_credentials,
    test_sse_c_key, unique_bucket, TestServer, RT,
};

fn run_local<F: std::future::Future>(f: F) -> F::Output {
    RT.block_on(f)
}

fn assert_error_code(body: &str, code: &str) {
    let expected = format!("<Code>{code}</Code>");
    assert!(
        body.contains(&expected),
        "expected {expected} in body, got {body}"
    );
}

#[test]
fn test_post_object_sse_c_requires_https() {
    run_local(async {
        let server = TestServer::start_http().await;
        let endpoint = server.endpoint().to_string();
        let client = build_client_with_ca(
            &endpoint,
            s3_tests::server::TEST_ACCESS_KEY,
            s3_tests::server::TEST_SECRET_KEY,
            s3_tests::server::TEST_REGION,
            None,
        )
        .await;
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let key = "post-sse-c-http";
        let file_data = b"insecure post sse-c";
        let customer_key = test_sse_c_key();
        let fields = sigv4_post_sse_c_fields_for_credentials(
            s3_tests::server::TEST_ACCESS_KEY,
            s3_tests::server::TEST_SECRET_KEY,
            s3_tests::server::TEST_REGION,
            &bucket,
            key,
            &customer_key,
        );
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let (status, body) = post_object_to_test_endpoint(
            &endpoint,
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
