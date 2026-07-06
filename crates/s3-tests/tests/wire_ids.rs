use s3_tests::{
    aws_sdk_s3::primitives::ByteStream,
    build_client_with_ca, object_url, send_signed_request_with_credentials,
    shape::{
        assert_error_ids_match_headers, assert_response_id_shapes, response_header_value,
        xml_tag_text, HOST_ID_HEADER, REQUEST_ID_HEADER,
    },
    unique_bucket, RawResponse, SignedRequestCredentials, TestServer,
};

fn assert_error_wire_ids(operation: &str, response: &RawResponse) {
    assert!(
        response_header_value(response, REQUEST_ID_HEADER).is_some(),
        "{operation}: missing {REQUEST_ID_HEADER} header: {response:?}"
    );
    assert!(
        response_header_value(response, HOST_ID_HEADER).is_some(),
        "{operation}: missing {HOST_ID_HEADER} header: {response:?}"
    );
    assert!(
        xml_tag_text(&response.body, "RequestId").is_some(),
        "{operation}: missing RequestId in error XML: {response:?}"
    );
    assert!(
        xml_tag_text(&response.body, "HostId").is_some(),
        "{operation}: missing HostId in error XML: {response:?}"
    );
    assert_response_id_shapes(operation, response);
    assert_error_ids_match_headers(operation, response);
}

#[test]
fn test_local_error_request_ids_match_headers_for_missing_key_and_invalid_redirect() {
    s3_tests::run(async {
        let server = TestServer::start_https().await;
        let client = build_client_with_ca(
            server.endpoint(),
            s3_tests::server::TEST_ACCESS_KEY,
            s3_tests::server::TEST_SECRET_KEY,
            s3_tests::server::TEST_REGION,
            server.tls_ca_pem(),
        );
        let bucket = unique_bucket();
        client
            .create_bucket()
            .bucket(&bucket)
            .send()
            .await
            .expect("create local bucket");

        let creds = SignedRequestCredentials {
            access_key: s3_tests::server::TEST_ACCESS_KEY,
            secret_key: s3_tests::server::TEST_SECRET_KEY,
            region: s3_tests::server::TEST_REGION,
            tls_ca_pem: server.tls_ca_pem(),
        };

        let missing_key = send_signed_request_with_credentials(
            "GET",
            &object_url(server.endpoint(), &bucket, "missing-key.txt", None),
            b"",
            std::iter::empty::<(&str, &str)>(),
            creds,
        );
        assert_eq!(missing_key.status, 404);
        assert_error_wire_ids("GetObject missing key", &missing_key);

        let invalid_redirect = send_signed_request_with_credentials(
            "PUT",
            &object_url(server.endpoint(), &bucket, "invalid-redirect.txt", None),
            b"body",
            [("x-amz-website-redirect-location", "docs/landing.html")],
            SignedRequestCredentials {
                access_key: s3_tests::server::TEST_ACCESS_KEY,
                secret_key: s3_tests::server::TEST_SECRET_KEY,
                region: s3_tests::server::TEST_REGION,
                tls_ca_pem: server.tls_ca_pem(),
            },
        );
        assert_eq!(invalid_redirect.status, 400);
        assert_error_wire_ids("PutObject invalid redirect", &invalid_redirect);

        client
            .put_object()
            .bucket(&bucket)
            .key("present.txt")
            .body(ByteStream::from_static(b"present"))
            .send()
            .await
            .expect("put local object");
    });
}
