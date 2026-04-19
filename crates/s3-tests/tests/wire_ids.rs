use s3_tests::{
    aws_sdk_s3::primitives::ByteStream, build_client_with_ca, object_url,
    send_signed_request_with_credentials, unique_bucket, RawResponse, SignedRequestCredentials,
    TestServer,
};

fn response_header_value<'a>(response: &'a RawResponse, name: &str) -> Option<&'a str> {
    response
        .headers
        .iter()
        .find(|(header_name, _)| header_name.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

fn xml_tag_text<'a>(body: &'a str, tag: &str) -> Option<&'a str> {
    let start_tag = format!("<{tag}>");
    let end_tag = format!("</{tag}>");
    let start = body.find(&start_tag)? + start_tag.len();
    let end = body[start..].find(&end_tag)? + start;
    Some(&body[start..end])
}

fn assert_error_ids_match_headers(response: &RawResponse) {
    assert_eq!(
        xml_tag_text(&response.body, "RequestId"),
        response_header_value(response, "x-amz-request-id"),
        "RequestId XML/header mismatch: {response:?}"
    );
    assert_eq!(
        xml_tag_text(&response.body, "HostId"),
        response_header_value(response, "x-amz-id-2"),
        "HostId XML/header mismatch: {response:?}"
    );
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
        assert_error_ids_match_headers(&missing_key);

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
        assert_error_ids_match_headers(&invalid_redirect);

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
