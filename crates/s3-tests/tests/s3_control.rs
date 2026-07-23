use s3_tests::shape::{assert_shape, error_response_headers, shape, ShapeSpec};
use s3_tests::{
    build_test_agent, create_account_regional_bucket_with_credentials,
    delete_bucket_retrying_operation_aborted, send_signed_request_for_service_with_credentials,
    unique_account_regional_bucket, unique_bucket, RawResponse, SignedRequestCredentials,
    SigningService, CTX,
};
use std::time::Duration;

fn percent_encode_path_segment(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut encoded = String::with_capacity(value.len());
    for &byte in value.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(char::from(byte));
            }
            _ => {
                encoded.push('%');
                encoded.push(char::from(HEX[usize::from(byte >> 4)]));
                encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
            }
        }
    }
    encoded
}

fn tags_url_at(endpoint: &str, bucket: &str) -> String {
    let resource = percent_encode_path_segment(&format!("arn:aws:s3:::{bucket}"));
    format!("{endpoint}/v20180820/tags/{resource}")
}

fn primary_credentials() -> SignedRequestCredentials<'static> {
    SignedRequestCredentials {
        access_key: CTX.access_key(),
        secret_key: CTX.secret_key(),
        region: CTX.region(),
        tls_ca_pem: CTX.tls_ca_pem(),
    }
}

fn list_tags_at(
    endpoint: &str,
    bucket: &str,
    account_id: &str,
    credentials: SignedRequestCredentials<'_>,
) -> RawResponse {
    send_signed_request_for_service_with_credentials(
        "GET",
        &tags_url_at(endpoint, bucket),
        &[],
        [("x-amz-account-id", account_id)],
        SigningService::S3Control,
        credentials,
    )
}

async fn wait_for_s3_control_bucket_at(
    endpoint: &str,
    bucket: &str,
    account_id: &str,
    credentials: SignedRequestCredentials<'_>,
) {
    const REQUIRED_CONSECUTIVE_SUCCESSES: usize = 3;
    const MAX_ATTEMPTS: usize = 40;

    let mut consecutive_successes = 0;
    let mut last_response = None;
    for attempt in 0..MAX_ATTEMPTS {
        let response = list_tags_at(endpoint, bucket, account_id, credentials);
        if response.status == 200 {
            consecutive_successes += 1;
            if consecutive_successes == REQUIRED_CONSECUTIVE_SUCCESSES {
                return;
            }
        } else {
            consecutive_successes = 0;
            last_response = Some(response);
        }
        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    panic!("S3 Control did not converge on existing bucket {bucket}: {last_response:?}");
}

async fn wait_for_s3_control_bucket(bucket: &str) {
    wait_for_s3_control_bucket_at(
        CTX.s3_control_endpoint(),
        bucket,
        CTX.account_id(),
        primary_credentials(),
    )
    .await;
}

fn options_at(endpoint: &str, bucket: &str) -> RawResponse {
    let timeout = std::env::var("S3_TEST_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .map_or(Duration::from_secs(30), Duration::from_secs);
    let agent = build_test_agent(endpoint, CTX.tls_ca_pem(), timeout);
    let mut response = agent
        .options(&tags_url_at(endpoint, bucket))
        .header("origin", "https://example.com")
        .header("access-control-request-method", "POST")
        .call()
        .expect("S3 Control OPTIONS transport error");
    let headers = response
        .headers()
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_string(),
                value
                    .to_str()
                    .expect("response header is valid UTF-8")
                    .to_string(),
            )
        })
        .collect();
    let (body, body_read_error) = match response.body_mut().read_to_string() {
        Ok(body) => (body, None),
        Err(error) => (String::new(), Some(error.to_string())),
    };
    RawResponse {
        status: response.status().as_u16(),
        headers,
        body,
        body_read_error,
    }
}

fn options(bucket: &str) -> RawResponse {
    options_at(CTX.s3_control_endpoint(), bucket)
}

fn s3_control_error_shape(status: u16, code: &str, message: &str, detail: &str) -> ShapeSpec {
    shape()
        .status(status)
        .headers(error_response_headers())
        .body(format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <ErrorResponse><Error><Code>{code}</Code><Message>{message}</Message>{detail}</Error>\
             <RequestId>{{request_id}}</RequestId><HostId>{{host_id}}</HostId></ErrorResponse>"
        ))
}

fn assert_s3_control_error(
    label: &str,
    response: &RawResponse,
    status: u16,
    code: &str,
    message: &str,
    detail: &str,
) {
    assert!(
        response
            .body
            .contains(&format!("<Code>{code}</Code><Message>{message}</Message>")),
        "{label}: unexpected S3 Control error: {response:?}"
    );
    assert_shape(
        label,
        response,
        &s3_control_error_shape(status, code, message, detail),
    );
}

#[test]
fn test_s3_control_unauthenticated_options_existing_and_missing_bucket_shapes() {
    s3_tests::run(async {
        let existing_bucket = unique_bucket();
        let missing_bucket = unique_bucket();
        s3_tests::create_bucket(CTX.client(), &existing_bucket)
            .await
            .unwrap();
        wait_for_s3_control_bucket(&existing_bucket).await;

        let existing = options(&existing_bucket);
        let missing = options(&missing_bucket);
        delete_bucket_retrying_operation_aborted(CTX.client(), &existing_bucket).await;

        assert_s3_control_error(
            "S3 Control OPTIONS existing bucket",
            &existing,
            403,
            "AccessForbidden",
            "CORSResponse: Bucket not found",
            "<Method>POST</Method><ResourceType>BUCKET</ResourceType>",
        );

        assert_s3_control_error(
            "S3 Control OPTIONS missing bucket",
            &missing,
            403,
            "AccessForbidden",
            "CORSResponse: Bucket not found",
            "<Method>POST</Method><ResourceType>BUCKET</ResourceType>",
        );
    });
}

#[test]
fn test_s3_control_options_does_not_reveal_foreign_account_regional_bucket_existence() {
    s3_tests::run(async {
        let existing_bucket = unique_account_regional_bucket();
        let missing_bucket = unique_account_regional_bucket();
        let created = create_account_regional_bucket_with_credentials(
            &existing_bucket,
            primary_credentials(),
        );
        assert_eq!(
            created.status, 200,
            "failed to create primary-account bucket: {created:#?}"
        );
        wait_for_s3_control_bucket(&existing_bucket).await;

        let existing = options_at(CTX.alt_s3_control_endpoint(), &existing_bucket);
        let missing = options_at(CTX.alt_s3_control_endpoint(), &missing_bucket);
        delete_bucket_retrying_operation_aborted(CTX.client(), &existing_bucket).await;

        for (label, response) in [("existing", &existing), ("missing", &missing)] {
            assert_s3_control_error(
                &format!("S3 Control OPTIONS {label} foreign account-regional bucket"),
                response,
                403,
                "AccessForbidden",
                "CORSResponse: Bucket not found",
                "<Method>POST</Method><ResourceType>BUCKET</ResourceType>",
            );
        }
    });
}
