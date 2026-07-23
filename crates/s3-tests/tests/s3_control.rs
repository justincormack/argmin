use s3_tests::shape::{assert_shape, error_response_headers, shape, xml_tag_text, ShapeSpec};
use s3_tests::{
    build_configured_test_agent, build_test_agent, create_account_regional_bucket_with_credentials,
    delete_bucket_retrying_operation_aborted, presign_url_for_service_with_aws_signer_credentials,
    send_checked_signed_payload_request_for_service_with_credentials,
    send_signed_request_for_service_with_credentials, unique_account_regional_bucket,
    unique_bucket, PresignedRequest, RawResponse, SignedRequestCredentials, SigningPayload,
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

fn tag_resource_body(value: &str) -> String {
    format!(
        "<TagResourceRequest xmlns=\"http://awss3control.amazonaws.com/doc/2018-08-20/\">\
         <Tags><Tag><Key>payload</Key><Value>{value}</Value></Tag></Tags>\
         </TagResourceRequest>"
    )
}

fn payload_tag_value(response: &RawResponse) -> Option<&str> {
    (response.status == 200 && xml_tag_text(&response.body, "Key") == Some("payload"))
        .then(|| xml_tag_text(&response.body, "Value"))
        .flatten()
}

async fn wait_for_payload_tag_value(bucket: &str, expected: &str) {
    const REQUIRED_CONSECUTIVE_SUCCESSES: usize = 3;
    const MAX_ATTEMPTS: usize = 40;

    let mut consecutive_successes = 0;
    let mut last_response = None;
    for attempt in 0..MAX_ATTEMPTS {
        let response = list_tags_at(
            CTX.s3_control_endpoint(),
            bucket,
            CTX.account_id(),
            primary_credentials(),
        );
        if payload_tag_value(&response) == Some(expected) {
            consecutive_successes += 1;
            if consecutive_successes == REQUIRED_CONSECUTIVE_SUCCESSES {
                return;
            }
        } else {
            consecutive_successes = 0;
        }
        last_response = Some(response);
        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    panic!(
        "S3 Control tags for {bucket} did not converge on payload={expected}: {last_response:?}"
    );
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

fn send_presigned_s3_control(presigned: &PresignedRequest, body: &[u8]) -> RawResponse {
    let mut request = build_configured_test_agent(CTX.s3_control_endpoint(), CTX.tls_ca_pem())
        .post(presigned.uri());
    for (name, value) in presigned.headers() {
        request = request.header(name, value);
    }
    let mut response = request
        .send(body)
        .expect("presigned S3 Control transport error");
    let status = response.status().as_u16();
    let headers = response
        .headers()
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_string(),
                value
                    .to_str()
                    .expect("presigned S3 Control response header is valid UTF-8")
                    .to_string(),
            )
        })
        .collect();
    let body_read_error = response.body_read_error().map(ToOwned::to_owned);
    let body = response.body_mut().read_to_string().unwrap_or_default();
    RawResponse {
        status,
        headers,
        body,
        body_read_error,
    }
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

fn assert_s3_control_tag_success(label: &str, response: &RawResponse) {
    assert_shape(
        label,
        response,
        &shape()
            .status(204)
            .headers([
                ("x-amz-id-2", "{host_id}"),
                ("x-amz-request-id", "{request_id}"),
            ])
            .body_empty(),
    );
}

#[test]
fn test_s3_control_sigv4_payload_modes_match_aws() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        s3_tests::create_bucket(CTX.client(), &bucket)
            .await
            .unwrap();
        wait_for_s3_control_bucket(&bucket).await;

        let url = tags_url_at(CTX.s3_control_endpoint(), &bucket);
        let credentials = primary_credentials();
        let base_headers = [
            ("content-type", "application/xml"),
            ("x-amz-account-id", CTX.account_id()),
        ];

        let header_fixed_body = tag_resource_body("header-fixed");
        let header_fixed_hash = auth::canonical::sha256_hex(header_fixed_body.as_bytes());
        let response = send_checked_signed_payload_request_for_service_with_credentials(
            "POST",
            &url,
            header_fixed_body.as_bytes(),
            SigningPayload::Precomputed(header_fixed_hash.as_str()),
            base_headers,
            SigningService::S3Control,
            "s3",
            credentials,
        );
        assert_s3_control_tag_success(
            "S3 Control header auth with correct payload hash",
            &response,
        );
        wait_for_payload_tag_value(&bucket, "header-fixed").await;

        let header_missing_body = tag_resource_body("header-missing");
        let response = send_checked_signed_payload_request_for_service_with_credentials(
            "POST",
            &url,
            header_missing_body.as_bytes(),
            SigningPayload::BodyWithoutHeader(header_missing_body.as_bytes()),
            base_headers,
            SigningService::S3Control,
            "s3",
            credentials,
        );
        assert_s3_control_error(
            "S3 Control header auth without payload hash header",
            &response,
            400,
            "InvalidRequest",
            "Missing required header for this request: x-amz-content-sha256",
            "",
        );
        wait_for_payload_tag_value(&bucket, "header-fixed").await;

        let header_missing_wrong_service_body = tag_resource_body("header-missing-wrong-service");
        let response = send_checked_signed_payload_request_for_service_with_credentials(
            "POST",
            &url,
            header_missing_wrong_service_body.as_bytes(),
            SigningPayload::BodyWithoutHeader(header_missing_wrong_service_body.as_bytes()),
            base_headers,
            SigningService::S3Control,
            "sts",
            credentials,
        );
        assert_s3_control_error(
            "S3 Control header auth without payload hash header and with wrong service",
            &response,
            400,
            "InvalidRequest",
            "Missing required header for this request: x-amz-content-sha256",
            "",
        );
        wait_for_payload_tag_value(&bucket, "header-fixed").await;

        let bad_secret_key = "0".repeat(40);
        let bad_credentials = SignedRequestCredentials {
            secret_key: &bad_secret_key,
            ..credentials
        };
        let header_missing_bad_hmac_body = tag_resource_body("header-missing-bad-hmac");
        let response = send_checked_signed_payload_request_for_service_with_credentials(
            "POST",
            &url,
            header_missing_bad_hmac_body.as_bytes(),
            SigningPayload::BodyWithoutHeader(header_missing_bad_hmac_body.as_bytes()),
            base_headers,
            SigningService::S3Control,
            "s3",
            bad_credentials,
        );
        assert_s3_control_error(
            "S3 Control header auth without payload hash header and with bad HMAC",
            &response,
            400,
            "InvalidRequest",
            "Missing required header for this request: x-amz-content-sha256",
            "",
        );
        wait_for_payload_tag_value(&bucket, "header-fixed").await;

        let header_unsigned_body = tag_resource_body("header-unsigned");
        let response = send_checked_signed_payload_request_for_service_with_credentials(
            "POST",
            &url,
            header_unsigned_body.as_bytes(),
            SigningPayload::Unsigned,
            base_headers,
            SigningService::S3Control,
            "s3",
            credentials,
        );
        assert_s3_control_tag_success("S3 Control header auth with UNSIGNED-PAYLOAD", &response);
        wait_for_payload_tag_value(&bucket, "header-unsigned").await;

        let header_mismatch_signed_body = tag_resource_body("header-mismatch-signed");
        let header_mismatch_altered_body = tag_resource_body("header-mismatch-altered");
        let header_mismatch_hash =
            auth::canonical::sha256_hex(header_mismatch_signed_body.as_bytes());
        let response = send_checked_signed_payload_request_for_service_with_credentials(
            "POST",
            &url,
            header_mismatch_altered_body.as_bytes(),
            SigningPayload::Precomputed(header_mismatch_hash.as_str()),
            base_headers,
            SigningService::S3Control,
            "s3",
            credentials,
        );
        assert_s3_control_error(
            "S3 Control header auth with mismatched payload hash",
            &response,
            400,
            "XAmzContentSHA256Mismatch",
            "The provided 'x-amz-content-sha256' header does not match what was computed.",
            &format!(
                "<ClientComputedContentSHA256>{header_mismatch_hash}</ClientComputedContentSHA256>\
                 <S3ComputedContentSHA256>{}</S3ComputedContentSHA256>",
                auth::canonical::sha256_hex(header_mismatch_altered_body.as_bytes())
            ),
        );
        wait_for_payload_tag_value(&bucket, "header-unsigned").await;

        let presigned_fixed_body = tag_resource_body("presigned-fixed");
        let presigned_fixed_hash = auth::canonical::sha256_hex(presigned_fixed_body.as_bytes());
        let presigned_correct = presign_url_for_service_with_aws_signer_credentials(
            "POST",
            &url,
            Duration::from_secs(900),
            [
                ("content-type", "application/xml"),
                ("x-amz-account-id", CTX.account_id()),
                ("x-amz-content-sha256", presigned_fixed_hash.as_str()),
            ],
            SigningPayload::Precomputed(presigned_fixed_hash.as_str()),
            "s3",
            credentials,
        );
        let response =
            send_presigned_s3_control(&presigned_correct, presigned_fixed_body.as_bytes());
        assert_s3_control_tag_success("presigned S3 Control with correct payload hash", &response);
        wait_for_payload_tag_value(&bucket, "presigned-fixed").await;

        let presigned_missing_body = tag_resource_body("presigned-missing");
        let presigned_without_header = presign_url_for_service_with_aws_signer_credentials(
            "POST",
            &url,
            Duration::from_secs(900),
            base_headers,
            SigningPayload::Unsigned,
            "s3",
            credentials,
        );
        let response =
            send_presigned_s3_control(&presigned_without_header, presigned_missing_body.as_bytes());
        assert_s3_control_tag_success(
            "presigned S3 Control without payload hash header",
            &response,
        );
        wait_for_payload_tag_value(&bucket, "presigned-missing").await;

        let presigned_unsigned_body = tag_resource_body("presigned-unsigned");
        let presigned_unsigned = presign_url_for_service_with_aws_signer_credentials(
            "POST",
            &url,
            Duration::from_secs(900),
            [
                ("content-type", "application/xml"),
                ("x-amz-account-id", CTX.account_id()),
                ("x-amz-content-sha256", "UNSIGNED-PAYLOAD"),
            ],
            SigningPayload::Unsigned,
            "s3",
            credentials,
        );
        let response =
            send_presigned_s3_control(&presigned_unsigned, presigned_unsigned_body.as_bytes());
        assert_s3_control_tag_success("presigned S3 Control with UNSIGNED-PAYLOAD", &response);
        wait_for_payload_tag_value(&bucket, "presigned-unsigned").await;

        let presigned_mismatch_signed_body = tag_resource_body("presigned-mismatch-signed");
        let presigned_mismatch_altered_body = tag_resource_body("presigned-mismatch-altered");
        let presigned_mismatch_hash =
            auth::canonical::sha256_hex(presigned_mismatch_signed_body.as_bytes());
        let presigned_mismatch = presign_url_for_service_with_aws_signer_credentials(
            "POST",
            &url,
            Duration::from_secs(900),
            [
                ("content-type", "application/xml"),
                ("x-amz-account-id", CTX.account_id()),
                ("x-amz-content-sha256", presigned_mismatch_hash.as_str()),
            ],
            SigningPayload::Precomputed(presigned_mismatch_hash.as_str()),
            "s3",
            credentials,
        );
        let response = send_presigned_s3_control(
            &presigned_mismatch,
            presigned_mismatch_altered_body.as_bytes(),
        );
        assert_s3_control_error(
            "presigned S3 Control with mismatched payload hash",
            &response,
            400,
            "XAmzContentSHA256Mismatch",
            "The provided 'x-amz-content-sha256' header does not match what was computed.",
            &format!(
                "<ClientComputedContentSHA256>{presigned_mismatch_hash}</ClientComputedContentSHA256>\
                 <S3ComputedContentSHA256>{}</S3ComputedContentSHA256>",
                auth::canonical::sha256_hex(presigned_mismatch_altered_body.as_bytes())
            ),
        );
        wait_for_payload_tag_value(&bucket, "presigned-unsigned").await;

        delete_bucket_retrying_operation_aborted(CTX.client(), &bucket).await;
    });
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
