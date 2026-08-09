// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use aws_smithy_types::{date_time::Format as DateTimeFormat, DateTime};
use s3_tests::{
    build_configured_test_agent, presign_url_for_service_with_aws_signer_credentials,
    send_checked_signed_payload_request_for_service_with_credentials,
    send_checked_signed_request_for_service_with_credentials,
    shape::{assert_shape, response_header_value, shape, xml_tag_text},
    RawResponse, SigningPayload, SigningService,
};
use sts_tests::CTX;

const STS_XMLNS: &str = "https://sts.amazonaws.com/doc/2011-06-15/";
const QUERY_CONTENT_TYPE: &str = "application/x-www-form-urlencoded";
const SIGNATURE_MISMATCH_MESSAGE: &str = "The request signature we calculated does not match the signature you provided. Check your AWS Secret Access Key and signing method. Consult the service documentation for details.";

#[test]
fn assume_role_uses_default_duration() {
    let session_name = "argmin-default-duration";
    assert_assume_role_success(
        session_name,
        &[
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", CTX.role_arn()),
            ("RoleSessionName", session_name),
        ],
        3_600,
    );
}

#[test]
fn assume_role_accepts_explicit_duration() {
    let session_name = "argmin-explicit-duration";
    assert_assume_role_success(
        session_name,
        &[
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", CTX.role_arn()),
            ("RoleSessionName", session_name),
            ("DurationSeconds", "900"),
        ],
        900,
    );
}

#[test]
fn assume_role_uses_first_scalar_parameter_values() {
    let session_name = "argmin-first-values";
    assert_assume_role_success(
        session_name,
        &[
            ("Action", "AssumeRole"),
            ("Action", "NoSuchAction"),
            ("Version", "2011-06-15"),
            ("Version", "2000-01-01"),
            ("RoleArn", CTX.role_arn()),
            ("RoleArn", "not-an-arn"),
            ("RoleSessionName", session_name),
            ("RoleSessionName", "bad/name"),
            ("DurationSeconds", "900"),
            ("DurationSeconds", "899"),
        ],
        900,
    );
}

#[test]
fn assume_role_validates_role_session_name() {
    let long_session_name = "a".repeat(65);
    let short_invalid_session_name = "!";
    let long_invalid_session_name = "/".repeat(65);
    let multibyte_short_invalid_session_name = "é";
    let supplementary_short_invalid_session_name = "😀";
    let long_session_message = format!(
        "1 validation error detected: Value '{long_session_name}' at 'roleSessionName' failed to satisfy constraint: Member must have length less than or equal to 64"
    );
    let short_invalid_session_message = format!(
        "2 validation errors detected: Value '{short_invalid_session_name}' at 'roleSessionName' failed to satisfy constraint: Member must satisfy regular expression pattern: [\\w+=,.@-]*; Value '{short_invalid_session_name}' at 'roleSessionName' failed to satisfy constraint: Member must have length greater than or equal to 2"
    );
    let long_invalid_session_message = format!(
        "2 validation errors detected: Value '{long_invalid_session_name}' at 'roleSessionName' failed to satisfy constraint: Member must satisfy regular expression pattern: [\\w+=,.@-]*; Value '{long_invalid_session_name}' at 'roleSessionName' failed to satisfy constraint: Member must have length less than or equal to 64"
    );
    let multibyte_short_invalid_session_message = format!(
        "2 validation errors detected: Value '{multibyte_short_invalid_session_name}' at 'roleSessionName' failed to satisfy constraint: Member must satisfy regular expression pattern: [\\w+=,.@-]*; Value '{multibyte_short_invalid_session_name}' at 'roleSessionName' failed to satisfy constraint: Member must have length greater than or equal to 2"
    );
    let supplementary_short_invalid_session_message = format!(
        "2 validation errors detected: Value '{supplementary_short_invalid_session_name}' at 'roleSessionName' failed to satisfy constraint: Member must satisfy regular expression pattern: [\\w+=,.@-]*; Value '{supplementary_short_invalid_session_name}' at 'roleSessionName' failed to satisfy constraint: Member must have length greater than or equal to 2"
    );
    let cases = [
        (
            "missing RoleSessionName",
            None,
            "1 validation error detected: Value null at 'roleSessionName' failed to satisfy constraint: Member must not be null",
        ),
        (
            "empty RoleSessionName",
            Some(""),
            "1 validation error detected: Value '' at 'roleSessionName' failed to satisfy constraint: Member must have length greater than or equal to 2",
        ),
        (
            "one-character RoleSessionName",
            Some("a"),
            "1 validation error detected: Value 'a' at 'roleSessionName' failed to satisfy constraint: Member must have length greater than or equal to 2",
        ),
        (
            "invalid-character RoleSessionName",
            Some("bad/name"),
            r"1 validation error detected: Value 'bad/name' at 'roleSessionName' failed to satisfy constraint: Member must satisfy regular expression pattern: [\w+=,.@-]*",
        ),
        (
            "overlong RoleSessionName",
            Some(long_session_name.as_str()),
            long_session_message.as_str(),
        ),
        (
            "short invalid-character RoleSessionName",
            Some(short_invalid_session_name),
            short_invalid_session_message.as_str(),
        ),
        (
            "overlong invalid-character RoleSessionName",
            Some(long_invalid_session_name.as_str()),
            long_invalid_session_message.as_str(),
        ),
        (
            "multibyte short invalid-character RoleSessionName",
            Some(multibyte_short_invalid_session_name),
            multibyte_short_invalid_session_message.as_str(),
        ),
        (
            "supplementary short invalid-character RoleSessionName",
            Some(supplementary_short_invalid_session_name),
            supplementary_short_invalid_session_message.as_str(),
        ),
    ];

    for (label, role_session_name, message) in cases {
        let mut parameters = vec![
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", CTX.role_arn()),
        ];
        if let Some(role_session_name) = role_session_name {
            parameters.push(("RoleSessionName", role_session_name));
        }
        assert_assume_role_error(label, &parameters, 400, "ValidationError", message);
    }
}

#[test]
fn assume_role_validates_role_session_name_before_role_resolution_and_trust() {
    let message = r"1 validation error detected: Value 'bad/name' at 'roleSessionName' failed to satisfy constraint: Member must satisfy regular expression pattern: [\w+=,.@-]*";
    let missing_role_arn = format!("{}-missing", CTX.role_arn());
    for (label, role_arn) in [
        (
            "invalid RoleSessionName with trust-denied role",
            CTX.denied_role_arn(),
        ),
        (
            "invalid RoleSessionName with unknown role",
            missing_role_arn.as_str(),
        ),
    ] {
        assert_assume_role_error(
            label,
            &[
                ("Action", "AssumeRole"),
                ("Version", "2011-06-15"),
                ("RoleArn", role_arn),
                ("RoleSessionName", "bad/name"),
            ],
            400,
            "ValidationError",
            message,
        );
    }
}

#[test]
fn assume_role_accepts_role_session_name_boundaries() {
    for role_session_name in ["aa".to_string(), "b".repeat(64)] {
        assert_assume_role_success(
            &role_session_name,
            &[
                ("Action", "AssumeRole"),
                ("Version", "2011-06-15"),
                ("RoleArn", CTX.role_arn()),
                ("RoleSessionName", &role_session_name),
            ],
            3_600,
        );
    }
}

#[test]
fn assume_role_requires_sigv4_payload_binding() {
    let original_session_name = "argmin-payload-original";
    let altered_session_name = "argmin-payload-altered";
    let original_body = assume_role_body(original_session_name);
    let altered_body = assume_role_body(altered_session_name);
    let original_hash = auth::canonical::sha256_hex(original_body.as_bytes());
    let endpoint = format!("{}/", CTX.endpoint());

    let explicit_hash = send_checked_signed_payload_request_for_service_with_credentials(
        "POST",
        &endpoint,
        original_body.as_bytes(),
        SigningPayload::Precomputed(&original_hash),
        [("content-type", QUERY_CONTENT_TYPE)],
        SigningService::Sts,
        "sts",
        CTX.credentials(),
    );
    assert_assume_role_success_response(
        "AssumeRole with explicit payload hash",
        explicit_hash,
        original_session_name,
        3_600,
    );

    for (label, signing_payload, transmitted_body) in [
        (
            "AssumeRole with UNSIGNED-PAYLOAD",
            SigningPayload::Unsigned,
            original_body.as_bytes(),
        ),
        (
            "AssumeRole with mismatched explicit payload hash",
            SigningPayload::Precomputed(&original_hash),
            altered_body.as_bytes(),
        ),
    ] {
        let response = send_checked_signed_payload_request_for_service_with_credentials(
            "POST",
            &endpoint,
            transmitted_body,
            signing_payload,
            [("content-type", QUERY_CONTENT_TYPE)],
            SigningService::Sts,
            "sts",
            CTX.credentials(),
        );
        assert_assume_role_response_error(
            label,
            &response,
            403,
            "SignatureDoesNotMatch",
            SIGNATURE_MISMATCH_MESSAGE,
        );
    }

    let presigned_explicit_hash = presign_url_for_service_with_aws_signer_credentials(
        "POST",
        &endpoint,
        Duration::from_secs(900),
        [
            ("content-type", QUERY_CONTENT_TYPE),
            ("x-amz-content-sha256", original_hash.as_str()),
        ],
        SigningPayload::Precomputed(&original_hash),
        "sts",
        CTX.credentials(),
    );
    let response = send_presigned_assume_role(&presigned_explicit_hash, &original_body);
    assert_assume_role_success_response(
        "presigned AssumeRole with explicit payload hash",
        response,
        original_session_name,
        3_600,
    );

    let presigned_unsigned = presign_url_for_service_with_aws_signer_credentials(
        "POST",
        &endpoint,
        Duration::from_secs(900),
        [("content-type", QUERY_CONTENT_TYPE)],
        SigningPayload::Unsigned,
        "sts",
        CTX.credentials(),
    );
    for (label, presigned, transmitted_body) in [
        (
            "presigned AssumeRole with unsigned payload",
            &presigned_unsigned,
            original_body.as_str(),
        ),
        (
            "presigned AssumeRole with mismatched explicit payload hash",
            &presigned_explicit_hash,
            altered_body.as_str(),
        ),
    ] {
        let response = send_presigned_assume_role(presigned, transmitted_body);
        assert_assume_role_response_error(
            label,
            &response,
            403,
            "SignatureDoesNotMatch",
            SIGNATURE_MISMATCH_MESSAGE,
        );
    }
}

fn assert_assume_role_success(
    role_session_name: &str,
    parameters: &[(&str, &str)],
    expected_duration_seconds: i64,
) {
    let response = send_assume_role(parameters);

    assert_assume_role_success_response(
        &format!("AssumeRole {role_session_name}"),
        response,
        role_session_name,
        expected_duration_seconds,
    );
}

fn assert_assume_role_success_response(
    label: &str,
    response: RawResponse,
    role_session_name: &str,
    expected_duration_seconds: i64,
) {
    assert_issued_credential_shapes(label, &response);
    assert_expiration(label, &response, expected_duration_seconds);
    assert_success_wire_shape(label, response, role_session_name);
}

fn assert_assume_role_error(
    label: &str,
    parameters: &[(&str, &str)],
    status: u16,
    code: &str,
    message: &str,
) {
    let response = send_assume_role(parameters);
    assert_assume_role_response_error(label, &response, status, code, message);
}

fn assert_assume_role_response_error(
    label: &str,
    response: &RawResponse,
    status: u16,
    code: &str,
    message: &str,
) {
    let request_id = required_response_header(response, "x-amzn-requestid", label);
    let extended_request_id =
        required_response_header(response, "x-amz-sts-extended-request-id", label);
    assert_shape(
        label,
        response,
        &shape()
            .status(status)
            .header("content-type", "text/xml")
            .header("x-amzn-requestid", "{sts_request_id}")
            .header("x-amz-sts-extended-request-id", "{sts_extended_request_id}")
            .body(format!(
                "<ErrorResponse xmlns=\"{STS_XMLNS}\">\n  <Error>\n    \
                 <Type>Sender</Type>\n    <Code>{code}</Code>\n    \
                 <Message>{message}</Message>\n  </Error>\n  \
                 <RequestId>{{sts_request_id}}</RequestId>\n</ErrorResponse>\n"
            ))
            .sub("sts_request_id", request_id)
            .sub("sts_extended_request_id", extended_request_id),
    );
}

fn assume_role_body(role_session_name: &str) -> String {
    let mut form = url::form_urlencoded::Serializer::new(String::new());
    for (name, value) in [
        ("Action", "AssumeRole"),
        ("Version", "2011-06-15"),
        ("RoleArn", CTX.role_arn()),
        ("RoleSessionName", role_session_name),
    ] {
        form.append_pair(name, value);
    }
    form.finish()
}

fn send_presigned_assume_role(presigned: &s3_tests::PresignedRequest, body: &str) -> RawResponse {
    let agent = build_configured_test_agent(CTX.endpoint(), CTX.credentials().tls_ca_pem);
    let mut request = agent.post(presigned.uri());
    for (name, value) in presigned.headers() {
        request = request.header(name, value);
    }
    let mut response = request
        .send(body.as_bytes())
        .expect("presigned STS transport error");
    let status = response.status().as_u16();
    let headers = response
        .headers()
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_string(),
                value
                    .to_str()
                    .expect("presigned STS response header is valid UTF-8")
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

fn send_assume_role(parameters: &[(&str, &str)]) -> RawResponse {
    let mut form = url::form_urlencoded::Serializer::new(String::new());
    for (name, value) in parameters {
        form.append_pair(name, value);
    }
    let body = form.finish();
    send_checked_signed_request_for_service_with_credentials(
        "POST",
        &format!("{}/", CTX.endpoint()),
        body.as_bytes(),
        [("content-type", QUERY_CONTENT_TYPE)],
        SigningService::Sts,
        "sts",
        CTX.credentials(),
    )
}

fn required_xml_text(response: &RawResponse, tag: &str, label: &str) -> String {
    xml_tag_text(&response.body, tag)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| panic!("{label}: missing or empty {tag} in {response:?}"))
        .to_string()
}

fn assert_issued_credential_shapes(label: &str, response: &RawResponse) {
    let access_key = required_xml_text(response, "AccessKeyId", label);
    assert!(
        (16..=128).contains(&access_key.len())
            && access_key.bytes().all(|byte| byte.is_ascii_alphanumeric()),
        "{label}: unexpected temporary access-key shape"
    );

    let secret_key = required_xml_text(response, "SecretAccessKey", label);
    assert!(
        secret_key.len() == 40
            && secret_key.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/' | b'=' | b'-' | b'_')
            }),
        "{label}: unexpected temporary secret-key shape"
    );

    let session_token = required_xml_text(response, "SessionToken", label);
    assert!(
        session_token.len() >= 100
            && session_token.bytes().all(|byte| {
                byte.is_ascii_alphanumeric()
                    || matches!(byte, b'+' | b'/' | b'=' | b'-' | b'_' | b'.')
            }),
        "{label}: unexpected session-token shape"
    );
}

fn assert_expiration(label: &str, response: &RawResponse, expected_duration_seconds: i64) {
    let response_date = response_header_value(response, "date")
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| panic!("{label}: missing date response header"));
    let response_date = DateTime::from_str(response_date, DateTimeFormat::HttpDate)
        .unwrap_or_else(|error| panic!("{label}: invalid date response header: {error}"));
    let expiration = required_xml_text(response, "Expiration", label);
    let expiration = DateTime::from_str(&expiration, DateTimeFormat::DateTime)
        .unwrap_or_else(|error| panic!("{label}: invalid Expiration timestamp: {error}"));
    let delta = expiration.secs() - response_date.secs();
    assert!(
        delta == expected_duration_seconds || delta == expected_duration_seconds - 1,
        "{label}: expiration is {delta} seconds after the response date; expected {expected_duration_seconds} or one second less"
    );
}

fn assert_success_wire_shape(label: &str, mut response: RawResponse, role_session_name: &str) {
    for (tag, marker) in [
        ("AccessKeyId", "SESSION_ACCESS_KEY"),
        ("SecretAccessKey", "SESSION_SECRET_KEY"),
        ("SessionToken", "SESSION_TOKEN"),
    ] {
        let value = required_xml_text(&response, tag, label);
        replace_xml_text(&mut response.body, tag, &value, marker);
    }

    let assumed_role_id = required_xml_text(&response, "AssumedRoleId", label);
    let role_id = assumed_role_id
        .strip_suffix(&format!(":{role_session_name}"))
        .unwrap_or_else(|| panic!("{label}: AssumedRoleId does not end with the session name"));
    assert!(
        !role_id.is_empty() && role_id.bytes().all(|byte| byte.is_ascii_alphanumeric()),
        "{label}: unexpected role ID shape"
    );
    replace_xml_text(
        &mut response.body,
        "AssumedRoleId",
        &assumed_role_id,
        &format!("ROLE_ID:{role_session_name}"),
    );

    let request_id = required_response_header(&response, "x-amzn-requestid", label);
    let extended_request_id =
        required_response_header(&response, "x-amz-sts-extended-request-id", label);
    let assumed_role_arn = format!(
        "arn:aws:sts::{}:assumed-role/{}/{role_session_name}",
        CTX.account_id(),
        CTX.role_name()
    );
    assert_shape(
        label,
        &response,
        &shape()
            .status(200)
            .header("content-type", "text/xml")
            .header("x-amzn-requestid", "{sts_request_id}")
            .header(
                "x-amz-sts-extended-request-id",
                "{sts_extended_request_id}",
            )
            .body(format!(
                "<AssumeRoleResponse xmlns=\"{STS_XMLNS}\">\n  <AssumeRoleResult>\n    \
                 <AssumedRoleUser>\n      <AssumedRoleId>ROLE_ID:{role_session_name}</AssumedRoleId>\n      \
                 <Arn>{{assumed_role_arn}}</Arn>\n    </AssumedRoleUser>\n    <Credentials>\n      \
                 <AccessKeyId>SESSION_ACCESS_KEY</AccessKeyId>\n      \
                 <SecretAccessKey>SESSION_SECRET_KEY</SecretAccessKey>\n      \
                 <SessionToken>SESSION_TOKEN</SessionToken>\n      \
                 <Expiration>{{iso8601}}</Expiration>\n    </Credentials>\n  \
                 </AssumeRoleResult>\n  <ResponseMetadata>\n    \
                 <RequestId>{{sts_request_id}}</RequestId>\n  \
                 </ResponseMetadata>\n</AssumeRoleResponse>\n"
            ))
            .sub("sts_request_id", request_id)
            .sub("sts_extended_request_id", extended_request_id)
            .sub("assumed_role_arn", assumed_role_arn),
    );
}

fn required_response_header(response: &RawResponse, name: &str, label: &str) -> String {
    response_header_value(response, name)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| panic!("{label}: missing {name} response header"))
        .to_string()
}

fn replace_xml_text(body: &mut String, tag: &str, value: &str, replacement: &str) {
    let needle = format!("<{tag}>{value}</{tag}>");
    assert_eq!(
        body.matches(&needle).count(),
        1,
        "expected exactly one {tag} element while normalizing STS output"
    );
    *body = body.replacen(&needle, &format!("<{tag}>{replacement}</{tag}>"), 1);
}
