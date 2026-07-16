//! Read-only AWS STS Query protocol oracle.
//!
//! This is an explicitly invoked AWS probe rather than an ordinary test
//! binary: Argmin does not expose STS yet, and the repository's local test
//! suite must remain green while Phase 0 pins the AWS wire contract.

use std::{
    env,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use aws_smithy_types::{date_time::Format as DateTimeFormat, DateTime};
use ring::hmac;
use s3_tests::{
    build_test_agent, post_object_raw_to_test_endpoint_with_headers,
    presign_url_for_service_with_credentials, send_signed_request_for_service_with_credentials,
    send_signed_request_to_endpoint_for_service_with_credentials,
    shape::{
        assert_shape, assert_shape_with_request_id_validator, error_response_headers,
        expected_error, id_headers, response_header_value, shape, xml_tag_text, ShapeSpec,
    },
    sigv4_post_fields_for_credentials, sigv4_post_fields_for_service_with_credentials,
    PresignedRequest, RawResponse, SignedRequestCredentials,
};

const QUERY_CONTENT_TYPE: &str = "application/x-www-form-urlencoded";
const QUERY_POST_MAX_BODY_BYTES: usize = 10_000_000;
const OBSERVED_GET_BOUNDARY_ENDPOINT: &str = "https://sts.eu-central-1.amazonaws.com";
const OBSERVED_GET_BOUNDARY_REGION: &str = "eu-central-1";
const OBSERVED_GET_BOUNDARY_ACCESS_KEY_BYTES: usize = 20;
const ORACLE_STANDARD_SIGNED_GET_MAX_QUERY_BYTES: usize = 15_870;
const STS_XMLNS: &str = "https://sts.amazonaws.com/doc/2011-06-15/";
const AWS_FAULT_XMLNS: &str = "http://webservices.amazon.com/AWSFault/2005-15-09";
const STS_WRONG_REGION_SCOPE_MESSAGE: &str = "Credential should be scoped to a valid region. ";
const STS_WRONG_SERVICE_SCOPE_MESSAGE: &str =
    "Credential should be scoped to correct service: 'sts'. ";
const STS_SIGNATURE_MISMATCH_MESSAGE: &str = "The request signature we calculated does not match the signature you provided. Check your AWS Secret Access Key and signing method. Consult the service documentation for details.";
const STS_WRONG_REGION_AND_SERVICE_SCOPE_MESSAGE: &str = concat!(
    "Credential should be scoped to a valid region. ",
    "Credential should be scoped to correct service: 'sts'. "
);

#[derive(Clone, Copy)]
enum Expected<'a> {
    Success,
    Error {
        code: &'a str,
        message: &'a str,
    },
    StsError {
        status: u16,
        code: &'a str,
        message: &'a str,
    },
    Redirect {
        location: &'a str,
    },
}

#[derive(Clone, Copy)]
struct Probe<'a> {
    label: &'a str,
    request: QueryRequest<'a>,
    expected: Expected<'a>,
}

#[derive(Clone, Copy)]
enum QueryRequest<'a> {
    Get(&'a str),
    Post {
        body: &'a str,
        content_type: Option<&'a str>,
    },
}

impl QueryRequest<'_> {
    fn send(self, endpoint: &str, credentials: SignedRequestCredentials<'_>) -> RawResponse {
        self.send_with_security_token(endpoint, credentials, None)
    }

    fn send_with_security_token(
        self,
        endpoint: &str,
        credentials: SignedRequestCredentials<'_>,
        security_token: Option<&str>,
    ) -> RawResponse {
        self.send_with_scope(endpoint, credentials, security_token, "sts")
    }

    fn send_with_scope(
        self,
        endpoint: &str,
        credentials: SignedRequestCredentials<'_>,
        security_token: Option<&str>,
        service: &str,
    ) -> RawResponse {
        match self {
            Self::Get(query) => {
                let headers = security_token
                    .map(|value| vec![("x-amz-security-token", value)])
                    .unwrap_or_default();
                send_signed_request_for_service_with_credentials(
                    "GET",
                    &format!("{endpoint}/?{query}"),
                    b"",
                    headers,
                    service,
                    credentials,
                )
            }
            Self::Post { body, content_type } => {
                let mut headers = Vec::new();
                if let Some(value) = content_type {
                    headers.push(("content-type", value));
                }
                if let Some(value) = security_token {
                    headers.push(("x-amz-security-token", value));
                }
                send_signed_request_for_service_with_credentials(
                    "POST",
                    &format!("{endpoint}/"),
                    body.as_bytes(),
                    headers,
                    service,
                    credentials,
                )
            }
        }
    }
}

fn required_env(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("{name} must be set by scripts/aws-sts-oracle"))
}

fn validate_https_endpoint(name: &str, endpoint: &str) -> Result<(), String> {
    let parsed = url::Url::parse(endpoint)
        .map_err(|error| format!("{name} must be a valid HTTPS URL: {error}"))?;
    if parsed.scheme() != "https" {
        return Err(format!("{name} must use https://"));
    }
    if parsed.host_str().is_none() {
        return Err(format!("{name} must include a host"));
    }
    Ok(())
}

fn required_https_endpoint(name: &str) -> String {
    let endpoint = required_env(name);
    validate_https_endpoint(name, &endpoint).unwrap_or_else(|error| panic!("{error}"));
    endpoint
}

fn sts_wire_shape(label: &str, response: &RawResponse) -> ShapeSpec {
    let request_id = response_header_value(response, "x-amzn-requestid")
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| panic!("{label}: missing x-amzn-requestid response header"));
    let extended_request_id = response_header_value(response, "x-amz-sts-extended-request-id")
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| panic!("{label}: missing x-amz-sts-extended-request-id header"));
    shape()
        .header("x-amzn-requestid", "{sts_request_id}")
        .header("x-amz-sts-extended-request-id", "{sts_extended_request_id}")
        .sub("sts_request_id", request_id)
        .sub("sts_extended_request_id", extended_request_id)
}

fn assert_get_caller_identity_success(label: &str, response: &RawResponse, account_id: &str) {
    let arn = xml_tag_text(&response.body, "Arn")
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| panic!("{label}: missing Arn"));
    let user_id = xml_tag_text(&response.body, "UserId")
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| panic!("{label}: missing UserId"));
    assert_shape(
        label,
        response,
        &sts_wire_shape(label, response)
            .status(200)
            .header("content-type", "text/xml")
            .body(format!(
                "<GetCallerIdentityResponse xmlns=\"{STS_XMLNS}\">\n  \
                 <GetCallerIdentityResult>\n    <Arn>{{arn}}</Arn>\n    \
                 <UserId>{{user_id}}</UserId>\n    \
                 <Account>{{account}}</Account>\n  \
                 </GetCallerIdentityResult>\n  <ResponseMetadata>\n    \
                 <RequestId>{{sts_request_id}}</RequestId>\n  \
                 </ResponseMetadata>\n</GetCallerIdentityResponse>\n"
            ))
            .sub("arn", arn)
            .sub("user_id", user_id)
            .sub("account", account_id),
    );
}

fn assert_probe(probe: &Probe<'_>, response: &RawResponse, account_id: &str) {
    match probe.expected {
        Expected::Success => {
            assert_get_caller_identity_success(probe.label, response, account_id);
        }
        Expected::Error { code, message } => {
            assert_error_probe(
                probe.label,
                response,
                400,
                AWS_FAULT_XMLNS,
                code,
                Some(message),
            );
        }
        Expected::StsError {
            status,
            code,
            message,
        } => {
            assert_error_probe(
                probe.label,
                response,
                status,
                STS_XMLNS,
                code,
                Some(message),
            );
        }
        Expected::Redirect { location } => {
            assert_shape(
                probe.label,
                response,
                &sts_wire_shape(probe.label, response)
                    .status(302)
                    .header("location", location)
                    .body_empty(),
            );
        }
    }
}

fn assert_assumed_role_caller_identity(
    label: &str,
    response: &RawResponse,
    account_id: &str,
    role_name: &str,
    role_session_name: &str,
    expected_role_id: Option<&str>,
) {
    let expected_arn =
        format!("arn:aws:sts::{account_id}:assumed-role/{role_name}/{role_session_name}");
    let arn = required_xml_text(response, "Arn", label);
    assert_eq!(arn, expected_arn, "{label}: unexpected caller ARN");
    let user_id = required_xml_text(response, "UserId", label);
    let role_id = user_id
        .strip_suffix(&format!(":{role_session_name}"))
        .unwrap_or_else(|| panic!("{label}: UserId does not end with the role session name"));
    assert!(
        role_id.len() == 21
            && role_id.starts_with("AROA")
            && role_id.bytes().all(|byte| byte.is_ascii_alphanumeric()),
        "{label}: caller UserId has an unexpected role ID shape"
    );
    if let Some(expected_role_id) = expected_role_id {
        assert_eq!(
            role_id, expected_role_id,
            "{label}: caller UserId has an unexpected stable role ID"
        );
    }
    assert_get_caller_identity_success(label, response, account_id);
    println!("{label}: ok");
}

fn send_get_caller_identity(
    endpoint: &str,
    credentials: SignedRequestCredentials<'_>,
    security_token: Option<&str>,
) -> RawResponse {
    QueryRequest::Post {
        body: "Action=GetCallerIdentity&Version=2011-06-15",
        content_type: Some(QUERY_CONTENT_TYPE),
    }
    .send_with_security_token(endpoint, credentials, security_token)
}

fn send_get_caller_identity_with_scope(
    endpoint: &str,
    credentials: SignedRequestCredentials<'_>,
    security_token: Option<&str>,
    service: &str,
) -> RawResponse {
    QueryRequest::Post {
        body: "Action=GetCallerIdentity&Version=2011-06-15",
        content_type: Some(QUERY_CONTENT_TYPE),
    }
    .send_with_scope(endpoint, credentials, security_token, service)
}

fn query_body_with_ignored_value(total_bytes: usize) -> (String, usize) {
    const PREFIX: &str = "Action=GetCallerIdentity&Version=2011-06-15&x=";
    assert!(total_bytes >= PREFIX.len());
    let value_bytes = total_bytes - PREFIX.len();
    let body = format!("{PREFIX}{}", "v".repeat(value_bytes));
    assert_eq!(body.len(), total_bytes);
    (body, value_bytes)
}

fn query_body_with_ignored_name(total_bytes: usize) -> (String, usize) {
    const PREFIX: &str = "Action=GetCallerIdentity&Version=2011-06-15&";
    assert!(total_bytes >= PREFIX.len());
    let name_bytes = total_bytes - PREFIX.len();
    let body = format!("{PREFIX}{}", "N".repeat(name_bytes));
    assert_eq!(body.len(), total_bytes);
    (body, name_bytes)
}

fn query_body_with_maximum_member_count(total_bytes: usize) -> (String, usize) {
    const REQUIRED_MEMBERS: &str = "Action=GetCallerIdentity&Version=2011-06-15";
    const IGNORED_MEMBER: &str = "&x";
    const FINAL_IGNORED_MEMBER: &str = "&y=";
    let remaining = total_bytes - REQUIRED_MEMBERS.len() - FINAL_IGNORED_MEMBER.len();
    assert_eq!(remaining % IGNORED_MEMBER.len(), 0);
    let repeated_member_count = remaining / IGNORED_MEMBER.len();
    let mut body = String::with_capacity(total_bytes);
    body.push_str(REQUIRED_MEMBERS);
    for _ in 0..repeated_member_count {
        body.push_str(IGNORED_MEMBER);
    }
    body.push_str(FINAL_IGNORED_MEMBER);
    assert_eq!(body.len(), total_bytes);
    (body, repeated_member_count + 3)
}

fn matches_observed_get_boundary_fixture(
    endpoint: &str,
    credentials: SignedRequestCredentials<'_>,
) -> bool {
    endpoint == OBSERVED_GET_BOUNDARY_ENDPOINT
        && credentials.region == OBSERVED_GET_BOUNDARY_REGION
        && credentials.access_key.len() == OBSERVED_GET_BOUNDARY_ACCESS_KEY_BYTES
}

fn run_query_limit_probes(
    endpoint: &str,
    credentials: SignedRequestCredentials<'_>,
    account_id: &str,
) {
    let (maximum_body, value_bytes) = query_body_with_ignored_value(QUERY_POST_MAX_BODY_BYTES);
    assert_eq!(value_bytes, 9_999_954);
    let maximum_response = QueryRequest::Post {
        body: &maximum_body,
        content_type: Some(QUERY_CONTENT_TYPE),
    }
    .send(endpoint, credentials);
    assert_get_caller_identity_success(
        "query-member-value-maximum-9999954-bytes",
        &maximum_response,
        account_id,
    );
    println!("query-member-value-maximum-9999954-bytes: ok");

    let (maximum_name_body, name_bytes) = query_body_with_ignored_name(QUERY_POST_MAX_BODY_BYTES);
    assert_eq!(name_bytes, 9_999_956);
    let maximum_name_response = QueryRequest::Post {
        body: &maximum_name_body,
        content_type: Some(QUERY_CONTENT_TYPE),
    }
    .send(endpoint, credentials);
    assert_get_caller_identity_success(
        "query-member-name-maximum-9999956-bytes",
        &maximum_name_response,
        account_id,
    );
    println!("query-member-name-maximum-9999956-bytes: ok");

    let (maximum_members_body, member_count) =
        query_body_with_maximum_member_count(QUERY_POST_MAX_BODY_BYTES);
    assert_eq!(member_count, 4_999_980);
    let maximum_members_response = QueryRequest::Post {
        body: &maximum_members_body,
        content_type: Some(QUERY_CONTENT_TYPE),
    }
    .send(endpoint, credentials);
    assert_get_caller_identity_success(
        "query-member-count-maximum-4999980",
        &maximum_members_response,
        account_id,
    );
    println!("query-member-count-maximum-4999980: ok");

    let (overlong_body, _) = query_body_with_ignored_value(QUERY_POST_MAX_BODY_BYTES + 1);
    let overlong_response = QueryRequest::Post {
        body: &overlong_body,
        content_type: Some(QUERY_CONTENT_TYPE),
    }
    .send(endpoint, credentials);
    assert_shape(
        "query-body-overlong-10000001-bytes",
        &overlong_response,
        &shape()
            .status(413)
            .headers(std::iter::empty::<(&str, &str)>())
            .body_empty(),
    );
    println!("query-body-overlong-10000001-bytes: ok");

    if !matches_observed_get_boundary_fixture(endpoint, credentials) {
        println!(
            "query-get-request-head-boundary: skipped (requires {OBSERVED_GET_BOUNDARY_ENDPOINT}, {OBSERVED_GET_BOUNDARY_REGION}, and a {OBSERVED_GET_BOUNDARY_ACCESS_KEY_BYTES}-byte access key)"
        );
        return;
    }

    let (maximum_query, _) =
        query_body_with_ignored_value(ORACLE_STANDARD_SIGNED_GET_MAX_QUERY_BYTES);
    let maximum_query_response = QueryRequest::Get(&maximum_query).send(endpoint, credentials);
    assert_get_caller_identity_success(
        "query-get-maximum-15870-query-bytes",
        &maximum_query_response,
        account_id,
    );
    println!("query-get-maximum-15870-query-bytes: ok");

    let (overlong_query, _) =
        query_body_with_ignored_value(ORACLE_STANDARD_SIGNED_GET_MAX_QUERY_BYTES + 1);
    let overlong_query_response = QueryRequest::Get(&overlong_query).send(endpoint, credentials);
    assert_shape(
        "query-get-overlong-15871-query-bytes",
        &overlong_query_response,
        &shape()
            .status(400)
            .headers(std::iter::empty::<(&str, &str)>())
            .body_empty(),
    );
    println!("query-get-overlong-15871-query-bytes: ok");

    let (smaller_query, _) = query_body_with_ignored_value(15_800);
    let smaller_query_with_header_response = send_signed_request_for_service_with_credentials(
        "GET",
        &format!("{endpoint}/?{smaller_query}"),
        b"",
        [("x-test-padding", "x")],
        "sts",
        credentials,
    );
    assert_get_caller_identity_success(
        "query-get-15800-query-bytes-with-signed-header",
        &smaller_query_with_header_response,
        account_id,
    );
    println!("query-get-15800-query-bytes-with-signed-header: ok");

    let maximum_query_with_header_response = send_signed_request_for_service_with_credentials(
        "GET",
        &format!("{endpoint}/?{maximum_query}"),
        b"",
        [("x-test-padding", "")],
        "sts",
        credentials,
    );
    assert_shape(
        "query-get-15870-query-bytes-with-signed-header",
        &maximum_query_with_header_response,
        &shape()
            .status(400)
            .headers(std::iter::empty::<(&str, &str)>())
            .body_empty(),
    );
    println!("query-get-15870-query-bytes-with-signed-header: ok");
}

fn assert_signing_scope_error(label: &str, response: &RawResponse, message: &str) {
    assert_error_probe(
        label,
        response,
        403,
        STS_XMLNS,
        "SignatureDoesNotMatch",
        Some(message),
    );
    println!("{label}: ok");
}

fn run_signing_scope_probes(
    endpoint: &str,
    credentials: SignedRequestCredentials<'_>,
    account_id: &str,
) {
    let wrong_region = if credentials.region == "us-east-1" {
        "us-west-2"
    } else {
        "us-east-1"
    };
    let wrong_region_credentials = SignedRequestCredentials {
        region: wrong_region,
        ..credentials
    };
    let regional_wrong_region =
        send_get_caller_identity_with_scope(endpoint, wrong_region_credentials, None, "sts");
    assert_signing_scope_error(
        "scope-regional-wrong-region",
        &regional_wrong_region,
        STS_WRONG_REGION_SCOPE_MESSAGE,
    );

    let regional_wrong_service =
        send_get_caller_identity_with_scope(endpoint, credentials, None, "s3");
    assert_signing_scope_error(
        "scope-regional-wrong-service",
        &regional_wrong_service,
        STS_WRONG_SERVICE_SCOPE_MESSAGE,
    );

    let regional_wrong_region_and_service =
        send_get_caller_identity_with_scope(endpoint, wrong_region_credentials, None, "s3");
    assert_signing_scope_error(
        "scope-regional-wrong-region-and-service",
        &regional_wrong_region_and_service,
        STS_WRONG_REGION_AND_SERVICE_SCOPE_MESSAGE,
    );

    let global_credentials = SignedRequestCredentials {
        region: "us-east-1",
        ..credentials
    };
    let global_success = send_get_caller_identity_with_scope(
        "https://sts.amazonaws.com",
        global_credentials,
        None,
        "sts",
    );
    assert_get_caller_identity_success("scope-global-us-east-1", &global_success, account_id);
    println!("scope-global-us-east-1: ok");

    let global_wrong_credentials = SignedRequestCredentials {
        region: "us-west-2",
        ..credentials
    };
    let global_wrong_region = send_get_caller_identity_with_scope(
        "https://sts.amazonaws.com",
        global_wrong_credentials,
        None,
        "sts",
    );
    assert_signing_scope_error(
        "scope-global-wrong-region",
        &global_wrong_region,
        STS_WRONG_REGION_SCOPE_MESSAGE,
    );
}

fn spaced_hex(value: &str) -> String {
    value
        .bytes()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn sanitize_s3_text(text: &str, access_key: &str, security_tokens: &[&str]) -> String {
    let mut sanitized = text.replace(&spaced_hex(access_key), "SESSION_ACCESS_KEY_BYTES");
    sanitized = sanitized.replace(access_key, "SESSION_ACCESS_KEY");
    for security_token in security_tokens {
        if security_token.is_empty() {
            continue;
        }
        let uri_encoded = auth::canonical::uri_encode(security_token);
        if uri_encoded != *security_token {
            sanitized =
                sanitized.replace(&spaced_hex(&uri_encoded), "SESSION_TOKEN_URI_ENCODED_BYTES");
            sanitized = sanitized.replace(&uri_encoded, "SESSION_TOKEN_URI_ENCODED");
        }
        sanitized = sanitized.replace(&spaced_hex(security_token), "SESSION_TOKEN_BYTES");
        sanitized = sanitized.replace(security_token, "SESSION_TOKEN");
    }
    sanitized
}

fn s3_response_with_sanitized_body(
    response: &RawResponse,
    access_key: &str,
    security_tokens: &[&str],
) -> RawResponse {
    RawResponse {
        status: response.status,
        headers: response.headers.clone(),
        body: sanitize_s3_text(&response.body, access_key, security_tokens),
        body_read_error: response.body_read_error.clone(),
    }
}

fn assert_s3_invalid_access_key_shape(label: &str, response: &RawResponse) {
    assert_shape(
        label,
        response,
        &shape()
            .status(403)
            .headers(error_response_headers())
            .body(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                 <Error><Code>InvalidAccessKeyId</Code>\
                 <Message>The AWS Access Key Id you provided does not exist in our records.</Message>\
                 <AWSAccessKeyId>SESSION_ACCESS_KEY</AWSAccessKeyId>\
                 <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>",
            ),
    );
    println!("{label}: ok");
}

fn assert_s3_invalid_access_key(
    label: &str,
    response: &RawResponse,
    access_key: &str,
    security_tokens: &[&str],
) {
    assert!(
        required_xml_text(response, "AWSAccessKeyId", label) == access_key,
        "{label}: S3 did not echo the session access key"
    );
    let response = s3_response_with_sanitized_body(response, access_key, security_tokens);
    assert_s3_invalid_access_key_shape(label, &response);
}

fn assert_s3_signature_mismatch<F>(
    label: &str,
    response: &RawResponse,
    credentials: SignedRequestCredentials<'_>,
    security_tokens: &[&str],
    expected_amz_date: Option<&str>,
    expected_signature: Option<&str>,
    canonical_request_for_date: F,
) where
    F: FnOnce(&str) -> String,
{
    assert!(
        required_xml_text(response, "AWSAccessKeyId", label) == credentials.access_key,
        "{label}: S3 did not echo the session access key"
    );
    let string_to_sign = required_xml_text(response, "StringToSign", label);
    let string_to_sign_bytes = required_xml_text(response, "StringToSignBytes", label);
    assert!(
        string_to_sign_bytes == spaced_hex(&string_to_sign),
        "{label}: StringToSignBytes does not encode StringToSign"
    );
    let mut string_to_sign_lines = string_to_sign.lines();
    assert!(
        string_to_sign_lines.next() == Some("AWS4-HMAC-SHA256"),
        "{label}: unexpected signing algorithm"
    );
    let amz_date = string_to_sign_lines
        .next()
        .unwrap_or_else(|| panic!("{label}: missing signing timestamp"));
    let amz_date_bytes = amz_date.as_bytes();
    assert!(
        amz_date.len() == 16
            && amz_date_bytes[8] == b'T'
            && amz_date_bytes[15] == b'Z'
            && amz_date_bytes[..8].iter().all(|byte| byte.is_ascii_digit())
            && amz_date_bytes[9..15]
                .iter()
                .all(|byte| byte.is_ascii_digit()),
        "{label}: malformed signing timestamp"
    );
    if let Some(expected_amz_date) = expected_amz_date {
        assert!(
            amz_date == expected_amz_date,
            "{label}: response StringToSign has the wrong request timestamp"
        );
    }
    let scope = format!("{}/{}/s3/aws4_request", &amz_date[..8], credentials.region);
    assert!(
        string_to_sign_lines.next() == Some(scope.as_str()),
        "{label}: unexpected S3 credential scope"
    );
    let canonical_request_hash = string_to_sign_lines
        .next()
        .unwrap_or_else(|| panic!("{label}: missing canonical request hash"));
    assert!(
        string_to_sign_lines.next().is_none(),
        "{label}: unexpected extra StringToSign line"
    );

    let canonical_request = canonical_request_for_date(amz_date);
    let observed_canonical_request = required_xml_text(response, "CanonicalRequest", label);
    let canonical_request_xml = canonical_request.replace('&', "&amp;");
    assert!(
        observed_canonical_request == canonical_request_xml,
        "{label}: unexpected canonical request\nexpected: {:?}\nobserved: {:?}",
        sanitize_s3_text(
            &canonical_request_xml,
            credentials.access_key,
            security_tokens
        ),
        sanitize_s3_text(
            &observed_canonical_request,
            credentials.access_key,
            security_tokens
        )
    );
    assert!(
        required_xml_text(response, "CanonicalRequestBytes", label)
            == spaced_hex(&canonical_request),
        "{label}: CanonicalRequestBytes does not encode CanonicalRequest"
    );
    assert!(
        canonical_request_hash == auth::canonical::sha256_hex(canonical_request.as_bytes()),
        "{label}: StringToSign has the wrong canonical request hash"
    );
    let signature = required_xml_text(response, "SignatureProvided", label);
    assert!(
        signature.len() == 64
            && signature
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "{label}: malformed provided signature"
    );
    if let Some(expected_signature) = expected_signature {
        assert!(
            signature == expected_signature,
            "{label}: response did not echo the presigned query signature"
        );
    }

    let response =
        s3_response_with_sanitized_body(response, credentials.access_key, security_tokens);
    let sanitized_canonical_request = sanitize_s3_text(
        &canonical_request_xml,
        credentials.access_key,
        security_tokens,
    );
    let sanitized_canonical_request_bytes = sanitize_s3_text(
        &spaced_hex(&canonical_request),
        credentials.access_key,
        security_tokens,
    );
    assert_shape(
        label,
        &response,
        &shape()
            .status(403)
            .headers(error_response_headers())
            .sub("string_to_sign", string_to_sign)
            .sub("signature", signature)
            .sub("string_to_sign_bytes", string_to_sign_bytes)
            .sub("canonical_request", sanitized_canonical_request)
            .sub("canonical_request_bytes", sanitized_canonical_request_bytes)
            .body(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                 <Error><Code>SignatureDoesNotMatch</Code>\
                 <Message>The request signature we calculated does not match the signature you provided. Check your key and signing method.</Message>\
                 <AWSAccessKeyId>SESSION_ACCESS_KEY</AWSAccessKeyId>\
                 <StringToSign>{string_to_sign}</StringToSign>\
                 <SignatureProvided>{signature}</SignatureProvided>\
                 <StringToSignBytes>{string_to_sign_bytes}</StringToSignBytes>\
                 <CanonicalRequest>{canonical_request}</CanonicalRequest>\
                 <CanonicalRequestBytes>{canonical_request_bytes}</CanonicalRequestBytes>\
                 <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>",
            ),
    );
    println!("{label}: ok");
}

fn assert_s3_header_signature_mismatch(
    label: &str,
    response: &RawResponse,
    endpoint: &str,
    credentials: SignedRequestCredentials<'_>,
    security_token: &str,
) {
    let parsed_endpoint = url::Url::parse(endpoint)
        .unwrap_or_else(|error| panic!("{label}: invalid S3 endpoint: {error}"));
    let host = parsed_endpoint
        .host_str()
        .unwrap_or_else(|| panic!("{label}: S3 endpoint has no host"));
    let empty_payload_hash = auth::canonical::sha256_hex(b"");
    assert_s3_signature_mismatch(
        label,
        response,
        credentials,
        &[security_token],
        None,
        None,
        |amz_date| {
            format!(
                "GET\n/\n\nhost:{host}\n\
                 x-amz-content-sha256:{empty_payload_hash}\n\
                 x-amz-date:{amz_date}\n\
                 x-amz-security-token:{security_token}\n\n\
                 host;x-amz-content-sha256;x-amz-date;x-amz-security-token\n\
                 {empty_payload_hash}"
            )
        },
    );
}

fn assert_s3_list_buckets_access_denied(
    label: &str,
    response: &RawResponse,
    assumed_role_arn: &str,
    access_key: &str,
    security_tokens: &[&str],
) {
    let response = s3_response_with_sanitized_body(response, access_key, security_tokens);
    assert_shape(
        label,
        &response,
        &shape()
            .status(403)
            .headers(error_response_headers())
            .sub("assumed_role_arn", assumed_role_arn)
            .body(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                 <Error><Code>AccessDenied</Code>\
                 <Message>User: {assumed_role_arn} is not authorized to perform: \
                 s3:ListAllMyBuckets because no identity-based policy allows the \
                 s3:ListAllMyBuckets action</Message>\
                 <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>",
            ),
    );
    println!("{label}: ok");
}

fn assert_s3_headers_not_signed(
    label: &str,
    response: &RawResponse,
    access_key: &str,
    security_tokens: &[&str],
) {
    let response = s3_response_with_sanitized_body(response, access_key, security_tokens);
    assert_shape(
        label,
        &response,
        &shape()
            .status(403)
            .headers(error_response_headers())
            .body(expected_error::headers_not_signed("x-amz-security-token")),
    );
    println!("{label}: ok");
}

#[derive(Clone, Copy)]
enum S3HeaderAuthExpected {
    AccessDenied,
    InvalidAccessKey,
    SignatureMismatch,
}

struct S3HeaderSessionProbeSet<'a> {
    live_credentials: SignedRequestCredentials<'a>,
    live_security_token: &'a str,
    live_role_name: &'a str,
    live_role_session_name: &'a str,
    other_live_security_token: &'a str,
    old_credentials: SignedRequestCredentials<'a>,
    old_security_token: &'a str,
}

fn run_s3_header_session_authentication_probes(
    endpoint: &str,
    account_id: &str,
    fixture: S3HeaderSessionProbeSet<'_>,
) {
    let wrong_secret = "0".repeat(40);
    let live_bad_signature_credentials = SignedRequestCredentials {
        secret_key: &wrong_secret,
        ..fixture.live_credentials
    };
    let old_bad_signature_credentials = SignedRequestCredentials {
        secret_key: &wrong_secret,
        ..fixture.old_credentials
    };
    for (label, credentials, security_token, expected) in [
        (
            "s3-header-auth-live-valid",
            fixture.live_credentials,
            Some(fixture.live_security_token),
            S3HeaderAuthExpected::AccessDenied,
        ),
        (
            "s3-header-auth-live-missing-token-valid-signature",
            fixture.live_credentials,
            None,
            S3HeaderAuthExpected::InvalidAccessKey,
        ),
        (
            "s3-header-auth-live-mismatched-token-valid-signature",
            fixture.live_credentials,
            Some(fixture.other_live_security_token),
            S3HeaderAuthExpected::InvalidAccessKey,
        ),
        (
            "s3-header-auth-live-valid-token-bad-signature",
            live_bad_signature_credentials,
            Some(fixture.live_security_token),
            S3HeaderAuthExpected::SignatureMismatch,
        ),
        (
            "s3-header-auth-live-missing-token-bad-signature",
            live_bad_signature_credentials,
            None,
            S3HeaderAuthExpected::InvalidAccessKey,
        ),
        (
            "s3-header-auth-live-mismatched-token-bad-signature",
            live_bad_signature_credentials,
            Some(fixture.other_live_security_token),
            S3HeaderAuthExpected::InvalidAccessKey,
        ),
        (
            "s3-header-auth-old-session-valid",
            fixture.old_credentials,
            Some(fixture.old_security_token),
            S3HeaderAuthExpected::InvalidAccessKey,
        ),
        (
            "s3-header-auth-old-session-missing-token-valid-signature",
            fixture.old_credentials,
            None,
            S3HeaderAuthExpected::InvalidAccessKey,
        ),
        (
            "s3-header-auth-old-session-mismatched-token-valid-signature",
            fixture.old_credentials,
            Some(fixture.live_security_token),
            S3HeaderAuthExpected::InvalidAccessKey,
        ),
        (
            "s3-header-auth-old-session-valid-token-bad-signature",
            old_bad_signature_credentials,
            Some(fixture.old_security_token),
            S3HeaderAuthExpected::InvalidAccessKey,
        ),
        (
            "s3-header-auth-old-session-missing-token-bad-signature",
            old_bad_signature_credentials,
            None,
            S3HeaderAuthExpected::InvalidAccessKey,
        ),
        (
            "s3-header-auth-old-session-mismatched-token-bad-signature",
            old_bad_signature_credentials,
            Some(fixture.live_security_token),
            S3HeaderAuthExpected::InvalidAccessKey,
        ),
    ] {
        let headers = security_token
            .map(|value| vec![("x-amz-security-token", value)])
            .unwrap_or_default();
        let response = send_signed_request_for_service_with_credentials(
            "GET",
            endpoint,
            b"",
            headers,
            "s3",
            credentials,
        );
        match expected {
            S3HeaderAuthExpected::AccessDenied => {
                let assumed_role_arn = format!(
                    "arn:aws:sts::{account_id}:assumed-role/{}/{}",
                    fixture.live_role_name, fixture.live_role_session_name
                );
                assert_s3_list_buckets_access_denied(
                    label,
                    &response,
                    &assumed_role_arn,
                    credentials.access_key,
                    security_token.as_slice(),
                );
            }
            S3HeaderAuthExpected::InvalidAccessKey => assert_s3_invalid_access_key(
                label,
                &response,
                credentials.access_key,
                security_token.as_slice(),
            ),
            S3HeaderAuthExpected::SignatureMismatch => assert_s3_header_signature_mismatch(
                label,
                &response,
                endpoint,
                credentials,
                security_token.expect("signature mismatch probe has a session token"),
            ),
        }
    }
}

struct S3HeaderScopeProbeSet<'a> {
    account_id: &'a str,
    bucket: &'a str,
    live_credentials: SignedRequestCredentials<'a>,
    live_security_token: &'a str,
    live_role_name: &'a str,
    live_role_session_name: &'a str,
    other_live_security_token: &'a str,
    old_credentials: SignedRequestCredentials<'a>,
    old_security_token: &'a str,
}

struct S3ListBucketDeniedFixture<'a> {
    account_id: &'a str,
    bucket: &'a str,
    role_name: &'a str,
    role_session_name: &'a str,
    credentials: SignedRequestCredentials<'a>,
    security_token: &'a str,
}

fn assert_s3_list_bucket_access_denied(
    label: &str,
    response: &RawResponse,
    fixture: S3ListBucketDeniedFixture<'_>,
) {
    let response = s3_response_with_sanitized_body(
        response,
        fixture.credentials.access_key,
        &[fixture.security_token],
    );
    let assumed_role_arn = format!(
        "arn:aws:sts::{}:assumed-role/{}/{}",
        fixture.account_id, fixture.role_name, fixture.role_session_name
    );
    assert_shape(
        label,
        &response,
        &shape()
            .status(403)
            .headers(error_response_headers())
            .header("x-amz-bucket-region", fixture.credentials.region)
            .sub("assumed_role_arn", assumed_role_arn)
            .sub("bucket", fixture.bucket)
            .body(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                 <Error><Code>AccessDenied</Code>\
                 <Message>User: {assumed_role_arn} is not authorized to perform: \
                 s3:ListBucket on resource: \"arn:aws:s3:::{bucket}\" because no \
                 identity-based policy allows the s3:ListBucket action</Message>\
                 <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>",
            ),
    );
    println!("{label}: ok");
}

fn assert_s3_header_wrong_region_scope(
    label: &str,
    response: &RawResponse,
    wrong_region: &str,
    expected_region: &str,
    access_key: &str,
    security_tokens: &[&str],
) {
    let response = s3_response_with_sanitized_body(response, access_key, security_tokens);
    let message = format!(
        "The authorization header is malformed; the region '{wrong_region}' is wrong; \
         expecting '{expected_region}'"
    );
    assert_shape(
        label,
        &response,
        &shape()
            .status(400)
            .headers(error_response_headers())
            .header("x-amz-bucket-region", expected_region)
            .body(expected_error::with_region(
                "AuthorizationHeaderMalformed",
                &message,
                expected_region,
            )),
    );
    println!("{label}: ok");
}

fn assert_s3_header_wrong_service_scope(
    label: &str,
    response: &RawResponse,
    expected_region: &str,
    access_key: &str,
    security_tokens: &[&str],
) {
    let response = s3_response_with_sanitized_body(response, access_key, security_tokens);
    assert_shape(
        label,
        &response,
        &shape()
            .status(400)
            .headers(error_response_headers())
            .header("x-amz-bucket-region", expected_region)
            .body(expected_error::with_host_id(
                "AuthorizationHeaderMalformed",
                "The authorization header is malformed; incorrect service \"sts\". This endpoint belongs to \"s3\".",
            )),
    );
    println!("{label}: ok");
}

fn run_s3_header_scope_probes(endpoint: &str, fixture: S3HeaderScopeProbeSet<'_>) {
    let positive_response = send_signed_request_for_service_with_credentials(
        "GET",
        endpoint,
        b"",
        [("x-amz-security-token", fixture.live_security_token)],
        "s3",
        fixture.live_credentials,
    );
    assert_s3_list_bucket_access_denied(
        "s3-header-scope-live-role-correct-scope",
        &positive_response,
        S3ListBucketDeniedFixture {
            account_id: fixture.account_id,
            bucket: fixture.bucket,
            role_name: fixture.live_role_name,
            role_session_name: fixture.live_role_session_name,
            credentials: fixture.live_credentials,
            security_token: fixture.live_security_token,
        },
    );

    let wrong_secret = "0".repeat(40);
    let wrong_region = if fixture.live_credentials.region == "us-east-1" {
        "us-west-2"
    } else {
        "us-east-1"
    };
    for (role_state, credentials, security_token, other_security_token) in [
        (
            "live-role",
            fixture.live_credentials,
            fixture.live_security_token,
            fixture.other_live_security_token,
        ),
        (
            "old-session",
            fixture.old_credentials,
            fixture.old_security_token,
            fixture.live_security_token,
        ),
    ] {
        let wrong_region_credentials = SignedRequestCredentials {
            region: wrong_region,
            ..credentials
        };
        let wrong_region_bad_signature_credentials = SignedRequestCredentials {
            secret_key: &wrong_secret,
            ..wrong_region_credentials
        };
        let wrong_service_bad_signature_credentials = SignedRequestCredentials {
            secret_key: &wrong_secret,
            ..credentials
        };
        for (case, signing_credentials, supplied_token, service) in [
            (
                "valid-token-wrong-region",
                wrong_region_credentials,
                Some(security_token),
                "s3",
            ),
            (
                "missing-token-wrong-region",
                wrong_region_credentials,
                None,
                "s3",
            ),
            (
                "mismatched-token-wrong-region",
                wrong_region_credentials,
                Some(other_security_token),
                "s3",
            ),
            (
                "valid-token-wrong-region-bad-signature",
                wrong_region_bad_signature_credentials,
                Some(security_token),
                "s3",
            ),
            (
                "valid-token-wrong-service",
                credentials,
                Some(security_token),
                "sts",
            ),
            ("missing-token-wrong-service", credentials, None, "sts"),
            (
                "mismatched-token-wrong-service",
                credentials,
                Some(other_security_token),
                "sts",
            ),
            (
                "valid-token-wrong-service-bad-signature",
                wrong_service_bad_signature_credentials,
                Some(security_token),
                "sts",
            ),
        ] {
            let label = format!("s3-header-scope-{role_state}-{case}");
            let headers = supplied_token
                .map(|token| vec![("x-amz-security-token", token)])
                .unwrap_or_default();
            let response = send_signed_request_for_service_with_credentials(
                "GET",
                endpoint,
                b"",
                headers,
                service,
                signing_credentials,
            );
            let presented_tokens = supplied_token.as_slice();
            if service == "s3" {
                assert_s3_header_wrong_region_scope(
                    &label,
                    &response,
                    wrong_region,
                    fixture.live_credentials.region,
                    signing_credentials.access_key,
                    presented_tokens,
                );
            } else {
                assert_s3_header_wrong_service_scope(
                    &label,
                    &response,
                    fixture.live_credentials.region,
                    signing_credentials.access_key,
                    presented_tokens,
                );
            }
        }
    }

    let wrong_region_credentials = SignedRequestCredentials {
        region: wrong_region,
        ..fixture.live_credentials
    };
    let both_wrong = send_signed_request_for_service_with_credentials(
        "GET",
        endpoint,
        b"",
        [("x-amz-security-token", fixture.live_security_token)],
        "sts",
        wrong_region_credentials,
    );
    assert_s3_header_wrong_region_scope(
        "s3-header-scope-live-role-both-wrong",
        &both_wrong,
        wrong_region,
        fixture.live_credentials.region,
        fixture.live_credentials.access_key,
        &[fixture.live_security_token],
    );
}

struct S3SessionContextProbeSet<'a> {
    credentials: SignedRequestCredentials<'a>,
    security_token: &'a str,
    assumed_role_arn: &'a str,
}

#[derive(Clone, Copy)]
enum S3SessionContextExpected {
    Success,
    AccessDenied,
    ExplicitResourceDeny,
}

fn assert_s3_session_context_put_success(label: &str, response: &RawResponse) {
    assert_shape(
        label,
        response,
        &shape()
            .status(200)
            .header("x-amz-id-2", "{host_id}")
            .header("x-amz-request-id", "{request_id}")
            .header("x-amz-server-side-encryption", "AES256")
            .header("etag", "\"d41d8cd98f00b204e9800998ecf8427e\"")
            .header("x-amz-checksum-crc64nvme", "AAAAAAAAAAA=")
            .header("x-amz-checksum-type", "FULL_OBJECT")
            .body_empty(),
    );
    println!("{label}: ok");
}

fn assert_s3_session_context_put_access_denied(
    label: &str,
    response: &RawResponse,
    bucket: &str,
    assumed_role_arn: &str,
    credentials: SignedRequestCredentials<'_>,
    security_token: &str,
) {
    let response =
        s3_response_with_sanitized_body(response, credentials.access_key, &[security_token]);
    assert_shape(
        label,
        &response,
        &shape()
            .status(403)
            .headers(error_response_headers())
            .sub("assumed_role_arn", assumed_role_arn)
            .sub("bucket", bucket)
            .sub("key", label)
            .body(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                 <Error><Code>AccessDenied</Code>\
                 <Message>User: {assumed_role_arn} is not authorized to perform: \
                 s3:PutObject on resource: \"arn:aws:s3:::{bucket}/{key}\" because no \
                 identity-based policy allows the s3:PutObject action</Message>\
                 <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>",
            ),
    );
    println!("{label}: ok");
}

fn assert_s3_session_context_put_explicit_deny(
    label: &str,
    response: &RawResponse,
    bucket: &str,
    assumed_role_arn: &str,
    credentials: SignedRequestCredentials<'_>,
    security_token: &str,
) {
    let response =
        s3_response_with_sanitized_body(response, credentials.access_key, &[security_token]);
    assert_shape(
        label,
        &response,
        &shape()
            .status(403)
            .headers(error_response_headers())
            .sub("assumed_role_arn", assumed_role_arn)
            .sub("bucket", bucket)
            .sub("key", label)
            .body(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                 <Error><Code>AccessDenied</Code>\
                 <Message>User: {assumed_role_arn} is not authorized to perform: \
                 s3:PutObject on resource: \"arn:aws:s3:::{bucket}/{key}\" with an \
                 explicit deny in a resource-based policy</Message>\
                 <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>",
            ),
    );
    println!("{label}: ok");
}

fn run_s3_session_context_probes(
    endpoint: &str,
    bucket: &str,
    fixture: S3SessionContextProbeSet<'_>,
) {
    for (label, expected) in [
        ("context-role-principal", S3SessionContextExpected::Success),
        (
            "context-session-principal",
            S3SessionContextExpected::Success,
        ),
        (
            "context-principal-arn-role",
            S3SessionContextExpected::Success,
        ),
        ("context-userid-match", S3SessionContextExpected::Success),
        (
            "context-token-issue-after-lower",
            S3SessionContextExpected::Success,
        ),
        (
            "context-token-issue-before-upper",
            S3SessionContextExpected::Success,
        ),
        (
            "context-token-issue-deny-before-lower",
            S3SessionContextExpected::Success,
        ),
        (
            "context-principal-arn-session",
            S3SessionContextExpected::AccessDenied,
        ),
        (
            "context-userid-mismatch",
            S3SessionContextExpected::AccessDenied,
        ),
        (
            "context-token-issue-before-lower",
            S3SessionContextExpected::AccessDenied,
        ),
        (
            "context-token-issue-after-upper",
            S3SessionContextExpected::AccessDenied,
        ),
        (
            "context-token-issue-deny-before-upper",
            S3SessionContextExpected::ExplicitResourceDeny,
        ),
    ] {
        let response = send_signed_request_for_service_with_credentials(
            "PUT",
            &format!("{endpoint}/{label}"),
            b"",
            [("x-amz-security-token", fixture.security_token)],
            "s3",
            fixture.credentials,
        );
        match expected {
            S3SessionContextExpected::Success => {
                assert_s3_session_context_put_success(label, &response);
            }
            S3SessionContextExpected::AccessDenied => {
                assert_s3_session_context_put_access_denied(
                    label,
                    &response,
                    bucket,
                    fixture.assumed_role_arn,
                    fixture.credentials,
                    fixture.security_token,
                );
            }
            S3SessionContextExpected::ExplicitResourceDeny => {
                assert_s3_session_context_put_explicit_deny(
                    label,
                    &response,
                    bucket,
                    fixture.assumed_role_arn,
                    fixture.credentials,
                    fixture.security_token,
                );
            }
        }
    }
}

struct S3RolePolicyMutationProbeSet<'a> {
    pre_credentials: SignedRequestCredentials<'a>,
    pre_security_token: &'a str,
    pre_assumed_role_arn: &'a str,
    post_credentials: SignedRequestCredentials<'a>,
    post_security_token: &'a str,
    post_assumed_role_arn: &'a str,
}

fn assert_s3_identity_policy_explicit_deny(
    label: &str,
    response: &RawResponse,
    bucket: &str,
    assumed_role_arn: &str,
    credentials: SignedRequestCredentials<'_>,
    security_token: &str,
) {
    let response =
        s3_response_with_sanitized_body(response, credentials.access_key, &[security_token]);
    assert_shape(
        label,
        &response,
        &shape()
            .status(403)
            .headers(error_response_headers())
            .sub("assumed_role_arn", assumed_role_arn)
            .sub("bucket", bucket)
            .sub("key", label)
            .body(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                 <Error><Code>AccessDenied</Code>\
                 <Message>User: {assumed_role_arn} is not authorized to perform: \
                 s3:PutObject on resource: \"arn:aws:s3:::{bucket}/{key}\" with an \
                 explicit deny in an identity-based policy</Message>\
                 <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>",
            ),
    );
    println!("{label}: ok");
}

fn assert_s3_put_signature_mismatch(
    label: &str,
    response: &RawResponse,
    endpoint: &str,
    credentials: SignedRequestCredentials<'_>,
    security_token: &str,
) {
    let parsed_endpoint = url::Url::parse(endpoint)
        .unwrap_or_else(|error| panic!("{label}: invalid S3 endpoint: {error}"));
    let host = parsed_endpoint
        .host_str()
        .unwrap_or_else(|| panic!("{label}: S3 endpoint has no host"));
    let path = parsed_endpoint.path();
    let empty_payload_hash = auth::canonical::sha256_hex(b"");
    assert_s3_signature_mismatch(
        label,
        response,
        credentials,
        &[security_token],
        None,
        None,
        |amz_date| {
            format!(
                "PUT\n{path}\n\nhost:{host}\n\
                 x-amz-content-sha256:{empty_payload_hash}\n\
                 x-amz-date:{amz_date}\n\
                 x-amz-security-token:{security_token}\n\n\
                 host;x-amz-content-sha256;x-amz-date;x-amz-security-token\n\
                 {empty_payload_hash}"
            )
        },
    );
}

fn run_s3_role_policy_mutation_probes(
    endpoint: &str,
    bucket: &str,
    fixture: S3RolePolicyMutationProbeSet<'_>,
) {
    for (label, credentials, security_token, assumed_role_arn) in [
        (
            "role-policy-mutation-pre-session-explicit-deny",
            fixture.pre_credentials,
            fixture.pre_security_token,
            fixture.pre_assumed_role_arn,
        ),
        (
            "role-policy-mutation-post-session-explicit-deny",
            fixture.post_credentials,
            fixture.post_security_token,
            fixture.post_assumed_role_arn,
        ),
    ] {
        let response = send_signed_request_for_service_with_credentials(
            "PUT",
            &format!("{endpoint}/{label}"),
            b"",
            [("x-amz-security-token", security_token)],
            "s3",
            credentials,
        );
        assert_s3_identity_policy_explicit_deny(
            label,
            &response,
            bucket,
            assumed_role_arn,
            credentials,
            security_token,
        );
    }

    let label = "role-policy-mutation-pre-session-bad-signature";
    let wrong_secret = "0".repeat(40);
    let bad_signature_credentials = SignedRequestCredentials {
        secret_key: &wrong_secret,
        ..fixture.pre_credentials
    };
    let request_endpoint = format!("{endpoint}/{label}");
    let response = send_signed_request_for_service_with_credentials(
        "PUT",
        &request_endpoint,
        b"",
        [("x-amz-security-token", fixture.pre_security_token)],
        "s3",
        bad_signature_credentials,
    );
    assert_s3_put_signature_mismatch(
        label,
        &response,
        &request_endpoint,
        bad_signature_credentials,
        fixture.pre_security_token,
    );
}

fn build_s3_root_presigned_request(
    endpoint: &str,
    credentials: SignedRequestCredentials<'_>,
    query_token: Option<&str>,
    signed_header_token: Option<&str>,
) -> PresignedRequest {
    build_s3_root_presigned_request_for_service(
        endpoint,
        credentials,
        &query_token.into_iter().collect::<Vec<_>>(),
        &signed_header_token.into_iter().collect::<Vec<_>>(),
        "s3",
    )
}

fn build_s3_root_presigned_request_for_service(
    endpoint: &str,
    credentials: SignedRequestCredentials<'_>,
    query_tokens: &[&str],
    signed_header_tokens: &[&str],
    service: &str,
) -> PresignedRequest {
    let url = query_tokens
        .iter()
        .map(|token| {
            format!(
                "X-Amz-Security-Token={}",
                auth::canonical::uri_encode(token)
            )
        })
        .collect::<Vec<_>>()
        .join("&");
    let url = if url.is_empty() {
        endpoint.to_string()
    } else {
        format!("{endpoint}?{url}")
    };
    let headers = signed_header_tokens
        .iter()
        .map(|token| ("x-amz-security-token", *token))
        .collect::<Vec<_>>();
    presign_url_for_service_with_credentials(
        "GET",
        &url,
        Duration::from_secs(900),
        headers,
        None,
        service,
        credentials,
    )
}

fn fetch_s3_presigned_request(
    endpoint: &str,
    presigned: &PresignedRequest,
    unsigned_header_token: Option<&str>,
) -> RawResponse {
    let mut request =
        build_test_agent(endpoint, None, Duration::from_secs(120)).get(presigned.uri());
    for (name, value) in presigned.headers() {
        request = request.header(name, value);
    }
    if let Some(token) = unsigned_header_token {
        request = request.header("x-amz-security-token", token);
    }
    let mut response = request.call().expect("presigned AWS transport error");
    let headers = response
        .headers()
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_string(),
                value
                    .to_str()
                    .expect("presigned AWS response header is valid UTF-8")
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

fn required_presigned_query_value(parsed: &url::Url, name: &str, label: &str) -> String {
    parsed
        .query_pairs()
        .find_map(|(candidate, value)| (candidate == name).then(|| value.into_owned()))
        .unwrap_or_else(|| panic!("{label}: presigned URL is missing {name}"))
}

fn assert_s3_presigned_signature_mismatch(
    label: &str,
    response: &RawResponse,
    presigned: &PresignedRequest,
    credentials: SignedRequestCredentials<'_>,
    security_tokens: &[&str],
) {
    let parsed = url::Url::parse(presigned.uri())
        .unwrap_or_else(|error| panic!("{label}: invalid presigned URL: {error}"));
    let amz_date = required_presigned_query_value(&parsed, "X-Amz-Date", label);
    let signature = required_presigned_query_value(&parsed, "X-Amz-Signature", label);
    let signed_headers = required_presigned_query_value(&parsed, "X-Amz-SignedHeaders", label);
    let query_without_signature = parsed
        .query()
        .unwrap_or_default()
        .split('&')
        .filter(|part| !part.starts_with("X-Amz-Signature="))
        .collect::<Vec<_>>()
        .join("&");
    let canonical_query = auth::canonical::canonical_query_string(&query_without_signature);
    let host = parsed
        .host_str()
        .map(|host| {
            parsed
                .port()
                .map_or_else(|| host.to_string(), |port| format!("{host}:{port}"))
        })
        .unwrap_or_else(|| panic!("{label}: presigned URL has no host"));
    let mut owned_headers = vec![("host".to_string(), host)];
    owned_headers.extend(
        presigned
            .headers()
            .map(|(name, value)| (name.to_string(), value.to_string())),
    );
    let header_refs = owned_headers
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect::<Vec<_>>();
    let canonical_headers = auth::canonical::canonical_headers(&header_refs);
    assert_s3_signature_mismatch(
        label,
        response,
        credentials,
        security_tokens,
        Some(&amz_date),
        Some(&signature),
        |_| {
            auth::canonical::canonical_request(
                "GET",
                parsed.path(),
                &canonical_query,
                &canonical_headers,
                &signed_headers,
                "UNSIGNED-PAYLOAD",
            )
        },
    );
}

#[derive(Clone, Copy)]
enum S3PresignedAuthExpected {
    AccessDenied,
    HeadersNotSigned,
    InvalidAccessKey,
    SignatureMismatch,
}

#[derive(Clone, Copy)]
struct S3PresignedSessionProbeSet<'a> {
    live_credentials: SignedRequestCredentials<'a>,
    live_security_token: &'a str,
    live_role_name: &'a str,
    live_role_session_name: &'a str,
    other_live_security_token: &'a str,
    old_credentials: SignedRequestCredentials<'a>,
    old_security_token: &'a str,
}

#[derive(Clone, Copy)]
struct S3PresignedProbe<'a> {
    label: &'a str,
    credentials: SignedRequestCredentials<'a>,
    query_token: Option<&'a str>,
    signed_header_token: Option<&'a str>,
    unsigned_header_token: Option<&'a str>,
    expected: S3PresignedAuthExpected,
}

fn run_s3_presigned_session_authentication_probes(
    endpoint: &str,
    account_id: &str,
    fixture: S3PresignedSessionProbeSet<'_>,
) {
    let wrong_secret = "0".repeat(40);
    let live_bad_signature_credentials = SignedRequestCredentials {
        secret_key: &wrong_secret,
        ..fixture.live_credentials
    };
    let old_bad_signature_credentials = SignedRequestCredentials {
        secret_key: &wrong_secret,
        ..fixture.old_credentials
    };
    let probes = [
        S3PresignedProbe {
            label: "s3-presigned-auth-live-query-token-valid",
            credentials: fixture.live_credentials,
            query_token: Some(fixture.live_security_token),
            signed_header_token: None,
            unsigned_header_token: None,
            expected: S3PresignedAuthExpected::AccessDenied,
        },
        S3PresignedProbe {
            label: "s3-presigned-auth-live-token-missing-valid-signature",
            credentials: fixture.live_credentials,
            query_token: None,
            signed_header_token: None,
            unsigned_header_token: None,
            expected: S3PresignedAuthExpected::InvalidAccessKey,
        },
        S3PresignedProbe {
            label: "s3-presigned-auth-live-query-token-mismatched-valid-signature",
            credentials: fixture.live_credentials,
            query_token: Some(fixture.other_live_security_token),
            signed_header_token: None,
            unsigned_header_token: None,
            expected: S3PresignedAuthExpected::InvalidAccessKey,
        },
        S3PresignedProbe {
            label: "s3-presigned-auth-live-query-token-valid-bad-signature",
            credentials: live_bad_signature_credentials,
            query_token: Some(fixture.live_security_token),
            signed_header_token: None,
            unsigned_header_token: None,
            expected: S3PresignedAuthExpected::SignatureMismatch,
        },
        S3PresignedProbe {
            label: "s3-presigned-auth-live-token-missing-bad-signature",
            credentials: live_bad_signature_credentials,
            query_token: None,
            signed_header_token: None,
            unsigned_header_token: None,
            expected: S3PresignedAuthExpected::InvalidAccessKey,
        },
        S3PresignedProbe {
            label: "s3-presigned-auth-live-query-token-mismatched-bad-signature",
            credentials: live_bad_signature_credentials,
            query_token: Some(fixture.other_live_security_token),
            signed_header_token: None,
            unsigned_header_token: None,
            expected: S3PresignedAuthExpected::InvalidAccessKey,
        },
        S3PresignedProbe {
            label: "s3-presigned-auth-live-signed-header-token-valid",
            credentials: fixture.live_credentials,
            query_token: None,
            signed_header_token: Some(fixture.live_security_token),
            unsigned_header_token: None,
            expected: S3PresignedAuthExpected::AccessDenied,
        },
        S3PresignedProbe {
            label: "s3-presigned-auth-live-signed-header-overrides-query-mismatch",
            credentials: fixture.live_credentials,
            query_token: Some(fixture.other_live_security_token),
            signed_header_token: Some(fixture.live_security_token),
            unsigned_header_token: None,
            expected: S3PresignedAuthExpected::AccessDenied,
        },
        S3PresignedProbe {
            label: "s3-presigned-auth-live-signed-header-mismatch-overrides-query-valid",
            credentials: fixture.live_credentials,
            query_token: Some(fixture.live_security_token),
            signed_header_token: Some(fixture.other_live_security_token),
            unsigned_header_token: None,
            expected: S3PresignedAuthExpected::InvalidAccessKey,
        },
        S3PresignedProbe {
            label:
                "s3-presigned-auth-live-signed-header-mismatch-overrides-query-valid-bad-signature",
            credentials: live_bad_signature_credentials,
            query_token: Some(fixture.live_security_token),
            signed_header_token: Some(fixture.other_live_security_token),
            unsigned_header_token: None,
            expected: S3PresignedAuthExpected::InvalidAccessKey,
        },
        S3PresignedProbe {
            label:
                "s3-presigned-auth-live-signed-header-valid-overrides-query-mismatch-bad-signature",
            credentials: live_bad_signature_credentials,
            query_token: Some(fixture.other_live_security_token),
            signed_header_token: Some(fixture.live_security_token),
            unsigned_header_token: None,
            expected: S3PresignedAuthExpected::SignatureMismatch,
        },
        S3PresignedProbe {
            label: "s3-presigned-auth-live-unsigned-header-token-valid",
            credentials: fixture.live_credentials,
            query_token: None,
            signed_header_token: None,
            unsigned_header_token: Some(fixture.live_security_token),
            expected: S3PresignedAuthExpected::HeadersNotSigned,
        },
        S3PresignedProbe {
            label: "s3-presigned-auth-live-query-valid-unsigned-header-mismatched",
            credentials: fixture.live_credentials,
            query_token: Some(fixture.live_security_token),
            signed_header_token: None,
            unsigned_header_token: Some(fixture.other_live_security_token),
            expected: S3PresignedAuthExpected::HeadersNotSigned,
        },
        S3PresignedProbe {
            label: "s3-presigned-auth-live-query-mismatched-unsigned-header-valid",
            credentials: fixture.live_credentials,
            query_token: Some(fixture.other_live_security_token),
            signed_header_token: None,
            unsigned_header_token: Some(fixture.live_security_token),
            expected: S3PresignedAuthExpected::HeadersNotSigned,
        },
        S3PresignedProbe {
            label: "s3-presigned-auth-live-unsigned-header-token-mismatched",
            credentials: fixture.live_credentials,
            query_token: None,
            signed_header_token: None,
            unsigned_header_token: Some(fixture.other_live_security_token),
            expected: S3PresignedAuthExpected::HeadersNotSigned,
        },
        S3PresignedProbe {
            label: "s3-presigned-auth-live-signed-header-token-valid-bad-signature",
            credentials: live_bad_signature_credentials,
            query_token: None,
            signed_header_token: Some(fixture.live_security_token),
            unsigned_header_token: None,
            expected: S3PresignedAuthExpected::SignatureMismatch,
        },
        S3PresignedProbe {
            label: "s3-presigned-auth-live-unsigned-header-token-valid-bad-signature",
            credentials: live_bad_signature_credentials,
            query_token: None,
            signed_header_token: None,
            unsigned_header_token: Some(fixture.live_security_token),
            expected: S3PresignedAuthExpected::HeadersNotSigned,
        },
        S3PresignedProbe {
            label: "s3-presigned-auth-old-session-query-token-valid",
            credentials: fixture.old_credentials,
            query_token: Some(fixture.old_security_token),
            signed_header_token: None,
            unsigned_header_token: None,
            expected: S3PresignedAuthExpected::InvalidAccessKey,
        },
        S3PresignedProbe {
            label: "s3-presigned-auth-old-session-query-token-valid-bad-signature",
            credentials: old_bad_signature_credentials,
            query_token: Some(fixture.old_security_token),
            signed_header_token: None,
            unsigned_header_token: None,
            expected: S3PresignedAuthExpected::InvalidAccessKey,
        },
        S3PresignedProbe {
            label: "s3-presigned-auth-old-session-token-missing",
            credentials: fixture.old_credentials,
            query_token: None,
            signed_header_token: None,
            unsigned_header_token: None,
            expected: S3PresignedAuthExpected::InvalidAccessKey,
        },
        S3PresignedProbe {
            label: "s3-presigned-auth-old-session-signed-header-token-valid",
            credentials: fixture.old_credentials,
            query_token: None,
            signed_header_token: Some(fixture.old_security_token),
            unsigned_header_token: None,
            expected: S3PresignedAuthExpected::InvalidAccessKey,
        },
        S3PresignedProbe {
            label: "s3-presigned-auth-old-session-signed-header-token-valid-bad-signature",
            credentials: old_bad_signature_credentials,
            query_token: None,
            signed_header_token: Some(fixture.old_security_token),
            unsigned_header_token: None,
            expected: S3PresignedAuthExpected::InvalidAccessKey,
        },
    ];

    for probe in probes {
        let presigned = build_s3_root_presigned_request(
            endpoint,
            probe.credentials,
            probe.query_token,
            probe.signed_header_token,
        );
        let response =
            fetch_s3_presigned_request(endpoint, &presigned, probe.unsigned_header_token);
        let sensitive_tokens = [
            probe.query_token,
            probe.signed_header_token,
            probe.unsigned_header_token,
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        match probe.expected {
            S3PresignedAuthExpected::AccessDenied => {
                let assumed_role_arn = format!(
                    "arn:aws:sts::{account_id}:assumed-role/{}/{}",
                    fixture.live_role_name, fixture.live_role_session_name
                );
                assert_s3_list_buckets_access_denied(
                    probe.label,
                    &response,
                    &assumed_role_arn,
                    probe.credentials.access_key,
                    &sensitive_tokens,
                );
            }
            S3PresignedAuthExpected::HeadersNotSigned => assert_s3_headers_not_signed(
                probe.label,
                &response,
                probe.credentials.access_key,
                &sensitive_tokens,
            ),
            S3PresignedAuthExpected::InvalidAccessKey => assert_s3_invalid_access_key(
                probe.label,
                &response,
                probe.credentials.access_key,
                &sensitive_tokens,
            ),
            S3PresignedAuthExpected::SignatureMismatch => {
                assert_s3_presigned_signature_mismatch(
                    probe.label,
                    &response,
                    &presigned,
                    probe.credentials,
                    &sensitive_tokens,
                );
            }
        }
    }
}

fn assert_s3_presigned_wrong_region_scope(
    label: &str,
    response: &RawResponse,
    wrong_region: &str,
    expected_region: &str,
    access_key: &str,
    security_tokens: &[&str],
) {
    let response = s3_response_with_sanitized_body(response, access_key, security_tokens);
    let message = format!(
        "Error parsing the X-Amz-Credential parameter; the region '{wrong_region}' is wrong; \
         expecting '{expected_region}'"
    );
    assert_shape(
        label,
        &response,
        &shape()
            .status(400)
            .headers(error_response_headers())
            .header("x-amz-bucket-region", expected_region)
            .body(expected_error::with_region(
                "AuthorizationQueryParametersError",
                &message,
                expected_region,
            )),
    );
    println!("{label}: ok");
}

fn assert_s3_presigned_wrong_service_scope(
    label: &str,
    response: &RawResponse,
    expected_region: &str,
    access_key: &str,
    security_tokens: &[&str],
) {
    let response = s3_response_with_sanitized_body(response, access_key, security_tokens);
    assert_shape(
        label,
        &response,
        &shape()
            .status(400)
            .headers(error_response_headers())
            .header("x-amz-bucket-region", expected_region)
            .body(expected_error::with_host_id(
                "AuthorizationQueryParametersError",
                "Error parsing the X-Amz-Credential parameter; incorrect service \"sts\". This endpoint belongs to \"s3\".",
            )),
    );
    println!("{label}: ok");
}

#[derive(Clone, Copy)]
enum S3PresignedScopeExpected {
    WrongRegion,
    WrongService,
}

fn run_s3_presigned_scope_probes(
    endpoint: &str,
    account_id: &str,
    bucket: &str,
    fixture: S3PresignedSessionProbeSet<'_>,
) {
    let positive = build_s3_root_presigned_request_for_service(
        endpoint,
        fixture.live_credentials,
        &[fixture.live_security_token],
        &[],
        "s3",
    );
    let positive_response = fetch_s3_presigned_request(endpoint, &positive, None);
    assert_s3_list_bucket_access_denied(
        "s3-presigned-scope-live-role-correct-scope",
        &positive_response,
        S3ListBucketDeniedFixture {
            account_id,
            bucket,
            role_name: fixture.live_role_name,
            role_session_name: fixture.live_role_session_name,
            credentials: fixture.live_credentials,
            security_token: fixture.live_security_token,
        },
    );

    let wrong_region = if fixture.live_credentials.region == "us-east-1" {
        "us-west-2"
    } else {
        "us-east-1"
    };
    let wrong_secret = "0".repeat(40);
    for (scope, live_credentials, old_credentials, service, expected) in [
        (
            "wrong-region",
            SignedRequestCredentials {
                region: wrong_region,
                ..fixture.live_credentials
            },
            SignedRequestCredentials {
                region: wrong_region,
                ..fixture.old_credentials
            },
            "s3",
            S3PresignedScopeExpected::WrongRegion,
        ),
        (
            "wrong-service",
            fixture.live_credentials,
            fixture.old_credentials,
            "sts",
            S3PresignedScopeExpected::WrongService,
        ),
    ] {
        let bad_signature_credentials = SignedRequestCredentials {
            secret_key: &wrong_secret,
            ..live_credentials
        };
        let mut conflicting_query_uri = None;
        for (case, credentials, query_tokens, signed_header_tokens, unsigned_header_token) in [
            (
                "valid-query",
                live_credentials,
                vec![fixture.live_security_token],
                vec![],
                None,
            ),
            ("missing-token", live_credentials, vec![], vec![], None),
            ("empty-query", live_credentials, vec![""], vec![], None),
            (
                "malformed-query",
                live_credentials,
                vec!["not-a-session-token"],
                vec![],
                None,
            ),
            (
                "mismatched-query",
                live_credentials,
                vec![fixture.other_live_security_token],
                vec![],
                None,
            ),
            (
                "valid-query-bad-signature",
                bad_signature_credentials,
                vec![fixture.live_security_token],
                vec![],
                None,
            ),
            (
                "old-session-valid-query",
                old_credentials,
                vec![fixture.old_security_token],
                vec![],
                None,
            ),
            (
                "duplicate-identical-query",
                live_credentials,
                vec![fixture.live_security_token, fixture.live_security_token],
                vec![],
                None,
            ),
            (
                "duplicate-conflicting-query",
                live_credentials,
                vec![
                    fixture.live_security_token,
                    fixture.other_live_security_token,
                ],
                vec![],
                None,
            ),
            (
                "duplicate-conflicting-query-reversed",
                live_credentials,
                vec![
                    fixture.other_live_security_token,
                    fixture.live_security_token,
                ],
                vec![],
                None,
            ),
            (
                "valid-signed-header",
                live_credentials,
                vec![],
                vec![fixture.live_security_token],
                None,
            ),
            (
                "mismatched-signed-header-valid-query",
                live_credentials,
                vec![fixture.live_security_token],
                vec![fixture.other_live_security_token],
                None,
            ),
            (
                "empty-signed-header",
                live_credentials,
                vec![],
                vec![""],
                None,
            ),
            (
                "malformed-signed-header",
                live_credentials,
                vec![],
                vec!["not-a-session-token"],
                None,
            ),
            (
                "unsigned-header-valid-query",
                live_credentials,
                vec![fixture.live_security_token],
                vec![],
                Some(fixture.live_security_token),
            ),
        ] {
            let label = format!("s3-presigned-scope-{scope}-{case}");
            let presigned = build_s3_root_presigned_request_for_service(
                endpoint,
                credentials,
                &query_tokens,
                &signed_header_tokens,
                service,
            );
            let parsed_presigned = url::Url::parse(presigned.uri())
                .unwrap_or_else(|error| panic!("{label}: invalid presigned URL: {error}"));
            let wire_query_tokens = parsed_presigned
                .query_pairs()
                .filter_map(|(name, value)| {
                    (name == "X-Amz-Security-Token").then_some(value.into_owned())
                })
                .collect::<Vec<_>>();
            assert!(
                wire_query_tokens.len() == query_tokens.len()
                    && wire_query_tokens
                        .iter()
                        .zip(&query_tokens)
                        .all(|(wire, supplied)| wire == *supplied),
                "{label}: presigned URI did not preserve the supplied token-query wire order"
            );
            if case == "duplicate-conflicting-query" {
                conflicting_query_uri = Some(presigned.uri().to_string());
            } else if case == "duplicate-conflicting-query-reversed" {
                let forward_uri = conflicting_query_uri.as_deref().unwrap_or_else(|| {
                    panic!("{label}: forward conflicting-query URI was not captured")
                });
                assert!(
                    forward_uri != presigned.uri(),
                    "{label}: reversed duplicate query produced the same wire URI"
                );
            }
            let response = fetch_s3_presigned_request(endpoint, &presigned, unsigned_header_token);
            let sensitive_tokens = query_tokens
                .iter()
                .chain(signed_header_tokens.iter())
                .copied()
                .chain(unsigned_header_token)
                .collect::<Vec<_>>();
            match expected {
                S3PresignedScopeExpected::WrongRegion => {
                    assert_s3_presigned_wrong_region_scope(
                        &label,
                        &response,
                        wrong_region,
                        fixture.live_credentials.region,
                        credentials.access_key,
                        &sensitive_tokens,
                    );
                }
                S3PresignedScopeExpected::WrongService => {
                    assert_s3_presigned_wrong_service_scope(
                        &label,
                        &response,
                        fixture.live_credentials.region,
                        credentials.access_key,
                        &sensitive_tokens,
                    );
                }
            }
        }
    }

    let both_wrong_credentials = SignedRequestCredentials {
        region: wrong_region,
        ..fixture.live_credentials
    };
    let presigned = build_s3_root_presigned_request_for_service(
        endpoint,
        both_wrong_credentials,
        &[fixture.live_security_token],
        &[],
        "sts",
    );
    let response = fetch_s3_presigned_request(endpoint, &presigned, None);
    assert_s3_presigned_wrong_region_scope(
        "s3-presigned-scope-both-wrong-valid-query",
        &response,
        wrong_region,
        fixture.live_credentials.region,
        fixture.live_credentials.access_key,
        &[fixture.live_security_token],
    );
}

#[derive(Clone, Copy)]
struct S3PostSessionProbeSet<'a> {
    live_credentials: SignedRequestCredentials<'a>,
    live_security_token: &'a str,
    live_role_name: &'a str,
    live_role_session_name: &'a str,
    other_live_security_token: &'a str,
    old_credentials: SignedRequestCredentials<'a>,
    old_security_token: &'a str,
}

#[derive(Clone, Copy)]
enum S3PostAuthExpected {
    AccessDenied,
    ExpiredToken,
    InvalidAccessKey,
    InvalidToken,
    NoAccessKeyPresented,
    PolicyConditionFailed,
    SignatureMismatch,
}

#[derive(Clone, Copy)]
enum S3PostFormTokens<'a> {
    Missing,
    One(&'a str),
    Two(&'a str, &'a str),
}

impl<'a> S3PostFormTokens<'a> {
    fn append_to(self, fields: &mut Vec<(String, String)>) {
        let mut append = |token: &'a str| {
            fields.push(("x-amz-security-token".to_string(), token.to_string()));
        };
        match self {
            Self::Missing => {}
            Self::One(token) => append(token),
            Self::Two(first, second) => {
                append(first);
                append(second);
            }
        }
    }

    fn values(self) -> [Option<&'a str>; 2] {
        match self {
            Self::Missing => [None, None],
            Self::One(token) => [Some(token), None],
            Self::Two(first, second) => [Some(first), Some(second)],
        }
    }
}

#[derive(Clone, Copy)]
struct S3PostProbe<'a> {
    label: &'a str,
    credentials: SignedRequestCredentials<'a>,
    policy_token: Option<&'a str>,
    form_tokens: S3PostFormTokens<'a>,
    header_token: Option<&'a str>,
    expected: S3PostAuthExpected,
}

struct S3PostResult {
    response: RawResponse,
    credential: String,
    policy: String,
    signature: String,
}

#[derive(Clone, Copy)]
struct S3PostScopeProbe<'a> {
    label: &'a str,
    credentials: SignedRequestCredentials<'a>,
    service: &'a str,
    policy_token: Option<&'a str>,
    form_tokens: S3PostFormTokens<'a>,
    header_token: Option<&'a str>,
}

fn required_post_field(fields: &[(String, String)], name: &str, label: &str) -> String {
    fields
        .iter()
        .find_map(|(candidate, value)| (candidate == name).then(|| value.clone()))
        .unwrap_or_else(|| panic!("{label}: POST Object fields are missing {name}"))
}

fn send_s3_post_probe(endpoint: &str, bucket: &str, probe: S3PostProbe<'_>) -> S3PostResult {
    let key = probe.label;
    let token_conditions = probe
        .policy_token
        .map(|token| vec![serde_json::json!({"x-amz-security-token": token})])
        .unwrap_or_default();
    let mut fields = sigv4_post_fields_for_credentials(
        probe.credentials.access_key,
        probe.credentials.secret_key,
        probe.credentials.region,
        bucket,
        key,
        &token_conditions,
    );
    probe.form_tokens.append_to(&mut fields);
    let headers = probe
        .header_token
        .map(|token| vec![("x-amz-security-token".to_string(), token.to_string())])
        .unwrap_or_default();
    let policy = required_post_field(&fields, "policy", probe.label);
    let credential = required_post_field(&fields, "x-amz-credential", probe.label);
    let signature = required_post_field(&fields, "x-amz-signature", probe.label);
    let response = post_object_raw_to_test_endpoint_with_headers(
        endpoint,
        None,
        bucket,
        &fields,
        b"STS POST Object oracle",
        "oracle.txt",
        &headers,
    );
    S3PostResult {
        response,
        credential,
        policy,
        signature,
    }
}

fn send_s3_post_scope_probe(
    endpoint: &str,
    bucket: &str,
    probe: S3PostScopeProbe<'_>,
) -> S3PostResult {
    let token_conditions = probe
        .policy_token
        .map(|token| vec![serde_json::json!({"x-amz-security-token": token})])
        .unwrap_or_default();
    let mut fields = sigv4_post_fields_for_service_with_credentials(
        probe.credentials,
        probe.service,
        bucket,
        probe.label,
        &token_conditions,
    );
    probe.form_tokens.append_to(&mut fields);
    let headers = probe
        .header_token
        .map(|token| vec![("x-amz-security-token".to_string(), token.to_string())])
        .unwrap_or_default();
    let policy = required_post_field(&fields, "policy", probe.label);
    let credential = required_post_field(&fields, "x-amz-credential", probe.label);
    let signature = required_post_field(&fields, "x-amz-signature", probe.label);
    let response = post_object_raw_to_test_endpoint_with_headers(
        endpoint,
        None,
        bucket,
        &fields,
        b"STS POST Object scope oracle",
        "oracle.txt",
        &headers,
    );
    S3PostResult {
        response,
        credential,
        policy,
        signature,
    }
}

fn s3_post_response_with_sanitized_body(
    response: &RawResponse,
    access_key: &str,
    security_tokens: &[&str],
    policy: &str,
) -> RawResponse {
    let mut body = response
        .body
        .replace(&spaced_hex(policy), "POST_POLICY_BYTES");
    body = body.replace(policy, "POST_POLICY");
    RawResponse {
        status: response.status,
        headers: response.headers.clone(),
        body: sanitize_s3_text(&body, access_key, security_tokens),
        body_read_error: response.body_read_error.clone(),
    }
}

fn assert_s3_post_access_denied(
    probe: S3PostProbe<'_>,
    result: &S3PostResult,
    bucket: &str,
    assumed_role_arn: &str,
    security_tokens: &[&str],
) {
    let response = s3_post_response_with_sanitized_body(
        &result.response,
        probe.credentials.access_key,
        security_tokens,
        &result.policy,
    );
    assert_shape(
        probe.label,
        &response,
        &shape()
            .status(403)
            .headers(error_response_headers())
            .sub("assumed_role_arn", assumed_role_arn)
            .sub("bucket", bucket)
            .sub("key", probe.label)
            .body(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                 <Error><Code>AccessDenied</Code>\
                 <Message>User: {assumed_role_arn} is not authorized to perform: \
                 s3:PutObject on resource: \"arn:aws:s3:::{bucket}/{key}\" because no \
                 identity-based policy allows the s3:PutObject action</Message>\
                 <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>",
            ),
    );
    println!("{}: ok", probe.label);
}

fn assert_s3_post_invalid_access_key(
    probe: S3PostProbe<'_>,
    result: &S3PostResult,
    security_tokens: &[&str],
) {
    assert!(
        required_xml_text(&result.response, "AWSAccessKeyId", probe.label)
            == probe.credentials.access_key,
        "{}: S3 did not echo the session access key",
        probe.label
    );
    let response = s3_post_response_with_sanitized_body(
        &result.response,
        probe.credentials.access_key,
        security_tokens,
        &result.policy,
    );
    assert_s3_invalid_access_key_shape(probe.label, &response);
}

fn assert_s3_post_no_access_key_presented(
    probe: S3PostProbe<'_>,
    result: &S3PostResult,
    security_tokens: &[&str],
) {
    let response = s3_post_response_with_sanitized_body(
        &result.response,
        probe.credentials.access_key,
        security_tokens,
        &result.policy,
    );
    assert_shape(
        probe.label,
        &response,
        &shape().status(403).headers(error_response_headers()).body(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                 <Error><Code>AccessDenied</Code>\
                 <Message>No AWSAccessKey was presented.</Message>\
                 <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>",
        ),
    );
    println!("{}: ok", probe.label);
}

fn assert_s3_post_invalid_token(
    probe: S3PostProbe<'_>,
    result: &S3PostResult,
    security_tokens: &[&str],
    rejected_token: &str,
) {
    assert!(
        required_xml_text(&result.response, "Token-0", probe.label) == rejected_token,
        "{}: S3 did not echo the rejected session token",
        probe.label
    );
    let response = s3_post_response_with_sanitized_body(
        &result.response,
        probe.credentials.access_key,
        security_tokens,
        &result.policy,
    );
    assert_shape(
        probe.label,
        &response,
        &shape()
            .status(400)
            .headers(error_response_headers())
            .body(expected_error::invalid_token(
                "The provided token is malformed or otherwise invalid.",
                "SESSION_TOKEN",
            )),
    );
    println!("{}: ok", probe.label);
}

fn assert_s3_expired_token_shape(label: &str, response: &RawResponse) {
    assert_shape(
        label,
        response,
        &shape().status(400).headers(error_response_headers()).body(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                 <Error><Code>ExpiredToken</Code>\
                 <Message>The provided token has expired.</Message>\
                 <Token-0>SESSION_TOKEN</Token-0>\
                 <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>",
        ),
    );
    println!("{label}: ok");
}

fn assert_s3_expired_token(
    label: &str,
    response: &RawResponse,
    credentials: SignedRequestCredentials<'_>,
    security_token: &str,
) {
    assert!(
        required_xml_text(response, "Token-0", label) == security_token,
        "{label}: S3 did not echo the expired session token"
    );
    let response =
        s3_response_with_sanitized_body(response, credentials.access_key, &[security_token]);
    assert_s3_expired_token_shape(label, &response);
}

fn assert_s3_post_expired_token(
    probe: S3PostProbe<'_>,
    result: &S3PostResult,
    security_token: &str,
) {
    assert!(
        required_xml_text(&result.response, "Token-0", probe.label) == security_token,
        "{}: S3 did not echo the expired session token",
        probe.label
    );
    let response = s3_post_response_with_sanitized_body(
        &result.response,
        probe.credentials.access_key,
        &[security_token],
        &result.policy,
    );
    assert_s3_expired_token_shape(probe.label, &response);
}

fn assert_s3_post_signature_mismatch(
    probe: S3PostProbe<'_>,
    result: &S3PostResult,
    security_tokens: &[&str],
) {
    assert!(
        required_xml_text(&result.response, "AWSAccessKeyId", probe.label)
            == probe.credentials.access_key,
        "{}: S3 did not echo the session access key",
        probe.label
    );
    assert!(
        required_xml_text(&result.response, "StringToSign", probe.label) == result.policy,
        "{}: S3 did not echo the POST policy as StringToSign",
        probe.label
    );
    assert!(
        required_xml_text(&result.response, "StringToSignBytes", probe.label)
            == spaced_hex(&result.policy),
        "{}: StringToSignBytes does not encode the POST policy",
        probe.label
    );
    let signature = required_xml_text(&result.response, "SignatureProvided", probe.label);
    assert!(
        signature.len() == 64
            && signature
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "{}: malformed provided signature",
        probe.label
    );
    assert!(
        signature == result.signature,
        "{}: S3 did not echo the POST policy signature",
        probe.label
    );
    let response = s3_post_response_with_sanitized_body(
        &result.response,
        probe.credentials.access_key,
        security_tokens,
        &result.policy,
    );
    assert_shape(
        probe.label,
        &response,
        &shape()
            .status(403)
            .headers(error_response_headers())
            .sub("signature", signature)
            .body(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                 <Error><Code>SignatureDoesNotMatch</Code>\
                 <Message>The request signature we calculated does not match the signature \
                 you provided. Check your key and signing method.</Message>\
                 <AWSAccessKeyId>SESSION_ACCESS_KEY</AWSAccessKeyId>\
                 <StringToSign>POST_POLICY</StringToSign>\
                 <SignatureProvided>{signature}</SignatureProvided>\
                 <StringToSignBytes>POST_POLICY_BYTES</StringToSignBytes>\
                 <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>",
            ),
    );
    println!("{}: ok", probe.label);
}

fn assert_s3_post_policy_condition_failed(
    probe: S3PostProbe<'_>,
    result: &S3PostResult,
    security_tokens: &[&str],
) {
    let response = s3_post_response_with_sanitized_body(
        &result.response,
        probe.credentials.access_key,
        security_tokens,
        &result.policy,
    );
    assert_shape(
        probe.label,
        &response,
        &shape().status(403).headers(error_response_headers()).body(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                 <Error><Code>AccessDenied</Code>\
                 <Message>Invalid according to Policy: Policy Condition failed: \
                 [\"eq\", \"$x-amz-security-token\", \"SESSION_TOKEN\"]</Message>\
                 <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>",
        ),
    );
    println!("{}: ok", probe.label);
}

fn run_s3_post_session_authentication_probes(
    endpoint: &str,
    account_id: &str,
    bucket: &str,
    fixture: S3PostSessionProbeSet<'_>,
) {
    let wrong_secret = "0".repeat(40);
    let malformed_security_token = "malformed-session-token";
    let live_bad_signature_credentials = SignedRequestCredentials {
        secret_key: &wrong_secret,
        ..fixture.live_credentials
    };
    let old_bad_signature_credentials = SignedRequestCredentials {
        secret_key: &wrong_secret,
        ..fixture.old_credentials
    };
    let probes = [
        S3PostProbe {
            label: "s3-post-auth-live-form-token-valid",
            credentials: fixture.live_credentials,
            policy_token: Some(fixture.live_security_token),
            form_tokens: S3PostFormTokens::One(fixture.live_security_token),
            header_token: None,
            expected: S3PostAuthExpected::AccessDenied,
        },
        S3PostProbe {
            label: "s3-post-auth-live-token-missing-valid-signature",
            credentials: fixture.live_credentials,
            policy_token: None,
            form_tokens: S3PostFormTokens::Missing,
            header_token: None,
            expected: S3PostAuthExpected::InvalidAccessKey,
        },
        S3PostProbe {
            label: "s3-post-auth-live-form-token-mismatched-valid-signature",
            credentials: fixture.live_credentials,
            policy_token: Some(fixture.other_live_security_token),
            form_tokens: S3PostFormTokens::One(fixture.other_live_security_token),
            header_token: None,
            expected: S3PostAuthExpected::InvalidAccessKey,
        },
        S3PostProbe {
            label: "s3-post-auth-live-form-token-empty-valid-signature",
            credentials: fixture.live_credentials,
            policy_token: Some(""),
            form_tokens: S3PostFormTokens::One(""),
            header_token: None,
            expected: S3PostAuthExpected::InvalidAccessKey,
        },
        S3PostProbe {
            label: "s3-post-auth-live-form-token-malformed-valid-signature",
            credentials: fixture.live_credentials,
            policy_token: Some(malformed_security_token),
            form_tokens: S3PostFormTokens::One(malformed_security_token),
            header_token: None,
            expected: S3PostAuthExpected::InvalidToken,
        },
        S3PostProbe {
            label: "s3-post-auth-live-form-token-valid-bad-signature",
            credentials: live_bad_signature_credentials,
            policy_token: Some(fixture.live_security_token),
            form_tokens: S3PostFormTokens::One(fixture.live_security_token),
            header_token: None,
            expected: S3PostAuthExpected::SignatureMismatch,
        },
        S3PostProbe {
            label: "s3-post-auth-live-token-missing-bad-signature",
            credentials: live_bad_signature_credentials,
            policy_token: None,
            form_tokens: S3PostFormTokens::Missing,
            header_token: None,
            expected: S3PostAuthExpected::InvalidAccessKey,
        },
        S3PostProbe {
            label: "s3-post-auth-live-form-token-mismatched-bad-signature",
            credentials: live_bad_signature_credentials,
            policy_token: Some(fixture.other_live_security_token),
            form_tokens: S3PostFormTokens::One(fixture.other_live_security_token),
            header_token: None,
            expected: S3PostAuthExpected::InvalidAccessKey,
        },
        S3PostProbe {
            label: "s3-post-auth-live-form-token-empty-bad-signature",
            credentials: live_bad_signature_credentials,
            policy_token: Some(""),
            form_tokens: S3PostFormTokens::One(""),
            header_token: None,
            expected: S3PostAuthExpected::InvalidAccessKey,
        },
        S3PostProbe {
            label: "s3-post-auth-live-form-token-malformed-bad-signature",
            credentials: live_bad_signature_credentials,
            policy_token: Some(malformed_security_token),
            form_tokens: S3PostFormTokens::One(malformed_security_token),
            header_token: None,
            expected: S3PostAuthExpected::InvalidToken,
        },
        S3PostProbe {
            label: "s3-post-auth-live-form-token-duplicate-valid-then-mismatched",
            credentials: fixture.live_credentials,
            policy_token: Some(fixture.live_security_token),
            form_tokens: S3PostFormTokens::Two(
                fixture.live_security_token,
                fixture.other_live_security_token,
            ),
            header_token: None,
            expected: S3PostAuthExpected::InvalidAccessKey,
        },
        S3PostProbe {
            label: "s3-post-auth-live-form-token-duplicate-mismatched-then-valid",
            credentials: fixture.live_credentials,
            policy_token: Some(fixture.other_live_security_token),
            form_tokens: S3PostFormTokens::Two(
                fixture.other_live_security_token,
                fixture.live_security_token,
            ),
            header_token: None,
            expected: S3PostAuthExpected::InvalidAccessKey,
        },
        S3PostProbe {
            label: "s3-post-auth-live-form-token-duplicate-valid-then-mismatched-bad-signature",
            credentials: live_bad_signature_credentials,
            policy_token: Some(fixture.live_security_token),
            form_tokens: S3PostFormTokens::Two(
                fixture.live_security_token,
                fixture.other_live_security_token,
            ),
            header_token: None,
            expected: S3PostAuthExpected::InvalidAccessKey,
        },
        S3PostProbe {
            label: "s3-post-auth-live-form-token-duplicate-mismatched-then-valid-bad-signature",
            credentials: live_bad_signature_credentials,
            policy_token: Some(fixture.other_live_security_token),
            form_tokens: S3PostFormTokens::Two(
                fixture.other_live_security_token,
                fixture.live_security_token,
            ),
            header_token: None,
            expected: S3PostAuthExpected::InvalidAccessKey,
        },
        S3PostProbe {
            label: "s3-post-auth-live-form-token-duplicate-identical-valid",
            credentials: fixture.live_credentials,
            policy_token: Some(fixture.live_security_token),
            form_tokens: S3PostFormTokens::Two(
                fixture.live_security_token,
                fixture.live_security_token,
            ),
            header_token: None,
            expected: S3PostAuthExpected::PolicyConditionFailed,
        },
        S3PostProbe {
            label: "s3-post-auth-live-form-token-duplicate-identical-valid-bad-signature",
            credentials: live_bad_signature_credentials,
            policy_token: Some(fixture.live_security_token),
            form_tokens: S3PostFormTokens::Two(
                fixture.live_security_token,
                fixture.live_security_token,
            ),
            header_token: None,
            expected: S3PostAuthExpected::SignatureMismatch,
        },
        S3PostProbe {
            label: "s3-post-auth-live-header-token-valid",
            credentials: fixture.live_credentials,
            policy_token: None,
            form_tokens: S3PostFormTokens::Missing,
            header_token: Some(fixture.live_security_token),
            expected: S3PostAuthExpected::NoAccessKeyPresented,
        },
        S3PostProbe {
            label: "s3-post-auth-live-form-valid-header-mismatched",
            credentials: fixture.live_credentials,
            policy_token: Some(fixture.live_security_token),
            form_tokens: S3PostFormTokens::One(fixture.live_security_token),
            header_token: Some(fixture.other_live_security_token),
            expected: S3PostAuthExpected::NoAccessKeyPresented,
        },
        S3PostProbe {
            label: "s3-post-auth-live-form-mismatched-header-valid",
            credentials: fixture.live_credentials,
            policy_token: Some(fixture.other_live_security_token),
            form_tokens: S3PostFormTokens::One(fixture.other_live_security_token),
            header_token: Some(fixture.live_security_token),
            expected: S3PostAuthExpected::NoAccessKeyPresented,
        },
        S3PostProbe {
            label: "s3-post-auth-live-header-token-valid-bad-signature",
            credentials: live_bad_signature_credentials,
            policy_token: None,
            form_tokens: S3PostFormTokens::Missing,
            header_token: Some(fixture.live_security_token),
            expected: S3PostAuthExpected::NoAccessKeyPresented,
        },
        S3PostProbe {
            label: "s3-post-auth-live-form-valid-header-malformed-bad-signature",
            credentials: live_bad_signature_credentials,
            policy_token: Some(fixture.live_security_token),
            form_tokens: S3PostFormTokens::One(fixture.live_security_token),
            header_token: Some(malformed_security_token),
            expected: S3PostAuthExpected::NoAccessKeyPresented,
        },
        S3PostProbe {
            label: "s3-post-auth-live-form-valid-header-invalidated-bad-signature",
            credentials: live_bad_signature_credentials,
            policy_token: Some(fixture.live_security_token),
            form_tokens: S3PostFormTokens::One(fixture.live_security_token),
            header_token: Some(fixture.old_security_token),
            expected: S3PostAuthExpected::NoAccessKeyPresented,
        },
        S3PostProbe {
            label: "s3-post-auth-old-form-token-valid",
            credentials: fixture.old_credentials,
            policy_token: Some(fixture.old_security_token),
            form_tokens: S3PostFormTokens::One(fixture.old_security_token),
            header_token: None,
            expected: S3PostAuthExpected::InvalidAccessKey,
        },
        S3PostProbe {
            label: "s3-post-auth-old-form-token-valid-bad-signature",
            credentials: old_bad_signature_credentials,
            policy_token: Some(fixture.old_security_token),
            form_tokens: S3PostFormTokens::One(fixture.old_security_token),
            header_token: None,
            expected: S3PostAuthExpected::InvalidAccessKey,
        },
        S3PostProbe {
            label: "s3-post-auth-old-token-missing-valid-signature",
            credentials: fixture.old_credentials,
            policy_token: None,
            form_tokens: S3PostFormTokens::Missing,
            header_token: None,
            expected: S3PostAuthExpected::InvalidAccessKey,
        },
        S3PostProbe {
            label: "s3-post-auth-old-form-token-mismatched-valid-signature",
            credentials: fixture.old_credentials,
            policy_token: Some(fixture.other_live_security_token),
            form_tokens: S3PostFormTokens::One(fixture.other_live_security_token),
            header_token: None,
            expected: S3PostAuthExpected::InvalidAccessKey,
        },
        S3PostProbe {
            label: "s3-post-auth-old-form-token-mismatched-bad-signature",
            credentials: old_bad_signature_credentials,
            policy_token: Some(fixture.other_live_security_token),
            form_tokens: S3PostFormTokens::One(fixture.other_live_security_token),
            header_token: None,
            expected: S3PostAuthExpected::InvalidAccessKey,
        },
    ];

    for probe in probes {
        let result = send_s3_post_probe(endpoint, bucket, probe);
        let form_tokens = probe.form_tokens.values();
        let sensitive_tokens = [
            probe.policy_token,
            form_tokens[0],
            form_tokens[1],
            probe.header_token,
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        match probe.expected {
            S3PostAuthExpected::AccessDenied => {
                let assumed_role_arn = format!(
                    "arn:aws:sts::{account_id}:assumed-role/{}/{}",
                    fixture.live_role_name, fixture.live_role_session_name
                );
                assert_s3_post_access_denied(
                    probe,
                    &result,
                    bucket,
                    &assumed_role_arn,
                    &sensitive_tokens,
                );
            }
            S3PostAuthExpected::ExpiredToken => {
                let expired_token = form_tokens[0]
                    .expect("ExpiredToken POST probe must present a form session token");
                assert_s3_post_expired_token(probe, &result, expired_token);
            }
            S3PostAuthExpected::InvalidAccessKey => {
                assert_s3_post_invalid_access_key(probe, &result, &sensitive_tokens);
            }
            S3PostAuthExpected::InvalidToken => {
                let rejected_token = form_tokens[0]
                    .expect("InvalidToken POST probe must present a form session token");
                assert_s3_post_invalid_token(probe, &result, &sensitive_tokens, rejected_token);
            }
            S3PostAuthExpected::NoAccessKeyPresented => {
                assert_s3_post_no_access_key_presented(probe, &result, &sensitive_tokens);
            }
            S3PostAuthExpected::PolicyConditionFailed => {
                assert_s3_post_policy_condition_failed(probe, &result, &sensitive_tokens);
            }
            S3PostAuthExpected::SignatureMismatch => {
                assert_s3_post_signature_mismatch(probe, &result, &sensitive_tokens);
            }
        }
    }
}

fn assert_s3_post_wrong_region_scope(
    label: &str,
    result: &S3PostResult,
    credentials: SignedRequestCredentials<'_>,
    security_tokens: &[&str],
    wrong_region: &str,
    expected_region: &str,
) {
    assert!(
        required_xml_text(&result.response, "ArgumentValue", label) == result.credential,
        "{label}: S3 did not echo the POST credential"
    );
    let credential = sanitize_s3_text(&result.credential, credentials.access_key, security_tokens);
    let response = s3_post_response_with_sanitized_body(
        &result.response,
        credentials.access_key,
        security_tokens,
        &result.policy,
    );
    let message = format!("the region '{wrong_region}' is wrong; expecting '{expected_region}'");
    assert_shape(
        label,
        &response,
        &shape()
            .status(400)
            .headers(error_response_headers())
            .sub("credential", credential)
            .sub("message", message)
            .sub("region", expected_region)
            .body(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                 <Error><Code>InvalidArgument</Code><Message>{message}</Message>\
                 <ArgumentName>X-Amz-Credential</ArgumentName>\
                 <ArgumentValue>{credential}</ArgumentValue><Region>{region}</Region>\
                 <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>",
            ),
    );
    println!("{label}: ok");
}

fn assert_s3_post_wrong_service_scope(
    label: &str,
    result: &S3PostResult,
    credentials: SignedRequestCredentials<'_>,
    security_tokens: &[&str],
) {
    assert!(
        required_xml_text(&result.response, "ArgumentValue", label) == result.credential,
        "{label}: S3 did not echo the POST credential"
    );
    let credential = sanitize_s3_text(&result.credential, credentials.access_key, security_tokens);
    let response = s3_post_response_with_sanitized_body(
        &result.response,
        credentials.access_key,
        security_tokens,
        &result.policy,
    );
    assert_shape(
        label,
        &response,
        &shape().status(400).headers(error_response_headers()).body(
            expected_error::invalid_argument_with_value(
                "incorrect service \"sts\". This endpoint belongs to \"s3\".",
                "X-Amz-Credential",
                &credential,
            ),
        ),
    );
    println!("{label}: ok");
}

fn assert_s3_post_scope_no_access_key_presented(
    label: &str,
    result: &S3PostResult,
    credentials: SignedRequestCredentials<'_>,
    security_tokens: &[&str],
) {
    let response = s3_post_response_with_sanitized_body(
        &result.response,
        credentials.access_key,
        security_tokens,
        &result.policy,
    );
    assert_shape(
        label,
        &response,
        &shape().status(403).headers(error_response_headers()).body(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <Error><Code>AccessDenied</Code>\
             <Message>No AWSAccessKey was presented.</Message>\
             <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>",
        ),
    );
    println!("{label}: ok");
}

#[derive(Clone, Copy)]
enum S3PostScopeExpected {
    WrongRegion,
    WrongService,
}

fn run_s3_post_scope_probes(endpoint: &str, bucket: &str, fixture: S3PostSessionProbeSet<'_>) {
    let wrong_region = if fixture.live_credentials.region == "us-east-1" {
        "us-west-2"
    } else {
        "us-east-1"
    };
    let wrong_secret = "0".repeat(40);
    let malformed_security_token = "malformed-session-token";
    for (scope, live_credentials, old_credentials, service, expected) in [
        (
            "wrong-region",
            SignedRequestCredentials {
                region: wrong_region,
                ..fixture.live_credentials
            },
            SignedRequestCredentials {
                region: wrong_region,
                ..fixture.old_credentials
            },
            "s3",
            S3PostScopeExpected::WrongRegion,
        ),
        (
            "wrong-service",
            fixture.live_credentials,
            fixture.old_credentials,
            "sts",
            S3PostScopeExpected::WrongService,
        ),
    ] {
        let bad_signature_credentials = SignedRequestCredentials {
            secret_key: &wrong_secret,
            ..live_credentials
        };
        for (case, credentials, policy_token, form_tokens, header_token) in [
            (
                "valid-form",
                live_credentials,
                Some(fixture.live_security_token),
                S3PostFormTokens::One(fixture.live_security_token),
                None,
            ),
            (
                "missing-token",
                live_credentials,
                None,
                S3PostFormTokens::Missing,
                None,
            ),
            (
                "empty-form",
                live_credentials,
                Some(""),
                S3PostFormTokens::One(""),
                None,
            ),
            (
                "malformed-form",
                live_credentials,
                Some(malformed_security_token),
                S3PostFormTokens::One(malformed_security_token),
                None,
            ),
            (
                "mismatched-form",
                live_credentials,
                Some(fixture.other_live_security_token),
                S3PostFormTokens::One(fixture.other_live_security_token),
                None,
            ),
            (
                "valid-form-bad-signature",
                bad_signature_credentials,
                Some(fixture.live_security_token),
                S3PostFormTokens::One(fixture.live_security_token),
                None,
            ),
            (
                "old-session-valid-form",
                old_credentials,
                Some(fixture.old_security_token),
                S3PostFormTokens::One(fixture.old_security_token),
                None,
            ),
            (
                "duplicate-identical-form",
                live_credentials,
                Some(fixture.live_security_token),
                S3PostFormTokens::Two(fixture.live_security_token, fixture.live_security_token),
                None,
            ),
            (
                "duplicate-conflicting-form",
                live_credentials,
                Some(fixture.live_security_token),
                S3PostFormTokens::Two(
                    fixture.live_security_token,
                    fixture.other_live_security_token,
                ),
                None,
            ),
            (
                "duplicate-conflicting-form-reversed",
                live_credentials,
                Some(fixture.other_live_security_token),
                S3PostFormTokens::Two(
                    fixture.other_live_security_token,
                    fixture.live_security_token,
                ),
                None,
            ),
            (
                "valid-header-valid-form",
                live_credentials,
                Some(fixture.live_security_token),
                S3PostFormTokens::One(fixture.live_security_token),
                Some(fixture.live_security_token),
            ),
            (
                "malformed-header-valid-form",
                live_credentials,
                Some(fixture.live_security_token),
                S3PostFormTokens::One(fixture.live_security_token),
                Some(malformed_security_token),
            ),
            (
                "old-session-header-valid-form",
                live_credentials,
                Some(fixture.live_security_token),
                S3PostFormTokens::One(fixture.live_security_token),
                Some(fixture.old_security_token),
            ),
        ] {
            let label = format!("s3-post-scope-{scope}-{case}");
            let result = send_s3_post_scope_probe(
                endpoint,
                bucket,
                S3PostScopeProbe {
                    label: &label,
                    credentials,
                    service,
                    policy_token,
                    form_tokens,
                    header_token,
                },
            );
            let form_token_values = form_tokens.values();
            let sensitive_tokens = [
                policy_token,
                form_token_values[0],
                form_token_values[1],
                header_token,
            ]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
            if header_token.is_some() {
                assert_s3_post_scope_no_access_key_presented(
                    &label,
                    &result,
                    credentials,
                    &sensitive_tokens,
                );
            } else {
                match expected {
                    S3PostScopeExpected::WrongRegion => assert_s3_post_wrong_region_scope(
                        &label,
                        &result,
                        credentials,
                        &sensitive_tokens,
                        wrong_region,
                        fixture.live_credentials.region,
                    ),
                    S3PostScopeExpected::WrongService => assert_s3_post_wrong_service_scope(
                        &label,
                        &result,
                        credentials,
                        &sensitive_tokens,
                    ),
                }
            }
        }
    }

    let label = "s3-post-scope-both-wrong-valid-form";
    let result = send_s3_post_scope_probe(
        endpoint,
        bucket,
        S3PostScopeProbe {
            label,
            credentials: SignedRequestCredentials {
                region: wrong_region,
                ..fixture.live_credentials
            },
            service: "sts",
            policy_token: Some(fixture.live_security_token),
            form_tokens: S3PostFormTokens::One(fixture.live_security_token),
            header_token: None,
        },
    );
    assert_s3_post_wrong_region_scope(
        label,
        &result,
        SignedRequestCredentials {
            region: wrong_region,
            ..fixture.live_credentials
        },
        &[fixture.live_security_token],
        wrong_region,
        fixture.live_credentials.region,
    );
}

const STREAMING_PAYLOAD_HASH: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
const STREAMING_DATA: &[u8] = b"STS streaming oracle";

#[derive(Clone, Copy)]
enum S3StreamingTokens<'a> {
    Missing,
    One(&'a str),
    Two(&'a str, &'a str),
}

impl<'a> S3StreamingTokens<'a> {
    fn values(self) -> [Option<&'a str>; 2] {
        match self {
            Self::Missing => [None, None],
            Self::One(token) => [Some(token), None],
            Self::Two(first, second) => [Some(first), Some(second)],
        }
    }

    fn canonical_value(self) -> Option<String> {
        match self {
            Self::Missing => None,
            Self::One(token) => Some(token.to_string()),
            Self::Two(first, second) => Some(format!("{first},{second}")),
        }
    }
}

#[derive(Clone, Copy)]
enum S3StreamingAuthExpected {
    Success,
    HeadersNotSigned,
    InvalidAccessKey,
    InvalidToken,
    SeedSignatureMismatch,
    ChunkSignatureMismatch,
}

#[derive(Clone, Copy)]
struct S3StreamingProbe<'a> {
    label: &'a str,
    credentials: SignedRequestCredentials<'a>,
    tokens: S3StreamingTokens<'a>,
    sign_token_header: bool,
    bad_chunk_signature: bool,
    expected: S3StreamingAuthExpected,
}

#[derive(Clone, Copy)]
struct S3StreamingRequest<'a> {
    label: &'a str,
    credentials: SignedRequestCredentials<'a>,
    tokens: S3StreamingTokens<'a>,
    sign_token_header: bool,
    bad_chunk_signature: bool,
    service: &'a str,
}

#[derive(Clone, Copy)]
struct S3StreamingSessionProbeSet<'a> {
    live_credentials: SignedRequestCredentials<'a>,
    live_security_token: &'a str,
    other_live_security_token: &'a str,
    old_credentials: SignedRequestCredentials<'a>,
    old_security_token: &'a str,
}

struct S3StreamingSignature {
    authorization: String,
    amz_date: String,
    canonical_request: String,
    scope: String,
    seed_signature: String,
    signing_key: Vec<u8>,
}

struct S3StreamingResult {
    response: RawResponse,
    signature: S3StreamingSignature,
    first_chunk_signature: String,
}

fn streaming_days_to_ymd(days: u64) -> (u64, u64, u64) {
    let z = days + 719_468;
    let era = z / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = if month_prime < 10 {
        month_prime + 3
    } else {
        month_prime - 9
    };
    let year = if month <= 2 { year + 1 } else { year };
    (year, month, day)
}

fn streaming_now_parts() -> (String, String) {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before the Unix epoch")
        .as_secs();
    let days = seconds / 86_400;
    let (year, month, day) = streaming_days_to_ymd(days);
    let time_of_day = seconds % 86_400;
    let hour = time_of_day / 3600;
    let minute = (time_of_day % 3600) / 60;
    let second = time_of_day % 60;
    let long = format!("{year:04}{month:02}{day:02}T{hour:02}{minute:02}{second:02}Z");
    let short = long[..8].to_string();
    (long, short)
}

fn hex_lower(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn streaming_hmac(key: &[u8], value: &str) -> String {
    hex_lower(hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), value.as_bytes()).as_ref())
}

#[cfg(test)]
fn sign_s3_streaming_request(
    endpoint: &str,
    path: &str,
    decoded_length: usize,
    credentials: SignedRequestCredentials<'_>,
    tokens: S3StreamingTokens<'_>,
    sign_token_header: bool,
) -> S3StreamingSignature {
    sign_s3_streaming_request_for_service(
        endpoint,
        path,
        decoded_length,
        credentials,
        tokens,
        sign_token_header,
        "s3",
    )
}

fn sign_s3_streaming_request_for_service(
    endpoint: &str,
    path: &str,
    decoded_length: usize,
    credentials: SignedRequestCredentials<'_>,
    tokens: S3StreamingTokens<'_>,
    sign_token_header: bool,
    service: &str,
) -> S3StreamingSignature {
    let parsed_endpoint = url::Url::parse(endpoint)
        .unwrap_or_else(|error| panic!("invalid S3 streaming endpoint: {error}"));
    let host = parsed_endpoint
        .host_str()
        .map(|host| {
            parsed_endpoint
                .port()
                .map_or_else(|| host.to_string(), |port| format!("{host}:{port}"))
        })
        .expect("S3 streaming endpoint has no host");
    let (amz_date, date) = streaming_now_parts();
    let decoded_length = decoded_length.to_string();
    let mut headers = vec![
        ("content-encoding", "aws-chunked".to_string()),
        ("host", host),
        ("x-amz-content-sha256", STREAMING_PAYLOAD_HASH.to_string()),
        ("x-amz-date", amz_date.clone()),
        ("x-amz-decoded-content-length", decoded_length),
    ];
    if sign_token_header {
        if let Some(value) = tokens.canonical_value() {
            headers.push(("x-amz-security-token", value));
        }
    }
    headers.sort_by_key(|(name, _)| *name);
    let signed_headers = headers
        .iter()
        .map(|(name, _)| *name)
        .collect::<Vec<_>>()
        .join(";");
    let header_refs = headers
        .iter()
        .map(|(name, value)| (*name, value.as_str()))
        .collect::<Vec<_>>();
    let canonical_headers = auth::canonical::canonical_headers(&header_refs);
    let canonical_request = auth::canonical::canonical_request(
        "PUT",
        path,
        "",
        &canonical_headers,
        &signed_headers,
        STREAMING_PAYLOAD_HASH,
    );
    let scope = format!("{date}/{}/{service}/aws4_request", credentials.region);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        auth::canonical::sha256_hex(canonical_request.as_bytes())
    );
    let secret = auth::SecretKey::new(credentials.secret_key.to_string());
    let signing_key = auth::sigv4::derive_signing_key(&secret, &date, credentials.region, service);
    let seed_signature = streaming_hmac(signing_key.as_ref(), &string_to_sign);
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={seed_signature}",
        credentials.access_key
    );
    S3StreamingSignature {
        authorization,
        amz_date,
        canonical_request,
        scope,
        seed_signature,
        signing_key: signing_key.as_ref().to_vec(),
    }
}

fn s3_streaming_chunk_signature(
    signature: &S3StreamingSignature,
    previous_signature: &str,
    data: &[u8],
) -> String {
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{}\n{}",
        signature.amz_date,
        signature.scope,
        previous_signature,
        auth::canonical::sha256_hex(b""),
        auth::canonical::sha256_hex(data)
    );
    streaming_hmac(&signature.signing_key, &string_to_sign)
}

fn build_s3_streaming_body(
    signature: &S3StreamingSignature,
    data: &[u8],
    bad_chunk_signature: bool,
) -> (Vec<u8>, String) {
    let valid_chunk_signature =
        s3_streaming_chunk_signature(signature, &signature.seed_signature, data);
    let presented_chunk_signature = if bad_chunk_signature {
        "0".repeat(64)
    } else {
        valid_chunk_signature.clone()
    };
    let terminal_signature =
        s3_streaming_chunk_signature(signature, &presented_chunk_signature, b"");
    let mut body = Vec::new();
    body.extend_from_slice(
        format!(
            "{:x};chunk-signature={presented_chunk_signature}\r\n",
            data.len()
        )
        .as_bytes(),
    );
    body.extend_from_slice(data);
    body.extend_from_slice(b"\r\n");
    body.extend_from_slice(format!("0;chunk-signature={terminal_signature}\r\n\r\n").as_bytes());
    (body, presented_chunk_signature)
}

fn send_s3_streaming_probe(
    endpoint: &str,
    bucket: &str,
    probe: S3StreamingProbe<'_>,
) -> S3StreamingResult {
    send_s3_streaming_request(
        endpoint,
        bucket,
        S3StreamingRequest {
            label: probe.label,
            credentials: probe.credentials,
            tokens: probe.tokens,
            sign_token_header: probe.sign_token_header,
            bad_chunk_signature: probe.bad_chunk_signature,
            service: "s3",
        },
    )
}

fn send_s3_streaming_request(
    endpoint: &str,
    bucket: &str,
    request: S3StreamingRequest<'_>,
) -> S3StreamingResult {
    let path = format!("/{bucket}/{}", request.label);
    let signature = sign_s3_streaming_request_for_service(
        endpoint,
        &path,
        STREAMING_DATA.len(),
        request.credentials,
        request.tokens,
        request.sign_token_header,
        request.service,
    );
    let (body, first_chunk_signature) =
        build_s3_streaming_body(&signature, STREAMING_DATA, request.bad_chunk_signature);
    let url = format!("{endpoint}{path}");
    let mut wire_request = build_test_agent(endpoint, None, Duration::from_secs(120))
        .put(&url)
        .header("authorization", &signature.authorization)
        .header("content-encoding", "aws-chunked")
        .header("x-amz-content-sha256", STREAMING_PAYLOAD_HASH)
        .header("x-amz-date", &signature.amz_date)
        .header(
            "x-amz-decoded-content-length",
            STREAMING_DATA.len().to_string(),
        );
    for token in request.tokens.values().into_iter().flatten() {
        wire_request = wire_request.header("x-amz-security-token", token);
    }
    let mut response = wire_request
        .send(&body)
        .expect("streaming AWS transport error");
    let headers = response
        .headers()
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_string(),
                value
                    .to_str()
                    .expect("streaming AWS response header is valid UTF-8")
                    .to_string(),
            )
        })
        .collect();
    let (body, body_read_error) = match response.body_mut().read_to_string() {
        Ok(body) => (body, None),
        Err(error) => (String::new(), Some(error.to_string())),
    };
    S3StreamingResult {
        response: RawResponse {
            status: response.status().as_u16(),
            headers,
            body,
            body_read_error,
        },
        signature,
        first_chunk_signature,
    }
}

fn assert_s3_streaming_success(probe: S3StreamingProbe<'_>, result: &S3StreamingResult) {
    assert_shape(
        probe.label,
        &result.response,
        &shape()
            .status(200)
            .header("x-amz-id-2", "{host_id}")
            .header("x-amz-request-id", "{request_id}")
            .header("x-amz-server-side-encryption", "AES256")
            .header("etag", "{etag}")
            .header("x-amz-checksum-crc64nvme", "C5AeGMhd5F4=")
            .header("x-amz-checksum-type", "FULL_OBJECT")
            .body_empty(),
    );
    println!("{}: ok", probe.label);
}

fn assert_s3_streaming_invalid_token(
    probe: S3StreamingProbe<'_>,
    result: &S3StreamingResult,
    security_tokens: &[&str],
) {
    let rejected_token = required_xml_text(&result.response, "Token-0", probe.label);
    assert!(
        security_tokens
            .iter()
            .any(|security_token| *security_token == rejected_token),
        "{}: S3 echoed an unexpected rejected token",
        probe.label
    );
    let response = s3_response_with_sanitized_body(
        &result.response,
        probe.credentials.access_key,
        security_tokens,
    );
    assert_shape(
        probe.label,
        &response,
        &shape()
            .status(400)
            .headers(error_response_headers())
            .body(expected_error::invalid_token(
                "The provided token is malformed or otherwise invalid.",
                "SESSION_TOKEN",
            )),
    );
    println!("{}: ok", probe.label);
}

fn assert_s3_streaming_seed_signature_mismatch(
    probe: S3StreamingProbe<'_>,
    result: &S3StreamingResult,
    security_tokens: &[&str],
) {
    assert_s3_signature_mismatch(
        probe.label,
        &result.response,
        probe.credentials,
        security_tokens,
        Some(&result.signature.amz_date),
        Some(&result.signature.seed_signature),
        |_| result.signature.canonical_request.clone(),
    );
}

fn assert_s3_streaming_chunk_signature_mismatch(
    probe: S3StreamingProbe<'_>,
    result: &S3StreamingResult,
    security_tokens: &[&str],
) {
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{}\n{}",
        result.signature.amz_date,
        result.signature.scope,
        result.signature.seed_signature,
        auth::canonical::sha256_hex(b""),
        auth::canonical::sha256_hex(STREAMING_DATA)
    );
    assert!(
        required_xml_text(&result.response, "AWSAccessKeyId", probe.label)
            == probe.credentials.access_key,
        "{}: S3 did not echo the session access key",
        probe.label,
    );
    assert!(
        required_xml_text(&result.response, "StringToSign", probe.label) == string_to_sign,
        "{}: S3 did not echo the chunk string to sign",
        probe.label,
    );
    assert!(
        required_xml_text(&result.response, "StringToSignBytes", probe.label)
            == spaced_hex(&string_to_sign),
        "{}: StringToSignBytes does not encode the chunk string to sign",
        probe.label,
    );
    assert!(
        required_xml_text(&result.response, "SignatureProvided", probe.label)
            == result.first_chunk_signature,
        "{}: S3 did not echo the bad chunk signature",
        probe.label,
    );
    assert!(
        required_xml_text(&result.response, "CanonicalRequest", probe.label)
            == result.signature.canonical_request,
        "{}: S3 did not echo the seed canonical request",
        probe.label,
    );
    assert!(
        required_xml_text(&result.response, "CanonicalRequestBytes", probe.label)
            == spaced_hex(&result.signature.canonical_request),
        "{}: CanonicalRequestBytes does not encode the seed canonical request",
        probe.label,
    );

    let response = s3_response_with_sanitized_body(
        &result.response,
        probe.credentials.access_key,
        security_tokens,
    );
    let sanitized_string_to_sign = sanitize_s3_text(
        &string_to_sign,
        probe.credentials.access_key,
        security_tokens,
    );
    let sanitized_string_to_sign_bytes = sanitize_s3_text(
        &spaced_hex(&string_to_sign),
        probe.credentials.access_key,
        security_tokens,
    );
    let sanitized_canonical_request = sanitize_s3_text(
        &result.signature.canonical_request,
        probe.credentials.access_key,
        security_tokens,
    );
    let sanitized_canonical_request_bytes = sanitize_s3_text(
        &spaced_hex(&result.signature.canonical_request),
        probe.credentials.access_key,
        security_tokens,
    );
    assert_shape(
        probe.label,
        &response,
        &shape()
            .status(403)
            .headers(error_response_headers())
            .sub("string_to_sign", sanitized_string_to_sign)
            .sub("signature", &result.first_chunk_signature)
            .sub("string_to_sign_bytes", sanitized_string_to_sign_bytes)
            .sub("canonical_request", sanitized_canonical_request)
            .sub("canonical_request_bytes", sanitized_canonical_request_bytes)
            .body(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                 <Error><Code>SignatureDoesNotMatch</Code>\
                 <Message>The request signature we calculated does not match the signature you provided. Check your key and signing method.</Message>\
                 <AWSAccessKeyId>SESSION_ACCESS_KEY</AWSAccessKeyId>\
                 <StringToSign>{string_to_sign}</StringToSign>\
                 <SignatureProvided>{signature}</SignatureProvided>\
                 <StringToSignBytes>{string_to_sign_bytes}</StringToSignBytes>\
                 <CanonicalRequest>{canonical_request}</CanonicalRequest>\
                 <CanonicalRequestBytes>{canonical_request_bytes}</CanonicalRequestBytes>\
                 <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>",
            ),
    );
    println!("{}: ok", probe.label);
}

fn run_s3_streaming_session_authentication_probes(
    endpoint: &str,
    bucket: &str,
    fixture: S3StreamingSessionProbeSet<'_>,
) {
    let wrong_secret = "0".repeat(40);
    let malformed_security_token = "malformed-session-token";
    let live_bad_signature_credentials = SignedRequestCredentials {
        secret_key: &wrong_secret,
        ..fixture.live_credentials
    };
    let old_bad_signature_credentials = SignedRequestCredentials {
        secret_key: &wrong_secret,
        ..fixture.old_credentials
    };
    let probes = [
        S3StreamingProbe {
            label: "streaming-live-valid",
            credentials: fixture.live_credentials,
            tokens: S3StreamingTokens::One(fixture.live_security_token),
            sign_token_header: true,
            bad_chunk_signature: false,
            expected: S3StreamingAuthExpected::Success,
        },
        S3StreamingProbe {
            label: "streaming-live-valid-token-bad-chunk-signature",
            credentials: fixture.live_credentials,
            tokens: S3StreamingTokens::One(fixture.live_security_token),
            sign_token_header: true,
            bad_chunk_signature: true,
            expected: S3StreamingAuthExpected::ChunkSignatureMismatch,
        },
        S3StreamingProbe {
            label: "streaming-live-missing-token-valid-signature",
            credentials: fixture.live_credentials,
            tokens: S3StreamingTokens::Missing,
            sign_token_header: true,
            bad_chunk_signature: false,
            expected: S3StreamingAuthExpected::InvalidAccessKey,
        },
        S3StreamingProbe {
            label: "streaming-live-empty-token-valid-signature",
            credentials: fixture.live_credentials,
            tokens: S3StreamingTokens::One(""),
            sign_token_header: true,
            bad_chunk_signature: false,
            expected: S3StreamingAuthExpected::InvalidAccessKey,
        },
        S3StreamingProbe {
            label: "streaming-live-malformed-token-valid-signature",
            credentials: fixture.live_credentials,
            tokens: S3StreamingTokens::One(malformed_security_token),
            sign_token_header: true,
            bad_chunk_signature: false,
            expected: S3StreamingAuthExpected::InvalidToken,
        },
        S3StreamingProbe {
            label: "streaming-live-mismatched-token-valid-signature",
            credentials: fixture.live_credentials,
            tokens: S3StreamingTokens::One(fixture.other_live_security_token),
            sign_token_header: true,
            bad_chunk_signature: false,
            expected: S3StreamingAuthExpected::InvalidAccessKey,
        },
        S3StreamingProbe {
            label: "streaming-live-identical-duplicate-token-valid-signature",
            credentials: fixture.live_credentials,
            tokens: S3StreamingTokens::Two(
                fixture.live_security_token,
                fixture.live_security_token,
            ),
            sign_token_header: true,
            bad_chunk_signature: false,
            expected: S3StreamingAuthExpected::Success,
        },
        S3StreamingProbe {
            label: "streaming-live-conflicting-duplicate-token-valid-signature",
            credentials: fixture.live_credentials,
            tokens: S3StreamingTokens::Two(
                fixture.live_security_token,
                fixture.other_live_security_token,
            ),
            sign_token_header: true,
            bad_chunk_signature: false,
            expected: S3StreamingAuthExpected::InvalidAccessKey,
        },
        S3StreamingProbe {
            label: "streaming-live-conflicting-duplicate-token-reversed-valid-signature",
            credentials: fixture.live_credentials,
            tokens: S3StreamingTokens::Two(
                fixture.other_live_security_token,
                fixture.live_security_token,
            ),
            sign_token_header: true,
            bad_chunk_signature: false,
            expected: S3StreamingAuthExpected::InvalidAccessKey,
        },
        S3StreamingProbe {
            label: "streaming-live-valid-token-unsigned",
            credentials: fixture.live_credentials,
            tokens: S3StreamingTokens::One(fixture.live_security_token),
            sign_token_header: false,
            bad_chunk_signature: false,
            expected: S3StreamingAuthExpected::HeadersNotSigned,
        },
        S3StreamingProbe {
            label: "streaming-live-missing-token-bad-chunk-signature",
            credentials: fixture.live_credentials,
            tokens: S3StreamingTokens::Missing,
            sign_token_header: true,
            bad_chunk_signature: true,
            expected: S3StreamingAuthExpected::InvalidAccessKey,
        },
        S3StreamingProbe {
            label: "streaming-live-empty-token-bad-chunk-signature",
            credentials: fixture.live_credentials,
            tokens: S3StreamingTokens::One(""),
            sign_token_header: true,
            bad_chunk_signature: true,
            expected: S3StreamingAuthExpected::InvalidAccessKey,
        },
        S3StreamingProbe {
            label: "streaming-live-malformed-token-bad-chunk-signature",
            credentials: fixture.live_credentials,
            tokens: S3StreamingTokens::One(malformed_security_token),
            sign_token_header: true,
            bad_chunk_signature: true,
            expected: S3StreamingAuthExpected::InvalidToken,
        },
        S3StreamingProbe {
            label: "streaming-live-mismatched-token-bad-chunk-signature",
            credentials: fixture.live_credentials,
            tokens: S3StreamingTokens::One(fixture.other_live_security_token),
            sign_token_header: true,
            bad_chunk_signature: true,
            expected: S3StreamingAuthExpected::InvalidAccessKey,
        },
        S3StreamingProbe {
            label: "streaming-live-identical-duplicate-token-bad-chunk-signature",
            credentials: fixture.live_credentials,
            tokens: S3StreamingTokens::Two(
                fixture.live_security_token,
                fixture.live_security_token,
            ),
            sign_token_header: true,
            bad_chunk_signature: true,
            expected: S3StreamingAuthExpected::ChunkSignatureMismatch,
        },
        S3StreamingProbe {
            label: "streaming-live-conflicting-duplicate-token-bad-chunk-signature",
            credentials: fixture.live_credentials,
            tokens: S3StreamingTokens::Two(
                fixture.live_security_token,
                fixture.other_live_security_token,
            ),
            sign_token_header: true,
            bad_chunk_signature: true,
            expected: S3StreamingAuthExpected::InvalidAccessKey,
        },
        S3StreamingProbe {
            label: "streaming-live-valid-token-unsigned-bad-chunk-signature",
            credentials: fixture.live_credentials,
            tokens: S3StreamingTokens::One(fixture.live_security_token),
            sign_token_header: false,
            bad_chunk_signature: true,
            expected: S3StreamingAuthExpected::HeadersNotSigned,
        },
        S3StreamingProbe {
            label: "streaming-live-valid-token-bad-seed-signature",
            credentials: live_bad_signature_credentials,
            tokens: S3StreamingTokens::One(fixture.live_security_token),
            sign_token_header: true,
            bad_chunk_signature: false,
            expected: S3StreamingAuthExpected::SeedSignatureMismatch,
        },
        S3StreamingProbe {
            label: "streaming-live-valid-token-unsigned-bad-seed-signature",
            credentials: live_bad_signature_credentials,
            tokens: S3StreamingTokens::One(fixture.live_security_token),
            sign_token_header: false,
            bad_chunk_signature: false,
            expected: S3StreamingAuthExpected::HeadersNotSigned,
        },
        S3StreamingProbe {
            label: "streaming-live-missing-token-bad-seed-signature",
            credentials: live_bad_signature_credentials,
            tokens: S3StreamingTokens::Missing,
            sign_token_header: true,
            bad_chunk_signature: false,
            expected: S3StreamingAuthExpected::InvalidAccessKey,
        },
        S3StreamingProbe {
            label: "streaming-live-empty-token-bad-seed-signature",
            credentials: live_bad_signature_credentials,
            tokens: S3StreamingTokens::One(""),
            sign_token_header: true,
            bad_chunk_signature: false,
            expected: S3StreamingAuthExpected::InvalidAccessKey,
        },
        S3StreamingProbe {
            label: "streaming-live-malformed-token-bad-seed-signature",
            credentials: live_bad_signature_credentials,
            tokens: S3StreamingTokens::One(malformed_security_token),
            sign_token_header: true,
            bad_chunk_signature: false,
            expected: S3StreamingAuthExpected::InvalidToken,
        },
        S3StreamingProbe {
            label: "streaming-live-mismatched-token-bad-seed-signature",
            credentials: live_bad_signature_credentials,
            tokens: S3StreamingTokens::One(fixture.other_live_security_token),
            sign_token_header: true,
            bad_chunk_signature: false,
            expected: S3StreamingAuthExpected::InvalidAccessKey,
        },
        S3StreamingProbe {
            label: "streaming-live-identical-duplicate-token-bad-seed-signature",
            credentials: live_bad_signature_credentials,
            tokens: S3StreamingTokens::Two(
                fixture.live_security_token,
                fixture.live_security_token,
            ),
            sign_token_header: true,
            bad_chunk_signature: false,
            expected: S3StreamingAuthExpected::SeedSignatureMismatch,
        },
        S3StreamingProbe {
            label: "streaming-live-conflicting-duplicate-token-bad-seed-signature",
            credentials: live_bad_signature_credentials,
            tokens: S3StreamingTokens::Two(
                fixture.other_live_security_token,
                fixture.live_security_token,
            ),
            sign_token_header: true,
            bad_chunk_signature: false,
            expected: S3StreamingAuthExpected::InvalidAccessKey,
        },
        S3StreamingProbe {
            label: "streaming-live-conflicting-duplicate-token-reversed-bad-seed-signature",
            credentials: live_bad_signature_credentials,
            tokens: S3StreamingTokens::Two(
                fixture.live_security_token,
                fixture.other_live_security_token,
            ),
            sign_token_header: true,
            bad_chunk_signature: false,
            expected: S3StreamingAuthExpected::InvalidAccessKey,
        },
        S3StreamingProbe {
            label: "streaming-old-session-valid",
            credentials: fixture.old_credentials,
            tokens: S3StreamingTokens::One(fixture.old_security_token),
            sign_token_header: true,
            bad_chunk_signature: false,
            expected: S3StreamingAuthExpected::InvalidAccessKey,
        },
        S3StreamingProbe {
            label: "streaming-old-session-valid-token-unsigned",
            credentials: fixture.old_credentials,
            tokens: S3StreamingTokens::One(fixture.old_security_token),
            sign_token_header: false,
            bad_chunk_signature: false,
            expected: S3StreamingAuthExpected::HeadersNotSigned,
        },
        S3StreamingProbe {
            label: "streaming-old-session-valid-token-unsigned-bad-seed-signature",
            credentials: old_bad_signature_credentials,
            tokens: S3StreamingTokens::One(fixture.old_security_token),
            sign_token_header: false,
            bad_chunk_signature: false,
            expected: S3StreamingAuthExpected::HeadersNotSigned,
        },
        S3StreamingProbe {
            label: "streaming-old-session-missing-token-valid-signature",
            credentials: fixture.old_credentials,
            tokens: S3StreamingTokens::Missing,
            sign_token_header: true,
            bad_chunk_signature: false,
            expected: S3StreamingAuthExpected::InvalidAccessKey,
        },
        S3StreamingProbe {
            label: "streaming-old-session-mismatched-token-valid-signature",
            credentials: fixture.old_credentials,
            tokens: S3StreamingTokens::One(fixture.live_security_token),
            sign_token_header: true,
            bad_chunk_signature: false,
            expected: S3StreamingAuthExpected::InvalidAccessKey,
        },
        S3StreamingProbe {
            label: "streaming-old-session-valid-token-bad-seed-signature",
            credentials: old_bad_signature_credentials,
            tokens: S3StreamingTokens::One(fixture.old_security_token),
            sign_token_header: true,
            bad_chunk_signature: false,
            expected: S3StreamingAuthExpected::InvalidAccessKey,
        },
        S3StreamingProbe {
            label: "streaming-old-session-valid-token-bad-chunk-signature",
            credentials: fixture.old_credentials,
            tokens: S3StreamingTokens::One(fixture.old_security_token),
            sign_token_header: true,
            bad_chunk_signature: true,
            expected: S3StreamingAuthExpected::InvalidAccessKey,
        },
    ];

    for probe in probes {
        let result = send_s3_streaming_probe(endpoint, bucket, probe);
        let sensitive_tokens = probe
            .tokens
            .values()
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        match probe.expected {
            S3StreamingAuthExpected::Success => assert_s3_streaming_success(probe, &result),
            S3StreamingAuthExpected::HeadersNotSigned => assert_s3_headers_not_signed(
                probe.label,
                &result.response,
                probe.credentials.access_key,
                &sensitive_tokens,
            ),
            S3StreamingAuthExpected::InvalidAccessKey => assert_s3_invalid_access_key(
                probe.label,
                &result.response,
                probe.credentials.access_key,
                &sensitive_tokens,
            ),
            S3StreamingAuthExpected::InvalidToken => {
                assert_s3_streaming_invalid_token(probe, &result, &sensitive_tokens);
            }
            S3StreamingAuthExpected::SeedSignatureMismatch => {
                assert_s3_streaming_seed_signature_mismatch(probe, &result, &sensitive_tokens);
            }
            S3StreamingAuthExpected::ChunkSignatureMismatch => {
                assert_s3_streaming_chunk_signature_mismatch(probe, &result, &sensitive_tokens);
            }
        }
    }
}

fn assert_s3_streaming_wrong_region_scope(
    label: &str,
    response: &RawResponse,
    wrong_region: &str,
    expected_region: &str,
    access_key: &str,
    security_tokens: &[&str],
) {
    let response = s3_response_with_sanitized_body(response, access_key, security_tokens);
    let message = format!(
        "The authorization header is malformed; the region '{wrong_region}' is wrong; \
         expecting '{expected_region}'"
    );
    assert_shape(
        label,
        &response,
        &shape()
            .status(400)
            .headers(error_response_headers())
            .body(expected_error::with_region(
                "AuthorizationHeaderMalformed",
                &message,
                expected_region,
            )),
    );
    println!("{label}: ok");
}

fn assert_s3_streaming_wrong_service_scope(
    label: &str,
    response: &RawResponse,
    access_key: &str,
    security_tokens: &[&str],
) {
    let response = s3_response_with_sanitized_body(response, access_key, security_tokens);
    assert_shape(
        label,
        &response,
        &shape()
            .status(400)
            .headers(error_response_headers())
            .body(expected_error::with_host_id(
                "AuthorizationHeaderMalformed",
                "The authorization header is malformed; incorrect service \"sts\". This endpoint belongs to \"s3\".",
            )),
    );
    println!("{label}: ok");
}

fn run_s3_streaming_scope_probes(
    endpoint: &str,
    bucket: &str,
    fixture: S3StreamingSessionProbeSet<'_>,
) {
    let wrong_region = if fixture.live_credentials.region == "us-east-1" {
        "us-west-2"
    } else {
        "us-east-1"
    };
    let wrong_secret = "0".repeat(40);
    let malformed_security_token = "malformed-session-token";

    for (scope, service, credentials) in [
        (
            "wrong-region",
            "s3",
            SignedRequestCredentials {
                region: wrong_region,
                ..fixture.live_credentials
            },
        ),
        ("wrong-service", "sts", fixture.live_credentials),
    ] {
        let bad_signature_credentials = SignedRequestCredentials {
            secret_key: &wrong_secret,
            ..credentials
        };
        let old_credentials = SignedRequestCredentials {
            region: credentials.region,
            ..fixture.old_credentials
        };
        let old_bad_signature_credentials = SignedRequestCredentials {
            secret_key: &wrong_secret,
            ..old_credentials
        };
        for (case, request_credentials, tokens, sign_token_header, bad_chunk_signature) in [
            (
                "valid",
                credentials,
                S3StreamingTokens::One(fixture.live_security_token),
                true,
                false,
            ),
            (
                "missing-token",
                credentials,
                S3StreamingTokens::Missing,
                true,
                false,
            ),
            (
                "empty-token",
                credentials,
                S3StreamingTokens::One(""),
                true,
                false,
            ),
            (
                "malformed-token",
                credentials,
                S3StreamingTokens::One(malformed_security_token),
                true,
                false,
            ),
            (
                "mismatched-token",
                credentials,
                S3StreamingTokens::One(fixture.other_live_security_token),
                true,
                false,
            ),
            (
                "identical-duplicate-token",
                credentials,
                S3StreamingTokens::Two(fixture.live_security_token, fixture.live_security_token),
                true,
                false,
            ),
            (
                "conflicting-duplicate-token",
                credentials,
                S3StreamingTokens::Two(
                    fixture.live_security_token,
                    fixture.other_live_security_token,
                ),
                true,
                false,
            ),
            (
                "conflicting-duplicate-token-reversed",
                credentials,
                S3StreamingTokens::Two(
                    fixture.other_live_security_token,
                    fixture.live_security_token,
                ),
                true,
                false,
            ),
            (
                "unsigned-token",
                credentials,
                S3StreamingTokens::One(fixture.live_security_token),
                false,
                false,
            ),
            (
                "bad-seed-signature",
                bad_signature_credentials,
                S3StreamingTokens::One(fixture.live_security_token),
                true,
                false,
            ),
            (
                "bad-chunk-signature",
                credentials,
                S3StreamingTokens::One(fixture.live_security_token),
                true,
                true,
            ),
            (
                "old-session",
                old_credentials,
                S3StreamingTokens::One(fixture.old_security_token),
                true,
                false,
            ),
            (
                "old-session-bad-seed-signature",
                old_bad_signature_credentials,
                S3StreamingTokens::One(fixture.old_security_token),
                true,
                false,
            ),
            (
                "old-session-bad-chunk-signature",
                old_credentials,
                S3StreamingTokens::One(fixture.old_security_token),
                true,
                true,
            ),
        ] {
            let label = format!("streaming-scope-{scope}-{case}");
            let result = send_s3_streaming_request(
                endpoint,
                bucket,
                S3StreamingRequest {
                    label: &label,
                    credentials: request_credentials,
                    tokens,
                    sign_token_header,
                    bad_chunk_signature,
                    service,
                },
            );
            let security_tokens = tokens.values().into_iter().flatten().collect::<Vec<_>>();
            if service == "s3" {
                assert_s3_streaming_wrong_region_scope(
                    &label,
                    &result.response,
                    wrong_region,
                    fixture.live_credentials.region,
                    request_credentials.access_key,
                    &security_tokens,
                );
            } else {
                assert_s3_streaming_wrong_service_scope(
                    &label,
                    &result.response,
                    request_credentials.access_key,
                    &security_tokens,
                );
            }
        }
    }

    let label = "streaming-scope-both-wrong-valid";
    let both_wrong_credentials = SignedRequestCredentials {
        region: wrong_region,
        ..fixture.live_credentials
    };
    let tokens = S3StreamingTokens::One(fixture.live_security_token);
    let result = send_s3_streaming_request(
        endpoint,
        bucket,
        S3StreamingRequest {
            label,
            credentials: both_wrong_credentials,
            tokens,
            sign_token_header: true,
            bad_chunk_signature: false,
            service: "sts",
        },
    );
    assert_s3_streaming_wrong_region_scope(
        label,
        &result.response,
        wrong_region,
        fixture.live_credentials.region,
        both_wrong_credentials.access_key,
        &[fixture.live_security_token],
    );
}

fn assert_error_probe(
    label: &str,
    response: &RawResponse,
    status: u16,
    namespace: &str,
    code: &str,
    message: Option<&str>,
) {
    let message_element = message
        .map(|message| format!("    <Message>{message}</Message>\n"))
        .unwrap_or_default();
    assert_shape(
        label,
        response,
        &sts_wire_shape(label, response)
            .status(status)
            .header("content-type", "text/xml")
            .body(format!(
                "<ErrorResponse xmlns=\"{namespace}\">\n  <Error>\n    \
                 <Type>Sender</Type>\n    <Code>{code}</Code>\n{message_element}  </Error>\n  \
                 <RequestId>{{sts_request_id}}</RequestId>\n</ErrorResponse>\n"
            )),
    );
}

fn form_body(parameters: &[(&str, &str)]) -> String {
    url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(parameters.iter().copied())
        .finish()
}

fn send_assume_role(
    endpoint: &str,
    credentials: SignedRequestCredentials<'_>,
    parameters: &[(&str, &str)],
) -> RawResponse {
    let body = form_body(parameters);
    QueryRequest::Post {
        body: &body,
        content_type: Some(QUERY_CONTENT_TYPE),
    }
    .send(endpoint, credentials)
}

fn send_assume_role_with_security_token(
    endpoint: &str,
    credentials: SignedRequestCredentials<'_>,
    security_token: &str,
    parameters: &[(&str, &str)],
) -> RawResponse {
    let body = form_body(parameters);
    QueryRequest::Post {
        body: &body,
        content_type: Some(QUERY_CONTENT_TYPE),
    }
    .send_with_security_token(endpoint, credentials, Some(security_token))
}

fn assert_assume_role_error(
    label: &str,
    endpoint: &str,
    credentials: SignedRequestCredentials<'_>,
    parameters: &[(&str, &str)],
    status: u16,
    code: &str,
    message: Option<&str>,
) {
    let response = send_assume_role(endpoint, credentials, parameters);
    assert_error_probe(label, &response, status, STS_XMLNS, code, message);
    println!("{label}: ok");
}

struct AssumeRoleErrorExpected<'a> {
    status: u16,
    code: &'a str,
    message: Option<&'a str>,
}

fn assert_assume_role_error_with_security_token(
    label: &str,
    endpoint: &str,
    credentials: SignedRequestCredentials<'_>,
    security_token: &str,
    parameters: &[(&str, &str)],
    expected: AssumeRoleErrorExpected<'_>,
) {
    let response =
        send_assume_role_with_security_token(endpoint, credentials, security_token, parameters);
    assert_error_probe(
        label,
        &response,
        expected.status,
        STS_XMLNS,
        expected.code,
        expected.message,
    );
    println!("{label}: ok");
}

fn required_xml_text(response: &RawResponse, tag: &str, label: &str) -> String {
    xml_tag_text(&response.body, tag)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| panic!("{label}: missing or empty {tag}"))
        .to_string()
}

fn replace_sensitive_xml_text(body: &mut String, tag: &str, value: &str, marker: &str) {
    let needle = format!("<{tag}>{value}</{tag}>");
    assert!(
        body.matches(&needle).count() == 1,
        "expected exactly one {tag} element while normalizing sensitive STS output"
    );
    *body = body.replacen(&needle, &format!("<{tag}>{marker}</{tag}>"), 1);
}

fn assert_assume_role_success(
    label: &str,
    response: &RawResponse,
    account_id: &str,
    role_name: &str,
    role_session_name: &str,
    duration_seconds: i64,
    source_identity: Option<&str>,
) {
    let access_key = required_xml_text(response, "AccessKeyId", label);
    let secret_key = required_xml_text(response, "SecretAccessKey", label);
    let session_token = required_xml_text(response, "SessionToken", label);
    let assumed_role_id = required_xml_text(response, "AssumedRoleId", label);
    let expiration = required_xml_text(response, "Expiration", label);

    let response_date = response_header_value(response, "date")
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| panic!("{label}: missing date response header"));
    let response_date = DateTime::from_str(response_date, DateTimeFormat::HttpDate)
        .unwrap_or_else(|error| panic!("{label}: invalid date response header: {error}"));
    let expiration = DateTime::from_str(&expiration, DateTimeFormat::DateTime)
        .unwrap_or_else(|error| panic!("{label}: invalid Expiration timestamp: {error}"));
    let expiration_from_response_date = expiration.secs() - response_date.secs();
    assert!(
        expiration_from_response_date == duration_seconds
            || expiration_from_response_date == duration_seconds - 1,
        "{label}: Expiration differs unexpectedly from the requested session duration: response-date delta is {expiration_from_response_date} seconds"
    );

    assert!(
        access_key.len() == 20
            && access_key.starts_with("ASIA")
            && access_key.bytes().all(|byte| byte.is_ascii_alphanumeric()),
        "{label}: temporary AWS access key has an unexpected shape"
    );
    assert!(
        secret_key.len() == 40
            && secret_key
                .bytes()
                .all(|byte| { byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/' | b'=') }),
        "{label}: temporary AWS secret key has an unexpected shape"
    );
    assert!(
        session_token.len() >= 100
            && session_token
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/' | b'=')),
        "{label}: AWS session token has an unexpected shape"
    );

    let role_id = assumed_role_id
        .strip_suffix(&format!(":{role_session_name}"))
        .unwrap_or_else(|| panic!("{label}: AssumedRoleId does not end with the session name"));
    assert!(
        role_id.len() == 21
            && role_id.starts_with("AROA")
            && role_id.bytes().all(|byte| byte.is_ascii_alphanumeric()),
        "{label}: IAM role unique ID has an unexpected shape"
    );

    let mut normalized = response.clone();
    replace_sensitive_xml_text(
        &mut normalized.body,
        "AccessKeyId",
        &access_key,
        "SESSION_ACCESS_KEY",
    );
    replace_sensitive_xml_text(
        &mut normalized.body,
        "SecretAccessKey",
        &secret_key,
        "SESSION_SECRET_KEY",
    );
    replace_sensitive_xml_text(
        &mut normalized.body,
        "SessionToken",
        &session_token,
        "SESSION_TOKEN",
    );
    replace_sensitive_xml_text(
        &mut normalized.body,
        "AssumedRoleId",
        &assumed_role_id,
        &format!("ROLE_UNIQUE_ID:{role_session_name}"),
    );

    let assumed_role_arn =
        format!("arn:aws:sts::{account_id}:assumed-role/{role_name}/{role_session_name}");
    let source_identity_element = source_identity
        .map(|value| format!("    <SourceIdentity>{value}</SourceIdentity>\n"))
        .unwrap_or_default();
    assert_shape(
        label,
        &normalized,
        &sts_wire_shape(label, &normalized)
            .status(200)
            .header("content-type", "text/xml")
            .body(format!(
                "<AssumeRoleResponse xmlns=\"{STS_XMLNS}\">\n  <AssumeRoleResult>\n    \
                 <AssumedRoleUser>\n      \
                 <AssumedRoleId>ROLE_UNIQUE_ID:{role_session_name}</AssumedRoleId>\n      \
                 <Arn>{{assumed_role_arn}}</Arn>\n    </AssumedRoleUser>\n    \
                 <Credentials>\n      <AccessKeyId>SESSION_ACCESS_KEY</AccessKeyId>\n      \
                 <SecretAccessKey>SESSION_SECRET_KEY</SecretAccessKey>\n      \
                 <SessionToken>SESSION_TOKEN</SessionToken>\n      \
                 <Expiration>{{iso8601}}</Expiration>\n    </Credentials>\n{source_identity_element}  \
                 </AssumeRoleResult>\n  \
                 <ResponseMetadata>\n    <RequestId>{{sts_request_id}}</RequestId>\n  \
                 </ResponseMetadata>\n</AssumeRoleResponse>\n"
            ))
            .sub("assumed_role_arn", assumed_role_arn),
    );
}

struct AssumeRoleSuccess<'a> {
    account_id: &'a str,
    role_name: &'a str,
    role_session_name: &'a str,
    duration_seconds: i64,
}

fn assert_assume_role_request_success(
    label: &str,
    endpoint: &str,
    credentials: SignedRequestCredentials<'_>,
    parameters: &[(&str, &str)],
    expected: AssumeRoleSuccess<'_>,
) {
    let response = send_assume_role(endpoint, credentials, parameters);
    assert_assume_role_success(
        label,
        &response,
        expected.account_id,
        expected.role_name,
        expected.role_session_name,
        expected.duration_seconds,
        None,
    );
    println!("{label}: ok");
}

fn assert_assume_role_source_identity_success(
    label: &str,
    endpoint: &str,
    credentials: SignedRequestCredentials<'_>,
    security_token: Option<&str>,
    parameters: &[(&str, &str)],
    expected: AssumeRoleSuccess<'_>,
    source_identity: &str,
) {
    let response = match security_token {
        Some(token) => {
            send_assume_role_with_security_token(endpoint, credentials, token, parameters)
        }
        None => send_assume_role(endpoint, credentials, parameters),
    };
    assert_assume_role_success(
        label,
        &response,
        expected.account_id,
        expected.role_name,
        expected.role_session_name,
        expected.duration_seconds,
        Some(source_identity),
    );
    println!("{label}: ok");
}

struct AssumeRoleProbeSet<'a> {
    caller_arn: &'a str,
    role_arn: &'a str,
    role_name: &'a str,
    default_max_role_arn: &'a str,
    default_max_role_name: &'a str,
    external_id: &'a str,
    external_id_role_arn: &'a str,
    external_id_role_name: &'a str,
    role_session_name: &'a str,
}

fn run_assume_role_probes(
    endpoint: &str,
    credentials: SignedRequestCredentials<'_>,
    account_id: &str,
    fixture: AssumeRoleProbeSet<'_>,
) {
    let AssumeRoleProbeSet {
        caller_arn,
        role_arn,
        role_name,
        default_max_role_arn,
        default_max_role_name,
        external_id,
        external_id_role_arn,
        external_id_role_name,
        role_session_name,
    } = fixture;
    let missing_role_arn = format!("{role_arn}-missing");
    let malformed_role_arn = "x".repeat(20);
    let max_role_arn = "x".repeat(2048);
    let overlong_role_arn = "x".repeat(2049);
    let max_bmp_role_arn = "é".repeat(2048);
    let overlong_bmp_role_arn = "é".repeat(2049);
    let supplementary_2048_role_arn = "😀".repeat(2048);
    let supplementary_2049_role_arn = "😀".repeat(2049);
    let decomposed_1025_role_arn = "e\u{301}".repeat(1025);
    let long_session_name = "a".repeat(65);
    let max_session_name = "b".repeat(64);
    let max_external_id = "e".repeat(1224);
    let overlong_external_id = "e".repeat(1225);
    let overlong_invalid_external_id = "!".repeat(1225);
    let multibyte_external_id = "é".repeat(613);
    let supplementary_external_id = "😀".repeat(613);
    let unknown_role_message = format!(
        "User: {caller_arn} is not authorized to perform: sts:AssumeRole on resource: {missing_role_arn}"
    );
    let long_session_message = format!(
        "1 validation error detected: Value '{long_session_name}' at 'roleSessionName' failed to satisfy constraint: Member must have length less than or equal to 64"
    );
    let malformed_role_message = format!("{malformed_role_arn} is invalid");
    let max_role_message = format!("{max_role_arn} is invalid");
    let overlong_role_message = format!(
        "1 validation error detected: Value '{overlong_role_arn}' at 'roleArn' failed to satisfy constraint: Member must have length less than or equal to 2048"
    );
    let role_arn_pattern =
        r"[\u0009\u000A\u000D\u0020-\u007E\u0085\u00A0-\uD7FF\uE000-\uFFFD\u10000-\u10FFFF]+";

    assert_eq!(max_bmp_role_arn.len(), 4096);
    assert_eq!(max_bmp_role_arn.chars().count(), 2048);
    assert_eq!(max_bmp_role_arn.encode_utf16().count(), 2048);
    assert_eq!(supplementary_2048_role_arn.len(), 8192);
    assert_eq!(supplementary_2048_role_arn.chars().count(), 2048);
    assert_eq!(supplementary_2048_role_arn.encode_utf16().count(), 4096);
    assert_eq!(decomposed_1025_role_arn.len(), 3075);
    assert_eq!(decomposed_1025_role_arn.chars().count(), 2050);
    assert_eq!(decomposed_1025_role_arn.encode_utf16().count(), 2050);
    assert_eq!(max_external_id.len(), 1224);
    assert_eq!(overlong_external_id.len(), 1225);
    assert_eq!(overlong_invalid_external_id.len(), 1225);
    assert_eq!(multibyte_external_id.len(), 1226);
    assert_eq!(multibyte_external_id.chars().count(), 613);
    assert_eq!(multibyte_external_id.encode_utf16().count(), 613);
    assert_eq!(supplementary_external_id.len(), 2452);
    assert_eq!(supplementary_external_id.chars().count(), 613);
    assert_eq!(supplementary_external_id.encode_utf16().count(), 1226);

    let max_bmp_role_message = format!("{max_bmp_role_arn} is invalid");
    let overlong_bmp_role_message = format!(
        "1 validation error detected: Value '{overlong_bmp_role_arn}' at 'roleArn' failed to satisfy constraint: Member must have length less than or equal to 2048"
    );
    let supplementary_2048_role_message = format!(
        "1 validation error detected: Value '{supplementary_2048_role_arn}' at 'roleArn' failed to satisfy constraint: Member must satisfy regular expression pattern: {role_arn_pattern}"
    );
    let supplementary_2049_role_message = format!(
        "2 validation errors detected: Value '{supplementary_2049_role_arn}' at 'roleArn' failed to satisfy constraint: Member must satisfy regular expression pattern: {role_arn_pattern}; Value '{supplementary_2049_role_arn}' at 'roleArn' failed to satisfy constraint: Member must have length less than or equal to 2048"
    );
    let decomposed_1025_role_message = format!(
        "1 validation error detected: Value '{decomposed_1025_role_arn}' at 'roleArn' failed to satisfy constraint: Member must have length less than or equal to 2048"
    );
    let external_id_pattern = r"[\w+=,.@:\/-]*";
    let overlong_external_id_message = format!(
        "1 validation error detected: Value '{overlong_external_id}' at 'externalId' failed to satisfy constraint: Member must have length less than or equal to 1224"
    );
    let overlong_invalid_external_id_message = format!(
        "2 validation errors detected: Value '{overlong_invalid_external_id}' at 'externalId' failed to satisfy constraint: Member must satisfy regular expression pattern: {external_id_pattern}; Value '{overlong_invalid_external_id}' at 'externalId' failed to satisfy constraint: Member must have length less than or equal to 1224"
    );
    let multibyte_external_id_message = format!(
        "1 validation error detected: Value '{multibyte_external_id}' at 'externalId' failed to satisfy constraint: Member must satisfy regular expression pattern: {external_id_pattern}"
    );
    let supplementary_external_id_message = format!(
        "1 validation error detected: Value '{supplementary_external_id}' at 'externalId' failed to satisfy constraint: Member must satisfy regular expression pattern: {external_id_pattern}"
    );

    for (label, value, message) in [
        (
            "assume-role-max-length-bmp-role-arn",
            max_bmp_role_arn.as_str(),
            max_bmp_role_message.as_str(),
        ),
        (
            "assume-role-overlong-bmp-role-arn",
            overlong_bmp_role_arn.as_str(),
            overlong_bmp_role_message.as_str(),
        ),
        (
            "assume-role-max-length-supplementary-role-arn",
            supplementary_2048_role_arn.as_str(),
            supplementary_2048_role_message.as_str(),
        ),
        (
            "assume-role-overlong-supplementary-role-arn",
            supplementary_2049_role_arn.as_str(),
            supplementary_2049_role_message.as_str(),
        ),
        (
            "assume-role-unnormalized-decomposed-role-arn",
            decomposed_1025_role_arn.as_str(),
            decomposed_1025_role_message.as_str(),
        ),
    ] {
        assert_assume_role_error(
            label,
            endpoint,
            credentials,
            &[
                ("Action", "AssumeRole"),
                ("Version", "2011-06-15"),
                ("RoleArn", value),
                ("RoleSessionName", role_session_name),
            ],
            400,
            "ValidationError",
            Some(message),
        );
    }

    assert_assume_role_error(
        "assume-role-missing-role-arn",
        endpoint,
        credentials,
        &[
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleSessionName", role_session_name),
        ],
        400,
        "ValidationError",
        Some("1 validation error detected: Value null at 'roleArn' failed to satisfy constraint: Member must not be null"),
    );
    assert_assume_role_error(
        "assume-role-empty-role-arn",
        endpoint,
        credentials,
        &[
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", ""),
            ("RoleSessionName", role_session_name),
        ],
        400,
        "ValidationError",
        Some(
            r"2 validation errors detected: Value '' at 'roleArn' failed to satisfy constraint: Member must satisfy regular expression pattern: [\u0009\u000A\u000D\u0020-\u007E\u0085\u00A0-\uD7FF\uE000-\uFFFD\u10000-\u10FFFF]+; Value '' at 'roleArn' failed to satisfy constraint: Member must have length greater than or equal to 20",
        ),
    );
    assert_assume_role_error(
        "assume-role-short-role-arn",
        endpoint,
        credentials,
        &[
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", "not-an-arn"),
            ("RoleSessionName", role_session_name),
        ],
        400,
        "ValidationError",
        Some("1 validation error detected: Value 'not-an-arn' at 'roleArn' failed to satisfy constraint: Member must have length greater than or equal to 20"),
    );
    assert_assume_role_error(
        "assume-role-malformed-role-arn",
        endpoint,
        credentials,
        &[
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", &malformed_role_arn),
            ("RoleSessionName", role_session_name),
        ],
        400,
        "ValidationError",
        Some(&malformed_role_message),
    );
    for (label, value, message) in [
        (
            "assume-role-max-length-role-arn",
            max_role_arn.as_str(),
            max_role_message.as_str(),
        ),
        (
            "assume-role-overlong-role-arn",
            overlong_role_arn.as_str(),
            overlong_role_message.as_str(),
        ),
    ] {
        assert_assume_role_error(
            label,
            endpoint,
            credentials,
            &[
                ("Action", "AssumeRole"),
                ("Version", "2011-06-15"),
                ("RoleArn", value),
                ("RoleSessionName", role_session_name),
            ],
            400,
            "ValidationError",
            Some(message),
        );
    }
    assert_assume_role_error(
        "assume-role-unknown-role-arn",
        endpoint,
        credentials,
        &[
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", &missing_role_arn),
            ("RoleSessionName", role_session_name),
        ],
        403,
        "AccessDenied",
        Some(&unknown_role_message),
    );
    assert_assume_role_error(
        "assume-role-missing-session-name",
        endpoint,
        credentials,
        &[
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", role_arn),
        ],
        400,
        "ValidationError",
        Some("1 validation error detected: Value null at 'roleSessionName' failed to satisfy constraint: Member must not be null"),
    );
    for (label, value, message) in [
        (
            "assume-role-empty-session-name",
            "",
            "1 validation error detected: Value '' at 'roleSessionName' failed to satisfy constraint: Member must have length greater than or equal to 2",
        ),
        (
            "assume-role-short-session-name",
            "a",
            "1 validation error detected: Value 'a' at 'roleSessionName' failed to satisfy constraint: Member must have length greater than or equal to 2",
        ),
        (
            "assume-role-invalid-session-name",
            "bad/name",
            r"1 validation error detected: Value 'bad/name' at 'roleSessionName' failed to satisfy constraint: Member must satisfy regular expression pattern: [\w+=,.@-]*",
        ),
    ] {
        assert_assume_role_error(
            label,
            endpoint,
            credentials,
            &[
                ("Action", "AssumeRole"),
                ("Version", "2011-06-15"),
                ("RoleArn", role_arn),
                ("RoleSessionName", value),
            ],
            400,
            "ValidationError",
            Some(message),
        );
    }
    assert_assume_role_error(
        "assume-role-long-session-name",
        endpoint,
        credentials,
        &[
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", role_arn),
            ("RoleSessionName", &long_session_name),
        ],
        400,
        "ValidationError",
        Some(&long_session_message),
    );

    for (label, value, message) in [
        (
            "assume-role-empty-external-id",
            "",
            "1 validation error detected: Value '' at 'externalId' failed to satisfy constraint: Member must have length greater than or equal to 2",
        ),
        (
            "assume-role-short-external-id",
            "a",
            "1 validation error detected: Value 'a' at 'externalId' failed to satisfy constraint: Member must have length greater than or equal to 2",
        ),
        (
            "assume-role-invalid-external-id",
            "bad value",
            r"1 validation error detected: Value 'bad value' at 'externalId' failed to satisfy constraint: Member must satisfy regular expression pattern: [\w+=,.@:\/-]*",
        ),
        (
            "assume-role-short-invalid-external-id",
            "!",
            r"2 validation errors detected: Value '!' at 'externalId' failed to satisfy constraint: Member must satisfy regular expression pattern: [\w+=,.@:\/-]*; Value '!' at 'externalId' failed to satisfy constraint: Member must have length greater than or equal to 2",
        ),
        (
            "assume-role-overlong-external-id",
            overlong_external_id.as_str(),
            overlong_external_id_message.as_str(),
        ),
        (
            "assume-role-overlong-invalid-external-id",
            overlong_invalid_external_id.as_str(),
            overlong_invalid_external_id_message.as_str(),
        ),
        (
            "assume-role-multibyte-external-id-length-units",
            multibyte_external_id.as_str(),
            multibyte_external_id_message.as_str(),
        ),
        (
            "assume-role-supplementary-external-id-length-units",
            supplementary_external_id.as_str(),
            supplementary_external_id_message.as_str(),
        ),
    ] {
        assert_assume_role_error(
            label,
            endpoint,
            credentials,
            &[
                ("Action", "AssumeRole"),
                ("Version", "2011-06-15"),
                ("RoleArn", role_arn),
                ("RoleSessionName", role_session_name),
                ("ExternalId", value),
            ],
            400,
            "ValidationError",
            Some(message),
        );
    }

    for (label, value, code, message) in [
        (
            "assume-role-empty-duration",
            "",
            "MalformedInput",
            Some("missing value for decimal type"),
        ),
        (
            "assume-role-nonnumeric-duration",
            "abc",
            "MalformedInput",
            None,
        ),
        (
            "assume-role-decimal-duration",
            "900.0",
            "MalformedInput",
            None,
        ),
        (
            "assume-role-negative-duration",
            "-1",
            "ValidationError",
            Some("1 validation error detected: Value '-1' at 'durationSeconds' failed to satisfy constraint: Member must have value greater than or equal to 900"),
        ),
        (
            "assume-role-short-duration",
            "899",
            "ValidationError",
            Some("1 validation error detected: Value '899' at 'durationSeconds' failed to satisfy constraint: Member must have value greater than or equal to 900"),
        ),
        (
            "assume-role-long-duration",
            "43201",
            "ValidationError",
            Some("1 validation error detected: Value '43201' at 'durationSeconds' failed to satisfy constraint: Member must have value less than or equal to 43200"),
        ),
        (
            "assume-role-i32-max-duration",
            "2147483647",
            "ValidationError",
            Some("1 validation error detected: Value '2147483647' at 'durationSeconds' failed to satisfy constraint: Member must have value less than or equal to 43200"),
        ),
        (
            "assume-role-i32-overflow-duration",
            "2147483648",
            "MalformedInput",
            None,
        ),
        (
            "assume-role-i32-min-duration",
            "-2147483648",
            "ValidationError",
            Some("1 validation error detected: Value '-2147483648' at 'durationSeconds' failed to satisfy constraint: Member must have value greater than or equal to 900"),
        ),
        (
            "assume-role-i32-underflow-duration",
            "-2147483649",
            "MalformedInput",
            None,
        ),
    ] {
        assert_assume_role_error(
            label,
            endpoint,
            credentials,
            &[
                ("Action", "AssumeRole"),
                ("Version", "2011-06-15"),
                ("RoleArn", role_arn),
                ("RoleSessionName", role_session_name),
                ("DurationSeconds", value),
            ],
            400,
            code,
            message,
        );
    }

    assert_assume_role_request_success(
        "assume-role-path-bearing-role",
        endpoint,
        credentials,
        &[
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", role_arn),
            ("RoleSessionName", role_session_name),
        ],
        AssumeRoleSuccess {
            account_id,
            role_name,
            role_session_name,
            duration_seconds: 3600,
        },
    );
    for (label, session_name) in [
        ("assume-role-min-session-name", "aa"),
        ("assume-role-max-session-name", max_session_name.as_str()),
    ] {
        assert_assume_role_request_success(
            label,
            endpoint,
            credentials,
            &[
                ("Action", "AssumeRole"),
                ("Version", "2011-06-15"),
                ("RoleArn", role_arn),
                ("RoleSessionName", session_name),
            ],
            AssumeRoleSuccess {
                account_id,
                role_name,
                role_session_name: session_name,
                duration_seconds: 3600,
            },
        );
    }
    for (label, external_id) in [
        ("assume-role-min-external-id", "ab"),
        (
            "assume-role-all-allowed-external-id-characters",
            "azAZ09_+=,.@:/-",
        ),
        ("assume-role-max-external-id", max_external_id.as_str()),
    ] {
        assert_assume_role_request_success(
            label,
            endpoint,
            credentials,
            &[
                ("Action", "AssumeRole"),
                ("Version", "2011-06-15"),
                ("RoleArn", role_arn),
                ("RoleSessionName", role_session_name),
                ("ExternalId", external_id),
            ],
            AssumeRoleSuccess {
                account_id,
                role_name,
                role_session_name,
                duration_seconds: 3600,
            },
        );
    }
    let external_id_denied_message = format!(
        "User: {caller_arn} is not authorized to perform: sts:AssumeRole on resource: {external_id_role_arn}"
    );
    assert_assume_role_request_success(
        "assume-role-external-id-trust-match",
        endpoint,
        credentials,
        &[
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", external_id_role_arn),
            ("RoleSessionName", role_session_name),
            ("ExternalId", external_id),
        ],
        AssumeRoleSuccess {
            account_id,
            role_name: external_id_role_name,
            role_session_name,
            duration_seconds: 3600,
        },
    );
    for (label, external_id_parameter) in [
        ("assume-role-external-id-trust-missing", None),
        (
            "assume-role-external-id-trust-mismatch",
            Some("wrong-external-id"),
        ),
    ] {
        let mut parameters = vec![
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", external_id_role_arn),
            ("RoleSessionName", role_session_name),
        ];
        if let Some(value) = external_id_parameter {
            parameters.push(("ExternalId", value));
        }
        assert_assume_role_error(
            label,
            endpoint,
            credentials,
            &parameters,
            403,
            "AccessDenied",
            Some(&external_id_denied_message),
        );
    }
    assert_assume_role_request_success(
        "assume-role-duplicate-external-id-trust-match-first",
        endpoint,
        credentials,
        &[
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", external_id_role_arn),
            ("RoleSessionName", role_session_name),
            ("ExternalId", external_id),
            ("ExternalId", "wrong-external-id"),
        ],
        AssumeRoleSuccess {
            account_id,
            role_name: external_id_role_name,
            role_session_name,
            duration_seconds: 3600,
        },
    );
    assert_assume_role_error(
        "assume-role-duplicate-external-id-trust-mismatch-first",
        endpoint,
        credentials,
        &[
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", external_id_role_arn),
            ("RoleSessionName", role_session_name),
            ("ExternalId", "wrong-external-id"),
            ("ExternalId", external_id),
        ],
        403,
        "AccessDenied",
        Some(&external_id_denied_message),
    );
    for (label, duration, duration_seconds) in [
        ("assume-role-min-duration", "900", 900),
        ("assume-role-max-duration", "43200", 43200),
        ("assume-role-leading-plus-duration", "+900", 900),
        ("assume-role-leading-zero-duration", "0900", 900),
    ] {
        assert_assume_role_request_success(
            label,
            endpoint,
            credentials,
            &[
                ("Action", "AssumeRole"),
                ("Version", "2011-06-15"),
                ("RoleArn", role_arn),
                ("RoleSessionName", role_session_name),
                ("DurationSeconds", duration),
            ],
            AssumeRoleSuccess {
                account_id,
                role_name,
                role_session_name,
                duration_seconds,
            },
        );
    }
    assert_assume_role_request_success(
        "assume-role-unknown-parameter",
        endpoint,
        credentials,
        &[
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", role_arn),
            ("RoleSessionName", role_session_name),
            ("Unknown", "value"),
        ],
        AssumeRoleSuccess {
            account_id,
            role_name,
            role_session_name,
            duration_seconds: 3600,
        },
    );

    assert_assume_role_request_success(
        "assume-role-duplicate-role-arn-valid-first",
        endpoint,
        credentials,
        &[
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", role_arn),
            ("RoleArn", "not-an-arn"),
            ("RoleSessionName", role_session_name),
        ],
        AssumeRoleSuccess {
            account_id,
            role_name,
            role_session_name,
            duration_seconds: 3600,
        },
    );
    assert_assume_role_error(
        "assume-role-duplicate-role-arn-invalid-first",
        endpoint,
        credentials,
        &[
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", "not-an-arn"),
            ("RoleArn", role_arn),
            ("RoleSessionName", role_session_name),
        ],
        400,
        "ValidationError",
        Some("1 validation error detected: Value 'not-an-arn' at 'roleArn' failed to satisfy constraint: Member must have length greater than or equal to 20"),
    );
    assert_assume_role_request_success(
        "assume-role-duplicate-session-name-valid-first",
        endpoint,
        credentials,
        &[
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", role_arn),
            ("RoleSessionName", role_session_name),
            ("RoleSessionName", "bad/name"),
        ],
        AssumeRoleSuccess {
            account_id,
            role_name,
            role_session_name,
            duration_seconds: 3600,
        },
    );
    assert_assume_role_error(
        "assume-role-duplicate-session-name-invalid-first",
        endpoint,
        credentials,
        &[
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", role_arn),
            ("RoleSessionName", "bad/name"),
            ("RoleSessionName", role_session_name),
        ],
        400,
        "ValidationError",
        Some(
            r"1 validation error detected: Value 'bad/name' at 'roleSessionName' failed to satisfy constraint: Member must satisfy regular expression pattern: [\w+=,.@-]*",
        ),
    );
    assert_assume_role_request_success(
        "assume-role-duplicate-external-id-valid-first",
        endpoint,
        credentials,
        &[
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", role_arn),
            ("RoleSessionName", role_session_name),
            ("ExternalId", "valid-external-id"),
            ("ExternalId", "bad value"),
        ],
        AssumeRoleSuccess {
            account_id,
            role_name,
            role_session_name,
            duration_seconds: 3600,
        },
    );
    assert_assume_role_error(
        "assume-role-duplicate-external-id-invalid-first",
        endpoint,
        credentials,
        &[
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", role_arn),
            ("RoleSessionName", role_session_name),
            ("ExternalId", "bad value"),
            ("ExternalId", "valid-external-id"),
        ],
        400,
        "ValidationError",
        Some(
            r"1 validation error detected: Value 'bad value' at 'externalId' failed to satisfy constraint: Member must satisfy regular expression pattern: [\w+=,.@:\/-]*",
        ),
    );
    assert_assume_role_request_success(
        "assume-role-duplicate-duration-valid-first",
        endpoint,
        credentials,
        &[
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", role_arn),
            ("RoleSessionName", role_session_name),
            ("DurationSeconds", "900"),
            ("DurationSeconds", "899"),
        ],
        AssumeRoleSuccess {
            account_id,
            role_name,
            role_session_name,
            duration_seconds: 900,
        },
    );
    assert_assume_role_error(
        "assume-role-duplicate-duration-invalid-first",
        endpoint,
        credentials,
        &[
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", role_arn),
            ("RoleSessionName", role_session_name),
            ("DurationSeconds", "899"),
            ("DurationSeconds", "900"),
        ],
        400,
        "ValidationError",
        Some("1 validation error detected: Value '899' at 'durationSeconds' failed to satisfy constraint: Member must have value greater than or equal to 900"),
    );

    assert_assume_role_request_success(
        "assume-role-at-role-maximum-duration",
        endpoint,
        credentials,
        &[
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", default_max_role_arn),
            ("RoleSessionName", role_session_name),
            ("DurationSeconds", "3600"),
        ],
        AssumeRoleSuccess {
            account_id,
            role_name: default_max_role_name,
            role_session_name,
            duration_seconds: 3600,
        },
    );
    assert_assume_role_error(
        "assume-role-over-role-maximum-duration",
        endpoint,
        credentials,
        &[
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", default_max_role_arn),
            ("RoleSessionName", role_session_name),
            ("DurationSeconds", "3601"),
        ],
        400,
        "ValidationError",
        Some("The requested DurationSeconds exceeds the MaxSessionDuration set for this role."),
    );
    assert_assume_role_error(
        "assume-role-over-api-maximum-on-low-maximum-role",
        endpoint,
        credentials,
        &[
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", default_max_role_arn),
            ("RoleSessionName", role_session_name),
            ("DurationSeconds", "43201"),
        ],
        400,
        "ValidationError",
        Some("1 validation error detected: Value '43201' at 'durationSeconds' failed to satisfy constraint: Member must have value less than or equal to 43200"),
    );
}

struct SourceIdentityProbeSet<'a> {
    caller_arn: &'a str,
    source_identity: &'a str,
    source_role_arn: &'a str,
    source_role_name: &'a str,
    condition_role_arn: &'a str,
    condition_role_name: &'a str,
    role_session_name: &'a str,
    source_session_token: &'a str,
    source_session_role_name: &'a str,
    source_session_name: &'a str,
    target_role_arn: &'a str,
    target_role_name: &'a str,
    target_session_name: &'a str,
    no_set_target_role_arn: &'a str,
}

fn run_source_identity_probes(
    endpoint: &str,
    primary_credentials: SignedRequestCredentials<'_>,
    source_credentials: SignedRequestCredentials<'_>,
    account_id: &str,
    fixture: SourceIdentityProbeSet<'_>,
) {
    let SourceIdentityProbeSet {
        caller_arn,
        source_identity,
        source_role_arn,
        source_role_name,
        condition_role_arn,
        condition_role_name,
        role_session_name,
        source_session_token,
        source_session_role_name,
        source_session_name,
        target_role_arn,
        target_role_name,
        target_session_name,
        no_set_target_role_arn,
    } = fixture;
    let max_source_identity = "s".repeat(64);
    let overlong_source_identity = "s".repeat(65);
    let overlong_invalid_source_identity = "!".repeat(65);
    let multibyte_source_identity = "é".repeat(33);
    let supplementary_source_identity = "😀".repeat(33);
    let source_identity_pattern = r"[\w+=,.@-]*";
    let overlong_source_identity_message = format!(
        "1 validation error detected: Value '{overlong_source_identity}' at 'sourceIdentity' failed to satisfy constraint: Member must have length less than or equal to 64"
    );
    let overlong_invalid_source_identity_message = format!(
        "2 validation errors detected: Value '{overlong_invalid_source_identity}' at 'sourceIdentity' failed to satisfy constraint: Member must satisfy regular expression pattern: {source_identity_pattern}; Value '{overlong_invalid_source_identity}' at 'sourceIdentity' failed to satisfy constraint: Member must have length less than or equal to 64"
    );
    let multibyte_source_identity_message = format!(
        "1 validation error detected: Value '{multibyte_source_identity}' at 'sourceIdentity' failed to satisfy constraint: Member must satisfy regular expression pattern: {source_identity_pattern}"
    );
    let supplementary_source_identity_message = format!(
        "1 validation error detected: Value '{supplementary_source_identity}' at 'sourceIdentity' failed to satisfy constraint: Member must satisfy regular expression pattern: {source_identity_pattern}"
    );
    assert_eq!(max_source_identity.len(), 64);
    assert_eq!(overlong_source_identity.len(), 65);
    assert_eq!(overlong_invalid_source_identity.len(), 65);
    assert_eq!(multibyte_source_identity.len(), 66);
    assert_eq!(multibyte_source_identity.chars().count(), 33);
    assert_eq!(multibyte_source_identity.encode_utf16().count(), 33);
    assert_eq!(supplementary_source_identity.len(), 132);
    assert_eq!(supplementary_source_identity.chars().count(), 33);
    assert_eq!(supplementary_source_identity.encode_utf16().count(), 66);

    for (label, value, message) in [
        (
            "assume-role-empty-source-identity",
            "",
            "1 validation error detected: Value '' at 'sourceIdentity' failed to satisfy constraint: Member must have length greater than or equal to 2",
        ),
        (
            "assume-role-short-source-identity",
            "a",
            "1 validation error detected: Value 'a' at 'sourceIdentity' failed to satisfy constraint: Member must have length greater than or equal to 2",
        ),
        (
            "assume-role-invalid-source-identity",
            "bad value",
            r"1 validation error detected: Value 'bad value' at 'sourceIdentity' failed to satisfy constraint: Member must satisfy regular expression pattern: [\w+=,.@-]*",
        ),
        (
            "assume-role-short-invalid-source-identity",
            "!",
            r"2 validation errors detected: Value '!' at 'sourceIdentity' failed to satisfy constraint: Member must satisfy regular expression pattern: [\w+=,.@-]*; Value '!' at 'sourceIdentity' failed to satisfy constraint: Member must have length greater than or equal to 2",
        ),
        (
            "assume-role-overlong-source-identity",
            overlong_source_identity.as_str(),
            overlong_source_identity_message.as_str(),
        ),
        (
            "assume-role-overlong-invalid-source-identity",
            overlong_invalid_source_identity.as_str(),
            overlong_invalid_source_identity_message.as_str(),
        ),
        (
            "assume-role-multibyte-source-identity-length-units",
            multibyte_source_identity.as_str(),
            multibyte_source_identity_message.as_str(),
        ),
        (
            "assume-role-supplementary-source-identity-length-units",
            supplementary_source_identity.as_str(),
            supplementary_source_identity_message.as_str(),
        ),
    ] {
        assert_assume_role_error(
            label,
            endpoint,
            primary_credentials,
            &[
                ("Action", "AssumeRole"),
                ("Version", "2011-06-15"),
                ("RoleArn", source_role_arn),
                ("RoleSessionName", role_session_name),
                ("SourceIdentity", value),
            ],
            400,
            "ValidationError",
            Some(message),
        );
    }

    for (label, value) in [
        ("assume-role-min-source-identity", "ab"),
        (
            "assume-role-all-allowed-source-identity-characters",
            "azAZ09_+=,.@-",
        ),
        (
            "assume-role-max-source-identity",
            max_source_identity.as_str(),
        ),
    ] {
        assert_assume_role_source_identity_success(
            label,
            endpoint,
            primary_credentials,
            None,
            &[
                ("Action", "AssumeRole"),
                ("Version", "2011-06-15"),
                ("RoleArn", source_role_arn),
                ("RoleSessionName", role_session_name),
                ("SourceIdentity", value),
            ],
            AssumeRoleSuccess {
                account_id,
                role_name: source_role_name,
                role_session_name,
                duration_seconds: 3600,
            },
            value,
        );
    }

    let condition_denied_message = format!(
        "User: {caller_arn} is not authorized to perform: sts:AssumeRole on resource: {condition_role_arn}"
    );
    assert_assume_role_source_identity_success(
        "assume-role-source-identity-condition-match",
        endpoint,
        primary_credentials,
        None,
        &[
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", condition_role_arn),
            ("RoleSessionName", role_session_name),
            ("SourceIdentity", source_identity),
        ],
        AssumeRoleSuccess {
            account_id,
            role_name: condition_role_name,
            role_session_name,
            duration_seconds: 3600,
        },
        source_identity,
    );
    assert_assume_role_error(
        "assume-role-source-identity-condition-missing",
        endpoint,
        primary_credentials,
        &[
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", condition_role_arn),
            ("RoleSessionName", role_session_name),
        ],
        403,
        "AccessDenied",
        Some(&condition_denied_message),
    );
    for (label, value) in [
        (
            "assume-role-source-identity-reserved-prefix-lower",
            "aws:reserved",
        ),
        (
            "assume-role-source-identity-reserved-prefix-upper",
            "AWS:reserved",
        ),
    ] {
        let message = format!(
            "1 validation error detected: Value '{value}' at 'sourceIdentity' failed to satisfy constraint: Member must satisfy regular expression pattern: {source_identity_pattern}"
        );
        assert_assume_role_error(
            label,
            endpoint,
            primary_credentials,
            &[
                ("Action", "AssumeRole"),
                ("Version", "2011-06-15"),
                ("RoleArn", source_role_arn),
                ("RoleSessionName", role_session_name),
                ("SourceIdentity", value),
            ],
            400,
            "ValidationError",
            Some(&message),
        );
    }
    let condition_set_denied_message = format!(
        "User: {caller_arn} is not authorized to perform: sts:SetSourceIdentity on resource: {condition_role_arn}"
    );
    assert_assume_role_error(
        "assume-role-source-identity-condition-mismatch",
        endpoint,
        primary_credentials,
        &[
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", condition_role_arn),
            ("RoleSessionName", role_session_name),
            ("SourceIdentity", "wrong-source-identity"),
        ],
        403,
        "AccessDenied",
        Some(&condition_set_denied_message),
    );
    assert_assume_role_source_identity_success(
        "assume-role-duplicate-source-identity-match-first",
        endpoint,
        primary_credentials,
        None,
        &[
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", condition_role_arn),
            ("RoleSessionName", role_session_name),
            ("SourceIdentity", source_identity),
            ("SourceIdentity", "wrong-source-identity"),
        ],
        AssumeRoleSuccess {
            account_id,
            role_name: condition_role_name,
            role_session_name,
            duration_seconds: 3600,
        },
        source_identity,
    );
    assert_assume_role_error(
        "assume-role-duplicate-source-identity-mismatch-first",
        endpoint,
        primary_credentials,
        &[
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", condition_role_arn),
            ("RoleSessionName", role_session_name),
            ("SourceIdentity", "wrong-source-identity"),
            ("SourceIdentity", source_identity),
        ],
        403,
        "AccessDenied",
        Some(&condition_set_denied_message),
    );
    assert_assume_role_source_identity_success(
        "assume-role-duplicate-source-identity-valid-before-invalid",
        endpoint,
        primary_credentials,
        None,
        &[
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", condition_role_arn),
            ("RoleSessionName", role_session_name),
            ("SourceIdentity", source_identity),
            ("SourceIdentity", "bad value"),
        ],
        AssumeRoleSuccess {
            account_id,
            role_name: condition_role_name,
            role_session_name,
            duration_seconds: 3600,
        },
        source_identity,
    );
    assert_assume_role_error(
        "assume-role-duplicate-source-identity-invalid-before-valid",
        endpoint,
        primary_credentials,
        &[
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", condition_role_arn),
            ("RoleSessionName", role_session_name),
            ("SourceIdentity", "bad value"),
            ("SourceIdentity", source_identity),
        ],
        400,
        "ValidationError",
        Some(
            r"1 validation error detected: Value 'bad value' at 'sourceIdentity' failed to satisfy constraint: Member must satisfy regular expression pattern: [\w+=,.@-]*",
        ),
    );

    let source_session_arn = format!(
        "arn:aws:sts::{account_id}:assumed-role/{source_session_role_name}/{source_session_name}"
    );
    assert_assume_role_source_identity_success(
        "assume-role-source-identity-chaining-inherits",
        endpoint,
        source_credentials,
        Some(source_session_token),
        &[
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", target_role_arn),
            ("RoleSessionName", target_session_name),
        ],
        AssumeRoleSuccess {
            account_id,
            role_name: target_role_name,
            role_session_name: target_session_name,
            duration_seconds: 3600,
        },
        source_identity,
    );
    assert_assume_role_source_identity_success(
        "assume-role-source-identity-chaining-explicit-same",
        endpoint,
        source_credentials,
        Some(source_session_token),
        &[
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", target_role_arn),
            ("RoleSessionName", target_session_name),
            ("SourceIdentity", source_identity),
        ],
        AssumeRoleSuccess {
            account_id,
            role_name: target_role_name,
            role_session_name: target_session_name,
            duration_seconds: 3600,
        },
        source_identity,
    );
    assert_assume_role_error_with_security_token(
        "assume-role-source-identity-chaining-cannot-change",
        endpoint,
        source_credentials,
        source_session_token,
        &[
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", target_role_arn),
            ("RoleSessionName", target_session_name),
            ("SourceIdentity", "different-source-identity"),
        ],
        AssumeRoleErrorExpected {
            status: 400,
            code: "ValidationError",
            message: Some("The source identity is already set for this assume role session"),
        },
    );
    let no_set_denied_message = format!(
        "User: {source_session_arn} is not authorized to perform: sts:SetSourceIdentity on resource: {no_set_target_role_arn}"
    );
    assert_assume_role_error_with_security_token(
        "assume-role-source-identity-chaining-requires-set-permission",
        endpoint,
        source_credentials,
        source_session_token,
        &[
            ("Action", "AssumeRole"),
            ("Version", "2011-06-15"),
            ("RoleArn", no_set_target_role_arn),
            ("RoleSessionName", target_session_name),
        ],
        AssumeRoleErrorExpected {
            status: 403,
            code: "AccessDenied",
            message: Some(&no_set_denied_message),
        },
    );
}

fn assert_cross_account_denied(
    label: &str,
    endpoint: &str,
    credentials: SignedRequestCredentials<'_>,
    target_account_id: &str,
    caller_arn: &str,
    role_arn: &str,
    role_session_name: &str,
) {
    let body = form_body(&[
        ("Action", "AssumeRole"),
        ("Version", "2011-06-15"),
        ("RoleArn", role_arn),
        ("RoleSessionName", role_session_name),
    ]);
    let message = format!(
        "User: {caller_arn} is not authorized to perform: sts:AssumeRole on resource: {role_arn}"
    );
    let probe = Probe {
        label,
        request: QueryRequest::Post {
            body: &body,
            content_type: Some(QUERY_CONTENT_TYPE),
        },
        expected: Expected::StsError {
            status: 403,
            code: "AccessDenied",
            message: &message,
        },
    };
    let response = probe.request.send(endpoint, credentials);
    assert_probe(&probe, &response, target_account_id);
    println!("{}: ok", probe.label);
}

struct RoleChainingProbeSet<'a> {
    security_token: &'a str,
    target_role_arn: &'a str,
    target_role_name: &'a str,
    target_session_name: &'a str,
    low_max_target_role_arn: &'a str,
}

fn run_role_chaining_probes(
    endpoint: &str,
    credentials: SignedRequestCredentials<'_>,
    account_id: &str,
    fixture: RoleChainingProbeSet<'_>,
) {
    let success_parameters = [
        ("Action", "AssumeRole"),
        ("Version", "2011-06-15"),
        ("RoleArn", fixture.target_role_arn),
        ("RoleSessionName", fixture.target_session_name),
        ("DurationSeconds", "3600"),
    ];
    let response = send_assume_role_with_security_token(
        endpoint,
        credentials,
        fixture.security_token,
        &success_parameters,
    );
    assert_assume_role_success(
        "assume-role-chaining-at-maximum-duration",
        &response,
        account_id,
        fixture.target_role_name,
        fixture.target_session_name,
        3600,
        None,
    );
    println!("assume-role-chaining-at-maximum-duration: ok");

    let overlong_parameters = [
        ("Action", "AssumeRole"),
        ("Version", "2011-06-15"),
        ("RoleArn", fixture.target_role_arn),
        ("RoleSessionName", fixture.target_session_name),
        ("DurationSeconds", "3601"),
    ];
    let response = send_assume_role_with_security_token(
        endpoint,
        credentials,
        fixture.security_token,
        &overlong_parameters,
    );
    assert_error_probe(
        "assume-role-chaining-over-maximum-duration",
        &response,
        400,
        STS_XMLNS,
        "ValidationError",
        Some(
            "The requested DurationSeconds exceeds the 1 hour session limit for roles assumed by role chaining.",
        ),
    );
    println!("assume-role-chaining-over-maximum-duration: ok");

    let over_both_contextual_limits_parameters = [
        ("Action", "AssumeRole"),
        ("Version", "2011-06-15"),
        ("RoleArn", fixture.low_max_target_role_arn),
        ("RoleSessionName", fixture.target_session_name),
        ("DurationSeconds", "3601"),
    ];
    let response = send_assume_role_with_security_token(
        endpoint,
        credentials,
        fixture.security_token,
        &over_both_contextual_limits_parameters,
    );
    assert_error_probe(
        "assume-role-chaining-over-chaining-and-role-maximum-duration",
        &response,
        400,
        STS_XMLNS,
        "ValidationError",
        Some("The requested DurationSeconds exceeds the MaxSessionDuration set for this role."),
    );
    println!("assume-role-chaining-over-chaining-and-role-maximum-duration: ok");

    let over_api_maximum_parameters = [
        ("Action", "AssumeRole"),
        ("Version", "2011-06-15"),
        ("RoleArn", fixture.target_role_arn),
        ("RoleSessionName", fixture.target_session_name),
        ("DurationSeconds", "43201"),
    ];
    let response = send_assume_role_with_security_token(
        endpoint,
        credentials,
        fixture.security_token,
        &over_api_maximum_parameters,
    );
    assert_error_probe(
        "assume-role-chaining-over-api-maximum-duration",
        &response,
        400,
        STS_XMLNS,
        "ValidationError",
        Some("1 validation error detected: Value '43201' at 'durationSeconds' failed to satisfy constraint: Member must have value less than or equal to 43200"),
    );
    println!("assume-role-chaining-over-api-maximum-duration: ok");
}

struct SessionAuthenticationProbeSet<'a> {
    active_credentials: SignedRequestCredentials<'a>,
    active_security_token: &'a str,
    other_live_security_token: &'a str,
    active_role_name: &'a str,
    active_role_session_name: &'a str,
    recreated_credentials: SignedRequestCredentials<'a>,
    recreated_security_token: &'a str,
    recreated_role_name: &'a str,
    recreated_role_session_name: &'a str,
    recreated_role_id: &'a str,
    old_credentials: SignedRequestCredentials<'a>,
    old_security_token: &'a str,
}

fn run_session_authentication_probes(
    endpoint: &str,
    account_id: &str,
    fixture: SessionAuthenticationProbeSet<'_>,
) {
    let invalid_token_message = "The security token included in the request is invalid.";
    let signature_mismatch_message = "The request signature we calculated does not match the signature you provided. Check your AWS Secret Access Key and signing method. Consult the service documentation for details.";
    let active_response = send_get_caller_identity(
        endpoint,
        fixture.active_credentials,
        Some(fixture.active_security_token),
    );
    assert_assumed_role_caller_identity(
        "session-auth-live-role-valid",
        &active_response,
        account_id,
        fixture.active_role_name,
        fixture.active_role_session_name,
        None,
    );
    let recreated_response = send_get_caller_identity(
        endpoint,
        fixture.recreated_credentials,
        Some(fixture.recreated_security_token),
    );
    assert_assumed_role_caller_identity(
        "session-auth-recreated-role-valid",
        &recreated_response,
        account_id,
        fixture.recreated_role_name,
        fixture.recreated_role_session_name,
        Some(fixture.recreated_role_id),
    );
    let old_response = send_get_caller_identity(
        endpoint,
        fixture.old_credentials,
        Some(fixture.old_security_token),
    );
    assert_error_probe(
        "session-auth-old-session-after-role-recreation-valid",
        &old_response,
        403,
        STS_XMLNS,
        "InvalidClientTokenId",
        Some(invalid_token_message),
    );
    println!("session-auth-old-session-after-role-recreation-valid: ok");

    let wrong_secret = "0".repeat(40);
    let wrong_region = if fixture.active_credentials.region == "us-east-1" {
        "us-west-2"
    } else {
        "us-east-1"
    };
    for (role_state, credentials, security_token, other_security_token) in [
        (
            "live-role",
            fixture.active_credentials,
            fixture.active_security_token,
            fixture.other_live_security_token,
        ),
        (
            "old-session",
            fixture.old_credentials,
            fixture.old_security_token,
            fixture.active_security_token,
        ),
    ] {
        let wrong_region_credentials = SignedRequestCredentials {
            region: wrong_region,
            ..credentials
        };
        let wrong_region_bad_signature_credentials = SignedRequestCredentials {
            secret_key: &wrong_secret,
            ..wrong_region_credentials
        };
        let wrong_service_bad_signature_credentials = SignedRequestCredentials {
            secret_key: &wrong_secret,
            ..credentials
        };
        for (case, signing_credentials, supplied_token, service) in [
            (
                "valid-token-wrong-region",
                wrong_region_credentials,
                Some(security_token),
                "sts",
            ),
            (
                "missing-token-wrong-region",
                wrong_region_credentials,
                None,
                "sts",
            ),
            (
                "mismatched-token-wrong-region",
                wrong_region_credentials,
                Some(other_security_token),
                "sts",
            ),
            (
                "valid-token-wrong-region-bad-signature",
                wrong_region_bad_signature_credentials,
                Some(security_token),
                "sts",
            ),
            (
                "valid-token-wrong-service",
                credentials,
                Some(security_token),
                "s3",
            ),
            ("missing-token-wrong-service", credentials, None, "s3"),
            (
                "mismatched-token-wrong-service",
                credentials,
                Some(other_security_token),
                "s3",
            ),
            (
                "valid-token-wrong-service-bad-signature",
                wrong_service_bad_signature_credentials,
                Some(security_token),
                "s3",
            ),
        ] {
            let label = format!("scope-{role_state}-{case}");
            let response = send_get_caller_identity_with_scope(
                endpoint,
                signing_credentials,
                supplied_token,
                service,
            );
            let message = if service == "sts" {
                STS_WRONG_REGION_SCOPE_MESSAGE
            } else {
                STS_WRONG_SERVICE_SCOPE_MESSAGE
            };
            assert_signing_scope_error(&label, &response, message);
        }
    }

    for (
        role_state,
        credentials,
        security_token,
        other_security_token,
        valid_token_bad_signature_error,
    ) in [
        (
            "live-role",
            fixture.active_credentials,
            fixture.active_security_token,
            fixture.other_live_security_token,
            ("SignatureDoesNotMatch", signature_mismatch_message),
        ),
        (
            "old-session-after-role-recreation",
            fixture.old_credentials,
            fixture.old_security_token,
            fixture.active_security_token,
            ("InvalidClientTokenId", invalid_token_message),
        ),
    ] {
        let wrong_secret_credentials = SignedRequestCredentials {
            access_key: credentials.access_key,
            secret_key: &wrong_secret,
            region: credentials.region,
            tls_ca_pem: credentials.tls_ca_pem,
        };
        for (case, signing_credentials, supplied_token, expected_error) in [
            (
                "missing-token-valid-signature",
                credentials,
                None,
                ("InvalidClientTokenId", invalid_token_message),
            ),
            (
                "mismatched-token-valid-signature",
                credentials,
                Some(other_security_token),
                ("InvalidClientTokenId", invalid_token_message),
            ),
            (
                "valid-token-bad-signature",
                wrong_secret_credentials,
                Some(security_token),
                valid_token_bad_signature_error,
            ),
            (
                "missing-token-bad-signature",
                wrong_secret_credentials,
                None,
                ("InvalidClientTokenId", invalid_token_message),
            ),
            (
                "mismatched-token-bad-signature",
                wrong_secret_credentials,
                Some(other_security_token),
                ("InvalidClientTokenId", invalid_token_message),
            ),
        ] {
            let label = format!("session-auth-{role_state}-{case}");
            let response = send_get_caller_identity(endpoint, signing_credentials, supplied_token);
            let (code, message) = expected_error;
            assert_error_probe(&label, &response, 403, STS_XMLNS, code, Some(message));
            println!("{label}: ok");
        }
    }
}

fn wait_for_s3_invalid_access_key_convergence<F>(label: &str, access_key: &str, mut send: F)
where
    F: FnMut() -> (RawResponse, RawResponse),
{
    let mut consecutive_invalid = 0;
    let mut last_status = 0;
    let mut last_code = None;
    for attempt in 1..=30 {
        let (response, sanitized_response) = send();
        last_status = response.status;
        last_code = xml_tag_text(&response.body, "Code").map(str::to_string);
        if response.status == 403 && last_code.as_deref() == Some("InvalidAccessKeyId") {
            consecutive_invalid += 1;
            if consecutive_invalid == 3 {
                assert!(
                    required_xml_text(&response, "AWSAccessKeyId", label) == access_key,
                    "{label}: S3 did not echo the liveness-control access key"
                );
                assert_s3_invalid_access_key_shape(label, &sanitized_response);
                return;
            }
        } else {
            consecutive_invalid = 0;
        }
        if attempt < 30 {
            std::thread::sleep(Duration::from_secs(2));
        }
    }
    panic!(
        "{label}: S3 did not converge to three consecutive InvalidAccessKeyId responses; \
         last status was {last_status}, last code was {}",
        last_code.as_deref().unwrap_or("missing")
    );
}

fn run_s3_deleted_issuer_convergence_probes(
    s3_endpoint: &str,
    bucket: &str,
    credentials: SignedRequestCredentials<'_>,
    security_token: &str,
) {
    wait_for_s3_invalid_access_key_convergence(
        "s3-header-expiry-liveness-control-after-delete",
        credentials.access_key,
        || {
            let response = send_signed_request_for_service_with_credentials(
                "GET",
                s3_endpoint,
                b"",
                [("x-amz-security-token", security_token)],
                "s3",
                credentials,
            );
            let sanitized = s3_response_with_sanitized_body(
                &response,
                credentials.access_key,
                &[security_token],
            );
            (response, sanitized)
        },
    );

    wait_for_s3_invalid_access_key_convergence(
        "s3-presigned-expiry-liveness-control-after-delete",
        credentials.access_key,
        || {
            let presigned = build_s3_root_presigned_request(
                s3_endpoint,
                credentials,
                Some(security_token),
                None,
            );
            let response = fetch_s3_presigned_request(s3_endpoint, &presigned, None);
            let sanitized = s3_response_with_sanitized_body(
                &response,
                credentials.access_key,
                &[security_token],
            );
            (response, sanitized)
        },
    );

    wait_for_s3_invalid_access_key_convergence(
        "s3-post-expiry-liveness-control-after-delete",
        credentials.access_key,
        || {
            let probe = S3PostProbe {
                label: "s3-post-expiry-liveness-control-after-delete",
                credentials,
                policy_token: Some(security_token),
                form_tokens: S3PostFormTokens::One(security_token),
                header_token: None,
                expected: S3PostAuthExpected::InvalidAccessKey,
            };
            let result = send_s3_post_probe(s3_endpoint, bucket, probe);
            let sanitized = s3_post_response_with_sanitized_body(
                &result.response,
                credentials.access_key,
                &[security_token],
                &result.policy,
            );
            (result.response, sanitized)
        },
    );

    wait_for_s3_invalid_access_key_convergence(
        "streaming-expiry-liveness-control-after-delete",
        credentials.access_key,
        || {
            let result = send_s3_streaming_request(
                s3_endpoint,
                bucket,
                S3StreamingRequest {
                    label: "streaming-expiry-liveness-control-after-delete",
                    credentials,
                    tokens: S3StreamingTokens::One(security_token),
                    sign_token_header: true,
                    bad_chunk_signature: false,
                    service: "s3",
                },
            );
            let sanitized = s3_response_with_sanitized_body(
                &result.response,
                credentials.access_key,
                &[security_token],
            );
            (result.response, sanitized)
        },
    );
}

fn run_expired_deleted_session_probes(
    sts_endpoint: &str,
    s3_endpoint: &str,
    bucket: &str,
    credentials: SignedRequestCredentials<'_>,
    security_token: &str,
) {
    let wrong_secret = "0".repeat(40);
    let bad_signature_credentials = SignedRequestCredentials {
        secret_key: &wrong_secret,
        ..credentials
    };
    let sts_expired_message = "The security token included in the request is expired";

    for (label, signing_credentials) in [
        ("session-auth-expired-deleted-valid", credentials),
        (
            "session-auth-expired-deleted-bad-signature",
            bad_signature_credentials,
        ),
    ] {
        let response =
            send_get_caller_identity(sts_endpoint, signing_credentials, Some(security_token));
        assert_error_probe(
            label,
            &response,
            403,
            STS_XMLNS,
            "ExpiredToken",
            Some(sts_expired_message),
        );
        println!("{label}: ok");
    }

    for (label, signing_credentials) in [
        ("s3-header-auth-expired-deleted-valid", credentials),
        (
            "s3-header-auth-expired-deleted-bad-signature",
            bad_signature_credentials,
        ),
    ] {
        let response = send_signed_request_for_service_with_credentials(
            "GET",
            s3_endpoint,
            b"",
            [("x-amz-security-token", security_token)],
            "s3",
            signing_credentials,
        );
        assert_s3_expired_token(label, &response, signing_credentials, security_token);
    }

    for (label, signing_credentials) in [
        ("s3-presigned-auth-expired-deleted-valid", credentials),
        (
            "s3-presigned-auth-expired-deleted-bad-signature",
            bad_signature_credentials,
        ),
    ] {
        let presigned = build_s3_root_presigned_request(
            s3_endpoint,
            signing_credentials,
            Some(security_token),
            None,
        );
        let response = fetch_s3_presigned_request(s3_endpoint, &presigned, None);
        assert_s3_expired_token(label, &response, signing_credentials, security_token);
    }

    for (label, signing_credentials) in [
        ("s3-post-auth-expired-deleted-valid", credentials),
        (
            "s3-post-auth-expired-deleted-bad-signature",
            bad_signature_credentials,
        ),
    ] {
        let probe = S3PostProbe {
            label,
            credentials: signing_credentials,
            policy_token: Some(security_token),
            form_tokens: S3PostFormTokens::One(security_token),
            header_token: None,
            expected: S3PostAuthExpected::ExpiredToken,
        };
        let result = send_s3_post_probe(s3_endpoint, bucket, probe);
        assert_s3_post_expired_token(probe, &result, security_token);
    }

    for (label, signing_credentials, bad_chunk_signature) in [
        ("streaming-expired-deleted-valid", credentials, false),
        (
            "streaming-expired-deleted-bad-seed-signature",
            bad_signature_credentials,
            false,
        ),
        (
            "streaming-expired-deleted-bad-chunk-signature",
            credentials,
            true,
        ),
    ] {
        let result = send_s3_streaming_request(
            s3_endpoint,
            bucket,
            S3StreamingRequest {
                label,
                credentials: signing_credentials,
                tokens: S3StreamingTokens::One(security_token),
                sign_token_header: true,
                bad_chunk_signature,
                service: "s3",
            },
        );
        assert_s3_expired_token(label, &result.response, signing_credentials, security_token);
    }
}

struct CrossAccountProbeSet<'a> {
    caller_arn: &'a str,
    role_session_name: &'a str,
    success_role_name: &'a str,
    success_role_arn: &'a str,
    trust_denied_role_arn: &'a str,
    caller_denied_role_arn: &'a str,
}

fn run_cross_account_probes(
    endpoint: &str,
    credentials: SignedRequestCredentials<'_>,
    target_account_id: &str,
    fixture: CrossAccountProbeSet<'_>,
) {
    assert_cross_account_denied(
        "assume-role-cross-account-trust-denied",
        endpoint,
        credentials,
        target_account_id,
        fixture.caller_arn,
        fixture.trust_denied_role_arn,
        fixture.role_session_name,
    );
    assert_cross_account_denied(
        "assume-role-cross-account-caller-policy-denied",
        endpoint,
        credentials,
        target_account_id,
        fixture.caller_arn,
        fixture.caller_denied_role_arn,
        fixture.role_session_name,
    );

    let body = form_body(&[
        ("Action", "AssumeRole"),
        ("Version", "2011-06-15"),
        ("RoleArn", fixture.success_role_arn),
        ("RoleSessionName", fixture.role_session_name),
    ]);
    let response = QueryRequest::Post {
        body: &body,
        content_type: Some(QUERY_CONTENT_TYPE),
    }
    .send(endpoint, credentials);
    assert_assume_role_success(
        "assume-role-cross-account-success",
        &response,
        target_account_id,
        fixture.success_role_name,
        fixture.role_session_name,
        3600,
        None,
    );
    println!("assume-role-cross-account-success: ok");
}

#[derive(Clone, Copy)]
struct S3ControlError<'a> {
    status: u16,
    code: &'a str,
    message: &'a str,
    detail: &'a str,
    allow: Option<&'a str>,
}

fn assert_s3_control_error(label: &str, response: &RawResponse, expected: S3ControlError<'_>) {
    let mut spec = shape()
        .status(expected.status)
        .headers(error_response_headers());
    if let Some(allow) = expected.allow {
        spec = spec.header("allow", allow);
    }
    assert_shape(
        label,
        response,
        &spec
            .sub("code", expected.code)
            .sub("message", expected.message)
            .sub("detail", expected.detail)
            .body(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                 <ErrorResponse><Error><Code>{code}</Code><Message>{message}</Message>{detail}</Error>\
                 <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></ErrorResponse>",
            ),
    );
}

fn assert_sts_unknown_operation(label: &str, response: &RawResponse, head: bool) {
    let spec = sts_wire_shape(label, response).status(404);
    let spec = if head {
        spec.header("content-length", "29").body_empty()
    } else {
        spec.body("<UnknownOperationException/>\n")
    };
    assert_shape(label, response, &spec);
}

fn assert_frontend_empty_bad_request(label: &str, response: &RawResponse) {
    let request_id = response_header_value(response, "x-amz-request-id")
        .unwrap_or_else(|| panic!("{label}: missing outer-frontend request ID"));
    assert_shape_with_request_id_validator(
        label,
        response,
        &shape()
            .status(400)
            .header("x-amz-request-id", "{frontend_request_id}")
            .sub("frontend_request_id", request_id)
            .body_empty(),
        is_outer_frontend_request_id,
    );
}

fn assert_s3_frontend_bad_request(label: &str, response: &RawResponse) {
    let request_id = response_header_value(response, "x-amz-request-id")
        .unwrap_or_else(|| panic!("{label}: missing outer-frontend request ID"));
    assert_shape_with_request_id_validator(
        label,
        response,
        &shape()
            .status(400)
            .header("x-amz-request-id", "{frontend_request_id}")
            .header("x-amz-id-2", "{host_id}")
            .header("content-type", "application/xml")
            .sub("frontend_request_id", request_id)
            .body(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                 <Error><Code>BadRequest</Code>\
                 <Message>An error occurred when parsing the HTTP request.</Message>\
                 <RequestId>{frontend_request_id}</RequestId><HostId>{host_id}</HostId></Error>",
            ),
        is_outer_frontend_request_id,
    );
}

fn is_outer_frontend_request_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 16
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'A'..=b'F').contains(&byte))
}

fn assert_s3_control_head_method_not_allowed(label: &str, response: &RawResponse) {
    assert_shape(
        label,
        response,
        &shape()
            .status(405)
            .headers(error_response_headers())
            .header("allow", "DELETE, POST, GET")
            .body_empty(),
    );
}

fn assert_sts_cors_preflight_success(label: &str, response: &RawResponse) {
    assert_shape(
        label,
        response,
        &sts_wire_shape(label, response)
            .status(200)
            .header("access-control-allow-origin", "*")
            .header("access-control-allow-methods", "POST")
            .header(
                "access-control-expose-headers",
                "x-amzn-RequestId,x-amzn-ErrorType,x-amzn-ErrorMessage,Date,smithy-protocol",
            )
            .header("access-control-max-age", "172800")
            .header("content-length", "0")
            .body_empty(),
    );
}

struct S3ControlCanonicalRequest<'a> {
    method: &'a str,
    endpoint: &'a str,
    path: &'a str,
    canonical_query: &'a str,
    body: &'a [u8],
    headers: &'a [(&'a str, &'a str)],
}

#[derive(Clone, Copy)]
enum StsMethodResult {
    UnknownOperation,
    UnknownOperationHead,
    FrontendBadRequest,
}

#[derive(Clone, Copy)]
enum S3ControlMethodResult {
    NoSuchResource,
    HeadMethodNotAllowed,
    MethodNotAllowed,
    OptionsBadRequest,
    FrontendBadRequest,
}

#[derive(Clone, Copy)]
struct RoutingMethodProbe<'a> {
    method: &'a str,
    sts: StsMethodResult,
    s3_control: S3ControlMethodResult,
}

enum StsPathResult {
    UnknownOperation,
    EmptyBadRequest,
}

enum S3ControlPathResult {
    NoSuchResource,
    InvalidUri(String),
    EmptyBadRequest,
}

struct RoutingPathProbe {
    label: &'static str,
    wire_path: String,
    signed_path: Option<String>,
    sts: StsPathResult,
    s3_control: S3ControlPathResult,
}

struct AccountIdHeaderProbe<'a> {
    label: &'static str,
    headers: Vec<(&'static str, &'a str)>,
}

#[derive(Clone, Copy)]
enum AccountIdOperation {
    List,
    Tag,
    Untag,
}

enum StsAuthCollisionResult {
    SignatureMismatch,
    UnknownOperation,
    EmptyBadRequest,
}

enum S3ControlAuthCollisionResult {
    SignatureMismatch,
    InvalidUri(String),
    EmptyBadRequest,
}

struct RoutingAuthCollisionProbe<'a> {
    label: &'static str,
    method: &'static str,
    request_path: String,
    canonical_path: &'a str,
    canonical_query: &'a str,
    body: &'a [u8],
    headers: Vec<(&'a str, &'a str)>,
    sts: StsAuthCollisionResult,
    s3_control: S3ControlAuthCollisionResult,
}

#[derive(Clone, Copy)]
enum S3ControlBodyResult<'a> {
    Error(S3ControlError<'a>),
    WriteSuccess,
}

struct RoutingBodyProbe<'a> {
    label: &'static str,
    method: &'static str,
    query: String,
    body: &'a [u8],
    headers: Vec<(&'a str, &'a str)>,
    s3_control: S3ControlBodyResult<'a>,
}

fn account_id_header_probes(account_id: &str) -> Vec<AccountIdHeaderProbe<'_>> {
    let wrong_account_id = if account_id == "000000000000" {
        "111111111111"
    } else {
        "000000000000"
    };
    vec![
        AccountIdHeaderProbe {
            label: "correct",
            headers: vec![("x-amz-account-id", account_id)],
        },
        AccountIdHeaderProbe {
            label: "missing",
            headers: vec![],
        },
        AccountIdHeaderProbe {
            label: "empty",
            headers: vec![("x-amz-account-id", "")],
        },
        AccountIdHeaderProbe {
            label: "wrong",
            headers: vec![("x-amz-account-id", wrong_account_id)],
        },
        AccountIdHeaderProbe {
            label: "malformed-short",
            headers: vec![("x-amz-account-id", "1")],
        },
        AccountIdHeaderProbe {
            label: "malformed-alpha",
            headers: vec![("x-amz-account-id", "not-an-account")],
        },
        AccountIdHeaderProbe {
            label: "duplicate-identical",
            headers: vec![
                ("x-amz-account-id", account_id),
                ("x-amz-account-id", account_id),
            ],
        },
        AccountIdHeaderProbe {
            label: "duplicate-correct-wrong",
            headers: vec![
                ("x-amz-account-id", account_id),
                ("x-amz-account-id", wrong_account_id),
            ],
        },
        AccountIdHeaderProbe {
            label: "duplicate-wrong-correct",
            headers: vec![
                ("x-amz-account-id", wrong_account_id),
                ("x-amz-account-id", account_id),
            ],
        },
    ]
}

fn assert_empty_bad_path_request(label: &str, response: &RawResponse) {
    assert_shape(label, response, &shape().status(400).body_empty());
}

fn assert_list_tags_for_resource_success(label: &str, response: &RawResponse) {
    assert_shape(
        label,
        response,
        &shape()
            .status(200)
            .headers(id_headers())
            .body(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                 <ListTagsForResourceResult xmlns=\"http://awss3control.amazonaws.com/doc/2018-08-20/\">\
                 <Tags><Tag><Key>routing-key</Key><Value>routing-value</Value></Tag></Tags>\
                 </ListTagsForResourceResult>",
            ),
    );
}

fn assert_s3_control_write_success(label: &str, response: &RawResponse) {
    assert_shape(
        label,
        response,
        &shape().status(204).headers(id_headers()).body_empty(),
    );
}

fn assert_s3_control_wrong_service(label: &str, response: &RawResponse, provided_service: &str) {
    let message = format!(
        "The authorization header is malformed; incorrect service \"{provided_service}\". \
         This endpoint belongs to \"s3\"."
    );
    assert_s3_control_error(
        label,
        response,
        S3ControlError {
            status: 400,
            code: "AuthorizationHeaderMalformed",
            message: &message,
            detail: "",
            allow: None,
        },
    );
}

fn assert_s3_control_signature_mismatch(
    label: &str,
    response: &RawResponse,
    request: S3ControlCanonicalRequest<'_>,
    credentials: SignedRequestCredentials<'_>,
) {
    assert!(
        required_xml_text(response, "AWSAccessKeyId", label) == credentials.access_key,
        "{label}: S3 Control did not echo the signing access key"
    );
    let string_to_sign = required_xml_text(response, "StringToSign", label);
    let string_to_sign_bytes = required_xml_text(response, "StringToSignBytes", label);
    assert!(
        string_to_sign_bytes == spaced_hex(&string_to_sign),
        "{label}: StringToSignBytes does not encode StringToSign"
    );
    let mut string_to_sign_lines = string_to_sign.lines();
    assert!(
        string_to_sign_lines.next() == Some("AWS4-HMAC-SHA256"),
        "{label}: unexpected signing algorithm"
    );
    let amz_date = string_to_sign_lines
        .next()
        .unwrap_or_else(|| panic!("{label}: missing signing timestamp"));
    let amz_date_bytes = amz_date.as_bytes();
    assert!(
        amz_date.len() == 16
            && amz_date_bytes[8] == b'T'
            && amz_date_bytes[15] == b'Z'
            && amz_date_bytes[..8].iter().all(|byte| byte.is_ascii_digit())
            && amz_date_bytes[9..15]
                .iter()
                .all(|byte| byte.is_ascii_digit()),
        "{label}: malformed signing timestamp"
    );
    let scope = format!("{}/{}/s3/aws4_request", &amz_date[..8], credentials.region);
    assert!(
        string_to_sign_lines.next() == Some(scope.as_str()),
        "{label}: unexpected S3 Control credential scope"
    );
    let canonical_request_hash = string_to_sign_lines
        .next()
        .unwrap_or_else(|| panic!("{label}: missing canonical request hash"));
    assert!(
        string_to_sign_lines.next().is_none(),
        "{label}: unexpected extra StringToSign line"
    );

    let parsed_endpoint = url::Url::parse(request.endpoint)
        .unwrap_or_else(|error| panic!("{label}: invalid S3 Control endpoint: {error}"));
    let host = parsed_endpoint
        .host_str()
        .map(|host| {
            if let Some(port) = parsed_endpoint.port() {
                format!("{host}:{port}")
            } else {
                host.to_string()
            }
        })
        .unwrap_or_else(|| panic!("{label}: S3 Control endpoint has no host"));
    let payload_hash = auth::canonical::sha256_hex(request.body);
    let S3ControlCanonicalRequest {
        method,
        path,
        canonical_query,
        headers,
        ..
    } = request;
    let mut canonical_header_pairs = vec![
        ("host", host.as_str()),
        ("x-amz-content-sha256", payload_hash.as_str()),
        ("x-amz-date", amz_date),
    ];
    canonical_header_pairs.extend(headers.iter().copied());
    canonical_header_pairs.sort_by(|left, right| left.0.cmp(right.0));
    let canonical_headers = auth::canonical::canonical_headers(&canonical_header_pairs);
    let mut signed_header_names = Vec::new();
    for (name, _) in &canonical_header_pairs {
        if signed_header_names.last().copied() != Some(*name) {
            signed_header_names.push(*name);
        }
    }
    let signed_headers = signed_header_names.join(";");
    let canonical_request = format!(
        "{method}\n{path}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    );
    let observed_canonical_request = required_xml_text(response, "CanonicalRequest", label);
    let canonical_request_xml = canonical_request.replace('&', "&amp;");
    assert!(
        observed_canonical_request == canonical_request_xml,
        "{label}: unexpected canonical request\nexpected: {canonical_request_xml:?}\nobserved: {observed_canonical_request:?}"
    );
    let canonical_request_bytes = spaced_hex(&canonical_request);
    assert!(
        required_xml_text(response, "CanonicalRequestBytes", label) == canonical_request_bytes,
        "{label}: CanonicalRequestBytes does not encode CanonicalRequest"
    );
    assert!(
        canonical_request_hash == auth::canonical::sha256_hex(canonical_request.as_bytes()),
        "{label}: StringToSign has the wrong canonical request hash"
    );
    let signature = required_xml_text(response, "SignatureProvided", label);
    assert!(
        signature.len() == 64
            && signature
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "{label}: malformed provided signature"
    );

    let response = s3_response_with_sanitized_body(response, credentials.access_key, &[]);
    assert_shape(
        label,
        &response,
        &shape()
            .status(403)
            .headers(error_response_headers())
            .sub("string_to_sign", string_to_sign)
            .sub("signature", signature)
            .sub("string_to_sign_bytes", string_to_sign_bytes)
            .sub("canonical_request", canonical_request_xml)
            .sub("canonical_request_bytes", canonical_request_bytes)
            .body(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                 <ErrorResponse><Error><Code>SignatureDoesNotMatch</Code>\
                 <Message>The request signature we calculated does not match the signature you provided. Check your key and signing method.</Message>\
                 <AWSAccessKeyId>SESSION_ACCESS_KEY</AWSAccessKeyId>\
                 <StringToSign>{string_to_sign}</StringToSign>\
                 <SignatureProvided>{signature}</SignatureProvided>\
                 <StringToSignBytes>{string_to_sign_bytes}</StringToSignBytes>\
                 <CanonicalRequest>{canonical_request}</CanonicalRequest>\
                 <CanonicalRequestBytes>{canonical_request_bytes}</CanonicalRequestBytes>\
                 </Error><RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></ErrorResponse>",
            ),
    );
}

fn run_cross_service_routing_probes(
    sts_endpoint: &str,
    s3_control_endpoint: &str,
    credentials: SignedRequestCredentials<'_>,
    account_id: &str,
) {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("current time must be after the Unix epoch")
        .as_nanos();
    let resource = format!("arn%3Aaws%3As3%3A%3A%3Aclaude-s3-sts-routing-{unique:x}");
    let tags_path = format!("/v20180820/tags/{resource}");
    let tags_path_with_query = format!("{tags_path}?Action=GetCallerIdentity&Version=2011-06-15");
    let query_body = b"Action=GetCallerIdentity&Version=2011-06-15";
    let tag_body = b"<?xml version=\"1.0\" encoding=\"UTF-8\"?><TagResourceRequest xmlns=\"http://awss3control.amazonaws.com/doc/2018-08-20/\"><Tags><Tag><Key>routing</Key><Value>probe</Value></Tag></Tags></TagResourceRequest>";

    let sts_query_root = send_signed_request_for_service_with_credentials(
        "POST",
        &format!("{sts_endpoint}/"),
        query_body,
        [
            ("content-type", QUERY_CONTENT_TYPE),
            ("x-amz-account-id", account_id),
        ],
        "sts",
        credentials,
    );
    assert_get_caller_identity_success("routing-sts-query-root", &sts_query_root, account_id);
    println!("routing-sts-query-root: ok");

    let s3_control_query_root = send_signed_request_for_service_with_credentials(
        "POST",
        &format!("{s3_control_endpoint}/"),
        query_body,
        [
            ("content-type", QUERY_CONTENT_TYPE),
            ("x-amz-account-id", account_id),
        ],
        "s3",
        credentials,
    );
    assert_s3_control_error(
        "routing-s3-control-query-root",
        &s3_control_query_root,
        S3ControlError {
            status: 400,
            code: "InvalidURI",
            message: "Couldn't parse the specified URI.",
            detail: "<URI>/</URI>",
            allow: None,
        },
    );
    println!("routing-s3-control-query-root: ok");

    let sts_query_tags_path = send_signed_request_for_service_with_credentials(
        "POST",
        &format!("{sts_endpoint}{tags_path}"),
        query_body,
        [
            ("content-type", QUERY_CONTENT_TYPE),
            ("x-amz-account-id", account_id),
        ],
        "sts",
        credentials,
    );
    assert_error_probe(
        "routing-sts-query-tags-path",
        &sts_query_tags_path,
        403,
        STS_XMLNS,
        "SignatureDoesNotMatch",
        Some(STS_SIGNATURE_MISMATCH_MESSAGE),
    );
    println!("routing-sts-query-tags-path: ok");

    let s3_control_query_tags_path = send_signed_request_for_service_with_credentials(
        "POST",
        &format!("{s3_control_endpoint}{tags_path}"),
        query_body,
        [
            ("content-type", QUERY_CONTENT_TYPE),
            ("x-amz-account-id", account_id),
        ],
        "s3",
        credentials,
    );
    assert_s3_control_signature_mismatch(
        "routing-s3-control-query-tags-path",
        &s3_control_query_tags_path,
        S3ControlCanonicalRequest {
            method: "POST",
            endpoint: s3_control_endpoint,
            path: &tags_path,
            canonical_query: std::str::from_utf8(query_body)
                .expect("routing Query body must be UTF-8"),
            body: query_body,
            headers: &[
                ("content-type", QUERY_CONTENT_TYPE),
                ("x-amz-account-id", account_id),
            ],
        },
        credentials,
    );
    println!("routing-s3-control-query-tags-path: ok");

    let sts_tags_path_query_action = send_signed_request_for_service_with_credentials(
        "POST",
        &format!("{sts_endpoint}{tags_path_with_query}"),
        tag_body,
        [
            ("content-type", "application/xml"),
            ("x-amz-account-id", account_id),
        ],
        "sts",
        credentials,
    );
    assert_error_probe(
        "routing-sts-tags-path-query-action",
        &sts_tags_path_query_action,
        403,
        STS_XMLNS,
        "SignatureDoesNotMatch",
        Some(STS_SIGNATURE_MISMATCH_MESSAGE),
    );
    println!("routing-sts-tags-path-query-action: ok");

    let s3_control_tags_path_query_action = send_signed_request_for_service_with_credentials(
        "POST",
        &format!("{s3_control_endpoint}{tags_path_with_query}"),
        tag_body,
        [
            ("content-type", "application/xml"),
            ("x-amz-account-id", account_id),
        ],
        "s3",
        credentials,
    );
    assert_s3_control_error(
        "routing-s3-control-tags-path-query-action",
        &s3_control_tags_path_query_action,
        S3ControlError {
            status: 404,
            code: "NoSuchResource",
            message: "The specified resource doesn't exist.",
            detail: "",
            allow: None,
        },
    );
    println!("routing-s3-control-tags-path-query-action: ok");

    let method_probes = [
        RoutingMethodProbe {
            method: "GET",
            sts: StsMethodResult::UnknownOperation,
            s3_control: S3ControlMethodResult::NoSuchResource,
        },
        RoutingMethodProbe {
            method: "HEAD",
            sts: StsMethodResult::UnknownOperationHead,
            s3_control: S3ControlMethodResult::HeadMethodNotAllowed,
        },
        RoutingMethodProbe {
            method: "POST",
            sts: StsMethodResult::UnknownOperation,
            s3_control: S3ControlMethodResult::NoSuchResource,
        },
        RoutingMethodProbe {
            method: "PUT",
            sts: StsMethodResult::UnknownOperation,
            s3_control: S3ControlMethodResult::MethodNotAllowed,
        },
        RoutingMethodProbe {
            method: "DELETE",
            sts: StsMethodResult::UnknownOperation,
            s3_control: S3ControlMethodResult::NoSuchResource,
        },
        RoutingMethodProbe {
            method: "OPTIONS",
            sts: StsMethodResult::UnknownOperation,
            s3_control: S3ControlMethodResult::OptionsBadRequest,
        },
        RoutingMethodProbe {
            method: "PATCH",
            sts: StsMethodResult::UnknownOperation,
            s3_control: S3ControlMethodResult::MethodNotAllowed,
        },
        RoutingMethodProbe {
            method: "PROPFIND",
            sts: StsMethodResult::FrontendBadRequest,
            s3_control: S3ControlMethodResult::FrontendBadRequest,
        },
        RoutingMethodProbe {
            method: "X-ARGMIN-PROBE",
            sts: StsMethodResult::FrontendBadRequest,
            s3_control: S3ControlMethodResult::FrontendBadRequest,
        },
    ];

    for probe in method_probes {
        let method = probe.method;
        let request_target = if method == "DELETE" {
            format!("{tags_path}?tagKeys=routing")
        } else {
            tags_path.clone()
        };
        let body = if method == "POST" {
            tag_body.as_slice()
        } else {
            b"".as_slice()
        };
        let mut headers = vec![("x-amz-account-id", account_id)];
        if method == "POST" {
            headers.push(("content-type", "application/xml"));
        }

        let sts_response = send_signed_request_for_service_with_credentials(
            method,
            &format!("{sts_endpoint}{request_target}"),
            body,
            headers.clone(),
            "sts",
            credentials,
        );
        let sts_label = format!("routing-method-sts-{method}");
        match probe.sts {
            StsMethodResult::UnknownOperation => {
                assert_sts_unknown_operation(&sts_label, &sts_response, false);
            }
            StsMethodResult::UnknownOperationHead => {
                assert_sts_unknown_operation(&sts_label, &sts_response, true);
            }
            StsMethodResult::FrontendBadRequest => {
                assert_frontend_empty_bad_request(&sts_label, &sts_response);
            }
        }
        println!("{sts_label}: ok");

        let s3_control_response = send_signed_request_for_service_with_credentials(
            method,
            &format!("{s3_control_endpoint}{request_target}"),
            body,
            headers,
            "s3",
            credentials,
        );
        let s3_control_label = format!("routing-method-s3-control-{method}");
        match probe.s3_control {
            S3ControlMethodResult::NoSuchResource => assert_s3_control_error(
                &s3_control_label,
                &s3_control_response,
                S3ControlError {
                    status: 404,
                    code: "NoSuchResource",
                    message: "The specified resource doesn't exist.",
                    detail: "",
                    allow: None,
                },
            ),
            S3ControlMethodResult::HeadMethodNotAllowed => {
                assert_s3_control_head_method_not_allowed(&s3_control_label, &s3_control_response);
            }
            S3ControlMethodResult::MethodNotAllowed => {
                let detail =
                    format!("<Method>{method}</Method><ResourceType>BUCKET_TAGS</ResourceType>");
                assert_s3_control_error(
                    &s3_control_label,
                    &s3_control_response,
                    S3ControlError {
                        status: 405,
                        code: "MethodNotAllowed",
                        message: "The specified method is not allowed against this resource.",
                        detail: &detail,
                        allow: Some("DELETE, POST, GET"),
                    },
                );
            }
            S3ControlMethodResult::OptionsBadRequest => assert_s3_control_error(
                &s3_control_label,
                &s3_control_response,
                S3ControlError {
                    status: 400,
                    code: "BadRequest",
                    message: "Insufficient information. Origin request header needed.",
                    detail: "",
                    allow: None,
                },
            ),
            S3ControlMethodResult::FrontendBadRequest => {
                assert_s3_frontend_bad_request(&s3_control_label, &s3_control_response);
            }
        }
        println!("{s3_control_label}: ok");
    }

    let cors_headers = [
        ("x-amz-account-id", account_id),
        ("origin", "https://example.com"),
        ("access-control-request-method", "POST"),
    ];
    let sts_options_cors = send_signed_request_for_service_with_credentials(
        "OPTIONS",
        &format!("{sts_endpoint}{tags_path}"),
        b"",
        cors_headers,
        "sts",
        credentials,
    );
    assert_sts_cors_preflight_success("routing-options-cors-sts", &sts_options_cors);
    println!("routing-options-cors-sts: ok");

    let s3_control_options_cors = send_signed_request_for_service_with_credentials(
        "OPTIONS",
        &format!("{s3_control_endpoint}{tags_path}"),
        b"",
        cors_headers,
        "s3",
        credentials,
    );
    assert_s3_control_error(
        "routing-options-cors-s3-control",
        &s3_control_options_cors,
        S3ControlError {
            status: 403,
            code: "AccessForbidden",
            message: "CORSResponse: Bucket not found",
            detail: "<Method>POST</Method><ResourceType>BUCKET</ResourceType>",
            allow: None,
        },
    );
    println!("routing-options-cors-s3-control: ok");

    let raw_resource = format!("arn:aws:s3:::claude-s3-sts-routing-{unique:x}");
    let path_probes = vec![
        RoutingPathProbe {
            label: "tags-no-resource",
            wire_path: "/v20180820/tags".to_string(),
            signed_path: None,
            sts: StsPathResult::UnknownOperation,
            s3_control: S3ControlPathResult::InvalidUri("tags".to_string()),
        },
        RoutingPathProbe {
            label: "tags-empty-resource",
            wire_path: "/v20180820/tags/".to_string(),
            signed_path: None,
            sts: StsPathResult::UnknownOperation,
            s3_control: S3ControlPathResult::InvalidUri("tags/".to_string()),
        },
        RoutingPathProbe {
            label: "tags-extra-segment",
            wire_path: format!("{tags_path}/unexpected"),
            signed_path: None,
            sts: StsPathResult::UnknownOperation,
            s3_control: S3ControlPathResult::InvalidUri(format!("tags/{raw_resource}/unexpected")),
        },
        RoutingPathProbe {
            label: "tag-singular",
            wire_path: format!("/v20180820/tag/{resource}"),
            signed_path: None,
            sts: StsPathResult::UnknownOperation,
            s3_control: S3ControlPathResult::InvalidUri(format!("tag/{raw_resource}")),
        },
        RoutingPathProbe {
            label: "tags-prefix-suffix",
            wire_path: format!("/v20180820/tagsx/{resource}"),
            signed_path: None,
            sts: StsPathResult::UnknownOperation,
            s3_control: S3ControlPathResult::InvalidUri(format!("tagsx/{raw_resource}")),
        },
        RoutingPathProbe {
            label: "wrong-version",
            wire_path: format!("/v20180819/tags/{resource}"),
            signed_path: None,
            sts: StsPathResult::UnknownOperation,
            s3_control: S3ControlPathResult::InvalidUri(format!("/v20180819/tags/{resource}")),
        },
        RoutingPathProbe {
            label: "uppercase-version",
            wire_path: format!("/V20180820/tags/{resource}"),
            signed_path: None,
            sts: StsPathResult::UnknownOperation,
            s3_control: S3ControlPathResult::InvalidUri(format!("/V20180820/tags/{resource}")),
        },
        RoutingPathProbe {
            label: "double-leading-slash",
            wire_path: format!("//v20180820/tags/{resource}"),
            signed_path: None,
            sts: StsPathResult::UnknownOperation,
            s3_control: S3ControlPathResult::InvalidUri(format!("//v20180820/tags/{resource}")),
        },
        RoutingPathProbe {
            label: "encoded-path-separator",
            wire_path: format!("/v20180820/tags%2F{resource}"),
            signed_path: Some(tags_path.clone()),
            sts: StsPathResult::UnknownOperation,
            s3_control: S3ControlPathResult::NoSuchResource,
        },
        RoutingPathProbe {
            label: "unencoded-valid-arn",
            wire_path: format!("/v20180820/tags/{raw_resource}"),
            signed_path: Some(tags_path.clone()),
            sts: StsPathResult::UnknownOperation,
            s3_control: S3ControlPathResult::NoSuchResource,
        },
        RoutingPathProbe {
            label: "malformed-percent-bare",
            wire_path: "/v20180820/tags/%".to_string(),
            signed_path: None,
            sts: StsPathResult::EmptyBadRequest,
            s3_control: S3ControlPathResult::EmptyBadRequest,
        },
        RoutingPathProbe {
            label: "malformed-percent-short",
            wire_path: "/v20180820/tags/%2".to_string(),
            signed_path: None,
            sts: StsPathResult::EmptyBadRequest,
            s3_control: S3ControlPathResult::EmptyBadRequest,
        },
        RoutingPathProbe {
            label: "malformed-percent-hex",
            wire_path: "/v20180820/tags/%GG".to_string(),
            signed_path: None,
            sts: StsPathResult::EmptyBadRequest,
            s3_control: S3ControlPathResult::EmptyBadRequest,
        },
        RoutingPathProbe {
            label: "invalid-utf8-percent",
            wire_path: "/v20180820/tags/%FF".to_string(),
            signed_path: None,
            sts: StsPathResult::UnknownOperation,
            s3_control: S3ControlPathResult::InvalidUri("/v20180820/tags/%FF".to_string()),
        },
        RoutingPathProbe {
            label: "malformed-arn",
            wire_path: "/v20180820/tags/not-an-arn".to_string(),
            signed_path: None,
            sts: StsPathResult::UnknownOperation,
            s3_control: S3ControlPathResult::InvalidUri("tags/not-an-arn".to_string()),
        },
        RoutingPathProbe {
            label: "empty-bucket-arn",
            wire_path: "/v20180820/tags/arn%3Aaws%3As3%3A%3A%3A".to_string(),
            signed_path: None,
            sts: StsPathResult::UnknownOperation,
            s3_control: S3ControlPathResult::InvalidUri("tags/arn:aws:s3:::".to_string()),
        },
        RoutingPathProbe {
            label: "wrong-service-arn",
            wire_path: format!("/v20180820/tags/arn%3Aaws%3Aiam%3A%3A{account_id}%3Arole%2Fprobe"),
            signed_path: None,
            sts: StsPathResult::UnknownOperation,
            s3_control: S3ControlPathResult::InvalidUri(format!(
                "tags/arn:aws:iam::{account_id}:role/probe"
            )),
        },
        RoutingPathProbe {
            label: "object-arn",
            wire_path: format!("{tags_path}%2Fobject"),
            signed_path: None,
            sts: StsPathResult::UnknownOperation,
            s3_control: S3ControlPathResult::InvalidUri(format!("tags/{raw_resource}/object")),
        },
        RoutingPathProbe {
            label: "double-encoded-arn",
            wire_path: format!("/v20180820/tags/{}", resource.replace('%', "%25")),
            signed_path: None,
            sts: StsPathResult::UnknownOperation,
            s3_control: S3ControlPathResult::InvalidUri(format!("tags/{resource}")),
        },
    ];
    for probe in path_probes {
        let signed_path = probe.signed_path.as_deref().unwrap_or(&probe.wire_path);
        if probe.signed_path.is_some() {
            assert_ne!(
                probe.wire_path, signed_path,
                "routing-path-{}: normalized signing path must differ from the wire path",
                probe.label
            );
        }

        let sts_label = format!("routing-path-sts-{}", probe.label);
        let sts_response = send_signed_request_to_endpoint_for_service_with_credentials(
            "GET",
            &format!("{sts_endpoint}{}", probe.wire_path),
            &format!("{sts_endpoint}{signed_path}"),
            b"",
            [("x-amz-account-id", account_id)],
            "sts",
            credentials,
        );
        match probe.sts {
            StsPathResult::UnknownOperation => {
                assert_sts_unknown_operation(&sts_label, &sts_response, false);
            }
            StsPathResult::EmptyBadRequest => {
                assert_empty_bad_path_request(&sts_label, &sts_response);
            }
        }
        println!("{sts_label}: ok");

        let s3_control_label = format!("routing-path-s3-control-{}", probe.label);
        let s3_control_response = send_signed_request_to_endpoint_for_service_with_credentials(
            "GET",
            &format!("{s3_control_endpoint}{}", probe.wire_path),
            &format!("{s3_control_endpoint}{signed_path}"),
            b"",
            [("x-amz-account-id", account_id)],
            "s3",
            credentials,
        );
        match probe.s3_control {
            S3ControlPathResult::NoSuchResource => assert_s3_control_error(
                &s3_control_label,
                &s3_control_response,
                S3ControlError {
                    status: 404,
                    code: "NoSuchResource",
                    message: "The specified resource doesn't exist.",
                    detail: "",
                    allow: None,
                },
            ),
            S3ControlPathResult::InvalidUri(uri) => {
                let detail = format!("<URI>{uri}</URI>");
                assert_s3_control_error(
                    &s3_control_label,
                    &s3_control_response,
                    S3ControlError {
                        status: 400,
                        code: "InvalidURI",
                        message: "Couldn't parse the specified URI.",
                        detail: &detail,
                        allow: None,
                    },
                );
            }
            S3ControlPathResult::EmptyBadRequest => {
                assert_empty_bad_path_request(&s3_control_label, &s3_control_response);
            }
        }
        println!("{s3_control_label}: ok");
    }

    for probe in account_id_header_probes(account_id) {
        let sts_label = format!("routing-account-id-sts-{}", probe.label);
        let sts_response = send_signed_request_for_service_with_credentials(
            "GET",
            &format!("{sts_endpoint}{tags_path}"),
            b"",
            probe.headers.iter().copied(),
            "sts",
            credentials,
        );
        assert_sts_unknown_operation(&sts_label, &sts_response, false);
        println!("{sts_label}: ok");

        let s3_control_label = format!("routing-account-id-s3-control-{}", probe.label);
        let s3_control_response = send_signed_request_for_service_with_credentials(
            "GET",
            &format!("{s3_control_endpoint}{tags_path}"),
            b"",
            probe.headers.iter().copied(),
            "s3",
            credentials,
        );
        assert_s3_control_error(
            &s3_control_label,
            &s3_control_response,
            S3ControlError {
                status: 404,
                code: "NoSuchResource",
                message: "The specified resource doesn't exist.",
                detail: "",
                allow: None,
            },
        );
        println!("{s3_control_label}: ok");
    }

    let wrong_secret = "0".repeat(40);
    let bad_signature_credentials = SignedRequestCredentials {
        secret_key: &wrong_secret,
        ..credentials
    };
    for (endpoint_kind, endpoint, correct_service, wrong_service) in [
        ("sts", sts_endpoint, "sts", "s3"),
        ("s3-control", s3_control_endpoint, "s3", "sts"),
    ] {
        for (scope, service) in [
            ("correct-service", correct_service),
            ("missing-service", ""),
            ("wrong-service", wrong_service),
        ] {
            for (signature, signing_credentials, bad_signature) in [
                ("valid-signature", credentials, false),
                ("bad-signature", bad_signature_credentials, true),
            ] {
                let label = format!("routing-auth-{endpoint_kind}-{scope}-{signature}");
                let response = send_signed_request_for_service_with_credentials(
                    "GET",
                    &format!("{endpoint}{tags_path}"),
                    b"",
                    [("x-amz-account-id", account_id)],
                    service,
                    signing_credentials,
                );
                if endpoint_kind == "sts" {
                    assert_sts_unknown_operation(&label, &response, false);
                } else if service != "s3" {
                    assert_s3_control_wrong_service(&label, &response, service);
                } else if bad_signature {
                    assert_s3_control_signature_mismatch(
                        &label,
                        &response,
                        S3ControlCanonicalRequest {
                            method: "GET",
                            endpoint: s3_control_endpoint,
                            path: &tags_path,
                            canonical_query: "",
                            body: b"",
                            headers: &[("x-amz-account-id", account_id)],
                        },
                        bad_signature_credentials,
                    );
                } else {
                    assert_s3_control_error(
                        &label,
                        &response,
                        S3ControlError {
                            status: 404,
                            code: "NoSuchResource",
                            message: "The specified resource doesn't exist.",
                            detail: "",
                            allow: None,
                        },
                    );
                }
                println!("{label}: ok");
            }
        }
    }

    let query_body_text =
        std::str::from_utf8(query_body).expect("routing Query body must be UTF-8");
    let auth_collision_probes = vec![
        RoutingAuthCollisionProbe {
            label: "query-root",
            method: "POST",
            request_path: "/".to_string(),
            canonical_path: "/",
            canonical_query: "",
            body: query_body,
            headers: vec![
                ("content-type", QUERY_CONTENT_TYPE),
                ("x-amz-account-id", account_id),
            ],
            sts: StsAuthCollisionResult::SignatureMismatch,
            s3_control: S3ControlAuthCollisionResult::InvalidUri("/".to_string()),
        },
        RoutingAuthCollisionProbe {
            label: "query-tags-path",
            method: "POST",
            request_path: tags_path.clone(),
            canonical_path: &tags_path,
            canonical_query: query_body_text,
            body: query_body,
            headers: vec![
                ("content-type", QUERY_CONTENT_TYPE),
                ("x-amz-account-id", account_id),
            ],
            sts: StsAuthCollisionResult::SignatureMismatch,
            s3_control: S3ControlAuthCollisionResult::SignatureMismatch,
        },
        RoutingAuthCollisionProbe {
            label: "action-query-tags-path",
            method: "POST",
            request_path: tags_path_with_query.clone(),
            canonical_path: &tags_path,
            canonical_query: query_body_text,
            body: tag_body,
            headers: vec![
                ("content-type", "application/xml"),
                ("x-amz-account-id", account_id),
            ],
            sts: StsAuthCollisionResult::SignatureMismatch,
            s3_control: S3ControlAuthCollisionResult::SignatureMismatch,
        },
        RoutingAuthCollisionProbe {
            label: "malformed-percent",
            method: "GET",
            request_path: "/v20180820/tags/%GG".to_string(),
            canonical_path: "/v20180820/tags/%GG",
            canonical_query: "",
            body: b"",
            headers: vec![("x-amz-account-id", account_id)],
            sts: StsAuthCollisionResult::EmptyBadRequest,
            s3_control: S3ControlAuthCollisionResult::EmptyBadRequest,
        },
        RoutingAuthCollisionProbe {
            label: "malformed-arn",
            method: "GET",
            request_path: "/v20180820/tags/not-an-arn".to_string(),
            canonical_path: "/v20180820/tags/not-an-arn",
            canonical_query: "",
            body: b"",
            headers: vec![("x-amz-account-id", account_id)],
            sts: StsAuthCollisionResult::UnknownOperation,
            s3_control: S3ControlAuthCollisionResult::InvalidUri("tags/not-an-arn".to_string()),
        },
    ];
    for probe in auth_collision_probes {
        let sts_label = format!("routing-auth-collision-sts-{}", probe.label);
        let sts_response = send_signed_request_for_service_with_credentials(
            probe.method,
            &format!("{sts_endpoint}{}", probe.request_path),
            probe.body,
            probe.headers.iter().copied(),
            "sts",
            bad_signature_credentials,
        );
        match probe.sts {
            StsAuthCollisionResult::SignatureMismatch => assert_error_probe(
                &sts_label,
                &sts_response,
                403,
                STS_XMLNS,
                "SignatureDoesNotMatch",
                Some(STS_SIGNATURE_MISMATCH_MESSAGE),
            ),
            StsAuthCollisionResult::UnknownOperation => {
                assert_sts_unknown_operation(&sts_label, &sts_response, false);
            }
            StsAuthCollisionResult::EmptyBadRequest => {
                assert_empty_bad_path_request(&sts_label, &sts_response);
            }
        }
        println!("{sts_label}: ok");

        let s3_control_label = format!("routing-auth-collision-s3-control-{}", probe.label);
        let s3_control_response = send_signed_request_for_service_with_credentials(
            probe.method,
            &format!("{s3_control_endpoint}{}", probe.request_path),
            probe.body,
            probe.headers.iter().copied(),
            "s3",
            bad_signature_credentials,
        );
        match probe.s3_control {
            S3ControlAuthCollisionResult::SignatureMismatch => {
                assert_s3_control_signature_mismatch(
                    &s3_control_label,
                    &s3_control_response,
                    S3ControlCanonicalRequest {
                        method: probe.method,
                        endpoint: s3_control_endpoint,
                        path: probe.canonical_path,
                        canonical_query: probe.canonical_query,
                        body: probe.body,
                        headers: &probe.headers,
                    },
                    bad_signature_credentials,
                );
            }
            S3ControlAuthCollisionResult::InvalidUri(uri) => {
                let detail = format!("<URI>{uri}</URI>");
                assert_s3_control_error(
                    &s3_control_label,
                    &s3_control_response,
                    S3ControlError {
                        status: 400,
                        code: "InvalidURI",
                        message: "Couldn't parse the specified URI.",
                        detail: &detail,
                        allow: None,
                    },
                );
            }
            S3ControlAuthCollisionResult::EmptyBadRequest => {
                assert_empty_bad_path_request(&s3_control_label, &s3_control_response);
            }
        }
        println!("{s3_control_label}: ok");
    }
}

fn run_list_tags_for_resource_success_probe(
    sts_endpoint: &str,
    s3_control_endpoint: &str,
    credentials: SignedRequestCredentials<'_>,
    account_id: &str,
    bucket: &str,
) {
    let resource_arn = auth::canonical::uri_encode(&format!("arn:aws:s3:::{bucket}"));
    let path = format!("/v20180820/tags/{resource_arn}");
    let sts_response = send_signed_request_for_service_with_credentials(
        "GET",
        &format!("{sts_endpoint}{path}"),
        b"",
        [("x-amz-account-id", account_id)],
        "sts",
        credentials,
    );
    assert_sts_unknown_operation("routing-list-tags-existing-sts", &sts_response, false);
    println!("routing-list-tags-existing-sts: ok");

    let s3_control_response = send_signed_request_for_service_with_credentials(
        "GET",
        &format!("{s3_control_endpoint}{path}"),
        b"",
        [("x-amz-account-id", account_id)],
        "s3",
        credentials,
    );
    assert_list_tags_for_resource_success(
        "routing-list-tags-existing-s3-control",
        &s3_control_response,
    );
    println!("routing-list-tags-existing-s3-control: ok");

    let overlong_tag_key = "x".repeat(129);
    let invalid_tag_message = "This request contains a tag key or value that isn't valid. Valid characters include the following: [a-zA-Z+-=._:/]. Tag keys can contain up to 128 characters. Tag values can contain up to 256 characters.";
    let at_least_one_tag = S3ControlBodyResult::Error(S3ControlError {
        status: 400,
        code: "InvalidTag",
        message: "At least one tag is required.",
        detail: "",
        allow: None,
    });
    let invalid_tag = S3ControlBodyResult::Error(S3ControlError {
        status: 400,
        code: "InvalidTag",
        message: invalid_tag_message,
        detail: "",
        allow: None,
    });
    let body_probes = vec![
        RoutingBodyProbe {
            label: "tag-empty-body",
            method: "POST",
            query: String::new(),
            body: b"",
            headers: vec![
                ("content-type", "application/xml"),
                ("x-amz-account-id", account_id),
            ],
            s3_control: S3ControlBodyResult::Error(S3ControlError {
                status: 400,
                code: "MissingRequestBodyError",
                message: "Request Body is empty",
                detail: "",
                allow: None,
            }),
        },
        RoutingBodyProbe {
            label: "tag-truncated-xml",
            method: "POST",
            query: String::new(),
            body: b"<TagResourceRequest",
            headers: vec![
                ("content-type", "application/xml"),
                ("x-amz-account-id", account_id),
            ],
            s3_control: S3ControlBodyResult::Error(S3ControlError {
                status: 400,
                code: "MalformedXML",
                message: "The XML you provided was not well-formed or did not validate against our published schema",
                detail: "",
                allow: None,
            }),
        },
        RoutingBodyProbe {
            label: "tag-wrong-root",
            method: "POST",
            query: String::new(),
            body: b"<WrongRoot/>",
            headers: vec![
                ("content-type", "application/xml"),
                ("x-amz-account-id", account_id),
            ],
            s3_control: at_least_one_tag,
        },
        RoutingBodyProbe {
            label: "tag-missing-tags",
            method: "POST",
            query: String::new(),
            body: b"<TagResourceRequest xmlns=\"http://awss3control.amazonaws.com/doc/2018-08-20/\"/>",
            headers: vec![
                ("content-type", "application/xml"),
                ("x-amz-account-id", account_id),
            ],
            s3_control: at_least_one_tag,
        },
        RoutingBodyProbe {
            label: "tag-empty-tags",
            method: "POST",
            query: String::new(),
            body: b"<TagResourceRequest xmlns=\"http://awss3control.amazonaws.com/doc/2018-08-20/\"><Tags/></TagResourceRequest>",
            headers: vec![
                ("content-type", "application/xml"),
                ("x-amz-account-id", account_id),
            ],
            s3_control: at_least_one_tag,
        },
        RoutingBodyProbe {
            label: "untag-missing-tag-keys",
            method: "DELETE",
            query: String::new(),
            body: b"",
            headers: vec![("x-amz-account-id", account_id)],
            s3_control: at_least_one_tag,
        },
        RoutingBodyProbe {
            label: "untag-empty-tag-key",
            method: "DELETE",
            query: "tagKeys=".to_string(),
            body: b"",
            headers: vec![("x-amz-account-id", account_id)],
            s3_control: invalid_tag,
        },
        RoutingBodyProbe {
            label: "untag-invalid-pattern",
            method: "DELETE",
            query: "tagKeys=%21".to_string(),
            body: b"",
            headers: vec![("x-amz-account-id", account_id)],
            s3_control: invalid_tag,
        },
        RoutingBodyProbe {
            label: "untag-overlong-tag-key",
            method: "DELETE",
            query: format!("tagKeys={overlong_tag_key}"),
            body: b"",
            headers: vec![("x-amz-account-id", account_id)],
            s3_control: invalid_tag,
        },
        RoutingBodyProbe {
            label: "untag-single-tag-key-control",
            method: "DELETE",
            query: "tagKeys=body-probe".to_string(),
            body: b"",
            headers: vec![("x-amz-account-id", account_id)],
            s3_control: S3ControlBodyResult::WriteSuccess,
        },
        RoutingBodyProbe {
            label: "untag-distinct-tag-keys-control",
            method: "DELETE",
            query: "tagKeys=body-probe-a&tagKeys=body-probe-b".to_string(),
            body: b"",
            headers: vec![("x-amz-account-id", account_id)],
            s3_control: S3ControlBodyResult::WriteSuccess,
        },
        RoutingBodyProbe {
            label: "untag-identical-tag-keys",
            method: "DELETE",
            query: "tagKeys=body-probe&tagKeys=body-probe".to_string(),
            body: b"",
            headers: vec![("x-amz-account-id", account_id)],
            s3_control: S3ControlBodyResult::Error(S3ControlError {
                status: 500,
                code: "InternalError",
                message: "We encountered an internal error. Please try again.",
                detail: "",
                allow: None,
            }),
        },
    ];
    let wrong_secret = "0".repeat(40);
    let bad_signature_credentials = SignedRequestCredentials {
        secret_key: &wrong_secret,
        ..credentials
    };
    for probe in &body_probes {
        let request_target = if probe.query.is_empty() {
            path.clone()
        } else {
            format!("{path}?{}", probe.query)
        };
        for (signature, signing_credentials, bad_signature) in [
            ("valid-signature", credentials, false),
            ("bad-signature", bad_signature_credentials, true),
        ] {
            let sts_label = format!("routing-body-sts-{}-{signature}", probe.label);
            let sts_response = send_signed_request_for_service_with_credentials(
                probe.method,
                &format!("{sts_endpoint}{request_target}"),
                probe.body,
                probe.headers.iter().copied(),
                "sts",
                signing_credentials,
            );
            assert_sts_unknown_operation(&sts_label, &sts_response, false);
            println!("{sts_label}: ok");

            let s3_control_label = format!("routing-body-s3-control-{}-{signature}", probe.label);
            let s3_control_response = send_signed_request_for_service_with_credentials(
                probe.method,
                &format!("{s3_control_endpoint}{request_target}"),
                probe.body,
                probe.headers.iter().copied(),
                "s3",
                signing_credentials,
            );
            if bad_signature {
                if probe.method == "DELETE" && probe.query.is_empty() {
                    match probe.s3_control {
                        S3ControlBodyResult::Error(expected) => assert_s3_control_error(
                            &s3_control_label,
                            &s3_control_response,
                            expected,
                        ),
                        S3ControlBodyResult::WriteSuccess => panic!(
                            "{s3_control_label}: a missing required query member cannot be a success control"
                        ),
                    }
                } else {
                    assert_s3_control_signature_mismatch(
                        &s3_control_label,
                        &s3_control_response,
                        S3ControlCanonicalRequest {
                            method: probe.method,
                            endpoint: s3_control_endpoint,
                            path: &path,
                            canonical_query: &probe.query,
                            body: probe.body,
                            headers: &probe.headers,
                        },
                        bad_signature_credentials,
                    );
                }
            } else {
                match probe.s3_control {
                    S3ControlBodyResult::Error(expected) => {
                        assert_s3_control_error(&s3_control_label, &s3_control_response, expected);
                    }
                    S3ControlBodyResult::WriteSuccess => {
                        assert_s3_control_write_success(&s3_control_label, &s3_control_response);
                    }
                }
            }
            println!("{s3_control_label}: ok");
        }
    }

    let identical_tag_keys_probe = body_probes
        .iter()
        .find(|probe| probe.label == "untag-identical-tag-keys")
        .expect("identical tag-key convergence probe must exist");
    for attempt in 1..=3 {
        let label = format!("routing-body-s3-control-untag-identical-consecutive-{attempt}");
        let response = send_signed_request_for_service_with_credentials(
            identical_tag_keys_probe.method,
            &format!(
                "{s3_control_endpoint}{path}?{}",
                identical_tag_keys_probe.query
            ),
            identical_tag_keys_probe.body,
            identical_tag_keys_probe.headers.iter().copied(),
            "s3",
            credentials,
        );
        match identical_tag_keys_probe.s3_control {
            S3ControlBodyResult::Error(expected) => {
                assert_s3_control_error(&label, &response, expected);
            }
            S3ControlBodyResult::WriteSuccess => {
                panic!("{label}: identical tag keys unexpectedly use a success golden");
            }
        }
        println!("{label}: ok");
    }

    for probe in [
        body_probes
            .iter()
            .find(|probe| probe.label == "tag-truncated-xml")
            .expect("tag malformed-body scope collision probe must exist"),
        body_probes
            .iter()
            .find(|probe| probe.label == "untag-missing-tag-keys")
            .expect("untag malformed-query scope collision probe must exist"),
    ] {
        let request_target = if probe.query.is_empty() {
            path.clone()
        } else {
            format!("{path}?{}", probe.query)
        };
        for (signature, signing_credentials) in [
            ("valid-signature", credentials),
            ("bad-signature", bad_signature_credentials),
        ] {
            let sts_label = format!("routing-body-scope-sts-{}-{signature}", probe.label);
            let sts_response = send_signed_request_for_service_with_credentials(
                probe.method,
                &format!("{sts_endpoint}{request_target}"),
                probe.body,
                probe.headers.iter().copied(),
                "s3",
                signing_credentials,
            );
            assert_sts_unknown_operation(&sts_label, &sts_response, false);
            println!("{sts_label}: ok");

            let s3_control_label =
                format!("routing-body-scope-s3-control-{}-{signature}", probe.label);
            let s3_control_response = send_signed_request_for_service_with_credentials(
                probe.method,
                &format!("{s3_control_endpoint}{request_target}"),
                probe.body,
                probe.headers.iter().copied(),
                "sts",
                signing_credentials,
            );
            if probe.method == "DELETE" && probe.query.is_empty() {
                match probe.s3_control {
                    S3ControlBodyResult::Error(expected) => {
                        assert_s3_control_error(&s3_control_label, &s3_control_response, expected);
                    }
                    S3ControlBodyResult::WriteSuccess => panic!(
                        "{s3_control_label}: a missing required query member cannot be a success control"
                    ),
                }
            } else {
                assert_s3_control_wrong_service(&s3_control_label, &s3_control_response, "sts");
            }
            println!("{s3_control_label}: ok");
        }
    }

    let tag_body = b"<?xml version=\"1.0\" encoding=\"UTF-8\"?><TagResourceRequest xmlns=\"http://awss3control.amazonaws.com/doc/2018-08-20/\"><Tags><Tag><Key>routing-key</Key><Value>routing-value</Value></Tag></Tags></TagResourceRequest>";
    for probe in account_id_header_probes(account_id) {
        for operation in [
            AccountIdOperation::List,
            AccountIdOperation::Tag,
            AccountIdOperation::Untag,
        ] {
            let (operation_label, method, request_path, body, content_type) = match operation {
                AccountIdOperation::List => ("list", "GET", path.clone(), b"".as_slice(), None),
                AccountIdOperation::Tag => (
                    "tag",
                    "POST",
                    path.clone(),
                    tag_body.as_slice(),
                    Some("application/xml"),
                ),
                AccountIdOperation::Untag => (
                    "untag",
                    "DELETE",
                    format!("{path}?tagKeys=account-id-probe"),
                    b"".as_slice(),
                    None,
                ),
            };
            let mut headers = probe.headers.clone();
            if let Some(content_type) = content_type {
                headers.push(("content-type", content_type));
            }

            let sts_label = format!(
                "routing-existing-account-id-sts-{operation_label}-{}",
                probe.label
            );
            let sts_response = send_signed_request_for_service_with_credentials(
                method,
                &format!("{sts_endpoint}{request_path}"),
                body,
                headers.iter().copied(),
                "sts",
                credentials,
            );
            assert_sts_unknown_operation(&sts_label, &sts_response, false);
            println!("{sts_label}: ok");

            let s3_control_label = format!(
                "routing-existing-account-id-s3-control-{operation_label}-{}",
                probe.label
            );
            let s3_control_response = send_signed_request_for_service_with_credentials(
                method,
                &format!("{s3_control_endpoint}{request_path}"),
                body,
                headers.iter().copied(),
                "s3",
                credentials,
            );
            match operation {
                AccountIdOperation::List => {
                    assert_list_tags_for_resource_success(&s3_control_label, &s3_control_response)
                }
                AccountIdOperation::Tag | AccountIdOperation::Untag => {
                    assert_s3_control_write_success(&s3_control_label, &s3_control_response);
                }
            }
            println!("{s3_control_label}: ok");
        }
    }
}

fn main() {
    let endpoint = required_https_endpoint("S3_TEST_STS_ENDPOINT");
    let s3_control_endpoint = required_https_endpoint("S3_TEST_S3_CONTROL_ENDPOINT");
    let access_key = required_env("S3_TEST_ACCESS_KEY");
    let secret_key = required_env("S3_TEST_SECRET_KEY");
    let account_id = required_env("S3_TEST_ACCOUNT_ID");
    let region = required_env("S3_TEST_REGION");
    let credentials = SignedRequestCredentials {
        access_key: &access_key,
        secret_key: &secret_key,
        region: &region,
        tls_ca_pem: None,
    };
    if let Ok(bucket) = env::var("S3_TEST_STS_POST_BUCKET") {
        run_list_tags_for_resource_success_probe(
            &endpoint,
            &s3_control_endpoint,
            credentials,
            &account_id,
            &bucket,
        );
    }
    run_cross_service_routing_probes(&endpoint, &s3_control_endpoint, credentials, &account_id);

    let probes = [
        Probe {
            label: "get-caller-identity-get",
            request: QueryRequest::Get("Action=GetCallerIdentity&Version=2011-06-15"),
            expected: Expected::Success,
        },
        Probe {
            label: "get-caller-identity-post",
            request: QueryRequest::Post {
                body: "Action=GetCallerIdentity&Version=2011-06-15",
                content_type: Some(QUERY_CONTENT_TYPE),
            },
            expected: Expected::Success,
        },
        Probe {
            label: "post-content-type-with-charset",
            request: QueryRequest::Post {
                body: "Action=GetCallerIdentity&Version=2011-06-15",
                content_type: Some("application/x-www-form-urlencoded; charset=utf-8"),
            },
            expected: Expected::Success,
        },
        Probe {
            label: "post-without-content-type",
            request: QueryRequest::Post {
                body: "Action=GetCallerIdentity&Version=2011-06-15",
                content_type: None,
            },
            expected: Expected::Redirect {
                location: "https://aws.amazon.com/iam",
            },
        },
        Probe {
            label: "missing-action",
            request: QueryRequest::Post {
                body: "Version=2011-06-15",
                content_type: Some(QUERY_CONTENT_TYPE),
            },
            expected: Expected::Redirect {
                location: "https://aws.amazon.com/iam",
            },
        },
        Probe {
            label: "unknown-action",
            request: QueryRequest::Post {
                body: "Action=NoSuchAction&Version=2011-06-15",
                content_type: Some(QUERY_CONTENT_TYPE),
            },
            expected: Expected::Error {
                code: "InvalidAction",
                message: "Could not find operation NoSuchAction for version 2011-06-15",
            },
        },
        Probe {
            label: "missing-version",
            request: QueryRequest::Post {
                body: "Action=GetCallerIdentity",
                content_type: Some(QUERY_CONTENT_TYPE),
            },
            expected: Expected::Error {
                code: "InvalidAction",
                message:
                    "Could not find operation GetCallerIdentity for version NO_VERSION_SPECIFIED",
            },
        },
        Probe {
            label: "empty-version",
            request: QueryRequest::Post {
                body: "Action=GetCallerIdentity&Version=",
                content_type: Some(QUERY_CONTENT_TYPE),
            },
            expected: Expected::Error {
                code: "InvalidAction",
                message: "Could not find operation GetCallerIdentity for version ",
            },
        },
        Probe {
            label: "unsupported-version",
            request: QueryRequest::Post {
                body: "Action=GetCallerIdentity&Version=2000-01-01",
                content_type: Some(QUERY_CONTENT_TYPE),
            },
            expected: Expected::Error {
                code: "InvalidAction",
                message: "Could not find operation GetCallerIdentity for version 2000-01-01",
            },
        },
        Probe {
            label: "duplicate-action",
            request: QueryRequest::Post {
                body: "Action=GetCallerIdentity&Action=NoSuchAction&Version=2011-06-15",
                content_type: Some(QUERY_CONTENT_TYPE),
            },
            expected: Expected::Success,
        },
        Probe {
            label: "duplicate-action-invalid-first",
            request: QueryRequest::Post {
                body: "Action=NoSuchAction&Action=GetCallerIdentity&Version=2011-06-15",
                content_type: Some(QUERY_CONTENT_TYPE),
            },
            expected: Expected::Error {
                code: "InvalidAction",
                message: "Could not find operation NoSuchAction for version 2011-06-15",
            },
        },
        Probe {
            label: "duplicate-version",
            request: QueryRequest::Post {
                body: "Action=GetCallerIdentity&Version=2011-06-15&Version=2000-01-01",
                content_type: Some(QUERY_CONTENT_TYPE),
            },
            expected: Expected::Success,
        },
        Probe {
            label: "duplicate-version-invalid-first",
            request: QueryRequest::Post {
                body: "Action=GetCallerIdentity&Version=2000-01-01&Version=2011-06-15",
                content_type: Some(QUERY_CONTENT_TYPE),
            },
            expected: Expected::Error {
                code: "InvalidAction",
                message: "Could not find operation GetCallerIdentity for version 2000-01-01",
            },
        },
        Probe {
            label: "empty-action",
            request: QueryRequest::Post {
                body: "Action=&Version=2011-06-15",
                content_type: Some(QUERY_CONTENT_TYPE),
            },
            expected: Expected::Error {
                code: "InvalidAction",
                message: "Could not find operation  for version 2011-06-15",
            },
        },
        Probe {
            label: "get-missing-action",
            request: QueryRequest::Get("Version=2011-06-15"),
            expected: Expected::Redirect {
                location: "https://aws.amazon.com/iam?Version=2011-06-15",
            },
        },
        Probe {
            label: "unknown-parameter",
            request: QueryRequest::Post {
                body: "Action=GetCallerIdentity&Version=2011-06-15&Unknown=value",
                content_type: Some(QUERY_CONTENT_TYPE),
            },
            expected: Expected::Success,
        },
    ];

    for probe in probes {
        let response = probe.request.send(&endpoint, credentials);
        assert_probe(&probe, &response, &account_id);
        println!("{}: ok", probe.label);
    }
    run_query_limit_probes(&endpoint, credentials, &account_id);
    run_signing_scope_probes(&endpoint, credentials, &account_id);

    if let Ok(role_arn) = env::var("S3_TEST_STS_ROLE_ARN") {
        let caller_arn = required_env("S3_TEST_STS_PRIMARY_ARN");
        let role_name = required_env("S3_TEST_STS_ROLE_NAME");
        let default_max_role_arn = required_env("S3_TEST_STS_DEFAULT_MAX_ROLE_ARN");
        let default_max_role_name = required_env("S3_TEST_STS_DEFAULT_MAX_ROLE_NAME");
        let external_id = required_env("S3_TEST_STS_EXTERNAL_ID");
        let external_id_role_arn = required_env("S3_TEST_STS_EXTERNAL_ID_ROLE_ARN");
        let external_id_role_name = required_env("S3_TEST_STS_EXTERNAL_ID_ROLE_NAME");
        let role_session_name = required_env("S3_TEST_STS_ROLE_SESSION_NAME");
        run_assume_role_probes(
            &endpoint,
            credentials,
            &account_id,
            AssumeRoleProbeSet {
                caller_arn: &caller_arn,
                role_arn: &role_arn,
                role_name: &role_name,
                default_max_role_arn: &default_max_role_arn,
                default_max_role_name: &default_max_role_name,
                external_id: &external_id,
                external_id_role_arn: &external_id_role_arn,
                external_id_role_name: &external_id_role_name,
                role_session_name: &role_session_name,
            },
        );

        let source_identity = required_env("S3_TEST_STS_SOURCE_IDENTITY");
        let source_identity_role_arn = required_env("S3_TEST_STS_SOURCE_IDENTITY_ROLE_ARN");
        let source_identity_role_name = required_env("S3_TEST_STS_SOURCE_IDENTITY_ROLE_NAME");
        let condition_role_arn = required_env("S3_TEST_STS_SOURCE_IDENTITY_CONDITION_ROLE_ARN");
        let condition_role_name = required_env("S3_TEST_STS_SOURCE_IDENTITY_CONDITION_ROLE_NAME");
        let source_access_key = required_env("S3_TEST_STS_SOURCE_IDENTITY_ACCESS_KEY");
        let source_secret_key = required_env("S3_TEST_STS_SOURCE_IDENTITY_SECRET_KEY");
        let source_session_token = required_env("S3_TEST_STS_SOURCE_IDENTITY_SESSION_TOKEN");
        let source_session_name = required_env("S3_TEST_STS_SOURCE_IDENTITY_SESSION_NAME");
        let target_role_arn = required_env("S3_TEST_STS_SOURCE_IDENTITY_TARGET_ROLE_ARN");
        let target_role_name = required_env("S3_TEST_STS_SOURCE_IDENTITY_TARGET_ROLE_NAME");
        let target_session_name = required_env("S3_TEST_STS_SOURCE_IDENTITY_TARGET_SESSION_NAME");
        let no_set_target_role_arn = required_env("S3_TEST_STS_CHAIN_TARGET_ROLE_ARN");
        let source_credentials = SignedRequestCredentials {
            access_key: &source_access_key,
            secret_key: &source_secret_key,
            region: &region,
            tls_ca_pem: None,
        };
        run_source_identity_probes(
            &endpoint,
            credentials,
            source_credentials,
            &account_id,
            SourceIdentityProbeSet {
                caller_arn: &caller_arn,
                source_identity: &source_identity,
                source_role_arn: &source_identity_role_arn,
                source_role_name: &source_identity_role_name,
                condition_role_arn: &condition_role_arn,
                condition_role_name: &condition_role_name,
                role_session_name: &role_session_name,
                source_session_token: &source_session_token,
                source_session_role_name: &source_identity_role_name,
                source_session_name: &source_session_name,
                target_role_arn: &target_role_arn,
                target_role_name: &target_role_name,
                target_session_name: &target_session_name,
                no_set_target_role_arn: &no_set_target_role_arn,
            },
        );
    }

    if let Ok(success_role_arn) = env::var("S3_TEST_STS_CROSS_SUCCESS_ROLE_ARN") {
        let alt_access_key = required_env("S3_TEST_STS_ALT_ACCESS_KEY");
        let alt_secret_key = required_env("S3_TEST_STS_ALT_SECRET_KEY");
        let alt_credentials = SignedRequestCredentials {
            access_key: &alt_access_key,
            secret_key: &alt_secret_key,
            region: &region,
            tls_ca_pem: None,
        };
        let caller_arn = required_env("S3_TEST_STS_ALT_ARN");
        let role_session_name = required_env("S3_TEST_STS_CROSS_SESSION_NAME");
        let success_role_name = required_env("S3_TEST_STS_CROSS_SUCCESS_ROLE_NAME");
        let trust_denied_role_arn = required_env("S3_TEST_STS_CROSS_TRUST_DENIED_ROLE_ARN");
        let caller_denied_role_arn = required_env("S3_TEST_STS_CROSS_CALLER_DENIED_ROLE_ARN");
        run_cross_account_probes(
            &endpoint,
            alt_credentials,
            &account_id,
            CrossAccountProbeSet {
                caller_arn: &caller_arn,
                role_session_name: &role_session_name,
                success_role_name: &success_role_name,
                success_role_arn: &success_role_arn,
                trust_denied_role_arn: &trust_denied_role_arn,
                caller_denied_role_arn: &caller_denied_role_arn,
            },
        );
    }

    if let Ok(target_role_arn) = env::var("S3_TEST_STS_CHAIN_TARGET_ROLE_ARN") {
        let chain_access_key = required_env("S3_TEST_STS_CHAIN_ACCESS_KEY");
        let chain_secret_key = required_env("S3_TEST_STS_CHAIN_SECRET_KEY");
        let chain_security_token = required_env("S3_TEST_STS_CHAIN_SESSION_TOKEN");
        let other_live_security_token = required_env("S3_TEST_STS_OTHER_LIVE_SESSION_TOKEN");
        let chain_role_name = required_env("S3_TEST_STS_ROLE_NAME");
        let chain_role_session_name = required_env("S3_TEST_STS_CHAIN_SOURCE_SESSION_NAME");
        let target_role_name = required_env("S3_TEST_STS_CHAIN_TARGET_ROLE_NAME");
        let target_session_name = required_env("S3_TEST_STS_CHAIN_TARGET_SESSION_NAME");
        let low_max_target_role_arn = required_env("S3_TEST_STS_DEFAULT_MAX_ROLE_ARN");
        let chain_credentials = SignedRequestCredentials {
            access_key: &chain_access_key,
            secret_key: &chain_secret_key,
            region: &region,
            tls_ca_pem: None,
        };
        run_role_chaining_probes(
            &endpoint,
            chain_credentials,
            &account_id,
            RoleChainingProbeSet {
                security_token: &chain_security_token,
                target_role_arn: &target_role_arn,
                target_role_name: &target_role_name,
                target_session_name: &target_session_name,
                low_max_target_role_arn: &low_max_target_role_arn,
            },
        );

        let deleted_access_key = required_env("S3_TEST_STS_DELETED_ROLE_ACCESS_KEY");
        let deleted_secret_key = required_env("S3_TEST_STS_DELETED_ROLE_SECRET_KEY");
        let deleted_security_token = required_env("S3_TEST_STS_DELETED_ROLE_SESSION_TOKEN");
        let deleted_credentials = SignedRequestCredentials {
            access_key: &deleted_access_key,
            secret_key: &deleted_secret_key,
            region: &region,
            tls_ca_pem: None,
        };
        let recreated_access_key = required_env("S3_TEST_STS_RECREATED_ROLE_ACCESS_KEY");
        let recreated_secret_key = required_env("S3_TEST_STS_RECREATED_ROLE_SECRET_KEY");
        let recreated_security_token = required_env("S3_TEST_STS_RECREATED_ROLE_SESSION_TOKEN");
        let recreated_role_name = required_env("S3_TEST_STS_DELETED_ROLE_NAME");
        let recreated_role_session_name = required_env("S3_TEST_STS_RECREATED_ROLE_SESSION_NAME");
        let recreated_role_id = required_env("S3_TEST_STS_RECREATED_ROLE_ID");
        let recreated_credentials = SignedRequestCredentials {
            access_key: &recreated_access_key,
            secret_key: &recreated_secret_key,
            region: &region,
            tls_ca_pem: None,
        };
        let post_bucket = required_env("S3_TEST_STS_POST_BUCKET");
        run_s3_header_session_authentication_probes(
            &format!("https://s3.{region}.amazonaws.com/"),
            &account_id,
            S3HeaderSessionProbeSet {
                live_credentials: recreated_credentials,
                live_security_token: &recreated_security_token,
                live_role_name: &recreated_role_name,
                live_role_session_name: &recreated_role_session_name,
                other_live_security_token: &other_live_security_token,
                old_credentials: deleted_credentials,
                old_security_token: &deleted_security_token,
            },
        );
        run_s3_header_scope_probes(
            &format!("https://{post_bucket}.s3.{region}.amazonaws.com/"),
            S3HeaderScopeProbeSet {
                account_id: &account_id,
                bucket: &post_bucket,
                live_credentials: recreated_credentials,
                live_security_token: &recreated_security_token,
                live_role_name: &recreated_role_name,
                live_role_session_name: &recreated_role_session_name,
                other_live_security_token: &other_live_security_token,
                old_credentials: deleted_credentials,
                old_security_token: &deleted_security_token,
            },
        );
        let presigned_fixture = S3PresignedSessionProbeSet {
            live_credentials: recreated_credentials,
            live_security_token: &recreated_security_token,
            live_role_name: &recreated_role_name,
            live_role_session_name: &recreated_role_session_name,
            other_live_security_token: &other_live_security_token,
            old_credentials: deleted_credentials,
            old_security_token: &deleted_security_token,
        };
        run_s3_presigned_session_authentication_probes(
            &format!("https://s3.{region}.amazonaws.com/"),
            &account_id,
            presigned_fixture,
        );
        run_s3_presigned_scope_probes(
            &format!("https://{post_bucket}.s3.{region}.amazonaws.com/"),
            &account_id,
            &post_bucket,
            presigned_fixture,
        );
        let recreated_session_arn = required_env("S3_TEST_STS_RECREATED_SESSION_ARN");
        run_s3_session_context_probes(
            &format!("https://{post_bucket}.s3.{region}.amazonaws.com"),
            &post_bucket,
            S3SessionContextProbeSet {
                credentials: recreated_credentials,
                security_token: &recreated_security_token,
                assumed_role_arn: &recreated_session_arn,
            },
        );
        let policy_pre_access_key = required_env("S3_TEST_STS_POLICY_MUTATION_PRE_ACCESS_KEY");
        let policy_pre_secret_key = required_env("S3_TEST_STS_POLICY_MUTATION_PRE_SECRET_KEY");
        let policy_pre_security_token =
            required_env("S3_TEST_STS_POLICY_MUTATION_PRE_SESSION_TOKEN");
        let policy_pre_assumed_role_arn =
            required_env("S3_TEST_STS_POLICY_MUTATION_PRE_SESSION_ARN");
        let policy_post_access_key = required_env("S3_TEST_STS_POLICY_MUTATION_POST_ACCESS_KEY");
        let policy_post_secret_key = required_env("S3_TEST_STS_POLICY_MUTATION_POST_SECRET_KEY");
        let policy_post_security_token =
            required_env("S3_TEST_STS_POLICY_MUTATION_POST_SESSION_TOKEN");
        let policy_post_assumed_role_arn =
            required_env("S3_TEST_STS_POLICY_MUTATION_POST_SESSION_ARN");
        run_s3_role_policy_mutation_probes(
            &format!("https://{post_bucket}.s3.{region}.amazonaws.com"),
            &post_bucket,
            S3RolePolicyMutationProbeSet {
                pre_credentials: SignedRequestCredentials {
                    access_key: &policy_pre_access_key,
                    secret_key: &policy_pre_secret_key,
                    region: &region,
                    tls_ca_pem: None,
                },
                pre_security_token: &policy_pre_security_token,
                pre_assumed_role_arn: &policy_pre_assumed_role_arn,
                post_credentials: SignedRequestCredentials {
                    access_key: &policy_post_access_key,
                    secret_key: &policy_post_secret_key,
                    region: &region,
                    tls_ca_pem: None,
                },
                post_security_token: &policy_post_security_token,
                post_assumed_role_arn: &policy_post_assumed_role_arn,
            },
        );
        let post_fixture = S3PostSessionProbeSet {
            live_credentials: recreated_credentials,
            live_security_token: &recreated_security_token,
            live_role_name: &recreated_role_name,
            live_role_session_name: &recreated_role_session_name,
            other_live_security_token: &other_live_security_token,
            old_credentials: deleted_credentials,
            old_security_token: &deleted_security_token,
        };
        run_s3_post_session_authentication_probes(
            &format!("https://s3.{region}.amazonaws.com"),
            &account_id,
            &post_bucket,
            post_fixture,
        );
        run_s3_post_scope_probes(
            &format!("https://s3.{region}.amazonaws.com"),
            &post_bucket,
            post_fixture,
        );
        let streaming_fixture = S3StreamingSessionProbeSet {
            live_credentials: recreated_credentials,
            live_security_token: &recreated_security_token,
            other_live_security_token: &other_live_security_token,
            old_credentials: deleted_credentials,
            old_security_token: &deleted_security_token,
        };
        run_s3_streaming_session_authentication_probes(
            &format!("https://s3.{region}.amazonaws.com"),
            &post_bucket,
            streaming_fixture,
        );
        run_s3_streaming_scope_probes(
            &format!("https://s3.{region}.amazonaws.com"),
            &post_bucket,
            streaming_fixture,
        );
        run_session_authentication_probes(
            &endpoint,
            &account_id,
            SessionAuthenticationProbeSet {
                active_credentials: chain_credentials,
                active_security_token: &chain_security_token,
                other_live_security_token: &other_live_security_token,
                active_role_name: &chain_role_name,
                active_role_session_name: &chain_role_session_name,
                recreated_credentials,
                recreated_security_token: &recreated_security_token,
                recreated_role_name: &recreated_role_name,
                recreated_role_session_name: &recreated_role_session_name,
                recreated_role_id: &recreated_role_id,
                old_credentials: deleted_credentials,
                old_security_token: &deleted_security_token,
            },
        );

        if let Ok(expired_deleted_access_key) = env::var("S3_TEST_STS_EXPIRED_DELETED_ACCESS_KEY") {
            let expiry_liveness_access_key = required_env("S3_TEST_STS_EXPIRY_LIVENESS_ACCESS_KEY");
            let expiry_liveness_secret_key = required_env("S3_TEST_STS_EXPIRY_LIVENESS_SECRET_KEY");
            let expiry_liveness_security_token =
                required_env("S3_TEST_STS_EXPIRY_LIVENESS_SESSION_TOKEN");
            let expiry_liveness_credentials = SignedRequestCredentials {
                access_key: &expiry_liveness_access_key,
                secret_key: &expiry_liveness_secret_key,
                region: &region,
                tls_ca_pem: None,
            };
            let expired_deleted_secret_key = required_env("S3_TEST_STS_EXPIRED_DELETED_SECRET_KEY");
            let expired_deleted_security_token =
                required_env("S3_TEST_STS_EXPIRED_DELETED_SESSION_TOKEN");
            let expired_deleted_credentials = SignedRequestCredentials {
                access_key: &expired_deleted_access_key,
                secret_key: &expired_deleted_secret_key,
                region: &region,
                tls_ca_pem: None,
            };
            let s3_endpoint = format!("https://s3.{region}.amazonaws.com");
            run_s3_deleted_issuer_convergence_probes(
                &s3_endpoint,
                &post_bucket,
                expiry_liveness_credentials,
                &expiry_liveness_security_token,
            );
            run_expired_deleted_session_probes(
                &endpoint,
                &s3_endpoint,
                &post_bucket,
                expired_deleted_credentials,
                &expired_deleted_security_token,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        matches_observed_get_boundary_fixture, s3_post_response_with_sanitized_body,
        s3_response_with_sanitized_body, sign_s3_streaming_request,
        sign_s3_streaming_request_for_service, spaced_hex, validate_https_endpoint,
        S3StreamingTokens, OBSERVED_GET_BOUNDARY_ENDPOINT,
    };
    use s3_tests::{RawResponse, SignedRequestCredentials};

    #[test]
    fn aws_oracle_endpoints_require_valid_https_urls() {
        assert!(validate_https_endpoint(
            "S3_TEST_STS_ENDPOINT",
            "https://sts.us-east-1.amazonaws.com"
        )
        .is_ok());
        assert!(validate_https_endpoint(
            "S3_TEST_S3_CONTROL_ENDPOINT",
            "https://111122223333.s3-control.us-east-1.amazonaws.com"
        )
        .is_ok());

        for (name, endpoint) in [
            ("S3_TEST_STS_ENDPOINT", "http://sts.us-east-1.amazonaws.com"),
            (
                "S3_TEST_S3_CONTROL_ENDPOINT",
                "http://111122223333.s3-control.us-east-1.amazonaws.com",
            ),
            ("S3_TEST_STS_ENDPOINT", "ftp://sts.us-east-1.amazonaws.com"),
            ("S3_TEST_STS_ENDPOINT", "https://"),
            ("S3_TEST_STS_ENDPOINT", "not-a-url"),
        ] {
            assert!(
                validate_https_endpoint(name, endpoint).is_err(),
                "unsafe endpoint unexpectedly accepted: {endpoint}"
            );
        }
    }

    #[test]
    fn get_request_head_boundary_requires_the_observed_fixture() {
        let credentials = SignedRequestCredentials {
            access_key: "ABCDEFGHIJKLMNOPQRST",
            secret_key: "secret",
            region: "eu-central-1",
            tls_ca_pem: None,
        };
        assert!(matches_observed_get_boundary_fixture(
            OBSERVED_GET_BOUNDARY_ENDPOINT,
            credentials
        ));
        assert!(!matches_observed_get_boundary_fixture(
            "https://sts.us-east-1.amazonaws.com",
            credentials
        ));
        assert!(!matches_observed_get_boundary_fixture(
            OBSERVED_GET_BOUNDARY_ENDPOINT,
            SignedRequestCredentials {
                region: "us-east-1",
                ..credentials
            }
        ));
        assert!(!matches_observed_get_boundary_fixture(
            OBSERVED_GET_BOUNDARY_ENDPOINT,
            SignedRequestCredentials {
                access_key: "short",
                ..credentials
            }
        ));
    }

    #[test]
    fn streaming_signer_canonicalizes_duplicate_tokens_in_wire_order() {
        let credentials = SignedRequestCredentials {
            access_key: "ARGMINSESSIONACCESSKEY",
            secret_key: "secret",
            region: "eu-central-1",
            tls_ca_pem: None,
        };
        let signed = sign_s3_streaming_request(
            "https://s3.eu-central-1.amazonaws.com",
            "/bucket/key",
            21,
            credentials,
            S3StreamingTokens::Two("first-token", "second-token"),
            true,
        );

        assert!(signed
            .canonical_request
            .contains("x-amz-security-token:first-token,second-token\n"));
        assert!(signed.authorization.contains(
            "SignedHeaders=content-encoding;host;x-amz-content-sha256;x-amz-date;\
             x-amz-decoded-content-length;x-amz-security-token"
        ));

        let unsigned = sign_s3_streaming_request(
            "https://s3.eu-central-1.amazonaws.com",
            "/bucket/key",
            21,
            credentials,
            S3StreamingTokens::One("unsigned-token"),
            false,
        );
        assert!(!unsigned.canonical_request.contains("security-token"));
        assert!(!unsigned.authorization.contains("security-token"));

        let wrong_service = sign_s3_streaming_request_for_service(
            "https://s3.eu-central-1.amazonaws.com",
            "/bucket/key",
            21,
            credentials,
            S3StreamingTokens::One("token"),
            true,
            "sts",
        );
        let date = &wrong_service.scope[..8];
        let expected_signing_key = auth::sigv4::derive_signing_key(
            &auth::SecretKey::new(credentials.secret_key.to_string()),
            date,
            credentials.region,
            "sts",
        );
        assert_eq!(
            wrong_service.scope,
            format!("{date}/eu-central-1/sts/aws4_request")
        );
        assert!(wrong_service.authorization.contains(&format!(
            "Credential=ARGMINSESSIONACCESSKEY/{date}/eu-central-1/sts/aws4_request"
        )));
        assert_eq!(wrong_service.signing_key, expected_signing_key.as_ref());
    }

    #[test]
    fn s3_response_sanitization_removes_literal_and_encoded_credentials() {
        let access_key = "SESSIONACCESSKEY12345";
        let security_token = "opaque+session/token=value";
        let other_security_token = "other+query/token=value";
        let uri_encoded_token = auth::canonical::uri_encode(security_token);
        let other_uri_encoded_token = auth::canonical::uri_encode(other_security_token);
        let response = RawResponse {
            status: 403,
            headers: Vec::new(),
            body: format!(
                "{access_key}|{}|{security_token}|{}|{uri_encoded_token}|{}|\
                 {other_security_token}|{}|{other_uri_encoded_token}|{}",
                spaced_hex(access_key),
                spaced_hex(security_token),
                spaced_hex(&uri_encoded_token),
                spaced_hex(other_security_token),
                spaced_hex(&other_uri_encoded_token)
            ),
            body_read_error: None,
        };

        let sanitized = s3_response_with_sanitized_body(
            &response,
            access_key,
            &[security_token, other_security_token],
        );
        assert!(!sanitized.body.contains(access_key));
        assert!(!sanitized.body.contains(&spaced_hex(access_key)));
        assert!(!sanitized.body.contains(security_token));
        assert!(!sanitized.body.contains(&spaced_hex(security_token)));
        assert!(!sanitized.body.contains(&uri_encoded_token));
        assert!(!sanitized.body.contains(&spaced_hex(&uri_encoded_token)));
        assert!(!sanitized.body.contains(other_security_token));
        assert!(!sanitized.body.contains(&spaced_hex(other_security_token)));
        assert!(!sanitized.body.contains(&other_uri_encoded_token));
        assert!(!sanitized
            .body
            .contains(&spaced_hex(&other_uri_encoded_token)));
        assert_eq!(
            sanitized.body,
            "SESSION_ACCESS_KEY|SESSION_ACCESS_KEY_BYTES|\
             SESSION_TOKEN|SESSION_TOKEN_BYTES|SESSION_TOKEN_URI_ENCODED|\
             SESSION_TOKEN_URI_ENCODED_BYTES|SESSION_TOKEN|SESSION_TOKEN_BYTES|\
             SESSION_TOKEN_URI_ENCODED|SESSION_TOKEN_URI_ENCODED_BYTES"
        );
    }

    #[test]
    fn s3_post_response_sanitization_removes_policy_and_credentials() {
        let access_key = "SESSIONACCESSKEY12345";
        let security_token = "opaque+session/token=value";
        let policy = "eyJjb25kaXRpb25zIjpbInNlc3Npb24tdG9rZW4iXX0=";
        let response = RawResponse {
            status: 403,
            headers: Vec::new(),
            body: format!(
                "{access_key}|{}|{security_token}|{}|{policy}|{}",
                spaced_hex(access_key),
                spaced_hex(security_token),
                spaced_hex(policy)
            ),
            body_read_error: None,
        };

        let sanitized = s3_post_response_with_sanitized_body(
            &response,
            access_key,
            &[security_token, ""],
            policy,
        );
        assert!(!sanitized.body.contains(access_key));
        assert!(!sanitized.body.contains(security_token));
        assert!(!sanitized.body.contains(policy));
        assert!(!sanitized.body.contains(&spaced_hex(policy)));
        assert_eq!(
            sanitized.body,
            "SESSION_ACCESS_KEY|SESSION_ACCESS_KEY_BYTES|SESSION_TOKEN|\
             SESSION_TOKEN_BYTES|POST_POLICY|POST_POLICY_BYTES"
        );
    }
}
