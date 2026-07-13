//! Read-only AWS STS Query protocol oracle.
//!
//! This is an explicitly invoked AWS probe rather than an ordinary test
//! binary: Argmin does not expose STS yet, and the repository's local test
//! suite must remain green while Phase 0 pins the AWS wire contract.

use std::env;

use aws_smithy_types::{date_time::Format as DateTimeFormat, DateTime};
use s3_tests::{
    send_signed_request_for_service_with_credentials,
    shape::{assert_shape, response_header_value, shape, xml_tag_text, ShapeSpec},
    RawResponse, SignedRequestCredentials,
};

const QUERY_CONTENT_TYPE: &str = "application/x-www-form-urlencoded";
const STS_XMLNS: &str = "https://sts.amazonaws.com/doc/2011-06-15/";
const AWS_FAULT_XMLNS: &str = "http://webservices.amazon.com/AWSFault/2005-15-09";

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
        match self {
            Self::Get(query) => send_signed_request_for_service_with_credentials(
                "GET",
                &format!("{endpoint}/?{query}"),
                b"",
                std::iter::empty::<(&str, &str)>(),
                "sts",
                credentials,
            ),
            Self::Post { body, content_type } => {
                let headers = content_type
                    .map(|value| vec![("content-type", value)])
                    .unwrap_or_default();
                send_signed_request_for_service_with_credentials(
                    "POST",
                    &format!("{endpoint}/"),
                    body.as_bytes(),
                    headers,
                    "sts",
                    credentials,
                )
            }
        }
    }
}

fn required_env(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("{name} must be set by scripts/aws-sts-oracle"))
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

fn assert_probe(probe: &Probe<'_>, response: &RawResponse, account_id: &str) {
    match probe.expected {
        Expected::Success => {
            let arn = xml_tag_text(&response.body, "Arn")
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| panic!("{}: missing Arn", probe.label));
            let user_id = xml_tag_text(&response.body, "UserId")
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| panic!("{}: missing UserId", probe.label));
            assert_shape(
                probe.label,
                response,
                &sts_wire_shape(probe.label, response)
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
    assert_eq!(
        expiration.secs() - response_date.secs(),
        duration_seconds,
        "{label}: Expiration does not match the requested session duration"
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
                 <Expiration>{{iso8601}}</Expiration>\n    </Credentials>\n  \
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
    );
    println!("{label}: ok");
}

struct AssumeRoleProbeSet<'a> {
    caller_arn: &'a str,
    role_arn: &'a str,
    role_name: &'a str,
    default_max_role_arn: &'a str,
    default_max_role_name: &'a str,
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
    );
    println!("assume-role-cross-account-success: ok");
}

fn main() {
    let endpoint = required_env("S3_TEST_STS_ENDPOINT");
    assert!(
        endpoint.starts_with("https://"),
        "S3_TEST_STS_ENDPOINT must be an HTTPS AWS STS endpoint"
    );
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

    if let Ok(role_arn) = env::var("S3_TEST_STS_ROLE_ARN") {
        let caller_arn = required_env("S3_TEST_STS_PRIMARY_ARN");
        let role_name = required_env("S3_TEST_STS_ROLE_NAME");
        let default_max_role_arn = required_env("S3_TEST_STS_DEFAULT_MAX_ROLE_ARN");
        let default_max_role_name = required_env("S3_TEST_STS_DEFAULT_MAX_ROLE_NAME");
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
                role_session_name: &role_session_name,
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
}
