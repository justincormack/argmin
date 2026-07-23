use aws_smithy_types::{date_time::Format as DateTimeFormat, DateTime};
use s3_tests::{
    send_checked_signed_request_for_service_with_credentials,
    shape::{assert_shape, response_header_value, shape, xml_tag_text},
    RawResponse, SigningService,
};
use sts_tests::CTX;

const STS_XMLNS: &str = "https://sts.amazonaws.com/doc/2011-06-15/";
const QUERY_CONTENT_TYPE: &str = "application/x-www-form-urlencoded";

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

fn assert_assume_role_success(
    role_session_name: &str,
    parameters: &[(&str, &str)],
    expected_duration_seconds: i64,
) {
    let mut form = url::form_urlencoded::Serializer::new(String::new());
    for (name, value) in parameters {
        form.append_pair(name, value);
    }
    let body = form.finish();
    let response = send_checked_signed_request_for_service_with_credentials(
        "POST",
        &format!("{}/", CTX.endpoint()),
        body.as_bytes(),
        [("content-type", QUERY_CONTENT_TYPE)],
        SigningService::Sts,
        "sts",
        CTX.credentials(),
    );

    let label = format!("AssumeRole {role_session_name}");
    assert_issued_credential_shapes(&label, &response);
    assert_expiration(&label, &response, expected_duration_seconds);
    assert_success_wire_shape(&label, response, role_session_name);
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
