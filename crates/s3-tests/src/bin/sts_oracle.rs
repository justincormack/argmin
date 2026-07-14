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
    build_test_agent, post_object_raw_to_test_endpoint_with_headers, presign_url_with_credentials,
    send_signed_request_for_service_with_credentials,
    shape::{
        assert_shape, error_response_headers, expected_error, response_header_value, shape,
        xml_tag_text, ShapeSpec,
    },
    sigv4_post_fields_for_credentials, PresignedRequest, RawResponse, SignedRequestCredentials,
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
        self.send_with_security_token(endpoint, credentials, None)
    }

    fn send_with_security_token(
        self,
        endpoint: &str,
        credentials: SignedRequestCredentials<'_>,
        security_token: Option<&str>,
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
                    "sts",
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
    assert_shape(
        label,
        &response,
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

fn build_s3_root_presigned_request(
    endpoint: &str,
    credentials: SignedRequestCredentials<'_>,
    query_token: Option<&str>,
    signed_header_token: Option<&str>,
) -> PresignedRequest {
    let url = query_token.map_or_else(
        || endpoint.to_string(),
        |token| {
            format!(
                "{endpoint}?X-Amz-Security-Token={}",
                auth::canonical::uri_encode(token)
            )
        },
    );
    let headers = signed_header_token
        .map(|token| vec![("x-amz-security-token", token)])
        .unwrap_or_default();
    presign_url_with_credentials(
        "GET",
        &url,
        Duration::from_secs(900),
        headers,
        None,
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
    policy: String,
    signature: String,
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
    assert_shape(
        probe.label,
        &response,
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
    println!("{}: ok", probe.label);
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

fn sign_s3_streaming_request(
    endpoint: &str,
    path: &str,
    decoded_length: usize,
    credentials: SignedRequestCredentials<'_>,
    tokens: S3StreamingTokens<'_>,
    sign_token_header: bool,
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
    let scope = format!("{date}/{}/s3/aws4_request", credentials.region);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        auth::canonical::sha256_hex(canonical_request.as_bytes())
    );
    let secret = auth::SecretKey::new(credentials.secret_key.to_string());
    let signing_key = auth::sigv4::derive_signing_key(&secret, &date, credentials.region, "s3");
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
    let path = format!("/{bucket}/{}", probe.label);
    let signature = sign_s3_streaming_request(
        endpoint,
        &path,
        STREAMING_DATA.len(),
        probe.credentials,
        probe.tokens,
        probe.sign_token_header,
    );
    let (body, first_chunk_signature) =
        build_s3_streaming_body(&signature, STREAMING_DATA, probe.bad_chunk_signature);
    let url = format!("{endpoint}{path}");
    let mut request = build_test_agent(endpoint, None, Duration::from_secs(120))
        .put(&url)
        .header("authorization", &signature.authorization)
        .header("content-encoding", "aws-chunked")
        .header("x-amz-content-sha256", STREAMING_PAYLOAD_HASH)
        .header("x-amz-date", &signature.amz_date)
        .header(
            "x-amz-decoded-content-length",
            STREAMING_DATA.len().to_string(),
        );
    for token in probe.tokens.values().into_iter().flatten() {
        request = request.header("x-amz-security-token", token);
    }
    let mut response = request.send(&body).expect("streaming AWS transport error");
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
        run_s3_presigned_session_authentication_probes(
            &format!("https://s3.{region}.amazonaws.com/"),
            &account_id,
            S3PresignedSessionProbeSet {
                live_credentials: recreated_credentials,
                live_security_token: &recreated_security_token,
                live_role_name: &recreated_role_name,
                live_role_session_name: &recreated_role_session_name,
                other_live_security_token: &other_live_security_token,
                old_credentials: deleted_credentials,
                old_security_token: &deleted_security_token,
            },
        );
        let post_bucket = required_env("S3_TEST_STS_POST_BUCKET");
        run_s3_post_session_authentication_probes(
            &format!("https://s3.{region}.amazonaws.com"),
            &account_id,
            &post_bucket,
            S3PostSessionProbeSet {
                live_credentials: recreated_credentials,
                live_security_token: &recreated_security_token,
                live_role_name: &recreated_role_name,
                live_role_session_name: &recreated_role_session_name,
                other_live_security_token: &other_live_security_token,
                old_credentials: deleted_credentials,
                old_security_token: &deleted_security_token,
            },
        );
        run_s3_streaming_session_authentication_probes(
            &format!("https://s3.{region}.amazonaws.com"),
            &post_bucket,
            S3StreamingSessionProbeSet {
                live_credentials: recreated_credentials,
                live_security_token: &recreated_security_token,
                other_live_security_token: &other_live_security_token,
                old_credentials: deleted_credentials,
                old_security_token: &deleted_security_token,
            },
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
    }
}

#[cfg(test)]
mod tests {
    use super::{
        s3_post_response_with_sanitized_body, s3_response_with_sanitized_body,
        sign_s3_streaming_request, spaced_hex, S3StreamingTokens,
    };
    use s3_tests::{RawResponse, SignedRequestCredentials};

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
