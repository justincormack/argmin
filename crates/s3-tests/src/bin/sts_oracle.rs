//! Read-only AWS STS Query protocol oracle.
//!
//! This is an explicitly invoked AWS probe rather than an ordinary test
//! binary: Argmin does not expose STS yet, and the repository's local test
//! suite must remain green while Phase 0 pins the AWS wire contract.

use std::env;

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
    Error { code: &'a str, message: &'a str },
    StsError { code: &'a str, message: &'a str },
    Redirect { location: &'a str },
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
            assert_error_probe(probe.label, response, AWS_FAULT_XMLNS, code, message);
        }
        Expected::StsError { code, message } => {
            assert_error_probe(probe.label, response, STS_XMLNS, code, message);
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
    namespace: &str,
    code: &str,
    message: &str,
) {
    assert_shape(
        label,
        response,
        &sts_wire_shape(label, response)
            .status(400)
            .header("content-type", "text/xml")
            .body(format!(
                "<ErrorResponse xmlns=\"{namespace}\">\n  <Error>\n    \
                 <Type>Sender</Type>\n    <Code>{code}</Code>\n    \
                 <Message>{message}</Message>\n  </Error>\n  \
                 <RequestId>{{sts_request_id}}</RequestId>\n</ErrorResponse>\n"
            )),
    );
}

fn form_body(parameters: &[(&str, &str)]) -> String {
    url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(parameters.iter().copied())
        .finish()
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
) {
    let access_key = required_xml_text(response, "AccessKeyId", label);
    let secret_key = required_xml_text(response, "SecretAccessKey", label);
    let session_token = required_xml_text(response, "SessionToken", label);
    let assumed_role_id = required_xml_text(response, "AssumedRoleId", label);

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

fn run_assume_role_probes(
    endpoint: &str,
    credentials: SignedRequestCredentials<'_>,
    account_id: &str,
    role_arn: &str,
    role_name: &str,
    role_session_name: &str,
) {
    let missing_role_arn_body = form_body(&[
        ("Action", "AssumeRole"),
        ("Version", "2011-06-15"),
        ("RoleSessionName", role_session_name),
    ]);
    let missing_role_arn = Probe {
        label: "assume-role-missing-role-arn",
        request: QueryRequest::Post {
            body: &missing_role_arn_body,
            content_type: Some(QUERY_CONTENT_TYPE),
        },
        expected: Expected::StsError {
            code: "ValidationError",
            message: "1 validation error detected: Value null at 'roleArn' failed to satisfy constraint: Member must not be null",
        },
    };
    let response = missing_role_arn.request.send(endpoint, credentials);
    assert_probe(&missing_role_arn, &response, account_id);
    println!("{}: ok", missing_role_arn.label);

    let missing_session_name_body = form_body(&[
        ("Action", "AssumeRole"),
        ("Version", "2011-06-15"),
        ("RoleArn", role_arn),
    ]);
    let missing_session_name = Probe {
        label: "assume-role-missing-session-name",
        request: QueryRequest::Post {
            body: &missing_session_name_body,
            content_type: Some(QUERY_CONTENT_TYPE),
        },
        expected: Expected::StsError {
            code: "ValidationError",
            message: "1 validation error detected: Value null at 'roleSessionName' failed to satisfy constraint: Member must not be null",
        },
    };
    let response = missing_session_name.request.send(endpoint, credentials);
    assert_probe(&missing_session_name, &response, account_id);
    println!("{}: ok", missing_session_name.label);

    let body = form_body(&[
        ("Action", "AssumeRole"),
        ("Version", "2011-06-15"),
        ("RoleArn", role_arn),
        ("RoleSessionName", role_session_name),
    ]);
    let response = QueryRequest::Post {
        body: &body,
        content_type: Some(QUERY_CONTENT_TYPE),
    }
    .send(endpoint, credentials);
    assert_assume_role_success(
        "assume-role-path-bearing-role",
        &response,
        account_id,
        role_name,
        role_session_name,
    );
    println!("assume-role-path-bearing-role: ok");
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
        let role_name = required_env("S3_TEST_STS_ROLE_NAME");
        let role_session_name = required_env("S3_TEST_STS_ROLE_SESSION_NAME");
        run_assume_role_probes(
            &endpoint,
            credentials,
            &account_id,
            &role_arn,
            &role_name,
            &role_session_name,
        );
    }
}
