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
            assert_shape(
                probe.label,
                response,
                &sts_wire_shape(probe.label, response)
                    .status(400)
                    .header("content-type", "text/xml")
                    .body(format!(
                        "<ErrorResponse xmlns=\"{AWS_FAULT_XMLNS}\">\n  <Error>\n    \
                         <Type>Sender</Type>\n    <Code>{code}</Code>\n    \
                         <Message>{message}</Message>\n  </Error>\n  \
                         <RequestId>{{sts_request_id}}</RequestId>\n</ErrorResponse>\n"
                    )),
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
}
