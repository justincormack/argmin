// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use auth::canonical::{
    canonical_headers, canonical_query_string, canonical_request, string_to_sign,
};
use auth::credential::SecretKey;
use auth::sigv4::{derive_signing_key, parse_auth_header};
use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    sign, PayloadChecksumKind, PercentEncodingMode, SignableBody, SignableRequest,
    SignatureLocation, SigningSettings, UriPathNormalizationMode,
};
use aws_sigv4::sign::v4;
use aws_smithy_runtime_api::client::identity::Identity;
use proptest::prelude::*;
use proptest::test_runner::FileFailurePersistence;

const ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const REGION: &str = "us-east-1";
const SERVICE: &str = "s3";
const HOST: &str = "examplebucket.s3.amazonaws.com";
const FIXED_TIMESTAMP: &str = "20250203T040506Z";
const FIXED_DATE: &str = "20250203";
const FIXED_TIME_SECS: u64 = 1_738_555_506;

#[derive(Debug, Clone)]
struct DifferentialRequest {
    method: String,
    path: String,
    query: String,
    extra_headers: Vec<(String, String)>,
    body_hash: String,
}

fn aws_signing_settings() -> SigningSettings {
    let mut settings = SigningSettings::default();
    settings.percent_encoding_mode = PercentEncodingMode::Single;
    settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
    settings.signature_location = SignatureLocation::Headers;
    settings.uri_path_normalization_mode = UriPathNormalizationMode::Disabled;
    settings
}

fn signing_time() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(FIXED_TIME_SECS)
}

fn uri_for_request(req: &DifferentialRequest) -> String {
    if req.query.is_empty() {
        format!("https://{HOST}{}", req.path)
    } else {
        format!("https://{HOST}{}?{}", req.path, req.query)
    }
}

fn input_headers(req: &DifferentialRequest) -> Vec<(&str, &str)> {
    let mut headers = Vec::with_capacity(req.extra_headers.len() + 1);
    headers.push(("host", HOST));
    for (name, value) in &req.extra_headers {
        headers.push((name.as_str(), value.as_str()));
    }
    headers
}

fn signed_headers_for_our_signer(req: &DifferentialRequest) -> Vec<(&str, &str)> {
    let mut headers = input_headers(req);
    headers.push(("x-amz-content-sha256", req.body_hash.as_str()));
    headers.push(("x-amz-date", FIXED_TIMESTAMP));
    headers
}

fn expected_authorization(req: &DifferentialRequest) -> String {
    let signed_headers = signed_headers_for_our_signer(req);
    let canonical_headers = canonical_headers(&signed_headers);

    let mut signed_header_names = signed_headers
        .iter()
        .map(|(name, _)| *name)
        .collect::<Vec<_>>();
    signed_header_names.sort_unstable();
    signed_header_names.dedup();
    let signed_headers_str = signed_header_names.join(";");

    let canonical_query = canonical_query_string(&req.query);
    let creq = canonical_request(
        &req.method,
        &req.path,
        &canonical_query,
        &canonical_headers,
        &signed_headers_str,
        &req.body_hash,
    );
    let scope = format!("{FIXED_DATE}/{REGION}/{SERVICE}/aws4_request");
    let sts = string_to_sign(
        FIXED_TIMESTAMP,
        &scope,
        &auth::canonical::sha256_hex(creq.as_bytes()),
    );
    let signing_key = derive_signing_key(
        &SecretKey::new(SECRET_KEY.to_string()),
        FIXED_DATE,
        REGION,
        SERVICE,
    );
    let signature = v4::calculate_signature(signing_key.as_ref(), sts.as_bytes());

    format!(
        "AWS4-HMAC-SHA256 Credential={ACCESS_KEY}/{scope}, SignedHeaders={signed_headers_str}, Signature={signature}"
    )
}

fn aws_authorization(req: &DifferentialRequest) -> String {
    let identity: Identity = Credentials::new(ACCESS_KEY, SECRET_KEY, None, None, "test").into();
    let params = v4::SigningParams::builder()
        .identity(&identity)
        .region(REGION)
        .name(SERVICE)
        .time(signing_time())
        .settings(aws_signing_settings())
        .build()
        .expect("signing params")
        .into();
    let signable_request = SignableRequest::new(
        &req.method,
        uri_for_request(req),
        input_headers(req).into_iter(),
        SignableBody::Precomputed(req.body_hash.clone()),
    )
    .expect("signable request");
    let signing_output = sign(signable_request, &params).expect("aws signer output");
    let (instructions, _signature) = signing_output.into_parts();
    let authorization = instructions
        .headers()
        .find_map(|(name, value)| {
            name.eq_ignore_ascii_case("authorization")
                .then_some(value.to_string())
        })
        .expect("authorization header");
    authorization
}

fn differential_request_strategy() -> impl Strategy<Value = DifferentialRequest> {
    let method = prop_oneof![
        Just("GET".to_string()),
        Just("HEAD".to_string()),
        Just("POST".to_string()),
        Just("PUT".to_string()),
        Just("DELETE".to_string()),
    ];
    let path =
        proptest::string::string_regex(r"/(?:[A-Za-z0-9._~/-]|%[0-9A-Fa-f]{2}){0,48}").unwrap();
    let query_pairs = proptest::collection::vec(
        (
            proptest::string::string_regex(r"(?:[A-Za-z0-9._~-]|%[0-9A-Fa-f]{2}){1,12}").unwrap(),
            proptest::string::string_regex(r"(?:[A-Za-z0-9._~-]|%[0-9A-Fa-f]{2}){0,12}").unwrap(),
        ),
        0..6,
    );
    let header_name = proptest::string::string_regex(r"[a-z][a-z0-9-]{0,12}").unwrap();
    let header_value = proptest::string::string_regex(r"[A-Za-z0-9 \t._~:/=-]{0,24}").unwrap();
    let extra_headers =
        proptest::collection::vec((header_name, header_value), 0..6).prop_map(|headers| {
            headers
                .into_iter()
                .filter(|(name, _)| {
                    !matches!(
                        name.as_str(),
                        "host"
                            | "authorization"
                            | "x-amz-date"
                            | "x-amz-content-sha256"
                            | "transfer-encoding"
                            | "user-agent"
                            | "x-amzn-trace-id"
                    )
                })
                .collect::<Vec<_>>()
        });
    let body_hash = prop_oneof![
        Just("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_string()),
        proptest::string::string_regex(r"[0-9a-f]{64}").unwrap(),
    ];

    (method, path, query_pairs, extra_headers, body_hash).prop_map(
        |(method, path, query_pairs, extra_headers, body_hash)| DifferentialRequest {
            method,
            path,
            query: query_pairs
                .into_iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join("&"),
            extra_headers,
            body_hash,
        },
    )
}

#[test]
fn aws_sigv4_header_signing_matches_s3_example_request() {
    let req = DifferentialRequest {
        method: "PUT".to_string(),
        path: "/test%24file.text".to_string(),
        query: "partNumber=1&uploadId=abc%2Fdef".to_string(),
        extra_headers: vec![
            (
                "date".to_string(),
                "Fri, 24 May 2013 00:00:00 GMT".to_string(),
            ),
            (
                "x-amz-storage-class".to_string(),
                "REDUCED_REDUNDANCY".to_string(),
            ),
            (
                "x-amz-meta-desc".to_string(),
                "  hello   world\tfrom  argmin ".to_string(),
            ),
        ],
        body_hash: "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072".to_string(),
    };

    let expected = expected_authorization(&req);
    let aws = aws_authorization(&req);
    assert_eq!(aws, expected);

    let parsed = parse_auth_header(&aws).expect("parse auth header");
    assert_eq!(parsed.credential.access_key_id, ACCESS_KEY);
    assert_eq!(parsed.credential.date, FIXED_DATE);
    assert_eq!(parsed.credential.region, REGION);
    assert_eq!(parsed.credential.service, SERVICE);
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        failure_persistence: Some(Box::new(FileFailurePersistence::WithSource("regressions"))),
        .. ProptestConfig::default()
    })]

    #[test]
    fn aws_sigv4_header_signing_matches_argmin_canonicalization(
        req in differential_request_strategy()
    ) {
        let expected = expected_authorization(&req);
        let aws = aws_authorization(&req);
        prop_assert_eq!(&aws, &expected);

        let parsed = parse_auth_header(&aws).expect("parse auth header");
        prop_assert_eq!(parsed.credential.access_key_id, ACCESS_KEY);
        prop_assert_eq!(parsed.credential.date, FIXED_DATE);
        prop_assert_eq!(parsed.credential.region, REGION);
        prop_assert_eq!(parsed.credential.service, SERVICE);
    }
}
