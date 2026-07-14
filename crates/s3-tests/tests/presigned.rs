use std::time::Duration;

use auth::canonical::{canonical_query_string, canonical_request, string_to_sign};
use auth::credential::SecretKey;
use aws_sdk_s3::primitives::ByteStream;
use ring::hmac;
use s3_tests::{
    create_public_bucket, object_url, presign_url_with_credentials,
    presign_url_without_host_signed_header, raw_fetch_url,
    send_signed_request_with_unsigned_headers, send_signed_request_without_host_signed_header,
    shape::{assert_shape, assert_status_and_body, error_response_headers, expected_error, shape},
    sse_c_header_values, test_sse_c_key, unique_account_regional_bucket, unique_bucket,
    PresignedRequest, SignedRequestCredentials, CTX,
};

const NO_HEADERS: [(&str, &str); 0] = [];

/// Create a bucket, returning its name.
async fn setup_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    bucket
}

async fn setup_sse_c_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    s3_tests::create_bucket_with_sse_c_enabled(client, &bucket)
        .await
        .unwrap();
    bucket
}

/// Build an agent that returns all HTTP responses (including 4xx/5xx) as Ok.
fn agent() -> s3_tests::Agent {
    s3_tests::test_agent()
}

fn endpoint_is_https() -> bool {
    CTX.endpoint().starts_with("https://")
}

fn require_https_endpoint() {
    assert!(
        endpoint_is_https(),
        "presigned SSE-C coverage requires an https:// endpoint; got {}",
        CTX.endpoint()
    );
}

macro_rules! with_presigned_headers {
    ($req:expr, $presigned:expr) => {{
        let mut req = $req;
        for (name, value) in $presigned.headers() {
            req = req.header(name, value);
        }
        req
    }};
}

fn sha256_hex(data: &[u8]) -> String {
    auth::canonical::sha256_hex(data)
}

fn days_to_ymd(days: u64) -> (u64, u64, u64) {
    let z = days + 719468;
    let era = z / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

fn format_amz_date(epoch_secs: u64) -> String {
    let days = epoch_secs / 86_400;
    let seconds = epoch_secs % 86_400;
    let (year, month, day) = days_to_ymd(days);
    let hour = seconds / 3_600;
    let minute = (seconds % 3_600) / 60;
    let second = seconds % 60;
    format!("{year:04}{month:02}{day:02}T{hour:02}{minute:02}{second:02}Z")
}

fn primary_credentials() -> SignedRequestCredentials<'static> {
    SignedRequestCredentials {
        access_key: CTX.access_key(),
        secret_key: CTX.secret_key(),
        region: CTX.region(),
        tls_ca_pem: CTX.tls_ca_pem(),
    }
}

fn alt_credentials() -> SignedRequestCredentials<'static> {
    SignedRequestCredentials {
        access_key: CTX.alt_access_key(),
        secret_key: CTX.alt_secret_key(),
        region: CTX.region(),
        tls_ca_pem: CTX.tls_ca_pem(),
    }
}

struct HeaderAuthorization {
    authorization: String,
    date: String,
}

/// Sign a request that already contains a complete presigned query string.
///
/// Date-only header signing avoids adding an unsigned `x-amz-date` header to
/// the independently valid presigned request. `UNSIGNED-PAYLOAD` is both the
/// header canonical payload value and the presigned canonical payload value.
fn header_authorization_for_presigned_url(
    credentials: SignedRequestCredentials<'_>,
    method: &str,
    url: &str,
) -> HeaderAuthorization {
    let parsed = url::Url::parse(url).expect("parse presigned URL");
    let path = parsed.path();
    let query = canonical_query_string(parsed.query().unwrap_or(""));
    let host = parsed
        .host_str()
        .map(|host| {
            if let Some(port) = parsed.port() {
                format!("{host}:{port}")
            } else {
                host.to_string()
            }
        })
        .expect("presigned URL has host");
    let epoch_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let date = format_amz_date(epoch_secs);
    let date_stamp = &date[..8];
    let signed_headers = "date;host";
    let canonical_headers = format!("date:{date}\nhost:{host}\n");
    let canonical_request = canonical_request(
        method,
        path,
        &query,
        &canonical_headers,
        signed_headers,
        "UNSIGNED-PAYLOAD",
    );
    let scope = format!("{date_stamp}/{}/s3/aws4_request", credentials.region);
    let string_to_sign = string_to_sign(&date, &scope, &sha256_hex(canonical_request.as_bytes()));
    let signing_key = auth::sigv4::derive_signing_key(
        &SecretKey::new(credentials.secret_key.to_string()),
        date_stamp,
        credentials.region,
        "s3",
    );
    let signature = hmac::sign(
        &hmac::Key::new(hmac::HMAC_SHA256, signing_key.as_ref()),
        string_to_sign.as_bytes(),
    )
    .as_ref()
    .iter()
    .map(|byte| format!("{byte:02x}"))
    .collect::<String>();
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        credentials.access_key
    );

    HeaderAuthorization {
        authorization,
        date,
    }
}

fn authorization_with_bad_signature(authorization: &str) -> String {
    let (prefix, _) = authorization
        .split_once("Signature=")
        .expect("authorization header contains Signature");
    format!("{prefix}Signature={}", "0".repeat(64))
}

fn presign_object_with_credentials<K, V, I>(
    credentials: SignedRequestCredentials<'_>,
    method: &str,
    object: (&str, &str),
    query: Option<&str>,
    expires: Duration,
    extra_headers: I,
    payload_hash: Option<&str>,
) -> PresignedRequest
where
    K: AsRef<str>,
    V: AsRef<str>,
    I: IntoIterator<Item = (K, V)>,
{
    presign_url_with_credentials(
        method,
        &object_url(CTX.endpoint(), object.0, object.1, query),
        expires,
        extra_headers,
        payload_hash,
        credentials,
    )
}

fn presign_object_without_host_signed_header<K, V, I>(
    method: &str,
    bucket: &str,
    key: &str,
    expires: Duration,
    extra_headers: I,
    payload_hash: Option<&str>,
) -> PresignedRequest
where
    K: AsRef<str>,
    V: AsRef<str>,
    I: IntoIterator<Item = (K, V)>,
{
    presign_url_without_host_signed_header(
        method,
        &object_url(CTX.endpoint(), bucket, key, None),
        expires,
        extra_headers,
        payload_hash,
        primary_credentials(),
    )
}

fn presign_object<K, V, I>(
    method: &str,
    bucket: &str,
    key: &str,
    query: Option<&str>,
    expires: Duration,
    extra_headers: I,
    payload_hash: Option<&str>,
) -> PresignedRequest
where
    K: AsRef<str>,
    V: AsRef<str>,
    I: IntoIterator<Item = (K, V)>,
{
    presign_object_with_credentials(
        primary_credentials(),
        method,
        (bucket, key),
        query,
        expires,
        extra_headers,
        payload_hash,
    )
}

/// Cleanup helper.
async fn cleanup_with_client(client: &aws_sdk_s3::Client, bucket: &str, keys: &[&str]) {
    for key in keys {
        let _ = s3_tests::delete_object_retrying_operation_aborted(client, bucket, key).await;
    }
    s3_tests::delete_bucket_retrying_operation_aborted(client, bucket).await;
}

async fn cleanup(bucket: &str, keys: &[&str]) {
    cleanup_with_client(CTX.client(), bucket, keys).await;
}

fn assert_headers_not_signed_error(status: u16, body: &str, expected_headers: &str) {
    assert_status_and_body(
        "headers not signed",
        status,
        body,
        &shape()
            .status(403)
            .body(expected_error::headers_not_signed(expected_headers)),
    );
}

fn assert_invalid_token_error(status: u16, body: &str, token: &str) {
    assert_status_and_body(
        "invalid token",
        status,
        body,
        &shape().status(400).body(expected_error::invalid_token(
            "The provided token is malformed or otherwise invalid.",
            token,
        )),
    );
}

/// The full SignatureDoesNotMatch body: the message plus AWS's echo of the
/// request's own canonical form. The signing inputs vary per request, so
/// they are pinned as non-empty `{any}` except the caller-known signature.
const SIGNATURE_MISMATCH_BODY: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
     <Error><Code>SignatureDoesNotMatch</Code>\
     <Message>The request signature we calculated does not match the \
     signature you provided. Check your key and signing method.</Message>\
     <AWSAccessKeyId>{access_key}</AWSAccessKeyId>\
     <StringToSign>{any}</StringToSign>\
     <SignatureProvided>{signature_provided}</SignatureProvided>\
     <StringToSignBytes>{any}</StringToSignBytes>\
     <CanonicalRequest>{any}</CanonicalRequest>\
     <CanonicalRequestBytes>{any}</CanonicalRequestBytes>\
     <RequestId>{request_id}</RequestId>\
     <HostId>{host_id}</HostId></Error>";

fn assert_signature_does_not_match(status: u16, body: &str, signature_provided: &str) {
    assert_status_and_body(
        "signature does not match",
        status,
        body,
        &shape()
            .status(403)
            .sub("access_key", CTX.access_key())
            .sub("signature_provided", signature_provided)
            .body(SIGNATURE_MISMATCH_BODY),
    );
}

fn presigned_url_with_bad_signature(url: &str) -> String {
    let (prefix, _) = url
        .split_once("X-Amz-Signature=")
        .expect("presigned URL contains X-Amz-Signature");
    format!("{prefix}X-Amz-Signature={}", "0".repeat(64))
}

fn resign_presigned_url(url: &str, credentials: SignedRequestCredentials<'_>) -> String {
    let parsed = url::Url::parse(url).expect("parse presigned URL");
    let raw_query = parsed.query().unwrap_or("");
    let query_without_signature = raw_query
        .split('&')
        .filter(|part| !part.starts_with("X-Amz-Signature="))
        .collect::<Vec<_>>()
        .join("&");
    let canonical_query = canonical_query_string(&query_without_signature);
    let amz_date = parsed
        .query_pairs()
        .find_map(|(name, value)| (name == "X-Amz-Date").then(|| value.into_owned()))
        .expect("presigned URL contains X-Amz-Date");
    let date_stamp = &amz_date[..8];
    let host = parsed
        .host_str()
        .map(|host| {
            if let Some(port) = parsed.port() {
                format!("{host}:{port}")
            } else {
                host.to_string()
            }
        })
        .expect("presigned URL has host");
    let canonical_headers = format!("host:{host}\n");
    let canonical_request = canonical_request(
        "GET",
        parsed.path(),
        &canonical_query,
        &canonical_headers,
        "host",
        "UNSIGNED-PAYLOAD",
    );
    let scope = format!("{date_stamp}/{}/s3/aws4_request", credentials.region);
    let string_to_sign =
        string_to_sign(&amz_date, &scope, &sha256_hex(canonical_request.as_bytes()));
    let signing_key = auth::sigv4::derive_signing_key(
        &SecretKey::new(credentials.secret_key.to_string()),
        date_stamp,
        credentials.region,
        "s3",
    );
    let signature = hmac::sign(
        &hmac::Key::new(hmac::HMAC_SHA256, signing_key.as_ref()),
        string_to_sign.as_bytes(),
    )
    .as_ref()
    .iter()
    .map(|byte| format!("{byte:02x}"))
    .collect::<String>();

    format!(
        "{}{}?{canonical_query}&X-Amz-Signature={signature}",
        parsed.origin().ascii_serialization(),
        parsed.path()
    )
}

fn duplicate_presigned_query_parameter(
    url: &str,
    parameter: &str,
    credentials: SignedRequestCredentials<'_>,
) -> String {
    if parameter == "X-Amz-Signature" {
        let resigned = resign_presigned_url(url, credentials);
        let signature = resigned
            .split_once("X-Amz-Signature=")
            .map(|(_, value)| value)
            .expect("resigned URL contains signature");
        return format!("{resigned}&X-Amz-Signature={signature}");
    }

    let parsed = url::Url::parse(url).expect("parse presigned URL");
    let query = parsed.query().unwrap_or("");
    let pair = query
        .split('&')
        .find(|part| {
            part.split_once('=')
                .is_some_and(|(name, _)| name == parameter)
        })
        .unwrap_or_else(|| panic!("presigned URL contains {parameter}"));
    let without_signature = query
        .split('&')
        .filter(|part| !part.starts_with("X-Amz-Signature="))
        .collect::<Vec<_>>()
        .join("&");
    let duplicated = format!(
        "{}{}?{without_signature}&{pair}",
        parsed.origin().ascii_serialization(),
        parsed.path()
    );
    resign_presigned_url(&duplicated, credentials)
}

fn add_conflicting_presigned_query_parameter(
    url: &str,
    parameter: &str,
    value: &str,
    prepend: bool,
    credentials: SignedRequestCredentials<'_>,
) -> String {
    let parsed = url::Url::parse(url).expect("parse presigned URL");
    let without_signature = parsed
        .query()
        .unwrap_or("")
        .split('&')
        .filter(|part| !part.starts_with("X-Amz-Signature="))
        .collect::<Vec<_>>()
        .join("&");
    let encoded_value = url::form_urlencoded::byte_serialize(value.as_bytes()).collect::<String>();
    let conflicting_pair = format!("{parameter}={encoded_value}");
    let query = if prepend {
        format!("{conflicting_pair}&{without_signature}")
    } else {
        format!("{without_signature}&{conflicting_pair}")
    };
    let unsigned_url = format!(
        "{}{}?{query}",
        parsed.origin().ascii_serialization(),
        parsed.path()
    );
    let resigned = resign_presigned_url(&unsigned_url, credentials);

    if parameter != "X-Amz-Signature" {
        let good_signature = resigned
            .split_once("X-Amz-Signature=")
            .map(|(_, signature)| signature)
            .expect("resigned URL contains signature");
        return format!(
            "{}{}?{query}&X-Amz-Signature={good_signature}",
            parsed.origin().ascii_serialization(),
            parsed.path()
        );
    }

    let (prefix, good_signature) = resigned
        .split_once("X-Amz-Signature=")
        .expect("resigned URL contains signature");
    if prepend {
        format!("{prefix}X-Amz-Signature={encoded_value}&X-Amz-Signature={good_signature}")
    } else {
        format!("{resigned}&X-Amz-Signature={encoded_value}")
    }
}

fn get_with_header_and_presigned_auth(
    url: &str,
    header_credentials: SignedRequestCredentials<'_>,
    invalidate_header: bool,
) -> (u16, String) {
    let signed = header_authorization_for_presigned_url(header_credentials, "GET", url);
    let authorization = if invalidate_header {
        authorization_with_bad_signature(&signed.authorization)
    } else {
        signed.authorization
    };
    let mut response = agent()
        .get(url)
        .header("Authorization", &authorization)
        .header("Date", &signed.date)
        .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
        .call()
        .expect("transport error");
    let status = response.status().as_u16();
    let body = response.body_mut().read_to_string().unwrap_or_default();
    (status, body)
}

fn assert_multiple_auth_rejected(case: &str, status: u16, body: &str) {
    assert_eq!(status, 400, "{case}: expected 400, got {status}: {body}");
    assert!(
        body.contains("<Code>InvalidArgument</Code>"),
        "{case}: expected InvalidArgument, got: {body}"
    );
    assert!(
        body.contains(
            "<Message>Only one auth mechanism allowed; only the X-Amz-Algorithm query parameter, Signature query string parameter or the Authorization header should be specified</Message>"
        ),
        "{case}: unexpected error message: {body}"
    );
    assert!(
        body.contains("<ArgumentName>Authorization</ArgumentName>"),
        "{case}: expected Authorization argument name, got: {body}"
    );
    assert!(
        body.contains("<ArgumentValue>AWS4-HMAC-SHA256 Credential="),
        "{case}: expected echoed Authorization value, got: {body}"
    );
}

fn presign_object_with_fixed_amz_date(
    credentials: SignedRequestCredentials<'_>,
    method: &str,
    bucket: &str,
    key: &str,
    expires: Duration,
    amz_date: &str,
) -> String {
    let date_stamp = &amz_date[..8];
    let url = object_url(CTX.endpoint(), bucket, key, None);
    let parsed = url::Url::parse(&url).expect("parse object URL");
    let path = parsed.path();
    let host = parsed
        .host_str()
        .map(|host| {
            if let Some(port) = parsed.port() {
                format!("{host}:{port}")
            } else {
                host.to_string()
            }
        })
        .expect("object URL has host");
    let signed_headers = "host";
    let canonical_headers = format!("host:{host}\n");
    let credential = format!(
        "{}/{}/{}/s3/aws4_request",
        credentials.access_key, date_stamp, credentials.region
    );
    let raw_query = [
        "X-Amz-Algorithm=AWS4-HMAC-SHA256".to_string(),
        format!("X-Amz-Credential={credential}"),
        format!("X-Amz-Date={amz_date}"),
        format!("X-Amz-Expires={}", expires.as_secs()),
        format!("X-Amz-SignedHeaders={signed_headers}"),
    ]
    .join("&");
    let canonical_query = canonical_query_string(&raw_query);
    let canonical_request = canonical_request(
        method,
        path,
        &canonical_query,
        &canonical_headers,
        signed_headers,
        "UNSIGNED-PAYLOAD",
    );
    let scope = format!("{date_stamp}/{}/s3/aws4_request", credentials.region);
    let string_to_sign =
        string_to_sign(amz_date, &scope, &sha256_hex(canonical_request.as_bytes()));
    let signing_key = auth::sigv4::derive_signing_key(
        &SecretKey::new(credentials.secret_key.to_string()),
        date_stamp,
        credentials.region,
        "s3",
    );
    let signature = hmac::sign(
        &hmac::Key::new(hmac::HMAC_SHA256, signing_key.as_ref()),
        string_to_sign.as_bytes(),
    )
    .as_ref()
    .iter()
    .map(|byte| format!("{byte:02x}"))
    .collect::<String>();

    format!(
        "{}{}?{}&X-Amz-Signature={signature}",
        parsed.origin().ascii_serialization(),
        path,
        canonical_query
    )
}

fn presign_object_with_credential_scope(
    method: &str,
    bucket: &str,
    key: &str,
    region: &str,
    service: &str,
) -> String {
    let url = object_url(CTX.endpoint(), bucket, key, None);
    let parsed = url::Url::parse(&url).expect("parse object URL");
    let path = parsed.path();
    let host = parsed
        .host_str()
        .map(|host| {
            if let Some(port) = parsed.port() {
                format!("{host}:{port}")
            } else {
                host.to_string()
            }
        })
        .expect("object URL has host");
    let expires = Duration::from_secs(900);
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let amz_date = format_amz_date(secs);
    let date_stamp = &amz_date[..8];
    let signed_headers = "host";
    let canonical_headers = format!("host:{host}\n");
    let credential = format!(
        "{}/{date_stamp}/{region}/{service}/aws4_request",
        CTX.access_key()
    );
    let raw_query = [
        "X-Amz-Algorithm=AWS4-HMAC-SHA256".to_string(),
        format!("X-Amz-Credential={credential}"),
        format!("X-Amz-Date={amz_date}"),
        format!("X-Amz-Expires={}", expires.as_secs()),
        format!("X-Amz-SignedHeaders={signed_headers}"),
    ]
    .join("&");
    let canonical_query = canonical_query_string(&raw_query);
    let canonical_request = canonical_request(
        method,
        path,
        &canonical_query,
        &canonical_headers,
        signed_headers,
        "UNSIGNED-PAYLOAD",
    );
    let scope = format!("{date_stamp}/{region}/{service}/aws4_request");
    let string_to_sign =
        string_to_sign(&amz_date, &scope, &sha256_hex(canonical_request.as_bytes()));
    let signing_key = auth::sigv4::derive_signing_key(
        &SecretKey::new(CTX.secret_key().to_string()),
        date_stamp,
        region,
        service,
    );
    let signature = hmac::sign(
        &hmac::Key::new(hmac::HMAC_SHA256, signing_key.as_ref()),
        string_to_sign.as_bytes(),
    )
    .as_ref()
    .iter()
    .map(|byte| format!("{byte:02x}"))
    .collect::<String>();

    format!(
        "{}{}?{}&X-Amz-Signature={signature}",
        parsed.origin().ascii_serialization(),
        path,
        canonical_query
    )
}

#[test]
fn test_simultaneous_header_and_presigned_query_auth_rejected() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "mixed-header-presigned-auth";
        let object = b"header-selected-principal";
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(object))
            .send()
            .await
            .unwrap();

        let primary_query = presign_object_with_credentials(
            primary_credentials(),
            "GET",
            (&bucket, key),
            None,
            Duration::from_secs(900),
            NO_HEADERS,
            None,
        );

        let (status, body) =
            get_with_header_and_presigned_auth(primary_query.uri(), primary_credentials(), false);
        assert_multiple_auth_rejected("both valid, same principal", status, &body);

        let bad_query_url = presigned_url_with_bad_signature(primary_query.uri());
        let (status, body) =
            get_with_header_and_presigned_auth(&bad_query_url, primary_credentials(), false);
        assert_multiple_auth_rejected("valid header, invalid query", status, &body);

        let (status, body) =
            get_with_header_and_presigned_auth(primary_query.uri(), primary_credentials(), true);
        assert_multiple_auth_rejected("invalid header, valid query", status, &body);

        let alt_query = presign_object_with_credentials(
            alt_credentials(),
            "GET",
            (&bucket, key),
            None,
            Duration::from_secs(900),
            NO_HEADERS,
            None,
        );
        let (status, body) =
            get_with_header_and_presigned_auth(alt_query.uri(), primary_credentials(), false);
        assert_multiple_auth_rejected(
            "authorized header principal, denied query principal",
            status,
            &body,
        );

        let (status, body) =
            get_with_header_and_presigned_auth(primary_query.uri(), alt_credentials(), false);
        assert_multiple_auth_rejected(
            "denied header principal, authorized query principal",
            status,
            &body,
        );

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_duplicate_presigned_auth_query_parameters_use_first_value() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "duplicate-presigned-auth-query";
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"duplicate presigned auth query"))
            .send()
            .await
            .unwrap();
        let presigned = presign_object(
            "GET",
            &bucket,
            key,
            None,
            Duration::from_secs(900),
            NO_HEADERS,
            None,
        );

        for parameter in [
            "X-Amz-Algorithm",
            "X-Amz-Credential",
            "X-Amz-Date",
            "X-Amz-Expires",
            "X-Amz-SignedHeaders",
            "X-Amz-Signature",
        ] {
            let url = duplicate_presigned_query_parameter(
                presigned.uri(),
                parameter,
                primary_credentials(),
            );
            let mut response = agent().get(&url).call().expect("transport error");
            let status = response.status().as_u16();
            let body = response.body_mut().read_to_string().unwrap_or_default();
            assert_eq!(
                status, 200,
                "identical duplicate {parameter} failed: {body}"
            );
            assert_eq!(body, "duplicate presigned auth query");
        }

        for (parameter, conflicting_value) in [
            ("X-Amz-Algorithm", "NOT-A-SIGNING-ALGORITHM"),
            ("X-Amz-Credential", "INVALID-CREDENTIAL"),
            ("X-Amz-Date", "20000101T000000Z"),
            ("X-Amz-Expires", "0"),
            ("X-Amz-SignedHeaders", "host;x-not-present"),
            ("X-Amz-Signature", "0"),
        ] {
            for prepend in [false, true] {
                let url = add_conflicting_presigned_query_parameter(
                    presigned.uri(),
                    parameter,
                    conflicting_value,
                    prepend,
                    primary_credentials(),
                );
                let mut response = agent().get(&url).call().expect("transport error");
                let status = response.status().as_u16();
                let body = response.body_mut().read_to_string().unwrap_or_default();
                if !prepend {
                    assert_eq!(
                        status, 200,
                        "conflicting second {parameter} was not ignored: {body}"
                    );
                    assert_eq!(body, "duplicate presigned auth query");
                    continue;
                }

                match parameter {
                    "X-Amz-Algorithm" | "X-Amz-Credential" => {
                        assert_eq!(status, 400, "conflicting first {parameter}: {body}");
                        assert!(
                            body.contains("<Code>AuthorizationQueryParametersError</Code>"),
                            "conflicting first {parameter}: {body}"
                        );
                    }
                    "X-Amz-Date" | "X-Amz-Expires" => {
                        assert_eq!(status, 403, "conflicting first {parameter}: {body}");
                        assert!(
                            body.contains("<Code>AccessDenied</Code>")
                                && body.contains("<Message>Request has expired</Message>"),
                            "conflicting first {parameter}: {body}"
                        );
                    }
                    "X-Amz-SignedHeaders" | "X-Amz-Signature" => {
                        assert_eq!(status, 403, "conflicting first {parameter}: {body}");
                        assert!(
                            body.contains("<Code>SignatureDoesNotMatch</Code>"),
                            "conflicting first {parameter}: {body}"
                        );
                    }
                    _ => unreachable!(),
                }
            }
        }

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_header_sigv4_requires_host_signed_header() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"header sigv4 missing signed host";

        client
            .put_object()
            .bucket(&bucket)
            .key("host-header-auth")
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();

        let response = send_signed_request_without_host_signed_header(
            "GET",
            &object_url(CTX.endpoint(), &bucket, "host-header-auth", None),
            b"",
            NO_HEADERS,
            primary_credentials(),
        );
        assert_headers_not_signed_error(response.status, &response.body, "host");

        cleanup(&bucket, &["host-header-auth"]).await;
    });
}

#[test]
fn test_presigned_sigv4_requires_host_signed_header() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"presigned sigv4 missing signed host";

        client
            .put_object()
            .bucket(&bucket)
            .key("host-presigned-auth")
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();

        let payload_hash = sha256_hex(b"");
        let presigned = presign_object_without_host_signed_header(
            "GET",
            &bucket,
            "host-presigned-auth",
            Duration::from_secs(900),
            NO_HEADERS,
            Some(&payload_hash),
        );

        let mut response = with_presigned_headers!(agent().get(presigned.uri()), presigned)
            .call()
            .expect("transport error");
        let status = response.status().as_u16();
        let body = response.body_mut().read_to_string().unwrap_or_default();
        assert_headers_not_signed_error(status, &body, "host");

        cleanup(&bucket, &["host-presigned-auth"]).await;
    });
}

#[test]
fn test_header_sigv4_unsigned_amz_header_reports_headers_not_signed() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"header sigv4 unsigned amz header";

        client
            .put_object()
            .bucket(&bucket)
            .key("unsigned-amz-header-auth")
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();

        let response = send_signed_request_with_unsigned_headers(
            "GET",
            &object_url(CTX.endpoint(), &bucket, "unsigned-amz-header-auth", None),
            b"",
            NO_HEADERS,
            &[("x-amz-meta-unsigned", "value")],
            primary_credentials(),
        );
        assert_headers_not_signed_error(response.status, &response.body, "x-amz-meta-unsigned");

        cleanup(&bucket, &["unsigned-amz-header-auth"]).await;
    });
}

#[test]
fn test_presigned_sigv4_unsigned_amz_header_reports_headers_not_signed() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"presigned sigv4 unsigned amz header";

        client
            .put_object()
            .bucket(&bucket)
            .key("unsigned-amz-presigned-auth")
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();

        let presigned = presign_object(
            "GET",
            &bucket,
            "unsigned-amz-presigned-auth",
            None,
            Duration::from_secs(900),
            NO_HEADERS,
            None,
        );

        let mut response = with_presigned_headers!(agent().get(presigned.uri()), presigned)
            .header("x-amz-meta-unsigned", "value")
            .call()
            .expect("transport error");
        let status = response.status().as_u16();
        let body = response.body_mut().read_to_string().unwrap_or_default();
        assert_headers_not_signed_error(status, &body, "x-amz-meta-unsigned");

        cleanup(&bucket, &["unsigned-amz-presigned-auth"]).await;
    });
}

#[test]
fn test_presigned_sigv4_unsigned_security_token_reports_headers_not_signed() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"presigned sigv4 unsigned security token";

        client
            .put_object()
            .bucket(&bucket)
            .key("unsigned-token-presigned-auth")
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();

        let presigned = presign_object(
            "GET",
            &bucket,
            "unsigned-token-presigned-auth",
            None,
            Duration::from_secs(900),
            NO_HEADERS,
            None,
        );

        let mut response = with_presigned_headers!(agent().get(presigned.uri()), presigned)
            .header("x-amz-security-token", "unsigned-token")
            .call()
            .expect("transport error");
        let status = response.status().as_u16();
        let body = response.body_mut().read_to_string().unwrap_or_default();
        assert_headers_not_signed_error(status, &body, "x-amz-security-token");

        cleanup(&bucket, &["unsigned-token-presigned-auth"]).await;
    });
}

#[test]
fn test_presigned_sigv4_signed_security_token_rejected_for_static_credentials() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "signed-token-presigned-auth";
        let body = b"presigned sigv4 signed security token";

        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();

        let token = "signed-token-causes-400";
        let presigned = presign_object(
            "GET",
            &bucket,
            key,
            None,
            Duration::from_secs(900),
            [("x-amz-security-token", token)],
            None,
        );

        let mut response = with_presigned_headers!(agent().get(presigned.uri()), presigned)
            .call()
            .expect("transport error");
        let status = response.status().as_u16();
        let body = response.body_mut().read_to_string().unwrap_or_default();
        assert_invalid_token_error(status, &body, token);

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_presigned_sigv4_bad_signature_with_signed_security_token_reports_signature_mismatch() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "bad-signature-signed-token-presigned-auth";
        let body = b"presigned sigv4 bad signature signed security token";

        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();

        let token = "bad-signature-signed-token-causes-400";
        let presigned = presign_object(
            "GET",
            &bucket,
            key,
            None,
            Duration::from_secs(900),
            [("x-amz-security-token", token)],
            None,
        );
        let tampered_url = presigned_url_with_bad_signature(presigned.uri());

        let mut response = with_presigned_headers!(agent().get(&tampered_url), presigned)
            .call()
            .expect("transport error");
        let status = response.status().as_u16();
        let body = response.body_mut().read_to_string().unwrap_or_default();
        assert_signature_does_not_match(status, &body, &"0".repeat(64));

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_presigned_sigv4_unsigned_acl_header_reports_headers_not_signed() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let body = b"presigned sigv4 unsigned acl header";

        let presigned = presign_object(
            "PUT",
            &bucket,
            "unsigned-acl-presigned-auth",
            None,
            Duration::from_secs(900),
            NO_HEADERS,
            None,
        );

        let mut response = with_presigned_headers!(agent().put(presigned.uri()), presigned)
            .header("x-amz-acl", "private")
            .send(&body[..])
            .expect("transport error");
        let status = response.status().as_u16();
        let body = response.body_mut().read_to_string().unwrap_or_default();
        assert_headers_not_signed_error(status, &body, "x-amz-acl");

        cleanup(&bucket, &["unsigned-acl-presigned-auth"]).await;
    });
}

#[test]
fn test_presigned_sigv4_unsigned_amz_content_sha256_mismatches_signature() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"presigned sigv4 unsigned amz header";

        client
            .put_object()
            .bucket(&bucket)
            .key("unsigned-amz-presigned-auth")
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();

        let payload_hash = sha256_hex(body);
        let presigned = presign_object(
            "GET",
            &bucket,
            "unsigned-amz-presigned-auth",
            None,
            Duration::from_secs(900),
            NO_HEADERS,
            None,
        );

        let mut response = with_presigned_headers!(agent().get(presigned.uri()), presigned)
            .header("x-amz-content-sha256", &payload_hash)
            .call()
            .expect("transport error");
        let status = response.status().as_u16();
        let body = response.body_mut().read_to_string().unwrap_or_default();
        let (_, signature) = presigned
            .uri()
            .split_once("X-Amz-Signature=")
            .expect("presigned URL contains a signature");
        assert_signature_does_not_match(status, &body, signature);

        cleanup(&bucket, &["unsigned-amz-presigned-auth"]).await;
    });
}

#[test]
fn test_presigned_sigv4_unsigned_amz_content_sha256_unsigned_payload_is_accepted() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"presigned sigv4 unsigned payload header";

        client
            .put_object()
            .bucket(&bucket)
            .key("unsigned-payload-amz-presigned-auth")
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();

        let presigned = presign_object(
            "GET",
            &bucket,
            "unsigned-payload-amz-presigned-auth",
            None,
            Duration::from_secs(900),
            NO_HEADERS,
            None,
        );

        let mut response = with_presigned_headers!(agent().get(presigned.uri()), presigned)
            .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
            .call()
            .expect("transport error");
        let status = response.status().as_u16();
        let response_body = response.body_mut().read_to_vec().unwrap();
        assert_eq!(
            status,
            200,
            "expected 200 for unsigned x-amz-content-sha256=UNSIGNED-PAYLOAD, got {status}: {}",
            String::from_utf8_lossy(&response_body)
        );
        assert_eq!(response_body, body);

        cleanup(&bucket, &["unsigned-payload-amz-presigned-auth"]).await;
    });
}

#[test]
fn test_presigned_sigv4_wrong_region_scope_returns_query_parameters_error() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let wrong_region = if CTX.region() == "us-east-1" {
            "us-west-2"
        } else {
            "us-east-1"
        };
        let presigned = presign_object_with_credential_scope(
            "GET",
            &bucket,
            "wrong-region-scope",
            wrong_region,
            "s3",
        );

        assert_shape(
            "presigned wrong region scope",
            &raw_fetch_url(&presigned, &[]),
            &shape()
                .status(400)
                .headers(error_response_headers())
                .sub("wrong_region", wrong_region)
                .sub("region", CTX.region())
                .body(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <Error><Code>AuthorizationQueryParametersError</Code>\
                     <Message>Error parsing the X-Amz-Credential parameter; \
                     the region '{wrong_region}' is wrong; expecting \
                     '{region}'</Message>\
                     <Region>{region}</Region>\
                     <RequestId>{request_id}</RequestId>\
                     <HostId>{host_id}</HostId></Error>",
                ),
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_presigned_sigv4_missing_account_regional_bucket_wrong_region_returns_query_parameters_error(
) {
    s3_tests::run(async {
        let bucket = unique_account_regional_bucket();
        let wrong_region = if CTX.region() == "us-east-1" {
            "us-west-2"
        } else {
            "us-east-1"
        };
        let presigned =
            presign_object_with_credential_scope("GET", &bucket, "missing-key", wrong_region, "s3");

        // The full header-set comparison also pins that no
        // x-amz-bucket-region header is attached for the missing bucket.
        assert_shape(
            "presigned wrong region scope, missing bucket",
            &raw_fetch_url(&presigned, &[]),
            &shape()
                .status(400)
                .headers(error_response_headers())
                .sub("wrong_region", wrong_region)
                .sub("region", CTX.region())
                .body(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <Error><Code>AuthorizationQueryParametersError</Code>\
                     <Message>Error parsing the X-Amz-Credential parameter; \
                     the region '{wrong_region}' is wrong; expecting \
                     '{region}'</Message>\
                     <Region>{region}</Region>\
                     <RequestId>{request_id}</RequestId>\
                     <HostId>{host_id}</HostId></Error>",
                ),
        );
    });
}

#[test]
fn test_presigned_sigv4_wrong_service_scope_returns_query_parameters_error() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let presigned = presign_object_with_credential_scope(
            "GET",
            &bucket,
            "wrong-service-scope",
            CTX.region(),
            "execute-api",
        );

        assert_shape(
            "presigned wrong service scope",
            &raw_fetch_url(&presigned, &[]),
            &shape().status(400).headers(error_response_headers()).body(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <Error><Code>AuthorizationQueryParametersError</Code>\
                     <Message>Error parsing the X-Amz-Credential parameter; \
                     incorrect service \"execute-api\". This endpoint belongs \
                     to \"s3\".</Message>\
                     <RequestId>{request_id}</RequestId>\
                     <HostId>{host_id}</HostId></Error>",
            ),
        );

        cleanup(&bucket, &[]).await;
    });
}

// ── Presigned GET ───────────────────────────────────────────────────────

#[test]
fn test_presigned_get_object() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"presigned get content";

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();

        let presigned = presign_object(
            "GET",
            &bucket,
            "obj",
            None,
            Duration::from_secs(900),
            NO_HEADERS,
            None,
        );

        let mut resp = agent()
            .get(presigned.uri())
            .call()
            .expect("transport error");
        assert_eq!(resp.status().as_u16(), 200);
        let data = resp.body_mut().read_to_vec().unwrap();
        assert_eq!(&data[..], body);

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_presigned_get_object_nonexistent() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;

        let presigned = presign_object(
            "GET",
            &bucket,
            "no-such-key",
            None,
            Duration::from_secs(900),
            NO_HEADERS,
            None,
        );

        let mut resp = agent()
            .get(presigned.uri())
            .call()
            .expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(status, 404);

        cleanup(&bucket, &[]).await;
    });
}

// ── Presigned PUT ───────────────────────────────────────────────────────

#[test]
fn test_presigned_put_object() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"presigned put content";

        let presigned = presign_object(
            "PUT",
            &bucket,
            "uploaded",
            None,
            Duration::from_secs(900),
            NO_HEADERS,
            None,
        );

        let mut resp = agent()
            .put(presigned.uri())
            .send(&body[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(status, 200);

        // Verify via normal GET
        let get_resp = client
            .get_object()
            .bucket(&bucket)
            .key("uploaded")
            .send()
            .await
            .unwrap();
        let data = get_resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], body);

        cleanup(&bucket, &["uploaded"]).await;
    });
}

async fn assert_presigned_put_object_with_acl(
    client: &aws_sdk_s3::Client,
    credentials: SignedRequestCredentials<'_>,
) {
    use aws_sdk_s3::types::{ObjectOwnership, OwnershipControls, OwnershipControlsRule};

    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    let ownership = OwnershipControls::builder()
        .rules(
            OwnershipControlsRule::builder()
                .object_ownership(ObjectOwnership::BucketOwnerPreferred)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    client
        .put_bucket_ownership_controls()
        .bucket(&bucket)
        .ownership_controls(ownership)
        .send()
        .await
        .unwrap();
    let body = b"hello world";

    let presigned = presign_object_with_credentials(
        credentials,
        "PUT",
        (&bucket, "foo"),
        None,
        Duration::from_secs(900),
        [("x-amz-acl", "private")],
        None,
    );

    let mut resp = with_presigned_headers!(agent().put(presigned.uri()), presigned)
        .send(&body[..])
        .expect("transport error");
    let status = resp.status().as_u16();
    let response_body = resp.body_mut().read_to_string().unwrap_or_default();
    assert_eq!(
        status, 200,
        "expected 200 for presigned PUT with x-amz-acl, got {} body={}",
        status, response_body
    );

    let get_presigned = presign_object_with_credentials(
        credentials,
        "GET",
        (&bucket, "foo"),
        None,
        Duration::from_secs(900),
        NO_HEADERS,
        None,
    );
    let mut get_resp = agent()
        .get(get_presigned.uri())
        .call()
        .expect("transport error");
    assert_eq!(get_resp.status().as_u16(), 200);
    let data = get_resp.body_mut().read_to_vec().unwrap();
    assert_eq!(&data[..], body);

    cleanup_with_client(client, &bucket, &["foo"]).await;
}

#[test]
fn test_presigned_sse_c_put_object() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_sse_c_bucket().await;
        let body = b"presigned sse-c put content";
        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);

        let presigned = presign_object(
            "PUT",
            &bucket,
            "uploaded-sse-c",
            None,
            Duration::from_secs(900),
            [
                ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
                (
                    "x-amz-server-side-encryption-customer-key",
                    key_b64.as_str(),
                ),
                (
                    "x-amz-server-side-encryption-customer-key-md5",
                    key_md5_b64.as_str(),
                ),
            ],
            None,
        );

        let mut resp = with_presigned_headers!(agent().put(presigned.uri()), presigned)
            .send(&body[..])
            .expect("transport error");
        assert_eq!(resp.status().as_u16(), 200);
        let alg = resp
            .headers()
            .get("x-amz-server-side-encryption-customer-algorithm")
            .map(|v| v.to_str().unwrap().to_string());
        let key_md5 = resp
            .headers()
            .get("x-amz-server-side-encryption-customer-key-md5")
            .map(|v| v.to_str().unwrap().to_string());
        let _ = resp.body_mut().read_to_string();
        assert_eq!(alg.as_deref(), Some("AES256"));
        assert_eq!(key_md5.as_deref(), Some(key_md5_b64.as_str()));

        let get_resp = client
            .get_object()
            .bucket(&bucket)
            .key("uploaded-sse-c")
            .sse_customer_algorithm("AES256")
            .sse_customer_key(key_b64.clone())
            .sse_customer_key_md5(key_md5_b64.clone())
            .send()
            .await
            .unwrap();
        let data = get_resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], body);

        cleanup(&bucket, &["uploaded-sse-c"]).await;
    });
}

#[test]
fn test_presigned_put_object_signed_payload() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"presigned put with signed payload";
        let body_hash = sha256_hex(body);

        let presigned = presign_object(
            "PUT",
            &bucket,
            "signed-body",
            None,
            Duration::from_secs(900),
            NO_HEADERS,
            Some(&body_hash),
        );

        let mut resp = with_presigned_headers!(agent().put(presigned.uri()), presigned)
            .send(&body[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(
            status, 200,
            "expected 200 for signed-payload presigned PUT, got {}",
            status
        );

        // Verify via normal GET
        let get_resp = client
            .get_object()
            .bucket(&bucket)
            .key("signed-body")
            .send()
            .await
            .unwrap();
        let data = get_resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], body);

        cleanup(&bucket, &["signed-body"]).await;
    });
}

#[test]
fn test_presigned_put_object_signed_payload_mismatch() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let body = b"the body that was signed";
        let body_hash = sha256_hex(body);

        // Sign the URL for this specific body
        let presigned = presign_object(
            "PUT",
            &bucket,
            "signed-body",
            None,
            Duration::from_secs(900),
            NO_HEADERS,
            Some(&body_hash),
        );

        // Send a different body — signature was for the original body
        let wrong_body = b"different body content";
        let mut resp = agent()
            .put(presigned.uri())
            .header("x-amz-content-sha256", sha256_hex(wrong_body))
            .send(&wrong_body[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(
            status, 403,
            "expected 403 for mismatched body hash, got {}",
            status
        );

        cleanup(&bucket, &[]).await;
    });
}

// ── Presigned DELETE ────────────────────────────────────────────────────

#[test]
fn test_presigned_delete_object() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("todelete")
            .body(ByteStream::from_static(b"bye"))
            .send()
            .await
            .unwrap();

        let presigned = presign_object(
            "DELETE",
            &bucket,
            "todelete",
            None,
            Duration::from_secs(900),
            NO_HEADERS,
            None,
        );

        let mut resp = agent()
            .delete(presigned.uri())
            .call()
            .expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        // AWS DeleteObject returns 204 No Content
        assert_eq!(status, 204, "expected 204, got {}", status);

        // Verify deleted
        let result = client
            .get_object()
            .bucket(&bucket)
            .key("todelete")
            .send()
            .await;
        assert!(result.is_err());

        cleanup(&bucket, &[]).await;
    });
}

// ── Presigned HEAD ──────────────────────────────────────────────────────

#[test]
fn test_presigned_head_object() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"head test"))
            .send()
            .await
            .unwrap();

        let presigned = presign_object(
            "HEAD",
            &bucket,
            "obj",
            None,
            Duration::from_secs(900),
            NO_HEADERS,
            None,
        );

        let mut resp = agent()
            .head(presigned.uri())
            .call()
            .expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(status, 200);

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_presigned_sse_c_get_object() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_sse_c_bucket().await;
        let body = b"presigned get sse-c content";
        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);

        client
            .put_object()
            .bucket(&bucket)
            .key("obj-sse-c")
            .sse_customer_algorithm("AES256")
            .sse_customer_key(key_b64.clone())
            .sse_customer_key_md5(key_md5_b64.clone())
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();

        let presigned = presign_object(
            "GET",
            &bucket,
            "obj-sse-c",
            None,
            Duration::from_secs(900),
            [
                ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
                (
                    "x-amz-server-side-encryption-customer-key",
                    key_b64.as_str(),
                ),
                (
                    "x-amz-server-side-encryption-customer-key-md5",
                    key_md5_b64.as_str(),
                ),
            ],
            None,
        );

        let mut resp = with_presigned_headers!(agent().get(presigned.uri()), presigned)
            .call()
            .expect("transport error");
        assert_eq!(resp.status().as_u16(), 200);
        let alg = resp
            .headers()
            .get("x-amz-server-side-encryption-customer-algorithm")
            .map(|v| v.to_str().unwrap().to_string());
        let key_md5 = resp
            .headers()
            .get("x-amz-server-side-encryption-customer-key-md5")
            .map(|v| v.to_str().unwrap().to_string());
        let data = resp.body_mut().read_to_vec().unwrap();
        assert_eq!(alg.as_deref(), Some("AES256"));
        assert_eq!(key_md5.as_deref(), Some(key_md5_b64.as_str()));
        assert_eq!(&data[..], body);

        cleanup(&bucket, &["obj-sse-c"]).await;
    });
}

#[test]
fn test_presigned_sse_c_get_requires_signed_headers() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_sse_c_bucket().await;
        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);

        client
            .put_object()
            .bucket(&bucket)
            .key("obj-sse-c-missing-headers")
            .sse_customer_algorithm("AES256")
            .sse_customer_key(key_b64.clone())
            .sse_customer_key_md5(key_md5_b64.clone())
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let presigned = presign_object(
            "GET",
            &bucket,
            "obj-sse-c-missing-headers",
            None,
            Duration::from_secs(900),
            [
                ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
                (
                    "x-amz-server-side-encryption-customer-key",
                    key_b64.as_str(),
                ),
                (
                    "x-amz-server-side-encryption-customer-key-md5",
                    key_md5_b64.as_str(),
                ),
            ],
            None,
        );

        let mut resp = agent()
            .get(presigned.uri())
            .call()
            .expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(status, 403, "expected 403, got {}", status);

        cleanup(&bucket, &["obj-sse-c-missing-headers"]).await;
    });
}

#[test]
fn test_presigned_sse_c_head_object() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_sse_c_bucket().await;
        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);

        client
            .put_object()
            .bucket(&bucket)
            .key("obj-head-sse-c")
            .sse_customer_algorithm("AES256")
            .sse_customer_key(key_b64.clone())
            .sse_customer_key_md5(key_md5_b64.clone())
            .body(ByteStream::from_static(b"head test"))
            .send()
            .await
            .unwrap();

        let presigned = presign_object(
            "HEAD",
            &bucket,
            "obj-head-sse-c",
            None,
            Duration::from_secs(900),
            [
                ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
                (
                    "x-amz-server-side-encryption-customer-key",
                    key_b64.as_str(),
                ),
                (
                    "x-amz-server-side-encryption-customer-key-md5",
                    key_md5_b64.as_str(),
                ),
            ],
            None,
        );

        let mut resp = with_presigned_headers!(agent().head(presigned.uri()), presigned)
            .call()
            .expect("transport error");
        assert_eq!(resp.status().as_u16(), 200);
        let alg = resp
            .headers()
            .get("x-amz-server-side-encryption-customer-algorithm")
            .map(|v| v.to_str().unwrap().to_string());
        let key_md5 = resp
            .headers()
            .get("x-amz-server-side-encryption-customer-key-md5")
            .map(|v| v.to_str().unwrap().to_string());
        let _ = resp.body_mut().read_to_string();
        assert_eq!(alg.as_deref(), Some("AES256"));
        assert_eq!(key_md5.as_deref(), Some(key_md5_b64.as_str()));

        cleanup(&bucket, &["obj-head-sse-c"]).await;
    });
}

// ── Expired URL ─────────────────────────────────────────────────────────

#[test]
fn test_presigned_get_expired() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        // Generate a URL that expires in 1 second
        let presigned = presign_object(
            "GET",
            &bucket,
            "obj",
            None,
            Duration::from_secs(1),
            NO_HEADERS,
            None,
        );

        // Wait for it to expire
        std::thread::sleep(Duration::from_secs(2));

        let response = raw_fetch_url(presigned.uri(), &[]);
        assert_shape(
            "expired presigned GET",
            &response,
            &shape().status(403).headers(error_response_headers()).body(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <Error><Code>AccessDenied</Code>\
                     <Message>Request has expired</Message>\
                     <X-Amz-Expires>1</X-Amz-Expires>\
                     <Expires>{iso8601}</Expires>\
                     <ServerTime>{iso8601}</ServerTime>\
                     <RequestId>{request_id}</RequestId>\
                     <HostId>{host_id}</HostId></Error>",
            ),
        );

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── Tampered signature ──────────────────────────────────────────────────

#[test]
fn test_presigned_get_bad_signature() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let presigned = presign_object(
            "GET",
            &bucket,
            "obj",
            None,
            Duration::from_secs(900),
            NO_HEADERS,
            None,
        );

        // Tamper with the signature
        let (prefix, _) = presigned
            .uri()
            .split_once("X-Amz-Signature=")
            .expect("presigned URL contains a signature");
        let tampered = format!("{prefix}X-Amz-Signature={}", "0".repeat(64));

        let response = raw_fetch_url(&tampered, &[]);
        assert_shape(
            "presigned GET bad signature",
            &response,
            &shape()
                .status(403)
                .headers(error_response_headers())
                .sub("access_key", CTX.access_key())
                .sub("signature_provided", "0".repeat(64))
                .body(SIGNATURE_MISMATCH_BODY),
        );

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── Tampered query param (key changed) ──────────────────────────────────

#[test]
fn test_presigned_get_tampered_key() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("original")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let presigned = presign_object(
            "GET",
            &bucket,
            "original",
            None,
            Duration::from_secs(900),
            NO_HEADERS,
            None,
        );

        // Change the key in the URL path from "original" to "different"
        let url = presigned.uri().to_string();
        let tampered = url.replace("/original?", "/different?");

        let mut resp = agent().get(&tampered).call().expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        // Signature covers the path; tampered key → SignatureDoesNotMatch
        assert_eq!(status, 403, "expected 403, got {}", status);

        cleanup(&bucket, &["original"]).await;
    });
}

// ── Wrong HTTP method ───────────────────────────────────────────────────

#[test]
fn test_presigned_wrong_method() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        // Generate a presigned GET URL
        let presigned = presign_object(
            "GET",
            &bucket,
            "obj",
            None,
            Duration::from_secs(900),
            NO_HEADERS,
            None,
        );

        // Use it with PUT instead of GET — signature was computed for GET
        let mut resp = agent()
            .put(presigned.uri())
            .send(b"overwrite attempt" as &[u8])
            .expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(status, 403, "expected 403 for wrong method, got {}", status);

        // Verify original content unchanged
        let get_resp = client
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send()
            .await
            .unwrap();
        let data = get_resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"data");

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── Missing signature param ─────────────────────────────────────────────

#[test]
fn test_presigned_missing_signature() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let presigned = presign_object(
            "GET",
            &bucket,
            "obj",
            None,
            Duration::from_secs(900),
            NO_HEADERS,
            None,
        );

        // Remove the X-Amz-Signature parameter
        let url = presigned.uri().to_string();
        let stripped: String = url
            .split('&')
            .filter(|p| !p.contains("X-Amz-Signature"))
            .collect::<Vec<_>>()
            .join("&");

        assert_shape(
            "presigned GET missing signature",
            &raw_fetch_url(&stripped, &[]),
            &shape().status(400).headers(error_response_headers()).body(
                expected_error::with_host_id(
                    "AuthorizationQueryParametersError",
                    "Query-string authentication version 4 requires the \
                     X-Amz-Algorithm, X-Amz-Credential, X-Amz-Signature, \
                     X-Amz-Date, X-Amz-SignedHeaders, and X-Amz-Expires \
                     parameters.",
                ),
            ),
        );

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── Presigned PUT then GET verifies round-trip ──────────────────────────

#[test]
fn test_presigned_put_get_round_trip() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let body = b"round-trip content via presigned URLs";

        // Presigned PUT
        let put_presigned = presign_object(
            "PUT",
            &bucket,
            "roundtrip",
            None,
            Duration::from_secs(900),
            NO_HEADERS,
            None,
        );

        let mut put_resp = agent()
            .put(put_presigned.uri())
            .send(&body[..])
            .expect("transport error");
        assert_eq!(put_resp.status().as_u16(), 200);
        let _ = put_resp.body_mut().read_to_string();

        // Presigned GET
        let get_presigned = presign_object(
            "GET",
            &bucket,
            "roundtrip",
            None,
            Duration::from_secs(900),
            NO_HEADERS,
            None,
        );

        let mut get_resp = agent()
            .get(get_presigned.uri())
            .call()
            .expect("transport error");
        assert_eq!(get_resp.status().as_u16(), 200);
        let data = get_resp.body_mut().read_to_vec().unwrap();
        assert_eq!(&data[..], body);

        cleanup(&bucket, &["roundtrip"]).await;
    });
}

// ── Presigned GET with response overrides ───────────────────────────────

#[test]
fn test_presigned_get_response_content_type() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let presigned = presign_object(
            "GET",
            &bucket,
            "obj",
            Some("response-content-type=application/pdf"),
            Duration::from_secs(900),
            NO_HEADERS,
            None,
        );

        let mut resp = agent()
            .get(presigned.uri())
            .call()
            .expect("transport error");
        assert_eq!(resp.status().as_u16(), 200);
        let ct = resp
            .headers()
            .get("Content-Type")
            .map(|v| v.to_str().unwrap().to_string());
        let _ = resp.body_mut().read_to_string();
        assert_eq!(ct.as_deref(), Some("application/pdf"));

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── X-Amz-Expires range tests ──────────────────────────────────────────

#[test]
fn test_object_raw_get_x_amz_expires_not_expired() {
    s3_tests::run(async {
        assert_object_raw_get_x_amz_expires_not_expired(CTX.client(), primary_credentials()).await;
    });
}

#[test]
fn test_object_raw_get_x_amz_expires_out_max_range() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        // Generate a valid presigned URL, then tamper X-Amz-Expires to exceed the
        // 604800-second (7-day) maximum.
        let presigned = presign_object(
            "GET",
            &bucket,
            "obj",
            None,
            Duration::from_secs(600),
            NO_HEADERS,
            None,
        );

        let tampered_url = presigned
            .uri()
            .replace("X-Amz-Expires=600", "X-Amz-Expires=604801");

        // Presigned URL validation: expires > 604800 is an auth parameter error → 400
        assert_shape(
            "presigned GET expires beyond a week",
            &raw_fetch_url(&tampered_url, &[]),
            &shape().status(400).headers(error_response_headers()).body(
                expected_error::with_host_id(
                    "AuthorizationQueryParametersError",
                    "X-Amz-Expires must be less than a week (in seconds); that is, \
                     the given X-Amz-Expires must be less than 604800 seconds",
                ),
            ),
        );

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_object_raw_get_x_amz_expires_out_positive_range() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        // Manually construct a URL with a negative X-Amz-Expires
        let presigned = presign_object(
            "GET",
            &bucket,
            "obj",
            None,
            Duration::from_secs(600),
            NO_HEADERS,
            None,
        );

        // Replace the X-Amz-Expires value with a negative number
        let tampered_url = presigned
            .uri()
            .replace("X-Amz-Expires=600", "X-Amz-Expires=-1");

        let mut resp = agent().get(&tampered_url).call().expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        // Negative X-Amz-Expires is an auth parameter error → 400
        assert_eq!(
            status, 400,
            "expected 400 for negative expires, got {}",
            status
        );

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_object_raw_get_x_amz_expires_out_range_zero() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        // Construct a URL with X-Amz-Expires=0
        let presigned = presign_object(
            "GET",
            &bucket,
            "obj",
            None,
            Duration::from_secs(600),
            NO_HEADERS,
            None,
        );

        let tampered_url = presigned
            .uri()
            .replace("X-Amz-Expires=600", "X-Amz-Expires=0");

        // Zero expires means immediately expired → auth expiry
        assert_shape(
            "presigned GET zero expires",
            &raw_fetch_url(&tampered_url, &[]),
            &shape().status(403).headers(error_response_headers()).body(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <Error><Code>AccessDenied</Code>\
                     <Message>Request has expired</Message>\
                     <X-Amz-Expires>0</X-Amz-Expires>\
                     <Expires>{iso8601}</Expires>\
                     <ServerTime>{iso8601}</ServerTime>\
                     <RequestId>{request_id}</RequestId>\
                     <HostId>{host_id}</HostId></Error>",
            ),
        );

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_object_raw_get_x_amz_epoch_date_is_expired() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let presigned_url = presign_object_with_fixed_amz_date(
            primary_credentials(),
            "GET",
            &bucket,
            "obj",
            Duration::from_secs(1),
            "19700101T000000Z",
        );

        // Epoch X-Amz-Date plus 1-second expiry pins the exact Expires time.
        assert_shape(
            "presigned GET epoch date expired",
            &raw_fetch_url(&presigned_url, &[]),
            &shape().status(403).headers(error_response_headers()).body(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <Error><Code>AccessDenied</Code>\
                     <Message>Request has expired</Message>\
                     <X-Amz-Expires>1</X-Amz-Expires>\
                     <Expires>1970-01-01T00:00:01Z</Expires>\
                     <ServerTime>{iso8601}</ServerTime>\
                     <RequestId>{request_id}</RequestId>\
                     <HostId>{host_id}</HostId></Error>",
            ),
        );

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_object_raw_get_x_amz_future_date_is_not_valid_yet() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let presigned_url = presign_object_with_fixed_amz_date(
            primary_credentials(),
            "GET",
            &bucket,
            "obj",
            Duration::from_secs(900),
            "21000101T000000Z",
        );

        // 21000101T000000Z is epoch 4102444800; AWS echoes it in epoch
        // milliseconds plus the would-be expiry (900s later).
        assert_shape(
            "presigned GET future date not yet valid",
            &raw_fetch_url(&presigned_url, &[]),
            &shape().status(403).headers(error_response_headers()).body(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <Error><Code>AccessDenied</Code>\
                     <Message>Request is not yet valid</Message>\
                     <X-Amz-Date>4102444800000</X-Amz-Date>\
                     <Expires>2100-01-01T00:15:00Z</Expires>\
                     <ServerTime>{iso8601}</ServerTime>\
                     <RequestId>{request_id}</RequestId>\
                     <HostId>{host_id}</HostId></Error>",
            ),
        );

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_object_raw_put_authenticated_expired() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;

        // Generate a presigned PUT URL that's already expired
        let presigned = presign_object(
            "PUT",
            &bucket,
            "obj",
            None,
            Duration::from_secs(1),
            NO_HEADERS,
            None,
        );

        // Wait for expiry
        std::thread::sleep(Duration::from_secs(2));

        let mut resp = agent()
            .put(presigned.uri())
            .send(b"data" as &[u8])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body = resp.body_mut().read_to_string().unwrap_or_default();
        // Expired presigned URL → auth expiry with the full expired shape.
        assert_status_and_body(
            "expired presigned PUT",
            status,
            &body,
            &shape().status(403).body(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                 <Error><Code>AccessDenied</Code>\
                 <Message>Request has expired</Message>\
                 <X-Amz-Expires>1</X-Amz-Expires>\
                 <Expires>{iso8601}</Expires>\
                 <ServerTime>{iso8601}</ServerTime>\
                 <RequestId>{request_id}</RequestId>\
                 <HostId>{host_id}</HostId></Error>",
            ),
        );

        cleanup(&bucket, &[]).await;
    });
}

// ── ACL / Tenant presigned (not implemented) ───────────────────────────

#[test]
fn test_object_presigned_put_object_with_acl() {
    s3_tests::run(async {
        assert_presigned_put_object_with_acl(CTX.client(), primary_credentials()).await;
    });
}

#[test]
fn test_object_presigned_put_object_with_acl_tenant() {
    s3_tests::run(async {
        assert_presigned_put_object_with_acl(CTX.alt_client(), alt_credentials()).await;
    });
}

async fn assert_object_raw_get_x_amz_expires_not_expired(
    client: &aws_sdk_s3::Client,
    credentials: SignedRequestCredentials<'_>,
) {
    let bucket = create_public_bucket(client).await;
    client
        .put_object()
        .bucket(&bucket)
        .key("obj")
        .acl(aws_sdk_s3::types::ObjectCannedAcl::PublicRead)
        .body(ByteStream::from_static(b"data"))
        .send()
        .await
        .unwrap();

    let presigned = presign_object_with_credentials(
        credentials,
        "GET",
        (&bucket, "obj"),
        None,
        Duration::from_secs(600),
        NO_HEADERS,
        None,
    );

    let mut options_resp = agent()
        .options(presigned.uri())
        .call()
        .expect("transport error");
    let options_status = options_resp.status().as_u16();
    let _ = options_resp.body_mut().read_to_string();
    assert_eq!(options_status, 400);

    let mut get_resp = agent()
        .get(presigned.uri())
        .call()
        .expect("transport error");
    assert_eq!(get_resp.status().as_u16(), 200);
    let data = get_resp.body_mut().read_to_vec().unwrap();
    assert_eq!(&data[..], b"data");

    cleanup_with_client(client, &bucket, &["obj"]).await;
}

#[test]
fn test_object_raw_get_x_amz_expires_not_expired_tenant() {
    s3_tests::run(async {
        assert_object_raw_get_x_amz_expires_not_expired(CTX.alt_client(), alt_credentials()).await;
    });
}
