use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use aws_sdk_s3::{primitives::ByteStream, types::ServerSideEncryption};
use ring::hmac;
use s3_tests::{
    assert_s3_err_code, err_status, post_object_raw_to_test_endpoint_with_headers,
    shape::{assert_shape, shape},
    sigv4_post_fields_for_credentials, sigv4_post_sse_c_fields_for_credentials,
    sse_c_header_values, test_sse_c_key, unique_bucket, RawResponse, SendRetryingOperationAborted,
    CTX,
};

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
        "POST Object SSE-C coverage requires an https:// endpoint; got {}",
        CTX.endpoint()
    );
}

/// Derive the SigV4 signing key.
fn derive_signing_key(secret: &str, date: &str, region: &str, service: &str) -> hmac::Tag {
    let k_secret = format!("AWS4{}", secret);
    let k_date = hmac_sha256(k_secret.as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(k_date.as_ref(), region.as_bytes());
    let k_service = hmac_sha256(k_region.as_ref(), service.as_bytes());
    hmac_sha256(k_service.as_ref(), b"aws4_request")
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> hmac::Tag {
    hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), data)
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Compute a SigV4 hex signature of a base64 policy string.
fn sign_policy_v4(policy_b64: &str, secret: &str, date: &str, region: &str) -> String {
    sign_policy_v4_with_service(policy_b64, secret, date, region, "s3")
}

fn sign_policy_v4_with_service(
    policy_b64: &str,
    secret: &str,
    date: &str,
    region: &str,
    service: &str,
) -> String {
    let signing_key = derive_signing_key(secret, date, region, service);
    let sig = hmac_sha256(signing_key.as_ref(), policy_b64.as_bytes());
    hex_encode(sig.as_ref())
}

/// Build a POST policy JSON and base64-encode it.
fn make_policy(
    bucket: &str,
    key: &str,
    expiry_secs: u64,
    extra_conditions: &[serde_json::Value],
) -> String {
    use base64::Engine;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let exp = now + expiry_secs;

    // Format expiration as ISO 8601
    let expiration = epoch_to_iso8601(exp);

    let mut conditions = vec![
        serde_json::json!({"bucket": bucket}),
        serde_json::json!({"key": key}),
    ];
    conditions.extend_from_slice(extra_conditions);

    let policy = serde_json::json!({
        "expiration": expiration,
        "conditions": conditions,
    });

    base64::engine::general_purpose::STANDARD.encode(policy.to_string().as_bytes())
}

/// Build a POST policy JSON with a raw conditions array (no implicit bucket/key).
fn make_policy_raw(expiration: &str, conditions: &[serde_json::Value]) -> String {
    use base64::Engine;

    let policy = serde_json::json!({
        "expiration": expiration,
        "conditions": conditions,
    });

    base64::engine::general_purpose::STANDARD.encode(policy.to_string().as_bytes())
}

/// Convert epoch seconds to ISO 8601 "YYYY-MM-DDTHH:MM:SSZ" format.
fn epoch_to_iso8601(epoch: u64) -> String {
    let secs = epoch;
    let days = secs / 86400;
    let time_of_day = secs % 86400;
    let hour = time_of_day / 3600;
    let min = (time_of_day % 3600) / 60;
    let sec = time_of_day % 60;

    // Calculate year, month, day from days since epoch
    let mut y = 1970u64;
    let mut remaining = days;
    loop {
        let year_days = if is_leap(y) { 366 } else { 365 };
        if remaining < year_days {
            break;
        }
        remaining -= year_days;
        y += 1;
    }
    let month_days: [u64; 12] = if is_leap(y) {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };
    let mut m = 0;
    while m < 12 && remaining >= month_days[m] {
        remaining -= month_days[m];
        m += 1;
    }
    let d = remaining + 1;

    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        y,
        m + 1,
        d,
        hour,
        min,
        sec
    )
}

fn is_leap(year: u64) -> bool {
    (year.is_multiple_of(4) && !year.is_multiple_of(100)) || year.is_multiple_of(400)
}

/// Get the current short date (YYYYMMDD) and full date (YYYYMMDDTHHMMSSZ).
fn current_dates() -> (String, String) {
    let epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let iso = epoch_to_iso8601(epoch);
    // short date: YYYYMMDD
    let short = iso[..10].replace('-', "");
    // full date: YYYYMMDDTHHMMSSZ
    let full = format!("{}T{}Z", short, iso[11..19].replace(':', ""));
    (short, full)
}

/// Build a multipart/form-data body from field pairs and file data.
/// Returns (content_type_header, body_bytes).
fn build_multipart(
    fields: &[(&str, &str)],
    file_data: &[u8],
    file_name: &str,
) -> (String, Vec<u8>) {
    let boundary = "----TestBoundary7MA4YWxkTrZu0gW";
    let mut body = Vec::new();

    for (name, value) in fields {
        body.extend_from_slice(format!("--{}\r\n", boundary).as_bytes());
        body.extend_from_slice(
            format!("Content-Disposition: form-data; name=\"{}\"\r\n\r\n", name).as_bytes(),
        );
        body.extend_from_slice(value.as_bytes());
        body.extend_from_slice(b"\r\n");
    }

    // File field must be last
    body.extend_from_slice(format!("--{}\r\n", boundary).as_bytes());
    body.extend_from_slice(
        format!(
            "Content-Disposition: form-data; name=\"file\"; filename=\"{}\"\r\n",
            file_name
        )
        .as_bytes(),
    );
    body.extend_from_slice(b"Content-Type: application/octet-stream\r\n\r\n");
    body.extend_from_slice(file_data);
    body.extend_from_slice(b"\r\n");
    body.extend_from_slice(format!("--{}--\r\n", boundary).as_bytes());

    let content_type = format!("multipart/form-data; boundary={}", boundary);
    (content_type, body)
}

/// Helper: build SigV4 POST form fields for a given bucket/key/file.
/// Returns fields vec including all auth fields plus key.
fn sigv4_fields(
    bucket: &str,
    key: &str,
    extra_conditions: &[serde_json::Value],
) -> Vec<(String, String)> {
    sigv4_fields_for_credentials(
        CTX.access_key(),
        CTX.secret_key(),
        CTX.region(),
        bucket,
        key,
        extra_conditions,
    )
}

fn sigv4_fields_for_credentials(
    access_key: &str,
    secret: &str,
    region: &str,
    bucket: &str,
    key: &str,
    extra_conditions: &[serde_json::Value],
) -> Vec<(String, String)> {
    sigv4_fields_for_credentials_and_service(
        access_key,
        secret,
        region,
        "s3",
        bucket,
        key,
        extra_conditions,
    )
}

fn sigv4_fields_for_credentials_and_service(
    access_key: &str,
    secret: &str,
    region: &str,
    service: &str,
    bucket: &str,
    key: &str,
    extra_conditions: &[serde_json::Value],
) -> Vec<(String, String)> {
    let (short_date, full_date) = current_dates();

    let credential = format!("{access_key}/{short_date}/{region}/{service}/aws4_request");

    // AWS requires ALL form fields to have matching policy conditions.
    // The SigV4 fields must be included in the policy.
    let mut all_conditions = vec![
        serde_json::json!({"x-amz-algorithm": "AWS4-HMAC-SHA256"}),
        serde_json::json!({"x-amz-credential": &credential}),
        serde_json::json!({"x-amz-date": &full_date}),
    ];
    all_conditions.extend_from_slice(extra_conditions);

    let policy_b64 = make_policy(bucket, key, 3600, &all_conditions);
    let signature = sign_policy_v4_with_service(&policy_b64, secret, &short_date, region, service);

    vec![
        ("key".to_string(), key.to_string()),
        (
            "x-amz-algorithm".to_string(),
            "AWS4-HMAC-SHA256".to_string(),
        ),
        ("x-amz-credential".to_string(), credential),
        ("x-amz-date".to_string(), full_date),
        ("policy".to_string(), policy_b64),
        ("x-amz-signature".to_string(), signature),
    ]
}

/// Send a POST Object request. Returns (status_code, response_body).
fn post_object(
    bucket: &str,
    fields: &[(&str, &str)],
    file_data: &[u8],
    file_name: &str,
) -> (u16, String) {
    post_object_with_headers(bucket, fields, file_data, file_name, &[])
}

fn post_object_with_headers(
    bucket: &str,
    fields: &[(&str, &str)],
    file_data: &[u8],
    file_name: &str,
    headers: &[(&str, &str)],
) -> (u16, String) {
    post_object_to_endpoint(
        CTX.endpoint(),
        &agent(),
        bucket,
        fields,
        file_data,
        file_name,
        headers,
    )
}

fn post_object_to_endpoint(
    endpoint: &str,
    agent: &s3_tests::Agent,
    bucket: &str,
    fields: &[(&str, &str)],
    file_data: &[u8],
    file_name: &str,
    headers: &[(&str, &str)],
) -> (u16, String) {
    let url = format!("{}/{}", endpoint, bucket);
    let (content_type, body) = build_multipart(fields, file_data, file_name);

    let deadline = Instant::now() + configured_post_object_retry_timeout();
    loop {
        let req = agent.post(&url).header("Content-Type", &content_type);
        let req = headers
            .iter()
            .fold(req, |req, (name, value)| req.header(*name, *value));
        let mut resp = req.send(&body[..]).expect("HTTP transport error");

        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();
        if status == 409
            && body_str.contains("<Code>OperationAborted</Code>")
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(100));
            continue;
        }
        return (status, body_str);
    }
}

fn configured_post_object_retry_timeout() -> Duration {
    let timeout_secs = std::env::var("S3_TEST_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(30);
    Duration::from_secs(timeout_secs)
}

fn post_object_raw(
    bucket: &str,
    fields: &[(String, String)],
    file_data: &[u8],
    file_name: &str,
) -> RawResponse {
    post_object_raw_to_test_endpoint_with_headers(
        CTX.endpoint(),
        CTX.tls_ca_pem(),
        bucket,
        fields,
        file_data,
        file_name,
        &[],
    )
}

fn response_header_value<'a>(response: &'a RawResponse, name: &str) -> Option<&'a str> {
    response
        .headers
        .iter()
        .find(|(header_name, _)| header_name.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

fn assert_error_code(body: &str, code: &str) {
    let expected = format!("<Code>{code}</Code>");
    assert!(
        body.contains(&expected),
        "expected {expected} in body: {body}"
    );
}

fn assert_invalid_token_response(status: u16, body: &str) {
    assert_eq!(status, 400, "expected 400, got {status} body={body}");
    assert_error_code(body, "InvalidToken");
    assert!(
        body.contains("The provided token is malformed or otherwise invalid."),
        "expected InvalidToken message, got: {body}"
    );
}

fn assert_invalid_policy_document_response(status: u16, body: &str, message: &str) {
    assert_eq!(status, 400, "expected 400, got {status} body={body}");
    assert_error_code(body, "InvalidPolicyDocument");
    assert!(
        body.contains(&format!("<Message>{message}</Message>")),
        "expected InvalidPolicyDocument message {message:?}, got: {body}"
    );
    assert!(
        body.contains("<RequestId>") && body.contains("<HostId>"),
        "expected AWS error shape with RequestId and HostId, got: {body}"
    );
    assert!(
        !body.contains("<Resource>"),
        "InvalidPolicyDocument response should omit Resource, got: {body}"
    );
}

fn append_security_token_field(fields: &mut Vec<(String, String)>, token: &str) {
    fields.push(("x-amz-security-token".to_string(), token.to_string()));
}

async fn wait_for_put_object_access_denied(client: &aws_sdk_s3::Client, bucket: &str, key: &str) {
    for attempt in 0..20 {
        let result = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(b"policy-convergence"))
            .send()
            .await;
        if result
            .as_ref()
            .err()
            .and_then(|err| err.raw_response().map(|r| r.status().as_u16()))
            == Some(403)
        {
            return;
        }
        if attempt + 1 < 20 {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    }

    panic!("PutObject policy denial did not converge");
}

// ── Basic upload ────────────────────────────────────────────────────────

#[test]
fn test_post_object_authenticated_request() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_sse_c_bucket().await;
        let key = "post-test-key";
        let file_data = b"hello from POST";

        let fields = sigv4_fields(&bucket, key, &[]);
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let (status, _) = post_object(&bucket, &field_refs, file_data, "test.txt");
        assert_eq!(status, 204, "expected 204, got {}", status);

        // Verify via GET
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], file_data);

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_post_object_sigv4_credential_service_must_be_s3() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "post-wrong-service";
        let fields = sigv4_fields_for_credentials_and_service(
            CTX.access_key(),
            CTX.secret_key(),
            CTX.region(),
            "execute-api",
            &bucket,
            key,
            &[],
        );
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let credential = fields
            .iter()
            .find(|(name, _)| name == "x-amz-credential")
            .map(|(_, value)| value.as_str())
            .expect("credential field");

        let (status, body) = post_object(&bucket, &field_refs, b"wrong service", "test.txt");
        assert_eq!(status, 400, "expected 400, got {status} body={body}");
        assert_error_code(&body, "InvalidArgument");
        assert!(
            body.contains(
                "<Message>incorrect service \"execute-api\". This endpoint belongs to \"s3\".</Message>"
            ),
            "expected wrong-service credential message, got: {body}"
        );
        assert!(
            body.contains("<ArgumentName>X-Amz-Credential</ArgumentName>")
                && body.contains(&format!("<ArgumentValue>{credential}</ArgumentValue>")),
            "expected POST credential argument details, got: {body}"
        );
        assert!(
            body.contains("<RequestId>") && body.contains("<HostId>"),
            "expected RequestId and HostId, got: {body}"
        );
        assert!(
            !body.contains("<Resource>") && !body.contains("<Region>"),
            "did not expect Resource or Region element, got: {body}"
        );

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_post_object_sigv4_credential_region_must_match_bucket_region() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "post-wrong-region";
        let wrong_region = if CTX.region() == "us-east-1" {
            "us-west-2"
        } else {
            "us-east-1"
        };
        let fields = sigv4_fields_for_credentials(
            CTX.access_key(),
            CTX.secret_key(),
            wrong_region,
            &bucket,
            key,
            &[],
        );
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let credential = fields
            .iter()
            .find(|(name, _)| name == "x-amz-credential")
            .map(|(_, value)| value.as_str())
            .expect("credential field");

        let (status, body) = post_object(&bucket, &field_refs, b"wrong region", "test.txt");
        assert_eq!(status, 400, "expected 400, got {status} body={body}");
        assert_error_code(&body, "InvalidArgument");
        assert!(
            body.contains(&format!(
                "<Message>the region '{wrong_region}' is wrong; expecting '{}'</Message>",
                CTX.region()
            )),
            "expected wrong-region credential message, got: {body}"
        );
        assert!(
            body.contains("<ArgumentName>X-Amz-Credential</ArgumentName>")
                && body.contains(&format!("<ArgumentValue>{credential}</ArgumentValue>")),
            "expected POST credential argument details, got: {body}"
        );
        assert!(
            body.contains(&format!("<Region>{}</Region>", CTX.region())),
            "expected Region element, got: {body}"
        );
        assert!(
            body.contains("<RequestId>") && body.contains("<HostId>"),
            "expected RequestId and HostId, got: {body}"
        );
        assert!(
            !body.contains("<Resource>"),
            "did not expect Resource element, got: {body}"
        );

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_post_object_sigv4_unexpected_security_token_rejected() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "post-unexpected-security-token";
        let token = "unexpected-post-security-token";
        let mut fields = sigv4_fields(
            &bucket,
            key,
            &[serde_json::json!({"x-amz-security-token": token})],
        );
        append_security_token_field(&mut fields, token);
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let (status, body) = post_object(&bucket, &field_refs, b"unexpected token", "test.txt");
        assert_invalid_token_response(status, &body);

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_post_object_sigv4_bad_signature_with_unexpected_security_token_reports_signature_mismatch()
{
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "post-bad-signature-unexpected-security-token";
        let token = "post-bad-signature-unexpected-security-token";
        let mut fields = sigv4_fields(
            &bucket,
            key,
            &[serde_json::json!({"x-amz-security-token": token})],
        );
        append_security_token_field(&mut fields, token);
        for (name, value) in &mut fields {
            if name == "x-amz-signature" {
                *value = "0".repeat(64);
            }
        }
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let (status, body) = post_object(&bucket, &field_refs, b"unexpected token", "test.txt");
        assert_eq!(status, 403, "expected 403, got {status} body={body}");
        assert_error_code(&body, "SignatureDoesNotMatch");

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_post_object_sigv4_overlong_unexpected_security_token_rejected() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "post-overlong-unexpected-security-token";
        let token = "x".repeat(4097);
        let mut fields = sigv4_fields(
            &bucket,
            key,
            &[serde_json::json!({"x-amz-security-token": token.as_str()})],
        );
        append_security_token_field(&mut fields, &token);
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let (status, body) = post_object(&bucket, &field_refs, b"unexpected token", "test.txt");
        assert_invalid_token_response(status, &body);

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_post_object_default_success_headers() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "post-default-success";
        let fields = sigv4_fields(&bucket, key, &[]);
        let expected_location = format!("{}/{bucket}/{key}", CTX.endpoint());

        let resp = post_object_raw(&bucket, &fields, b"data", "test.txt");
        assert_eq!(resp.status, 204, "expected 204, got {:?}", resp);
        assert_eq!(resp.body, "");
        assert_eq!(
            response_header_value(&resp, "Location"),
            Some(expected_location.as_str())
        );
        assert_eq!(
            response_header_value(&resp, "x-amz-server-side-encryption"),
            Some("AES256")
        );
        assert_eq!(
            response_header_value(&resp, "x-amz-checksum-type"),
            Some("FULL_OBJECT")
        );
        assert!(
            response_header_value(&resp, "x-amz-checksum-crc64nvme").is_some(),
            "expected checksum header in {:?}",
            resp.headers
        );

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_post_object_ignores_expected_bucket_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_sse_c_bucket().await;
        let key = "post-expected-owner";
        let file_data = b"hello from POST expected owner";

        let fields = sigv4_fields(&bucket, key, &[]);
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let (status, body) = post_object_with_headers(
            &bucket,
            &field_refs,
            file_data,
            "test.txt",
            &[("x-amz-expected-bucket-owner", "000000000000")],
        );
        assert_eq!(status, 204, "expected 204, got {} body={}", status, body);

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], file_data);

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_post_object_sse_c_round_trip() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_sse_c_bucket().await;
        let key = "post-sse-c";
        let file_data = b"hello from POST with SSE-C";
        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);

        let fields = sigv4_post_sse_c_fields_for_credentials(
            CTX.access_key(),
            CTX.secret_key(),
            CTX.region(),
            &bucket,
            key,
            &customer_key,
        );
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let (status, body) = post_object(&bucket, &field_refs, file_data, "test.txt");
        assert_eq!(status, 204, "expected 204, got {} body={}", status, body);

        let head = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .sse_customer_algorithm("AES256")
            .sse_customer_key(key_b64.clone())
            .sse_customer_key_md5(key_md5_b64.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(head.content_length(), Some(file_data.len() as i64));
        assert_eq!(head.sse_customer_algorithm(), Some("AES256"));
        assert_eq!(head.sse_customer_key_md5(), Some(key_md5_b64.as_str()));

        let get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .sse_customer_algorithm("AES256")
            .sse_customer_key(key_b64.clone())
            .sse_customer_key_md5(key_md5_b64.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(get.sse_customer_algorithm(), Some("AES256"));
        assert_eq!(get.sse_customer_key_md5(), Some(key_md5_b64.as_str()));
        let data = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], file_data);

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_post_object_sse_s3_round_trip() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "post-sse-s3";
        let file_data = b"hello from POST with SSE-S3";

        let mut fields = sigv4_fields(
            &bucket,
            key,
            &[serde_json::json!({"x-amz-server-side-encryption": "AES256"})],
        );
        fields.push((
            "x-amz-server-side-encryption".to_string(),
            "AES256".to_string(),
        ));
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let (status, body) = post_object(&bucket, &field_refs, file_data, "test.txt");
        assert_eq!(status, 204, "expected 204, got {} body={}", status, body);

        let head = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(head.content_length(), Some(file_data.len() as i64));
        assert_eq!(
            head.server_side_encryption(),
            Some(&ServerSideEncryption::Aes256)
        );

        let get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(
            get.server_side_encryption(),
            Some(&ServerSideEncryption::Aes256)
        );
        let data = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], file_data);

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_post_object_inherits_bucket_default_sse_s3() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "post-default-sse-s3";
        let file_data = b"hello from POST with inherited SSE-S3";

        let fields = sigv4_fields(&bucket, key, &[]);
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let (status, body) = post_object(&bucket, &field_refs, file_data, "test.txt");
        assert_eq!(status, 204, "expected 204, got {} body={}", status, body);

        let head = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(head.content_length(), Some(file_data.len() as i64));
        assert_eq!(
            head.server_side_encryption(),
            Some(&ServerSideEncryption::Aes256)
        );

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_post_object_sse_c_bucket_policy_rejects_lowercase_algorithm() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "post-sse-c-lowercase";
        let file_data = b"hello from lowercase POST with SSE-C";
        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);

        let policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:PutObject",
                "Resource": format!("arn:aws:s3:::{bucket}/*"),
                "Condition": {
                    "StringNotEquals": {
                        "s3:x-amz-server-side-encryption-customer-algorithm": "aes256"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let mut fields = sigv4_fields(
            &bucket,
            key,
            &[
                serde_json::json!({"x-amz-server-side-encryption-customer-algorithm": "aes256"}),
                serde_json::json!({"x-amz-server-side-encryption-customer-key": &key_b64}),
                serde_json::json!({"x-amz-server-side-encryption-customer-key-md5": &key_md5_b64}),
            ],
        );
        fields.push((
            "x-amz-server-side-encryption-customer-algorithm".to_string(),
            "aes256".to_string(),
        ));
        fields.push((
            "x-amz-server-side-encryption-customer-key".to_string(),
            key_b64.clone(),
        ));
        fields.push((
            "x-amz-server-side-encryption-customer-key-md5".to_string(),
            key_md5_b64.clone(),
        ));
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let (status, body) = post_object(&bucket, &field_refs, file_data, "test.txt");
        assert_eq!(status, 400, "expected 400, got {} body={}", status, body);
        assert!(
            body.contains("<Code>InvalidEncryptionAlgorithmError</Code>"),
            "expected InvalidEncryptionAlgorithmError body, got {body}"
        );
        assert!(
            body.contains("<ArgumentValue>aes256</ArgumentValue>"),
            "expected lowercase algorithm to be echoed in body, got {body}"
        );

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_post_object_sse_s3_bucket_policy_requires_explicit_header() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "post-sse-s3-policy";
        let file_data = b"hello from POST with SSE-S3 policy";

        let policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:PutObject",
                "Resource": format!("arn:aws:s3:::{bucket}/*"),
                "Condition": {
                    "Null": {
                        "s3:x-amz-server-side-encryption": "true"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let denied_fields = sigv4_fields(&bucket, key, &[]);
        let denied_field_refs: Vec<(&str, &str)> = denied_fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let (denied_status, denied_body) =
            post_object(&bucket, &denied_field_refs, file_data, "test.txt");
        assert_eq!(
            denied_status, 403,
            "expected 403, got {} body={}",
            denied_status, denied_body
        );
        assert_error_code(&denied_body, "AccessDenied");

        let head = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("head object after denied POST")
            .await;
        assert_eq!(err_status(&head), 404);

        let mut allowed_fields = sigv4_fields(
            &bucket,
            key,
            &[serde_json::json!({"x-amz-server-side-encryption": "AES256"})],
        );
        allowed_fields.push((
            "x-amz-server-side-encryption".to_string(),
            "AES256".to_string(),
        ));
        let allowed_field_refs: Vec<(&str, &str)> = allowed_fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let (allowed_status, allowed_body) =
            post_object(&bucket, &allowed_field_refs, file_data, "test.txt");
        assert_eq!(
            allowed_status, 204,
            "expected 204, got {} body={}",
            allowed_status, allowed_body
        );

        let get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(
            get.server_side_encryption(),
            Some(&ServerSideEncryption::Aes256)
        );
        let data = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], file_data);

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_post_object_bucket_policy_object_creation_operation_condition_is_absent() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let post_key = "post-object-conditional-write-object-creation";
        let put_allowed_key = "post-object-conditional-write-put-allowed";
        let convergence_key = "post-object-conditional-write-convergence";
        let file_data = b"POST Object does not set ObjectCreationOperation";

        let policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:PutObject",
                "Resource": format!("arn:aws:s3:::{bucket}/*"),
                "Condition": {
                    "Bool": {
                        "s3:ObjectCreationOperation": "true"
                    },
                    "Null": {
                        "s3:if-none-match": "true"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();
        wait_for_put_object_access_denied(client, &bucket, convergence_key).await;

        client
            .put_object()
            .bucket(&bucket)
            .key(put_allowed_key)
            .if_none_match("*")
            .body(ByteStream::from_static(b"PUT with If-None-Match"))
            .send()
            .await
            .unwrap();

        let fields = sigv4_fields(&bucket, post_key, &[]);
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let (status, body) = post_object(&bucket, &field_refs, file_data, "test.txt");
        assert_eq!(status, 204, "expected 204, got {status} body={body}");

        let get = client
            .get_object()
            .bucket(&bucket)
            .key(post_key)
            .send()
            .await
            .unwrap();
        let data = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], file_data);

        client
            .delete_object()
            .bucket(&bucket)
            .key(post_key)
            .send()
            .await
            .unwrap();

        client
            .delete_object()
            .bucket(&bucket)
            .key(put_allowed_key)
            .send()
            .await
            .unwrap();
        client
            .delete_object()
            .bucket(&bucket)
            .key(convergence_key)
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_post_object_bucket_policy_if_none_match_header_is_policy_only() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let missing_key = "post-object-if-none-match-missing";
        let header_key = "post-object-if-none-match-header";
        let convergence_key = "post-object-if-none-match-convergence";
        let file_data = b"POST Object with If-None-Match";

        let policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:PutObject",
                "Resource": format!("arn:aws:s3:::{bucket}/*"),
                "Condition": {
                    "Null": {
                        "s3:if-none-match": "true"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();
        wait_for_put_object_access_denied(client, &bucket, convergence_key).await;

        let fields = sigv4_fields(&bucket, missing_key, &[]);
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let (status, body) = post_object(&bucket, &field_refs, file_data, "test.txt");
        assert_eq!(status, 403, "expected 403, got {status} body={body}");
        assert_error_code(&body, "AccessDenied");

        let fields = sigv4_fields(&bucket, header_key, &[]);
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let (status, body) = post_object_with_headers(
            &bucket,
            &field_refs,
            file_data,
            "test.txt",
            &[("If-None-Match", "*")],
        );
        assert_eq!(status, 204, "expected 204, got {status} body={body}");

        let repeat = post_object_with_headers(
            &bucket,
            &field_refs,
            b"overwrite",
            "test.txt",
            &[("If-None-Match", "*")],
        );
        assert_eq!(
            repeat.0, 204,
            "expected 204, got {} body={}",
            repeat.0, repeat.1
        );

        let get = client
            .get_object()
            .bucket(&bucket)
            .key(header_key)
            .send()
            .await
            .unwrap();
        let data = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"overwrite");

        client
            .delete_object()
            .bucket(&bucket)
            .key(header_key)
            .send()
            .await
            .unwrap();
        client
            .delete_object()
            .bucket(&bucket)
            .key(convergence_key)
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_post_object_bucket_policy_if_none_match_string_equals() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let missing_key = "post-object-if-none-match-equals-missing";
        let header_key = "post-object-if-none-match-equals-header";
        let convergence_key = "post-object-if-none-match-equals-convergence";
        let file_data = b"POST Object with exact If-None-Match policy";

        let policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": "s3:PutObject",
                    "Resource": format!("arn:aws:s3:::{bucket}/*"),
                    "Condition": {
                        "Null": {
                            "s3:if-none-match": "true"
                        }
                    }
                },
                {
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": "s3:PutObject",
                    "Resource": format!("arn:aws:s3:::{bucket}/*"),
                    "Condition": {
                        "StringNotEquals": {
                            "s3:if-none-match": "*"
                        }
                    }
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();
        wait_for_put_object_access_denied(client, &bucket, convergence_key).await;

        let fields = sigv4_fields(&bucket, missing_key, &[]);
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let (status, body) = post_object(&bucket, &field_refs, file_data, "test.txt");
        assert_eq!(status, 403, "expected 403, got {status} body={body}");
        assert_error_code(&body, "AccessDenied");

        let fields = sigv4_fields(&bucket, header_key, &[]);
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let (status, body) = post_object_with_headers(
            &bucket,
            &field_refs,
            file_data,
            "test.txt",
            &[("If-None-Match", "*")],
        );
        assert_eq!(status, 204, "expected 204, got {status} body={body}");

        let repeat = post_object_with_headers(
            &bucket,
            &field_refs,
            b"overwrite",
            "test.txt",
            &[("If-None-Match", "*")],
        );
        assert_eq!(
            repeat.0, 204,
            "expected 204, got {} body={}",
            repeat.0, repeat.1
        );

        let get = client
            .get_object()
            .bucket(&bucket)
            .key(header_key)
            .send()
            .await
            .unwrap();
        let data = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"overwrite");

        client
            .delete_object()
            .bucket(&bucket)
            .key(header_key)
            .send()
            .await
            .unwrap();
        client
            .delete_object()
            .bucket(&bucket)
            .key(convergence_key)
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_post_object_bucket_policy_if_match_string_equals_is_policy_only() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let missing_key = "post-object-if-match-equals-missing";
        let wrong_key = "post-object-if-match-equals-wrong";
        let header_key = "post-object-if-match-equals-header";
        let convergence_key = "post-object-if-match-equals-convergence";
        let expected_entity_tag = "post-policy-etag";
        let expected_if_match_header = format!("\"{expected_entity_tag}\"");
        let file_data = b"POST Object with exact If-Match policy";

        let policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": "s3:PutObject",
                    "Resource": format!("arn:aws:s3:::{bucket}/*"),
                    "Condition": {
                        "Null": {
                            "s3:if-match": "true"
                        }
                    }
                },
                {
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": "s3:PutObject",
                    "Resource": format!("arn:aws:s3:::{bucket}/*"),
                    "Condition": {
                        "StringNotEquals": {
                            "s3:if-match": expected_if_match_header
                        }
                    }
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let convergence_fields = sigv4_fields(&bucket, convergence_key, &[]);
        let convergence_field_refs: Vec<(&str, &str)> = convergence_fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        for attempt in 0..20 {
            let (status, _) = post_object_with_headers(
                &bucket,
                &convergence_field_refs,
                b"policy-convergence",
                "test.txt",
                &[("If-Match", "\"wrong\"")],
            );
            if status == 403 {
                break;
            }
            if attempt + 1 == 20 {
                panic!("PutObject policy denial did not converge");
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }

        let fields = sigv4_fields(&bucket, missing_key, &[]);
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let (status, body) = post_object(&bucket, &field_refs, file_data, "test.txt");
        assert_eq!(status, 403, "expected 403, got {status} body={body}");
        assert_error_code(&body, "AccessDenied");

        let fields = sigv4_fields(&bucket, wrong_key, &[]);
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let (status, body) = post_object_with_headers(
            &bucket,
            &field_refs,
            file_data,
            "test.txt",
            &[("If-Match", "\"wrong\"")],
        );
        assert_eq!(status, 403, "expected 403, got {status} body={body}");
        assert_error_code(&body, "AccessDenied");

        let fields = sigv4_fields(&bucket, header_key, &[]);
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let (status, body) = post_object_with_headers(
            &bucket,
            &field_refs,
            file_data,
            "test.txt",
            &[("If-Match", expected_if_match_header.as_str())],
        );
        assert_eq!(status, 204, "expected 204, got {status} body={body}");

        let repeat = post_object_with_headers(
            &bucket,
            &field_refs,
            b"overwrite",
            "test.txt",
            &[("If-Match", expected_if_match_header.as_str())],
        );
        assert_eq!(
            repeat.0, 204,
            "expected 204, got {} body={}",
            repeat.0, repeat.1
        );

        let get = client
            .get_object()
            .bucket(&bucket)
            .key(header_key)
            .send()
            .await
            .unwrap();
        let data = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"overwrite");

        client
            .delete_object()
            .bucket(&bucket)
            .key(header_key)
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_post_object_sse_c_requires_complete_form_fields() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "post-sse-c-missing-md5";
        let customer_key = test_sse_c_key();
        let (key_b64, _) = sse_c_header_values(&customer_key);

        let mut fields = sigv4_fields(
            &bucket,
            key,
            &[
                serde_json::json!({"x-amz-server-side-encryption-customer-algorithm": "AES256"}),
                serde_json::json!({"x-amz-server-side-encryption-customer-key": &key_b64}),
            ],
        );
        fields.push((
            "x-amz-server-side-encryption-customer-algorithm".to_string(),
            "AES256".to_string(),
        ));
        fields.push((
            "x-amz-server-side-encryption-customer-key".to_string(),
            key_b64.clone(),
        ));
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let (status, body) = post_object(&bucket, &field_refs, b"secret", "test.txt");
        assert_eq!(status, 400, "expected 400, got {} body={}", status, body);
        assert_error_code(&body, "InvalidArgument");

        let get_result = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("get object after invalid POST")
            .await;
        assert_eq!(err_status(&get_result), 404);
        assert_s3_err_code(&get_result, "NoSuchKey");

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_post_object_sse_c_headers_without_form_fields_are_ignored() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_sse_c_bucket().await;
        let key = "post-sse-c-headers-only";
        let file_data = b"hello from POST with header-only SSE-C";
        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);

        let fields = sigv4_fields(&bucket, key, &[]);
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let headers = [
            ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
            (
                "x-amz-server-side-encryption-customer-key",
                key_b64.as_str(),
            ),
            (
                "x-amz-server-side-encryption-customer-key-md5",
                key_md5_b64.as_str(),
            ),
        ];
        let (status, body) =
            post_object_with_headers(&bucket, &field_refs, file_data, "test.txt", &headers);
        assert_eq!(status, 204, "expected 204, got {} body={}", status, body);

        let head = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(head.content_length(), Some(file_data.len() as i64));
        assert_eq!(head.sse_customer_algorithm(), None);
        assert_eq!(head.sse_customer_key_md5(), None);

        let get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(get.sse_customer_algorithm(), None);
        assert_eq!(get.sse_customer_key_md5(), None);
        let data = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], file_data);

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_post_object_authenticated_no_content_type() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "post-no-ct";
        let file_data = b"data without content type";

        let fields = sigv4_fields(&bucket, key, &[]);
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let (status, _) = post_object(&bucket, &field_refs, file_data, "test.bin");
        assert_eq!(status, 204, "expected 204, got {}", status);

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_post_object_set_content_type() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "post-ct";
        let file_data = b"text content";

        let mut fields = sigv4_fields(
            &bucket,
            key,
            &[serde_json::json!(["starts-with", "$Content-Type", "text/"])],
        );
        fields.push(("Content-Type".to_string(), "text/plain".to_string()));
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let (status, _) = post_object(&bucket, &field_refs, file_data, "test.txt");
        assert_eq!(status, 204, "expected 204, got {}", status);

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_type(), Some("text/plain"));

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_post_object_empty_body() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "post-empty";

        let fields = sigv4_fields(&bucket, key, &[]);
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let (status, _) = post_object(&bucket, &field_refs, b"", "empty.txt");
        assert_eq!(status, 204, "expected 204, got {}", status);

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(data.len(), 0);

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── success_action_status ───────────────────────────────────────────────

#[test]
fn test_post_object_set_success_code() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "post-success-201";

        let mut fields = sigv4_fields(
            &bucket,
            key,
            &[serde_json::json!([
                "starts-with",
                "$success_action_status",
                ""
            ])],
        );
        fields.push(("success_action_status".to_string(), "201".to_string()));
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let (status, body) = post_object(&bucket, &field_refs, b"data", "test.txt");
        assert_eq!(status, 201, "expected 201, got {}", status);
        assert!(
            body.contains("<Bucket>"),
            "expected Bucket in XML: {}",
            body
        );
        assert!(body.contains("<Key>"), "expected Key in XML: {}", body);
        assert!(body.contains("<ETag>"), "expected ETag in XML: {}", body);

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_post_object_set_success_code_200() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "post-200";

        let mut fields = sigv4_fields(
            &bucket,
            key,
            &[serde_json::json!([
                "starts-with",
                "$success_action_status",
                ""
            ])],
        );
        fields.push(("success_action_status".to_string(), "200".to_string()));
        let expected_location = format!("{}/{bucket}/{key}", CTX.endpoint());

        let resp = post_object_raw(&bucket, &fields, b"data", "test.txt");
        assert_eq!(resp.status, 200, "expected 200, got {:?}", resp);
        assert_eq!(resp.body, "");
        assert_eq!(
            response_header_value(&resp, "Location"),
            Some(expected_location.as_str())
        );
        assert_eq!(
            response_header_value(&resp, "x-amz-server-side-encryption"),
            Some("AES256")
        );
        assert_eq!(
            response_header_value(&resp, "x-amz-checksum-type"),
            Some("FULL_OBJECT")
        );
        assert!(
            response_header_value(&resp, "x-amz-checksum-crc64nvme").is_some(),
            "expected checksum header in {:?}",
            resp.headers
        );

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_post_object_set_success_code_201() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "post-201";

        let mut fields = sigv4_fields(
            &bucket,
            key,
            &[serde_json::json!([
                "starts-with",
                "$success_action_status",
                ""
            ])],
        );
        fields.push(("success_action_status".to_string(), "201".to_string()));
        let expected_location = format!("{}/{bucket}/{key}", CTX.endpoint());

        let resp = post_object_raw(&bucket, &fields, b"data", "test.txt");
        assert_eq!(resp.status, 201, "expected 201, got {:?}", resp);
        assert_eq!(
            response_header_value(&resp, "Location"),
            Some(expected_location.as_str())
        );
        assert_eq!(
            response_header_value(&resp, "x-amz-server-side-encryption"),
            Some("AES256")
        );
        assert_eq!(
            response_header_value(&resp, "x-amz-checksum-type"),
            Some("FULL_OBJECT")
        );
        assert!(
            response_header_value(&resp, "x-amz-checksum-crc64nvme").is_some(),
            "expected checksum header in {:?}",
            resp.headers
        );
        // 201 response should contain XML with Location, Bucket, Key, ETag
        assert!(
            resp.body.contains("<Bucket>"),
            "expected Bucket in XML: {}",
            resp.body
        );
        assert!(
            resp.body.contains("<Key>"),
            "expected Key in XML: {}",
            resp.body
        );
        assert!(
            resp.body.contains("<ETag>"),
            "expected ETag in XML: {}",
            resp.body
        );

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_post_object_set_invalid_success_code() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "post-invalid-code";

        let mut fields = sigv4_fields(
            &bucket,
            key,
            &[serde_json::json!([
                "starts-with",
                "$success_action_status",
                ""
            ])],
        );
        fields.push(("success_action_status".to_string(), "999".to_string()));
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let (status, _) = post_object(&bucket, &field_refs, b"data", "test.txt");
        // Invalid code defaults to 204
        assert_eq!(status, 204);

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── Key handling ────────────────────────────────────────────────────────

#[test]
fn test_post_object_set_key_from_filename() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key_template = "uploads/${filename}";
        let file_name = "myfile.txt";
        let expected_key = "uploads/myfile.txt";

        let (short_date, full_date) = current_dates();
        let secret = CTX.secret_key();
        let access_key = CTX.access_key();
        let region = CTX.region();
        let credential = format!("{}/{}/{}/s3/aws4_request", access_key, short_date, region);

        // Policy uses starts-with for key to allow ${filename} substitution
        let policy_b64 = make_policy_raw(
            &epoch_to_iso8601(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    + 3600,
            ),
            &[
                serde_json::json!({"bucket": bucket}),
                serde_json::json!(["starts-with", "$key", "uploads/"]),
                serde_json::json!({"x-amz-algorithm": "AWS4-HMAC-SHA256"}),
                serde_json::json!({"x-amz-credential": &credential}),
                serde_json::json!({"x-amz-date": &full_date}),
            ],
        );
        let signature = sign_policy_v4(&policy_b64, secret, &short_date, region);

        let fields: Vec<(&str, &str)> = vec![
            ("key", key_template),
            ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
            ("x-amz-credential", &credential),
            ("x-amz-date", &full_date),
            ("policy", &policy_b64),
            ("x-amz-signature", &signature),
        ];

        let (status, _) = post_object(&bucket, &fields, b"file content", file_name);
        assert_eq!(status, 204, "expected 204, got {}", status);

        // Verify the key was resolved
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(expected_key)
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"file content");

        client
            .delete_object()
            .bucket(&bucket)
            .key(expected_key)
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_post_object_no_key_specified() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;

        let (short_date, full_date) = current_dates();
        let secret = CTX.secret_key();
        let access_key = CTX.access_key();
        let region = CTX.region();
        let credential = format!("{}/{}/{}/s3/aws4_request", access_key, short_date, region);

        let policy_b64 = make_policy_raw(
            &epoch_to_iso8601(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    + 3600,
            ),
            &[serde_json::json!({"bucket": bucket})],
        );
        let signature = sign_policy_v4(&policy_b64, secret, &short_date, region);

        // No "key" field
        let fields: Vec<(&str, &str)> = vec![
            ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
            ("x-amz-credential", &credential),
            ("x-amz-date", &full_date),
            ("policy", &policy_b64),
            ("x-amz-signature", &signature),
        ];

        let (status, _) = post_object(&bucket, &fields, b"data", "test.txt");
        assert!(status >= 400, "expected error status, got {}", status);

        s3_tests::delete_bucket_retrying_operation_aborted(CTX.client(), &bucket).await;
    });
}

#[test]
fn test_post_object_missing_file() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "post-no-file";

        let fields = sigv4_fields(&bucket, key, &[]);
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        // Send with no file field — build multipart manually without file part
        let boundary = "----TestBoundary7MA4YWxkTrZu0gW";
        let mut body = Vec::new();
        for (name, value) in &field_refs {
            body.extend_from_slice(format!("--{}\r\n", boundary).as_bytes());
            body.extend_from_slice(
                format!("Content-Disposition: form-data; name=\"{}\"\r\n\r\n", name).as_bytes(),
            );
            body.extend_from_slice(value.as_bytes());
            body.extend_from_slice(b"\r\n");
        }
        body.extend_from_slice(format!("--{}--\r\n", boundary).as_bytes());
        let content_type = format!("multipart/form-data; boundary={}", boundary);

        let url = format!("{}/{}", CTX.endpoint(), bucket);
        let mut resp = agent()
            .post(&url)
            .header("Content-Type", &content_type)
            .send(&body[..])
            .expect("HTTP transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();

        assert!(status >= 400, "expected error status, got {}", status);

        s3_tests::delete_bucket_retrying_operation_aborted(CTX.client(), &bucket).await;
    });
}

// ── Auth errors ─────────────────────────────────────────────────────────

#[test]
fn test_post_object_authenticated_request_bad_access_key() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "post-bad-key";

        let (short_date, full_date) = current_dates();
        let region = CTX.region();
        let credential = format!("INVALIDKEY/{}/{}/s3/aws4_request", short_date, region);
        let policy_b64 = make_policy(&bucket, key, 3600, &[]);
        let signature = sign_policy_v4(&policy_b64, "fake-secret", &short_date, region);

        let fields: Vec<(&str, &str)> = vec![
            ("key", key),
            ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
            ("x-amz-credential", &credential),
            ("x-amz-date", &full_date),
            ("policy", &policy_b64),
            ("x-amz-signature", &signature),
        ];

        let (status, _) = post_object(&bucket, &fields, b"data", "test.txt");
        assert_eq!(status, 403, "expected 403, got {}", status);

        s3_tests::delete_bucket_retrying_operation_aborted(CTX.client(), &bucket).await;
    });
}

#[test]
fn test_post_object_invalid_signature() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "post-bad-sig";

        let (short_date, full_date) = current_dates();
        let access_key = CTX.access_key();
        let region = CTX.region();
        let credential = format!("{}/{}/{}/s3/aws4_request", access_key, short_date, region);
        let policy_b64 = make_policy(&bucket, key, 3600, &[]);

        let fields: Vec<(&str, &str)> = vec![
            ("key", key),
            ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
            ("x-amz-credential", &credential),
            ("x-amz-date", &full_date),
            ("policy", &policy_b64),
            (
                "x-amz-signature",
                "0000000000000000000000000000000000000000000000000000000000000000",
            ),
        ];

        let (status, _) = post_object(&bucket, &fields, b"data", "test.txt");
        assert_eq!(status, 403, "expected 403, got {}", status);

        s3_tests::delete_bucket_retrying_operation_aborted(CTX.client(), &bucket).await;
    });
}

#[test]
fn test_post_object_missing_policy() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "post-no-policy";

        let (short_date, full_date) = current_dates();
        let access_key = CTX.access_key();
        let region = CTX.region();
        let credential = format!("{}/{}/{}/s3/aws4_request", access_key, short_date, region);

        // SigV4 fields present but no policy
        let fields: Vec<(&str, &str)> = vec![
            ("key", key),
            ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
            ("x-amz-credential", &credential),
            ("x-amz-date", &full_date),
            ("x-amz-signature", "abc"),
        ];

        let (status, _) = post_object(&bucket, &fields, b"data", "test.txt");
        assert!(status >= 400, "expected error status, got {}", status);

        s3_tests::delete_bucket_retrying_operation_aborted(CTX.client(), &bucket).await;
    });
}

#[test]
fn test_post_object_missing_signature() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "post-no-sig";

        let (short_date, full_date) = current_dates();
        let access_key = CTX.access_key();
        let region = CTX.region();
        let credential = format!("{}/{}/{}/s3/aws4_request", access_key, short_date, region);
        let policy_b64 = make_policy(&bucket, key, 3600, &[]);

        // SigV4 fields present but no signature
        let fields: Vec<(&str, &str)> = vec![
            ("key", key),
            ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
            ("x-amz-credential", &credential),
            ("x-amz-date", &full_date),
            ("policy", &policy_b64),
        ];

        let (status, _) = post_object(&bucket, &fields, b"data", "test.txt");
        assert!(status >= 400, "expected error status, got {}", status);

        s3_tests::delete_bucket_retrying_operation_aborted(CTX.client(), &bucket).await;
    });
}

// ── Policy: expiration ──────────────────────────────────────────────────

#[test]
fn test_post_object_expired_policy() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "post-expired";

        let (short_date, full_date) = current_dates();
        let secret = CTX.secret_key();
        let access_key = CTX.access_key();
        let region = CTX.region();
        let credential = format!("{}/{}/{}/s3/aws4_request", access_key, short_date, region);

        // Policy that expired in the past
        let policy_b64 = make_policy_raw(
            "2020-01-01T00:00:00Z",
            &[
                serde_json::json!({"bucket": bucket}),
                serde_json::json!({"key": key}),
            ],
        );
        let signature = sign_policy_v4(&policy_b64, secret, &short_date, region);

        let fields: Vec<(&str, &str)> = vec![
            ("key", key),
            ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
            ("x-amz-credential", &credential),
            ("x-amz-date", &full_date),
            ("policy", &policy_b64),
            ("x-amz-signature", &signature),
        ];

        let (status, _) = post_object(&bucket, &fields, b"data", "test.txt");
        assert_eq!(
            status, 403,
            "expected 403 for expired policy, got {}",
            status
        );

        s3_tests::delete_bucket_retrying_operation_aborted(CTX.client(), &bucket).await;
    });
}

#[test]
fn test_post_object_invalid_date_format() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "post-bad-date";

        let (short_date, full_date) = current_dates();
        let secret = CTX.secret_key();
        let access_key = CTX.access_key();
        let region = CTX.region();
        let credential = format!("{}/{}/{}/s3/aws4_request", access_key, short_date, region);

        // Policy with malformed expiration
        let policy_b64 = make_policy_raw(
            "not-a-date",
            &[
                serde_json::json!({"bucket": bucket}),
                serde_json::json!({"key": key}),
            ],
        );
        let signature = sign_policy_v4(&policy_b64, secret, &short_date, region);

        let fields: Vec<(&str, &str)> = vec![
            ("key", key),
            ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
            ("x-amz-credential", &credential),
            ("x-amz-date", &full_date),
            ("policy", &policy_b64),
            ("x-amz-signature", &signature),
        ];

        let (status, _) = post_object(&bucket, &fields, b"data", "test.txt");
        assert_eq!(
            status, 400,
            "expected 400 for malformed date, got {}",
            status
        );

        s3_tests::delete_bucket_retrying_operation_aborted(CTX.client(), &bucket).await;
    });
}

#[test]
fn test_post_object_expires_is_case_sensitive() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "post-case-exp";

        let (short_date, full_date) = current_dates();
        let secret = CTX.secret_key();
        let access_key = CTX.access_key();
        let region = CTX.region();
        let credential = format!("{}/{}/{}/s3/aws4_request", access_key, short_date, region);

        // Policy with "Expiration" (capital E) instead of "expiration"
        use base64::Engine;
        let policy_json = serde_json::json!({
            "Expiration": "2099-01-01T00:00:00Z",
            "conditions": [
                {"bucket": bucket},
                {"key": key},
            ],
        });
        let policy_b64 =
            base64::engine::general_purpose::STANDARD.encode(policy_json.to_string().as_bytes());
        let signature = sign_policy_v4(&policy_b64, secret, &short_date, region);

        let fields: Vec<(&str, &str)> = vec![
            ("key", key),
            ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
            ("x-amz-credential", &credential),
            ("x-amz-date", &full_date),
            ("policy", &policy_b64),
            ("x-amz-signature", &signature),
        ];

        let (status, _) = post_object(&bucket, &fields, b"data", "test.txt");
        assert_eq!(
            status, 400,
            "expected 400 for wrong-case expiration, got {}",
            status
        );

        s3_tests::delete_bucket_retrying_operation_aborted(CTX.client(), &bucket).await;
    });
}

// ── Policy: conditions ──────────────────────────────────────────────────

#[test]
fn test_post_object_empty_conditions() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "post-empty-cond";

        let (short_date, full_date) = current_dates();
        let secret = CTX.secret_key();
        let access_key = CTX.access_key();
        let region = CTX.region();
        let credential = format!("{}/{}/{}/s3/aws4_request", access_key, short_date, region);

        // Empty conditions array — S3 rejects this because bucket/key are not validated
        let policy_b64 = make_policy_raw(
            &epoch_to_iso8601(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    + 3600,
            ),
            &[],
        );
        let signature = sign_policy_v4(&policy_b64, secret, &short_date, region);

        let fields: Vec<(&str, &str)> = vec![
            ("key", key),
            ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
            ("x-amz-credential", &credential),
            ("x-amz-date", &full_date),
            ("policy", &policy_b64),
            ("x-amz-signature", &signature),
        ];

        let (status, _) = post_object(&bucket, &fields, b"data", "test.txt");
        assert_eq!(status, 400, "expected 400, got {}", status);

        let _ = CTX
            .client()
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await;
        s3_tests::delete_bucket_retrying_operation_aborted(CTX.client(), &bucket).await;
    });
}

#[test]
fn test_post_object_missing_conditions_list() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "post-no-conditions";

        let (short_date, full_date) = current_dates();
        let secret = CTX.secret_key();
        let access_key = CTX.access_key();
        let region = CTX.region();
        let credential = format!("{}/{}/{}/s3/aws4_request", access_key, short_date, region);

        // Policy without "conditions" key
        use base64::Engine;
        let policy_json = serde_json::json!({
            "expiration": "2099-01-01T00:00:00Z",
        });
        let policy_b64 =
            base64::engine::general_purpose::STANDARD.encode(policy_json.to_string().as_bytes());
        let signature = sign_policy_v4(&policy_b64, secret, &short_date, region);

        let fields: Vec<(&str, &str)> = vec![
            ("key", key),
            ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
            ("x-amz-credential", &credential),
            ("x-amz-date", &full_date),
            ("policy", &policy_b64),
            ("x-amz-signature", &signature),
        ];

        let (status, _) = post_object(&bucket, &fields, b"data", "test.txt");
        assert_eq!(
            status, 400,
            "expected 400 for missing conditions, got {}",
            status
        );

        s3_tests::delete_bucket_retrying_operation_aborted(CTX.client(), &bucket).await;
    });
}

#[test]
fn test_post_object_condition_is_case_sensitive() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "post-case-cond";

        let (short_date, full_date) = current_dates();
        let secret = CTX.secret_key();
        let access_key = CTX.access_key();
        let region = CTX.region();
        let credential = format!("{}/{}/{}/s3/aws4_request", access_key, short_date, region);

        // "Bucket" (capital B) in condition — the special "bucket" condition key
        // is case-sensitive (must be lowercase). "Bucket" is treated as an unknown
        // form-field condition, so the actual "bucket" is unconstrained. AWS returns
        // 403 for this.
        let policy_b64 = make_policy_raw(
            &epoch_to_iso8601(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    + 3600,
            ),
            &[
                serde_json::json!({"Bucket": bucket}),
                serde_json::json!({"key": key}),
            ],
        );
        let signature = sign_policy_v4(&policy_b64, secret, &short_date, region);

        let fields: Vec<(&str, &str)> = vec![
            ("key", key),
            ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
            ("x-amz-credential", &credential),
            ("x-amz-date", &full_date),
            ("policy", &policy_b64),
            ("x-amz-signature", &signature),
        ];

        let (status, _) = post_object(&bucket, &fields, b"data", "test.txt");
        assert_eq!(status, 403, "expected 403, got {}", status);

        let _ = CTX
            .client()
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await;
        s3_tests::delete_bucket_retrying_operation_aborted(CTX.client(), &bucket).await;
    });
}

#[test]
fn test_post_object_case_insensitive_condition_fields() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "post-ci-fields";

        let (short_date, full_date) = current_dates();
        let secret = CTX.secret_key();
        let access_key = CTX.access_key();
        let region = CTX.region();
        let credential = format!("{}/{}/{}/s3/aws4_request", access_key, short_date, region);

        // "Key" (capital K) in condition — should still match "key" form field
        let policy_b64 = make_policy_raw(
            &epoch_to_iso8601(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    + 3600,
            ),
            &[
                serde_json::json!({"bucket": bucket}),
                serde_json::json!({"Key": key}),
                serde_json::json!({"x-amz-algorithm": "AWS4-HMAC-SHA256"}),
                serde_json::json!({"x-amz-credential": &credential}),
                serde_json::json!({"x-amz-date": &full_date}),
            ],
        );
        let signature = sign_policy_v4(&policy_b64, secret, &short_date, region);

        let fields: Vec<(&str, &str)> = vec![
            ("key", key),
            ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
            ("x-amz-credential", &credential),
            ("x-amz-date", &full_date),
            ("policy", &policy_b64),
            ("x-amz-signature", &signature),
        ];

        let (status, _) = post_object(&bucket, &fields, b"data", "test.txt");
        assert_eq!(status, 204, "expected 204, got {}", status);

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_post_object_escaped_field_values() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "post/special chars & more";

        let fields = sigv4_fields(&bucket, key, &[]);
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let (status, _) = post_object(&bucket, &field_refs, b"data", "test.txt");
        assert_eq!(status, 204, "expected 204, got {}", status);

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"data");

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

/// Policy with only bucket + starts-with $key but no SigV4 conditions → 403
#[test]
fn test_post_object_missing_sigv4_policy_conditions() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key_template = "uploads/${filename}";

        let (short_date, full_date) = current_dates();
        let secret = CTX.secret_key();
        let access_key = CTX.access_key();
        let region = CTX.region();
        let credential = format!("{}/{}/{}/s3/aws4_request", access_key, short_date, region);

        // Policy intentionally omits SigV4 conditions
        let policy_b64 = make_policy_raw(
            &epoch_to_iso8601(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    + 3600,
            ),
            &[
                serde_json::json!({"bucket": bucket}),
                serde_json::json!(["starts-with", "$key", "uploads/"]),
            ],
        );
        let signature = sign_policy_v4(&policy_b64, secret, &short_date, region);

        let fields: Vec<(&str, &str)> = vec![
            ("key", key_template),
            ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
            ("x-amz-credential", &credential),
            ("x-amz-date", &full_date),
            ("policy", &policy_b64),
            ("x-amz-signature", &signature),
        ];

        let (status, _) = post_object(&bucket, &fields, b"file content", "myfile.txt");
        assert_eq!(status, 403, "expected 403, got {}", status);

        // Cleanup
        let _ = CTX
            .client()
            .delete_object()
            .bucket(&bucket)
            .key("uploads/myfile.txt")
            .send()
            .await;
        s3_tests::delete_bucket_retrying_operation_aborted(CTX.client(), &bucket).await;
    });
}

#[test]
fn test_post_object_missing_policy_condition() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "post-extra-field";

        let (short_date, full_date) = current_dates();
        let secret = CTX.secret_key();
        let access_key = CTX.access_key();
        let region = CTX.region();
        let credential = format!("{}/{}/{}/s3/aws4_request", access_key, short_date, region);

        // Policy doesn't include Content-Type condition, but form has it — AWS rejects with 403
        let policy_b64 = make_policy(&bucket, key, 3600, &[]);
        let signature = sign_policy_v4(&policy_b64, secret, &short_date, region);

        let fields: Vec<(&str, &str)> = vec![
            ("key", key),
            ("Content-Type", "text/plain"),
            ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
            ("x-amz-credential", &credential),
            ("x-amz-date", &full_date),
            ("policy", &policy_b64),
            ("x-amz-signature", &signature),
        ];

        let (status, _) = post_object(&bucket, &fields, b"data", "test.txt");
        assert_eq!(status, 403, "expected 403, got {}", status);

        // Cleanup
        let _ = CTX
            .client()
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await;
        s3_tests::delete_bucket_retrying_operation_aborted(CTX.client(), &bucket).await;
    });
}

#[test]
fn test_post_object_request_missing_policy_specified_field() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "post-missing-field";

        let (short_date, full_date) = current_dates();
        let secret = CTX.secret_key();
        let access_key = CTX.access_key();
        let region = CTX.region();
        let credential = format!("{}/{}/{}/s3/aws4_request", access_key, short_date, region);

        // Policy requires Content-Type to be "text/plain" but form doesn't include it
        let policy_b64 = make_policy_raw(
            &epoch_to_iso8601(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    + 3600,
            ),
            &[
                serde_json::json!({"bucket": bucket}),
                serde_json::json!({"key": key}),
                serde_json::json!({"Content-Type": "text/plain"}),
            ],
        );
        let signature = sign_policy_v4(&policy_b64, secret, &short_date, region);

        let fields: Vec<(&str, &str)> = vec![
            ("key", key),
            ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
            ("x-amz-credential", &credential),
            ("x-amz-date", &full_date),
            ("policy", &policy_b64),
            ("x-amz-signature", &signature),
        ];

        let (status, _) = post_object(&bucket, &fields, b"data", "test.txt");
        assert_eq!(
            status, 403,
            "expected 403 for missing required field, got {}",
            status
        );

        s3_tests::delete_bucket_retrying_operation_aborted(CTX.client(), &bucket).await;
    });
}

#[test]
fn test_post_object_invalid_request_field_value() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "post-wrong-value";

        let (short_date, full_date) = current_dates();
        let secret = CTX.secret_key();
        let access_key = CTX.access_key();
        let region = CTX.region();
        let credential = format!("{}/{}/{}/s3/aws4_request", access_key, short_date, region);

        // Policy requires Content-Type == "text/plain" but form has "image/png"
        let policy_b64 = make_policy_raw(
            &epoch_to_iso8601(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    + 3600,
            ),
            &[
                serde_json::json!({"bucket": bucket}),
                serde_json::json!({"key": key}),
                serde_json::json!({"Content-Type": "text/plain"}),
            ],
        );
        let signature = sign_policy_v4(&policy_b64, secret, &short_date, region);

        let fields: Vec<(&str, &str)> = vec![
            ("key", key),
            ("Content-Type", "image/png"),
            ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
            ("x-amz-credential", &credential),
            ("x-amz-date", &full_date),
            ("policy", &policy_b64),
            ("x-amz-signature", &signature),
        ];

        let (status, _) = post_object(&bucket, &fields, b"data", "test.txt");
        assert_eq!(
            status, 403,
            "expected 403 for value mismatch, got {}",
            status
        );

        s3_tests::delete_bucket_retrying_operation_aborted(CTX.client(), &bucket).await;
    });
}

#[test]
fn test_post_object_starts_with() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "post-starts-with";

        let fields = sigv4_fields(
            &bucket,
            key,
            &[serde_json::json!(["starts-with", "$Content-Type", "text/"])],
        );
        let mut field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        field_refs.push(("Content-Type", "text/html"));

        let (status, _) = post_object(&bucket, &field_refs, b"<html>", "test.html");
        assert_eq!(status, 204, "expected 204, got {}", status);

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_post_object_eq_condition() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "post-eq-cond";

        let fields = sigv4_fields(
            &bucket,
            key,
            &[serde_json::json!([
                "eq",
                "$Content-Type",
                "application/json"
            ])],
        );
        let mut field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        field_refs.push(("Content-Type", "application/json"));

        let (status, _) = post_object(&bucket, &field_refs, b"{}", "data.json");
        assert_eq!(status, 204, "expected 204, got {}", status);

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── Policy: content-length-range ────────────────────────────────────────

#[test]
fn test_post_object_content_length_range() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "post-clr";

        let fields = sigv4_fields(
            &bucket,
            key,
            &[serde_json::json!(["content-length-range", 1, 1024])],
        );
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let (status, _) = post_object(&bucket, &field_refs, b"hello", "test.txt");
        assert_eq!(status, 204, "expected 204, got {}", status);

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_post_object_upload_size_limit_exceeded() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "post-too-large";

        let fields = sigv4_fields(
            &bucket,
            key,
            &[serde_json::json!(["content-length-range", 0, 5])],
        );
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let (status, _) = post_object(&bucket, &field_refs, b"this is way too long", "test.txt");
        assert!(
            status >= 400,
            "expected error for too large, got {}",
            status
        );

        s3_tests::delete_bucket_retrying_operation_aborted(CTX.client(), &bucket).await;
    });
}

#[test]
fn test_post_object_upload_size_below_minimum() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "post-too-small";

        let fields = sigv4_fields(
            &bucket,
            key,
            &[serde_json::json!(["content-length-range", 100, 1024])],
        );
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let (status, _) = post_object(&bucket, &field_refs, b"tiny", "test.txt");
        assert!(
            status >= 400,
            "expected error for too small, got {}",
            status
        );

        s3_tests::delete_bucket_retrying_operation_aborted(CTX.client(), &bucket).await;
    });
}

#[test]
fn test_post_object_missing_content_length_argument() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "post-bad-clr";

        let (status, body) = post_object_with_malformed_policy_condition(
            &bucket,
            key,
            serde_json::json!(["content-length-range", 0]),
        );
        assert_invalid_policy_document_response(
            status,
            &body,
            "Invalid Policy: Invalid content-length-range: wrong number of arguments.",
        );

        s3_tests::delete_bucket_retrying_operation_aborted(CTX.client(), &bucket).await;
    });
}

#[test]
fn test_post_object_policy_long_content_length_range_condition() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "post-long-clr";

        let (status, body) = post_object_with_malformed_policy_condition(
            &bucket,
            key,
            serde_json::json!(["content-length-range", 0, 1024, 2048]),
        );
        assert_invalid_policy_document_response(
            status,
            &body,
            "Invalid Policy: Invalid content-length-range: wrong number of arguments.",
        );

        s3_tests::delete_bucket_retrying_operation_aborted(CTX.client(), &bucket).await;
    });
}

fn post_object_with_malformed_policy_condition(
    bucket: &str,
    key: &str,
    condition: serde_json::Value,
) -> (u16, String) {
    let fields = sigv4_fields(bucket, key, &[condition]);
    let field_refs: Vec<(&str, &str)> = fields
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();

    post_object(bucket, &field_refs, b"data", "test.txt")
}

#[test]
fn test_post_object_policy_short_starts_with_condition() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "post-short-starts-with";

        let (status, body) = post_object_with_malformed_policy_condition(
            &bucket,
            key,
            serde_json::json!(["starts-with", "$key"]),
        );
        assert_invalid_policy_document_response(
            status,
            &body,
            "Invalid Policy: Invalid starts-with: wrong number of arguments.",
        );

        s3_tests::delete_bucket_retrying_operation_aborted(CTX.client(), &bucket).await;
    });
}

#[test]
fn test_post_object_policy_long_starts_with_condition() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "post-long-starts-with";

        let (status, body) = post_object_with_malformed_policy_condition(
            &bucket,
            key,
            serde_json::json!(["starts-with", "$key", "post-", "extra"]),
        );
        assert_invalid_policy_document_response(
            status,
            &body,
            "Invalid Policy: Invalid starts-with: wrong number of arguments.",
        );

        s3_tests::delete_bucket_retrying_operation_aborted(CTX.client(), &bucket).await;
    });
}

#[test]
fn test_post_object_policy_short_eq_condition() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "post-short-eq";

        let (status, body) = post_object_with_malformed_policy_condition(
            &bucket,
            key,
            serde_json::json!(["eq", "$key"]),
        );
        assert_invalid_policy_document_response(
            status,
            &body,
            "Invalid Policy: Invalid Eq: wrong number of arguments.",
        );

        s3_tests::delete_bucket_retrying_operation_aborted(CTX.client(), &bucket).await;
    });
}

#[test]
fn test_post_object_policy_long_eq_condition() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "post-long-eq";

        let (status, body) = post_object_with_malformed_policy_condition(
            &bucket,
            key,
            serde_json::json!(["eq", "$key", "post-long-eq", "extra"]),
        );
        assert_invalid_policy_document_response(
            status,
            &body,
            "Invalid Policy: Invalid Eq: wrong number of arguments.",
        );

        s3_tests::delete_bucket_retrying_operation_aborted(CTX.client(), &bucket).await;
    });
}

#[test]
fn test_post_object_policy_unknown_operator_condition() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "post-unknown-operator";

        let (status, body) = post_object_with_malformed_policy_condition(
            &bucket,
            key,
            serde_json::json!(["unknown-op", "$key", "post-"]),
        );
        assert_invalid_policy_document_response(
            status,
            &body,
            "Invalid Policy: Invalid Condition: unknown operation 'unknown-op'.",
        );

        s3_tests::delete_bucket_retrying_operation_aborted(CTX.client(), &bucket).await;
    });
}

#[test]
fn test_post_object_policy_scalar_condition() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "post-scalar-condition";

        let (status, body) = post_object_with_malformed_policy_condition(
            &bucket,
            key,
            serde_json::json!("just a string"),
        );
        assert_invalid_policy_document_response(
            status,
            &body,
            "Invalid Policy: Invalid condition test: must be a List or Object.",
        );

        s3_tests::delete_bucket_retrying_operation_aborted(CTX.client(), &bucket).await;
    });
}

// ── Redirect ────────────────────────────────────────────────────────────

#[test]
fn test_post_object_success_redirect_action() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "foo.txt";
        let redirect_url = format!("{}/{}", CTX.endpoint(), bucket);

        let mut fields = sigv4_fields(
            &bucket,
            key,
            &[
                serde_json::json!(["eq", "$success_action_redirect", &redirect_url]),
                serde_json::json!(["starts-with", "$Content-Type", "text/plain"]),
            ],
        );
        fields.push(("success_action_redirect".to_string(), redirect_url.clone()));
        fields.push(("Content-Type".to_string(), "text/plain".to_string()));
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let url = format!("{}/{}", CTX.endpoint(), bucket);
        let (content_type, body) = build_multipart(&field_refs, b"bar", "test.txt");
        let mut resp = agent()
            .post(&url)
            .header("Content-Type", &content_type)
            .send(&body[..])
            .expect("HTTP transport error");

        assert_eq!(resp.status().as_u16(), 303);
        assert_eq!(resp.body_mut().read_to_string().unwrap_or_default(), "");

        let head = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let etag = head.e_tag().unwrap().trim_matches('"');
        let location = resp
            .headers()
            .get(hyper::header::LOCATION)
            .and_then(|value| value.to_str().ok())
            .expect("missing Location header");
        let expected_location =
            format!("{redirect_url}?bucket={bucket}&key={key}&etag=%22{etag}%22");
        assert_eq!(location, expected_location);

        let body = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap()
            .body
            .collect()
            .await
            .unwrap()
            .into_bytes();
        assert_eq!(&body[..], b"bar");

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── Metadata ────────────────────────────────────────────────────────────

#[test]
fn test_post_object_metadata() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "post-meta";

        let mut fields = sigv4_fields(
            &bucket,
            key,
            &[serde_json::json!(["starts-with", "$x-amz-meta-custom", ""])],
        );
        fields.push(("x-amz-meta-custom".to_string(), "my-value".to_string()));
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let (status, _) = post_object(&bucket, &field_refs, b"data", "test.txt");
        assert_eq!(status, 204, "expected 204, got {}", status);

        // Verify metadata via HEAD
        let resp = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let meta = resp.metadata().unwrap();
        assert_eq!(meta.get("custom").map(|s| s.as_str()), Some("my-value"));

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_post_object_user_specified_header() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "post-ignored";

        // Include an unknown form field without a policy condition — AWS rejects with 403
        let mut fields = sigv4_fields(&bucket, key, &[]);
        fields.push(("x-unknown-field".to_string(), "whatever".to_string()));
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let (status, _) = post_object(&bucket, &field_refs, b"data", "test.txt");
        assert_eq!(status, 403, "expected 403, got {}", status);

        // Cleanup
        let _ = CTX
            .client()
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await;
        s3_tests::delete_bucket_retrying_operation_aborted(CTX.client(), &bucket).await;
    });
}

#[test]
fn test_post_object_ignored_header() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "post-ignored-hdr";

        // x-ignore-* fields are exempt from policy coverage per AWS docs
        let mut fields = sigv4_fields(&bucket, key, &[]);
        fields.push(("x-ignore-me".to_string(), "value".to_string()));
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let (status, _) = post_object(&bucket, &field_refs, b"data", "test.txt");
        assert_eq!(status, 204, "expected 204, got {}", status);

        // Verify object was created
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"data");

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_post_object_wrong_bucket() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "post-wrong-bucket";

        let (short_date, full_date) = current_dates();
        let secret = CTX.secret_key();
        let access_key = CTX.access_key();
        let region = CTX.region();
        let credential = format!("{}/{}/{}/s3/aws4_request", access_key, short_date, region);

        // Policy says "wrong-bucket" but we POST to actual bucket
        let policy_b64 = make_policy_raw(
            &epoch_to_iso8601(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    + 3600,
            ),
            &[
                serde_json::json!({"bucket": "wrong-bucket-name"}),
                serde_json::json!({"key": key}),
            ],
        );
        let signature = sign_policy_v4(&policy_b64, secret, &short_date, region);

        let fields: Vec<(&str, &str)> = vec![
            ("key", key),
            ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
            ("x-amz-credential", &credential),
            ("x-amz-date", &full_date),
            ("policy", &policy_b64),
            ("x-amz-signature", &signature),
        ];

        let (status, _) = post_object(&bucket, &fields, b"data", "test.txt");
        assert_eq!(status, 403, "expected 403 for wrong bucket, got {}", status);

        s3_tests::delete_bucket_retrying_operation_aborted(CTX.client(), &bucket).await;
    });
}

// ── Checksum ────────────────────────────────────────────────────────────

#[test]
fn test_post_object_upload_checksum() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "post-checksum";
        let file_data = b"checksum test data";

        use base64::Engine;
        let digest = ring::digest::digest(&ring::digest::SHA256, file_data);
        let checksum_b64 = base64::engine::general_purpose::STANDARD.encode(digest.as_ref());

        // Valid checksum → 204
        let mut fields = sigv4_fields(
            &bucket,
            key,
            &[serde_json::json!([
                "starts-with",
                "$x-amz-checksum-sha256",
                ""
            ])],
        );
        fields.push(("x-amz-checksum-sha256".to_string(), checksum_b64));
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let (status, _) = post_object(&bucket, &field_refs, file_data, "test.txt");
        assert_eq!(status, 204, "expected 204, got {}", status);

        // Bad checksum → 400
        let bad_key = "post-checksum-bad";
        let bad_checksum =
            base64::engine::general_purpose::STANDARD.encode(b"wrong-digest-value-here!!!!!!!!");
        let mut bad_fields = sigv4_fields(
            &bucket,
            bad_key,
            &[serde_json::json!([
                "starts-with",
                "$x-amz-checksum-sha256",
                ""
            ])],
        );
        bad_fields.push(("x-amz-checksum-sha256".to_string(), bad_checksum));
        let bad_field_refs: Vec<(&str, &str)> = bad_fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let (bad_status, _) = post_object(&bucket, &bad_field_refs, file_data, "test.txt");
        assert_eq!(
            bad_status, 400,
            "expected 400 for bad checksum, got {}",
            bad_status
        );

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── Large file ──────────────────────────────────────────────────────────

#[test]
fn test_post_object_upload_larger_than_chunk() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "post-large";
        let file_data = vec![0x42u8; 1024 * 1024]; // 1MB

        let fields = sigv4_fields(
            &bucket,
            key,
            &[serde_json::json!(["content-length-range", 0, 2_000_000])],
        );
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let (status, _) = post_object(&bucket, &field_refs, &file_data, "large.bin");
        assert_eq!(status, 204, "expected 204, got {}", status);

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(data.len(), 1024 * 1024);

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_post_object_upload_16mb_non_chunked() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "post-16mb";
        let file_data = vec![0x2a_u8; 16 * 1024 * 1024];

        let fields = sigv4_fields(
            &bucket,
            key,
            &[serde_json::json!(["content-length-range", 0, 20_971_520])], // 20 MiB
        );
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let (status, body) = post_object(&bucket, &field_refs, &file_data, "large16mb.bin");
        assert_eq!(status, 204, "expected 204, got {}: {}", status, body);

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(data.len(), file_data.len());
        assert_eq!(&data[..], &file_data[..]);

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── Additional Ceph tests ──────────────────────────────────────────────

#[test]
fn test_post_object_invalid_access_key() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "post-invalid-key";

        let (short_date, full_date) = current_dates();
        let region = CTX.region();
        // Use a completely malformed access key (not just wrong, but invalid format)
        let credential = format!("/{}/{}/s3/aws4_request", short_date, region);
        let policy_b64 = make_policy(&bucket, key, 3600, &[]);
        let signature = sign_policy_v4(&policy_b64, "fake-secret", &short_date, region);

        let fields: Vec<(&str, &str)> = vec![
            ("key", key),
            ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
            ("x-amz-credential", &credential),
            ("x-amz-date", &full_date),
            ("policy", &policy_b64),
            ("x-amz-signature", &signature),
        ];

        let (status, _) = post_object(&bucket, &fields, b"data", "test.txt");
        assert_eq!(status, 400, "expected 400, got {}", status);

        s3_tests::delete_bucket_retrying_operation_aborted(CTX.client(), &bucket).await;
    });
}

#[test]
fn test_post_object_invalid_content_length_argument() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "post-bad-clr";

        // Use an invalid content-length-range (min > max)
        let fields = sigv4_fields(
            &bucket,
            key,
            &[serde_json::json!(["content-length-range", 100, 10])],
        );
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let (status, _) = post_object(&bucket, &field_refs, b"data", "test.txt");
        assert_eq!(status, 400, "expected 400, got {}", status);

        s3_tests::delete_bucket_retrying_operation_aborted(CTX.client(), &bucket).await;
    });
}

#[test]
fn test_post_object_missing_expires_condition() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "post-no-expiry";

        let (short_date, full_date) = current_dates();
        let access_key = CTX.access_key();
        let secret_key = CTX.secret_key();
        let region = CTX.region();
        let credential = format!("{}/{}/{}/s3/aws4_request", access_key, short_date, region);

        // Policy without expiration field
        use base64::Engine;
        let policy_json = serde_json::json!({
            "conditions": [
                {"bucket": bucket},
                ["eq", "$key", key],
            ]
        });
        let policy_b64 = base64::engine::general_purpose::STANDARD
            .encode(serde_json::to_string(&policy_json).unwrap());
        let signature = sign_policy_v4(&policy_b64, secret_key, &short_date, region);

        let fields: Vec<(&str, &str)> = vec![
            ("key", key),
            ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
            ("x-amz-credential", &credential),
            ("x-amz-date", &full_date),
            ("policy", &policy_b64),
            ("x-amz-signature", &signature),
        ];

        let (status, _) = post_object(&bucket, &fields, b"data", "test.txt");
        assert_eq!(status, 400, "expected 400, got {}", status);

        s3_tests::delete_bucket_retrying_operation_aborted(CTX.client(), &bucket).await;
    });
}

#[test]
fn test_post_object_tags_authenticated_request() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "post-tagged";
        let tagging_xml = concat!(
            "<Tagging><TagSet>",
            "<Tag><Key>env</Key><Value>staging</Value></Tag>",
            "<Tag><Key>cost-center</Key><Value>123</Value></Tag>",
            "</TagSet></Tagging>"
        );

        let mut fields = sigv4_fields(
            &bucket,
            key,
            &[serde_json::json!(["starts-with", "$tagging", ""])],
        );
        fields.push(("tagging".to_string(), tagging_xml.to_string()));
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let (status, _) = post_object(&bucket, &field_refs, b"data", "test.txt");
        assert_eq!(status, 204, "expected 204, got {}", status);

        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();

        let tag_set = result.tag_set();
        assert_eq!(tag_set.len(), 2);
        assert!(tag_set
            .iter()
            .any(|t| t.key() == "env" && t.value() == "staging"));
        assert!(tag_set
            .iter()
            .any(|t| t.key() == "cost-center" && t.value() == "123"));

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_post_object_tags_malformed_xml() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "post-tagged-malformed";
        let malformed_tagging_xml =
            "<Tagging><TagSet><Tag><Key>env</Key><Value>staging</Value></TagSet></Tagging>";

        let mut fields = sigv4_fields(
            &bucket,
            key,
            &[serde_json::json!(["starts-with", "$tagging", ""])],
        );
        fields.push(("tagging".to_string(), malformed_tagging_xml.to_string()));
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let (status, body) = post_object(&bucket, &field_refs, b"data", "test.txt");
        assert_eq!(status, 400, "expected 400, got {status}: {body}");
        assert_error_code(&body, "MalformedXML");

        let head = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("head object after malformed tagging POST")
            .await;
        assert!(
            head.is_err(),
            "malformed tagging POST should not create object"
        );

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_post_object_tags_duplicate_keys_rejected() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "post-tagged-duplicate";
        let duplicate_tagging_xml = concat!(
            "<Tagging><TagSet>",
            "<Tag><Key>env</Key><Value>staging</Value></Tag>",
            "<Tag><Key>env</Key><Value>prod</Value></Tag>",
            "</TagSet></Tagging>"
        );

        let mut fields = sigv4_fields(
            &bucket,
            key,
            &[serde_json::json!(["starts-with", "$tagging", ""])],
        );
        fields.push(("tagging".to_string(), duplicate_tagging_xml.to_string()));
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let (status, body) = post_object(&bucket, &field_refs, b"data", "test.txt");
        assert_eq!(status, 400, "expected 400, got {status}: {body}");
        assert_error_code(&body, "InvalidTag");

        let head = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("head object after duplicate tag POST")
            .await;
        assert!(head.is_err(), "duplicate-tag POST should not create object");

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── Coverage tests for multipart parser edge cases ──────────────────────

/// POST Object with wrong Content-Type (not multipart/form-data).
/// AWS returns 412 Precondition Failed.
#[test]
fn test_post_object_wrong_content_type() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let url = format!("{}/{}", CTX.endpoint(), bucket);
        let mut resp = agent()
            .post(&url)
            .header("Content-Type", "text/plain")
            .send(b"hello" as &[u8])
            .expect("HTTP transport error");
        let status = resp.status().as_u16();
        let body = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 412);
        assert!(
            body.contains("<Code>PreconditionFailed</Code>"),
            "expected PreconditionFailed error, got: {body}"
        );
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

/// POST Object with multipart/form-data but no boundary parameter.
/// AWS returns MalformedPOSTRequest.
#[test]
fn test_post_object_missing_boundary() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let url = format!("{}/{}", CTX.endpoint(), bucket);
        let mut resp = agent()
            .post(&url)
            .header("Content-Type", "multipart/form-data")
            .send(b"data" as &[u8])
            .expect("HTTP transport error");
        let status = resp.status().as_u16();
        let body = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 400);
        assert!(
            body.contains("<Code>MalformedPOSTRequest</Code>"),
            "expected MalformedPOSTRequest, got: {body}"
        );
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

/// POST Object with empty boundary value (boundary="").
/// AWS may return MalformedPOSTRequest or InvalidArgument.
#[test]
fn test_post_object_empty_boundary() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let url = format!("{}/{}", CTX.endpoint(), bucket);
        let mut resp = agent()
            .post(&url)
            .header("Content-Type", "multipart/form-data; boundary=\"\"")
            .send(b"data" as &[u8])
            .expect("HTTP transport error");
        let status = resp.status().as_u16();
        let body = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 400);
        assert!(
            body.contains("<Code>MalformedPOSTRequest</Code>")
                || body.contains("<Code>InvalidArgument</Code>"),
            "expected MalformedPOSTRequest or InvalidArgument, got: {body}"
        );
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

/// POST Object with a multipart part whose Content-Disposition has no name=.
/// Exercises parse_content_disposition missing name error.
/// AWS returns InvalidArgument ("POST requires exactly one file upload").
#[test]
fn test_post_object_part_missing_name() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let url = format!("{}/{}", CTX.endpoint(), bucket);
        let boundary = "TestBoundary";
        let mut body = Vec::new();
        // Part with no name= in Content-Disposition
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(b"Content-Disposition: form-data\r\n\r\n");
        body.extend_from_slice(b"value\r\n");
        body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());

        let content_type = format!("multipart/form-data; boundary={boundary}");
        let mut resp = agent()
            .post(&url)
            .header("Content-Type", &content_type)
            .send(&body[..])
            .expect("HTTP transport error");
        let status = resp.status().as_u16();
        let body = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 400);
        assert!(
            body.contains("<Code>MalformedPOSTRequest</Code>")
                || body.contains("<Code>InvalidArgument</Code>"),
            "expected MalformedPOSTRequest or InvalidArgument, got: {body}"
        );
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

/// POST Object with unquoted name= values in Content-Disposition.
/// Exercises the unquote non-quoted path.
#[test]
fn test_post_object_unquoted_field_names() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let fields = sigv4_fields(&bucket, "test.txt", &[]);
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        // Build multipart body with unquoted name= values
        let boundary = "----TestBoundary7MA4YWxkTrZu0gW";
        let mut body = Vec::new();
        for (name, value) in &field_refs {
            body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
            // Unquoted name= value
            body.extend_from_slice(
                format!("Content-Disposition: form-data; name={name}\r\n\r\n").as_bytes(),
            );
            body.extend_from_slice(value.as_bytes());
            body.extend_from_slice(b"\r\n");
        }
        // File field with unquoted name
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            b"Content-Disposition: form-data; name=file; filename=\"test.txt\"\r\n",
        );
        body.extend_from_slice(b"Content-Type: application/octet-stream\r\n\r\n");
        body.extend_from_slice(b"hello");
        body.extend_from_slice(b"\r\n");
        body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());

        let content_type = format!("multipart/form-data; boundary={boundary}");
        let url = format!("{}/{}", CTX.endpoint(), bucket);
        let mut resp = agent()
            .post(&url)
            .header("Content-Type", &content_type)
            .send(&body[..])
            .expect("HTTP transport error");
        let status = resp.status().as_u16();
        let _body = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 204);

        client
            .delete_object()
            .bucket(&bucket)
            .key("test.txt")
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_post_object_response_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        let key = "shape-post-object.txt";

        let fields = sigv4_post_fields_for_credentials(
            CTX.access_key(),
            CTX.secret_key(),
            CTX.region(),
            &bucket,
            key,
            &[],
        );
        let response = post_object_raw_to_test_endpoint_with_headers(
            CTX.endpoint(),
            CTX.tls_ca_pem(),
            &bucket,
            &fields,
            b"post-body",
            "test.txt",
            &[],
        );
        // The Location authority is endpoint-specific; the path suffix and
        // everything else is pinned. The bucket/key are baked into the
        // pattern so {any} terminates on the full path, not the first '/'.
        let location_pattern = format!("http{{any}}/{bucket}/{key}");
        assert_shape(
            "PostObject shape",
            &response,
            &shape()
                .status(204)
                .headers([
                    ("location", location_pattern.as_str()),
                    ("x-amz-checksum-crc64nvme", "1pmdgt0Q3gY="),
                    ("x-amz-checksum-type", "FULL_OBJECT"),
                    ("etag", "{etag}"),
                    ("x-amz-server-side-encryption", "AES256"),
                    ("x-amz-request-id", "{request_id}"),
                    ("x-amz-id-2", "{host_id}"),
                ])
                .body_empty(),
        );

        s3_tests::delete_object_retrying_operation_aborted(client, &bucket, key)
            .await
            .expect("delete post shape fixture");
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}
