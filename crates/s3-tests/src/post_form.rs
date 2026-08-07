use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ring::hmac;

use crate::helpers::{is_retryable_operation_contention_response, SignedRequestCredentials};
use crate::{build_test_agent, sse_c_header_values, RawResponse};

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

fn sign_policy_v4(
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

fn epoch_to_iso8601(epoch: u64) -> String {
    let secs = epoch;
    let days = secs / 86400;
    let time_of_day = secs % 86400;
    let hour = time_of_day / 3600;
    let min = (time_of_day % 3600) / 60;
    let sec = time_of_day % 60;

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

fn current_dates() -> (String, String) {
    let epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    dates_for_epoch(epoch)
}

fn dates_for_epoch(epoch: u64) -> (String, String) {
    let iso = epoch_to_iso8601(epoch);
    let short = iso[..10].replace('-', "");
    let full = format!("{}T{}Z", short, iso[11..19].replace(':', ""));
    (short, full)
}

pub fn build_post_object_multipart_body(
    fields: &[(&str, &str)],
    file_data: &[u8],
    file_name: &str,
) -> (String, Vec<u8>, usize) {
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

    body.extend_from_slice(format!("--{}\r\n", boundary).as_bytes());
    body.extend_from_slice(
        format!(
            "Content-Disposition: form-data; name=\"file\"; filename=\"{}\"\r\n",
            file_name
        )
        .as_bytes(),
    );
    body.extend_from_slice(b"Content-Type: application/octet-stream\r\n\r\n");
    let file_offset = body.len();
    body.extend_from_slice(file_data);
    body.extend_from_slice(b"\r\n");
    body.extend_from_slice(format!("--{}--\r\n", boundary).as_bytes());

    let content_type = format!("multipart/form-data; boundary={}", boundary);
    (content_type, body, file_offset)
}

pub fn sigv4_post_fields_for_credentials(
    access_key: &str,
    secret: &str,
    region: &str,
    bucket: &str,
    key: &str,
    extra_conditions: &[serde_json::Value],
) -> Vec<(String, String)> {
    let (short_date, full_date) = current_dates();
    sigv4_post_fields_for_credentials_with_dates(
        PostSigningCredentials {
            access_key,
            secret,
            region,
            service: "s3",
        },
        bucket,
        key,
        (&short_date, &full_date),
        extra_conditions,
    )
}

pub fn sigv4_post_fields_for_service_with_credentials(
    credentials: SignedRequestCredentials<'_>,
    service: &str,
    bucket: &str,
    key: &str,
    extra_conditions: &[serde_json::Value],
) -> Vec<(String, String)> {
    let (short_date, full_date) = current_dates();
    sigv4_post_fields_for_credentials_with_dates(
        PostSigningCredentials {
            access_key: credentials.access_key,
            secret: credentials.secret_key,
            region: credentials.region,
            service,
        },
        bucket,
        key,
        (&short_date, &full_date),
        extra_conditions,
    )
}

pub fn sigv4_post_fields_for_credentials_at_epoch(
    access_key: &str,
    secret: &str,
    region: &str,
    bucket: &str,
    key: &str,
    signing_epoch_secs: u64,
    extra_conditions: &[serde_json::Value],
) -> Vec<(String, String)> {
    let (short_date, full_date) = dates_for_epoch(signing_epoch_secs);
    sigv4_post_fields_for_credentials_with_dates(
        PostSigningCredentials {
            access_key,
            secret,
            region,
            service: "s3",
        },
        bucket,
        key,
        (&short_date, &full_date),
        extra_conditions,
    )
}

#[derive(Clone, Copy)]
struct PostSigningCredentials<'a> {
    access_key: &'a str,
    secret: &'a str,
    region: &'a str,
    service: &'a str,
}

fn sigv4_post_fields_for_credentials_with_dates(
    credentials: PostSigningCredentials<'_>,
    bucket: &str,
    key: &str,
    dates: (&str, &str),
    extra_conditions: &[serde_json::Value],
) -> Vec<(String, String)> {
    let (short_date, full_date) = dates;
    let credential = format!(
        "{}/{}/{}/{}/aws4_request",
        credentials.access_key, short_date, credentials.region, credentials.service
    );

    let mut all_conditions = vec![
        serde_json::json!({"x-amz-algorithm": "AWS4-HMAC-SHA256"}),
        serde_json::json!({"x-amz-credential": &credential}),
        serde_json::json!({"x-amz-date": full_date}),
    ];
    all_conditions.extend_from_slice(extra_conditions);

    let policy_b64 = make_policy(bucket, key, 3600, &all_conditions);
    let signature = sign_policy_v4(
        &policy_b64,
        credentials.secret,
        short_date,
        credentials.region,
        credentials.service,
    );

    vec![
        ("key".to_string(), key.to_string()),
        (
            "x-amz-algorithm".to_string(),
            "AWS4-HMAC-SHA256".to_string(),
        ),
        ("x-amz-credential".to_string(), credential),
        ("x-amz-date".to_string(), full_date.to_string()),
        ("policy".to_string(), policy_b64),
        ("x-amz-signature".to_string(), signature),
    ]
}

pub fn sigv4_post_sse_c_fields_for_credentials(
    access_key: &str,
    secret: &str,
    region: &str,
    bucket: &str,
    key: &str,
    customer_key: &[u8; 32],
) -> Vec<(String, String)> {
    let (key_b64, key_md5_b64) = sse_c_header_values(customer_key);
    let mut fields = sigv4_post_fields_for_credentials(
        access_key,
        secret,
        region,
        bucket,
        key,
        &[
            serde_json::json!({"x-amz-server-side-encryption-customer-algorithm": "AES256"}),
            serde_json::json!({"x-amz-server-side-encryption-customer-key": &key_b64}),
            serde_json::json!({"x-amz-server-side-encryption-customer-key-md5": &key_md5_b64}),
        ],
    );
    fields.push((
        "x-amz-server-side-encryption-customer-algorithm".to_string(),
        "AES256".to_string(),
    ));
    fields.push((
        "x-amz-server-side-encryption-customer-key".to_string(),
        key_b64,
    ));
    fields.push((
        "x-amz-server-side-encryption-customer-key-md5".to_string(),
        key_md5_b64,
    ));
    fields
}

pub fn post_object_to_test_endpoint_with_headers(
    endpoint: &str,
    tls_ca_pem: Option<&[u8]>,
    bucket: &str,
    fields: &[(&str, &str)],
    file_data: &[u8],
    file_name: &str,
    headers: &[(&str, &str)],
) -> (u16, String) {
    let url = format!("{}/{}", endpoint, bucket);
    let (content_type, body, _) = build_post_object_multipart_body(fields, file_data, file_name);
    let agent = build_test_agent(endpoint, tls_ca_pem, crate::configured_test_timeout());
    let deadline = Instant::now() + crate::configured_test_timeout();

    loop {
        let req = agent.post(&url).header("Content-Type", &content_type);
        let req = headers
            .iter()
            .fold(req, |req, (name, value)| req.header(*name, *value));
        let mut resp = req.send(&body[..]).expect("HTTP transport error");
        let status = resp.status().as_u16();
        let response_body = resp.body_mut().read_to_string().unwrap_or_default();
        if is_retryable_operation_contention_response(status, &response_body)
            && Instant::now() < deadline
        {
            thread::sleep(Duration::from_millis(100));
            continue;
        }
        return (status, response_body);
    }
}

pub fn post_object_raw_to_test_endpoint_with_headers(
    endpoint: &str,
    tls_ca_pem: Option<&[u8]>,
    bucket: &str,
    fields: &[(String, String)],
    file_data: &[u8],
    file_name: &str,
    headers: &[(String, String)],
) -> RawResponse {
    let url = format!("{}/{}", endpoint, bucket);
    let field_refs: Vec<(&str, &str)> = fields
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect();
    let (content_type, body, _) =
        build_post_object_multipart_body(&field_refs, file_data, file_name);
    let agent = build_test_agent(endpoint, tls_ca_pem, crate::configured_test_timeout());
    let deadline = Instant::now() + crate::configured_test_timeout();

    loop {
        let req = agent.post(&url).header("Content-Type", &content_type);
        let req = headers.iter().fold(req, |req, (name, value)| {
            req.header(name.as_str(), value.as_str())
        });
        let mut resp = req.send(&body[..]).expect("HTTP transport error");
        let status = resp.status().as_u16();
        let headers = resp
            .headers()
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_string(),
                    value
                        .to_str()
                        .expect("response header is valid utf-8")
                        .to_string(),
                )
            })
            .collect();
        let response_body = resp.body_mut().read_to_string().unwrap_or_default();
        if is_retryable_operation_contention_response(status, &response_body)
            && Instant::now() < deadline
        {
            thread::sleep(Duration::from_millis(100));
            continue;
        }
        return RawResponse {
            status,
            headers,
            body: response_body,
            body_read_error: None,
        };
    }
}

pub fn post_object_to_test_endpoint(
    endpoint: &str,
    tls_ca_pem: Option<&[u8]>,
    bucket: &str,
    fields: &[(&str, &str)],
    file_data: &[u8],
    file_name: &str,
) -> (u16, String) {
    post_object_to_test_endpoint_with_headers(
        endpoint,
        tls_ca_pem,
        bucket,
        fields,
        file_data,
        file_name,
        &[],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn post_contention_retry_classification_preserves_status_and_code_pairing() {
        assert!(is_retryable_operation_contention_response(
            409,
            "<Error><Code>OperationAborted</Code></Error>"
        ));
        assert!(is_retryable_operation_contention_response(
            503,
            "<Error><Code>SlowDown</Code></Error>"
        ));
        assert!(!is_retryable_operation_contention_response(
            503,
            "<Error><Code>OperationAborted</Code></Error>"
        ));
        assert!(!is_retryable_operation_contention_response(
            409,
            "<Error><Code>SlowDown</Code></Error>"
        ));
    }

    #[test]
    fn post_fields_for_service_use_requested_credential_scope() {
        let fields = sigv4_post_fields_for_service_with_credentials(
            SignedRequestCredentials {
                access_key: "ACCESSKEY",
                secret_key: "secret",
                region: "test-region-1",
                tls_ca_pem: None,
            },
            "sts",
            "bucket",
            "key",
            &[],
        );
        let field = |name: &str| {
            fields
                .iter()
                .find_map(|(candidate, value)| (candidate == name).then_some(value.as_str()))
                .unwrap_or_else(|| panic!("missing {name} POST field"))
        };
        let credential = field("x-amz-credential");

        assert!(credential.starts_with("ACCESSKEY/"));
        assert!(credential.ends_with("/test-region-1/sts/aws4_request"));
        let policy = {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD
                .decode(field("policy"))
                .expect("decode POST policy")
        };
        let policy = String::from_utf8(policy).expect("POST policy is UTF-8");
        assert!(policy.contains(credential));
    }
}
