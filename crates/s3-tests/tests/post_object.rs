use std::time::{SystemTime, UNIX_EPOCH};

use ring::hmac;
use s3_tests::{unique_bucket, CTX};

/// Create a bucket, returning its name.
async fn setup_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    client.create_bucket().bucket(&bucket).send().await.unwrap();
    bucket
}

/// Build an agent that returns all HTTP responses (including 4xx/5xx) as Ok.
fn agent() -> ureq::Agent {
    ureq::Agent::config_builder()
        .http_status_as_error(false)
        .build()
        .new_agent()
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
    let signing_key = derive_signing_key(secret, date, region, "s3");
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
    let (short_date, full_date) = current_dates();
    let secret = CTX.secret_key();
    let access_key = CTX.access_key();
    let region = CTX.region();

    let credential = format!("{}/{}/{}/s3/aws4_request", access_key, short_date, region);

    // AWS requires ALL form fields to have matching policy conditions.
    // The SigV4 fields must be included in the policy.
    let mut all_conditions = vec![
        serde_json::json!({"x-amz-algorithm": "AWS4-HMAC-SHA256"}),
        serde_json::json!({"x-amz-credential": &credential}),
        serde_json::json!({"x-amz-date": &full_date}),
    ];
    all_conditions.extend_from_slice(extra_conditions);

    let policy_b64 = make_policy(bucket, key, 3600, &all_conditions);
    let signature = sign_policy_v4(&policy_b64, secret, &short_date, region);

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
    let url = format!("{}/{}", CTX.endpoint(), bucket);
    let (content_type, body) = build_multipart(fields, file_data, file_name);

    let mut resp = agent()
        .post(&url)
        .header("Content-Type", &content_type)
        .send(&body[..])
        .expect("HTTP transport error");

    let status = resp.status().as_u16();
    let body_str = resp.body_mut().read_to_string().unwrap_or_default();
    (status, body_str)
}

// ── Basic upload ────────────────────────────────────────────────────────

#[test]
fn test_post_object_authenticated_request() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
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
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
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
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
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
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
#[ignore]
fn test_post_object_anonymous_request() {
    // Anonymous POST requires bucket policy allowing public writes.
    // Not implemented yet.
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
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
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
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
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
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let (status, _) = post_object(&bucket, &field_refs, b"data", "test.txt");
        assert_eq!(status, 200);

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
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
        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let (status, body) = post_object(&bucket, &field_refs, b"data", "test.txt");
        assert_eq!(status, 201);
        // 201 response should contain XML with Location, Bucket, Key, ETag
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
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
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
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
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
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
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

        CTX.client()
            .delete_bucket()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
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

        CTX.client()
            .delete_bucket()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
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

        CTX.client()
            .delete_bucket()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
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

        CTX.client()
            .delete_bucket()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
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

        CTX.client()
            .delete_bucket()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
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

        CTX.client()
            .delete_bucket()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
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

        CTX.client()
            .delete_bucket()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
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

        CTX.client()
            .delete_bucket()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
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

        CTX.client()
            .delete_bucket()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
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
        CTX.client()
            .delete_bucket()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
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

        CTX.client()
            .delete_bucket()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
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
        CTX.client()
            .delete_bucket()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
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
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
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
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
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
        CTX.client()
            .delete_bucket()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
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
        CTX.client()
            .delete_bucket()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
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

        CTX.client()
            .delete_bucket()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
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

        CTX.client()
            .delete_bucket()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
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
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
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
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
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
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
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

        CTX.client()
            .delete_bucket()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
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

        CTX.client()
            .delete_bucket()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
    });
}

#[test]
fn test_post_object_missing_content_length_argument() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "post-bad-clr";

        let (short_date, full_date) = current_dates();
        let secret = CTX.secret_key();
        let access_key = CTX.access_key();
        let region = CTX.region();
        let credential = format!("{}/{}/{}/s3/aws4_request", access_key, short_date, region);

        // Malformed content-length-range with only 1 argument
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
                serde_json::json!(["content-length-range", 0]),
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
            "expected 400 for malformed content-length-range, got {}",
            status
        );

        CTX.client()
            .delete_bucket()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
    });
}

// ── Redirect ────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn test_post_object_success_redirect_action() {
    // success_action_redirect returns 303 with Location header.
    // Not yet implemented in the server.
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
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
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
        CTX.client()
            .delete_bucket()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
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
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
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

        CTX.client()
            .delete_bucket()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
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
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
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
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
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
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
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

        CTX.client()
            .delete_bucket()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
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

        CTX.client()
            .delete_bucket()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
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

        CTX.client()
            .delete_bucket()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
    });
}

#[test]
#[ignore = "not implemented: tagging"]
fn test_post_object_tags_anonymous_request() {
    s3_tests::run(async {});
}

#[test]
#[ignore = "not implemented: tagging"]
fn test_post_object_tags_authenticated_request() {
    s3_tests::run(async {});
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
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
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
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
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
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
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
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
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
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}
