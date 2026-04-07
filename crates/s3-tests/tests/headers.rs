use std::time::{SystemTime, UNIX_EPOCH};

use aws_sdk_s3::primitives::ByteStream;
use base64::Engine;
use ring::{digest, hmac};
use s3_tests::{unique_bucket, CTX};
use s3_types::requires_sigv4;

// ── Helpers ─────────────────────────────────────────────────────────────

fn agent() -> ureq::Agent {
    s3_tests::test_agent()
}

async fn setup_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    s3_tests::create_bucket_request(client, &bucket)
        .send()
        .await
        .unwrap();
    bucket
}

async fn setup_public_bucket() -> String {
    s3_tests::create_public_bucket(CTX.client()).await
}

async fn cleanup(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = client.delete_object().bucket(bucket).key(*key).send().await;
    }
    client.delete_bucket().bucket(bucket).send().await.unwrap();
}

fn assert_error_code(body: &str, code: &str) {
    let expected = format!("<Code>{}</Code>", code);
    assert!(
        body.contains(&expected),
        "expected {} in body: {}",
        expected,
        body
    );
}

// ── Crypto helpers (duplicated from presigned.rs) ───────────────────────

fn sha256_hex(data: &[u8]) -> String {
    let d = digest::digest(&digest::SHA256, data);
    d.as_ref().iter().map(|b| format!("{:02x}", b)).collect()
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> hmac::Tag {
    hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), data)
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

fn derive_signing_key(secret: &str, date: &str, region: &str, service: &str) -> hmac::Tag {
    let k_secret = format!("AWS4{}", secret);
    let k_date = hmac_sha256(k_secret.as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(k_date.as_ref(), region.as_bytes());
    let k_service = hmac_sha256(k_region.as_ref(), service.as_bytes());
    hmac_sha256(k_service.as_ref(), b"aws4_request")
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

fn current_http_date() -> String {
    const WEEKDAYS: [&str; 7] = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"];
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];

    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let days = secs / 86_400;
    let (year, month, day) = days_to_ymd(days);
    let secs_today = secs % 86_400;
    let hour = secs_today / 3_600;
    let minute = (secs_today % 3_600) / 60;
    let second = secs_today % 60;
    let weekday = WEEKDAYS[(days % 7) as usize];
    let month_name = MONTHS[(month - 1) as usize];

    format!("{weekday}, {day:02} {month_name} {year:04} {hour:02}:{minute:02}:{second:02} GMT")
}

fn sigv2_authorization(bucket: &str, date: &str) -> String {
    let string_to_sign = format!("GET\n\n\n{date}\n/{bucket}");
    let key = hmac::Key::new(
        hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY,
        CTX.secret_key().as_bytes(),
    );
    let signature = hmac::sign(&key, string_to_sign.as_bytes());
    let signature_b64 = base64::engine::general_purpose::STANDARD.encode(signature.as_ref());
    format!("AWS {}:{signature_b64}", CTX.access_key())
}

fn sigv2_unsupported_in_region(region: &str) -> bool {
    requires_sigv4(region)
}

fn host() -> &'static str {
    CTX.endpoint()
        .trim_start_matches("http://")
        .trim_start_matches("https://")
}

// ── SigV4 Header-Auth Signer ────────────────────────────────────────────

struct SignedHeaders {
    authorization: String,
    amz_date: String,
    amz_content_sha256: String,
}

struct Signer {
    method: String,
    path: String,
    query: String,
    access_key: String,
    secret_key: String,
    region: String,
    service: String,
    body_hash: Option<String>,
    include_content_sha256: bool,
    timestamp: Option<u64>,
}

impl Signer {
    fn new(method: &str, path: &str) -> Self {
        Self {
            method: method.to_string(),
            path: path.to_string(),
            query: String::new(),
            access_key: CTX.access_key().to_string(),
            secret_key: CTX.secret_key().to_string(),
            region: CTX.region().to_string(),
            service: "s3".to_string(),
            body_hash: None,
            include_content_sha256: true,
            timestamp: None,
        }
    }

    fn access_key(mut self, key: &str) -> Self {
        self.access_key = key.to_string();
        self
    }

    fn secret_key(mut self, key: &str) -> Self {
        self.secret_key = key.to_string();
        self
    }

    fn region(mut self, region: &str) -> Self {
        self.region = region.to_string();
        self
    }

    fn service(mut self, service: &str) -> Self {
        self.service = service.to_string();
        self
    }

    fn query(mut self, query: &str) -> Self {
        self.query = query.to_string();
        self
    }

    fn body_hash(mut self, hash: &str) -> Self {
        self.body_hash = Some(hash.to_string());
        self
    }

    fn omit_content_sha256_header(mut self) -> Self {
        self.include_content_sha256 = false;
        self
    }

    fn at_time(mut self, epoch_secs: u64) -> Self {
        self.timestamp = Some(epoch_secs);
        self
    }

    fn sign(self) -> SignedHeaders {
        let secs = self.timestamp.unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
        });
        let days = secs / 86400;
        let (year, month, day) = days_to_ymd(days);
        let time_of_day = secs % 86400;
        let hour = time_of_day / 3600;
        let minute = (time_of_day % 3600) / 60;
        let second = time_of_day % 60;
        let date_long = format!(
            "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
            year, month, day, hour, minute, second
        );
        let date_short = &date_long[..8];

        let content_sha256 = self.body_hash.unwrap_or_else(|| sha256_hex(b""));

        let host_val = host();
        let (signed_headers, canonical_headers) = if self.include_content_sha256 {
            (
                "host;x-amz-content-sha256;x-amz-date",
                format!(
                    "host:{}\nx-amz-content-sha256:{}\nx-amz-date:{}\n",
                    host_val, content_sha256, date_long
                ),
            )
        } else {
            (
                "host;x-amz-date",
                format!("host:{}\nx-amz-date:{}\n", host_val, date_long),
            )
        };

        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            self.method, self.path, self.query, canonical_headers, signed_headers, content_sha256
        );

        let canonical_hash = sha256_hex(canonical_request.as_bytes());
        let scope = format!(
            "{}/{}/{}/aws4_request",
            date_short, self.region, self.service
        );
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            date_long, scope, canonical_hash
        );

        let signing_key =
            derive_signing_key(&self.secret_key, date_short, &self.region, &self.service);
        let signature = hmac_sha256(signing_key.as_ref(), string_to_sign.as_bytes());
        let sig_hex = hex_encode(signature.as_ref());

        let credential = format!(
            "{}/{}/{}/{}/aws4_request",
            self.access_key, date_short, self.region, self.service
        );
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}, SignedHeaders={}, Signature={}",
            credential, signed_headers, sig_hex
        );

        SignedHeaders {
            authorization,
            amz_date: date_long,
            amz_content_sha256: content_sha256,
        }
    }
}

/// Send a signed PUT with body to the given bucket/key.
fn signed_put(bucket: &str, key: &str, body: &[u8]) -> (u16, String) {
    let path = format!("/{}/{}", bucket, key);
    let body_hash = sha256_hex(body);
    let s = Signer::new("PUT", &path).body_hash(&body_hash).sign();
    let url = format!("{}{}", CTX.endpoint(), path);
    let mut resp = agent()
        .put(&url)
        .header("Authorization", &s.authorization)
        .header("x-amz-date", &s.amz_date)
        .header("x-amz-content-sha256", &s.amz_content_sha256)
        .send(body)
        .expect("transport error");
    let status = resp.status().as_u16();
    let body_str = resp.body_mut().read_to_string().unwrap_or_default();
    (status, body_str)
}

// ── Group 1: Checksum Headers ───────────────────────────────────────────

#[test]
fn test_put_bad_checksum_sha256() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let body = b"checksum test body";
        let path = format!("/{}/ckobj", bucket);
        let body_hash = sha256_hex(body);
        let s = Signer::new("PUT", &path).body_hash(&body_hash).sign();
        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &s.authorization)
            .header("x-amz-date", &s.amz_date)
            .header("x-amz-content-sha256", &s.amz_content_sha256)
            .header(
                "x-amz-checksum-sha256",
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            )
            .send(body.as_ref())
            .expect("transport error");
        let status = resp.status().as_u16();
        let rbody = resp.body_mut().read_to_string().unwrap();
        assert_eq!(status, 403, "expected 403, got {}", status);
        assert_error_code(&rbody, "AccessDenied");
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_put_bad_checksum_crc32() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let body = b"crc32 body";
        let path = format!("/{}/ckobj", bucket);
        let body_hash = sha256_hex(body);
        let s = Signer::new("PUT", &path).body_hash(&body_hash).sign();
        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &s.authorization)
            .header("x-amz-date", &s.amz_date)
            .header("x-amz-content-sha256", &s.amz_content_sha256)
            .header("x-amz-checksum-crc32", "AAAAAAA=")
            .send(body.as_ref())
            .expect("transport error");
        let status = resp.status().as_u16();
        let rbody = resp.body_mut().read_to_string().unwrap();
        assert_eq!(status, 403, "expected 403, got {}", status);
        assert_error_code(&rbody, "AccessDenied");
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_put_bad_checksum_crc32c() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let body = b"crc32c body";
        let path = format!("/{}/ckobj", bucket);
        let body_hash = sha256_hex(body);
        let s = Signer::new("PUT", &path).body_hash(&body_hash).sign();
        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &s.authorization)
            .header("x-amz-date", &s.amz_date)
            .header("x-amz-content-sha256", &s.amz_content_sha256)
            .header("x-amz-checksum-crc32c", "AAAAAAA=")
            .send(body.as_ref())
            .expect("transport error");
        let status = resp.status().as_u16();
        let rbody = resp.body_mut().read_to_string().unwrap();
        assert_eq!(status, 403, "expected 403, got {}", status);
        assert_error_code(&rbody, "AccessDenied");
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_put_bad_checksum_crc64nvme() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let body = b"crc64 body";
        let path = format!("/{}/ckobj", bucket);
        let body_hash = sha256_hex(body);
        let s = Signer::new("PUT", &path).body_hash(&body_hash).sign();
        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &s.authorization)
            .header("x-amz-date", &s.amz_date)
            .header("x-amz-content-sha256", &s.amz_content_sha256)
            .header("x-amz-checksum-crc64nvme", "AAAAAAAAAAA=")
            .send(body.as_ref())
            .expect("transport error");
        let status = resp.status().as_u16();
        let rbody = resp.body_mut().read_to_string().unwrap();
        assert_eq!(status, 403, "expected 403, got {}", status);
        assert_error_code(&rbody, "AccessDenied");
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_put_bad_checksum_sha1() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let body = b"sha1 body";
        let path = format!("/{}/ckobj", bucket);
        let body_hash = sha256_hex(body);
        let s = Signer::new("PUT", &path).body_hash(&body_hash).sign();
        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &s.authorization)
            .header("x-amz-date", &s.amz_date)
            .header("x-amz-content-sha256", &s.amz_content_sha256)
            .header("x-amz-checksum-sha1", "AAAAAAAAAAAAAAAAAAAAAAAAAAA=")
            .send(body.as_ref())
            .expect("transport error");
        let status = resp.status().as_u16();
        let rbody = resp.body_mut().read_to_string().unwrap();
        assert_eq!(status, 403, "expected 403, got {}", status);
        assert_error_code(&rbody, "AccessDenied");
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_put_empty_checksum_sha256() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let body = b"empty cksum body";
        let path = format!("/{}/ckobj", bucket);
        let body_hash = sha256_hex(body);
        let s = Signer::new("PUT", &path).body_hash(&body_hash).sign();
        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &s.authorization)
            .header("x-amz-date", &s.amz_date)
            .header("x-amz-content-sha256", &s.amz_content_sha256)
            .header("x-amz-checksum-sha256", "")
            .send(body.as_ref())
            .expect("transport error");
        let status = resp.status().as_u16();
        let rbody = resp.body_mut().read_to_string().unwrap();
        assert_eq!(status, 403, "expected 403, got {}", status);
        assert_error_code(&rbody, "AccessDenied");
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_put_no_checksum_succeeds() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let body = b"no checksum body";
        let (status, _) = signed_put(&bucket, "obj", body);
        assert_eq!(status, 200);
        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_put_multiple_checksums() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let body = b"multi cksum body";
        let path = format!("/{}/ckobj", bucket);
        let body_hash = sha256_hex(body);
        let s = Signer::new("PUT", &path).body_hash(&body_hash).sign();
        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &s.authorization)
            .header("x-amz-date", &s.amz_date)
            .header("x-amz-content-sha256", &s.amz_content_sha256)
            .header(
                "x-amz-checksum-sha256",
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            )
            .header("x-amz-checksum-crc32", "AAAAAAA=")
            .send(body.as_ref())
            .expect("transport error");
        let status = resp.status().as_u16();
        let rbody = resp.body_mut().read_to_string().unwrap();
        assert_eq!(status, 403, "expected 403, got {}", status);
        assert_error_code(&rbody, "AccessDenied");
        cleanup(&bucket, &[]).await;
    });
}

// ── Group 2: Content-Type ───────────────────────────────────────────────

#[test]
fn test_put_invalid_content_type() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let body = b"content type test";
        let path = format!("/{}/ctobj", bucket);
        let body_hash = sha256_hex(body);
        let s = Signer::new("PUT", &path).body_hash(&body_hash).sign();
        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &s.authorization)
            .header("x-amz-date", &s.amz_date)
            .header("x-amz-content-sha256", &s.amz_content_sha256)
            .header("Content-Type", "text/plain")
            .send(body.as_ref())
            .expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(status, 200);
        cleanup(&bucket, &["ctobj"]).await;
    });
}

#[test]
fn test_put_empty_content_type() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let body = b"empty ct body";
        let path = format!("/{}/ctobj", bucket);
        let body_hash = sha256_hex(body);
        let s = Signer::new("PUT", &path).body_hash(&body_hash).sign();
        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &s.authorization)
            .header("x-amz-date", &s.amz_date)
            .header("x-amz-content-sha256", &s.amz_content_sha256)
            .header("Content-Type", "")
            .send(body.as_ref())
            .expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(status, 200);
        cleanup(&bucket, &["ctobj"]).await;
    });
}

#[test]
fn test_put_no_content_type() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let body = b"no ct body";
        let (status, _) = signed_put(&bucket, "ctobj", body);
        assert_eq!(status, 200);
        cleanup(&bucket, &["ctobj"]).await;
    });
}

// ── Group 3: Authorization Header ───────────────────────────────────────

#[test]
fn test_put_empty_authorization() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let url = format!("{}/{}/obj", CTX.endpoint(), bucket);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", "")
            .send(b"data" as &[u8])
            .expect("transport error");
        let status = resp.status().as_u16();
        let rbody = resp.body_mut().read_to_string().unwrap();
        assert_eq!(status, 403, "expected 403, got {}", status);
        assert_error_code(&rbody, "AccessDenied");
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_put_no_authorization_private() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let url = format!("{}/{}/obj", CTX.endpoint(), bucket);
        let mut resp = agent()
            .put(&url)
            .send(b"data" as &[u8])
            .expect("transport error");
        let status = resp.status().as_u16();
        let rbody = resp.body_mut().read_to_string().unwrap();
        assert_eq!(status, 403, "expected 403, got {}", status);
        assert_error_code(&rbody, "AccessDenied");
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_put_malformed_authorization() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let url = format!("{}/{}/obj", CTX.endpoint(), bucket);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", "garbage")
            .send(b"data" as &[u8])
            .expect("transport error");
        let status = resp.status().as_u16();
        let rbody = resp.body_mut().read_to_string().unwrap();
        assert_eq!(status, 400, "expected 400, got {}", status);
        assert_error_code(&rbody, "InvalidArgument");
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_put_empty_authorization() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        let url = format!("{}/{}", CTX.endpoint(), bucket);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", "")
            .send(b"" as &[u8])
            .expect("transport error");
        let status = resp.status().as_u16();
        let rbody = resp.body_mut().read_to_string().unwrap();
        assert_eq!(status, 403, "expected 403, got {}", status);
        assert_error_code(&rbody, "AccessDenied");
    });
}

#[test]
fn test_put_wrong_access_key() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let body = b"wrong key body";
        let path = format!("/{}/obj", bucket);
        let body_hash = sha256_hex(body);
        let s = Signer::new("PUT", &path)
            .access_key("AKIAIOSFODNN7INVALID")
            .body_hash(&body_hash)
            .sign();
        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &s.authorization)
            .header("x-amz-date", &s.amz_date)
            .header("x-amz-content-sha256", &s.amz_content_sha256)
            .send(body.as_ref())
            .expect("transport error");
        let status = resp.status().as_u16();
        let rbody = resp.body_mut().read_to_string().unwrap();
        assert_eq!(status, 403, "expected 403, got {}", status);
        assert_error_code(&rbody, "InvalidAccessKeyId");
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_put_wrong_secret_key() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let body = b"wrong secret body";
        let path = format!("/{}/obj", bucket);
        let body_hash = sha256_hex(body);
        let s = Signer::new("PUT", &path)
            .secret_key("wJalrXUtnFEMI/K7MDENG/bPxRfiCYWRONGKEY000")
            .body_hash(&body_hash)
            .sign();
        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &s.authorization)
            .header("x-amz-date", &s.amz_date)
            .header("x-amz-content-sha256", &s.amz_content_sha256)
            .send(body.as_ref())
            .expect("transport error");
        let status = resp.status().as_u16();
        let rbody = resp.body_mut().read_to_string().unwrap();
        assert_eq!(status, 403, "expected 403, got {}", status);
        assert_error_code(&rbody, "SignatureDoesNotMatch");
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_put_missing_content_sha256_header_rejected() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let actual_body = b"body sent without x-amz-content-sha256";
        let path = format!("/{}/obj", bucket);
        let s = Signer::new("PUT", &path)
            .body_hash(&sha256_hex(b""))
            .omit_content_sha256_header()
            .sign();
        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &s.authorization)
            .header("x-amz-date", &s.amz_date)
            .send(actual_body.as_ref())
            .expect("transport error");
        let status = resp.status().as_u16();
        let rbody = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 400, "expected 400, got {} body: {}", status, rbody);
        assert_error_code(&rbody, "InvalidRequest");
        assert!(
            rbody.contains("Missing required header for this request: x-amz-content-sha256"),
            "expected missing x-amz-content-sha256 message, got: {}",
            rbody
        );
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_upload_part_missing_content_sha256_header_rejected() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "missing-content-sha256-part";
        let actual_body = b"multipart body sent without x-amz-content-sha256";
        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();
        let query = format!(
            "partNumber=1&uploadId={}",
            url::form_urlencoded::byte_serialize(upload_id.as_bytes()).collect::<String>()
        );
        let path = format!("/{bucket}/{key}");
        let url = format!("{}{}?{}", CTX.endpoint(), path, query);
        let signed = Signer::new("PUT", &path)
            .query(&query)
            .body_hash(&sha256_hex(b""))
            .omit_content_sha256_header()
            .sign();
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &signed.authorization)
            .header("x-amz-date", &signed.amz_date)
            .send(actual_body.as_ref())
            .expect("transport error");
        let status = resp.status().as_u16();
        let rbody = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 400, "expected 400, got {} body: {}", status, rbody);
        assert_error_code(&rbody, "InvalidRequest");
        assert!(
            rbody.contains("Missing required header for this request: x-amz-content-sha256"),
            "expected missing x-amz-content-sha256 message, got: {}",
            rbody
        );
        let _ = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await;
        cleanup(&bucket, &[]).await;
    });
}

// ── Group 4: Date Header ────────────────────────────────────────────────

/// x-amz-date without Authorization → AWS returns 403 (incomplete signed request).
#[test]
fn test_get_date_empty_anonymous() {
    s3_tests::run(async {
        let bucket = setup_public_bucket().await;
        let client = CTX.client();
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let url = format!("{}/{}/obj", CTX.endpoint(), bucket);
        let resp = agent()
            .get(&url)
            .header("x-amz-date", "")
            .call()
            .expect("transport error");
        assert_eq!(
            resp.status().as_u16(),
            403,
            "x-amz-date without Authorization is rejected"
        );
        cleanup(&bucket, &["obj"]).await;
    });
}

/// x-amz-date without Authorization → AWS returns 403 (incomplete signed request).
#[test]
fn test_get_date_invalid_anonymous() {
    s3_tests::run(async {
        let bucket = setup_public_bucket().await;
        let client = CTX.client();
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let url = format!("{}/{}/obj", CTX.endpoint(), bucket);
        let resp = agent()
            .get(&url)
            .header("x-amz-date", "garbage")
            .call()
            .expect("transport error");
        assert_eq!(
            resp.status().as_u16(),
            403,
            "x-amz-date without Authorization is rejected"
        );
        cleanup(&bucket, &["obj"]).await;
    });
}

/// x-amz-date without Authorization → AWS returns 403 (incomplete signed request).
#[test]
fn test_get_date_before_epoch_anonymous() {
    s3_tests::run(async {
        let bucket = setup_public_bucket().await;
        let client = CTX.client();
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let url = format!("{}/{}/obj", CTX.endpoint(), bucket);
        let resp = agent()
            .get(&url)
            .header("x-amz-date", "19690101T000000Z")
            .call()
            .expect("transport error");
        assert_eq!(
            resp.status().as_u16(),
            403,
            "x-amz-date without Authorization is rejected"
        );
        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_put_date_skew_past() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let body = b"skew past body";
        let path = format!("/{}/obj", bucket);
        let body_hash = sha256_hex(body);
        // 20 minutes in the past
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let skewed = now - 20 * 60;
        let s = Signer::new("PUT", &path)
            .body_hash(&body_hash)
            .at_time(skewed)
            .sign();
        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &s.authorization)
            .header("x-amz-date", &s.amz_date)
            .header("x-amz-content-sha256", &s.amz_content_sha256)
            .send(body.as_ref())
            .expect("transport error");
        let status = resp.status().as_u16();
        let rbody = resp.body_mut().read_to_string().unwrap();
        assert_eq!(status, 403, "expected 403, got {}", status);
        assert_error_code(&rbody, "RequestTimeTooSkewed");
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_put_date_skew_future() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let body = b"skew future body";
        let path = format!("/{}/obj", bucket);
        let body_hash = sha256_hex(body);
        // 20 minutes in the future
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let skewed = now + 20 * 60;
        let s = Signer::new("PUT", &path)
            .body_hash(&body_hash)
            .at_time(skewed)
            .sign();
        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &s.authorization)
            .header("x-amz-date", &s.amz_date)
            .header("x-amz-content-sha256", &s.amz_content_sha256)
            .send(body.as_ref())
            .expect("transport error");
        let status = resp.status().as_u16();
        let rbody = resp.body_mut().read_to_string().unwrap();
        assert_eq!(status, 403, "expected 403, got {}", status);
        assert_error_code(&rbody, "RequestTimeTooSkewed");
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_put_date_tampered() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let body = b"tampered date body";
        let path = format!("/{}/obj", bucket);
        let body_hash = sha256_hex(body);
        let s = Signer::new("PUT", &path).body_hash(&body_hash).sign();
        let url = format!("{}{}", CTX.endpoint(), path);
        // Send a different x-amz-date than what was signed
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &s.authorization)
            .header("x-amz-date", "20200101T000000Z")
            .header("x-amz-content-sha256", &s.amz_content_sha256)
            .send(body.as_ref())
            .expect("transport error");
        let status = resp.status().as_u16();
        let rbody = resp.body_mut().read_to_string().unwrap();
        assert_eq!(status, 403, "expected 403, got {}", status);
        assert_error_code(&rbody, "RequestTimeTooSkewed");
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_put_date_missing_signed() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let body = b"missing date body";
        let path = format!("/{}/obj", bucket);
        let body_hash = sha256_hex(body);
        let s = Signer::new("PUT", &path).body_hash(&body_hash).sign();
        let url = format!("{}{}", CTX.endpoint(), path);
        // Strip x-amz-date entirely — auth header declares it as signed
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &s.authorization)
            .header("x-amz-content-sha256", &s.amz_content_sha256)
            .send(body.as_ref())
            .expect("transport error");
        let status = resp.status().as_u16();
        let rbody = resp.body_mut().read_to_string().unwrap();
        assert_eq!(status, 403, "expected 403, got {}", status);
        assert_error_code(&rbody, "AccessDenied");
        cleanup(&bucket, &[]).await;
    });
}

// ── Group 5: User-Agent ─────────────────────────────────────────────────

#[test]
fn test_put_empty_user_agent() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let body = b"empty ua body";
        let path = format!("/{}/obj", bucket);
        let body_hash = sha256_hex(body);
        let s = Signer::new("PUT", &path).body_hash(&body_hash).sign();
        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &s.authorization)
            .header("x-amz-date", &s.amz_date)
            .header("x-amz-content-sha256", &s.amz_content_sha256)
            .header("User-Agent", "")
            .send(body.as_ref())
            .expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(status, 200);
        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_put_no_user_agent() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let body = b"no ua body";
        let (status, _) = signed_put(&bucket, "obj", body);
        assert_eq!(status, 200);
        cleanup(&bucket, &["obj"]).await;
    });
}

// ── Group 6: Expect Header ──────────────────────────────────────────────

#[test]
fn test_put_expect_100_continue() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let body = b"expect body";
        let path = format!("/{}/obj", bucket);
        let body_hash = sha256_hex(body);
        let s = Signer::new("PUT", &path).body_hash(&body_hash).sign();
        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &s.authorization)
            .header("x-amz-date", &s.amz_date)
            .header("x-amz-content-sha256", &s.amz_content_sha256)
            .header("Expect", "100-continue")
            .send(body.as_ref())
            .expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(status, 200);
        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_put_expect_empty() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let body = b"expect empty body";
        let path = format!("/{}/obj", bucket);
        let body_hash = sha256_hex(body);
        let s = Signer::new("PUT", &path).body_hash(&body_hash).sign();
        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &s.authorization)
            .header("x-amz-date", &s.amz_date)
            .header("x-amz-content-sha256", &s.amz_content_sha256)
            .header("Expect", "")
            .send(body.as_ref())
            .expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(status, 200);
        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_put_expect_garbage() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let body = b"expect garbage body";
        let path = format!("/{}/obj", bucket);
        let body_hash = sha256_hex(body);
        let s = Signer::new("PUT", &path).body_hash(&body_hash).sign();
        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &s.authorization)
            .header("x-amz-date", &s.amz_date)
            .header("x-amz-content-sha256", &s.amz_content_sha256)
            .header("Expect", "garbage")
            .send(body.as_ref())
            .expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        // AWS ignores unknown Expect values and processes the request normally
        assert_eq!(status, 200, "expected 200, got {}", status);
        // Object may have been created if status was 200
        cleanup(&bucket, &["obj"]).await;
    });
}

// ── Group 7: ACL Header ─────────────────────────────────────────────────

#[test]
fn test_bucket_create_bad_acl() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        let path = format!("/{}", bucket);
        let s = Signer::new("PUT", &path).body_hash(&sha256_hex(b"")).sign();
        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &s.authorization)
            .header("x-amz-date", &s.amz_date)
            .header("x-amz-content-sha256", &s.amz_content_sha256)
            .header("x-amz-acl", "garbage")
            .send(b"" as &[u8])
            .expect("transport error");
        let status = resp.status().as_u16();
        let rbody = resp.body_mut().read_to_string().unwrap();
        assert_eq!(status, 403, "expected 403, got {}", status);
        assert_error_code(&rbody, "AccessDenied");
    });
}

// ── Group 8: Payload Integrity ──────────────────────────────────────────

#[test]
fn test_put_body_sha256_mismatch() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let signed_body = b"the body that was signed";
        let actual_body = b"different body content";
        let path = format!("/{}/obj", bucket);
        let body_hash = sha256_hex(signed_body);
        // Sign with hash of signed_body
        let s = Signer::new("PUT", &path).body_hash(&body_hash).sign();
        let url = format!("{}{}", CTX.endpoint(), path);
        // But send actual_body — signature was for signed_body
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &s.authorization)
            .header("x-amz-date", &s.amz_date)
            .header("x-amz-content-sha256", &s.amz_content_sha256)
            .send(actual_body.as_ref())
            .expect("transport error");
        let status = resp.status().as_u16();
        let rbody = resp.body_mut().read_to_string().unwrap();
        // Body doesn't match x-amz-content-sha256 → 400
        assert_eq!(status, 400, "expected 400, got {}", status);
        assert_error_code(&rbody, "XAmzContentSHA256Mismatch");
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_put_content_sha256_mismatch() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let body = b"the actual body";
        let path = format!("/{}/obj", bucket);
        // Claim a hash that doesn't match the body, but sign using that claimed hash
        // so the signature itself is valid (the credential check passes).
        let fake_hash = sha256_hex(b"some other data");
        let s = Signer::new("PUT", &path).body_hash(&fake_hash).sign();
        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &s.authorization)
            .header("x-amz-date", &s.amz_date)
            .header("x-amz-content-sha256", &s.amz_content_sha256)
            .send(body.as_ref())
            .expect("transport error");
        let status = resp.status().as_u16();
        let rbody = resp.body_mut().read_to_string().unwrap();
        assert_eq!(status, 400, "expected 400, got {}", status);
        assert_error_code(&rbody, "XAmzContentSHA256Mismatch");
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_put_unsigned_payload() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let body = b"unsigned payload body";
        let path = format!("/{}/obj", bucket);
        let s = Signer::new("PUT", &path)
            .body_hash("UNSIGNED-PAYLOAD")
            .sign();
        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &s.authorization)
            .header("x-amz-date", &s.amz_date)
            .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
            .send(body.as_ref())
            .expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(status, 200);
        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_put_empty_body() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let (status, _) = signed_put(&bucket, "obj", b"");
        assert_eq!(status, 200);
        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_get_unsigned_payload() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let client = CTX.client();
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let path = format!("/{}/obj", bucket);
        let s = Signer::new("GET", &path)
            .body_hash("UNSIGNED-PAYLOAD")
            .sign();
        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .get(&url)
            .header("Authorization", &s.authorization)
            .header("x-amz-date", &s.amz_date)
            .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
            .call()
            .expect("transport error");
        let status = resp.status().as_u16();
        let data = resp.body_mut().read_to_vec().unwrap();
        assert_eq!(status, 200);
        assert_eq!(&data[..], b"data");
        cleanup(&bucket, &["obj"]).await;
    });
}

// ── Group 9: Malformed SigV4 ────────────────────────────────────────────

#[test]
fn test_put_wrong_algorithm() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let body = b"wrong algo body";
        let path = format!("/{}/obj", bucket);
        let body_hash = sha256_hex(body);
        let s = Signer::new("PUT", &path).body_hash(&body_hash).sign();
        // Replace the algorithm in the Authorization header
        let bad_auth = s
            .authorization
            .replace("AWS4-HMAC-SHA256", "AWS4-HMAC-SHA512");
        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &bad_auth)
            .header("x-amz-date", &s.amz_date)
            .header("x-amz-content-sha256", &s.amz_content_sha256)
            .send(body.as_ref())
            .expect("transport error");
        let status = resp.status().as_u16();
        let rbody = resp.body_mut().read_to_string().unwrap();
        assert_eq!(status, 400, "expected 400, got {}", status);
        assert_error_code(&rbody, "InvalidArgument");
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_put_missing_credential() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let url = format!("{}/{}/obj", CTX.endpoint(), bucket);
        let auth =
            "AWS4-HMAC-SHA256 SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=abc123";
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let (y, m, d) = days_to_ymd(now / 86400);
        let secs_today = now % 86400;
        let (hh, mm, ss) = (secs_today / 3600, (secs_today % 3600) / 60, secs_today % 60);
        let amz_date = format!("{:04}{:02}{:02}T{:02}{:02}{:02}Z", y, m, d, hh, mm, ss);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", auth)
            .header("x-amz-date", &amz_date)
            .header("x-amz-content-sha256", &sha256_hex(b"data"))
            .send(b"data" as &[u8])
            .expect("transport error");
        let status = resp.status().as_u16();
        let rbody = resp.body_mut().read_to_string().unwrap();
        assert_eq!(status, 400, "expected 400, got {}", status);
        assert_error_code(&rbody, "AuthorizationHeaderMalformed");
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_put_missing_signature() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let url = format!("{}/{}/obj", CTX.endpoint(), bucket);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let (y, m, d) = days_to_ymd(now / 86400);
        let secs_today = now % 86400;
        let (hh, mm, ss) = (secs_today / 3600, (secs_today % 3600) / 60, secs_today % 60);
        let short_date = format!("{:04}{:02}{:02}", y, m, d);
        let amz_date = format!("{}T{:02}{:02}{:02}Z", short_date, hh, mm, ss);
        let cred = format!(
            "{}/{}/{}/s3/aws4_request",
            CTX.access_key(),
            short_date,
            CTX.region()
        );
        let auth = format!(
            "AWS4-HMAC-SHA256 Credential={}, SignedHeaders=host;x-amz-content-sha256;x-amz-date",
            cred
        );
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &auth)
            .header("x-amz-date", &amz_date)
            .header("x-amz-content-sha256", &sha256_hex(b"data"))
            .send(b"data" as &[u8])
            .expect("transport error");
        let status = resp.status().as_u16();
        let rbody = resp.body_mut().read_to_string().unwrap();
        assert_eq!(status, 400, "expected 400, got {}", status);
        assert_error_code(&rbody, "AuthorizationHeaderMalformed");
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_put_bad_credential_scope() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let url = format!("{}/{}/obj", CTX.endpoint(), bucket);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let (y, m, d) = days_to_ymd(now / 86400);
        let secs_today = now % 86400;
        let (hh, mm, ss) = (secs_today / 3600, (secs_today % 3600) / 60, secs_today % 60);
        let short_date = format!("{:04}{:02}{:02}", y, m, d);
        let amz_date = format!("{}T{:02}{:02}{:02}Z", short_date, hh, mm, ss);
        // Credential with wrong part count (only 3 parts instead of 5)
        let auth = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}/bad, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=abc123",
            CTX.access_key(),
            short_date,
        );
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &auth)
            .header("x-amz-date", &amz_date)
            .header("x-amz-content-sha256", &sha256_hex(b"data"))
            .send(b"data" as &[u8])
            .expect("transport error");
        let status = resp.status().as_u16();
        let rbody = resp.body_mut().read_to_string().unwrap();
        assert_eq!(status, 400, "expected 400, got {}", status);
        assert_error_code(&rbody, "AuthorizationHeaderMalformed");
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_put_wrong_region() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let path = format!("/{bucket}");
        let wrong_region = if CTX.region() == "us-east-1" {
            "us-west-2"
        } else {
            "us-east-1"
        };
        let s = Signer::new("GET", &path)
            .body_hash(&sha256_hex(b""))
            .region(wrong_region)
            .sign();
        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .get(&url)
            .header("Authorization", &s.authorization)
            .header("x-amz-date", &s.amz_date)
            .header("x-amz-content-sha256", &s.amz_content_sha256)
            .call()
            .expect("transport error");
        let status = resp.status().as_u16();
        let bucket_region = resp
            .headers()
            .get("x-amz-bucket-region")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let rbody = resp.body_mut().read_to_string().unwrap();
        assert_eq!(status, 400, "expected 400, got {}", status);
        assert_error_code(&rbody, "AuthorizationHeaderMalformed");
        assert_eq!(bucket_region.as_deref(), Some(CTX.region()));
        assert!(rbody.contains(&format!("<Region>{}</Region>", CTX.region())));
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_put_object_wrong_region_includes_region_hint() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let body = b"wrong region body";
        let path = format!("/{bucket}/obj");
        let wrong_region = if CTX.region() == "us-east-1" {
            "us-west-2"
        } else {
            "us-east-1"
        };
        let signer = Signer::new("PUT", &path)
            .body_hash(&sha256_hex(body))
            .region(wrong_region)
            .sign();
        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &signer.authorization)
            .header("x-amz-date", &signer.amz_date)
            .header("x-amz-content-sha256", &signer.amz_content_sha256)
            .send(body.as_ref())
            .expect("transport error");
        let status = resp.status().as_u16();
        let rbody = resp.body_mut().read_to_string().unwrap();
        assert_eq!(status, 400, "expected 400, got {}", status);
        assert_error_code(&rbody, "AuthorizationHeaderMalformed");
        assert!(rbody.contains(&format!("<Region>{}</Region>", CTX.region())));
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_head_bucket_returns_bucket_region_header() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let path = format!("/{bucket}");
        let s = Signer::new("HEAD", &path)
            .body_hash(&sha256_hex(b""))
            .sign();
        let url = format!("{}{}", CTX.endpoint(), path);
        let response = agent()
            .head(&url)
            .header("Authorization", &s.authorization)
            .header("x-amz-date", &s.amz_date)
            .header("x-amz-content-sha256", &s.amz_content_sha256)
            .call()
            .expect("transport error");
        assert_eq!(response.status().as_u16(), 200);
        let bucket_region = response
            .headers()
            .get("x-amz-bucket-region")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        assert_eq!(bucket_region.as_deref(), Some(CTX.region()));
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_put_wrong_service() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let body = b"wrong service body";
        let path = format!("/{}/obj", bucket);
        let body_hash = sha256_hex(body);
        let s = Signer::new("PUT", &path)
            .body_hash(&body_hash)
            .service("iam")
            .sign();
        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &s.authorization)
            .header("x-amz-date", &s.amz_date)
            .header("x-amz-content-sha256", &s.amz_content_sha256)
            .send(body.as_ref())
            .expect("transport error");
        let status = resp.status().as_u16();
        let rbody = resp.body_mut().read_to_string().unwrap();
        assert_eq!(status, 400, "expected 400, got {}", status);
        assert_error_code(&rbody, "AuthorizationHeaderMalformed");
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_sigv2_rejected_in_region_that_requires_sigv4() {
    s3_tests::run(async {
        if !sigv2_unsupported_in_region(CTX.region()) {
            return;
        }
        let bucket = setup_bucket().await;
        let url = format!("{}/{}", CTX.endpoint(), bucket);
        let date = current_http_date();
        let mut resp = agent()
            .get(&url)
            .header("Authorization", &sigv2_authorization(&bucket, &date))
            .header("Date", &date)
            .call()
            .expect("transport error");
        let status = resp.status().as_u16();
        let rbody = resp.body_mut().read_to_string().unwrap();
        assert_eq!(status, 400, "expected 400, got {}", status);
        assert_error_code(&rbody, "InvalidRequest");
        assert!(rbody.contains(
            "The authorization mechanism you have provided is not supported. Please use AWS4-HMAC-SHA256."
        ));
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_unexpected_security_token_on_static_credentials_returns_bad_request() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "token-check.txt";
        let put = signed_put(&bucket, key, b"token check body");
        assert_eq!(put.0, 200, "expected setup PUT to succeed, got {}", put.0);

        let url = format!("{}/{}/{}", CTX.endpoint(), bucket, key);
        let response = s3_tests::send_signed_request(
            "GET",
            &url,
            b"",
            [("x-amz-security-token", "bad-token-causes-400")],
        );
        assert_eq!(
            response.status, 400,
            "expected 400, got {}",
            response.status
        );
        assert_error_code(&response.body, "InvalidToken");
        assert!(
            response.body.contains(
                "<Message>The provided token is malformed or otherwise invalid.</Message>"
            ),
            "expected InvalidToken message, got: {}",
            response.body
        );
        assert!(
            response
                .body
                .contains("<Token-0>bad-token-causes-400</Token-0>"),
            "expected echoed token, got: {}",
            response.body
        );

        cleanup(&bucket, &[key]).await;
    });
}

// ── Group: request.rs error paths ──────────────────────────────────────

/// PUT with invalid percent-encoded UTF-8 in the object key should fail.
/// e.g. `%80` is not valid UTF-8 (it's a continuation byte without a leader).
#[test]
fn test_put_invalid_percent_encoding_in_key() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        // %80 is an invalid UTF-8 byte — percent_decode_strict should reject it.
        let path = format!("/{}/bad%80key", bucket);
        let s = Signer::new("PUT", &path).body_hash(&sha256_hex(b"")).sign();
        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &s.authorization)
            .header("x-amz-date", &s.amz_date)
            .header("x-amz-content-sha256", &s.amz_content_sha256)
            .send(b"".as_ref())
            .expect("transport error");
        let status = resp.status().as_u16();
        let body = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(
            status, 400,
            "expected 400 for invalid percent-encoding, got {status}"
        );
        assert_error_code(&body, "InvalidURI");
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_put_duplicate_authorization() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let body = b"dup auth body";
        let path = format!("/{}/obj", bucket);
        let body_hash = sha256_hex(body);
        let s = Signer::new("PUT", &path).body_hash(&body_hash).sign();
        let url = format!("{}{}", CTX.endpoint(), path);
        // Send two Authorization headers
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &s.authorization)
            .header("Authorization", &s.authorization)
            .header("x-amz-date", &s.amz_date)
            .header("x-amz-content-sha256", &s.amz_content_sha256)
            .send(body.as_ref())
            .expect("transport error");
        let status = resp.status().as_u16();
        let rbody = resp.body_mut().read_to_string().unwrap();
        assert_eq!(status, 501, "expected 501, got {}", status);
        assert_error_code(&rbody, "NotImplemented");
        assert!(
            rbody.contains("<Header>Authorization</Header>"),
            "expected Header element in response body, got {rbody}"
        );
        cleanup(&bucket, &[]).await;
    });
}
