use std::time::{SystemTime, UNIX_EPOCH};

use ring::{digest, hmac};
use s3_tests::{unique_bucket, CTX};

fn agent() -> s3_tests::Agent {
    s3_tests::test_agent()
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

fn host() -> &'static str {
    CTX.endpoint()
        .trim_start_matches("http://")
        .trim_start_matches("https://")
}

struct SignedHeaders {
    authorization: String,
    amz_date: String,
    amz_content_sha256: String,
}

fn sign_put_without_acl_header(path: &str, body: &[u8]) -> SignedHeaders {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
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
    let content_sha256 = sha256_hex(body);
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let canonical_headers = format!(
        "host:{}\nx-amz-content-sha256:{}\nx-amz-date:{}\n",
        host(),
        content_sha256,
        date_long
    );
    let canonical_request = format!(
        "PUT\n{}\n\n{}\n{}\n{}",
        path, canonical_headers, signed_headers, content_sha256
    );
    let canonical_hash = sha256_hex(canonical_request.as_bytes());
    let scope = format!("{}/{}/s3/aws4_request", date_short, CTX.region());
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        date_long, scope, canonical_hash
    );
    let signing_key = derive_signing_key(CTX.secret_key(), date_short, CTX.region(), "s3");
    let signature = hmac_sha256(signing_key.as_ref(), string_to_sign.as_bytes());
    let sig_hex = hex_encode(signature.as_ref());
    let credential = format!(
        "{}/{}/{}/s3/aws4_request",
        CTX.access_key(),
        date_short,
        CTX.region()
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

#[test]
fn test_bucket_create_bad_acl() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        let path = format!("/{}", bucket);
        let signed = sign_put_without_acl_header(&path, b"");
        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &signed.authorization)
            .header("x-amz-date", &signed.amz_date)
            .header("x-amz-content-sha256", &signed.amz_content_sha256)
            .header("x-amz-acl", "garbage")
            .send(b"" as &[u8])
            .expect("transport error");
        let status = resp.status().as_u16();
        let rbody = resp.body_mut().read_to_string().unwrap();
        assert_eq!(status, 403, "expected 403, got {}", status);
        assert_error_code(&rbody, "AccessDenied");
    });
}
