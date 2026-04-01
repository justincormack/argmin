use base64::Engine;
use std::time::{SystemTime, UNIX_EPOCH};

use aws_sdk_s3::types::{BucketLocationConstraint, CreateBucketConfiguration};
use ring::hmac;
use s3_tests::{unique_bucket, CTX};

fn agent() -> ureq::Agent {
    s3_tests::test_agent()
}

async fn create_bucket_in_test_region(bucket: &str) {
    let client = CTX.client();
    let mut request = client.create_bucket().bucket(bucket);
    if CTX.region() != "us-east-1" {
        let config = CreateBucketConfiguration::builder()
            .location_constraint(BucketLocationConstraint::from(CTX.region()))
            .build();
        request = request.create_bucket_configuration(config);
    }
    request.send().await.unwrap();
}

async fn cleanup_bucket(bucket: &str) {
    let client = CTX.client();
    let _ = client.delete_bucket_lifecycle().bucket(bucket).send().await;
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

fn sha256_hex(data: &[u8]) -> String {
    let digest = ring::digest::digest(&ring::digest::SHA256, data);
    digest
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let key = hmac::Key::new(hmac::HMAC_SHA256, key);
    hmac::sign(&key, data).as_ref().to_vec()
}

fn md5_b64(data: &[u8]) -> String {
    use md5_legacy::Digest;

    let digest = md5_legacy::Md5::digest(data);
    base64::engine::general_purpose::STANDARD.encode(&digest[..])
}

fn format_amz_date(epoch_secs: u64) -> String {
    let days = epoch_secs / 86_400;
    let time_of_day = epoch_secs % 86_400;
    let (year, month, day) = days_to_date(days as i64);
    format!(
        "{year:04}{month:02}{day:02}T{:02}{:02}{:02}Z",
        time_of_day / 3_600,
        (time_of_day % 3_600) / 60,
        time_of_day % 60,
    )
}

fn days_to_date(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = (z - era * 146_097) as u32;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era as i64 + era * 400;
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

fn normalize_query(raw: &str) -> String {
    if raw.is_empty() {
        return String::new();
    }
    let mut pairs: Vec<(String, String)> = raw
        .split('&')
        .filter(|segment| !segment.is_empty())
        .map(|segment| {
            let mut parts = segment.splitn(2, '=');
            let key = parts.next().unwrap_or("").to_string();
            let value = parts.next().unwrap_or("").to_string();
            (key, value)
        })
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn send_signed_put(url_str: &str, body: &[u8], include_content_md5: bool) -> (u16, String) {
    let parsed = url::Url::parse(url_str).expect("parse lifecycle URL");
    let host = parsed
        .host_str()
        .map(|host| {
            if let Some(port) = parsed.port() {
                format!("{host}:{port}")
            } else {
                host.to_string()
            }
        })
        .expect("host in lifecycle URL");
    let path = parsed.path();
    let query = normalize_query(parsed.query().unwrap_or(""));

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let amz_date = format_amz_date(now);
    let date_stamp = &amz_date[..8];

    let payload_hash = sha256_hex(body);
    let content_md5 = include_content_md5.then(|| md5_b64(body));
    let mut canonical_headers = String::new();
    let mut signed_headers = Vec::new();
    if let Some(content_md5) = &content_md5 {
        canonical_headers.push_str(&format!("content-md5:{content_md5}\n"));
        signed_headers.push("content-md5");
    }
    canonical_headers.push_str(&format!(
        "host:{host}\n\
         x-amz-content-sha256:{payload_hash}\n\
         x-amz-date:{amz_date}\n"
    ));
    signed_headers.extend(["host", "x-amz-content-sha256", "x-amz-date"]);
    let signed_headers = signed_headers.join(";");
    let canonical_request =
        format!("PUT\n{path}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}");
    let scope = format!("{}/{}/s3/aws4_request", date_stamp, CTX.region());
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );

    let k_date = hmac_sha256(
        format!("AWS4{}", CTX.secret_key()).as_bytes(),
        date_stamp.as_bytes(),
    );
    let k_region = hmac_sha256(&k_date, CTX.region().as_bytes());
    let k_service = hmac_sha256(&k_region, b"s3");
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    let signature: String = hmac_sha256(&k_signing, string_to_sign.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        CTX.access_key()
    );

    let mut request = agent()
        .put(url_str)
        .header("Authorization", &authorization)
        .header("x-amz-content-sha256", &payload_hash)
        .header("x-amz-date", &amz_date)
        .header("Content-Type", "application/xml");
    if let Some(content_md5) = &content_md5 {
        request = request.header("Content-MD5", content_md5);
    }
    let mut response = request.send(body).expect("lifecycle PUT transport error");
    let status = response.status().as_u16();
    let body = response.body_mut().read_to_string().unwrap_or_default();
    (status, body)
}

#[test]
fn test_put_bucket_lifecycle_accepts_midnight_utc_timestamp_without_millis() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        create_bucket_in_test_region(&bucket).await;

        let body = "<LifecycleConfiguration>\
                <Rule>\
                    <ID>expire-by-date</ID>\
                    <Filter><Prefix>logs/</Prefix></Filter>\
                    <Status>Enabled</Status>\
                    <Expiration><Date>2099-01-01T00:00:00Z</Date></Expiration>\
                </Rule>\
            </LifecycleConfiguration>"
            .to_string();
        let url = format!("{}/{}?lifecycle", CTX.endpoint(), bucket);
        let (status, response_body) = send_signed_put(&url, body.as_bytes(), true);

        let get_result = CTX
            .client()
            .get_bucket_lifecycle_configuration()
            .bucket(&bucket)
            .send()
            .await;

        cleanup_bucket(&bucket).await;

        if std::env::var("S3_TEST_ENDPOINT").is_ok()
            && status == 403
            && response_body.contains("<Code>AccessDenied</Code>")
            && response_body.contains("s3:PutLifecycleConfiguration")
        {
            eprintln!(
                "skipping external lifecycle compatibility probe: missing s3:PutLifecycleConfiguration permission"
            );
            return;
        }

        assert_eq!(
            status, 200,
            "expected lifecycle PUT to succeed, got status {status} body {response_body}"
        );
        let lifecycle = get_result.expect("expected stored lifecycle configuration");
        assert_eq!(lifecycle.rules().len(), 1);
    });
}

#[test]
fn test_put_bucket_lifecycle_requires_content_md5() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        create_bucket_in_test_region(&bucket).await;

        let body = "<LifecycleConfiguration>\
                <Rule>\
                    <ID>expire-by-date</ID>\
                    <Filter><Prefix>logs/</Prefix></Filter>\
                    <Status>Enabled</Status>\
                    <Expiration><Date>2099-01-01T00:00:00Z</Date></Expiration>\
                </Rule>\
            </LifecycleConfiguration>"
            .to_string();
        let url = format!("{}/{}?lifecycle", CTX.endpoint(), bucket);
        let (status, response_body) = send_signed_put(&url, body.as_bytes(), false);

        cleanup_bucket(&bucket).await;

        if std::env::var("S3_TEST_ENDPOINT").is_ok()
            && status == 403
            && response_body.contains("<Code>AccessDenied</Code>")
            && response_body.contains("s3:PutLifecycleConfiguration")
        {
            eprintln!(
                "skipping external lifecycle compatibility probe: missing s3:PutLifecycleConfiguration permission"
            );
            return;
        }

        assert_eq!(
            status, 400,
            "expected lifecycle PUT without Content-MD5 to fail, got status {status} body {response_body}"
        );
        assert!(response_body.contains("<Code>InvalidRequest</Code>"));
        assert!(response_body.contains("Content-MD5"));
    });
}
