use std::time::{SystemTime, UNIX_EPOCH};

use auth::canonical::canonical_query_string;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::ObjectCannedAcl;
use ring::{digest, hmac};
use s3_tests::CTX;

fn agent() -> s3_tests::Agent {
    s3_tests::test_agent()
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

struct Signer {
    method: String,
    path: String,
    query: String,
    body_hash: Option<String>,
}

impl Signer {
    fn new(method: &str, path: &str) -> Self {
        Self {
            method: method.to_string(),
            path: path.to_string(),
            query: String::new(),
            body_hash: None,
        }
    }

    fn query(mut self, query: &str) -> Self {
        self.query = query.to_string();
        self
    }

    fn body_hash(mut self, hash: &str) -> Self {
        self.body_hash = Some(hash.to_string());
        self
    }

    fn sign(self) -> SignedHeaders {
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

        let content_sha256 = self.body_hash.unwrap_or_else(|| sha256_hex(b""));
        let host_val = host();
        let signed_headers = "host;x-amz-content-sha256;x-amz-date";
        let canonical_headers = format!(
            "host:{}\nx-amz-content-sha256:{}\nx-amz-date:{}\n",
            host_val, content_sha256, date_long
        );
        let canonical_query = canonical_query_string(&self.query);
        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            self.method,
            self.path,
            canonical_query,
            canonical_headers,
            signed_headers,
            content_sha256
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
}

fn signed_get(bucket: &str, key: &str, query: &str) -> (u16, Vec<(String, String)>, String) {
    let path = format!("/{}/{}", bucket, key);
    let url = if query.is_empty() {
        format!("{}{}", CTX.endpoint(), path)
    } else {
        format!("{}{}?{}", CTX.endpoint(), path, query)
    };
    let s = Signer::new("GET", &path)
        .query(query)
        .body_hash(&sha256_hex(b""))
        .sign();
    let mut resp = agent()
        .get(&url)
        .header("Authorization", &s.authorization)
        .header("x-amz-date", &s.amz_date)
        .header("x-amz-content-sha256", &s.amz_content_sha256)
        .call()
        .expect("transport error");
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
    let body = resp.body_mut().read_to_string().unwrap_or_default();
    (status, headers, body)
}

fn anonymous_get(bucket: &str, key: &str, query: &str) -> (u16, Vec<(String, String)>, String) {
    let path = format!("/{}/{}", bucket, key);
    let url = if query.is_empty() {
        format!("{}{}", CTX.endpoint(), path)
    } else {
        format!("{}{}?{}", CTX.endpoint(), path, query)
    };
    let mut resp = agent().get(&url).call().expect("transport error");
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
    let body = resp.body_mut().read_to_string().unwrap_or_default();
    (status, headers, body)
}

fn response_header(headers: &[(String, String)], name: &str) -> Option<String> {
    headers
        .iter()
        .find(|(header_name, _)| header_name.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.clone())
}

#[test]
fn test_get_response_override_headers_require_signed_requests() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_public_bucket().await;
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .acl(ObjectCannedAcl::PublicRead)
            .content_type("application/octet-stream")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let query = "response-content-type=text%2Fplain";

        let (signed_status, signed_headers, signed_body) = signed_get(&bucket, "obj", query);
        assert_eq!(signed_status, 200);
        assert_eq!(signed_body, "data");
        assert_eq!(
            response_header(&signed_headers, "Content-Type"),
            Some("text/plain".to_string())
        );

        let (anon_status, anon_headers, anon_body) = anonymous_get(&bucket, "obj", query);
        assert_eq!(
            anon_status, 400,
            "unexpected anonymous response status={anon_status} body={anon_body}"
        );
        assert!(
            anon_body.contains("<Code>InvalidRequest</Code>"),
            "unexpected anonymous response body: {anon_body}"
        );
        assert!(
            anon_body.contains(
                "Request specific response headers cannot be used for anonymous GET requests."
            ),
            "unexpected anonymous response body: {anon_body}"
        );
        assert!(response_header(&anon_headers, "Content-Type").is_some());

        cleanup(&bucket, &["obj"]).await;
    });
}

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
