use base64::Engine;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_sdk_s3::primitives::{ByteStream, DateTime, DateTimeFormat};
use aws_sdk_s3::types::{
    AbortIncompleteMultipartUpload, BucketLifecycleConfiguration, BucketLocationConstraint,
    BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, CreateBucketConfiguration,
    ExpirationStatus, LifecycleExpiration, LifecycleRule, LifecycleRuleAndOperator,
    LifecycleRuleFilter, NoncurrentVersionExpiration, Tag, VersioningConfiguration,
};
use ring::hmac;
use s3_tests::{
    cleanup_versioned_bucket, content_md5_header, delete_all_and_bucket, err_status,
    put_bucket_lifecycle_with_md5, send_signed_request, unique_bucket, CTX,
};

fn agent() -> s3_tests::Agent {
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

async fn enable_versioning(bucket: &str) {
    CTX.client()
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
}

async fn cleanup_versioned_lifecycle_bucket(bucket: &str) {
    let client = CTX.client();
    let _ = client.delete_bucket_lifecycle().bucket(bucket).send().await;
    cleanup_versioned_bucket(client, bucket).await;
}

fn assert_error_code(body: &str, code: &str) {
    let expected = format!("<Code>{code}</Code>");
    assert!(
        body.contains(&expected),
        "expected {expected} in body: {body}"
    );
}

fn assert_error_message(body: &str, message: &str) {
    let expected = format!("<Message>{message}</Message>");
    assert!(
        body.contains(&expected),
        "expected {expected} in body: {body}"
    );
}

fn assert_lifecycle_expiration_header(expiration: Option<&str>, rule_id: &str) {
    let expiration = expiration.expect("expected x-amz-expiration header");
    assert!(
        expiration.contains("expiry-date=\""),
        "expected expiry-date in header: {expiration}"
    );
    assert!(
        expiration.contains(&format!("rule-id=\"{rule_id}\"")),
        "expected rule-id {rule_id} in header: {expiration}"
    );
}

async fn put_object_until_expiration_header(
    bucket: &str,
    key: &str,
    body: &[u8],
    tagging: Option<&str>,
    rule_id: &str,
) -> aws_sdk_s3::operation::put_object::PutObjectOutput {
    const MAX_ATTEMPTS: usize = 10;

    for attempt in 0..MAX_ATTEMPTS {
        let mut request = CTX
            .client()
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(body.to_vec()));
        if let Some(tagging) = tagging {
            request = request.tagging(tagging);
        }
        let output = request.send().await.unwrap();
        if let Some(expiration) = output.expiration() {
            assert_lifecycle_expiration_header(Some(expiration), rule_id);
            return output;
        }
        if attempt + 1 == MAX_ATTEMPTS {
            panic!(
                "expected x-amz-expiration header on PutObject for key {key} after {MAX_ATTEMPTS} attempts"
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    unreachable!()
}

async fn assert_head_object_expiration_header_eventually(bucket: &str, key: &str, rule_id: &str) {
    const MAX_ATTEMPTS: usize = 10;

    for attempt in 0..MAX_ATTEMPTS {
        let output = CTX
            .client()
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        if let Some(expiration) = output.expiration() {
            assert_lifecycle_expiration_header(Some(expiration), rule_id);
            return;
        }
        if attempt + 1 == MAX_ATTEMPTS {
            panic!(
                "expected x-amz-expiration header on HeadObject for key {key} after {MAX_ATTEMPTS} attempts"
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn assert_get_object_expiration_header_eventually(bucket: &str, key: &str, rule_id: &str) {
    const MAX_ATTEMPTS: usize = 10;

    for attempt in 0..MAX_ATTEMPTS {
        let output = CTX
            .client()
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        if let Some(expiration) = output.expiration() {
            assert_lifecycle_expiration_header(Some(expiration), rule_id);
            return;
        }
        if attempt + 1 == MAX_ATTEMPTS {
            panic!(
                "expected x-amz-expiration header on GetObject for key {key} after {MAX_ATTEMPTS} attempts"
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn assert_lifecycle_deleted_eventually(bucket: &str) {
    const MAX_ATTEMPTS: usize = 20;

    for attempt in 0..MAX_ATTEMPTS {
        let result = CTX
            .client()
            .get_bucket_lifecycle_configuration()
            .bucket(bucket)
            .send()
            .await;
        if result.is_err() && err_status(&result) == 404 {
            s3_tests::assert_s3_err_code(&result, "NoSuchLifecycleConfiguration");
            return;
        }
        if attempt + 1 == MAX_ATTEMPTS {
            panic!(
                "GetBucketLifecycleConfiguration did not converge to NoSuchLifecycleConfiguration for {bucket}: {:?}",
                result
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
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

async fn assert_invalid_lifecycle_put_rejected(body: &str, expected_code: &str) {
    let bucket = unique_bucket();
    create_bucket_in_test_region(&bucket).await;

    let url = format!("{}/{}?lifecycle", CTX.endpoint(), bucket);
    let (status, response_body) = send_signed_put(&url, body.as_bytes(), true);

    cleanup_bucket(&bucket).await;

    assert_eq!(
        status, 400,
        "expected lifecycle PUT to fail, got status {status} body {response_body}"
    );
    assert_error_code(&response_body, expected_code);
}

async fn assert_invalid_lifecycle_put_rejected_with_message(
    body: &str,
    expected_code: &str,
    expected_message: &str,
) {
    let bucket = unique_bucket();
    create_bucket_in_test_region(&bucket).await;

    let url = format!("{}/{}?lifecycle", CTX.endpoint(), bucket);
    let (status, response_body) = send_signed_put(&url, body.as_bytes(), true);

    cleanup_bucket(&bucket).await;

    assert_eq!(
        status, 400,
        "expected lifecycle PUT to fail, got status {status} body {response_body}"
    );
    assert_error_code(&response_body, expected_code);
    assert_error_message(&response_body, expected_message);
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

        assert_eq!(
            status, 400,
            "expected lifecycle PUT without Content-MD5 to fail, got status {status} body {response_body}"
        );
        assert!(response_body.contains("<Code>InvalidRequest</Code>"));
        assert!(
            response_body.contains(
                "<Message>Missing required header for this request: Content-MD5</Message>"
            ),
            "body: {response_body}"
        );
    });
}

#[test]
fn test_get_bucket_lifecycle_configuration_absent_returns_no_such_lifecycle_configuration() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        let client = CTX.client();
        create_bucket_in_test_region(&bucket).await;

        let get = client
            .get_bucket_lifecycle_configuration()
            .bucket(&bucket)
            .send()
            .await;

        cleanup_bucket(&bucket).await;

        assert_eq!(err_status(&get), 404);
        s3_tests::assert_s3_err_code(&get, "NoSuchLifecycleConfiguration");
    });
}

#[test]
fn test_put_bucket_lifecycle_rejects_rule_id_longer_than_255_chars() {
    s3_tests::run(async {
        let long_id = "a".repeat(256);
        let body = format!(
            "<LifecycleConfiguration>\
                <Rule>\
                    <ID>{long_id}</ID>\
                    <Filter><Prefix>logs/</Prefix></Filter>\
                    <Status>Enabled</Status>\
                    <Expiration><Days>1</Days></Expiration>\
                </Rule>\
            </LifecycleConfiguration>"
        );
        assert_invalid_lifecycle_put_rejected(&body, "InvalidArgument").await;
    });
}

#[test]
fn test_put_bucket_lifecycle_rejects_duplicate_rule_ids() {
    s3_tests::run(async {
        let body = "<LifecycleConfiguration>\
                <Rule>\
                    <ID>same</ID>\
                    <Filter><Prefix>logs/</Prefix></Filter>\
                    <Status>Enabled</Status>\
                    <Expiration><Days>1</Days></Expiration>\
                </Rule>\
                <Rule>\
                    <ID>same</ID>\
                    <Filter><Prefix>archive/</Prefix></Filter>\
                    <Status>Enabled</Status>\
                    <Expiration><Days>2</Days></Expiration>\
                </Rule>\
            </LifecycleConfiguration>";
        assert_invalid_lifecycle_put_rejected(body, "InvalidArgument").await;
    });
}

#[test]
fn test_put_bucket_lifecycle_rejects_invalid_status() {
    s3_tests::run(async {
        let body = "<LifecycleConfiguration>\
                <Rule>\
                    <Filter><Prefix>logs/</Prefix></Filter>\
                    <Status>enabled</Status>\
                    <Expiration><Days>1</Days></Expiration>\
                </Rule>\
            </LifecycleConfiguration>";
        assert_invalid_lifecycle_put_rejected(body, "MalformedXML").await;
    });
}

#[test]
fn test_put_bucket_lifecycle_rejects_invalid_date() {
    s3_tests::run(async {
        let body = "<LifecycleConfiguration>\
                <Rule>\
                    <Filter><Prefix>logs/</Prefix></Filter>\
                    <Status>Enabled</Status>\
                    <Expiration><Date>20200101</Date></Expiration>\
                </Rule>\
            </LifecycleConfiguration>";
        assert_invalid_lifecycle_put_rejected(body, "MalformedXML").await;
    });
}

#[test]
fn test_put_bucket_lifecycle_rejects_zero_days() {
    s3_tests::run(async {
        let body = "<LifecycleConfiguration>\
                <Rule>\
                    <Filter><Prefix>logs/</Prefix></Filter>\
                    <Status>Enabled</Status>\
                    <Expiration><Days>0</Days></Expiration>\
                </Rule>\
            </LifecycleConfiguration>";
        assert_invalid_lifecycle_put_rejected(body, "InvalidArgument").await;
    });
}

#[test]
fn test_put_bucket_lifecycle_rejects_expired_object_delete_marker_with_days() {
    s3_tests::run(async {
        let body = "<LifecycleConfiguration>\
                <Rule>\
                    <Filter><Prefix>logs/</Prefix></Filter>\
                    <Status>Enabled</Status>\
                    <Expiration>\
                        <Days>1</Days>\
                        <ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker>\
                    </Expiration>\
                </Rule>\
            </LifecycleConfiguration>";
        assert_invalid_lifecycle_put_rejected(body, "MalformedXML").await;
    });
}

#[test]
fn test_put_bucket_lifecycle_rejects_expired_object_delete_marker_with_date() {
    s3_tests::run(async {
        let body = "<LifecycleConfiguration>\
                <Rule>\
                    <Filter><Prefix>logs/</Prefix></Filter>\
                    <Status>Enabled</Status>\
                    <Expiration>\
                        <Date>2099-01-01T00:00:00Z</Date>\
                        <ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker>\
                    </Expiration>\
                </Rule>\
            </LifecycleConfiguration>";
        assert_invalid_lifecycle_put_rejected(body, "MalformedXML").await;
    });
}

#[test]
fn test_put_bucket_lifecycle_rejects_expired_object_delete_marker_with_tag_filter() {
    s3_tests::run(async {
        let body = "<LifecycleConfiguration>\
                <Rule>\
                    <Filter>\
                        <Tag><Key>env</Key><Value>prod</Value></Tag>\
                    </Filter>\
                    <Status>Enabled</Status>\
                    <Expiration>\
                        <ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker>\
                    </Expiration>\
                </Rule>\
            </LifecycleConfiguration>";
        assert_invalid_lifecycle_put_rejected_with_message(
            body,
            "InvalidRequest",
            "ExpiredObjectDeleteMarker cannot be specified with Tags.",
        )
        .await;
    });
}

#[test]
fn test_put_bucket_lifecycle_rejects_abort_incomplete_multipart_with_tag_filter() {
    s3_tests::run(async {
        let body = "<LifecycleConfiguration>\
                <Rule>\
                    <Filter>\
                        <Tag><Key>env</Key><Value>prod</Value></Tag>\
                    </Filter>\
                    <Status>Enabled</Status>\
                    <AbortIncompleteMultipartUpload>\
                        <DaysAfterInitiation>1</DaysAfterInitiation>\
                    </AbortIncompleteMultipartUpload>\
                </Rule>\
            </LifecycleConfiguration>";
        assert_invalid_lifecycle_put_rejected_with_message(
            body,
            "InvalidRequest",
            "AbortIncompleteMultipartUpload cannot be specified with Tags.",
        )
        .await;
    });
}

#[test]
fn test_put_bucket_lifecycle_rejects_abort_incomplete_multipart_with_size_filter() {
    s3_tests::run(async {
        let body = "<LifecycleConfiguration>\
                <Rule>\
                    <Filter>\
                        <ObjectSizeGreaterThan>10</ObjectSizeGreaterThan>\
                    </Filter>\
                    <Status>Enabled</Status>\
                    <AbortIncompleteMultipartUpload>\
                        <DaysAfterInitiation>1</DaysAfterInitiation>\
                    </AbortIncompleteMultipartUpload>\
                </Rule>\
            </LifecycleConfiguration>";
        assert_invalid_lifecycle_put_rejected_with_message(
            body,
            "InvalidRequest",
            "AbortIncompleteMultipartUpload cannot be specified with Object Size.",
        )
        .await;
    });
}

#[test]
fn test_put_bucket_lifecycle_rejects_invalid_object_size_range() {
    s3_tests::run(async {
        let body = "<LifecycleConfiguration>\
                <Rule>\
                    <Filter>\
                        <And>\
                            <ObjectSizeGreaterThan>10</ObjectSizeGreaterThan>\
                            <ObjectSizeLessThan>10</ObjectSizeLessThan>\
                        </And>\
                    </Filter>\
                    <Status>Enabled</Status>\
                    <Expiration><Days>1</Days></Expiration>\
                </Rule>\
            </LifecycleConfiguration>";
        assert_invalid_lifecycle_put_rejected_with_message(
            body,
            "InvalidRequest",
            "'ObjectSizeLessThan' has to be a value greater than 'ObjectSizeGreaterThan'.",
        )
        .await;
    });
}

#[test]
fn test_put_bucket_lifecycle_rejects_newer_noncurrent_versions_without_filter() {
    s3_tests::run(async {
        let body = "<LifecycleConfiguration>\
                <Rule>\
                    <Status>Enabled</Status>\
                    <NoncurrentVersionExpiration>\
                        <NoncurrentDays>1</NoncurrentDays>\
                        <NewerNoncurrentVersions>1</NewerNoncurrentVersions>\
                    </NoncurrentVersionExpiration>\
                </Rule>\
            </LifecycleConfiguration>";
        assert_invalid_lifecycle_put_rejected(body, "MalformedXML").await;
    });
}

#[test]
fn test_delete_bucket_lifecycle_is_idempotent() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        let client = CTX.client();
        create_bucket_in_test_region(&bucket).await;

        client
            .delete_bucket_lifecycle()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        let config = BucketLifecycleConfiguration::builder()
            .rules(
                LifecycleRule::builder()
                    .id("expire-current")
                    .filter(LifecycleRuleFilter::builder().prefix("logs/").build())
                    .status(ExpirationStatus::Enabled)
                    .expiration(LifecycleExpiration::builder().days(30).build())
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();
        put_bucket_lifecycle_with_md5(client, &bucket, config)
            .send()
            .await
            .unwrap();

        client
            .delete_bucket_lifecycle()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        client
            .delete_bucket_lifecycle()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        assert_lifecycle_deleted_eventually(&bucket).await;

        cleanup_bucket(&bucket).await;
    });
}

#[test]
fn test_bucket_lifecycle_crud_round_trip() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        let client = CTX.client();
        create_bucket_in_test_region(&bucket).await;

        let config = BucketLifecycleConfiguration::builder()
            .rules(
                LifecycleRule::builder()
                    .id("expire-current")
                    .filter(LifecycleRuleFilter::builder().prefix("logs/").build())
                    .status(ExpirationStatus::Enabled)
                    .expiration(LifecycleExpiration::builder().days(30).build())
                    .build()
                    .unwrap(),
            )
            .rules(
                LifecycleRule::builder()
                    .id("expire-noncurrent")
                    .filter(LifecycleRuleFilter::builder().prefix("versions/").build())
                    .status(ExpirationStatus::Enabled)
                    .noncurrent_version_expiration(
                        NoncurrentVersionExpiration::builder()
                            .noncurrent_days(7)
                            .build(),
                    )
                    .build()
                    .unwrap(),
            )
            .rules(
                LifecycleRule::builder()
                    .id("expire-marker")
                    .filter(LifecycleRuleFilter::builder().prefix("markers/").build())
                    .status(ExpirationStatus::Enabled)
                    .expiration(
                        LifecycleExpiration::builder()
                            .expired_object_delete_marker(true)
                            .build(),
                    )
                    .build()
                    .unwrap(),
            )
            .rules(
                LifecycleRule::builder()
                    .id("abort-incomplete")
                    .filter(LifecycleRuleFilter::builder().prefix("uploads/").build())
                    .status(ExpirationStatus::Enabled)
                    .abort_incomplete_multipart_upload(
                        AbortIncompleteMultipartUpload::builder()
                            .days_after_initiation(7)
                            .build(),
                    )
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        let put_result = put_bucket_lifecycle_with_md5(client, &bucket, config)
            .send()
            .await;
        put_result.unwrap();

        let get = client
            .get_bucket_lifecycle_configuration()
            .bucket(&bucket)
            .send()
            .await;
        let get = get.unwrap();

        assert_eq!(get.rules().len(), 4);
        let expire_current = &get.rules()[0];
        assert_eq!(expire_current.id(), Some("expire-current"));
        assert_eq!(
            expire_current
                .expiration()
                .and_then(LifecycleExpiration::days),
            Some(30)
        );

        let expire_noncurrent = &get.rules()[1];
        assert_eq!(expire_noncurrent.id(), Some("expire-noncurrent"));
        assert_eq!(
            expire_noncurrent
                .noncurrent_version_expiration()
                .and_then(NoncurrentVersionExpiration::noncurrent_days),
            Some(7)
        );

        let expire_marker = &get.rules()[2];
        assert_eq!(expire_marker.id(), Some("expire-marker"));
        assert_eq!(
            expire_marker
                .expiration()
                .and_then(LifecycleExpiration::expired_object_delete_marker),
            Some(true)
        );

        let abort_incomplete = &get.rules()[3];
        assert_eq!(abort_incomplete.id(), Some("abort-incomplete"));
        assert_eq!(
            abort_incomplete
                .abort_incomplete_multipart_upload()
                .and_then(AbortIncompleteMultipartUpload::days_after_initiation),
            Some(7)
        );

        client
            .delete_bucket_lifecycle()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        assert_lifecycle_deleted_eventually(&bucket).await;

        cleanup_bucket(&bucket).await;
    });
}

#[test]
fn test_bucket_lifecycle_round_trip_preserves_disabled_rule() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        let client = CTX.client();
        create_bucket_in_test_region(&bucket).await;

        let config = BucketLifecycleConfiguration::builder()
            .rules(
                LifecycleRule::builder()
                    .id("enabled-expire")
                    .filter(LifecycleRuleFilter::builder().prefix("enabled/").build())
                    .status(ExpirationStatus::Enabled)
                    .expiration(LifecycleExpiration::builder().days(30).build())
                    .build()
                    .unwrap(),
            )
            .rules(
                LifecycleRule::builder()
                    .id("disabled-abort")
                    .filter(LifecycleRuleFilter::builder().prefix("disabled/").build())
                    .status(ExpirationStatus::Disabled)
                    .abort_incomplete_multipart_upload(
                        AbortIncompleteMultipartUpload::builder()
                            .days_after_initiation(3)
                            .build(),
                    )
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        put_bucket_lifecycle_with_md5(client, &bucket, config)
            .send()
            .await
            .unwrap();

        let get = client
            .get_bucket_lifecycle_configuration()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        assert_eq!(get.rules().len(), 2);
        assert_eq!(get.rules()[0].id(), Some("enabled-expire"));
        assert_eq!(get.rules()[0].status(), &ExpirationStatus::Enabled);
        assert_eq!(
            get.rules()[0]
                .expiration()
                .and_then(LifecycleExpiration::days),
            Some(30)
        );
        assert_eq!(get.rules()[1].id(), Some("disabled-abort"));
        assert_eq!(get.rules()[1].status(), &ExpirationStatus::Disabled);
        assert_eq!(
            get.rules()[1]
                .abort_incomplete_multipart_upload()
                .and_then(AbortIncompleteMultipartUpload::days_after_initiation),
            Some(3)
        );

        cleanup_bucket(&bucket).await;
    });
}

#[test]
fn test_bucket_lifecycle_get_assigns_ids_when_missing() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        let client = CTX.client();
        create_bucket_in_test_region(&bucket).await;

        let config = BucketLifecycleConfiguration::builder()
            .rules(
                LifecycleRule::builder()
                    .filter(LifecycleRuleFilter::builder().prefix("test1/").build())
                    .status(ExpirationStatus::Enabled)
                    .expiration(LifecycleExpiration::builder().days(31).build())
                    .build()
                    .unwrap(),
            )
            .rules(
                LifecycleRule::builder()
                    .filter(LifecycleRuleFilter::builder().prefix("test2/").build())
                    .status(ExpirationStatus::Enabled)
                    .expiration(LifecycleExpiration::builder().days(120).build())
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        put_bucket_lifecycle_with_md5(client, &bucket, config)
            .send()
            .await
            .unwrap();

        let get = client
            .get_bucket_lifecycle_configuration()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        assert_eq!(get.rules().len(), 2);
        for rule in get.rules() {
            match rule.filter().and_then(LifecycleRuleFilter::prefix) {
                Some("test1/") => {
                    assert!(rule.id().is_some_and(|id| !id.is_empty()));
                    assert_eq!(
                        rule.expiration().and_then(LifecycleExpiration::days),
                        Some(31)
                    );
                }
                Some("test2/") => {
                    assert!(rule.id().is_some_and(|id| !id.is_empty()));
                    assert_eq!(
                        rule.expiration().and_then(LifecycleExpiration::days),
                        Some(120)
                    );
                }
                other => panic!("unexpected lifecycle rule prefix: {other:?}"),
            }
        }

        cleanup_bucket(&bucket).await;
    });
}

#[test]
fn test_bucket_lifecycle_round_trip_expiration_date_rule() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        let client = CTX.client();
        create_bucket_in_test_region(&bucket).await;

        let expiration_date = DateTime::from_str("2099-01-01T00:00:00Z", DateTimeFormat::DateTime)
            .expect("valid lifecycle expiration date");
        let config = BucketLifecycleConfiguration::builder()
            .rules(
                LifecycleRule::builder()
                    .id("expire-by-date")
                    .filter(LifecycleRuleFilter::builder().prefix("archive/").build())
                    .status(ExpirationStatus::Enabled)
                    .expiration(LifecycleExpiration::builder().date(expiration_date).build())
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        put_bucket_lifecycle_with_md5(client, &bucket, config)
            .send()
            .await
            .unwrap();

        let get = client
            .get_bucket_lifecycle_configuration()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        assert_eq!(get.rules().len(), 1);
        let rule = &get.rules()[0];
        assert_eq!(rule.id(), Some("expire-by-date"));
        assert_eq!(
            rule.filter().and_then(LifecycleRuleFilter::prefix),
            Some("archive/")
        );
        assert_eq!(
            rule.expiration().and_then(LifecycleExpiration::date),
            Some(&expiration_date)
        );

        cleanup_bucket(&bucket).await;
    });
}

#[test]
fn test_bucket_lifecycle_round_trip_empty_filter_rule() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        let client = CTX.client();
        create_bucket_in_test_region(&bucket).await;

        let config = BucketLifecycleConfiguration::builder()
            .rules(
                LifecycleRule::builder()
                    .id("empty-filter")
                    .filter(LifecycleRuleFilter::builder().build())
                    .status(ExpirationStatus::Enabled)
                    .expiration(LifecycleExpiration::builder().days(7).build())
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        put_bucket_lifecycle_with_md5(client, &bucket, config)
            .send()
            .await
            .unwrap();

        let get = client
            .get_bucket_lifecycle_configuration()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        assert_eq!(get.rules().len(), 1);
        let rule = &get.rules()[0];
        let filter = rule.filter().expect("expected empty filter to round-trip");
        assert_eq!(rule.id(), Some("empty-filter"));
        assert_eq!(
            rule.expiration().and_then(LifecycleExpiration::days),
            Some(7)
        );
        assert_eq!(filter.prefix(), None);
        assert_eq!(filter.tag(), None);
        assert_eq!(filter.object_size_greater_than(), None);
        assert_eq!(filter.object_size_less_than(), None);
        if let Some(and) = filter.and() {
            assert_eq!(and.prefix(), None);
            assert_eq!(and.tags(), &[]);
            assert_eq!(and.object_size_greater_than(), None);
            assert_eq!(and.object_size_less_than(), None);
        }

        cleanup_bucket(&bucket).await;
    });
}

#[test]
fn test_bucket_lifecycle_round_trip_tag_filter_rule() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        let client = CTX.client();
        create_bucket_in_test_region(&bucket).await;

        let config = BucketLifecycleConfiguration::builder()
            .rules(
                LifecycleRule::builder()
                    .id("tag-filter")
                    .filter(
                        LifecycleRuleFilter::builder()
                            .tag(Tag::builder().key("env").value("prod").build().unwrap())
                            .build(),
                    )
                    .status(ExpirationStatus::Enabled)
                    .expiration(LifecycleExpiration::builder().days(14).build())
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        put_bucket_lifecycle_with_md5(client, &bucket, config)
            .send()
            .await
            .unwrap();

        let get = client
            .get_bucket_lifecycle_configuration()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        assert_eq!(get.rules().len(), 1);
        let rule = &get.rules()[0];
        let tag = rule
            .filter()
            .and_then(LifecycleRuleFilter::tag)
            .expect("expected tag filter to round-trip");
        assert_eq!(rule.id(), Some("tag-filter"));
        assert_eq!(
            rule.expiration().and_then(LifecycleExpiration::days),
            Some(14)
        );
        assert_eq!(tag.key(), "env");
        assert_eq!(tag.value(), "prod");

        cleanup_bucket(&bucket).await;
    });
}

#[test]
fn test_bucket_lifecycle_raw_get_returns_canonical_xml() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        create_bucket_in_test_region(&bucket).await;

        let body = br#"
            <LifecycleConfiguration>
                <Rule>
                    <Status>Enabled</Status>
                    <Expiration>
                        <Days>30</Days>
                    </Expiration>
                    <Filter>
                        <And>
                            <Tag>
                                <Key>env</Key>
                                <Value>prod</Value>
                            </Tag>
                            <Prefix>logs/</Prefix>
                        </And>
                    </Filter>
                    <ID>rule-a</ID>
                </Rule>
                <Rule>
                    <AbortIncompleteMultipartUpload>
                        <DaysAfterInitiation>7</DaysAfterInitiation>
                    </AbortIncompleteMultipartUpload>
                    <Status>Disabled</Status>
                    <Filter>
                        <Prefix>uploads/</Prefix>
                    </Filter>
                    <ID>rule-b</ID>
                </Rule>
            </LifecycleConfiguration>
        "#;

        let parsed = s3_types::parse_lifecycle_configuration_xml(body)
            .expect("test lifecycle XML should parse");
        let expected = s3_types::render_lifecycle_configuration_xml(&parsed);

        let url = format!("{}/{}?lifecycle", CTX.endpoint(), bucket);
        let put = send_signed_request("PUT", &url, body, [content_md5_header(body)]);
        assert_eq!(
            put.status, 200,
            "unexpected lifecycle PUT body: {}",
            put.body
        );

        let get = send_signed_request("GET", &url, b"", std::iter::empty::<(String, String)>());
        cleanup_bucket(&bucket).await;

        assert_eq!(
            get.status, 200,
            "unexpected lifecycle GET body: {}",
            get.body
        );
        assert_eq!(get.body, expected);
    });
}

#[test]
fn test_put_bucket_lifecycle_rejects_mixed_filter_and_legacy_prefix() {
    s3_tests::run(async {
        assert_invalid_lifecycle_put_rejected_with_message(
            "<LifecycleConfiguration>\
                <Rule>\
                    <ID>filter-rule</ID>\
                    <Filter><Prefix>logs/</Prefix></Filter>\
                    <Status>Enabled</Status>\
                    <Expiration><Days>30</Days></Expiration>\
                </Rule>\
                <Rule>\
                    <ID>legacy-prefix</ID>\
                    <Prefix>logs/</Prefix>\
                    <Status>Enabled</Status>\
                    <Expiration><Days>30</Days></Expiration>\
                </Rule>\
            </LifecycleConfiguration>",
            "InvalidRequest",
            "Base level prefix cannot be used in Lifecycle V2, prefixes are only supported in the Filter.",
        )
        .await;
    });
}

#[test]
fn test_put_bucket_lifecycle_rejects_prefix_over_1024_bytes() {
    s3_tests::run(async {
        let prefix = "a".repeat(1025);
        let body = format!(
            "<LifecycleConfiguration>\
                <Rule>\
                    <ID>long-prefix</ID>\
                    <Filter><Prefix>{prefix}</Prefix></Filter>\
                    <Status>Enabled</Status>\
                    <Expiration><Days>30</Days></Expiration>\
                </Rule>\
            </LifecycleConfiguration>"
        );

        assert_invalid_lifecycle_put_rejected_with_message(
            &body,
            "InvalidRequest",
            "The maximum size of a prefix is 1024",
        )
        .await;
    });
}

#[test]
fn test_put_bucket_lifecycle_rejects_tag_key_over_128_chars() {
    s3_tests::run(async {
        let key = "k".repeat(129);
        let body = format!(
            "<LifecycleConfiguration>\
                <Rule>\
                    <ID>tag-key-too-long</ID>\
                    <Filter><Tag><Key>{key}</Key><Value>v</Value></Tag></Filter>\
                    <Status>Enabled</Status>\
                    <Expiration><Days>30</Days></Expiration>\
                </Rule>\
            </LifecycleConfiguration>"
        );

        assert_invalid_lifecycle_put_rejected_with_message(
            &body,
            "InvalidRequest",
            "A Tag's Key must be a length between 1 and 128.",
        )
        .await;
    });
}

#[test]
fn test_put_bucket_lifecycle_rejects_tag_value_over_256_chars() {
    s3_tests::run(async {
        let value = "v".repeat(257);
        let body = format!(
            "<LifecycleConfiguration>\
                <Rule>\
                    <ID>tag-value-too-long</ID>\
                    <Filter><Tag><Key>env</Key><Value>{value}</Value></Tag></Filter>\
                    <Status>Enabled</Status>\
                    <Expiration><Days>30</Days></Expiration>\
                </Rule>\
            </LifecycleConfiguration>"
        );

        assert_invalid_lifecycle_put_rejected_with_message(
            &body,
            "InvalidRequest",
            "A Tag's Value must be a length between 0 and 256.",
        )
        .await;
    });
}

#[test]
fn test_put_bucket_lifecycle_accepts_51_filter_tags() {
    s3_tests::run(async {
        let tags = (0..51)
            .map(|idx| format!("<Tag><Key>k{idx}</Key><Value>v{idx}</Value></Tag>"))
            .collect::<String>();
        let body = format!(
            "<LifecycleConfiguration>\
                <Rule>\
                    <ID>too-many-tags</ID>\
                    <Filter><And>{tags}</And></Filter>\
                    <Status>Enabled</Status>\
                    <Expiration><Days>30</Days></Expiration>\
                </Rule>\
            </LifecycleConfiguration>"
        );

        let bucket = unique_bucket();
        let client = CTX.client();
        create_bucket_in_test_region(&bucket).await;

        let url = format!("{}/{}?lifecycle", CTX.endpoint(), bucket);
        let put = send_signed_request(
            "PUT",
            &url,
            body.as_bytes(),
            [content_md5_header(body.as_bytes())],
        );
        assert_eq!(
            put.status, 200,
            "unexpected lifecycle PUT body: {}",
            put.body
        );

        let get = client
            .get_bucket_lifecycle_configuration()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        cleanup_bucket(&bucket).await;

        assert_eq!(get.rules().len(), 1);
        let rule = &get.rules()[0];
        let filter = rule.filter().expect("expected filter");
        let and = filter.and().expect("expected And filter");
        assert_eq!(and.tags().len(), 51);
        assert_eq!(and.tags()[0].key(), "k0");
        assert_eq!(and.tags()[50].key(), "k50");
    });
}

#[test]
fn test_put_object_and_head_object_report_expiration_header_for_prefix_filter() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        let client = CTX.client();
        create_bucket_in_test_region(&bucket).await;

        let config = BucketLifecycleConfiguration::builder()
            .rules(
                LifecycleRule::builder()
                    .id("expire-current")
                    .filter(LifecycleRuleFilter::builder().prefix("logs/").build())
                    .status(ExpirationStatus::Enabled)
                    .expiration(LifecycleExpiration::builder().days(3).build())
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();
        let put_lifecycle = put_bucket_lifecycle_with_md5(client, &bucket, config)
            .send()
            .await;
        put_lifecycle.unwrap();

        put_object_until_expiration_header(&bucket, "logs/match", b"match", None, "expire-current")
            .await;

        let nonmatching = client
            .put_object()
            .bucket(&bucket)
            .key("other/skip")
            .body(ByteStream::from_static(b"skip"))
            .send()
            .await
            .unwrap();
        assert!(nonmatching.expiration().is_none());

        assert_head_object_expiration_header_eventually(&bucket, "logs/match", "expire-current")
            .await;

        assert_get_object_expiration_header_eventually(&bucket, "logs/match", "expire-current")
            .await;

        let _ = client
            .delete_bucket_lifecycle()
            .bucket(&bucket)
            .send()
            .await;
        delete_all_and_bucket(
            client,
            &bucket,
            &["logs/match".to_string(), "other/skip".to_string()],
        )
        .await;
    });
}

#[test]
fn test_put_object_and_head_object_report_expiration_header_for_tag_filter() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        let client = CTX.client();
        create_bucket_in_test_region(&bucket).await;

        let config = BucketLifecycleConfiguration::builder()
            .rules(
                LifecycleRule::builder()
                    .id("expire-tagged")
                    .filter(
                        LifecycleRuleFilter::builder()
                            .tag(Tag::builder().key("env").value("prod").build().unwrap())
                            .build(),
                    )
                    .status(ExpirationStatus::Enabled)
                    .expiration(LifecycleExpiration::builder().days(3).build())
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();
        let put_lifecycle = put_bucket_lifecycle_with_md5(client, &bucket, config)
            .send()
            .await;
        put_lifecycle.unwrap();

        put_object_until_expiration_header(
            &bucket,
            "objects/match",
            b"match",
            Some("env=prod"),
            "expire-tagged",
        )
        .await;

        let nonmatching = client
            .put_object()
            .bucket(&bucket)
            .key("objects/skip")
            .tagging("env=dev")
            .body(ByteStream::from_static(b"skip"))
            .send()
            .await
            .unwrap();
        assert!(nonmatching.expiration().is_none());

        assert_head_object_expiration_header_eventually(&bucket, "objects/match", "expire-tagged")
            .await;

        assert_get_object_expiration_header_eventually(&bucket, "objects/match", "expire-tagged")
            .await;

        let _ = client
            .delete_bucket_lifecycle()
            .bucket(&bucket)
            .send()
            .await;
        delete_all_and_bucket(
            client,
            &bucket,
            &["objects/match".to_string(), "objects/skip".to_string()],
        )
        .await;
    });
}

#[test]
fn test_put_object_and_head_object_report_expiration_header_for_and_filter() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        let client = CTX.client();
        create_bucket_in_test_region(&bucket).await;

        let config = BucketLifecycleConfiguration::builder()
            .rules(
                LifecycleRule::builder()
                    .id("expire-and")
                    .filter(
                        LifecycleRuleFilter::builder()
                            .and(
                                LifecycleRuleAndOperator::builder()
                                    .prefix("logs/")
                                    .tags(Tag::builder().key("env").value("prod").build().unwrap())
                                    .build(),
                            )
                            .build(),
                    )
                    .status(ExpirationStatus::Enabled)
                    .expiration(LifecycleExpiration::builder().days(3).build())
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();
        let put_lifecycle = put_bucket_lifecycle_with_md5(client, &bucket, config)
            .send()
            .await;
        put_lifecycle.unwrap();

        put_object_until_expiration_header(
            &bucket,
            "logs/match",
            b"match",
            Some("env=prod"),
            "expire-and",
        )
        .await;

        let wrong_prefix = client
            .put_object()
            .bucket(&bucket)
            .key("other/skip")
            .tagging("env=prod")
            .body(ByteStream::from_static(b"skip"))
            .send()
            .await
            .unwrap();
        assert!(wrong_prefix.expiration().is_none());

        let wrong_tag = client
            .put_object()
            .bucket(&bucket)
            .key("logs/wrong-tag")
            .tagging("env=dev")
            .body(ByteStream::from_static(b"skip"))
            .send()
            .await
            .unwrap();
        assert!(wrong_tag.expiration().is_none());

        assert_head_object_expiration_header_eventually(&bucket, "logs/match", "expire-and").await;

        assert_get_object_expiration_header_eventually(&bucket, "logs/match", "expire-and").await;

        let _ = client
            .delete_bucket_lifecycle()
            .bucket(&bucket)
            .send()
            .await;
        delete_all_and_bucket(
            client,
            &bucket,
            &[
                "logs/match".to_string(),
                "other/skip".to_string(),
                "logs/wrong-tag".to_string(),
            ],
        )
        .await;
    });
}

#[test]
fn test_put_object_and_head_object_report_expiration_header_for_date_rule() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        let client = CTX.client();
        create_bucket_in_test_region(&bucket).await;

        let expiration_date = DateTime::from_str("2099-01-01T00:00:00Z", DateTimeFormat::DateTime)
            .expect("valid lifecycle expiration date");
        let config = BucketLifecycleConfiguration::builder()
            .rules(
                LifecycleRule::builder()
                    .id("expire-by-date")
                    .filter(LifecycleRuleFilter::builder().prefix("archive/").build())
                    .status(ExpirationStatus::Enabled)
                    .expiration(LifecycleExpiration::builder().date(expiration_date).build())
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();
        put_bucket_lifecycle_with_md5(client, &bucket, config)
            .send()
            .await
            .unwrap();

        put_object_until_expiration_header(
            &bucket,
            "archive/match",
            b"match",
            None,
            "expire-by-date",
        )
        .await;

        let nonmatching = client
            .put_object()
            .bucket(&bucket)
            .key("other/skip")
            .body(ByteStream::from_static(b"skip"))
            .send()
            .await
            .unwrap();
        assert!(nonmatching.expiration().is_none());

        assert_head_object_expiration_header_eventually(&bucket, "archive/match", "expire-by-date")
            .await;

        assert_get_object_expiration_header_eventually(&bucket, "archive/match", "expire-by-date")
            .await;

        let _ = client
            .delete_bucket_lifecycle()
            .bucket(&bucket)
            .send()
            .await;
        delete_all_and_bucket(
            client,
            &bucket,
            &["archive/match".to_string(), "other/skip".to_string()],
        )
        .await;
    });
}

#[test]
fn test_put_object_and_head_object_report_expiration_header_for_object_size_greater_than_filter() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        let client = CTX.client();
        create_bucket_in_test_region(&bucket).await;

        let config = BucketLifecycleConfiguration::builder()
            .rules(
                LifecycleRule::builder()
                    .id("expire-large")
                    .filter(
                        LifecycleRuleFilter::builder()
                            .object_size_greater_than(2_000)
                            .build(),
                    )
                    .status(ExpirationStatus::Enabled)
                    .expiration(LifecycleExpiration::builder().days(3).build())
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();
        put_bucket_lifecycle_with_md5(client, &bucket, config)
            .send()
            .await
            .unwrap();

        put_object_until_expiration_header(
            &bucket,
            "large",
            &vec![b'a'; 3_000],
            None,
            "expire-large",
        )
        .await;

        let nonmatching = client
            .put_object()
            .bucket(&bucket)
            .key("small")
            .body(ByteStream::from(vec![b'b'; 1_000]))
            .send()
            .await
            .unwrap();
        assert!(nonmatching.expiration().is_none());

        assert_head_object_expiration_header_eventually(&bucket, "large", "expire-large").await;

        assert_get_object_expiration_header_eventually(&bucket, "large", "expire-large").await;

        let _ = client
            .delete_bucket_lifecycle()
            .bucket(&bucket)
            .send()
            .await;
        delete_all_and_bucket(client, &bucket, &["large".to_string(), "small".to_string()]).await;
    });
}

#[test]
fn test_put_object_and_head_object_report_expiration_header_for_object_size_less_than_filter() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        let client = CTX.client();
        create_bucket_in_test_region(&bucket).await;

        let config = BucketLifecycleConfiguration::builder()
            .rules(
                LifecycleRule::builder()
                    .id("expire-small")
                    .filter(
                        LifecycleRuleFilter::builder()
                            .object_size_less_than(2_000)
                            .build(),
                    )
                    .status(ExpirationStatus::Enabled)
                    .expiration(LifecycleExpiration::builder().days(3).build())
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();
        put_bucket_lifecycle_with_md5(client, &bucket, config)
            .send()
            .await
            .unwrap();

        put_object_until_expiration_header(
            &bucket,
            "small",
            &vec![b'a'; 1_000],
            None,
            "expire-small",
        )
        .await;

        let nonmatching = client
            .put_object()
            .bucket(&bucket)
            .key("large")
            .body(ByteStream::from(vec![b'b'; 3_000]))
            .send()
            .await
            .unwrap();
        assert!(nonmatching.expiration().is_none());

        assert_head_object_expiration_header_eventually(&bucket, "small", "expire-small").await;

        assert_get_object_expiration_header_eventually(&bucket, "small", "expire-small").await;

        let _ = client
            .delete_bucket_lifecycle()
            .bucket(&bucket)
            .send()
            .await;
        delete_all_and_bucket(client, &bucket, &["small".to_string(), "large".to_string()]).await;
    });
}

#[test]
fn test_get_and_head_object_only_report_expiration_for_current_live_version() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        let client = CTX.client();
        create_bucket_in_test_region(&bucket).await;
        enable_versioning(&bucket).await;

        let config = BucketLifecycleConfiguration::builder()
            .rules(
                LifecycleRule::builder()
                    .id("expire-current")
                    .filter(LifecycleRuleFilter::builder().prefix("logs/").build())
                    .status(ExpirationStatus::Enabled)
                    .expiration(LifecycleExpiration::builder().days(3).build())
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();
        put_bucket_lifecycle_with_md5(client, &bucket, config)
            .send()
            .await
            .unwrap();

        let first = put_object_until_expiration_header(
            &bucket,
            "logs/object",
            b"v1",
            None,
            "expire-current",
        )
        .await;
        let first_version_id = first.version_id().unwrap().to_string();

        let second = put_object_until_expiration_header(
            &bucket,
            "logs/object",
            b"v2",
            None,
            "expire-current",
        )
        .await;
        let second_version_id = second.version_id().unwrap().to_string();

        assert_head_object_expiration_header_eventually(&bucket, "logs/object", "expire-current")
            .await;

        assert_get_object_expiration_header_eventually(&bucket, "logs/object", "expire-current")
            .await;

        let current_head = client
            .head_object()
            .bucket(&bucket)
            .key("logs/object")
            .version_id(&second_version_id)
            .send()
            .await
            .unwrap();
        assert!(current_head.expiration().is_none());

        let current_get = client
            .get_object()
            .bucket(&bucket)
            .key("logs/object")
            .version_id(&second_version_id)
            .send()
            .await
            .unwrap();
        assert!(current_get.expiration().is_none());

        let old_head = client
            .head_object()
            .bucket(&bucket)
            .key("logs/object")
            .version_id(&first_version_id)
            .send()
            .await
            .unwrap();
        assert!(old_head.expiration().is_none());

        let old_get = client
            .get_object()
            .bucket(&bucket)
            .key("logs/object")
            .version_id(&first_version_id)
            .send()
            .await
            .unwrap();
        assert!(old_get.expiration().is_none());

        cleanup_versioned_lifecycle_bucket(&bucket).await;
    });
}

#[test]
fn test_create_multipart_upload_and_list_parts_skip_abort_headers_for_nonmatching_key() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        let client = CTX.client();
        create_bucket_in_test_region(&bucket).await;

        let config = BucketLifecycleConfiguration::builder()
            .rules(
                LifecycleRule::builder()
                    .id("abort-incomplete")
                    .filter(LifecycleRuleFilter::builder().prefix("uploads/").build())
                    .status(ExpirationStatus::Enabled)
                    .abort_incomplete_multipart_upload(
                        AbortIncompleteMultipartUpload::builder()
                            .days_after_initiation(7)
                            .build(),
                    )
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();
        put_bucket_lifecycle_with_md5(client, &bucket, config)
            .send()
            .await
            .unwrap();

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("other/incomplete")
            .send()
            .await
            .unwrap();
        assert!(create.abort_date().is_none());
        assert!(create.abort_rule_id().is_none());
        let upload_id = create.upload_id().unwrap().to_string();

        let list_parts = client
            .list_parts()
            .bucket(&bucket)
            .key("other/incomplete")
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();
        assert!(list_parts.abort_date().is_none());
        assert!(list_parts.abort_rule_id().is_none());

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("other/incomplete")
            .upload_id(upload_id)
            .send()
            .await
            .unwrap();
        cleanup_bucket(&bucket).await;
    });
}

#[test]
fn test_create_multipart_upload_and_list_parts_report_abort_headers() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        let client = CTX.client();
        create_bucket_in_test_region(&bucket).await;

        let config = BucketLifecycleConfiguration::builder()
            .rules(
                LifecycleRule::builder()
                    .id("abort-incomplete")
                    .filter(LifecycleRuleFilter::builder().prefix("uploads/").build())
                    .status(ExpirationStatus::Enabled)
                    .abort_incomplete_multipart_upload(
                        AbortIncompleteMultipartUpload::builder()
                            .days_after_initiation(7)
                            .build(),
                    )
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();
        let put_lifecycle = put_bucket_lifecycle_with_md5(client, &bucket, config)
            .send()
            .await;
        put_lifecycle.unwrap();

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("uploads/incomplete")
            .send()
            .await
            .unwrap();
        assert!(create.abort_date().is_some());
        assert_eq!(create.abort_rule_id(), Some("abort-incomplete"));
        let upload_id = create.upload_id().unwrap().to_string();

        let list_parts = client
            .list_parts()
            .bucket(&bucket)
            .key("uploads/incomplete")
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();
        assert!(list_parts.abort_date().is_some());
        assert_eq!(list_parts.abort_rule_id(), Some("abort-incomplete"));

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("uploads/incomplete")
            .upload_id(upload_id)
            .send()
            .await
            .unwrap();
        cleanup_bucket(&bucket).await;
    });
}

#[test]
fn test_complete_multipart_upload_reports_expiration_header() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        let client = CTX.client();
        create_bucket_in_test_region(&bucket).await;

        let config = BucketLifecycleConfiguration::builder()
            .rules(
                LifecycleRule::builder()
                    .id("expire-complete")
                    .filter(LifecycleRuleFilter::builder().prefix("logs/").build())
                    .status(ExpirationStatus::Enabled)
                    .expiration(LifecycleExpiration::builder().days(7).build())
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();
        put_bucket_lifecycle_with_md5(client, &bucket, config)
            .send()
            .await
            .unwrap();

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("logs/complete")
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let upload_part = client
            .upload_part()
            .bucket(&bucket)
            .key("logs/complete")
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from_static(b"hello world"))
            .send()
            .await
            .unwrap();
        let etag = upload_part.e_tag().unwrap().to_string();

        let complete = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key("logs/complete")
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(CompletedPart::builder().part_number(1).e_tag(etag).build())
                    .build(),
            )
            .send()
            .await
            .unwrap();
        assert_lifecycle_expiration_header(complete.expiration(), "expire-complete");

        let _ = client
            .delete_bucket_lifecycle()
            .bucket(&bucket)
            .send()
            .await;
        delete_all_and_bucket(client, &bucket, &["logs/complete".to_string()]).await;
    });
}
