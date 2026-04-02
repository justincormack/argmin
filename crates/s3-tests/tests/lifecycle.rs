use base64::Engine;
use std::time::{SystemTime, UNIX_EPOCH};

use aws_sdk_s3::primitives::{ByteStream, DateTime, DateTimeFormat};
use aws_sdk_s3::types::{
    AbortIncompleteMultipartUpload, BucketLifecycleConfiguration, BucketLocationConstraint,
    CreateBucketConfiguration, ExpirationStatus, LifecycleExpiration, LifecycleRule,
    LifecycleRuleAndOperator, LifecycleRuleFilter, NoncurrentVersionExpiration, Tag,
};
use ring::hmac;
use s3_tests::{
    delete_all_and_bucket, err_status, put_bucket_lifecycle_with_md5, unique_bucket, CTX,
};

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

fn assert_error_code(body: &str, code: &str) {
    let expected = format!("<Code>{code}</Code>");
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
        assert_invalid_lifecycle_put_rejected(body, "InvalidRequest").await;
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
        assert_invalid_lifecycle_put_rejected(body, "InvalidRequest").await;
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
        assert_invalid_lifecycle_put_rejected(body, "InvalidRequest").await;
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
        assert_invalid_lifecycle_put_rejected(body, "InvalidRequest").await;
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

        let get_deleted = client
            .get_bucket_lifecycle_configuration()
            .bucket(&bucket)
            .send()
            .await;
        assert_eq!(err_status(&get_deleted), 404);
        s3_tests::assert_s3_err_code(&get_deleted, "NoSuchLifecycleConfiguration");

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

        let matching = client
            .put_object()
            .bucket(&bucket)
            .key("logs/match")
            .body(ByteStream::from_static(b"match"))
            .send()
            .await
            .unwrap();
        assert_lifecycle_expiration_header(matching.expiration(), "expire-current");

        let nonmatching = client
            .put_object()
            .bucket(&bucket)
            .key("other/skip")
            .body(ByteStream::from_static(b"skip"))
            .send()
            .await
            .unwrap();
        assert!(nonmatching.expiration().is_none());

        let head = client
            .head_object()
            .bucket(&bucket)
            .key("logs/match")
            .send()
            .await
            .unwrap();
        assert_lifecycle_expiration_header(head.expiration(), "expire-current");

        let get = client
            .get_object()
            .bucket(&bucket)
            .key("logs/match")
            .send()
            .await
            .unwrap();
        assert_lifecycle_expiration_header(get.expiration(), "expire-current");

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

        let matching = client
            .put_object()
            .bucket(&bucket)
            .key("objects/match")
            .tagging("env=prod")
            .body(ByteStream::from_static(b"match"))
            .send()
            .await
            .unwrap();
        assert_lifecycle_expiration_header(matching.expiration(), "expire-tagged");

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

        let head = client
            .head_object()
            .bucket(&bucket)
            .key("objects/match")
            .send()
            .await
            .unwrap();
        assert_lifecycle_expiration_header(head.expiration(), "expire-tagged");

        let get = client
            .get_object()
            .bucket(&bucket)
            .key("objects/match")
            .send()
            .await
            .unwrap();
        assert_lifecycle_expiration_header(get.expiration(), "expire-tagged");

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

        let matching = client
            .put_object()
            .bucket(&bucket)
            .key("logs/match")
            .tagging("env=prod")
            .body(ByteStream::from_static(b"match"))
            .send()
            .await
            .unwrap();
        assert_lifecycle_expiration_header(matching.expiration(), "expire-and");

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

        let head = client
            .head_object()
            .bucket(&bucket)
            .key("logs/match")
            .send()
            .await
            .unwrap();
        assert_lifecycle_expiration_header(head.expiration(), "expire-and");

        let get = client
            .get_object()
            .bucket(&bucket)
            .key("logs/match")
            .send()
            .await
            .unwrap();
        assert_lifecycle_expiration_header(get.expiration(), "expire-and");

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
