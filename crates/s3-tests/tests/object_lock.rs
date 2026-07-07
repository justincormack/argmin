use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::{ByteStream, DateTime, DateTimeFormat};
use aws_sdk_s3::types::{
    AbacStatus, BucketAbacStatus, BucketVersioningStatus, CompletedMultipartUpload, CompletedPart,
    DefaultRetention, Delete, ObjectIdentifier, ObjectLockConfiguration, ObjectLockEnabled,
    ObjectLockLegalHold, ObjectLockLegalHoldStatus, ObjectLockMode, ObjectLockRetention,
    ObjectLockRetentionMode, ObjectLockRule, Tag, Tagging, VersioningConfiguration,
};
use base64::Engine;
use ring::hmac;
use s3_tests::{
    assert_s3_err_code, content_md5_header, err_status, raw_bucket, raw_object, raw_object_query,
    send_signed_request,
    shape::{
        assert_shape, chunked_response_headers, error_response_headers, expected_error, shape,
    },
    unique_bucket, SendRetryingOperationAborted, CTX,
};
use serde_json::json;

// Keep AWS-backed Object Lock tests on short retention windows so cleanup does
// not strand long-lived governed objects if a test fails midway. Compliance
// cases use a small-but-not-tiny window because cleanup cannot bypass
// compliance, while the assertions still need the retention to remain active
// long enough to reach S3 reliably.
const GOVERNANCE_RETENTION_SECS: u64 = 24 * 60 * 60;
const GOVERNANCE_RETENTION_LATER_SECS: u64 = 2 * 24 * 60 * 60;
const COMPLIANCE_RETENTION_SECS: u64 = 3;

fn agent() -> s3_tests::Agent {
    s3_tests::test_agent()
}

fn future_date(seconds_from_now: u64) -> DateTime {
    DateTime::from_secs(now_epoch_secs() + seconds_from_now as i64)
}

fn past_date(seconds_ago: u64) -> DateTime {
    DateTime::from_secs(now_epoch_secs() - seconds_ago as i64)
}

fn now_epoch_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

async fn setup_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    bucket
}

async fn setup_object_lock_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    s3_tests::create_bucket_request(client, &bucket)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();
    bucket
}

fn bucket_resource(bucket: &str) -> String {
    format!("arn:aws:s3:::{bucket}")
}

fn bucket_wildcard_resource(bucket: &str) -> String {
    format!("arn:aws:s3:::{bucket}/*")
}

fn alt_policy_principal() -> serde_json::Value {
    json!({ "AWS": format!("arn:aws:iam::{}:root", CTX.alt_account_id()) })
}

fn owner_policy_principal() -> serde_json::Value {
    json!({ "AWS": format!("arn:aws:iam::{}:root", CTX.account_id()) })
}

fn object_tagging(key: &str, value: &str) -> Tagging {
    Tagging::builder()
        .tag_set(Tag::builder().key(key).value(value).build().unwrap())
        .build()
        .unwrap()
}

async fn enable_bucket_abac_with_security_tag(bucket: &str, value: &str) {
    CTX.client()
        .put_bucket_tagging()
        .bucket(bucket)
        .tagging(object_tagging("security", value))
        .send()
        .await
        .unwrap();
    CTX.client()
        .put_bucket_abac()
        .bucket(bucket)
        .abac_status(
            AbacStatus::builder()
                .status(BucketAbacStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
}

async fn put_bucket_policy_json(bucket: &str, policy: serde_json::Value) {
    CTX.client()
        .put_bucket_policy()
        .bucket(bucket)
        .policy(policy.to_string())
        .send()
        .await
        .unwrap();
}

async fn fresh_alt_client_for_policy_retry() -> aws_sdk_s3::Client {
    if std::env::var("S3_TEST_ENDPOINT").is_ok() {
        let access_key = std::env::var("S3_TEST_ALT_ACCESS_KEY")
            .expect("S3_TEST_ALT_ACCESS_KEY required for external policy retries");
        let secret_key = std::env::var("S3_TEST_ALT_SECRET_KEY")
            .expect("S3_TEST_ALT_SECRET_KEY required for external policy retries");
        s3_tests::build_client_with_ca(
            CTX.endpoint(),
            &access_key,
            &secret_key,
            CTX.region(),
            CTX.tls_ca_pem(),
        )
    } else {
        CTX.alt_client().clone()
    }
}

async fn wait_for_bypass_retention_update_to_succeed(
    bucket: &str,
    key: &str,
    version_id: &str,
    retention: ObjectLockRetention,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);

    loop {
        let client = fresh_alt_client_for_policy_retry().await;
        let result = client
            .put_object_retention()
            .bucket(bucket)
            .key(key)
            .version_id(version_id)
            .retention(retention.clone())
            .bypass_governance_retention(true)
            .send()
            .await;

        if result.is_ok() {
            return;
        }

        assert_eq!(
            err_status(&result),
            403,
            "unexpected retry result: {result:?}"
        );
        assert_s3_err_code(&result, "AccessDenied");

        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for bypass policy to propagate: {result:?}");
        }

        // AWS bucket policy reads can converge before the corresponding
        // data-plane authorization update is visible to object-lock bypass.
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn wait_for_bypass_delete_to_succeed(bucket: &str, key: &str, version_id: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);

    loop {
        let client = fresh_alt_client_for_policy_retry().await;
        let result = client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .version_id(version_id)
            .bypass_governance_retention(true)
            .send()
            .await;

        if result.is_ok() {
            return;
        }

        assert_eq!(
            err_status(&result),
            403,
            "unexpected delete retry result: {result:?}"
        );
        assert_s3_err_code(&result, "AccessDenied");

        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for bypass delete policy to propagate: {result:?}");
        }

        // AWS bucket policy reads can converge before the corresponding
        // data-plane authorization update is visible to object-lock bypass.
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn put_object_bytes(bucket: &str, key: &str, body: &[u8]) -> String {
    CTX.client()
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(body.to_vec()))
        .send()
        .await
        .unwrap()
        .version_id()
        .expect("expected version_id on object lock bucket")
        .to_string()
}

async fn put_object_lock_configuration(
    bucket: &str,
    config: ObjectLockConfiguration,
) -> aws_sdk_s3::operation::put_object_lock_configuration::PutObjectLockConfigurationOutput {
    CTX.client()
        .put_object_lock_configuration()
        .bucket(bucket)
        .object_lock_configuration(config)
        .send()
        .await
        .unwrap()
}

fn bucket_lock_config_days(mode: ObjectLockRetentionMode, days: i32) -> ObjectLockConfiguration {
    ObjectLockConfiguration::builder()
        .object_lock_enabled(ObjectLockEnabled::Enabled)
        .rule(
            ObjectLockRule::builder()
                .default_retention(DefaultRetention::builder().mode(mode).days(days).build())
                .build(),
        )
        .build()
}

fn bucket_lock_config_years(mode: ObjectLockRetentionMode, years: i32) -> ObjectLockConfiguration {
    ObjectLockConfiguration::builder()
        .object_lock_enabled(ObjectLockEnabled::Enabled)
        .rule(
            ObjectLockRule::builder()
                .default_retention(DefaultRetention::builder().mode(mode).years(years).build())
                .build(),
        )
        .build()
}

fn retention(mode: ObjectLockRetentionMode, retain_until_date: DateTime) -> ObjectLockRetention {
    ObjectLockRetention::builder()
        .mode(mode)
        .retain_until_date(retain_until_date)
        .build()
}

fn legal_hold(status: ObjectLockLegalHoldStatus) -> ObjectLockLegalHold {
    ObjectLockLegalHold::builder().status(status).build()
}

fn object_id(key: &str, version_id: &str) -> ObjectIdentifier {
    ObjectIdentifier::builder()
        .key(key)
        .version_id(version_id)
        .build()
        .unwrap()
}

async fn delete_version_with_bypass(bucket: &str, key: &str, version_id: &str) {
    CTX.client()
        .delete_object()
        .bucket(bucket)
        .key(key)
        .version_id(version_id)
        .bypass_governance_retention(true)
        .send_retrying_operation_aborted("delete object-lock version with bypass")
        .await
        .unwrap();
}

async fn cleanup_plain_bucket(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = s3_tests::delete_object_retrying_operation_aborted(client, bucket, key).await;
    }
    s3_tests::delete_bucket_retrying_operation_aborted(client, bucket).await;
}

async fn cleanup_object_lock_bucket(bucket: &str) {
    let client = CTX.client();

    'retry: loop {
        let resp = client
            .list_object_versions()
            .bucket(bucket)
            .send_retrying_operation_aborted("list object-lock versions during cleanup")
            .await
            .unwrap();

        if resp.versions().is_empty() && resp.delete_markers().is_empty() {
            match client
                .delete_bucket()
                .bucket(bucket)
                .send_retrying_operation_aborted("delete object-lock bucket during cleanup")
                .await
            {
                Ok(_) => return,
                Err(err) => {
                    let raw = format!("{err:?}");
                    if raw.contains("NoSuchBucket") {
                        return;
                    }
                    if raw.contains("BucketNotEmpty") || raw.contains("OperationAborted") {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        continue;
                    }
                    panic!("delete_bucket failed unexpectedly: {raw}");
                }
            }
        }

        for marker in resp.delete_markers() {
            client
                .delete_object()
                .bucket(bucket)
                .key(marker.key().unwrap())
                .version_id(marker.version_id().unwrap())
                .send()
                .await
                .unwrap();
        }

        for version in resp.versions() {
            let key = version.key().unwrap();
            let version_id = version.version_id().unwrap();
            let head = client
                .head_object()
                .bucket(bucket)
                .key(key)
                .version_id(version_id)
                .send()
                .await
                .unwrap();

            if head.object_lock_legal_hold_status() == Some(&ObjectLockLegalHoldStatus::On) {
                client
                    .put_object_legal_hold()
                    .bucket(bucket)
                    .key(key)
                    .version_id(version_id)
                    .legal_hold(legal_hold(ObjectLockLegalHoldStatus::Off))
                    .send()
                    .await
                    .unwrap();
            }

            let delete = client
                .delete_object()
                .bucket(bucket)
                .key(key)
                .version_id(version_id)
                .bypass_governance_retention(true)
                .send()
                .await;

            if delete.is_err() && err_status(&delete) == 403 {
                if let Some(retain_until) = head.object_lock_retain_until_date() {
                    let wait_secs =
                        (retain_until.as_secs_f64().ceil() as i64 - now_epoch_secs() + 1).max(1);
                    tokio::time::sleep(Duration::from_secs(wait_secs as u64)).await;
                    continue 'retry;
                }
            }

            delete.unwrap();
        }
    }
}

fn assert_xml_error_code(body: &str, code: &str) {
    let expected = format!("<Code>{code}</Code>");
    assert!(
        body.contains(&expected),
        "expected {expected} in body: {body}",
    );
}

fn sdk_error_status_and_code<T, E>(
    result: Result<T, aws_sdk_s3::error::SdkError<E>>,
) -> (u16, Option<String>)
where
    E: std::fmt::Debug + ProvideErrorMetadata,
{
    match result {
        Ok(_) => panic!("expected error, got Ok"),
        Err(err) => {
            let status = err
                .raw_response()
                .map(|response| response.status().as_u16())
                .unwrap_or_else(|| panic!("error has no raw HTTP response: {err:?}"));
            let code = err
                .as_service_error()
                .and_then(ProvideErrorMetadata::code)
                .map(str::to_owned);
            (status, code)
        }
    }
}

fn md5_b64(data: &[u8]) -> String {
    use md5_legacy::Digest;

    let digest = md5_legacy::Md5::digest(data);
    base64::engine::general_purpose::STANDARD.encode(&digest[..])
}

fn sha256_hex(data: &[u8]) -> String {
    let digest = ring::digest::digest(&ring::digest::SHA256, data);
    digest.as_ref().iter().map(|b| format!("{b:02x}")).collect()
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let key = hmac::Key::new(hmac::HMAC_SHA256, key);
    hmac::sign(&key, data).as_ref().to_vec()
}

fn format_amz_date(epoch_secs: u64) -> String {
    let days = epoch_secs / 86_400;
    let seconds_of_day = epoch_secs % 86_400;
    let (year, month, day) = days_to_date(days as i64);
    format!(
        "{year:04}{month:02}{day:02}T{:02}{:02}{:02}Z",
        seconds_of_day / 3600,
        (seconds_of_day % 3600) / 60,
        seconds_of_day % 60
    )
}

fn days_to_date(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

fn normalize_query(raw: &str) -> String {
    if raw.is_empty() {
        return String::new();
    }

    let mut pairs: Vec<(String, String)> = raw
        .split('&')
        .filter(|s| !s.is_empty())
        .map(|pair| {
            let mut parts = pair.splitn(2, '=');
            let key = parts.next().unwrap_or_default().to_string();
            let value = parts.next().unwrap_or_default().to_string();
            (key, value)
        })
        .collect();
    pairs.sort();
    pairs
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

struct SigV4Headers {
    authorization: String,
    amz_date: String,
    payload_hash: String,
}

fn sign_request(
    method: &str,
    url_str: &str,
    body: &[u8],
    extra_headers: &[(&str, &str)],
) -> SigV4Headers {
    let parsed = url::Url::parse(url_str).expect("parse URL");
    let path = parsed.path();
    let query = normalize_query(parsed.query().unwrap_or_default());

    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let amz_date = format_amz_date(secs);
    let date_stamp = &amz_date[..8];

    let access_key = CTX.access_key();
    let secret_key = CTX.secret_key();
    let region = CTX.region();
    let service = "s3";

    let host = parsed
        .host_str()
        .map(|host| {
            if let Some(port) = parsed.port() {
                format!("{host}:{port}")
            } else {
                host.to_string()
            }
        })
        .unwrap();

    let payload_hash = sha256_hex(body);

    let mut header_map: Vec<(String, String)> = vec![
        ("host".to_string(), host),
        ("x-amz-content-sha256".to_string(), payload_hash.clone()),
        ("x-amz-date".to_string(), amz_date.clone()),
    ];
    for &(name, value) in extra_headers {
        header_map.push((name.to_lowercase(), value.to_string()));
    }
    header_map.sort_by(|a, b| a.0.cmp(&b.0));

    let signed_headers = header_map
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical_headers: String = header_map
        .iter()
        .map(|(name, value)| format!("{name}:{value}\n"))
        .collect();
    let canonical_request =
        format!("{method}\n{path}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}");

    let scope = format!("{date_stamp}/{region}/{service}/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );

    let k_date = hmac_sha256(
        format!("AWS4{secret_key}").as_bytes(),
        date_stamp.as_bytes(),
    );
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    let signature: String = hmac_sha256(&k_signing, string_to_sign.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();

    SigV4Headers {
        authorization: format!(
            "AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
        ),
        amz_date,
        payload_hash,
    }
}

#[test]
fn test_bucket_object_lock_configuration_raw_get_returns_canonical_xml() {
    s3_tests::run(async {
        let bucket = setup_object_lock_bucket().await;

        let body = br#"
            <ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <ObjectLockEnabled>Enabled</ObjectLockEnabled>
                <Rule>
                    <DefaultRetention>
                        <Mode>GOVERNANCE</Mode>
                        <Days>1</Days>
                    </DefaultRetention>
                </Rule>
            </ObjectLockConfiguration>
        "#;

        let parsed =
            server_http::http::xml::parse_bucket_object_lock_configuration_xml(body).unwrap();
        let expected = server_http::http::xml::get_bucket_object_lock_configuration_xml(
            s3_types::BucketObjectLockConfig {
                enabled: parsed.object_lock_enabled.unwrap_or(false),
                default_retention: parsed.default_retention,
            },
        );

        let url = format!("{}/{}?object-lock", CTX.endpoint(), bucket);
        let put = send_signed_request("PUT", &url, body, [content_md5_header(body)]);
        assert_eq!(put.status, 200, "unexpected body: {}", put.body);

        let get = send_signed_request("GET", &url, b"", std::iter::empty::<(String, String)>());

        cleanup_object_lock_bucket(&bucket).await;

        assert_eq!(get.status, 200, "unexpected body: {}", get.body);
        assert_eq!(get.body, expected);
    });
}

fn signed_put_xml(url: &str, body: &[u8]) -> (u16, String) {
    let content_md5 = md5_b64(body);
    let extra_headers = [
        ("Content-MD5", content_md5.as_str()),
        ("Content-Type", "application/xml"),
    ];
    let sig = sign_request("PUT", url, body, &extra_headers);

    let mut response = agent()
        .put(url)
        .header("Authorization", &sig.authorization)
        .header("x-amz-date", &sig.amz_date)
        .header("x-amz-content-sha256", &sig.payload_hash)
        .header("Content-MD5", &content_md5)
        .header("Content-Type", "application/xml")
        .send(body)
        .expect("transport error");

    let status = response.status().as_u16();
    let body = response.body_mut().read_to_string().unwrap_or_default();
    (status, body)
}

fn signed_head_header(url: &str, header_name: &str) -> (u16, Option<String>) {
    let sig = sign_request("HEAD", url, b"", &[]);

    let response = agent()
        .head(url)
        .header("Authorization", &sig.authorization)
        .header("x-amz-date", &sig.amz_date)
        .header("x-amz-content-sha256", &sig.payload_hash)
        .call()
        .expect("transport error");

    let status = response.status().as_u16();
    let value = response
        .headers()
        .get(header_name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    (status, value)
}

fn bucket_lock_url(bucket: &str) -> String {
    format!("{}/{}?object-lock", CTX.endpoint(), bucket)
}

fn object_retention_url(bucket: &str, key: &str) -> String {
    format!("{}/{}/{}?retention", CTX.endpoint(), bucket, key)
}

fn object_legal_hold_url(bucket: &str, key: &str) -> String {
    format!("{}/{}/{}?legal-hold", CTX.endpoint(), bucket, key)
}

fn object_head_url(bucket: &str, key: &str, version_id: &str) -> String {
    let encoded_version_id: String =
        url::form_urlencoded::byte_serialize(version_id.as_bytes()).collect();
    format!(
        "{}/{}/{}?versionId={encoded_version_id}",
        CTX.endpoint(),
        bucket,
        key
    )
}

fn object_lock_configuration_xml(
    status: &str,
    mode: &str,
    days: Option<i32>,
    years: Option<i32>,
) -> String {
    let mut period = String::new();
    if let Some(days) = days {
        period.push_str(&format!("<Days>{days}</Days>"));
    }
    if let Some(years) = years {
        period.push_str(&format!("<Years>{years}</Years>"));
    }
    format!(
        "<ObjectLockConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
            <ObjectLockEnabled>{status}</ObjectLockEnabled>\
            <Rule><DefaultRetention><Mode>{mode}</Mode>{period}</DefaultRetention></Rule>\
        </ObjectLockConfiguration>"
    )
}

fn object_retention_xml(mode: &str, retain_until_date: &str) -> String {
    format!(
        "<ObjectLockRetention xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
            <Mode>{mode}</Mode>\
            <RetainUntilDate>{retain_until_date}</RetainUntilDate>\
        </ObjectLockRetention>"
    )
}

fn legal_hold_xml(status: &str) -> String {
    format!(
        "<LegalHold xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
            <Status>{status}</Status>\
        </LegalHold>"
    )
}

fn governance_retain_until() -> DateTime {
    future_date(GOVERNANCE_RETENTION_SECS)
}

fn governance_retain_until_later() -> DateTime {
    future_date(GOVERNANCE_RETENTION_LATER_SECS)
}

fn assert_retention_within_window(
    response: &aws_sdk_s3::operation::get_object_retention::GetObjectRetentionOutput,
    mode: ObjectLockRetentionMode,
    min_retain_until: i64,
    max_retain_until: i64,
) {
    let retention = response.retention().expect("missing retention");
    assert_eq!(retention.mode(), Some(&mode));
    let retain_until = retention
        .retain_until_date()
        .expect("missing retain-until date")
        .secs();
    assert!(
        (min_retain_until..=max_retain_until).contains(&retain_until),
        "retain_until={retain_until} outside expected window [{min_retain_until}, {max_retain_until}]",
    );
}

#[test]
fn test_object_lock_put_obj_lock() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;

        put_object_lock_configuration(
            &bucket,
            bucket_lock_config_days(ObjectLockRetentionMode::Governance, 1),
        )
        .await;
        put_object_lock_configuration(
            &bucket,
            bucket_lock_config_years(ObjectLockRetentionMode::Compliance, 1),
        )
        .await;

        let versioning = client
            .get_bucket_versioning()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(versioning.status(), Some(&BucketVersioningStatus::Enabled));

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_put_obj_lock_invalid_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let config = bucket_lock_config_days(ObjectLockRetentionMode::Governance, 1);

        let result = client
            .put_object_lock_configuration()
            .bucket(&bucket)
            .object_lock_configuration(config)
            .send()
            .await;
        assert_eq!(err_status(&result), 409);
        assert_s3_err_code(&result, "InvalidBucketState");

        cleanup_plain_bucket(&bucket, &[]).await;
    });
}

#[test]
fn test_object_lock_put_obj_lock_enable_after_create() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let config = bucket_lock_config_days(ObjectLockRetentionMode::Governance, 1);

        let result = client
            .put_object_lock_configuration()
            .bucket(&bucket)
            .object_lock_configuration(config.clone())
            .send()
            .await;
        assert_eq!(err_status(&result), 409);
        assert_s3_err_code(&result, "InvalidBucketState");

        client
            .put_bucket_versioning()
            .bucket(&bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Suspended)
                    .build(),
            )
            .send()
            .await
            .unwrap();
        let result = client
            .put_object_lock_configuration()
            .bucket(&bucket)
            .object_lock_configuration(config.clone())
            .send()
            .await;
        assert_eq!(err_status(&result), 409);
        assert_s3_err_code(&result, "InvalidBucketState");

        client
            .put_bucket_versioning()
            .bucket(&bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send()
            .await
            .unwrap();
        client
            .put_object_lock_configuration()
            .bucket(&bucket)
            .object_lock_configuration(config)
            .send()
            .await
            .unwrap();

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_put_obj_lock_with_days_and_years() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;

        let config = ObjectLockConfiguration::builder()
            .object_lock_enabled(ObjectLockEnabled::Enabled)
            .rule(
                ObjectLockRule::builder()
                    .default_retention(
                        DefaultRetention::builder()
                            .mode(ObjectLockRetentionMode::Governance)
                            .days(1)
                            .years(1)
                            .build(),
                    )
                    .build(),
            )
            .build();

        let result = client
            .put_object_lock_configuration()
            .bucket(&bucket)
            .object_lock_configuration(config)
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "MalformedXML");

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_put_obj_lock_invalid_days() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;

        let result = client
            .put_object_lock_configuration()
            .bucket(&bucket)
            .object_lock_configuration(bucket_lock_config_days(
                ObjectLockRetentionMode::Governance,
                0,
            ))
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidArgument");

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_put_obj_lock_invalid_years() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;

        let result = client
            .put_object_lock_configuration()
            .bucket(&bucket)
            .object_lock_configuration(bucket_lock_config_years(
                ObjectLockRetentionMode::Governance,
                -1,
            ))
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidArgument");

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_put_obj_lock_invalid_mode() {
    s3_tests::run(async {
        let bucket = setup_object_lock_bucket().await;
        let url = bucket_lock_url(&bucket);

        let body = object_lock_configuration_xml("Enabled", "abc", None, Some(1));
        let (status, response_body) = signed_put_xml(&url, body.as_bytes());
        assert_eq!(status, 400, "body: {response_body}");
        assert_xml_error_code(&response_body, "MalformedXML");

        let body = object_lock_configuration_xml("Enabled", "governance", None, Some(1));
        let (status, response_body) = signed_put_xml(&url, body.as_bytes());
        assert_eq!(status, 400, "body: {response_body}");
        assert_xml_error_code(&response_body, "MalformedXML");

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_put_obj_lock_invalid_status() {
    s3_tests::run(async {
        let bucket = setup_object_lock_bucket().await;
        let url = bucket_lock_url(&bucket);
        let body = object_lock_configuration_xml("Disabled", "GOVERNANCE", None, Some(1));

        let (status, response_body) = signed_put_xml(&url, body.as_bytes());
        assert_eq!(status, 400, "body: {response_body}");
        assert_xml_error_code(&response_body, "MalformedXML");

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_suspend_versioning() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;

        let result = client
            .put_bucket_versioning()
            .bucket(&bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Suspended)
                    .build(),
            )
            .send()
            .await;
        assert_eq!(err_status(&result), 409);
        assert_s3_err_code(&result, "InvalidBucketState");

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_get_obj_lock() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let config = bucket_lock_config_days(ObjectLockRetentionMode::Governance, 1);

        put_object_lock_configuration(&bucket, config.clone()).await;
        let response = client
            .get_object_lock_configuration()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(response.object_lock_configuration(), Some(&config));

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_bucket_policy_get_obj_lock() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = setup_object_lock_bucket().await;
        let config = bucket_lock_config_days(ObjectLockRetentionMode::Governance, 1);

        put_object_lock_configuration(&bucket, config.clone()).await;
        put_bucket_policy_json(
            &bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": alt_policy_principal(),
                    "Action": "s3:GetBucketObjectLockConfiguration",
                    "Resource": bucket_resource(&bucket),
                }],
            }),
        )
        .await;
        client
            .get_bucket_policy()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        let response = alt_client
            .get_object_lock_configuration()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(response.object_lock_configuration(), Some(&config));

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_get_obj_lock_invalid_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let result = client
            .get_object_lock_configuration()
            .bucket(&bucket)
            .send()
            .await;
        assert_eq!(err_status(&result), 404);
        assert_s3_err_code(&result, "ObjectLockConfigurationNotFoundError");

        cleanup_plain_bucket(&bucket, &[]).await;
    });
}

#[test]
fn test_object_lock_put_obj_retention() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        put_object_bytes(&bucket, key, b"abc").await;
        let retain_until = governance_retain_until();
        let retention = retention(ObjectLockRetentionMode::Governance, retain_until);

        client
            .put_object_retention()
            .bucket(&bucket)
            .key(key)
            .retention(retention.clone())
            .send()
            .await
            .unwrap();
        let response = client
            .get_object_retention()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(response.retention(), Some(&retention));

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_bucket_policy_put_get_obj_retention() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        put_object_bytes(&bucket, key, b"abc").await;
        let retain_until = governance_retain_until();
        let retention = retention(ObjectLockRetentionMode::Governance, retain_until);

        put_bucket_policy_json(
            &bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": alt_policy_principal(),
                    "Action": ["s3:PutObjectRetention", "s3:GetObjectRetention"],
                    "Resource": bucket_wildcard_resource(&bucket),
                }],
            }),
        )
        .await;
        client
            .get_bucket_policy()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        alt_client
            .put_object_retention()
            .bucket(&bucket)
            .key(key)
            .retention(retention.clone())
            .send()
            .await
            .unwrap();
        let response = alt_client
            .get_object_retention()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(response.retention(), Some(&retention));

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_bucket_policy_get_obj_retention_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let key = "file1";
        let retain_until = governance_retain_until();
        let retention = retention(ObjectLockRetentionMode::Governance, retain_until);

        let allowed_bucket = setup_object_lock_bucket().await;
        put_object_bytes(&allowed_bucket, key, b"abc").await;
        client
            .put_object_retention()
            .bucket(&allowed_bucket)
            .key(key)
            .retention(retention.clone())
            .send()
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(&allowed_bucket, "allow").await;
        put_bucket_policy_json(
            &allowed_bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": alt_policy_principal(),
                    "Action": "s3:GetObjectRetention",
                    "Resource": bucket_wildcard_resource(&allowed_bucket),
                    "Condition": {
                        "StringEquals": {
                            "s3:BucketTag/security": "allow"
                        }
                    }
                }],
            }),
        )
        .await;

        let denied_bucket = setup_object_lock_bucket().await;
        put_object_bytes(&denied_bucket, key, b"abc").await;
        client
            .put_object_retention()
            .bucket(&denied_bucket)
            .key(key)
            .retention(retention.clone())
            .send()
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(&denied_bucket, "deny").await;
        put_bucket_policy_json(
            &denied_bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": alt_policy_principal(),
                    "Action": "s3:GetObjectRetention",
                    "Resource": bucket_wildcard_resource(&denied_bucket),
                    "Condition": {
                        "StringEquals": {
                            "s3:BucketTag/security": "allow"
                        }
                    }
                }],
            }),
        )
        .await;

        let response = alt_client
            .get_object_retention()
            .bucket(&allowed_bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(response.retention(), Some(&retention));

        let denied = alt_client
            .get_object_retention()
            .bucket(&denied_bucket)
            .key(key)
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        cleanup_object_lock_bucket(&allowed_bucket).await;
        cleanup_object_lock_bucket(&denied_bucket).await;
    });
}

#[test]
fn test_object_lock_bucket_policy_existing_tag_condition_is_rejected_for_get_retention() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        put_object_bytes(&bucket, key, b"abc").await;
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .tagging(object_tagging("security", "allow"))
            .send()
            .await
            .unwrap();

        let result = client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:GetObjectRetention",
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:ExistingObjectTag/security": "allow"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "MalformedPolicy");

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_bucket_policy_bypass_governance_retention_requires_explicit_allow() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        let version_id = put_object_bytes(&bucket, key, b"abc").await;

        client
            .put_object_retention()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .retention(retention(
                ObjectLockRetentionMode::Governance,
                governance_retain_until_later(),
            ))
            .send()
            .await
            .unwrap();

        put_bucket_policy_json(
            &bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": alt_policy_principal(),
                    "Action": "s3:PutObjectRetention",
                    "Resource": bucket_wildcard_resource(&bucket),
                }],
            }),
        )
        .await;
        client
            .get_bucket_policy()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        let denied_without_bypass_action = alt_client
            .put_object_retention()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .retention(retention(
                ObjectLockRetentionMode::Governance,
                governance_retain_until(),
            ))
            .bypass_governance_retention(true)
            .send()
            .await;
        assert_eq!(err_status(&denied_without_bypass_action), 403);
        assert_s3_err_code(&denied_without_bypass_action, "AccessDenied");

        put_bucket_policy_json(
            &bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": alt_policy_principal(),
                    "Action": ["s3:PutObjectRetention", "s3:BypassGovernanceRetention"],
                    "Resource": bucket_wildcard_resource(&bucket),
                }],
            }),
        )
        .await;
        client
            .get_bucket_policy()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        wait_for_bypass_retention_update_to_succeed(
            &bucket,
            key,
            &version_id,
            retention(
                ObjectLockRetentionMode::Governance,
                governance_retain_until(),
            ),
        )
        .await;

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_bucket_policy_bypass_governance_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let key = "file1";

        let allowed_bucket = setup_object_lock_bucket().await;
        let allowed_version_id = put_object_bytes(&allowed_bucket, key, b"abc").await;
        client
            .put_object_retention()
            .bucket(&allowed_bucket)
            .key(key)
            .version_id(&allowed_version_id)
            .retention(retention(
                ObjectLockRetentionMode::Governance,
                governance_retain_until_later(),
            ))
            .send()
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(&allowed_bucket, "allow").await;
        put_bucket_policy_json(
            &allowed_bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": alt_policy_principal(),
                    "Action": ["s3:PutObjectRetention", "s3:BypassGovernanceRetention"],
                    "Resource": bucket_wildcard_resource(&allowed_bucket),
                    "Condition": {
                        "StringEquals": {
                            "s3:BucketTag/security": "allow"
                        }
                    }
                }],
            }),
        )
        .await;

        let denied_bucket = setup_object_lock_bucket().await;
        let denied_version_id = put_object_bytes(&denied_bucket, key, b"abc").await;
        client
            .put_object_retention()
            .bucket(&denied_bucket)
            .key(key)
            .version_id(&denied_version_id)
            .retention(retention(
                ObjectLockRetentionMode::Governance,
                governance_retain_until_later(),
            ))
            .send()
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(&denied_bucket, "deny").await;
        put_bucket_policy_json(
            &denied_bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": alt_policy_principal(),
                    "Action": ["s3:PutObjectRetention", "s3:BypassGovernanceRetention"],
                    "Resource": bucket_wildcard_resource(&denied_bucket),
                    "Condition": {
                        "StringEquals": {
                            "s3:BucketTag/security": "allow"
                        }
                    }
                }],
            }),
        )
        .await;

        wait_for_bypass_retention_update_to_succeed(
            &allowed_bucket,
            key,
            &allowed_version_id,
            retention(
                ObjectLockRetentionMode::Governance,
                governance_retain_until(),
            ),
        )
        .await;

        let denied = alt_client
            .put_object_retention()
            .bucket(&denied_bucket)
            .key(key)
            .version_id(&denied_version_id)
            .retention(retention(
                ObjectLockRetentionMode::Governance,
                governance_retain_until(),
            ))
            .bypass_governance_retention(true)
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        cleanup_object_lock_bucket(&allowed_bucket).await;
        cleanup_object_lock_bucket(&denied_bucket).await;
    });
}

#[test]
fn test_object_lock_bucket_policy_existing_tag_condition_is_rejected_for_put_retention() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        put_object_bytes(&bucket, key, b"abc").await;
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .tagging(object_tagging("security", "allow"))
            .send()
            .await
            .unwrap();

        let result = client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:PutObjectRetention",
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:ExistingObjectTag/security": "allow"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "MalformedPolicy");

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_bucket_policy_existing_tag_condition_is_rejected_for_bypass_governance() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        put_object_bytes(&bucket, key, b"abc").await;
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .tagging(object_tagging("security", "allow"))
            .send()
            .await
            .unwrap();

        let result = client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:BypassGovernanceRetention",
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:ExistingObjectTag/security": "allow"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "MalformedPolicy");

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_bucket_policy_existing_tag_condition_is_rejected_for_put_legal_hold() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        put_object_bytes(&bucket, key, b"abc").await;
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .tagging(object_tagging("security", "allow"))
            .send()
            .await
            .unwrap();

        let result = client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:PutObjectLegalHold",
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:ExistingObjectTag/security": "allow"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "MalformedPolicy");

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_bucket_policy_request_object_tag_condition_is_rejected_for_put_retention() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        put_object_bytes(&bucket, key, b"abc").await;

        let result = client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:PutObjectRetention",
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:RequestObjectTag/security": "allow"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "MalformedPolicy");

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_bucket_policy_request_object_tag_condition_is_rejected_for_put_legal_hold() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        put_object_bytes(&bucket, key, b"abc").await;

        let result = client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:PutObjectLegalHold",
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:RequestObjectTag/security": "allow"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "MalformedPolicy");

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_bucket_policy_explicit_deny_blocks_owner_bypass_retention() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        let version_id = put_object_bytes(&bucket, key, b"abc").await;

        client
            .put_object_retention()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .retention(retention(
                ObjectLockRetentionMode::Governance,
                governance_retain_until_later(),
            ))
            .send()
            .await
            .unwrap();

        put_bucket_policy_json(
            &bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Deny",
                    "Principal": owner_policy_principal(),
                    "Action": "s3:BypassGovernanceRetention",
                    "Resource": bucket_wildcard_resource(&bucket),
                }],
            }),
        )
        .await;
        client
            .get_bucket_policy()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        let denied = client
            .put_object_retention()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .retention(retention(
                ObjectLockRetentionMode::Governance,
                governance_retain_until(),
            ))
            .bypass_governance_retention(true)
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        client
            .delete_bucket_policy()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_bucket_policy_explicit_deny_blocks_owner_bypass_delete() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        let version_id = put_object_bytes(&bucket, key, b"abc").await;

        client
            .put_object_retention()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .retention(retention(
                ObjectLockRetentionMode::Governance,
                governance_retain_until_later(),
            ))
            .send()
            .await
            .unwrap();

        put_bucket_policy_json(
            &bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Deny",
                    "Principal": owner_policy_principal(),
                    "Action": "s3:BypassGovernanceRetention",
                    "Resource": bucket_wildcard_resource(&bucket),
                }],
            }),
        )
        .await;
        client
            .get_bucket_policy()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        let denied = client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .bypass_governance_retention(true)
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        client
            .delete_bucket_policy()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_bucket_policy_bypass_governance_delete_requires_explicit_allow() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        let version_id = put_object_bytes(&bucket, key, b"abc").await;

        client
            .put_object_retention()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .retention(retention(
                ObjectLockRetentionMode::Governance,
                governance_retain_until_later(),
            ))
            .send()
            .await
            .unwrap();

        let denied_without_delete_version_allow = alt_client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .bypass_governance_retention(true)
            .send()
            .await;
        assert_eq!(err_status(&denied_without_delete_version_allow), 403);
        assert_s3_err_code(&denied_without_delete_version_allow, "AccessDenied");

        put_bucket_policy_json(
            &bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": alt_policy_principal(),
                    "Action": "s3:DeleteObjectVersion",
                    "Resource": bucket_wildcard_resource(&bucket),
                }],
            }),
        )
        .await;
        client
            .get_bucket_policy()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        let denied_without_bypass_allow = alt_client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .bypass_governance_retention(true)
            .send()
            .await;
        assert_eq!(err_status(&denied_without_bypass_allow), 403);
        assert_s3_err_code(&denied_without_bypass_allow, "AccessDenied");

        put_bucket_policy_json(
            &bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": alt_policy_principal(),
                    "Action": ["s3:DeleteObjectVersion", "s3:BypassGovernanceRetention"],
                    "Resource": bucket_wildcard_resource(&bucket),
                }],
            }),
        )
        .await;
        client
            .get_bucket_policy()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        wait_for_bypass_delete_to_succeed(&bucket, key, &version_id).await;

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_missing_version_delete_with_bypass_header_requires_bypass_allow() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        let missing_version_id = put_object_bytes(&bucket, key, b"abc").await;
        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&missing_version_id)
            .send()
            .await
            .unwrap();

        put_bucket_policy_json(
            &bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": alt_policy_principal(),
                    "Action": "s3:DeleteObjectVersion",
                    "Resource": bucket_wildcard_resource(&bucket),
                }],
            }),
        )
        .await;
        client
            .get_bucket_policy()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        let missing_without_bypass = alt_client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&missing_version_id)
            .send()
            .await;
        missing_without_bypass.unwrap();

        let missing_with_bypass = alt_client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&missing_version_id)
            .bypass_governance_retention(true)
            .send()
            .await;
        assert_eq!(err_status(&missing_with_bypass), 403);
        assert_s3_err_code(&missing_with_bypass, "AccessDenied");

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_non_version_delete_with_bypass_header_does_not_require_bypass_allow() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        let version_id = put_object_bytes(&bucket, key, b"abc").await;

        client
            .put_object_retention()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .retention(retention(
                ObjectLockRetentionMode::Governance,
                governance_retain_until_later(),
            ))
            .send()
            .await
            .unwrap();

        put_bucket_policy_json(
            &bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": alt_policy_principal(),
                    "Action": "s3:DeleteObject",
                    "Resource": bucket_wildcard_resource(&bucket),
                }],
            }),
        )
        .await;
        client
            .get_bucket_policy()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        let deleted = alt_client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .bypass_governance_retention(true)
            .send()
            .await;
        deleted.unwrap();

        let current = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("get object-lock current object after delete")
            .await;
        assert_eq!(err_status(&current), 404);
        assert_s3_err_code(&current, "NoSuchKey");

        let retained = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .send()
            .await
            .unwrap();
        assert_eq!(
            retained.body.collect().await.unwrap().into_bytes().as_ref(),
            b"abc"
        );

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_put_object_headers_persist() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        let retain_until = governance_retain_until();
        let version_id = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"abc"))
            .object_lock_mode(ObjectLockMode::Governance)
            .object_lock_retain_until_date(retain_until)
            .object_lock_legal_hold_status(ObjectLockLegalHoldStatus::On)
            .send()
            .await
            .unwrap()
            .version_id()
            .unwrap()
            .to_string();

        let retention_response = client
            .get_object_retention()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .send()
            .await
            .unwrap();
        assert_eq!(
            retention_response.retention(),
            Some(&retention(
                ObjectLockRetentionMode::Governance,
                retain_until,
            ))
        );

        let legal_hold_response = client
            .get_object_legal_hold()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .send()
            .await
            .unwrap();
        assert_eq!(
            legal_hold_response.legal_hold(),
            Some(&legal_hold(ObjectLockLegalHoldStatus::On))
        );

        client
            .put_object_legal_hold()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .legal_hold(legal_hold(ObjectLockLegalHoldStatus::Off))
            .send()
            .await
            .unwrap();
        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_put_object_headers_invalid_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "file1";

        let result = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"abc"))
            .object_lock_mode(ObjectLockMode::Governance)
            .object_lock_retain_until_date(governance_retain_until())
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidRequest");

        cleanup_plain_bucket(&bucket, &[]).await;
    });
}

#[test]
fn test_object_lock_put_object_headers_invalid_bucket_large_body() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "file1";
        let retain_until = governance_retain_until();
        let body = vec![b'x'; server_core::coordinator::INTERNAL_SEGMENT_SIZE + 1];

        let result = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(body))
            .object_lock_mode(ObjectLockMode::Governance)
            .object_lock_retain_until_date(retain_until)
            .object_lock_legal_hold_status(ObjectLockLegalHoldStatus::On)
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidRequest");

        let head = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("head object-lock object after rejected put")
            .await;
        assert_eq!(err_status(&head), 404);

        cleanup_plain_bucket(&bucket, &[]).await;
    });
}

#[test]
fn test_object_lock_put_object_headers_reject_past_retain_until() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";

        let result = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"abc"))
            .object_lock_mode(ObjectLockMode::Governance)
            .object_lock_retain_until_date(past_date(60))
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidArgument");

        let head = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted(
                "head object-lock object after rejected past retention",
            )
            .await;
        assert_eq!(err_status(&head), 404);

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_put_obj_retention_invalid_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "file1";
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"abc"))
            .send()
            .await
            .unwrap();

        let result = client
            .put_object_retention()
            .bucket(&bucket)
            .key(key)
            .retention(retention(
                ObjectLockRetentionMode::Governance,
                governance_retain_until(),
            ))
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidRequest");

        cleanup_plain_bucket(&bucket, &[key]).await;
    });
}

#[test]
fn test_object_lock_unauthorized_calls_do_not_reveal_bucket_lock_configuration() {
    s3_tests::run(async {
        let alt_client = CTX.alt_client();
        let plain_bucket = setup_bucket().await;
        let lock_bucket = setup_object_lock_bucket().await;
        let key = "file1";

        CTX.client()
            .put_object()
            .bucket(&plain_bucket)
            .key(key)
            .body(ByteStream::from_static(b"abc"))
            .send()
            .await
            .unwrap();
        put_object_bytes(&lock_bucket, key, b"abc").await;

        let expected_retention = retention(
            ObjectLockRetentionMode::Governance,
            governance_retain_until(),
        );
        let expected_legal_hold = legal_hold(ObjectLockLegalHoldStatus::On);

        let plain_get_retention = sdk_error_status_and_code(
            alt_client
                .get_object_retention()
                .bucket(&plain_bucket)
                .key(key)
                .send()
                .await,
        );
        let lock_get_retention = sdk_error_status_and_code(
            alt_client
                .get_object_retention()
                .bucket(&lock_bucket)
                .key(key)
                .send()
                .await,
        );
        assert_eq!(
            plain_get_retention, lock_get_retention,
            "GetObjectRetention leaked bucket lock configuration: plain={plain_get_retention:?}, lock={lock_get_retention:?}"
        );

        let plain_put_retention = sdk_error_status_and_code(
            alt_client
                .put_object_retention()
                .bucket(&plain_bucket)
                .key(key)
                .retention(expected_retention.clone())
                .send()
                .await,
        );
        let lock_put_retention = sdk_error_status_and_code(
            alt_client
                .put_object_retention()
                .bucket(&lock_bucket)
                .key(key)
                .retention(expected_retention)
                .send()
                .await,
        );
        assert_eq!(
            plain_put_retention, lock_put_retention,
            "PutObjectRetention leaked bucket lock configuration: plain={plain_put_retention:?}, lock={lock_put_retention:?}"
        );

        let plain_get_legal_hold = sdk_error_status_and_code(
            alt_client
                .get_object_legal_hold()
                .bucket(&plain_bucket)
                .key(key)
                .send()
                .await,
        );
        let lock_get_legal_hold = sdk_error_status_and_code(
            alt_client
                .get_object_legal_hold()
                .bucket(&lock_bucket)
                .key(key)
                .send()
                .await,
        );
        assert_eq!(
            plain_get_legal_hold, lock_get_legal_hold,
            "GetObjectLegalHold leaked bucket lock configuration: plain={plain_get_legal_hold:?}, lock={lock_get_legal_hold:?}"
        );

        let plain_put_legal_hold = sdk_error_status_and_code(
            alt_client
                .put_object_legal_hold()
                .bucket(&plain_bucket)
                .key(key)
                .legal_hold(expected_legal_hold.clone())
                .send()
                .await,
        );
        let lock_put_legal_hold = sdk_error_status_and_code(
            alt_client
                .put_object_legal_hold()
                .bucket(&lock_bucket)
                .key(key)
                .legal_hold(expected_legal_hold)
                .send()
                .await,
        );
        assert_eq!(
            plain_put_legal_hold, lock_put_legal_hold,
            "PutObjectLegalHold leaked bucket lock configuration: plain={plain_put_legal_hold:?}, lock={lock_put_legal_hold:?}"
        );

        cleanup_plain_bucket(&plain_bucket, &[key]).await;
        cleanup_object_lock_bucket(&lock_bucket).await;
    });
}

#[test]
fn test_object_lock_put_obj_retention_invalid_mode() {
    s3_tests::run(async {
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        put_object_bytes(&bucket, key, b"abc").await;
        let url = object_retention_url(&bucket, key);
        let retain_until = governance_retain_until()
            .fmt(DateTimeFormat::DateTime)
            .unwrap();

        let body = object_retention_xml("governance", &retain_until);
        let (status, response_body) = signed_put_xml(&url, body.as_bytes());
        assert_eq!(status, 400, "body: {response_body}");
        assert_xml_error_code(&response_body, "MalformedXML");

        let body = object_retention_xml("abc", &retain_until);
        let (status, response_body) = signed_put_xml(&url, body.as_bytes());
        assert_eq!(status, 400, "body: {response_body}");
        assert_xml_error_code(&response_body, "MalformedXML");

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_get_obj_retention() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        put_object_bytes(&bucket, key, b"abc").await;
        let retention = retention(
            ObjectLockRetentionMode::Governance,
            governance_retain_until_later(),
        );

        client
            .put_object_retention()
            .bucket(&bucket)
            .key(key)
            .retention(retention.clone())
            .send()
            .await
            .unwrap();
        let response = client
            .get_object_retention()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(response.retention(), Some(&retention));

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_get_obj_retention_iso8601() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        let version_id = put_object_bytes(&bucket, key, b"abc").await;
        let retain_until = governance_retain_until();
        let retention = retention(ObjectLockRetentionMode::Governance, retain_until);

        client
            .put_object_retention()
            .bucket(&bucket)
            .key(key)
            .retention(retention)
            .send()
            .await
            .unwrap();

        let url = object_head_url(&bucket, key, &version_id);
        let (status, header_value) =
            signed_head_header(&url, "x-amz-object-lock-retain-until-date");
        assert_eq!(status, 200);
        let header_value = header_value.expect("missing retain-until header");
        let parsed = DateTime::from_str(&header_value, DateTimeFormat::DateTime).unwrap();
        assert_eq!(parsed, retain_until);

        delete_version_with_bypass(&bucket, key, &version_id).await;
        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_get_obj_retention_invalid_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "file1";
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"abc"))
            .send()
            .await
            .unwrap();

        let result = client
            .get_object_retention()
            .bucket(&bucket)
            .key(key)
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidRequest");

        cleanup_plain_bucket(&bucket, &[key]).await;
    });
}

#[test]
fn test_object_lock_put_obj_retention_versionid() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        let _version_id1 = put_object_bytes(&bucket, key, b"abc").await;
        let version_id2 = put_object_bytes(&bucket, key, b"abc").await;
        let retention = retention(
            ObjectLockRetentionMode::Governance,
            governance_retain_until(),
        );

        client
            .put_object_retention()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id2)
            .retention(retention.clone())
            .send()
            .await
            .unwrap();
        let response = client
            .get_object_retention()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id2)
            .send()
            .await
            .unwrap();
        assert_eq!(response.retention(), Some(&retention));

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_put_obj_retention_override_default_retention() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        put_object_lock_configuration(
            &bucket,
            bucket_lock_config_days(ObjectLockRetentionMode::Governance, 1),
        )
        .await;

        let key = "file1";
        let version_id = put_object_bytes(&bucket, key, b"abc").await;
        let retention = retention(
            ObjectLockRetentionMode::Governance,
            governance_retain_until_later(),
        );
        client
            .put_object_retention()
            .bucket(&bucket)
            .key(key)
            .retention(retention.clone())
            .send()
            .await
            .unwrap();
        let response = client
            .get_object_retention()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(response.retention(), Some(&retention));

        delete_version_with_bypass(&bucket, key, &version_id).await;
        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_default_retention_applies_on_put_object() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        put_object_lock_configuration(
            &bucket,
            bucket_lock_config_days(ObjectLockRetentionMode::Governance, 1),
        )
        .await;

        let key = "file1";
        let before = now_epoch_secs();
        let version_id = put_object_bytes(&bucket, key, b"abc").await;
        let after = now_epoch_secs();

        let response = client
            .get_object_retention()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .send()
            .await
            .unwrap();
        assert_retention_within_window(
            &response,
            ObjectLockRetentionMode::Governance,
            before + 86_400,
            after + 86_405,
        );

        delete_version_with_bypass(&bucket, key, &version_id).await;
        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_put_obj_retention_increase_period() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        let version_id = put_object_bytes(&bucket, key, b"abc").await;
        let retention1 = retention(
            ObjectLockRetentionMode::Governance,
            governance_retain_until(),
        );
        let retention2 = retention(
            ObjectLockRetentionMode::Governance,
            governance_retain_until_later(),
        );

        client
            .put_object_retention()
            .bucket(&bucket)
            .key(key)
            .retention(retention1)
            .send()
            .await
            .unwrap();
        client
            .put_object_retention()
            .bucket(&bucket)
            .key(key)
            .retention(retention2.clone())
            .send()
            .await
            .unwrap();

        let response = client
            .get_object_retention()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(response.retention(), Some(&retention2));

        delete_version_with_bypass(&bucket, key, &version_id).await;
        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_put_obj_retention_shorten_period() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        let version_id = put_object_bytes(&bucket, key, b"abc").await;

        client
            .put_object_retention()
            .bucket(&bucket)
            .key(key)
            .retention(retention(
                ObjectLockRetentionMode::Governance,
                governance_retain_until_later(),
            ))
            .send()
            .await
            .unwrap();
        let shortened_until = lock_date_header(&governance_retain_until());
        let retention_xml = format!(
            "<Retention xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Mode>GOVERNANCE</Mode><RetainUntilDate>{shortened_until}</RetainUntilDate>\
             </Retention>"
        );
        let response = send_signed_request(
            "PUT",
            &format!("{}/{}/{}?retention=", CTX.endpoint(), bucket, key),
            retention_xml.as_bytes(),
            [content_md5_header(retention_xml.as_bytes())],
        );
        assert_shape(
            "PutObjectRetention shorten explicit retention",
            &response,
            &shape().status(403).headers(error_response_headers()).body(
                expected_error::with_host_id(
                    "AccessDenied",
                    "Access Denied because object protected by object lock.",
                ),
            ),
        );

        delete_version_with_bypass(&bucket, key, &version_id).await;
        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_put_obj_retention_shorten_period_bypass() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        let version_id = put_object_bytes(&bucket, key, b"abc").await;
        let shortened = retention(
            ObjectLockRetentionMode::Governance,
            governance_retain_until(),
        );

        client
            .put_object_retention()
            .bucket(&bucket)
            .key(key)
            .retention(retention(
                ObjectLockRetentionMode::Governance,
                governance_retain_until_later(),
            ))
            .send()
            .await
            .unwrap();
        client
            .put_object_retention()
            .bucket(&bucket)
            .key(key)
            .retention(shortened.clone())
            .bypass_governance_retention(true)
            .send()
            .await
            .unwrap();

        let response = client
            .get_object_retention()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(response.retention(), Some(&shortened));

        delete_version_with_bypass(&bucket, key, &version_id).await;
        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_delete_object_with_retention() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        let version_id = put_object_bytes(&bucket, key, b"abc").await;

        client
            .put_object_retention()
            .bucket(&bucket)
            .key(key)
            .retention(retention(
                ObjectLockRetentionMode::Governance,
                governance_retain_until(),
            ))
            .send()
            .await
            .unwrap();

        let response = send_signed_request(
            "DELETE",
            &format!(
                "{}/{}/{}?versionId={}",
                CTX.endpoint(),
                bucket,
                key,
                version_id
            ),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        assert_shape(
            "DeleteObject locked version without bypass",
            &response,
            &shape().status(403).headers(error_response_headers()).body(
                expected_error::with_host_id(
                    "AccessDenied",
                    "Access Denied because object protected by object lock.",
                ),
            ),
        );

        delete_version_with_bypass(&bucket, key, &version_id).await;
        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_delete_multipart_object_with_retention() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        let retain_until = governance_retain_until();

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .object_lock_mode(ObjectLockMode::Governance)
            .object_lock_retain_until_date(retain_until)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let part = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from_static(b"abc"))
            .send()
            .await
            .unwrap();
        let complete = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(part.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .unwrap();
        let version_id = complete.version_id().unwrap();

        let result = client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .version_id(version_id)
            .send()
            .await;
        assert_eq!(err_status(&result), 403);
        assert_s3_err_code(&result, "AccessDenied");

        delete_version_with_bypass(&bucket, key, version_id).await;
        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_delete_object_with_retention_and_marker() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        let version_id = put_object_bytes(&bucket, key, b"abc").await;

        client
            .put_object_retention()
            .bucket(&bucket)
            .key(key)
            .retention(retention(
                ObjectLockRetentionMode::Governance,
                governance_retain_until(),
            ))
            .send()
            .await
            .unwrap();

        let delete_marker = client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(delete_marker.delete_marker(), Some(true));

        let result = client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .send()
            .await;
        assert_eq!(err_status(&result), 403);
        assert_s3_err_code(&result, "AccessDenied");

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .version_id(delete_marker.version_id().unwrap())
            .send()
            .await
            .unwrap();

        let result = client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .send()
            .await;
        assert_eq!(err_status(&result), 403);
        assert_s3_err_code(&result, "AccessDenied");

        delete_version_with_bypass(&bucket, key, &version_id).await;
        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_multi_delete_object_with_retention() {
    s3_tests::run(async {
        use md5_legacy::Digest;

        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key1 = "file1";
        let key2 = "file2";

        let version_id1 = put_object_bytes(&bucket, key1, b"abc").await;
        let version_id2 = put_object_bytes(&bucket, key2, b"abc").await;

        client
            .put_object_retention()
            .bucket(&bucket)
            .key(key1)
            .retention(retention(
                ObjectLockRetentionMode::Governance,
                governance_retain_until(),
            ))
            .send()
            .await
            .unwrap();

        let delete = Delete::builder()
            .objects(object_id(key1, &version_id1))
            .objects(object_id(key2, &version_id2))
            .build()
            .unwrap();
        let response = client
            .delete_objects()
            .bucket(&bucket)
            .delete(delete)
            .customize()
            .mutate_request(|req| {
                let body = req.body().bytes().expect("DeleteObjects body in memory");
                let digest = md5_legacy::Md5::digest(body);
                let content_md5 = base64::engine::general_purpose::STANDARD.encode(&digest[..]);
                req.headers_mut().insert("content-md5", content_md5);
            })
            .send()
            .await
            .unwrap();

        assert_eq!(response.deleted().len(), 1);
        assert_eq!(response.errors().len(), 1);
        let failed = &response.errors()[0];
        assert_eq!(failed.code(), Some("AccessDenied"));
        assert_eq!(failed.key(), Some(key1));
        assert_eq!(failed.version_id(), Some(version_id1.as_str()));
        let deleted = &response.deleted()[0];
        assert_eq!(deleted.key(), Some(key2));
        assert_eq!(deleted.version_id(), Some(version_id2.as_str()));

        let delete = Delete::builder()
            .objects(object_id(key1, &version_id1))
            .build()
            .unwrap();
        let response = client
            .delete_objects()
            .bucket(&bucket)
            .delete(delete)
            .bypass_governance_retention(true)
            .customize()
            .mutate_request(|req| {
                let body = req.body().bytes().expect("DeleteObjects body in memory");
                let digest = md5_legacy::Md5::digest(body);
                let content_md5 = base64::engine::general_purpose::STANDARD.encode(&digest[..]);
                req.headers_mut().insert("content-md5", content_md5);
            })
            .send()
            .await
            .unwrap();
        assert!(response.errors().is_empty());
        assert_eq!(response.deleted().len(), 1);
        let deleted = &response.deleted()[0];
        assert_eq!(deleted.key(), Some(key1));
        assert_eq!(deleted.version_id(), Some(version_id1.as_str()));

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_put_legal_hold() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        put_object_bytes(&bucket, key, b"abc").await;

        client
            .put_object_legal_hold()
            .bucket(&bucket)
            .key(key)
            .legal_hold(legal_hold(ObjectLockLegalHoldStatus::On))
            .send()
            .await
            .unwrap();
        client
            .put_object_legal_hold()
            .bucket(&bucket)
            .key(key)
            .legal_hold(legal_hold(ObjectLockLegalHoldStatus::Off))
            .send()
            .await
            .unwrap();

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_bucket_policy_put_get_legal_hold() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        put_object_bytes(&bucket, key, b"abc").await;

        put_bucket_policy_json(
            &bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": alt_policy_principal(),
                    "Action": ["s3:PutObjectLegalHold", "s3:GetObjectLegalHold"],
                    "Resource": bucket_wildcard_resource(&bucket),
                }],
            }),
        )
        .await;
        client
            .get_bucket_policy()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        alt_client
            .put_object_legal_hold()
            .bucket(&bucket)
            .key(key)
            .legal_hold(legal_hold(ObjectLockLegalHoldStatus::On))
            .send()
            .await
            .unwrap();
        let response = alt_client
            .get_object_legal_hold()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.legal_hold(),
            Some(&legal_hold(ObjectLockLegalHoldStatus::On))
        );

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_bucket_policy_get_obj_legal_hold_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let key = "file1";
        let hold_on = legal_hold(ObjectLockLegalHoldStatus::On);

        let allowed_bucket = setup_object_lock_bucket().await;
        put_object_bytes(&allowed_bucket, key, b"abc").await;
        client
            .put_object_legal_hold()
            .bucket(&allowed_bucket)
            .key(key)
            .legal_hold(hold_on.clone())
            .send()
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(&allowed_bucket, "allow").await;
        put_bucket_policy_json(
            &allowed_bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": alt_policy_principal(),
                    "Action": "s3:GetObjectLegalHold",
                    "Resource": bucket_wildcard_resource(&allowed_bucket),
                    "Condition": {
                        "StringEquals": {
                            "s3:BucketTag/security": "allow"
                        }
                    }
                }],
            }),
        )
        .await;

        let denied_bucket = setup_object_lock_bucket().await;
        put_object_bytes(&denied_bucket, key, b"abc").await;
        client
            .put_object_legal_hold()
            .bucket(&denied_bucket)
            .key(key)
            .legal_hold(hold_on.clone())
            .send()
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(&denied_bucket, "deny").await;
        put_bucket_policy_json(
            &denied_bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": alt_policy_principal(),
                    "Action": "s3:GetObjectLegalHold",
                    "Resource": bucket_wildcard_resource(&denied_bucket),
                    "Condition": {
                        "StringEquals": {
                            "s3:BucketTag/security": "allow"
                        }
                    }
                }],
            }),
        )
        .await;

        let response = alt_client
            .get_object_legal_hold()
            .bucket(&allowed_bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(response.legal_hold(), Some(&hold_on));

        let denied = alt_client
            .get_object_legal_hold()
            .bucket(&denied_bucket)
            .key(key)
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        cleanup_object_lock_bucket(&allowed_bucket).await;
        cleanup_object_lock_bucket(&denied_bucket).await;
    });
}

#[test]
fn test_object_lock_bucket_policy_put_obj_retention_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let key = "file1";
        let retain_until = governance_retain_until();
        let retention = retention(ObjectLockRetentionMode::Governance, retain_until);

        let allowed_bucket = setup_object_lock_bucket().await;
        put_object_bytes(&allowed_bucket, key, b"abc").await;
        enable_bucket_abac_with_security_tag(&allowed_bucket, "allow").await;
        put_bucket_policy_json(
            &allowed_bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": alt_policy_principal(),
                    "Action": "s3:PutObjectRetention",
                    "Resource": bucket_wildcard_resource(&allowed_bucket),
                    "Condition": {
                        "StringEquals": {
                            "s3:BucketTag/security": "allow"
                        }
                    }
                }],
            }),
        )
        .await;

        let denied_bucket = setup_object_lock_bucket().await;
        put_object_bytes(&denied_bucket, key, b"abc").await;
        enable_bucket_abac_with_security_tag(&denied_bucket, "deny").await;
        put_bucket_policy_json(
            &denied_bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": alt_policy_principal(),
                    "Action": "s3:PutObjectRetention",
                    "Resource": bucket_wildcard_resource(&denied_bucket),
                    "Condition": {
                        "StringEquals": {
                            "s3:BucketTag/security": "allow"
                        }
                    }
                }],
            }),
        )
        .await;

        alt_client
            .put_object_retention()
            .bucket(&allowed_bucket)
            .key(key)
            .retention(retention.clone())
            .send()
            .await
            .unwrap();
        let response = client
            .get_object_retention()
            .bucket(&allowed_bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(response.retention(), Some(&retention));

        let denied = alt_client
            .put_object_retention()
            .bucket(&denied_bucket)
            .key(key)
            .retention(retention.clone())
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        cleanup_object_lock_bucket(&allowed_bucket).await;
        cleanup_object_lock_bucket(&denied_bucket).await;
    });
}

#[test]
fn test_object_lock_bucket_policy_put_obj_legal_hold_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let key = "file1";
        let hold_on = legal_hold(ObjectLockLegalHoldStatus::On);

        let allowed_bucket = setup_object_lock_bucket().await;
        put_object_bytes(&allowed_bucket, key, b"abc").await;
        enable_bucket_abac_with_security_tag(&allowed_bucket, "allow").await;
        put_bucket_policy_json(
            &allowed_bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": alt_policy_principal(),
                    "Action": "s3:PutObjectLegalHold",
                    "Resource": bucket_wildcard_resource(&allowed_bucket),
                    "Condition": {
                        "StringEquals": {
                            "s3:BucketTag/security": "allow"
                        }
                    }
                }],
            }),
        )
        .await;

        let denied_bucket = setup_object_lock_bucket().await;
        put_object_bytes(&denied_bucket, key, b"abc").await;
        enable_bucket_abac_with_security_tag(&denied_bucket, "deny").await;
        put_bucket_policy_json(
            &denied_bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": alt_policy_principal(),
                    "Action": "s3:PutObjectLegalHold",
                    "Resource": bucket_wildcard_resource(&denied_bucket),
                    "Condition": {
                        "StringEquals": {
                            "s3:BucketTag/security": "allow"
                        }
                    }
                }],
            }),
        )
        .await;

        alt_client
            .put_object_legal_hold()
            .bucket(&allowed_bucket)
            .key(key)
            .legal_hold(hold_on.clone())
            .send()
            .await
            .unwrap();
        let response = client
            .get_object_legal_hold()
            .bucket(&allowed_bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(response.legal_hold(), Some(&hold_on));

        let denied = alt_client
            .put_object_legal_hold()
            .bucket(&denied_bucket)
            .key(key)
            .legal_hold(hold_on)
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        cleanup_object_lock_bucket(&allowed_bucket).await;
        cleanup_object_lock_bucket(&denied_bucket).await;
    });
}

#[test]
fn test_object_lock_bucket_policy_existing_tag_condition_is_rejected_for_get_legal_hold() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        put_object_bytes(&bucket, key, b"abc").await;
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .tagging(object_tagging("security", "allow"))
            .send()
            .await
            .unwrap();

        let result = client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:GetObjectLegalHold",
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:ExistingObjectTag/security": "allow"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "MalformedPolicy");

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_put_legal_hold_invalid_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "file1";
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"abc"))
            .send()
            .await
            .unwrap();

        let result = client
            .put_object_legal_hold()
            .bucket(&bucket)
            .key(key)
            .legal_hold(legal_hold(ObjectLockLegalHoldStatus::On))
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidRequest");

        cleanup_plain_bucket(&bucket, &[key]).await;
    });
}

#[test]
fn test_object_lock_put_legal_hold_invalid_status() {
    s3_tests::run(async {
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        put_object_bytes(&bucket, key, b"abc").await;
        let url = object_legal_hold_url(&bucket, key);
        let body = legal_hold_xml("abc");

        let (status, response_body) = signed_put_xml(&url, body.as_bytes());
        assert_eq!(status, 400, "body: {response_body}");
        assert_xml_error_code(&response_body, "MalformedXML");

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_get_legal_hold() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        put_object_bytes(&bucket, key, b"abc").await;

        client
            .put_object_legal_hold()
            .bucket(&bucket)
            .key(key)
            .legal_hold(legal_hold(ObjectLockLegalHoldStatus::On))
            .send()
            .await
            .unwrap();
        let response = client
            .get_object_legal_hold()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.legal_hold(),
            Some(&legal_hold(ObjectLockLegalHoldStatus::On))
        );

        client
            .put_object_legal_hold()
            .bucket(&bucket)
            .key(key)
            .legal_hold(legal_hold(ObjectLockLegalHoldStatus::Off))
            .send()
            .await
            .unwrap();
        let response = client
            .get_object_legal_hold()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.legal_hold(),
            Some(&legal_hold(ObjectLockLegalHoldStatus::Off))
        );

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_put_legal_hold_versionid() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        let version_id1 = put_object_bytes(&bucket, key, b"abc").await;
        let version_id2 = put_object_bytes(&bucket, key, b"def").await;

        client
            .put_object_legal_hold()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id1)
            .legal_hold(legal_hold(ObjectLockLegalHoldStatus::On))
            .send()
            .await
            .unwrap();
        client
            .put_object_legal_hold()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id2)
            .legal_hold(legal_hold(ObjectLockLegalHoldStatus::Off))
            .send()
            .await
            .unwrap();

        let response = client
            .get_object_legal_hold()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id1)
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.legal_hold(),
            Some(&legal_hold(ObjectLockLegalHoldStatus::On))
        );

        let response = client
            .get_object_legal_hold()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id2)
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.legal_hold(),
            Some(&legal_hold(ObjectLockLegalHoldStatus::Off))
        );

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_get_legal_hold_invalid_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "file1";
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"abc"))
            .send()
            .await
            .unwrap();

        let result = client
            .get_object_legal_hold()
            .bucket(&bucket)
            .key(key)
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidRequest");

        cleanup_plain_bucket(&bucket, &[key]).await;
    });
}

#[test]
fn test_object_lock_copy_object_headers_persist() {
    s3_tests::run(async {
        let client = CTX.client();
        let src_bucket = setup_bucket().await;
        let dst_bucket = setup_object_lock_bucket().await;
        let src_key = "src";
        let dst_key = "dst";
        let retain_until = governance_retain_until();

        client
            .put_object()
            .bucket(&src_bucket)
            .key(src_key)
            .body(ByteStream::from_static(b"abc"))
            .send()
            .await
            .unwrap();

        let version_id = client
            .copy_object()
            .copy_source(format!("{src_bucket}/{src_key}"))
            .bucket(&dst_bucket)
            .key(dst_key)
            .object_lock_mode(ObjectLockMode::Governance)
            .object_lock_retain_until_date(retain_until)
            .object_lock_legal_hold_status(ObjectLockLegalHoldStatus::On)
            .send()
            .await
            .unwrap()
            .version_id()
            .unwrap()
            .to_string();

        let retention_response = client
            .get_object_retention()
            .bucket(&dst_bucket)
            .key(dst_key)
            .version_id(&version_id)
            .send()
            .await
            .unwrap();
        assert_eq!(
            retention_response.retention(),
            Some(&retention(
                ObjectLockRetentionMode::Governance,
                retain_until,
            ))
        );
        let legal_hold_response = client
            .get_object_legal_hold()
            .bucket(&dst_bucket)
            .key(dst_key)
            .version_id(&version_id)
            .send()
            .await
            .unwrap();
        assert_eq!(
            legal_hold_response.legal_hold(),
            Some(&legal_hold(ObjectLockLegalHoldStatus::On))
        );

        client
            .put_object_legal_hold()
            .bucket(&dst_bucket)
            .key(dst_key)
            .version_id(&version_id)
            .legal_hold(legal_hold(ObjectLockLegalHoldStatus::Off))
            .send()
            .await
            .unwrap();
        delete_version_with_bypass(&dst_bucket, dst_key, &version_id).await;
        cleanup_plain_bucket(&src_bucket, &[src_key]).await;
        cleanup_object_lock_bucket(&dst_bucket).await;
    });
}

#[test]
fn test_object_lock_copy_object_headers_invalid_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let src_bucket = setup_bucket().await;
        let dst_bucket = setup_bucket().await;
        let src_key = "src";
        let dst_key = "dst";
        let retain_until = governance_retain_until();
        let source_body = vec![b'a'; server_core::coordinator::INTERNAL_SEGMENT_SIZE + 1];

        client
            .put_object()
            .bucket(&src_bucket)
            .key(src_key)
            .body(ByteStream::from(source_body))
            .send()
            .await
            .unwrap();

        let result = client
            .copy_object()
            .copy_source(format!("{src_bucket}/{src_key}"))
            .bucket(&dst_bucket)
            .key(dst_key)
            .object_lock_mode(ObjectLockMode::Governance)
            .object_lock_retain_until_date(retain_until)
            .object_lock_legal_hold_status(ObjectLockLegalHoldStatus::On)
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidRequest");

        let head = client
            .head_object()
            .bucket(&dst_bucket)
            .key(dst_key)
            .send()
            .await;
        assert_eq!(err_status(&head), 404);

        cleanup_plain_bucket(&src_bucket, &[src_key]).await;
        cleanup_plain_bucket(&dst_bucket, &[]).await;
    });
}

#[test]
fn test_object_lock_copy_object_headers_reject_past_retain_until() {
    s3_tests::run(async {
        let client = CTX.client();
        let src_bucket = setup_bucket().await;
        let dst_bucket = setup_object_lock_bucket().await;
        let src_key = "src";
        let dst_key = "dst";

        client
            .put_object()
            .bucket(&src_bucket)
            .key(src_key)
            .body(ByteStream::from_static(b"abc"))
            .send()
            .await
            .unwrap();

        let result = client
            .copy_object()
            .copy_source(format!("{src_bucket}/{src_key}"))
            .bucket(&dst_bucket)
            .key(dst_key)
            .object_lock_mode(ObjectLockMode::Governance)
            .object_lock_retain_until_date(past_date(60))
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidArgument");

        let head = client
            .head_object()
            .bucket(&dst_bucket)
            .key(dst_key)
            .send()
            .await;
        assert_eq!(err_status(&head), 404);

        cleanup_plain_bucket(&src_bucket, &[src_key]).await;
        cleanup_object_lock_bucket(&dst_bucket).await;
    });
}

#[test]
fn test_object_lock_delete_object_with_legal_hold_on() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        let version_id = put_object_bytes(&bucket, key, b"abc").await;

        client
            .put_object_legal_hold()
            .bucket(&bucket)
            .key(key)
            .legal_hold(legal_hold(ObjectLockLegalHoldStatus::On))
            .send()
            .await
            .unwrap();

        let result = client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .send()
            .await;
        assert_eq!(err_status(&result), 403);
        assert_s3_err_code(&result, "AccessDenied");

        client
            .put_object_legal_hold()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .legal_hold(legal_hold(ObjectLockLegalHoldStatus::Off))
            .send()
            .await
            .unwrap();

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_delete_multipart_object_with_legal_hold_on() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .object_lock_legal_hold_status(ObjectLockLegalHoldStatus::On)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let part = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from_static(b"abc"))
            .send()
            .await
            .unwrap();
        let complete = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(part.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .unwrap();
        let version_id = complete.version_id().unwrap().to_string();

        let result = client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .send()
            .await;
        assert_eq!(err_status(&result), 403);
        assert_s3_err_code(&result, "AccessDenied");

        client
            .put_object_legal_hold()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .legal_hold(legal_hold(ObjectLockLegalHoldStatus::Off))
            .send()
            .await
            .unwrap();

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_delete_object_with_legal_hold_off() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        let version_id = put_object_bytes(&bucket, key, b"abc").await;

        client
            .put_object_legal_hold()
            .bucket(&bucket)
            .key(key)
            .legal_hold(legal_hold(ObjectLockLegalHoldStatus::Off))
            .send()
            .await
            .unwrap();
        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .send()
            .await
            .unwrap();

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_get_obj_metadata() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        let version_id = put_object_bytes(&bucket, key, b"abc").await;
        let retain_until = governance_retain_until();

        client
            .put_object_legal_hold()
            .bucket(&bucket)
            .key(key)
            .legal_hold(legal_hold(ObjectLockLegalHoldStatus::On))
            .send()
            .await
            .unwrap();
        client
            .put_object_retention()
            .bucket(&bucket)
            .key(key)
            .retention(retention(ObjectLockRetentionMode::Governance, retain_until))
            .send()
            .await
            .unwrap();

        let head = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(head.object_lock_mode(), Some(&ObjectLockMode::Governance));
        assert_eq!(head.object_lock_retain_until_date(), Some(&retain_until));
        assert_eq!(
            head.object_lock_legal_hold_status(),
            Some(&ObjectLockLegalHoldStatus::On)
        );

        client
            .put_object_legal_hold()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .legal_hold(legal_hold(ObjectLockLegalHoldStatus::Off))
            .send()
            .await
            .unwrap();
        delete_version_with_bypass(&bucket, key, &version_id).await;
        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_uploading_obj() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        let retain_until = governance_retain_until();
        let version_id = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"abc"))
            .object_lock_mode(ObjectLockMode::Governance)
            .object_lock_retain_until_date(retain_until)
            .object_lock_legal_hold_status(ObjectLockLegalHoldStatus::On)
            .send()
            .await
            .unwrap()
            .version_id()
            .unwrap()
            .to_string();

        let head = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(head.object_lock_mode(), Some(&ObjectLockMode::Governance));
        assert_eq!(head.object_lock_retain_until_date(), Some(&retain_until));
        assert_eq!(
            head.object_lock_legal_hold_status(),
            Some(&ObjectLockLegalHoldStatus::On)
        );

        client
            .put_object_legal_hold()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .legal_hold(legal_hold(ObjectLockLegalHoldStatus::Off))
            .send()
            .await
            .unwrap();
        delete_version_with_bypass(&bucket, key, &version_id).await;
        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_changing_mode_from_governance_with_bypass() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        let retain_until = future_date(COMPLIANCE_RETENTION_SECS);
        let version_id = put_object_bytes(&bucket, key, b"abc").await;

        client
            .put_object_retention()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .retention(retention(ObjectLockRetentionMode::Governance, retain_until))
            .send()
            .await
            .unwrap();

        client
            .put_object_retention()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .retention(retention(ObjectLockRetentionMode::Compliance, retain_until))
            .bypass_governance_retention(true)
            .send()
            .await
            .unwrap();

        let response = client
            .get_object_retention()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.retention(),
            Some(&retention(
                ObjectLockRetentionMode::Compliance,
                retain_until,
            ))
        );

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_create_multipart_upload_headers_persist() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        let retain_until = governance_retain_until();

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .object_lock_mode(ObjectLockMode::Governance)
            .object_lock_retain_until_date(retain_until)
            .object_lock_legal_hold_status(ObjectLockLegalHoldStatus::On)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let part = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from_static(b"abc"))
            .send()
            .await
            .unwrap();
        let version_id = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(part.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .unwrap()
            .version_id()
            .unwrap()
            .to_string();

        let retention_response = client
            .get_object_retention()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .send()
            .await
            .unwrap();
        assert_eq!(
            retention_response.retention(),
            Some(&retention(
                ObjectLockRetentionMode::Governance,
                retain_until,
            ))
        );
        let legal_hold_response = client
            .get_object_legal_hold()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .send()
            .await
            .unwrap();
        assert_eq!(
            legal_hold_response.legal_hold(),
            Some(&legal_hold(ObjectLockLegalHoldStatus::On))
        );

        client
            .put_object_legal_hold()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .legal_hold(legal_hold(ObjectLockLegalHoldStatus::Off))
            .send()
            .await
            .unwrap();
        delete_version_with_bypass(&bucket, key, &version_id).await;
        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_create_multipart_upload_headers_reject_past_retain_until() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";

        let result = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .object_lock_mode(ObjectLockMode::Governance)
            .object_lock_retain_until_date(past_date(60))
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidArgument");

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_default_retention_applies_on_complete_multipart_upload() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        put_object_lock_configuration(
            &bucket,
            bucket_lock_config_days(ObjectLockRetentionMode::Governance, 1),
        )
        .await;
        let key = "file1";
        let before = now_epoch_secs();

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let part = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from_static(b"abc"))
            .send()
            .await
            .unwrap();
        let version_id = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(part.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .unwrap()
            .version_id()
            .unwrap()
            .to_string();
        let after = now_epoch_secs();

        let response = client
            .get_object_retention()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .send()
            .await
            .unwrap();
        assert_retention_within_window(
            &response,
            ObjectLockRetentionMode::Governance,
            before + 86_400,
            after + 86_405,
        );

        delete_version_with_bypass(&bucket, key, &version_id).await;
        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_changing_mode_from_governance_without_bypass() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        let retain_until = future_date(COMPLIANCE_RETENTION_SECS);
        let version_id = put_object_bytes(&bucket, key, b"abc").await;

        client
            .put_object_retention()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .retention(retention(ObjectLockRetentionMode::Governance, retain_until))
            .send()
            .await
            .unwrap();

        let result = client
            .put_object_retention()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .retention(retention(ObjectLockRetentionMode::Compliance, retain_until))
            .send()
            .await;
        assert_eq!(err_status(&result), 403);
        assert_s3_err_code(&result, "AccessDenied");

        let response = client
            .get_object_retention()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.retention(),
            Some(&retention(
                ObjectLockRetentionMode::Governance,
                retain_until,
            ))
        );

        delete_version_with_bypass(&bucket, key, &version_id).await;
        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_changing_mode_from_compliance() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key = "file1";
        let retain_until = future_date(COMPLIANCE_RETENTION_SECS);
        let version_id = put_object_bytes(&bucket, key, b"abc").await;

        client
            .put_object_retention()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .retention(retention(ObjectLockRetentionMode::Compliance, retain_until))
            .send()
            .await
            .unwrap();

        let result = client
            .put_object_retention()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .retention(retention(ObjectLockRetentionMode::Governance, retain_until))
            .send()
            .await;
        assert_eq!(err_status(&result), 403);
        assert_s3_err_code(&result, "AccessDenied");

        cleanup_object_lock_bucket(&bucket).await;
    });
}

// ── Response shapes ─────────────────────────────────────────────────

/// The lock timestamp as it appears in response headers, e.g.
/// `2026-07-08T10:54:10Z`.
fn lock_date_header(date: &DateTime) -> String {
    date.fmt(DateTimeFormat::DateTime)
        .expect("format lock date header")
}

/// The lock timestamp as it appears in XML bodies, with milliseconds.
fn lock_date_xml(date: &DateTime) -> String {
    lock_date_header(date).replace('Z', ".000Z")
}

#[test]
fn test_get_bucket_object_lock_configuration_response_shape() {
    s3_tests::run(async {
        let bucket = setup_object_lock_bucket().await;
        put_object_lock_configuration(
            &bucket,
            bucket_lock_config_days(ObjectLockRetentionMode::Governance, 7),
        )
        .await;

        let response = raw_bucket("GET", &bucket, Some("object-lock="));
        assert_shape(
            "GetBucketObjectLockConfiguration shape",
            &response,
            &shape()
                .status(200)
                .headers(chunked_response_headers())
                .body(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ObjectLockConfiguration \
                     xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                     <ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention>\
                     <Mode>GOVERNANCE</Mode><Days>7</Days></DefaultRetention></Rule>\
                     </ObjectLockConfiguration>",
                ),
        );

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_get_object_retention_response_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key = "shape-retention.txt";
        let retain_until = future_date(24 * 60 * 60);

        let version = put_object_bytes(&bucket, key, b"retention").await;
        client
            .put_object_retention()
            .bucket(&bucket)
            .key(key)
            .version_id(&version)
            .retention(retention(ObjectLockRetentionMode::Governance, retain_until))
            .send()
            .await
            .expect("put object retention");

        let response = raw_object_query(
            "GET",
            &bucket,
            key,
            &format!("retention=&versionId={version}"),
        );
        assert_shape(
            "GetObjectRetention shape",
            &response,
            &shape()
                .status(200)
                .headers(chunked_response_headers())
                .body(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Retention \
                     xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Mode>GOVERNANCE</Mode>\
                     <RetainUntilDate>{retain_until}</RetainUntilDate></Retention>",
                )
                .sub("retain_until", lock_date_xml(&retain_until)),
        );

        delete_version_with_bypass(&bucket, key, &version).await;
        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_get_object_legal_hold_response_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let key = "shape-legal-hold.txt";

        let version = put_object_bytes(&bucket, key, b"legal-hold").await;
        client
            .put_object_legal_hold()
            .bucket(&bucket)
            .key(key)
            .version_id(&version)
            .legal_hold(
                ObjectLockLegalHold::builder()
                    .status(ObjectLockLegalHoldStatus::On)
                    .build(),
            )
            .send()
            .await
            .expect("put legal hold");

        let response = raw_object_query(
            "GET",
            &bucket,
            key,
            &format!("legal-hold=&versionId={version}"),
        );
        assert_shape(
            "GetObjectLegalHold shape",
            &response,
            &shape()
                .status(200)
                .headers(chunked_response_headers())
                .body(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<LegalHold \
                     xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Status>ON</Status>\
                     </LegalHold>",
                ),
        );

        client
            .put_object_legal_hold()
            .bucket(&bucket)
            .key(key)
            .version_id(&version)
            .legal_hold(
                ObjectLockLegalHold::builder()
                    .status(ObjectLockLegalHoldStatus::Off)
                    .build(),
            )
            .send()
            .await
            .expect("clear legal hold");
        delete_version_with_bypass(&bucket, key, &version).await;
        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_read_response_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_object_lock_bucket().await;
        let locked_key = "shape-object-lock.txt";
        let plain_key = "shape-object-lock-plain.txt";
        let retain_until = future_date(24 * 60 * 60);

        let locked_put = client
            .put_object()
            .bucket(&bucket)
            .key(locked_key)
            .body(ByteStream::from_static(b"object-lock-body"))
            .object_lock_mode(ObjectLockMode::Governance)
            .object_lock_retain_until_date(retain_until)
            .object_lock_legal_hold_status(ObjectLockLegalHoldStatus::On)
            .send()
            .await
            .expect("put locked object");
        let locked_version = locked_put.version_id().expect("locked version").to_string();
        let plain_version = put_object_bytes(&bucket, plain_key, b"plain-object-lock-body").await;

        let locked_headers = [
            ("etag", "{etag}"),
            ("last-modified", "{http_date}"),
            ("accept-ranges", "bytes"),
            ("x-amz-version-id", "{version_id}"),
            ("content-type", "application/octet-stream"),
            ("x-amz-object-lock-mode", "GOVERNANCE"),
            ("x-amz-object-lock-retain-until-date", "{retain_until}"),
            ("x-amz-object-lock-legal-hold", "ON"),
            ("x-amz-server-side-encryption", "AES256"),
            ("content-length", "16"),
            ("x-amz-request-id", "{request_id}"),
            ("x-amz-id-2", "{host_id}"),
        ];
        let get = raw_object("GET", &bucket, locked_key);
        let get_captures = assert_shape(
            "GetObject locked shape",
            &get,
            &shape()
                .status(200)
                .headers(locked_headers)
                .sub("version_id", locked_version.as_str())
                .sub("retain_until", lock_date_header(&retain_until))
                .body("object-lock-body"),
        );
        let head = raw_object("HEAD", &bucket, locked_key);
        let head_captures = assert_shape(
            "HeadObject locked shape",
            &head,
            &shape()
                .status(200)
                .headers(locked_headers)
                .sub("version_id", locked_version.as_str())
                .sub("retain_until", lock_date_header(&retain_until))
                .body_empty(),
        );
        assert_eq!(get_captures["etag"], head_captures["etag"]);

        // Explicit-version reads carry the same lock headers.
        let version_get = raw_object_query(
            "GET",
            &bucket,
            locked_key,
            &format!("versionId={locked_version}"),
        );
        assert_shape(
            "GetObject locked explicit version shape",
            &version_get,
            &shape()
                .status(200)
                .headers(locked_headers)
                .sub("version_id", locked_version.as_str())
                .sub("retain_until", lock_date_header(&retain_until))
                .body("object-lock-body"),
        );
        let version_head = raw_object_query(
            "HEAD",
            &bucket,
            locked_key,
            &format!("versionId={locked_version}"),
        );
        assert_shape(
            "HeadObject locked explicit version shape",
            &version_head,
            &shape()
                .status(200)
                .headers(locked_headers)
                .sub("version_id", locked_version.as_str())
                .sub("retain_until", lock_date_header(&retain_until))
                .body_empty(),
        );

        // GetObjectAttributes on a locked object carries no lock headers.
        let attributes = s3_tests::send_signed_request(
            "GET",
            &format!("{}/{}/{}?attributes=", CTX.endpoint(), bucket, locked_key),
            b"",
            [("x-amz-object-attributes", "ObjectSize")],
        );
        assert_shape(
            "GetObjectAttributes locked shape",
            &attributes,
            &shape()
                .status(200)
                .headers([
                    ("content-length", "173"),
                    ("last-modified", "{http_date}"),
                    ("x-amz-version-id", "{version_id}"),
                    ("x-amz-request-id", "{request_id}"),
                    ("x-amz-id-2", "{host_id}"),
                ])
                .sub("version_id", locked_version.as_str())
                .body(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<GetObjectAttributesResponse \
                     xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                     <ObjectSize>16</ObjectSize></GetObjectAttributesResponse>",
                ),
        );

        let plain_headers = [
            ("etag", "{etag}"),
            ("last-modified", "{http_date}"),
            ("accept-ranges", "bytes"),
            ("x-amz-version-id", "{version_id}"),
            ("content-type", "application/octet-stream"),
            ("x-amz-server-side-encryption", "AES256"),
            ("content-length", "22"),
            ("x-amz-request-id", "{request_id}"),
            ("x-amz-id-2", "{host_id}"),
        ];
        let plain = raw_object("GET", &bucket, plain_key);
        assert_shape(
            "GetObject plain in lock bucket shape",
            &plain,
            &shape()
                .status(200)
                .headers(plain_headers)
                .sub("version_id", plain_version.as_str())
                .body("plain-object-lock-body"),
        );
        let plain_head = raw_object("HEAD", &bucket, plain_key);
        assert_shape(
            "HeadObject plain in lock bucket shape",
            &plain_head,
            &shape()
                .status(200)
                .headers(plain_headers)
                .sub("version_id", plain_version.as_str())
                .body_empty(),
        );

        client
            .put_object_legal_hold()
            .bucket(&bucket)
            .key(locked_key)
            .version_id(&locked_version)
            .legal_hold(
                ObjectLockLegalHold::builder()
                    .status(ObjectLockLegalHoldStatus::Off)
                    .build(),
            )
            .send()
            .await
            .expect("clear locked legal hold");
        delete_version_with_bypass(&bucket, locked_key, &locked_version).await;
        delete_version_with_bypass(&bucket, plain_key, &plain_version).await;
        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_put_obj_retention_shorten_default_retention_denied() {
    s3_tests::run(async {
        let bucket = setup_object_lock_bucket().await;
        put_object_lock_configuration(
            &bucket,
            bucket_lock_config_days(ObjectLockRetentionMode::Governance, 7),
        )
        .await;
        let key = "shorten-default-retention";

        // The object's retention comes from the bucket default, not an
        // explicit PutObjectRetention; shortening it must be protected the
        // same way as explicitly-set governance retention.
        let version_id = put_object_bytes(&bucket, key, b"abc").await;
        let shortened_until = future_date(60 * 60)
            .fmt(DateTimeFormat::DateTime)
            .expect("format shortened retain-until");
        let retention_xml = format!(
            "<Retention xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Mode>GOVERNANCE</Mode><RetainUntilDate>{shortened_until}</RetainUntilDate>\
             </Retention>"
        );
        let response = s3_tests::send_signed_request(
            "PUT",
            &format!(
                "{}/{}/{}?retention=&versionId={}",
                CTX.endpoint(),
                bucket,
                key,
                version_id
            ),
            retention_xml.as_bytes(),
            [content_md5_header(retention_xml.as_bytes())],
        );
        assert_shape(
            "PutObjectRetention shorten default retention",
            &response,
            &shape().status(403).headers(error_response_headers()).body(
                expected_error::with_host_id(
                    "AccessDenied",
                    "Access Denied because object protected by object lock.",
                ),
            ),
        );

        delete_version_with_bypass(&bucket, key, &version_id).await;
        cleanup_object_lock_bucket(&bucket).await;
    });
}
