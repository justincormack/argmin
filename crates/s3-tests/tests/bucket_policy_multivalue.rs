use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{Tag, Tagging};
use s3_tests::{
    disable_bucket_public_access_block,
    send_signed_request_to_endpoint_for_service_with_credentials, unique_bucket,
    SignedRequestCredentials, CTX,
};
use serde_json::json;
use std::future::Future;
use std::time::Duration;

async fn cleanup(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = client.delete_object().bucket(bucket).key(*key).send().await;
    }

    for _ in 0..10 {
        match client.delete_bucket().bucket(bucket).send().await {
            Ok(_) => return,
            Err(err) => {
                let raw = format!("{err:?}");
                if raw.contains("OperationAborted") || raw.contains("BucketNotEmpty") {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
                panic!("delete_bucket failed unexpectedly: {raw}");
            }
        }
    }

    client.delete_bucket().bucket(bucket).send().await.unwrap();
}

async fn create_bucket_allowing_policy(client: &aws_sdk_s3::Client) -> String {
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    disable_bucket_public_access_block(client, &bucket).await;
    bucket
}

async fn eventually_ok<T, E, F, Fut>(description: &str, mut op: F) -> T
where
    E: std::fmt::Debug,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    const MAX_ATTEMPTS: usize = 40;

    let mut last_err = None;
    for attempt in 0..MAX_ATTEMPTS {
        match op().await {
            Ok(output) => return output,
            Err(err) => {
                last_err = Some(format!("{err:?}"));
                if attempt + 1 < MAX_ATTEMPTS {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            }
        }
    }

    panic!(
        "{description} failed unexpectedly: {}",
        last_err.unwrap_or_else(|| "no response".to_string())
    );
}

async fn eventually_access_denied<T, E, F, Fut>(description: &str, mut op: F)
where
    E: ProvideErrorMetadata + std::fmt::Debug,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, aws_sdk_s3::error::SdkError<E>>>,
{
    const MAX_ATTEMPTS: usize = 40;

    let mut last_result = None;
    for attempt in 0..MAX_ATTEMPTS {
        let result = op().await;
        match &result {
            Err(err)
                if err.raw_response().map(|r| r.status().as_u16()) == Some(403)
                    && err.as_service_error().and_then(ProvideErrorMetadata::code)
                        == Some("AccessDenied") =>
            {
                return;
            }
            Ok(_) => {
                last_result = Some("Ok".to_string());
                if attempt + 1 < MAX_ATTEMPTS {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            }
            Err(err) => {
                last_result = Some(format!("{err:?}"));
                if attempt + 1 < MAX_ATTEMPTS {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            }
        }
    }

    panic!(
        "{description} did not converge to AccessDenied: {}",
        last_result.unwrap_or_else(|| "no response".to_string())
    );
}

async fn eventually_raw_status(
    description: &str,
    expected_status: u16,
    mut op: impl FnMut() -> s3_tests::RawResponse,
) -> s3_tests::RawResponse {
    const MAX_ATTEMPTS: usize = 40;

    let mut last_response = None;
    for attempt in 0..MAX_ATTEMPTS {
        let response = op();
        if response.status == expected_status {
            return response;
        }
        last_response = Some(response);
        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    panic!("{description} did not converge to status {expected_status}: {last_response:?}");
}

fn tag(key: &str, value: &str) -> Tag {
    Tag::builder().key(key).value(value).build().unwrap()
}

fn tagging(tags: Vec<Tag>) -> Tagging {
    Tagging::builder().set_tag_set(Some(tags)).build().unwrap()
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

fn raw_alt_credentials() -> SignedRequestCredentials<'static> {
    SignedRequestCredentials {
        access_key: CTX.alt_access_key(),
        secret_key: CTX.alt_secret_key(),
        region: CTX.region(),
        tls_ca_pem: CTX.tls_ca_pem(),
    }
}

fn raw_primary_credentials() -> SignedRequestCredentials<'static> {
    SignedRequestCredentials {
        access_key: CTX.access_key(),
        secret_key: CTX.secret_key(),
        region: CTX.region(),
        tls_ca_pem: CTX.tls_ca_pem(),
    }
}

fn percent_encode_path_segment(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(value.len());
    for &b in value.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => {
                out.push('%');
                out.push(HEX[(b >> 4) as usize] as char);
                out.push(HEX[(b & 0x0F) as usize] as char);
            }
        }
    }
    out
}

fn bucket_abac_control_endpoint() -> String {
    if CTX.tls_ca_pem().is_some() || !CTX.endpoint().contains("amazonaws.com") {
        CTX.endpoint().to_string()
    } else {
        format!(
            "https://{}.s3-control.{}.amazonaws.com",
            CTX.account_id(),
            CTX.region()
        )
    }
}

fn bucket_abac_connect_endpoint() -> String {
    if CTX.tls_ca_pem().is_some() || !CTX.endpoint().contains("amazonaws.com") {
        CTX.endpoint().to_string()
    } else {
        bucket_abac_control_endpoint()
    }
}

fn tag_resource_body(tags: &[(&str, &str)]) -> String {
    let mut body = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><TagResourceRequest xmlns=\"http://awss3control.amazonaws.com/doc/2018-08-20/\"><Tags>",
    );
    for (key, value) in tags {
        body.push_str("<Tag><Key>");
        body.push_str(key);
        body.push_str("</Key><Value>");
        body.push_str(value);
        body.push_str("</Value></Tag>");
    }
    body.push_str("</Tags></TagResourceRequest>");
    body
}

fn tag_resource_with_credentials(
    bucket: &str,
    tags: &[(&str, &str)],
    credentials: SignedRequestCredentials<'static>,
) -> s3_tests::RawResponse {
    let endpoint = bucket_abac_control_endpoint();
    let connect_endpoint = bucket_abac_connect_endpoint();
    let resource = percent_encode_path_segment(&bucket_resource(bucket));
    let signed_url = format!("{endpoint}/v20180820/tags/{resource}");
    let connect_url = format!("{connect_endpoint}/v20180820/tags/{resource}");
    send_signed_request_to_endpoint_for_service_with_credentials(
        "POST",
        &connect_url,
        &signed_url,
        tag_resource_body(tags).as_bytes(),
        [("x-amz-account-id", CTX.account_id())],
        "s3",
        credentials,
    )
}

fn untag_resource_with_credentials(
    bucket: &str,
    tag_keys: &[&str],
    credentials: SignedRequestCredentials<'static>,
) -> s3_tests::RawResponse {
    let query = tag_keys
        .iter()
        .map(|key| format!("tagKeys={}", percent_encode_path_segment(key)))
        .collect::<Vec<_>>()
        .join("&");
    untag_resource_query_with_credentials(bucket, Some(&query), credentials)
}

fn untag_resource_query_with_credentials(
    bucket: &str,
    query: Option<&str>,
    credentials: SignedRequestCredentials<'static>,
) -> s3_tests::RawResponse {
    let endpoint = bucket_abac_control_endpoint();
    let connect_endpoint = bucket_abac_connect_endpoint();
    let resource = percent_encode_path_segment(&bucket_resource(bucket));
    let query = query.map_or_else(String::new, |query| format!("?{query}"));
    let signed_url = format!("{endpoint}/v20180820/tags/{resource}{query}");
    let connect_url = format!("{connect_endpoint}/v20180820/tags/{resource}{query}");
    send_signed_request_to_endpoint_for_service_with_credentials(
        "DELETE",
        &connect_url,
        &signed_url,
        &[],
        [("x-amz-account-id", CTX.account_id())],
        "s3",
        credentials,
    )
}

#[test]
fn test_bucket_policy_request_object_tag_keys_on_put_object_inline_tags() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_policy(client).await;
        let allowed_key = "request-object-tag-keys-inline-allowed";
        let denied_key = "request-object-tag-keys-inline-denied";

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": ["s3:PutObject", "s3:PutObjectTagging"],
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "ForAllValues:StringEquals": {
                                "s3:RequestObjectTagKeys": ["security", "team"]
                            },
                            "Null": {
                                "s3:RequestObjectTagKeys": "false"
                            }
                        }
                    }]
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_ok(
            "PutObject with inline tags allowed when every request tag key is in the allow-list",
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key(allowed_key)
                    .tagging("security=allow&team=storage")
                    .body(ByteStream::from_static(b"data"))
                    .send()
            },
        )
        .await;
        eventually_access_denied(
            "PutObject with inline tags denied when any request tag key is outside the allow-list",
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key(denied_key)
                    .tagging("security=allow&project=argmin")
                    .body(ByteStream::from_static(b"data"))
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &[allowed_key, denied_key]).await;
    });
}

#[test]
fn test_bucket_policy_request_object_tag_keys_for_all_values() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_policy(client).await;
        let key = "request-object-tag-keys-for-all";

        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:PutObjectTagging",
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "ForAllValues:StringEquals": {
                                "s3:RequestObjectTagKeys": ["security", "team"]
                            },
                            "Null": {
                                "s3:RequestObjectTagKeys": "false"
                            }
                        }
                    }]
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_ok(
            "PutObjectTagging allowed when every request tag key is in the allow-list",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&bucket)
                    .key(key)
                    .tagging(tagging(vec![tag("security", "allow")]))
                    .send()
            },
        )
        .await;
        eventually_ok(
            "PutObjectTagging allowed when multiple request tag keys are all in the allow-list",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&bucket)
                    .key(key)
                    .tagging(tagging(vec![
                        tag("security", "allow"),
                        tag("team", "storage"),
                    ]))
                    .send()
            },
        )
        .await;

        eventually_access_denied(
            "PutObjectTagging denied when any request tag key is outside the allow-list",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&bucket)
                    .key(key)
                    .tagging(tagging(vec![
                        tag("security", "allow"),
                        tag("project", "argmin"),
                    ]))
                    .send()
            },
        )
        .await;
        eventually_access_denied(
            "PutObjectTagging denied for empty tag set when Null:false is required",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&bucket)
                    .key(key)
                    .tagging(tagging(Vec::new()))
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_request_object_tag_keys_for_all_values_empty_tag_set_allowed_without_null_guard(
) {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_policy(client).await;
        let key = "request-object-tag-keys-for-all-empty";

        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:PutObjectTagging",
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "ForAllValues:StringEquals": {
                                "s3:RequestObjectTagKeys": ["security", "team"]
                            }
                        }
                    }]
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_ok(
            "PutObjectTagging allowed when policy without Null:false guard has converged",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&bucket)
                    .key(key)
                    .tagging(tagging(vec![tag("security", "allow")]))
                    .send()
            },
        )
        .await;
        eventually_ok(
            "PutObjectTagging allowed for empty tag set when ForAllValues has no Null:false guard",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&bucket)
                    .key(key)
                    .tagging(tagging(Vec::new()))
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_request_object_tag_keys_for_any_value() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_policy(client).await;
        let key = "request-object-tag-keys-for-any";

        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:PutObjectTagging",
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "ForAnyValue:StringEquals": {
                                "s3:RequestObjectTagKeys": ["security"]
                            }
                        }
                    }]
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_ok(
            "PutObjectTagging allowed when at least one request tag key is in the allow-list",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&bucket)
                    .key(key)
                    .tagging(tagging(vec![
                        tag("security", "allow"),
                        tag("project", "argmin"),
                    ]))
                    .send()
            },
        )
        .await;

        eventually_access_denied(
            "PutObjectTagging denied when no request tag key is in the allow-list",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&bucket)
                    .key(key)
                    .tagging(tagging(vec![tag("project", "argmin")]))
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_request_object_tag_keys_for_all_values_string_equals_ignore_case() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_policy(client).await;
        let key = "request-object-tag-keys-for-all-ignore-case";

        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:PutObjectTagging",
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "ForAllValues:StringEqualsIgnoreCase": {
                                "s3:RequestObjectTagKeys": ["security", "team"]
                            },
                            "Null": {
                                "s3:RequestObjectTagKeys": "false"
                            }
                        }
                    }]
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_ok(
            "PutObjectTagging allowed when every request tag key matches ignoring case",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&bucket)
                    .key(key)
                    .tagging(tagging(vec![
                        tag("SECURITY", "allow"),
                        tag("TEAM", "storage"),
                    ]))
                    .send()
            },
        )
        .await;
        eventually_access_denied(
            "PutObjectTagging denied when any request tag key does not match ignoring case",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&bucket)
                    .key(key)
                    .tagging(tagging(vec![
                        tag("SECURITY", "allow"),
                        tag("project", "argmin"),
                    ]))
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_request_object_tag_keys_for_any_value_string_equals_ignore_case() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_policy(client).await;
        let key = "request-object-tag-keys-for-any-ignore-case";

        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:PutObjectTagging",
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "ForAnyValue:StringEqualsIgnoreCase": {
                                "s3:RequestObjectTagKeys": ["SECURITY"]
                            }
                        }
                    }]
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_ok(
            "PutObjectTagging allowed when at least one request tag key matches ignoring case",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&bucket)
                    .key(key)
                    .tagging(tagging(vec![
                        tag("security", "allow"),
                        tag("project", "argmin"),
                    ]))
                    .send()
            },
        )
        .await;
        eventually_access_denied(
            "PutObjectTagging denied when no request tag key matches ignoring case",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&bucket)
                    .key(key)
                    .tagging(tagging(vec![tag("project", "argmin")]))
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_request_object_tag_keys_for_all_values_string_not_equals_ignore_case() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_policy(client).await;
        let key = "request-object-tag-keys-for-all-not-ignore-case";

        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:PutObjectTagging",
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "ForAllValues:StringNotEqualsIgnoreCase": {
                                "s3:RequestObjectTagKeys": ["security"]
                            },
                            "Null": {
                                "s3:RequestObjectTagKeys": "false"
                            }
                        }
                    }]
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_ok(
            "PutObjectTagging allowed when every request tag key differs ignoring case",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&bucket)
                    .key(key)
                    .tagging(tagging(vec![
                        tag("project", "argmin"),
                        tag("team", "storage"),
                    ]))
                    .send()
            },
        )
        .await;
        eventually_access_denied(
            "PutObjectTagging denied when any request tag key equals ignoring case",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&bucket)
                    .key(key)
                    .tagging(tagging(vec![
                        tag("project", "argmin"),
                        tag("SECURITY", "allow"),
                    ]))
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_request_object_tag_keys_for_any_value_string_not_equals_ignore_case() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_policy(client).await;
        let key = "request-object-tag-keys-for-any-not-ignore-case";

        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:PutObjectTagging",
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "ForAnyValue:StringNotEqualsIgnoreCase": {
                                "s3:RequestObjectTagKeys": ["security"]
                            }
                        }
                    }]
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_ok(
            "PutObjectTagging allowed when at least one request tag key differs ignoring case",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&bucket)
                    .key(key)
                    .tagging(tagging(vec![
                        tag("security", "allow"),
                        tag("project", "argmin"),
                    ]))
                    .send()
            },
        )
        .await;
        eventually_access_denied(
            "PutObjectTagging denied when every request tag key equals ignoring case",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&bucket)
                    .key(key)
                    .tagging(tagging(vec![tag("SECURITY", "allow")]))
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_request_object_tag_keys_for_all_values_string_like() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_policy(client).await;
        let key = "request-object-tag-keys-for-all-like";

        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:PutObjectTagging",
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "ForAllValues:StringLike": {
                                "s3:RequestObjectTagKeys": ["sec*", "team"]
                            },
                            "Null": {
                                "s3:RequestObjectTagKeys": "false"
                            }
                        }
                    }]
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_ok(
            "PutObjectTagging allowed when every request tag key matches a wildcard",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&bucket)
                    .key(key)
                    .tagging(tagging(vec![
                        tag("security", "allow"),
                        tag("team", "storage"),
                    ]))
                    .send()
            },
        )
        .await;
        eventually_access_denied(
            "PutObjectTagging denied when any request tag key misses every wildcard",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&bucket)
                    .key(key)
                    .tagging(tagging(vec![
                        tag("security", "allow"),
                        tag("project", "argmin"),
                    ]))
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_request_object_tag_keys_for_any_value_string_like() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_policy(client).await;
        let key = "request-object-tag-keys-for-any-like";

        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:PutObjectTagging",
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "ForAnyValue:StringLike": {
                                "s3:RequestObjectTagKeys": ["sec*"]
                            }
                        }
                    }]
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_ok(
            "PutObjectTagging allowed when at least one request tag key matches a wildcard",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&bucket)
                    .key(key)
                    .tagging(tagging(vec![
                        tag("security", "allow"),
                        tag("project", "argmin"),
                    ]))
                    .send()
            },
        )
        .await;
        eventually_access_denied(
            "PutObjectTagging denied when no request tag key matches a wildcard",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&bucket)
                    .key(key)
                    .tagging(tagging(vec![tag("project", "argmin")]))
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_request_object_tag_keys_for_all_values_string_not_like() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_policy(client).await;
        let key = "request-object-tag-keys-for-all-not-like";

        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:PutObjectTagging",
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "ForAllValues:StringNotLike": {
                                "s3:RequestObjectTagKeys": ["sec*"]
                            },
                            "Null": {
                                "s3:RequestObjectTagKeys": "false"
                            }
                        }
                    }]
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_ok(
            "PutObjectTagging allowed when every request tag key misses the wildcard",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&bucket)
                    .key(key)
                    .tagging(tagging(vec![
                        tag("project", "argmin"),
                        tag("team", "storage"),
                    ]))
                    .send()
            },
        )
        .await;
        eventually_access_denied(
            "PutObjectTagging denied when any request tag key matches the wildcard",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&bucket)
                    .key(key)
                    .tagging(tagging(vec![
                        tag("project", "argmin"),
                        tag("security", "allow"),
                    ]))
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_request_object_tag_keys_for_any_value_string_not_like() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_policy(client).await;
        let key = "request-object-tag-keys-for-any-not-like";

        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:PutObjectTagging",
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "ForAnyValue:StringNotLike": {
                                "s3:RequestObjectTagKeys": ["sec*"]
                            }
                        }
                    }]
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_ok(
            "PutObjectTagging allowed when at least one request tag key misses the wildcard",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&bucket)
                    .key(key)
                    .tagging(tagging(vec![
                        tag("security", "allow"),
                        tag("project", "argmin"),
                    ]))
                    .send()
            },
        )
        .await;
        eventually_access_denied(
            "PutObjectTagging denied when every request tag key matches the wildcard",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&bucket)
                    .key(key)
                    .tagging(tagging(vec![tag("security", "allow")]))
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_tag_resource_tag_keys_for_all_values() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_policy(client).await;

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:TagResource",
                        "Resource": bucket_resource(&bucket),
                        "Condition": {
                            "ForAllValues:StringEquals": {
                                "aws:TagKeys": ["security", "team"]
                            },
                            "Null": {
                                "aws:TagKeys": "false"
                            }
                        }
                    }]
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_raw_status(
            "TagResource allowed when every request tag key is in the allow-list",
            204,
            || {
                tag_resource_with_credentials(
                    &bucket,
                    &[("security", "allow"), ("team", "storage")],
                    raw_alt_credentials(),
                )
            },
        )
        .await;

        let denied = eventually_raw_status(
            "TagResource denied when any request tag key is outside the allow-list",
            403,
            || {
                tag_resource_with_credentials(
                    &bucket,
                    &[("security", "allow"), ("project", "argmin")],
                    raw_alt_credentials(),
                )
            },
        )
        .await;
        assert!(
            denied.body.contains("<Code>AccessDenied</Code>"),
            "unexpected denied response: {denied:?}"
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_tag_resource_tag_keys_for_any_value() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_policy(client).await;

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:TagResource",
                        "Resource": bucket_resource(&bucket),
                        "Condition": {
                            "ForAnyValue:StringEquals": {
                                "aws:TagKeys": ["security"]
                            }
                        }
                    }]
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_raw_status(
            "TagResource allowed when at least one request tag key is in the allow-list",
            204,
            || {
                tag_resource_with_credentials(
                    &bucket,
                    &[("security", "allow"), ("project", "argmin")],
                    raw_alt_credentials(),
                )
            },
        )
        .await;

        let denied = eventually_raw_status(
            "TagResource denied when no request tag key is in the allow-list",
            403,
            || {
                tag_resource_with_credentials(
                    &bucket,
                    &[("project", "argmin")],
                    raw_alt_credentials(),
                )
            },
        )
        .await;
        assert!(
            denied.body.contains("<Code>AccessDenied</Code>"),
            "unexpected denied response: {denied:?}"
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_tag_resource_request_tag_condition() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_policy(client).await;

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:TagResource",
                        "Resource": bucket_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "aws:RequestTag/security": "allow"
                            }
                        }
                    }]
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_raw_status(
            "TagResource allowed when aws:RequestTag value matches",
            204,
            || {
                tag_resource_with_credentials(
                    &bucket,
                    &[("security", "allow")],
                    raw_alt_credentials(),
                )
            },
        )
        .await;
        let denied = eventually_raw_status(
            "TagResource denied when aws:RequestTag value does not match",
            403,
            || {
                tag_resource_with_credentials(
                    &bucket,
                    &[("security", "deny")],
                    raw_alt_credentials(),
                )
            },
        )
        .await;
        assert!(
            denied.body.contains("<Code>AccessDenied</Code>"),
            "unexpected denied response: {denied:?}"
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_untag_resource_is_not_authorized_by_tag_resource() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_policy(client).await;

        client
            .put_bucket_tagging()
            .bucket(&bucket)
            .tagging(tagging(vec![
                tag("security", "allow"),
                tag("team", "storage"),
            ]))
            .send()
            .await
            .unwrap();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:TagResource",
                        "Resource": bucket_resource(&bucket)
                    }]
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_raw_status(
            "TagResource allowed once TagResource-only policy has converged",
            204,
            || {
                tag_resource_with_credentials(
                    &bucket,
                    &[("security", "allow")],
                    raw_alt_credentials(),
                )
            },
        )
        .await;
        let denied = eventually_raw_status(
            "UntagResource denied when policy only allows TagResource",
            403,
            || untag_resource_with_credentials(&bucket, &["security"], raw_alt_credentials()),
        )
        .await;
        assert!(
            denied.body.contains("<Code>AccessDenied</Code>"),
            "unexpected denied response: {denied:?}"
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_untag_resource_tag_keys_for_all_values_partial_removal() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_policy(client).await;

        client
            .put_bucket_tagging()
            .bucket(&bucket)
            .tagging(tagging(vec![
                tag("security", "allow"),
                tag("team", "storage"),
            ]))
            .send()
            .await
            .unwrap();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:UntagResource",
                        "Resource": bucket_resource(&bucket),
                        "Condition": {
                            "ForAllValues:StringEquals": {
                                "aws:TagKeys": ["security"]
                            },
                            "Null": {
                                "aws:TagKeys": "false"
                            }
                        }
                    }]
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_raw_status(
            "UntagResource allowed for partial removal when every request tag key is in the allow-list",
            204,
            || untag_resource_with_credentials(&bucket, &["security"], raw_alt_credentials()),
        )
        .await;
        let denied = eventually_raw_status(
            "UntagResource denied for partial removal when any request tag key is outside the allow-list",
            403,
            || untag_resource_with_credentials(&bucket, &["team"], raw_alt_credentials()),
        )
        .await;
        assert!(
            denied.body.contains("<Code>AccessDenied</Code>"),
            "unexpected denied response: {denied:?}"
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_untag_resource_tag_keys_for_all_values_full_removal() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_policy(client).await;

        client
            .put_bucket_tagging()
            .bucket(&bucket)
            .tagging(tagging(vec![tag("security", "allow")]))
            .send()
            .await
            .unwrap();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:UntagResource",
                        "Resource": bucket_resource(&bucket),
                        "Condition": {
                            "ForAllValues:StringEquals": {
                                "aws:TagKeys": ["security"]
                            },
                            "Null": {
                                "aws:TagKeys": "false"
                            }
                        }
                    }]
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_raw_status(
            "UntagResource allowed for full removal when every request tag key is in the allow-list",
            204,
            || untag_resource_with_credentials(&bucket, &["security"], raw_alt_credentials()),
        )
        .await;

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_rejects_request_object_tag_condition_on_tag_resource() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_policy(client).await;

        let err = client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:TagResource",
                        "Resource": bucket_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:RequestObjectTag/security": "allow"
                            }
                        }
                    }]
                })
                .to_string(),
            )
            .send()
            .await
            .expect_err("TagResource policy with s3:RequestObjectTag should be rejected");
        assert_eq!(err.raw_response().map(|r| r.status().as_u16()), Some(400));
        assert_eq!(
            err.as_service_error().and_then(ProvideErrorMetadata::code),
            Some("MalformedPolicy")
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_rejects_request_object_tag_keys_condition_on_tag_resource() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_policy(client).await;

        let err = client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:TagResource",
                        "Resource": bucket_resource(&bucket),
                        "Condition": {
                            "ForAnyValue:StringEquals": {
                                "s3:RequestObjectTagKeys": ["security"]
                            }
                        }
                    }]
                })
                .to_string(),
            )
            .send()
            .await
            .expect_err("TagResource policy with s3:RequestObjectTagKeys should be rejected");
        assert_eq!(err.raw_response().map(|r| r.status().as_u16()), Some(400));
        assert_eq!(
            err.as_service_error().and_then(ProvideErrorMetadata::code),
            Some("MalformedPolicy")
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_rejects_request_object_tag_condition_on_untag_resource() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_policy(client).await;

        let err = client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:UntagResource",
                        "Resource": bucket_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:RequestObjectTag/security": "allow"
                            }
                        }
                    }]
                })
                .to_string(),
            )
            .send()
            .await
            .expect_err("UntagResource policy with s3:RequestObjectTag should be rejected");
        assert_eq!(err.raw_response().map(|r| r.status().as_u16()), Some(400));
        assert_eq!(
            err.as_service_error().and_then(ProvideErrorMetadata::code),
            Some("MalformedPolicy")
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_request_tag_condition_on_untag_resource_is_accepted_but_does_not_match_value()
{
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_policy(client).await;

        client
            .put_bucket_tagging()
            .bucket(&bucket)
            .tagging(tagging(vec![
                tag("security", "allow"),
                tag("team", "storage"),
            ]))
            .send()
            .await
            .unwrap();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": alt_policy_principal(),
                            "Action": "s3:UntagResource",
                            "Resource": bucket_resource(&bucket)
                        },
                        {
                            "Effect": "Deny",
                            "Principal": alt_policy_principal(),
                            "Action": "s3:UntagResource",
                            "Resource": bucket_resource(&bucket),
                            "Condition": {
                                "StringEquals": {
                                    "aws:RequestTag/security": "allow"
                                }
                            }
                        }
                    ]
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_raw_status(
            "UntagResource allowed because aws:RequestTag value condition does not match untag tagKeys",
            204,
            || untag_resource_with_credentials(&bucket, &["security"], raw_alt_credentials()),
        )
        .await;

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_request_tag_condition_on_untag_resource_matches_empty_value() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_policy(client).await;

        client
            .put_bucket_tagging()
            .bucket(&bucket)
            .tagging(tagging(vec![tag("security", "allow")]))
            .send()
            .await
            .unwrap();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": alt_policy_principal(),
                            "Action": "s3:UntagResource",
                            "Resource": bucket_resource(&bucket)
                        },
                        {
                            "Effect": "Deny",
                            "Principal": alt_policy_principal(),
                            "Action": "s3:UntagResource",
                            "Resource": bucket_resource(&bucket),
                            "Condition": {
                                "StringEquals": {
                                    "aws:RequestTag/security": ""
                                }
                            }
                        }
                    ]
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let denied = eventually_raw_status(
            "UntagResource denied because aws:RequestTag matches an empty value for untag tagKeys",
            403,
            || untag_resource_with_credentials(&bucket, &["security"], raw_alt_credentials()),
        )
        .await;
        assert!(
            denied.body.contains("<Code>AccessDenied</Code>"),
            "unexpected denied response: {denied:?}"
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_rejects_request_object_tag_keys_condition_on_untag_resource() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_policy(client).await;

        let err = client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:UntagResource",
                        "Resource": bucket_resource(&bucket),
                        "Condition": {
                            "ForAnyValue:StringEquals": {
                                "s3:RequestObjectTagKeys": ["security"]
                            }
                        }
                    }]
                })
                .to_string(),
            )
            .send()
            .await
            .expect_err("UntagResource policy with s3:RequestObjectTagKeys should be rejected");
        assert_eq!(err.raw_response().map(|r| r.status().as_u16()), Some(400));
        assert_eq!(
            err.as_service_error().and_then(ProvideErrorMetadata::code),
            Some("MalformedPolicy")
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_request_object_tag_keys_string_equals_is_accepted_but_does_not_match() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_policy(client).await;
        let key = "request-object-tag-keys-string-equals";

        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": alt_policy_principal(),
                            "Action": "s3:PutObjectTagging",
                            "Resource": bucket_wildcard_resource(&bucket)
                        },
                        {
                            "Effect": "Deny",
                            "Principal": alt_policy_principal(),
                            "Action": "s3:PutObjectTagging",
                            "Resource": bucket_wildcard_resource(&bucket),
                            "Condition": {
                                "StringEquals": {
                                    "s3:RequestObjectTagKeys": "security"
                                }
                            }
                        }
                    ]
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_ok(
            "PutObjectTagging allowed because scalar StringEquals does not match s3:RequestObjectTagKeys",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&bucket)
                    .key(key)
                    .tagging(tagging(vec![tag("security", "allow")]))
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &[key]).await;

        let bucket = create_bucket_allowing_policy(client).await;
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": alt_policy_principal(),
                            "Action": "s3:PutObjectTagging",
                            "Resource": bucket_wildcard_resource(&bucket)
                        },
                        {
                            "Effect": "Deny",
                            "Principal": alt_policy_principal(),
                            "Action": "s3:PutObjectTagging",
                            "Resource": bucket_wildcard_resource(&bucket),
                            "Condition": {
                                "StringEquals": {
                                    "s3:RequestObjectTagKeys": ["security", "team"]
                                }
                            }
                        }
                    ]
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_ok(
            "PutObjectTagging allowed because StringEquals list does not match s3:RequestObjectTagKeys",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&bucket)
                    .key(key)
                    .tagging(tagging(vec![
                        tag("security", "allow"),
                        tag("team", "storage"),
                    ]))
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_tag_keys_string_equals_is_accepted_but_does_not_match() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_policy(client).await;

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": alt_policy_principal(),
                            "Action": "s3:TagResource",
                            "Resource": bucket_resource(&bucket)
                        },
                        {
                            "Effect": "Deny",
                            "Principal": alt_policy_principal(),
                            "Action": "s3:TagResource",
                            "Resource": bucket_resource(&bucket),
                            "Condition": {
                                "StringEquals": {
                                    "aws:TagKeys": "security"
                                }
                            }
                        }
                    ]
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_raw_status(
            "TagResource allowed because scalar StringEquals does not match aws:TagKeys",
            204,
            || {
                tag_resource_with_credentials(
                    &bucket,
                    &[("security", "allow")],
                    raw_alt_credentials(),
                )
            },
        )
        .await;

        cleanup(&bucket, &[]).await;

        let bucket = create_bucket_allowing_policy(client).await;

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": alt_policy_principal(),
                            "Action": "s3:TagResource",
                            "Resource": bucket_resource(&bucket)
                        },
                        {
                            "Effect": "Deny",
                            "Principal": alt_policy_principal(),
                            "Action": "s3:TagResource",
                            "Resource": bucket_resource(&bucket),
                            "Condition": {
                                "StringEquals": {
                                    "aws:TagKeys": ["security", "team"]
                                }
                            }
                        }
                    ]
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_raw_status(
            "TagResource allowed because StringEquals list does not match aws:TagKeys",
            204,
            || {
                tag_resource_with_credentials(
                    &bucket,
                    &[("security", "allow"), ("team", "storage")],
                    raw_alt_credentials(),
                )
            },
        )
        .await;

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_untag_resource_rejects_missing_empty_and_invalid_tag_keys() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_policy(client).await;

        let missing =
            untag_resource_query_with_credentials(&bucket, None, raw_primary_credentials());
        assert_eq!(
            missing.status, 400,
            "unexpected missing tagKeys response: {missing:?}"
        );
        assert!(
            missing.body.contains("<Code>InvalidTag</Code>"),
            "unexpected missing tagKeys response: {missing:?}"
        );

        let empty = untag_resource_query_with_credentials(
            &bucket,
            Some("tagKeys="),
            raw_primary_credentials(),
        );
        assert_eq!(
            empty.status, 400,
            "unexpected empty tagKeys response: {empty:?}"
        );
        assert!(
            empty.body.contains("<Code>InvalidTag</Code>"),
            "unexpected empty tagKeys response: {empty:?}"
        );

        let long_key = "x".repeat(129);
        let long_query = format!("tagKeys={long_key}");
        let overlong = untag_resource_query_with_credentials(
            &bucket,
            Some(&long_query),
            raw_primary_credentials(),
        );
        assert_eq!(
            overlong.status, 204,
            "unexpected overlong tagKeys response: {overlong:?}"
        );

        let too_many_query = (0..51)
            .map(|idx| format!("tagKeys=k{idx}"))
            .collect::<Vec<_>>()
            .join("&");
        let too_many = untag_resource_query_with_credentials(
            &bucket,
            Some(&too_many_query),
            raw_primary_credentials(),
        );
        let matches_aws_bug =
            too_many.status == 500 && too_many.body.contains("<Code>InternalError</Code>");
        let matches_argmin =
            too_many.status == 400 && too_many.body.contains("<Code>InvalidTag</Code>");
        assert!(
            matches_aws_bug || matches_argmin,
            "expected AWS 500/InternalError or Argmin 400/InvalidTag for too many tagKeys, got {too_many:?}"
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_tag_resource_unauthorized_request_does_not_validate_merged_hidden_tags() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_policy(client).await;

        let existing_tags = (0..49)
            .map(|idx| tag(&format!("existing-{idx}"), "value"))
            .collect::<Vec<_>>();
        client
            .put_bucket_tagging()
            .bucket(&bucket)
            .tagging(tagging(existing_tags))
            .send()
            .await
            .unwrap();

        let new_tags = [
            ("new-tag-0".to_string(), "value".to_string()),
            ("new-tag-1".to_string(), "value".to_string()),
        ];
        let new_tag_refs = new_tags
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect::<Vec<_>>();
        let denied = eventually_raw_status(
            "Unauthorized TagResource should deny before validating the hidden merged tag set",
            403,
            || tag_resource_with_credentials(&bucket, &new_tag_refs, raw_alt_credentials()),
        )
        .await;
        assert!(
            denied.body.contains("<Code>AccessDenied</Code>"),
            "unexpected denied response: {denied:?}"
        );
        assert!(
            !denied.body.contains("<Code>InvalidTag</Code>"),
            "unauthorized TagResource leaked merged tag validation: {denied:?}"
        );

        cleanup(&bucket, &[]).await;
    });
}
