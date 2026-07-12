use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    AccessControlPolicy, BlockedEncryptionTypes, BucketCannedAcl, BucketLifecycleConfiguration,
    BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, CorsConfiguration, CorsRule,
    DefaultRetention, EncryptionType, ExpirationStatus, Grant, Grantee, LifecycleExpiration,
    LifecycleRule, LifecycleRuleFilter, ObjectAttributes, ObjectCannedAcl, ObjectLockConfiguration,
    ObjectLockEnabled, ObjectLockRetentionMode, ObjectLockRule, ObjectOwnership, Owner,
    OwnershipControls, OwnershipControlsRule, Permission, PublicAccessBlockConfiguration,
    ServerSideEncryption, ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration,
    ServerSideEncryptionRule, Tag, Tagging, Type, VersioningConfiguration,
};
use s3_tests::{
    assert_s3_err_code, cleanup_versioned_bucket, create_public_bucket,
    disable_bucket_public_access_block, err_status, object_url,
    post_object_raw_to_test_endpoint_with_headers, presign_url_with_credentials,
    put_bucket_lifecycle_with_md5, raw_alt_object_request, raw_anonymous, raw_bucket,
    raw_fetch_url, send_signed_request, send_signed_request_with_credentials,
    shape::{
        assert_shape, error_response_headers, escape_literal, expected_error, id_headers, shape,
    },
    sigv4_post_fields_for_credentials, sigv4_post_fields_for_credentials_at_epoch,
    sse_c_header_values, test_sse_c_key, unique_bucket, RawAltObjectRequest, RawResponse,
    SendRetryingOperationAborted, SignedRequestCredentials, CTX,
};
use serde_json::json;
use std::future::Future;

fn agent() -> s3_tests::Agent {
    s3_tests::test_agent()
}

fn tag(key: &str, value: &str) -> Tag {
    Tag::builder().key(key).value(value).build().unwrap()
}

fn tagging(tags: Vec<Tag>) -> Tagging {
    Tagging::builder().set_tag_set(Some(tags)).build().unwrap()
}

fn anonymous_get_status_and_body(url: &str) -> (u16, String) {
    let mut resp = agent().get(url).call().expect("transport error");
    let status = resp.status().as_u16();
    let body = resp.body_mut().read_to_string().unwrap_or_default();
    (status, body)
}

fn endpoint_is_https() -> bool {
    CTX.endpoint().starts_with("https://")
}

fn require_https_endpoint() {
    assert!(
        endpoint_is_https(),
        "bucket policy SSE-C coverage requires an https:// endpoint; got {}",
        CTX.endpoint()
    );
}

fn percent_encode_first_byte(value: &str) -> String {
    let (first, rest) = value
        .as_bytes()
        .split_first()
        .expect("value must not be empty");
    let mut encoded = format!("%{:02X}", *first);
    encoded.extend(url::form_urlencoded::byte_serialize(rest));
    encoded
}

fn copy_part_etag_from_body(body: &str) -> &str {
    body.split_once("<ETag>")
        .and_then(|(_, rest)| rest.split_once("</ETag>"))
        .map(|(etag, _)| etag)
        .unwrap_or_else(|| panic!("UploadPartCopy response should include ETag: {body}"))
}

macro_rules! with_sse_c_headers {
    ($op:expr, $algorithm:expr, $key_b64:expr, $key_md5_b64:expr) => {{
        $op.customize().mutate_request({
            let algorithm = $algorithm.to_string();
            let key_b64 = $key_b64.clone();
            let key_md5_b64 = $key_md5_b64.clone();
            move |req| {
                req.headers_mut().insert(
                    "x-amz-server-side-encryption-customer-algorithm",
                    algorithm.clone(),
                );
                req.headers_mut()
                    .insert("x-amz-server-side-encryption-customer-key", key_b64.clone());
                req.headers_mut().insert(
                    "x-amz-server-side-encryption-customer-key-md5",
                    key_md5_b64.clone(),
                );
            }
        })
    }};
}

macro_rules! with_sse_s3_header {
    ($op:expr) => {{
        $op.customize().mutate_request(move |req| {
            req.headers_mut()
                .insert("x-amz-server-side-encryption", "AES256");
        })
    }};
}

async fn cleanup_with_client(client: &aws_sdk_s3::Client, bucket: &str, keys: &[&str]) {
    for key in keys {
        let _ = s3_tests::delete_object_retrying_operation_aborted(client, bucket, key).await;
    }

    for _ in 0..10 {
        let uploads = match client
            .list_multipart_uploads()
            .bucket(bucket)
            .send_retrying_operation_aborted("list multipart uploads during bucket policy cleanup")
            .await
        {
            Ok(uploads) => uploads,
            Err(err)
                if err.as_service_error().and_then(ProvideErrorMetadata::code)
                    == Some("NoSuchBucket") =>
            {
                return;
            }
            Err(err) => panic!("list multipart uploads during bucket policy cleanup: {err:?}"),
        };
        for upload in uploads.uploads() {
            let _ = client
                .abort_multipart_upload()
                .bucket(bucket)
                .key(upload.key().unwrap())
                .upload_id(upload.upload_id().unwrap())
                .send_retrying_operation_aborted(
                    "abort multipart upload during bucket policy cleanup",
                )
                .await;
        }

        match client
            .delete_bucket()
            .bucket(bucket)
            .send_retrying_operation_aborted("delete bucket policy test bucket")
            .await
        {
            Ok(_) => return,
            Err(err) => {
                if err.as_service_error().and_then(ProvideErrorMetadata::code)
                    == Some("NoSuchBucket")
                {
                    return;
                }
                let raw = format!("{err:?}");
                if raw.contains("OperationAborted") || raw.contains("BucketNotEmpty") {
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    continue;
                }
                panic!("delete_bucket failed unexpectedly: {raw}");
            }
        }
    }

    s3_tests::delete_bucket_retrying_operation_aborted(client, bucket).await;
}

async fn cleanup(bucket: &str, keys: &[&str]) {
    cleanup_with_client(CTX.client(), bucket, keys).await;
}

async fn alt_list_objects_v1_eventually(
    bucket: &str,
) -> aws_sdk_s3::operation::list_objects::ListObjectsOutput {
    const MAX_ATTEMPTS: usize = 20;

    for attempt in 0..MAX_ATTEMPTS {
        match CTX
            .alt_client()
            .list_objects()
            .bucket(bucket)
            .send_retrying_operation_aborted("list objects as alternate account")
            .await
        {
            Ok(output) => return output,
            Err(_) if attempt + 1 < MAX_ATTEMPTS => {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            Err(err) => panic!("alternate ListObjects failed unexpectedly: {err:?}"),
        }
    }

    unreachable!()
}

async fn alt_list_objects_v2_eventually(
    bucket: &str,
) -> aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output {
    const MAX_ATTEMPTS: usize = 20;

    for attempt in 0..MAX_ATTEMPTS {
        match CTX
            .alt_client()
            .list_objects_v2()
            .bucket(bucket)
            .send_retrying_operation_aborted("list objects v2 as alternate account")
            .await
        {
            Ok(output) => return output,
            Err(_) if attempt + 1 < MAX_ATTEMPTS => {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            Err(err) => panic!("alternate ListObjectsV2 failed unexpectedly: {err:?}"),
        }
    }

    unreachable!()
}

async fn anonymous_list_bucket_access_denied_eventually(url: &str) {
    const MAX_ATTEMPTS: usize = 20;

    for attempt in 0..MAX_ATTEMPTS {
        let (status, body) = anonymous_get_status_and_body(url);
        if status == 403 && body.contains("<Code>AccessDenied</Code>") {
            return;
        }
        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            continue;
        }
        panic!(
            "anonymous bucket listing did not converge to AccessDenied for {url}, last status {status}, last body {body}"
        );
    }

    unreachable!()
}

async fn anonymous_get_object_status_eventually(url: &str, expected_status: u16) -> String {
    const MAX_ATTEMPTS: usize = 20;

    for attempt in 0..MAX_ATTEMPTS {
        let (status, body) = anonymous_get_status_and_body(url);
        if status == expected_status {
            return body;
        }
        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            continue;
        }
        panic!(
            "anonymous GET did not converge to status {expected_status} for {url}, last status {status}, last body {body}"
        );
    }

    unreachable!()
}

async fn upload_part_copy_eventually(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    copy_source: String,
) -> aws_sdk_s3::operation::upload_part_copy::UploadPartCopyOutput {
    const MAX_ATTEMPTS: usize = 20;

    for attempt in 0..MAX_ATTEMPTS {
        match client
            .upload_part_copy()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .copy_source(copy_source.clone())
            .send_retrying_operation_aborted("upload part copy in bucket policy test")
            .await
        {
            Ok(output) => return output,
            Err(_) if attempt + 1 < MAX_ATTEMPTS => {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            Err(err) => panic!("UploadPartCopy failed unexpectedly: {err:?}"),
        }
    }

    unreachable!()
}

async fn eventually_ok<T, E, F, Fut>(description: &str, mut op: F) -> T
where
    E: std::fmt::Debug,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    eventually_ok_with_retry(
        description,
        20,
        std::time::Duration::from_millis(200),
        &mut op,
    )
    .await
}

async fn eventually_ok_with_retry<T, E, F, Fut>(
    description: &str,
    max_attempts: usize,
    delay: std::time::Duration,
    mut op: F,
) -> T
where
    E: std::fmt::Debug,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    for attempt in 0..max_attempts {
        match op().await {
            Ok(output) => return output,
            Err(_) if attempt + 1 < max_attempts => {
                tokio::time::sleep(delay).await;
            }
            Err(err) => panic!("{description} failed unexpectedly: {err:?}"),
        }
    }

    unreachable!()
}

async fn eventually_result_matches<T, E, F, Fut, P>(
    description: &str,
    max_attempts: usize,
    delay: std::time::Duration,
    mut op: F,
    mut predicate: P,
) where
    E: std::fmt::Debug,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    P: FnMut(&Result<T, E>) -> bool,
{
    for attempt in 0..max_attempts {
        let result = op().await;
        if predicate(&result) {
            return;
        }
        if attempt + 1 < max_attempts {
            tokio::time::sleep(delay).await;
            continue;
        }
        let last_result = match &result {
            Ok(_) => "Ok".to_string(),
            Err(err) => format!("Err({err:?})"),
        };
        panic!("{description} did not converge: {}", last_result);
    }

    unreachable!()
}

async fn eventually_access_denied<T, E, F, Fut>(description: &str, mut op: F)
where
    E: std::fmt::Debug,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, aws_sdk_s3::error::SdkError<E>>>,
{
    eventually_result_matches(
        description,
        20,
        std::time::Duration::from_millis(200),
        &mut op,
        |result| {
            result
                .as_ref()
                .err()
                .and_then(|err| err.raw_response().map(|r| r.status().as_u16()))
                == Some(403)
        },
    )
    .await;
}

async fn eventually_raw_bucket_location(
    description: &str,
    bucket: &str,
    credentials: SignedRequestCredentials<'_>,
) -> s3_tests::RawResponse {
    let url = s3_tests::bucket_location_url(CTX.endpoint(), bucket);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        let response = send_signed_request_with_credentials(
            "GET",
            &url,
            b"",
            std::iter::empty::<(&str, &str)>(),
            credentials,
        );
        if response.status == 200 {
            return response;
        }
        if response.status == 403 && std::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            continue;
        }
        panic!("{description} failed unexpectedly: {response:?}");
    }
}

async fn get_object_eventually(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
) -> aws_sdk_s3::operation::get_object::GetObjectOutput {
    eventually_ok("GetObject", || {
        client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send_retrying_operation_aborted("get object in bucket policy test")
    })
    .await
}

async fn get_bucket_policy_status_eventually(
    client: &aws_sdk_s3::Client,
    bucket: &str,
) -> aws_sdk_s3::operation::get_bucket_policy_status::GetBucketPolicyStatusOutput {
    eventually_ok("GetBucketPolicyStatus", || {
        client
            .get_bucket_policy_status()
            .bucket(bucket)
            .send_retrying_operation_aborted("get bucket policy status")
    })
    .await
}

async fn alt_get_bucket_policy_status_access_denied_eventually(bucket: &str) {
    // This waits for a policy transition from allow to explicit deny on the
    // same action. On AWS the old allow decision can remain visible for longer
    // than the simpler allow-propagation cases in this file.
    const MAX_ATTEMPTS: usize = 60;

    for attempt in 0..MAX_ATTEMPTS {
        let result = CTX
            .alt_client()
            .get_bucket_policy_status()
            .bucket(bucket)
            .send_retrying_operation_aborted("get bucket policy status as alternate account")
            .await;
        match &result {
            Ok(_) => {}
            Err(err)
                if err.raw_response().map(|r| r.status().as_u16()) == Some(403)
                    && err.as_service_error().and_then(ProvideErrorMetadata::code)
                        == Some("AccessDenied") =>
            {
                return;
            }
            Err(_) => {}
        }
        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            continue;
        }
        panic!("GetBucketPolicyStatus did not converge to AccessDenied for bucket {bucket}: {result:?}");
    }

    unreachable!()
}

fn simple_cors_configuration(origin: &str, method: &str) -> CorsConfiguration {
    CorsConfiguration::builder()
        .cors_rules(
            CorsRule::builder()
                .allowed_origins(origin)
                .allowed_methods(method)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap()
}

fn simple_bucket_tagging(key: &str, value: &str) -> Tagging {
    Tagging::builder()
        .tag_set(Tag::builder().key(key).value(value).build().unwrap())
        .build()
        .unwrap()
}

fn raw_alt_credentials() -> SignedRequestCredentials<'static> {
    SignedRequestCredentials {
        access_key: CTX.alt_access_key(),
        secret_key: CTX.alt_secret_key(),
        region: CTX.region(),
        tls_ca_pem: CTX.tls_ca_pem(),
    }
}

async fn raw_alt_object_status_eventually(
    description: &str,
    request: RawAltObjectRequest<'_>,
    expected_status: u16,
) -> s3_tests::RawResponse {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);

    loop {
        let response = raw_alt_object_request(request);
        if response.status == expected_status {
            return response;
        }

        if std::time::Instant::now() >= deadline {
            panic!("{description} did not converge to {expected_status}: {response:?}");
        }

        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
}

async fn raw_presigned_status_eventually(
    description: &str,
    url: &str,
    expected_status: u16,
) -> s3_tests::RawResponse {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);

    loop {
        let response = raw_fetch_url(url, &[]);
        if response.status == expected_status {
            return response;
        }

        if std::time::Instant::now() >= deadline {
            panic!("{description} did not converge to {expected_status}: {response:?}");
        }

        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
}

async fn raw_alt_post_object_status_eventually(
    description: &str,
    bucket: &str,
    key: &str,
    fields: Vec<(String, String)>,
    expected_status: u16,
) -> RawResponse {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);

    loop {
        let response = post_object_raw_to_test_endpoint_with_headers(
            CTX.endpoint(),
            CTX.tls_ca_pem(),
            bucket,
            &fields,
            key.as_bytes(),
            "policy-post.txt",
            &[],
        );
        if response.status == expected_status {
            return response;
        }

        if std::time::Instant::now() >= deadline {
            panic!("{description} did not converge to {expected_status}: {response:?}");
        }

        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
}

fn assert_raw_access_denied(operation: &str, response: &s3_tests::RawResponse) {
    assert_eq!(response.status, 403, "{operation}: {response:?}");
    assert!(
        response.body.contains("<Code>AccessDenied</Code>"),
        "{operation}: expected AccessDenied body: {}",
        response.body
    );
}

fn simple_lifecycle_configuration(prefix: &str, days: i32) -> BucketLifecycleConfiguration {
    BucketLifecycleConfiguration::builder()
        .rules(
            LifecycleRule::builder()
                .id("expire-current")
                .filter(LifecycleRuleFilter::builder().prefix(prefix).build())
                .status(ExpirationStatus::Enabled)
                .expiration(LifecycleExpiration::builder().days(days).build())
                .build()
                .unwrap(),
        )
        .build()
        .unwrap()
}

fn simple_bucket_ownership_controls(ownership: ObjectOwnership) -> OwnershipControls {
    OwnershipControls::builder()
        .rules(
            OwnershipControlsRule::builder()
                .object_ownership(ownership)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap()
}

fn simple_bucket_encryption(blocked: EncryptionType) -> ServerSideEncryptionConfiguration {
    let default = ServerSideEncryptionByDefault::builder()
        .sse_algorithm(ServerSideEncryption::Aes256)
        .build()
        .unwrap();
    ServerSideEncryptionConfiguration::builder()
        .rules(
            ServerSideEncryptionRule::builder()
                .apply_server_side_encryption_by_default(default)
                .blocked_encryption_types(
                    BlockedEncryptionTypes::builder()
                        .encryption_type(blocked)
                        .build(),
                )
                .build(),
        )
        .build()
        .unwrap()
}

fn blocked_encryption_types(rule: &ServerSideEncryptionRule) -> Vec<String> {
    rule.blocked_encryption_types()
        .map(|blocked| {
            blocked
                .encryption_type()
                .iter()
                .map(|value| value.as_str().to_string())
                .collect()
        })
        .unwrap_or_default()
}

fn simple_public_access_block() -> PublicAccessBlockConfiguration {
    PublicAccessBlockConfiguration::builder()
        .block_public_acls(true)
        .ignore_public_acls(true)
        .block_public_policy(true)
        .restrict_public_buckets(false)
        .build()
}

fn simple_object_lock_configuration() -> ObjectLockConfiguration {
    ObjectLockConfiguration::builder()
        .object_lock_enabled(ObjectLockEnabled::Enabled)
        .rule(
            ObjectLockRule::builder()
                .default_retention(
                    DefaultRetention::builder()
                        .mode(ObjectLockRetentionMode::Governance)
                        .days(1)
                        .build(),
                )
                .build(),
        )
        .build()
}

async fn create_bucket_allowing_public_policy(client: &aws_sdk_s3::Client) -> String {
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    disable_bucket_public_access_block(client, &bucket).await;
    bucket
}

async fn create_bucket_allowing_sse_c(client: &aws_sdk_s3::Client) -> String {
    let bucket = unique_bucket();
    s3_tests::create_bucket_with_sse_c_enabled(client, &bucket)
        .await
        .unwrap();
    bucket
}

async fn create_object_lock_bucket(client: &aws_sdk_s3::Client) -> String {
    let bucket = unique_bucket();
    s3_tests::create_bucket_request(client, &bucket)
        .object_lock_enabled_for_bucket(true)
        .send_retrying_operation_aborted("create object lock bucket for bucket policy test")
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

fn object_resource(bucket: &str, key: &str) -> String {
    format!("arn:aws:s3:::{bucket}/{key}")
}

fn source_ip_list_bucket_policy(bucket: &str, operator: &str, cidr: &str) -> String {
    json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:ListBucket",
            "Resource": bucket_resource(bucket),
            "Condition": {
                operator: {
                    "aws:SourceIp": cidr
                }
            }
        }],
    })
    .to_string()
}

fn source_ip_get_object_policy(bucket: &str, operator: &str, cidr: &str) -> String {
    json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": bucket_wildcard_resource(bucket),
            "Condition": {
                operator: {
                    "aws:SourceIp": cidr
                }
            }
        }],
    })
    .to_string()
}

fn source_ip_get_object_allow_with_deny_policy(bucket: &str, operator: &str, cidr: &str) -> String {
    json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": bucket_wildcard_resource(bucket)
            },
            {
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": bucket_wildcard_resource(bucket),
                "Condition": {
                    operator: {
                        "aws:SourceIp": cidr
                    }
                }
            }
        ],
    })
    .to_string()
}

fn alt_policy_principal() -> serde_json::Value {
    json!({ "AWS": format!("arn:aws:iam::{}:root", CTX.alt_account_id()) })
}

fn fixed_nonpublic_principal() -> serde_json::Value {
    json!({ "AWS": format!("arn:aws:iam::{}:root", CTX.alt_account_id()) })
}

fn multipart_put_object_policy_for_alt_and_same_account(
    bucket: &str,
    same_account_principal: &str,
) -> String {
    json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": alt_policy_principal(),
                "Action": "s3:PutObject",
                "Resource": bucket_wildcard_resource(bucket),
            },
            {
                "Effect": "Allow",
                "Principal": { "AWS": same_account_principal },
                "Action": "s3:PutObject",
                "Resource": bucket_wildcard_resource(bucket),
            }
        ],
    })
    .to_string()
}

async fn same_account_exact_principal() -> String {
    if std::env::var_os("S3_TEST_ENDPOINT").is_none() {
        return format!("arn:aws:iam::{}:user/limited", CTX.account_id());
    }
    if let Ok(principal) = std::env::var("S3_TEST_SECOND_PRINCIPAL") {
        assert!(
            principal.starts_with("arn:aws:iam::"),
            "S3_TEST_SECOND_PRINCIPAL must be an IAM ARN"
        );
        return principal;
    }

    let root_client = CTX.client();
    let constrained_client = CTX.require_second_client();
    let bucket = unique_bucket();
    s3_tests::create_bucket(root_client, &bucket).await.unwrap();
    let denied = constrained_client
        .put_object()
        .bucket(&bucket)
        .key("principal-discovery")
        .body(ByteStream::from_static(b"principal-discovery"))
        .send()
        .await;
    let message = denied
        .as_ref()
        .err()
        .and_then(|err| err.as_service_error())
        .and_then(ProvideErrorMetadata::message)
        .unwrap_or_else(|| {
            panic!("expected AccessDenied message while discovering same-account principal")
        });
    let principal = message
        .strip_prefix("User: ")
        .and_then(|rest| rest.split(" is not authorized").next())
        .filter(|principal| principal.starts_with("arn:aws:iam::"))
        .unwrap_or_else(|| {
            panic!("failed to parse same-account principal from AccessDenied message: {message}")
        })
        .to_string();
    cleanup_with_client(root_client, &bucket, &[]).await;
    principal
}

async fn bucket_policy_status_is_public(client: &aws_sdk_s3::Client, bucket: &str) -> bool {
    client
        .get_bucket_policy_status()
        .bucket(bucket)
        .send()
        .await
        .unwrap()
        .policy_status()
        .and_then(|status| status.is_public())
        .expect("expected PolicyStatus.IsPublic")
}

async fn set_object_writer_ownership(bucket: &str) {
    let rule = aws_sdk_s3::types::OwnershipControlsRule::builder()
        .object_ownership(ObjectOwnership::ObjectWriter)
        .build()
        .unwrap();
    let controls = aws_sdk_s3::types::OwnershipControls::builder()
        .rules(rule)
        .build()
        .unwrap();
    CTX.client()
        .put_bucket_ownership_controls()
        .bucket(bucket)
        .ownership_controls(controls)
        .send()
        .await
        .unwrap();
}

async fn canonical_owner_id(client: &aws_sdk_s3::Client, bucket: &str) -> String {
    client
        .get_bucket_acl()
        .bucket(bucket)
        .send()
        .await
        .unwrap()
        .owner()
        .expect("expected owner in GetBucketAcl")
        .id()
        .expect("expected owner ID in GetBucketAcl")
        .to_string()
}

async fn client_canonical_id(client: &aws_sdk_s3::Client) -> String {
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    let owner_id = canonical_owner_id(client, &bucket).await;
    cleanup_with_client(client, &bucket, &[]).await;
    owner_id
}

fn has_grant(grants: &[Grant], permission: Permission, canonical_user_id: Option<&str>) -> bool {
    grants.iter().any(|grant| {
        grant.permission() == Some(&permission)
            && grant
                .grantee()
                .is_some_and(|grantee| grantee.id() == canonical_user_id)
    })
}

fn bucket_policy_document(
    principal: serde_json::Value,
    effect: &str,
    action: &str,
    resource: String,
) -> String {
    json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": effect,
            "Principal": principal,
            "Action": action,
            "Resource": resource,
        }],
    })
    .to_string()
}

async fn complete_single_part_upload(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    etag: &str,
) {
    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(CompletedPart::builder().e_tag(etag).part_number(1).build())
                .build(),
        )
        .send_retrying_operation_aborted("complete single part upload in bucket policy test")
        .await
        .unwrap();
}

#[test]
fn test_bucket_policy_put_get_delete() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_public_policy(client).await;

        let policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Sid": "DenyAllGetObject",
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": format!("arn:aws:s3:::{bucket}/*"),
            }],
        })
        .to_string();

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy.clone())
            .send()
            .await
            .unwrap();

        let resp = client
            .get_bucket_policy()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let actual_policy: serde_json::Value =
            serde_json::from_str(resp.policy().unwrap()).unwrap();
        let expected_policy: serde_json::Value = serde_json::from_str(&policy).unwrap();
        assert_eq!(actual_policy, expected_policy);

        client
            .delete_bucket_policy()
            .bucket(&bucket)
            .send_retrying_operation_aborted("delete bucket policy")
            .await
            .unwrap();

        let result = client
            .get_bucket_policy()
            .bucket(&bucket)
            .send_retrying_operation_aborted("get bucket policy after delete")
            .await;
        assert_eq!(err_status(&result), 404);
        let err = result.unwrap_err();
        assert_eq!(
            err.as_service_error().and_then(ProvideErrorMetadata::code),
            Some("NoSuchBucketPolicy")
        );

        client
            .delete_bucket_policy()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_get_bucket_policy_status_private_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let result = client
            .get_bucket_policy_status()
            .bucket(&bucket)
            .send()
            .await;
        assert_eq!(err_status(&result), 404);
        assert_eq!(
            result
                .unwrap_err()
                .as_service_error()
                .and_then(ProvideErrorMetadata::code),
            Some("NoSuchBucketPolicy")
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_get_bucket_policy_status_public_bucket_acl() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_public_bucket(client).await;

        let result = client
            .get_bucket_policy_status()
            .bucket(&bucket)
            .send()
            .await;
        assert_eq!(err_status(&result), 404);
        assert_eq!(
            result
                .unwrap_err()
                .as_service_error()
                .and_then(ProvideErrorMetadata::code),
            Some("NoSuchBucketPolicy")
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_get_bucket_policy_status_public_bucket_policy() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_public_policy(client).await;

        let put_result = client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                json!("*"),
                "Allow",
                "s3:ListBucket",
                bucket_resource(&bucket),
            ))
            .send()
            .await;
        match put_result {
            Ok(_) => {}
            Err(err)
                if std::env::var("S3_TEST_ENDPOINT").is_ok()
                    && err.as_service_error().and_then(ProvideErrorMetadata::code)
                        == Some("AccessDenied")
                    && format!("{err:?}").contains("BlockPublicPolicy") =>
            {
                cleanup(&bucket, &[]).await;
                panic!(
                    "account-level S3 Block Public Access must allow public bucket policies for AWS s3-tests; put_bucket_policy failed with BlockPublicPolicy: {err:?}"
                );
            }
            Err(err) => panic!("put_bucket_policy failed unexpectedly: {err:?}"),
        }

        assert!(bucket_policy_status_is_public(client, &bucket).await);

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_get_bucket_policy_status_nonpublic_bucket_policy() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:ListBucket",
                "Resource": bucket_resource(&bucket),
                "Condition": {
                    "IpAddress": {
                        "aws:SourceIp": "10.0.0.0/32"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        assert!(!bucket_policy_status_is_public(client, &bucket).await);

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_get_bucket_policy_status_public_ipv4_full_range_bucket_policy() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:ListBucket",
                "Resource": bucket_resource(&bucket),
                "Condition": {
                    "IpAddress": {
                        "aws:SourceIp": "0.0.0.0/0"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        assert!(bucket_policy_status_is_public(client, &bucket).await);

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_get_bucket_policy_status_public_ipv4_cidr_broader_than_slash_8_bucket_policy() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:ListBucket",
                "Resource": bucket_resource(&bucket),
                "Condition": {
                    "IpAddress": {
                        "aws:SourceIp": "11.0.0.0/7"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        assert!(bucket_policy_status_is_public(client, &bucket).await);

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_get_bucket_policy_status_nonpublic_ipv4_slash_8_bucket_policy() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(source_ip_list_bucket_policy(
                &bucket,
                "IpAddress",
                "11.0.0.0/8",
            ))
            .send()
            .await
            .unwrap();

        assert!(!bucket_policy_status_is_public(client, &bucket).await);

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_get_bucket_policy_status_qualified_source_ip_operator_classification() {
    s3_tests::run(async {
        let client = CTX.client();

        for (operator, expected_public) in [
            ("IpAddressIfExists", true),
            ("ForAllValues:IpAddress", true),
            ("ForAnyValue:IpAddress", false),
        ] {
            let bucket = create_bucket_allowing_public_policy(client).await;
            client
                .put_bucket_policy()
                .bucket(&bucket)
                .policy(source_ip_list_bucket_policy(
                    &bucket,
                    operator,
                    "10.0.0.0/8",
                ))
                .send()
                .await
                .unwrap();

            assert_eq!(
                bucket_policy_status_is_public(client, &bucket).await,
                expected_public,
                "{operator} public-policy classification"
            );

            cleanup(&bucket, &[]).await;
        }
    });
}

#[test]
fn test_get_bucket_policy_status_public_ipv6_ula_bucket_policy() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:ListBucket",
                "Resource": bucket_resource(&bucket),
                "Condition": {
                    "IpAddress": {
                        "aws:SourceIp": "fd00::/8"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        assert!(bucket_policy_status_is_public(client, &bucket).await);

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_source_ip_for_any_value_ip_address_allows_matching_ipv4_client() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let key = "source-ip-for-any-value-ip-address-allow";
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"source-ip-for-any-allow"))
            .send()
            .await
            .unwrap();

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(source_ip_get_object_policy(
                &bucket,
                "ForAnyValue:IpAddress",
                "0.0.0.0/0",
            ))
            .send()
            .await
            .unwrap();

        let url = object_url(CTX.endpoint(), &bucket, key, None);
        let body = anonymous_get_object_status_eventually(&url, 200).await;
        assert_eq!(body, "source-ip-for-any-allow");

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_source_ip_ip_address_denies_nonmatching_ipv4_subnet() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let key = "source-ip-ip-address-subnet-deny";
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"source-ip-subnet-deny"))
            .send()
            .await
            .unwrap();

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(source_ip_get_object_policy(
                &bucket,
                "IpAddress",
                "192.0.2.0/24",
            ))
            .send()
            .await
            .unwrap();

        let url = object_url(CTX.endpoint(), &bucket, key, None);
        let body = anonymous_get_object_status_eventually(&url, 403).await;
        assert!(
            body.contains("<Code>AccessDenied</Code>"),
            "expected AccessDenied body: {body}"
        );

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_source_ip_ip_address_allows_matching_ipv4_client() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let key = "source-ip-ip-address-allow";
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"source-ip-allow"))
            .send()
            .await
            .unwrap();

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(source_ip_get_object_policy(
                &bucket,
                "IpAddress",
                "0.0.0.0/0",
            ))
            .send()
            .await
            .unwrap();

        let url = object_url(CTX.endpoint(), &bucket, key, None);
        let body = anonymous_get_object_status_eventually(&url, 200).await;
        assert_eq!(body, "source-ip-allow");

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_source_ip_ip_address_denies_nonmatching_ipv4_client() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let key = "source-ip-ip-address-deny";
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"source-ip-deny"))
            .send()
            .await
            .unwrap();

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(source_ip_get_object_policy(&bucket, "IpAddress", "::/0"))
            .send()
            .await
            .unwrap();

        let url = object_url(CTX.endpoint(), &bucket, key, None);
        let body = anonymous_get_object_status_eventually(&url, 403).await;
        assert!(
            body.contains("<Code>AccessDenied</Code>"),
            "expected AccessDenied body: {body}"
        );

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_source_ip_not_ip_address_denies_matching_ipv4_client() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let key = "source-ip-not-ip-address-deny";
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"not-ip-deny"))
            .send()
            .await
            .unwrap();

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(source_ip_get_object_allow_with_deny_policy(
                &bucket,
                "NotIpAddress",
                "::/0",
            ))
            .send()
            .await
            .unwrap();

        let url = object_url(CTX.endpoint(), &bucket, key, None);
        let body = anonymous_get_object_status_eventually(&url, 403).await;
        assert!(
            body.contains("<Code>AccessDenied</Code>"),
            "expected AccessDenied body: {body}"
        );

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_source_ip_not_ip_address_allows_nonmatching_ipv4_client() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let key = "source-ip-not-ip-address-allow";
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"not-ip-allow"))
            .send()
            .await
            .unwrap();

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(source_ip_get_object_allow_with_deny_policy(
                &bucket,
                "NotIpAddress",
                "0.0.0.0/0",
            ))
            .send()
            .await
            .unwrap();

        let url = object_url(CTX.endpoint(), &bucket, key, None);
        let body = anonymous_get_object_status_eventually(&url, 200).await;
        assert_eq!(body, "not-ip-allow");

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_get_bucket_policy_status_nonpublic_fixed_principal_policy() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_sse_c(client).await;
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": fixed_nonpublic_principal(),
                "Action": "s3:ListBucket",
                "Resource": bucket_resource(&bucket),
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        assert!(!bucket_policy_status_is_public(client, &bucket).await);

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_get_bucket_policy_status_cross_account_allow() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_bucket_allowing_sse_c(client).await;
        let policy = bucket_policy_document(
            principal,
            "Allow",
            "s3:GetBucketPolicyStatus",
            bucket_resource(&bucket),
        );
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();
        client
            .get_bucket_policy()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let result = get_bucket_policy_status_eventually(alt_client, &bucket).await;
        assert!(!result
            .policy_status()
            .and_then(|status| status.is_public())
            .expect("expected PolicyStatus.IsPublic"));

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_get_bucket_policy_status_cross_account_deny_overrides_allow() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_bucket_allowing_sse_c(client).await;
        let allow_policy = bucket_policy_document(
            principal.clone(),
            "Allow",
            "s3:GetBucketPolicyStatus",
            bucket_resource(&bucket),
        );
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(allow_policy)
            .send()
            .await
            .unwrap();
        client
            .get_bucket_policy()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let allow_result = get_bucket_policy_status_eventually(alt_client, &bucket).await;
        assert!(!allow_result
            .policy_status()
            .and_then(|status| status.is_public())
            .expect("expected PolicyStatus.IsPublic"));

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": principal.clone(),
                    "Action": "s3:GetBucketPolicyStatus",
                    "Resource": bucket_resource(&bucket),
                },
                {
                    "Effect": "Deny",
                    "Principal": principal,
                    "Action": "s3:GetBucketPolicyStatus",
                    "Resource": bucket_resource(&bucket),
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();
        client
            .get_bucket_policy()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        alt_get_bucket_policy_status_access_denied_eventually(&bucket).await;

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_put_bucket_policy_not_principal_rejected() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_sse_c(client).await;
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "NotPrincipal": fixed_nonpublic_principal(),
                "Action": "s3:ListBucket",
                "Resource": bucket_resource(&bucket),
            }],
        })
        .to_string();

        let result = client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "MalformedPolicy");

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_put_bucket_policy_allows_raw_over_limit_when_normalized_under_limit() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_sse_c(client).await;
        let mut statements = Vec::new();
        let (pretty_policy, normalized_policy) = loop {
            statements.push(json!({
                "Sid": format!("Stmt{:04}", statements.len()),
                "Effect": "Allow",
                "Principal": fixed_nonpublic_principal(),
                "Action": ["s3:GetObject"],
                "Resource": [bucket_wildcard_resource(&bucket)],
            }));

            let policy_value = json!({
                "Version": "2012-10-17",
                "Statement": statements,
            });
            let pretty = serde_json::to_string_pretty(&policy_value).unwrap();
            let normalized = auth::parse_bucket_policy(&pretty)
                .unwrap()
                .normalized_json();
            if pretty.len() > 20 * 1024 && normalized.len() <= 20 * 1024 {
                break (pretty, normalized);
            }
        };

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(pretty_policy)
            .send()
            .await
            .unwrap();

        let fetched = client
            .get_bucket_policy()
            .bucket(&bucket)
            .send()
            .await
            .unwrap()
            .policy()
            .unwrap()
            .to_string();
        assert_eq!(fetched, normalized_policy);

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_put_bucket_policy_rejects_oversized_normalized_policy() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_sse_c(client).await;
        let mut statements = Vec::new();
        let normalized_policy = loop {
            statements.push(json!({
                "Sid": format!("Stmt{:04}", statements.len()),
                "Effect": "Allow",
                "Principal": fixed_nonpublic_principal(),
                "Action": "s3:GetObject",
                "Resource": format!("{}path-{:04}*", bucket_wildcard_resource(&bucket), statements.len()),
            }));

            let policy = serde_json::to_string(&json!({
                "Version": "2012-10-17",
                "Statement": statements,
            }))
            .unwrap();
            let normalized = auth::parse_bucket_policy(&policy)
                .unwrap()
                .normalized_json();
            if normalized.len() > 24 * 1024 {
                break normalized;
            }
        };

        let result = client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(normalized_policy)
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        let err = result.unwrap_err();
        assert_eq!(err.code(), Some("MalformedPolicy"));
        assert_eq!(
            err.message(),
            Some("Normalized policy document exceeds the maximum allowed size of 20480 bytes")
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_get_returns_normalized_json() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let resource = bucket_resource(&bucket);
        let policy = format!(
            "{{\n  \"Statement\": {{\n    \"Resource\": [\"{resource}\"],\n    \"Action\": [\"s3:ListBucket\"],\n    \"Principal\": {principal},\n    \"Effect\": \"Allow\",\n    \"Sid\": \"One\"\n  }},\n  \"Version\": \"2012-10-17\"\n}}",
            principal = fixed_nonpublic_principal()
        );
        let expected = format!(
            "{{\"Version\":\"2012-10-17\",\"Statement\":[{{\"Sid\":\"One\",\"Effect\":\"Allow\",\"Principal\":{principal},\"Action\":\"s3:ListBucket\",\"Resource\":\"{resource}\"}}]}}",
            principal = fixed_nonpublic_principal()
        );

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let fetched = client
            .get_bucket_policy()
            .bucket(&bucket)
            .send()
            .await
            .unwrap()
            .policy()
            .unwrap()
            .to_string();
        assert_eq!(fetched, expected);

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_list_objects_v1() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();

        let bucket = create_bucket_allowing_sse_c(client).await;
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                principal,
                "Allow",
                "s3:ListBucket",
                bucket_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        let response = alt_list_objects_v1_eventually(&bucket).await;
        assert_eq!(response.contents().len(), 1);
        assert_eq!(response.contents()[0].key(), Some("obj"));

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_bucket_policy_get_bucket_cors_cross_account_allow() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_bucket_allowing_sse_c(client).await;
        let config = simple_cors_configuration("https://example.com", "GET");
        client
            .put_bucket_cors()
            .bucket(&bucket)
            .cors_configuration(config)
            .send()
            .await
            .unwrap();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                principal,
                "Allow",
                "s3:GetBucketCORS",
                bucket_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        let response = eventually_ok("GetBucketCORS", || {
            alt_client.get_bucket_cors().bucket(&bucket).send()
        })
        .await;
        assert_eq!(response.cors_rules().len(), 1);
        assert_eq!(
            response.cors_rules()[0].allowed_origins(),
            ["https://example.com"]
        );
        assert!(response.cors_rules()[0]
            .allowed_methods()
            .contains(&"GET".to_string()));

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_put_bucket_cors_cross_account_allow() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_bucket_allowing_sse_c(client).await;
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                principal,
                "Allow",
                "s3:PutBucketCORS",
                bucket_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        let put_config = simple_cors_configuration("https://put.example.com", "PUT");
        eventually_ok("PutBucketCORS", || {
            alt_client
                .put_bucket_cors()
                .bucket(&bucket)
                .cors_configuration(put_config.clone())
                .send()
        })
        .await;

        let read_back = client
            .get_bucket_cors()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(read_back.cors_rules().len(), 1);
        assert_eq!(
            read_back.cors_rules()[0].allowed_origins(),
            ["https://put.example.com"]
        );

        eventually_ok("DeleteBucketCORS", || {
            alt_client.delete_bucket_cors().bucket(&bucket).send()
        })
        .await;

        eventually_result_matches(
            "GetBucketCORS absent",
            60,
            std::time::Duration::from_millis(500),
            || client.get_bucket_cors().bucket(&bucket).send(),
            |result| {
                result.as_ref().err().is_some_and(|err| {
                    err.raw_response().map(|r| r.status().as_u16()) == Some(404)
                        && err.as_service_error().and_then(ProvideErrorMetadata::code)
                            == Some("NoSuchCORSConfiguration")
                })
            },
        )
        .await;

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_get_bucket_tagging_cross_account_allow() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_bucket_allowing_sse_c(client).await;
        client
            .put_bucket_tagging()
            .bucket(&bucket)
            .tagging(simple_bucket_tagging("env", "test"))
            .send()
            .await
            .unwrap();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                principal,
                "Allow",
                "s3:GetBucketTagging",
                bucket_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        let response = eventually_ok("GetBucketTagging", || {
            alt_client.get_bucket_tagging().bucket(&bucket).send()
        })
        .await;
        assert_eq!(response.tag_set().len(), 1);
        assert_eq!(response.tag_set()[0].key(), "env");
        assert_eq!(response.tag_set()[0].value(), "test");

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_put_bucket_tagging_cross_account_allow() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_bucket_allowing_sse_c(client).await;
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                principal,
                "Allow",
                "s3:PutBucketTagging",
                bucket_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        eventually_ok("PutBucketTagging", || {
            alt_client
                .put_bucket_tagging()
                .bucket(&bucket)
                .tagging(simple_bucket_tagging("team", "storage"))
                .send()
        })
        .await;

        let read_back = client
            .get_bucket_tagging()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(read_back.tag_set().len(), 1);
        assert_eq!(read_back.tag_set()[0].key(), "team");
        assert_eq!(read_back.tag_set()[0].value(), "storage");

        eventually_ok("DeleteBucketTagging", || {
            alt_client.delete_bucket_tagging().bucket(&bucket).send()
        })
        .await;

        eventually_result_matches(
            "GetBucketTagging absent",
            60,
            std::time::Duration::from_millis(500),
            || client.get_bucket_tagging().bucket(&bucket).send(),
            |result| {
                result.as_ref().err().is_some_and(|err| {
                    err.raw_response().map(|r| r.status().as_u16()) == Some(404)
                        && err.as_service_error().and_then(ProvideErrorMetadata::code)
                            == Some("NoSuchTagSet")
                })
            },
        )
        .await;

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_get_bucket_lifecycle_cross_account_allow() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        put_bucket_lifecycle_with_md5(client, &bucket, simple_lifecycle_configuration("logs/", 30))
            .send()
            .await
            .unwrap();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                principal,
                "Allow",
                "s3:GetLifecycleConfiguration",
                bucket_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        let response = eventually_ok_with_retry(
            "GetBucketLifecycleConfiguration",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .get_bucket_lifecycle_configuration()
                    .bucket(&bucket)
                    .send()
            },
        )
        .await;
        assert_eq!(response.rules().len(), 1);
        assert_eq!(response.rules()[0].id(), Some("expire-current"));
        assert_eq!(
            response.rules()[0].filter().and_then(|f| f.prefix()),
            Some("logs/")
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_put_bucket_lifecycle_cross_account_allow() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                principal,
                "Allow",
                "s3:PutLifecycleConfiguration",
                bucket_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        eventually_ok_with_retry(
            "PutBucketLifecycleConfiguration",
            60,
            std::time::Duration::from_millis(500),
            || {
                put_bucket_lifecycle_with_md5(
                    alt_client,
                    &bucket,
                    simple_lifecycle_configuration("archive/", 14),
                )
                .send()
            },
        )
        .await;

        let read_back = client
            .get_bucket_lifecycle_configuration()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(read_back.rules().len(), 1);
        assert_eq!(
            read_back.rules()[0].filter().and_then(|f| f.prefix()),
            Some("archive/")
        );

        eventually_ok("DeleteBucketLifecycle", || {
            alt_client.delete_bucket_lifecycle().bucket(&bucket).send()
        })
        .await;

        eventually_result_matches(
            "GetBucketLifecycleConfiguration absent",
            60,
            std::time::Duration::from_millis(500),
            || {
                client
                    .get_bucket_lifecycle_configuration()
                    .bucket(&bucket)
                    .send()
            },
            |result| {
                result.as_ref().err().is_some_and(|err| {
                    err.raw_response().map(|r| r.status().as_u16()) == Some(404)
                        && err.as_service_error().and_then(ProvideErrorMetadata::code)
                            == Some("NoSuchLifecycleConfiguration")
                })
            },
        )
        .await;

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_get_bucket_public_access_block_cross_account_allow() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_public_access_block()
            .bucket(&bucket)
            .public_access_block_configuration(simple_public_access_block())
            .send()
            .await
            .unwrap();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                principal,
                "Allow",
                "s3:GetBucketPublicAccessBlock",
                bucket_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        let response = eventually_ok_with_retry(
            "GetPublicAccessBlock",
            60,
            std::time::Duration::from_millis(500),
            || alt_client.get_public_access_block().bucket(&bucket).send(),
        )
        .await;
        let config = response.public_access_block_configuration().unwrap();
        assert_eq!(config.block_public_acls(), Some(true));
        assert_eq!(config.ignore_public_acls(), Some(true));
        assert_eq!(config.block_public_policy(), Some(true));
        assert_eq!(config.restrict_public_buckets(), Some(false));

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_put_bucket_public_access_block_cross_account_allow() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                principal,
                "Allow",
                "s3:PutBucketPublicAccessBlock",
                bucket_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        eventually_ok_with_retry(
            "PutPublicAccessBlock",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_public_access_block()
                    .bucket(&bucket)
                    .public_access_block_configuration(simple_public_access_block())
                    .send()
            },
        )
        .await;

        let read_back = client
            .get_public_access_block()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let config = read_back.public_access_block_configuration().unwrap();
        assert_eq!(config.block_public_acls(), Some(true));
        assert_eq!(config.ignore_public_acls(), Some(true));
        assert_eq!(config.block_public_policy(), Some(true));
        assert_eq!(config.restrict_public_buckets(), Some(false));

        eventually_ok("DeletePublicAccessBlock", || {
            alt_client
                .delete_public_access_block()
                .bucket(&bucket)
                .send()
        })
        .await;

        eventually_result_matches(
            "GetPublicAccessBlock absent",
            60,
            std::time::Duration::from_millis(500),
            || client.get_public_access_block().bucket(&bucket).send(),
            |result| {
                result.as_ref().err().is_some_and(|err| {
                    err.raw_response().map(|r| r.status().as_u16()) == Some(404)
                        && err.as_service_error().and_then(ProvideErrorMetadata::code)
                            == Some("NoSuchPublicAccessBlockConfiguration")
                })
            },
        )
        .await;

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_get_bucket_object_lock_configuration_cross_account_allow() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_object_lock_bucket(client).await;
        let config = simple_object_lock_configuration();
        client
            .put_object_lock_configuration()
            .bucket(&bucket)
            .object_lock_configuration(config.clone())
            .send()
            .await
            .unwrap();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                principal,
                "Allow",
                "s3:GetBucketObjectLockConfiguration",
                bucket_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        let response = eventually_ok_with_retry(
            "GetObjectLockConfiguration",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .get_object_lock_configuration()
                    .bucket(&bucket)
                    .send()
            },
        )
        .await;
        assert_eq!(response.object_lock_configuration(), Some(&config));

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_put_bucket_object_lock_configuration_cross_account_allow() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_object_lock_bucket(client).await;
        let config = simple_object_lock_configuration();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                principal,
                "Allow",
                "s3:PutBucketObjectLockConfiguration",
                bucket_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        eventually_ok_with_retry(
            "PutObjectLockConfiguration",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object_lock_configuration()
                    .bucket(&bucket)
                    .object_lock_configuration(config.clone())
                    .send()
            },
        )
        .await;

        let read_back = client
            .get_object_lock_configuration()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(read_back.object_lock_configuration(), Some(&config));

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_get_bucket_ownership_controls_cross_account_allow() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_bucket_ownership_controls()
            .bucket(&bucket)
            .ownership_controls(simple_bucket_ownership_controls(
                ObjectOwnership::BucketOwnerPreferred,
            ))
            .send()
            .await
            .unwrap();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                principal,
                "Allow",
                "s3:GetBucketOwnershipControls",
                bucket_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        let response = eventually_ok_with_retry(
            "GetBucketOwnershipControls",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .get_bucket_ownership_controls()
                    .bucket(&bucket)
                    .send()
            },
        )
        .await;
        let rules = response.ownership_controls().unwrap().rules();
        assert_eq!(rules.len(), 1);
        assert_eq!(
            rules[0].object_ownership,
            ObjectOwnership::BucketOwnerPreferred
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_put_bucket_ownership_controls_cross_account_allow() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                principal,
                "Allow",
                "s3:PutBucketOwnershipControls",
                bucket_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        eventually_ok_with_retry(
            "PutBucketOwnershipControls",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_bucket_ownership_controls()
                    .bucket(&bucket)
                    .ownership_controls(simple_bucket_ownership_controls(
                        ObjectOwnership::BucketOwnerPreferred,
                    ))
                    .send()
            },
        )
        .await;

        let read_back = client
            .get_bucket_ownership_controls()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let rules = read_back.ownership_controls().unwrap().rules();
        assert_eq!(rules.len(), 1);
        assert_eq!(
            rules[0].object_ownership,
            ObjectOwnership::BucketOwnerPreferred
        );

        eventually_ok("DeleteBucketOwnershipControls", || {
            alt_client
                .delete_bucket_ownership_controls()
                .bucket(&bucket)
                .send()
        })
        .await;

        eventually_result_matches(
            "GetBucketOwnershipControls absent",
            60,
            std::time::Duration::from_millis(500),
            || {
                client
                    .get_bucket_ownership_controls()
                    .bucket(&bucket)
                    .send()
            },
            |result| {
                result.as_ref().err().is_some_and(|err| {
                    err.raw_response().map(|r| r.status().as_u16()) == Some(404)
                        && err.as_service_error().and_then(ProvideErrorMetadata::code)
                            == Some("OwnershipControlsNotFoundError")
                })
            },
        )
        .await;

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_get_bucket_encryption_cross_account_allow() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_bucket_encryption()
            .bucket(&bucket)
            .server_side_encryption_configuration(simple_bucket_encryption(EncryptionType::SseC))
            .send()
            .await
            .unwrap();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                principal,
                "Allow",
                "s3:GetEncryptionConfiguration",
                bucket_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        let response = eventually_ok_with_retry(
            "GetBucketEncryption",
            60,
            std::time::Duration::from_millis(500),
            || alt_client.get_bucket_encryption().bucket(&bucket).send(),
        )
        .await;
        let rules = response
            .server_side_encryption_configuration()
            .unwrap()
            .rules();
        assert_eq!(rules.len(), 1);
        assert_eq!(
            blocked_encryption_types(&rules[0]),
            vec!["SSE-C".to_string()]
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_put_bucket_encryption_cross_account_allow() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        let initial = client
            .get_bucket_encryption()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let initial_rules = initial
            .server_side_encryption_configuration()
            .unwrap()
            .rules();
        let initial_blocked = blocked_encryption_types(&initial_rules[0]);
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                principal,
                "Allow",
                "s3:PutEncryptionConfiguration",
                bucket_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        eventually_ok_with_retry(
            "PutBucketEncryption",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_bucket_encryption()
                    .bucket(&bucket)
                    .server_side_encryption_configuration(simple_bucket_encryption(
                        EncryptionType::SseC,
                    ))
                    .send()
            },
        )
        .await;

        let read_back = client
            .get_bucket_encryption()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let rules = read_back
            .server_side_encryption_configuration()
            .unwrap()
            .rules();
        assert_eq!(
            blocked_encryption_types(&rules[0]),
            vec!["SSE-C".to_string()]
        );

        eventually_ok("DeleteBucketEncryption", || {
            alt_client.delete_bucket_encryption().bucket(&bucket).send()
        })
        .await;

        let after_delete = client
            .get_bucket_encryption()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let rules = after_delete
            .server_side_encryption_configuration()
            .unwrap()
            .rules();
        let blocked = blocked_encryption_types(&rules[0]);
        assert_eq!(
            blocked, initial_blocked,
            "DeleteBucketEncryption should restore the bucket's prior default blocked types"
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_list_objects_v2() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();

        let bucket = create_bucket_allowing_sse_c(client).await;
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                principal,
                "Allow",
                "s3:ListBucket",
                bucket_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        let response = alt_list_objects_v2_eventually(&bucket).await;
        assert_eq!(response.contents().len(), 1);
        assert_eq!(response.contents()[0].key(), Some("obj"));

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_bucket_policy_list_deny_overrides_bucket_acl() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_public_bucket(client).await;
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let url = format!("{}/{}", CTX.endpoint(), bucket);
        let mut allowed = agent().get(&url).call().expect("transport error");
        assert_eq!(allowed.status().as_u16(), 200);
        let allowed_body = allowed.body_mut().read_to_string().unwrap();
        assert!(
            allowed_body.contains("<Key>obj</Key>"),
            "expected anonymous listing before deny policy: {allowed_body}"
        );

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                json!("*"),
                "Deny",
                "s3:ListBucket",
                bucket_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();
        client
            .get_bucket_policy()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        anonymous_list_bucket_access_denied_eventually(&url).await;

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_bucket_policy_list_requires_bucket_resource() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let result = client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                json!("*"),
                "Allow",
                "s3:ListBucket",
                bucket_wildcard_resource(&bucket),
            ))
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "MalformedPolicy");

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_put_obj_grant_full_control() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        let control_bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        s3_tests::create_bucket(client, &control_bucket)
            .await
            .unwrap();
        set_object_writer_ownership(&bucket).await;
        set_object_writer_ownership(&control_bucket).await;
        let owner_id = canonical_owner_id(client, &bucket).await;
        let full_control_header = format!("id=\"{owner_id}\"");
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal.clone(),
                "Action": "s3:PutObject",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:x-amz-grant-full-control": full_control_header
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();
        client
            .put_bucket_policy()
            .bucket(&control_bucket)
            .policy(bucket_policy_document(
                principal,
                "Allow",
                "s3:PutObject",
                bucket_wildcard_resource(&control_bucket),
            ))
            .send()
            .await
            .unwrap();
        client
            .get_bucket_policy()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        client
            .get_bucket_policy()
            .bucket(&control_bucket)
            .send()
            .await
            .unwrap();

        let denied = alt_client
            .put_object()
            .bucket(&bucket)
            .key("denied")
            .body(ByteStream::from_static(b"denied"))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        eventually_ok("PutObject with grant-full-control", || {
            let full_control_header = full_control_header.clone();
            alt_client
                .put_object()
                .bucket(&bucket)
                .key("allowed")
                .body(ByteStream::from_static(b"allowed"))
                .customize()
                .mutate_request(move |req| {
                    req.headers_mut()
                        .insert("x-amz-grant-full-control", full_control_header.clone());
                })
                .send()
        })
        .await;

        eventually_ok("PutObject control write", || {
            alt_client
                .put_object()
                .bucket(&control_bucket)
                .key("control")
                .body(ByteStream::from_static(b"control"))
                .send()
        })
        .await;

        let allowed_acl = alt_client
            .get_object_acl()
            .bucket(&bucket)
            .key("allowed")
            .send()
            .await
            .unwrap();
        let control_acl = alt_client
            .get_object_acl()
            .bucket(&control_bucket)
            .key("control")
            .send()
            .await
            .unwrap();
        assert_eq!(
            allowed_acl.owner().and_then(|owner| owner.id()),
            control_acl.owner().and_then(|owner| owner.id())
        );
        assert!(
            has_grant(
                allowed_acl.grants(),
                Permission::FullControl,
                Some(&owner_id)
            ),
            "expected FULL_CONTROL grant for bucket owner, got {:?}",
            allowed_acl.grants()
        );
        assert!(
            !has_grant(
                control_acl.grants(),
                Permission::FullControl,
                Some(&owner_id)
            ),
            "did not expect FULL_CONTROL grant for bucket owner, got {:?}",
            control_acl.grants()
        );

        let control_denied = client
            .get_object_acl()
            .bucket(&control_bucket)
            .key("control")
            .send()
            .await;
        assert_eq!(err_status(&control_denied), 403);
        assert_s3_err_code(&control_denied, "AccessDenied");

        let allowed_owner_view = client
            .get_object_acl()
            .bucket(&bucket)
            .key("allowed")
            .send()
            .await
            .unwrap();
        assert!(
            has_grant(
                allowed_owner_view.grants(),
                Permission::FullControl,
                Some(&owner_id),
            ),
            "expected FULL_CONTROL grant for bucket owner, got {:?}",
            allowed_owner_view.grants()
        );

        cleanup(&bucket, &["allowed", "denied"]).await;
        cleanup(&control_bucket, &["control"]).await;
    });
}

#[test]
fn test_bucket_policy_copy_object_grant_full_control() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_bucket_allowing_sse_c(client).await;
        set_object_writer_ownership(&bucket).await;
        let owner_id = canonical_owner_id(client, &bucket).await;
        let full_control_header = format!("id=\"{owner_id}\"");
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": principal.clone(),
                    "Action": "s3:GetObject",
                    "Resource": bucket_wildcard_resource(&bucket)
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:PutObject",
                    "Resource": bucket_wildcard_resource(&bucket),
                    "Condition": {
                        "StringEquals": {
                            "s3:x-amz-grant-full-control": full_control_header
                        }
                    }
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        client
            .put_object()
            .bucket(&bucket)
            .key("src")
            .body(ByteStream::from_static(b"copy-source"))
            .send()
            .await
            .unwrap();

        let denied = alt_client
            .copy_object()
            .bucket(&bucket)
            .key("denied")
            .copy_source(format!("{bucket}/src"))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        eventually_ok("CopyObject with grant-full-control", || {
            let full_control_header = full_control_header.clone();
            alt_client
                .copy_object()
                .bucket(&bucket)
                .key("allowed")
                .copy_source(format!("{bucket}/src"))
                .customize()
                .mutate_request(move |req| {
                    req.headers_mut()
                        .insert("x-amz-grant-full-control", full_control_header.clone());
                })
                .send()
        })
        .await;

        let allowed_acl = alt_client
            .get_object_acl()
            .bucket(&bucket)
            .key("allowed")
            .send()
            .await
            .unwrap();
        assert!(
            has_grant(
                allowed_acl.grants(),
                Permission::FullControl,
                Some(&owner_id),
            ),
            "expected FULL_CONTROL grant for bucket owner, got {:?}",
            allowed_acl.grants()
        );

        cleanup(&bucket, &["src", "allowed", "denied"]).await;
    });
}

#[test]
fn test_bucket_policy_copy_object_grant_write() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_bucket_allowing_sse_c(client).await;
        set_object_writer_ownership(&bucket).await;
        let owner_id = canonical_owner_id(client, &bucket).await;
        let grant_write_header = format!("id=\"{owner_id}\"");
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": principal.clone(),
                    "Action": "s3:GetObject",
                    "Resource": bucket_wildcard_resource(&bucket)
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:PutObject",
                    "Resource": bucket_wildcard_resource(&bucket),
                    "Condition": {
                        "StringEquals": {
                            "s3:x-amz-grant-write": grant_write_header.clone()
                        }
                    }
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        client
            .put_object()
            .bucket(&bucket)
            .key("src")
            .body(ByteStream::from_static(b"copy-source"))
            .send()
            .await
            .unwrap();

        let denied = alt_client
            .copy_object()
            .bucket(&bucket)
            .key("denied")
            .copy_source(format!("{bucket}/src"))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        eventually_ok("CopyObject with grant-write", || {
            let grant_write_header = grant_write_header.clone();
            alt_client
                .copy_object()
                .bucket(&bucket)
                .key("allowed")
                .copy_source(format!("{bucket}/src"))
                .customize()
                .mutate_request(move |req| {
                    req.headers_mut()
                        .insert("x-amz-grant-write", grant_write_header.clone());
                })
                .send()
        })
        .await;

        let allowed_acl = alt_client
            .get_object_acl()
            .bucket(&bucket)
            .key("allowed")
            .send()
            .await
            .unwrap();
        assert!(
            has_grant(allowed_acl.grants(), Permission::Write, Some(&owner_id)),
            "expected WRITE grant for bucket owner, got {:?}",
            allowed_acl.grants()
        );

        cleanup(&bucket, &["src", "allowed", "denied"]).await;
    });
}

#[test]
fn test_bucket_policy_copy_object_replace_tags_require_put_object_tagging() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        set_object_writer_ownership(&bucket).await;
        client
            .put_object()
            .bucket(&bucket)
            .key("src")
            .body(ByteStream::from_static(b"copy-source"))
            .send()
            .await
            .unwrap();

        let initial_policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": principal.clone(),
                    "Action": "s3:GetObject",
                    "Resource": bucket_wildcard_resource(&bucket),
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:PutObject",
                    "Resource": bucket_wildcard_resource(&bucket),
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(initial_policy)
            .send()
            .await
            .unwrap();

        eventually_result_matches(
            "CopyObject with REPLACE tags denied without PutObjectTagging permission",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .copy_object()
                    .bucket(&bucket)
                    .key("denied")
                    .copy_source(format!("{bucket}/src"))
                    .tagging_directive(aws_sdk_s3::types::TaggingDirective::Replace)
                    .tagging("security=public")
                    .send()
            },
            |result| {
                result.as_ref().err().is_some_and(|err| {
                    err.raw_response().map(|r| r.status().as_u16()) == Some(403)
                        && err.as_service_error().and_then(ProvideErrorMetadata::code)
                            == Some("AccessDenied")
                })
            },
        )
        .await;

        let updated_policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": principal.clone(),
                    "Action": "s3:GetObject",
                    "Resource": bucket_wildcard_resource(&bucket),
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": ["s3:PutObject", "s3:PutObjectTagging"],
                    "Resource": bucket_wildcard_resource(&bucket),
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(updated_policy)
            .send()
            .await
            .unwrap();

        eventually_ok_with_retry(
            "CopyObject with REPLACE tags under PutObjectTagging policy",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .copy_object()
                    .bucket(&bucket)
                    .key("allowed")
                    .copy_source(format!("{bucket}/src"))
                    .tagging_directive(aws_sdk_s3::types::TaggingDirective::Replace)
                    .tagging("security=public")
                    .send()
            },
        )
        .await;

        let tagging = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("allowed")
            .send()
            .await
            .unwrap();
        assert!(
            tagging
                .tag_set()
                .iter()
                .any(|tag| tag.key() == "security" && tag.value() == "public"),
            "expected copied object tags to contain security=public, got {:?}",
            tagging.tag_set()
        );

        cleanup(&bucket, &["src", "allowed", "denied"]).await;
    });
}

#[test]
fn test_bucket_policy_copy_object_acl_condition_applies() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        set_object_writer_ownership(&bucket).await;
        client
            .put_object()
            .bucket(&bucket)
            .key("src")
            .body(ByteStream::from_static(b"copy-source"))
            .send()
            .await
            .unwrap();

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": principal.clone(),
                    "Action": "s3:GetObject",
                    "Resource": bucket_wildcard_resource(&bucket),
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:PutObject",
                    "Resource": bucket_wildcard_resource(&bucket),
                    "Condition": {
                        "StringEquals": {
                            "s3:x-amz-acl": "private"
                        }
                    }
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let denied = alt_client
            .copy_object()
            .bucket(&bucket)
            .key("denied")
            .copy_source(format!("{bucket}/src"))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        eventually_ok("CopyObject with x-amz-acl=private", || {
            alt_client
                .copy_object()
                .bucket(&bucket)
                .key("allowed")
                .copy_source(format!("{bucket}/src"))
                .acl(ObjectCannedAcl::Private)
                .send()
        })
        .await;

        let copied = alt_client
            .get_object()
            .bucket(&bucket)
            .key("allowed")
            .send()
            .await
            .unwrap();
        assert_eq!(
            copied.body.collect().await.unwrap().into_bytes().as_ref(),
            b"copy-source"
        );

        cleanup(&bucket, &["src", "allowed", "denied"]).await;
    });
}

#[test]
fn test_bucket_policy_copy_object_grant_read_condition() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        set_object_writer_ownership(&bucket).await;

        let owner_id = canonical_owner_id(client, &bucket).await;
        let grant_read_header = format!("id=\"{owner_id}\"");
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": principal.clone(),
                    "Action": "s3:GetObject",
                    "Resource": bucket_wildcard_resource(&bucket),
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:PutObject",
                    "Resource": bucket_wildcard_resource(&bucket),
                    "Condition": {
                        "StringEquals": {
                            "s3:x-amz-grant-read": grant_read_header.clone()
                        }
                    }
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        client
            .put_object()
            .bucket(&bucket)
            .key("src")
            .body(ByteStream::from_static(b"copy-source"))
            .send()
            .await
            .unwrap();

        let denied = alt_client
            .copy_object()
            .bucket(&bucket)
            .key("denied")
            .copy_source(format!("{bucket}/src"))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        eventually_ok("CopyObject with grant-read", || {
            let grant_read_header = grant_read_header.clone();
            alt_client
                .copy_object()
                .bucket(&bucket)
                .key("allowed")
                .copy_source(format!("{bucket}/src"))
                .customize()
                .mutate_request(move |req| {
                    req.headers_mut()
                        .insert("x-amz-grant-read", grant_read_header.clone());
                })
                .send()
        })
        .await;

        let allowed_acl = alt_client
            .get_object_acl()
            .bucket(&bucket)
            .key("allowed")
            .send()
            .await
            .unwrap();
        assert!(
            has_grant(allowed_acl.grants(), Permission::Read, Some(&owner_id)),
            "expected READ grant for bucket owner, got {:?}",
            allowed_acl.grants()
        );

        cleanup(&bucket, &["src", "allowed", "denied"]).await;
    });
}

#[test]
fn test_bucket_policy_copy_object_grant_read_acp_condition() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        set_object_writer_ownership(&bucket).await;

        let owner_id = canonical_owner_id(client, &bucket).await;
        let grant_read_acp_header = format!("id=\"{owner_id}\"");
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": principal.clone(),
                    "Action": "s3:GetObject",
                    "Resource": bucket_wildcard_resource(&bucket),
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:PutObject",
                    "Resource": bucket_wildcard_resource(&bucket),
                    "Condition": {
                        "StringEquals": {
                            "s3:x-amz-grant-read-acp": grant_read_acp_header.clone()
                        }
                    }
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        client
            .put_object()
            .bucket(&bucket)
            .key("src")
            .body(ByteStream::from_static(b"copy-source"))
            .send()
            .await
            .unwrap();

        let denied = alt_client
            .copy_object()
            .bucket(&bucket)
            .key("denied")
            .copy_source(format!("{bucket}/src"))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        eventually_ok("CopyObject with grant-read-acp", || {
            let grant_read_acp_header = grant_read_acp_header.clone();
            alt_client
                .copy_object()
                .bucket(&bucket)
                .key("allowed")
                .copy_source(format!("{bucket}/src"))
                .customize()
                .mutate_request(move |req| {
                    req.headers_mut()
                        .insert("x-amz-grant-read-acp", grant_read_acp_header.clone());
                })
                .send()
        })
        .await;

        let allowed_acl = alt_client
            .get_object_acl()
            .bucket(&bucket)
            .key("allowed")
            .send()
            .await
            .unwrap();
        assert!(
            has_grant(allowed_acl.grants(), Permission::ReadAcp, Some(&owner_id)),
            "expected READ_ACP grant for bucket owner, got {:?}",
            allowed_acl.grants()
        );

        cleanup(&bucket, &["src", "allowed", "denied"]).await;
    });
}

#[test]
fn test_bucket_policy_copy_object_grant_write_acp_condition() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        set_object_writer_ownership(&bucket).await;

        let owner_id = canonical_owner_id(client, &bucket).await;
        let grant_write_acp_header = format!("id=\"{owner_id}\"");
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": principal.clone(),
                    "Action": "s3:GetObject",
                    "Resource": bucket_wildcard_resource(&bucket),
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:PutObject",
                    "Resource": bucket_wildcard_resource(&bucket),
                    "Condition": {
                        "StringEquals": {
                            "s3:x-amz-grant-write-acp": grant_write_acp_header.clone()
                        }
                    }
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        client
            .put_object()
            .bucket(&bucket)
            .key("src")
            .body(ByteStream::from_static(b"copy-source"))
            .send()
            .await
            .unwrap();

        let denied = alt_client
            .copy_object()
            .bucket(&bucket)
            .key("denied")
            .copy_source(format!("{bucket}/src"))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        eventually_ok("CopyObject with grant-write-acp", || {
            let grant_write_acp_header = grant_write_acp_header.clone();
            alt_client
                .copy_object()
                .bucket(&bucket)
                .key("allowed")
                .copy_source(format!("{bucket}/src"))
                .customize()
                .mutate_request(move |req| {
                    req.headers_mut()
                        .insert("x-amz-grant-write-acp", grant_write_acp_header.clone());
                })
                .send()
        })
        .await;

        let allowed_acl = alt_client
            .get_object_acl()
            .bucket(&bucket)
            .key("allowed")
            .send()
            .await
            .unwrap();
        assert!(
            has_grant(allowed_acl.grants(), Permission::WriteAcp, Some(&owner_id)),
            "expected WRITE_ACP grant for bucket owner, got {:?}",
            allowed_acl.grants()
        );

        cleanup(&bucket, &["src", "allowed", "denied"]).await;
    });
}

#[test]
fn test_bucket_policy_put_object_existing_tag_condition_is_rejected() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let public_key = "public-overwrite";
        let private_key = "private-overwrite";
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        for key in [public_key, private_key] {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(b"tagged-body"))
                .send()
                .await
                .unwrap();
        }

        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(public_key)
            .tagging(simple_bucket_tagging("security", "public"))
            .send()
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(private_key)
            .tagging(simple_bucket_tagging("security", "private"))
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
                        "Action": "s3:PutObject",
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:ExistingObjectTag/security": "public"
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

        cleanup(&bucket, &[public_key, private_key]).await;
    });
}

#[test]
fn test_bucket_policy_copy_object_source_existing_tag_condition() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let src_bucket = unique_bucket();
        let dst_bucket = unique_bucket();
        let public_key = "public-copy";
        let private_key = "private-copy";

        s3_tests::create_bucket(client, &src_bucket).await.unwrap();
        s3_tests::create_bucket(alt_client, &dst_bucket)
            .await
            .unwrap();

        for key in [public_key, private_key] {
            s3_tests::put_object_retrying_operation_aborted(
                client,
                &src_bucket,
                key,
                b"copy-source".to_vec(),
            )
            .await;
        }

        client
            .put_object_tagging()
            .bucket(&src_bucket)
            .key(public_key)
            .tagging(simple_bucket_tagging("security", "public"))
            .send_retrying_operation_aborted(
                "put public source tag for copy object bucket policy test",
            )
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&src_bucket)
            .key(private_key)
            .tagging(simple_bucket_tagging("security", "private"))
            .send_retrying_operation_aborted(
                "put private source tag for copy object bucket policy test",
            )
            .await
            .unwrap();

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:GetObject",
                "Resource": bucket_wildcard_resource(&src_bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:ExistingObjectTag/security": "public"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&src_bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let source_read = eventually_ok_with_retry(
            "GetObject with ExistingObjectTag-conditioned source bucket policy",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .get_object()
                    .bucket(&src_bucket)
                    .key(public_key)
                    .send()
            },
        )
        .await;
        assert_eq!(
            source_read
                .body
                .collect()
                .await
                .unwrap()
                .into_bytes()
                .as_ref(),
            b"copy-source"
        );

        eventually_access_denied(
            "CopyObject denied for public source under ExistingObjectTag policy",
            || {
                alt_client
                    .copy_object()
                    .bucket(&dst_bucket)
                    .key("copied")
                    .copy_source(format!("{src_bucket}/{public_key}"))
                    .send()
            },
        )
        .await;

        eventually_access_denied(
            "CopyObject denied for private source under ExistingObjectTag policy",
            || {
                alt_client
                    .copy_object()
                    .bucket(&dst_bucket)
                    .key("copied-denied")
                    .copy_source(format!("{src_bucket}/{private_key}"))
                    .send()
            },
        )
        .await;

        cleanup_with_client(alt_client, &dst_bucket, &["copied", "copied-denied"]).await;
        cleanup(&src_bucket, &[public_key, private_key]).await;
    });
}

#[test]
fn test_bucket_policy_put_object_existing_tag_condition_is_rejected_for_mixed_copy_statement() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let result = client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": ["s3:GetObject", "s3:PutObject"],
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:ExistingObjectTag/security": "public"
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

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_get_object_acl_existing_tag_condition() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let public_key = "publictag-acl-get";
        let private_key = "privatetag-acl-get";

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key(public_key)
            .body(ByteStream::from_static(b"tagged-body"))
            .send()
            .await
            .unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key(private_key)
            .body(ByteStream::from_static(b"tagged-body"))
            .send()
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(public_key)
            .tagging(simple_bucket_tagging("security", "public"))
            .send()
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(private_key)
            .tagging(simple_bucket_tagging("security", "private"))
            .send()
            .await
            .unwrap();

        let denied = alt_client
            .get_object_acl()
            .bucket(&bucket)
            .key(public_key)
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:GetObjectAcl",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:ExistingObjectTag/security": "public"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let acl = eventually_ok_with_retry(
            "GetObjectAcl with ExistingObjectTag bucket policy",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .get_object_acl()
                    .bucket(&bucket)
                    .key(public_key)
                    .send()
            },
        )
        .await;
        assert!(acl.owner().is_some(), "expected owner in GetObjectAcl");

        let private_denied = alt_client
            .get_object_acl()
            .bucket(&bucket)
            .key(private_key)
            .send()
            .await;
        assert_eq!(err_status(&private_denied), 403);
        assert_s3_err_code(&private_denied, "AccessDenied");

        cleanup(&bucket, &[public_key, private_key]).await;
    });
}

#[test]
fn test_bucket_policy_put_object_acl_existing_tag_condition() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let public_key = "publictag-acl-put";
        let private_key = "privatetag-acl-put";

        let bucket = create_bucket_allowing_public_policy(client).await;
        set_object_writer_ownership(&bucket).await;
        client
            .put_object()
            .bucket(&bucket)
            .key(public_key)
            .body(ByteStream::from_static(b"tagged-body"))
            .send()
            .await
            .unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key(private_key)
            .body(ByteStream::from_static(b"tagged-body"))
            .send()
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(public_key)
            .tagging(simple_bucket_tagging("security", "public"))
            .send()
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(private_key)
            .tagging(simple_bucket_tagging("security", "private"))
            .send()
            .await
            .unwrap();

        let alt_id = client_canonical_id(alt_client).await;
        let grant_read_header = format!("id=\"{alt_id}\"");

        let denied = alt_client
            .put_object_acl()
            .bucket(&bucket)
            .key(public_key)
            .customize()
            .mutate_request({
                let grant_read_header = grant_read_header.clone();
                move |req| {
                    req.headers_mut()
                        .insert("x-amz-grant-read", grant_read_header.clone());
                }
            })
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:PutObjectAcl",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:ExistingObjectTag/security": "public"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_ok_with_retry(
            "PutObjectAcl with ExistingObjectTag bucket policy",
            60,
            std::time::Duration::from_millis(500),
            || {
                let grant_read_header = grant_read_header.clone();
                alt_client
                    .put_object_acl()
                    .bucket(&bucket)
                    .key(public_key)
                    .customize()
                    .mutate_request(move |req| {
                        req.headers_mut()
                            .insert("x-amz-grant-read", grant_read_header.clone());
                    })
                    .send()
            },
        )
        .await;

        let owner_view = client
            .get_object_acl()
            .bucket(&bucket)
            .key(public_key)
            .send()
            .await
            .unwrap();
        assert!(
            has_grant(owner_view.grants(), Permission::Read, Some(&alt_id)),
            "expected READ grant for alternate account, got {:?}",
            owner_view.grants()
        );

        let private_denied = alt_client
            .put_object_acl()
            .bucket(&bucket)
            .key(private_key)
            .customize()
            .mutate_request({
                let grant_read_header = grant_read_header.clone();
                move |req| {
                    req.headers_mut()
                        .insert("x-amz-grant-read", grant_read_header.clone());
                }
            })
            .send()
            .await;
        assert_eq!(err_status(&private_denied), 403);
        assert_s3_err_code(&private_denied, "AccessDenied");

        cleanup(&bucket, &[public_key, private_key]).await;
    });
}

#[test]
fn test_bucket_policy_put_object_acl_cross_account_allow() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        set_object_writer_ownership(&bucket).await;
        client
            .put_object()
            .bucket(&bucket)
            .key("owned-by-bucket")
            .body(ByteStream::from_static(b"owned-by-bucket"))
            .send()
            .await
            .unwrap();

        let alt_id = client_canonical_id(alt_client).await;
        let grant_read_header = format!("id=\"{alt_id}\"");

        let denied = alt_client
            .put_object_acl()
            .bucket(&bucket)
            .key("owned-by-bucket")
            .customize()
            .mutate_request({
                let grant_read_header = grant_read_header.clone();
                move |req| {
                    req.headers_mut()
                        .insert("x-amz-grant-read", grant_read_header.clone());
                }
            })
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                principal,
                "Allow",
                "s3:PutObjectAcl",
                bucket_wildcard_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        eventually_ok_with_retry(
            "PutObjectAcl with bucket policy",
            60,
            std::time::Duration::from_millis(500),
            || {
                let grant_read_header = grant_read_header.clone();
                alt_client
                    .put_object_acl()
                    .bucket(&bucket)
                    .key("owned-by-bucket")
                    .customize()
                    .mutate_request(move |req| {
                        req.headers_mut()
                            .insert("x-amz-grant-read", grant_read_header.clone());
                    })
                    .send()
            },
        )
        .await;

        let owner_view = client
            .get_object_acl()
            .bucket(&bucket)
            .key("owned-by-bucket")
            .send()
            .await
            .unwrap();
        assert!(
            has_grant(owner_view.grants(), Permission::Read, Some(&alt_id)),
            "expected READ grant for alternate account, got {:?}",
            owner_view.grants()
        );

        let alt_get = eventually_ok_with_retry(
            "GetObject after PutObjectAcl",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .get_object()
                    .bucket(&bucket)
                    .key("owned-by-bucket")
                    .send()
            },
        )
        .await;
        assert_eq!(
            alt_get.body.collect().await.unwrap().into_bytes().as_ref(),
            b"owned-by-bucket"
        );

        cleanup(&bucket, &["owned-by-bucket"]).await;
    });
}

#[test]
fn test_bucket_policy_put_object_canned_acl_uses_put_object_permission() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_bucket_allowing_public_policy(client).await;
        set_object_writer_ownership(&bucket).await;

        let denied = alt_client
            .put_object()
            .bucket(&bucket)
            .key("public-read")
            .acl(ObjectCannedAcl::PublicRead)
            .body(ByteStream::from_static(b"public-read"))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                principal,
                "Allow",
                "s3:PutObject",
                bucket_wildcard_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        eventually_ok_with_retry(
            "PutObject with canned ACL under PutObject policy",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("public-read")
                    .acl(ObjectCannedAcl::PublicRead)
                    .body(ByteStream::from_static(b"public-read"))
                    .send()
            },
        )
        .await;

        let url = format!("{}/{}/public-read", CTX.endpoint(), bucket);
        let mut last = None;
        for _ in 0..60 {
            let (status, body) = anonymous_get_status_and_body(&url);
            if status == 200 && body == "public-read" {
                cleanup(&bucket, &["public-read"]).await;
                return;
            }
            last = Some((status, body));
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }

        cleanup(&bucket, &["public-read"]).await;
        panic!("anonymous GET did not observe public-read ACL: {last:?}");
    });
}

#[test]
fn test_bucket_policy_put_object_deny_on_public_acl() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": principal.clone(),
                    "Action": "s3:PutObject",
                    "Resource": bucket_wildcard_resource(&bucket),
                },
                {
                    "Effect": "Deny",
                    "Principal": principal,
                    "Action": "s3:PutObject",
                    "Resource": bucket_wildcard_resource(&bucket),
                    "Condition": {
                        "StringLike": {
                            "s3:x-amz-acl": "public*"
                        }
                    }
                }
            ]
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        alt_client
            .put_object()
            .bucket(&bucket)
            .key("private-key")
            .body(ByteStream::from_static(b"private-key"))
            .send()
            .await
            .unwrap();

        let denied = alt_client
            .put_object()
            .bucket(&bucket)
            .key("public-key")
            .acl(ObjectCannedAcl::PublicRead)
            .body(ByteStream::from_static(b"public-key"))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        let streaming_denied = alt_client
            .put_object()
            .bucket(&bucket)
            .key("public-streaming-key")
            .acl(ObjectCannedAcl::PublicRead)
            .body(ByteStream::from(vec![
                0x5Au8;
                server_core::coordinator::INTERNAL_SEGMENT_SIZE
                    + 1
            ]))
            .send()
            .await;
        assert_eq!(err_status(&streaming_denied), 403);
        assert_s3_err_code(&streaming_denied, "AccessDenied");

        cleanup(
            &bucket,
            &["private-key", "public-key", "public-streaming-key"],
        )
        .await;
    });
}

#[test]
fn test_bucket_policy_put_object_acl_null_condition_treats_absent_header_as_null() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        set_object_writer_ownership(&bucket).await;

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": principal.clone(),
                    "Action": "s3:PutObject",
                    "Resource": bucket_wildcard_resource(&bucket),
                },
                {
                    "Effect": "Deny",
                    "Principal": principal,
                    "Action": "s3:PutObject",
                    "Resource": bucket_wildcard_resource(&bucket),
                    "Condition": {
                        "Null": {
                            "s3:x-amz-acl": "true"
                        }
                    }
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_result_matches(
            "PutObject denied when x-amz-acl is absent under Null condition",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("denied")
                    .body(ByteStream::from_static(b"denied"))
                    .send()
            },
            |result| {
                result.as_ref().err().is_some_and(|err| {
                    err.raw_response().map(|r| r.status().as_u16()) == Some(403)
                        && err.as_service_error().and_then(ProvideErrorMetadata::code)
                            == Some("AccessDenied")
                })
            },
        )
        .await;

        eventually_ok_with_retry(
            "PutObject with explicit private ACL under Null condition",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("allowed")
                    .acl(ObjectCannedAcl::Private)
                    .body(ByteStream::from_static(b"allowed"))
                    .send()
            },
        )
        .await;

        let allowed = alt_client
            .get_object()
            .bucket(&bucket)
            .key("allowed")
            .send()
            .await
            .unwrap();
        let body = allowed.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), b"allowed");

        cleanup(&bucket, &["allowed", "denied"]).await;
    });
}

#[test]
fn test_bucket_policy_put_object_acl_string_not_equals_treats_absent_header_as_not_equal() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        set_object_writer_ownership(&bucket).await;

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": principal.clone(),
                    "Action": "s3:PutObject",
                    "Resource": bucket_wildcard_resource(&bucket),
                },
                {
                    "Effect": "Deny",
                    "Principal": principal,
                    "Action": "s3:PutObject",
                    "Resource": bucket_wildcard_resource(&bucket),
                    "Condition": {
                        "StringNotEquals": {
                            "s3:x-amz-acl": "private"
                        }
                    }
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_result_matches(
            "PutObject denied when x-amz-acl is absent under StringNotEquals",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("absent")
                    .body(ByteStream::from_static(b"absent"))
                    .send()
            },
            |result| {
                result.as_ref().err().is_some_and(|err| {
                    err.raw_response().map(|r| r.status().as_u16()) == Some(403)
                        && err.as_service_error().and_then(ProvideErrorMetadata::code)
                            == Some("AccessDenied")
                })
            },
        )
        .await;

        eventually_result_matches(
            "PutObject denied when x-amz-acl does not equal private",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("wrong")
                    .acl(ObjectCannedAcl::BucketOwnerFullControl)
                    .body(ByteStream::from_static(b"wrong"))
                    .send()
            },
            |result| {
                result.as_ref().err().is_some_and(|err| {
                    err.raw_response().map(|r| r.status().as_u16()) == Some(403)
                        && err.as_service_error().and_then(ProvideErrorMetadata::code)
                            == Some("AccessDenied")
                })
            },
        )
        .await;

        eventually_ok_with_retry(
            "PutObject with x-amz-acl=private under StringNotEquals",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("allowed")
                    .acl(ObjectCannedAcl::Private)
                    .body(ByteStream::from_static(b"allowed"))
                    .send()
            },
        )
        .await;

        let allowed = alt_client
            .get_object()
            .bucket(&bucket)
            .key("allowed")
            .send()
            .await
            .unwrap();
        let body = allowed.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), b"allowed");

        cleanup(&bucket, &["absent", "wrong", "allowed"]).await;
    });
}

#[test]
fn test_bucket_policy_put_object_acl_null_treats_absent_header_as_null() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        set_object_writer_ownership(&bucket).await;
        client
            .put_object()
            .bucket(&bucket)
            .key("owned-by-bucket")
            .body(ByteStream::from_static(b"owned-by-bucket"))
            .send()
            .await
            .unwrap();

        let alt_id = client_canonical_id(alt_client).await;
        let grant_read_header = format!("id=\"{alt_id}\"");
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": principal.clone(),
                    "Action": "s3:PutObjectAcl",
                    "Resource": bucket_wildcard_resource(&bucket),
                },
                {
                    "Effect": "Deny",
                    "Principal": principal,
                    "Action": "s3:PutObjectAcl",
                    "Resource": bucket_wildcard_resource(&bucket),
                    "Condition": {
                        "Null": {
                            "s3:x-amz-acl": "true"
                        }
                    }
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_result_matches(
            "PutObjectAcl denied when x-amz-acl is absent under Null condition",
            60,
            std::time::Duration::from_millis(500),
            || {
                let grant_read_header = grant_read_header.clone();
                alt_client
                    .put_object_acl()
                    .bucket(&bucket)
                    .key("owned-by-bucket")
                    .customize()
                    .mutate_request(move |req| {
                        req.headers_mut()
                            .insert("x-amz-grant-read", grant_read_header.clone());
                    })
                    .send()
            },
            |result| {
                result.as_ref().err().is_some_and(|err| {
                    err.raw_response().map(|r| r.status().as_u16()) == Some(403)
                        && err.as_service_error().and_then(ProvideErrorMetadata::code)
                            == Some("AccessDenied")
                })
            },
        )
        .await;

        eventually_ok_with_retry(
            "PutObjectAcl with explicit private ACL under Null condition",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object_acl()
                    .bucket(&bucket)
                    .key("owned-by-bucket")
                    .acl(ObjectCannedAcl::Private)
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &["owned-by-bucket"]).await;
    });
}

#[test]
fn test_bucket_policy_put_object_acl_string_not_equals_treats_absent_acl_header_as_not_equal() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        set_object_writer_ownership(&bucket).await;
        client
            .put_object()
            .bucket(&bucket)
            .key("owned-by-bucket")
            .body(ByteStream::from_static(b"owned-by-bucket"))
            .send()
            .await
            .unwrap();

        let alt_id = client_canonical_id(alt_client).await;
        let grant_read_header = format!("id=\"{alt_id}\"");
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": principal.clone(),
                    "Action": "s3:PutObjectAcl",
                    "Resource": bucket_wildcard_resource(&bucket),
                },
                {
                    "Effect": "Deny",
                    "Principal": principal,
                    "Action": "s3:PutObjectAcl",
                    "Resource": bucket_wildcard_resource(&bucket),
                    "Condition": {
                        "StringNotEquals": {
                            "s3:x-amz-acl": "private"
                        }
                    }
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_result_matches(
            "PutObjectAcl denied when x-amz-acl is absent under StringNotEquals",
            60,
            std::time::Duration::from_millis(500),
            || {
                let grant_read_header = grant_read_header.clone();
                alt_client
                    .put_object_acl()
                    .bucket(&bucket)
                    .key("owned-by-bucket")
                    .customize()
                    .mutate_request(move |req| {
                        req.headers_mut()
                            .insert("x-amz-grant-read", grant_read_header.clone());
                    })
                    .send()
            },
            |result| {
                result.as_ref().err().is_some_and(|err| {
                    err.raw_response().map(|r| r.status().as_u16()) == Some(403)
                        && err.as_service_error().and_then(ProvideErrorMetadata::code)
                            == Some("AccessDenied")
                })
            },
        )
        .await;

        eventually_result_matches(
            "PutObjectAcl denied when x-amz-acl does not equal private",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object_acl()
                    .bucket(&bucket)
                    .key("owned-by-bucket")
                    .acl(ObjectCannedAcl::BucketOwnerFullControl)
                    .send()
            },
            |result| {
                result.as_ref().err().is_some_and(|err| {
                    err.raw_response().map(|r| r.status().as_u16()) == Some(403)
                        && err.as_service_error().and_then(ProvideErrorMetadata::code)
                            == Some("AccessDenied")
                })
            },
        )
        .await;

        eventually_ok_with_retry(
            "PutObjectAcl with x-amz-acl=private under StringNotEquals",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object_acl()
                    .bucket(&bucket)
                    .key("owned-by-bucket")
                    .acl(ObjectCannedAcl::Private)
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &["owned-by-bucket"]).await;
    });
}

#[test]
fn test_bucket_policy_put_obj_grant_read_condition() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        let control_bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        s3_tests::create_bucket(client, &control_bucket)
            .await
            .unwrap();
        set_object_writer_ownership(&bucket).await;
        set_object_writer_ownership(&control_bucket).await;

        let owner_id = canonical_owner_id(client, &bucket).await;
        let grant_read_header = format!("id=\"{owner_id}\"");
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal.clone(),
                "Action": "s3:PutObject",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:x-amz-grant-read": grant_read_header.clone()
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();
        client
            .put_bucket_policy()
            .bucket(&control_bucket)
            .policy(bucket_policy_document(
                principal,
                "Allow",
                "s3:PutObject",
                bucket_wildcard_resource(&control_bucket),
            ))
            .send()
            .await
            .unwrap();

        eventually_result_matches(
            "PutObject denied without matching grant-read header",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("denied")
                    .body(ByteStream::from_static(b"denied"))
                    .send()
            },
            |result| {
                result.as_ref().err().is_some_and(|err| {
                    err.raw_response().map(|r| r.status().as_u16()) == Some(403)
                        && err.as_service_error().and_then(ProvideErrorMetadata::code)
                            == Some("AccessDenied")
                })
            },
        )
        .await;

        eventually_ok_with_retry(
            "PutObject with grant-read condition",
            60,
            std::time::Duration::from_millis(500),
            || {
                let grant_read_header = grant_read_header.clone();
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("allowed")
                    .body(ByteStream::from_static(b"allowed"))
                    .customize()
                    .mutate_request(move |req| {
                        req.headers_mut()
                            .insert("x-amz-grant-read", grant_read_header.clone());
                    })
                    .send()
            },
        )
        .await;

        eventually_ok_with_retry(
            "PutObject control write without grant-read header",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object()
                    .bucket(&control_bucket)
                    .key("control")
                    .body(ByteStream::from_static(b"control"))
                    .send()
            },
        )
        .await;

        let allowed = eventually_ok_with_retry(
            "Bucket owner GetObject after grant-read PutObject",
            60,
            std::time::Duration::from_millis(500),
            || client.get_object().bucket(&bucket).key("allowed").send(),
        )
        .await;
        let body = allowed.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), b"allowed");

        eventually_result_matches(
            "Bucket owner GetObject remains denied without grant-read PutObject",
            60,
            std::time::Duration::from_millis(500),
            || {
                client
                    .get_object()
                    .bucket(&control_bucket)
                    .key("control")
                    .send()
            },
            |result| {
                result.as_ref().err().is_some_and(|err| {
                    err.raw_response().map(|r| r.status().as_u16()) == Some(403)
                        && err.as_service_error().and_then(ProvideErrorMetadata::code)
                            == Some("AccessDenied")
                })
            },
        )
        .await;

        cleanup(&bucket, &["allowed", "denied"]).await;
        cleanup(&control_bucket, &["control"]).await;
    });
}

#[test]
fn test_bucket_policy_grant_read_condition_uses_raw_header_spacing() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        set_object_writer_ownership(&bucket).await;

        let owner_id = canonical_owner_id(client, &bucket).await;
        let canonical_grant = format!("id=\"{owner_id}\"");
        let spaced_grant = format!("id = \"{owner_id}\"");

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Deny",
                        "Principal": "*",
                        "Action": "s3:PutObject",
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:x-amz-grant-read": canonical_grant
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_access_denied("PutObject denied with canonical grant-read header", || {
            client
                .put_object()
                .bucket(&bucket)
                .key("canonical-denied")
                .body(ByteStream::from_static(b"canonical-denied"))
                .customize()
                .mutate_request({
                    let canonical_grant = canonical_grant.clone();
                    move |req| {
                        req.headers_mut()
                            .insert("x-amz-grant-read", canonical_grant.clone());
                    }
                })
                .send()
        })
        .await;

        eventually_ok("PutObject with spaced grant-read header", || {
            let spaced_grant = spaced_grant.clone();
            client
                .put_object()
                .bucket(&bucket)
                .key("spaced-allowed")
                .body(ByteStream::from_static(b"spaced-allowed"))
                .customize()
                .mutate_request(move |req| {
                    req.headers_mut()
                        .insert("x-amz-grant-read", spaced_grant.clone());
                })
                .send()
        })
        .await;

        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key("spaced-allowed")
            .send()
            .await
            .unwrap();
        assert!(has_grant(acl.grants(), Permission::Read, Some(&owner_id)));

        cleanup(&bucket, &["canonical-denied", "spaced-allowed"]).await;
    });
}

#[test]
fn test_bucket_policy_put_obj_grant_read_acp_condition() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        let control_bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        s3_tests::create_bucket(client, &control_bucket)
            .await
            .unwrap();
        set_object_writer_ownership(&bucket).await;
        set_object_writer_ownership(&control_bucket).await;

        let owner_id = canonical_owner_id(client, &bucket).await;
        let grant_read_acp_header = format!("id=\"{owner_id}\"");
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal.clone(),
                "Action": "s3:PutObject",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:x-amz-grant-read-acp": grant_read_acp_header.clone()
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();
        client
            .put_bucket_policy()
            .bucket(&control_bucket)
            .policy(bucket_policy_document(
                principal,
                "Allow",
                "s3:PutObject",
                bucket_wildcard_resource(&control_bucket),
            ))
            .send()
            .await
            .unwrap();

        eventually_result_matches(
            "PutObject denied without matching grant-read-acp header",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("denied")
                    .body(ByteStream::from_static(b"denied"))
                    .send()
            },
            |result| {
                result.as_ref().err().is_some_and(|err| {
                    err.raw_response().map(|r| r.status().as_u16()) == Some(403)
                        && err.as_service_error().and_then(ProvideErrorMetadata::code)
                            == Some("AccessDenied")
                })
            },
        )
        .await;

        eventually_ok_with_retry(
            "PutObject with grant-read-acp condition",
            60,
            std::time::Duration::from_millis(500),
            || {
                let grant_read_acp_header = grant_read_acp_header.clone();
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("allowed")
                    .body(ByteStream::from_static(b"allowed"))
                    .customize()
                    .mutate_request(move |req| {
                        req.headers_mut()
                            .insert("x-amz-grant-read-acp", grant_read_acp_header.clone());
                    })
                    .send()
            },
        )
        .await;

        eventually_ok_with_retry(
            "PutObject control write without grant-read-acp header",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object()
                    .bucket(&control_bucket)
                    .key("control")
                    .body(ByteStream::from_static(b"control"))
                    .send()
            },
        )
        .await;

        let allowed_acl = eventually_ok_with_retry(
            "Bucket owner GetObjectAcl after grant-read-acp PutObject",
            60,
            std::time::Duration::from_millis(500),
            || {
                client
                    .get_object_acl()
                    .bucket(&bucket)
                    .key("allowed")
                    .send()
            },
        )
        .await;
        assert!(
            has_grant(allowed_acl.grants(), Permission::ReadAcp, Some(&owner_id),),
            "expected READ_ACP grant for bucket owner, got {:?}",
            allowed_acl.grants()
        );

        let control_denied = client
            .get_object_acl()
            .bucket(&control_bucket)
            .key("control")
            .send()
            .await;
        assert_eq!(err_status(&control_denied), 403);
        assert_s3_err_code(&control_denied, "AccessDenied");

        cleanup(&bucket, &["allowed", "denied"]).await;
        cleanup(&control_bucket, &["control"]).await;
    });
}

#[test]
fn test_bucket_policy_put_obj_grant_write_condition() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        let control_bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        s3_tests::create_bucket(client, &control_bucket)
            .await
            .unwrap();
        set_object_writer_ownership(&bucket).await;
        set_object_writer_ownership(&control_bucket).await;

        let owner_id = canonical_owner_id(client, &bucket).await;
        let grant_write_header = format!("id=\"{owner_id}\"");
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal.clone(),
                "Action": "s3:PutObject",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:x-amz-grant-write": grant_write_header.clone()
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();
        client
            .put_bucket_policy()
            .bucket(&control_bucket)
            .policy(bucket_policy_document(
                principal,
                "Allow",
                "s3:PutObject",
                bucket_wildcard_resource(&control_bucket),
            ))
            .send()
            .await
            .unwrap();

        eventually_result_matches(
            "PutObject denied without matching grant-write header",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("denied")
                    .body(ByteStream::from_static(b"denied"))
                    .send()
            },
            |result| {
                result.as_ref().err().is_some_and(|err| {
                    err.raw_response().map(|r| r.status().as_u16()) == Some(403)
                        && err.as_service_error().and_then(ProvideErrorMetadata::code)
                            == Some("AccessDenied")
                })
            },
        )
        .await;

        eventually_ok_with_retry(
            "PutObject with grant-write condition",
            60,
            std::time::Duration::from_millis(500),
            || {
                let grant_write_header = grant_write_header.clone();
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("allowed")
                    .body(ByteStream::from_static(b"allowed"))
                    .customize()
                    .mutate_request(move |req| {
                        req.headers_mut()
                            .insert("x-amz-grant-write", grant_write_header.clone());
                    })
                    .send()
            },
        )
        .await;

        eventually_ok_with_retry(
            "PutObject control write without grant-write header",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object()
                    .bucket(&control_bucket)
                    .key("control")
                    .body(ByteStream::from_static(b"control"))
                    .send()
            },
        )
        .await;

        let allowed_acl = alt_client
            .get_object_acl()
            .bucket(&bucket)
            .key("allowed")
            .send()
            .await
            .unwrap();
        assert!(
            has_grant(allowed_acl.grants(), Permission::Write, Some(&owner_id)),
            "expected WRITE grant for bucket owner, got {:?}",
            allowed_acl.grants()
        );

        let control_acl = alt_client
            .get_object_acl()
            .bucket(&control_bucket)
            .key("control")
            .send()
            .await
            .unwrap();
        assert!(
            !has_grant(control_acl.grants(), Permission::Write, Some(&owner_id)),
            "did not expect WRITE grant for bucket owner, got {:?}",
            control_acl.grants()
        );

        cleanup(&bucket, &["allowed", "denied"]).await;
        cleanup(&control_bucket, &["control"]).await;
    });
}

#[test]
fn test_bucket_policy_put_obj_grant_write_acp_condition() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        let control_bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        s3_tests::create_bucket(client, &control_bucket)
            .await
            .unwrap();
        set_object_writer_ownership(&bucket).await;
        set_object_writer_ownership(&control_bucket).await;

        let owner_id = canonical_owner_id(client, &bucket).await;
        let grant_write_acp_header = format!("id=\"{owner_id}\"");
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal.clone(),
                "Action": "s3:PutObject",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:x-amz-grant-write-acp": grant_write_acp_header.clone()
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();
        client
            .put_bucket_policy()
            .bucket(&control_bucket)
            .policy(bucket_policy_document(
                principal,
                "Allow",
                "s3:PutObject",
                bucket_wildcard_resource(&control_bucket),
            ))
            .send()
            .await
            .unwrap();

        eventually_result_matches(
            "PutObject denied without matching grant-write-acp header",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("denied")
                    .body(ByteStream::from_static(b"denied"))
                    .send()
            },
            |result| {
                result.as_ref().err().is_some_and(|err| {
                    err.raw_response().map(|r| r.status().as_u16()) == Some(403)
                        && err.as_service_error().and_then(ProvideErrorMetadata::code)
                            == Some("AccessDenied")
                })
            },
        )
        .await;

        eventually_ok_with_retry(
            "PutObject with grant-write-acp condition",
            60,
            std::time::Duration::from_millis(500),
            || {
                let grant_write_acp_header = grant_write_acp_header.clone();
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("allowed")
                    .body(ByteStream::from_static(b"allowed"))
                    .customize()
                    .mutate_request(move |req| {
                        req.headers_mut()
                            .insert("x-amz-grant-write-acp", grant_write_acp_header.clone());
                    })
                    .send()
            },
        )
        .await;

        eventually_ok_with_retry(
            "PutObject control write without grant-write-acp header",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object()
                    .bucket(&control_bucket)
                    .key("control")
                    .body(ByteStream::from_static(b"control"))
                    .send()
            },
        )
        .await;

        eventually_ok_with_retry(
            "Bucket owner PutObjectAcl after grant-write-acp PutObject",
            60,
            std::time::Duration::from_millis(500),
            || {
                client
                    .put_object_acl()
                    .bucket(&bucket)
                    .key("allowed")
                    .acl(ObjectCannedAcl::Private)
                    .send()
            },
        )
        .await;

        let control_denied = client
            .put_object_acl()
            .bucket(&control_bucket)
            .key("control")
            .acl(ObjectCannedAcl::Private)
            .send()
            .await;
        assert_eq!(err_status(&control_denied), 403);
        assert_s3_err_code(&control_denied, "AccessDenied");

        cleanup(&bucket, &["allowed", "denied"]).await;
        cleanup(&control_bucket, &["control"]).await;
    });
}

#[test]
fn test_bucket_policy_put_object_public_allow_is_blocked_by_restrict_public_buckets() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_bucket_allowing_public_policy(client).await;
        let public_policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": "s3:PutObject",
                "Resource": bucket_wildcard_resource(&bucket),
            }],
        })
        .to_string();
        match client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(public_policy)
            .send()
            .await
        {
            Ok(_) => {}
            Err(err) => {
                if std::env::var("S3_TEST_ENDPOINT").is_ok()
                    && err.raw_response().map(|resp| resp.status().as_u16()) == Some(403)
                {
                    cleanup(&bucket, &[]).await;
                    panic!(
                        "account-level S3 Block Public Access must allow public bucket policies for AWS s3-tests; put_bucket_policy failed while setting up RestrictPublicBuckets PutObject coverage: {err:?}"
                    );
                }
                panic!("put_bucket_policy failed: {err:?}");
            }
        }

        eventually_ok_with_retry(
            "PutObject with public bucket policy",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("public-allowed")
                    .body(ByteStream::from_static(b"public-allowed"))
                    .send()
            },
        )
        .await;

        let pab = PublicAccessBlockConfiguration::builder()
            .block_public_acls(false)
            .ignore_public_acls(false)
            .block_public_policy(false)
            .restrict_public_buckets(true)
            .build();
        client
            .put_public_access_block()
            .bucket(&bucket)
            .public_access_block_configuration(pab)
            .send()
            .await
            .unwrap();

        eventually_result_matches(
            "PutObject denied by RestrictPublicBuckets under public policy",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("restricted")
                    .body(ByteStream::from_static(b"restricted"))
                    .send()
            },
            |result| {
                result.as_ref().err().is_some_and(|err| {
                    err.raw_response().map(|r| r.status().as_u16()) == Some(403)
                        && err.as_service_error().and_then(ProvideErrorMetadata::code)
                            == Some("AccessDenied")
                })
            },
        )
        .await;

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                alt_policy_principal(),
                "Allow",
                "s3:PutObject",
                bucket_wildcard_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        eventually_ok_with_retry(
            "PutObject with fixed principal under RestrictPublicBuckets",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("explicit-allowed")
                    .body(ByteStream::from_static(b"explicit-allowed"))
                    .send()
            },
        )
        .await;

        client
            .put_object()
            .bucket(&bucket)
            .key("owner")
            .body(ByteStream::from_static(b"owner"))
            .send()
            .await
            .unwrap();

        cleanup(
            &bucket,
            &["public-allowed", "restricted", "explicit-allowed", "owner"],
        )
        .await;
    });
}

#[test]
fn test_bucket_policy_put_object_acl_deny_on_public_acl() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_bucket_allowing_public_policy(client).await;
        set_object_writer_ownership(&bucket).await;
        client
            .put_object()
            .bucket(&bucket)
            .key("owned-by-bucket")
            .body(ByteStream::from_static(b"owned-by-bucket"))
            .send()
            .await
            .unwrap();

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": principal.clone(),
                    "Action": "s3:PutObjectAcl",
                    "Resource": bucket_wildcard_resource(&bucket),
                },
                {
                    "Effect": "Deny",
                    "Principal": principal,
                    "Action": "s3:PutObjectAcl",
                    "Resource": bucket_wildcard_resource(&bucket),
                    "Condition": {
                        "StringLike": {
                            "s3:x-amz-acl": "public*"
                        }
                    }
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_ok_with_retry(
            "PutObjectAcl private under bucket policy",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object_acl()
                    .bucket(&bucket)
                    .key("owned-by-bucket")
                    .acl(ObjectCannedAcl::Private)
                    .send()
            },
        )
        .await;

        let denied = alt_client
            .put_object_acl()
            .bucket(&bucket)
            .key("owned-by-bucket")
            .acl(ObjectCannedAcl::PublicRead)
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        let owner_view = client
            .get_object_acl()
            .bucket(&bucket)
            .key("owned-by-bucket")
            .send()
            .await
            .unwrap();
        assert!(
            !has_grant(owner_view.grants(), Permission::Read, None),
            "did not expect READ grant for AllUsers, got {:?}",
            owner_view.grants()
        );

        cleanup(&bucket, &["owned-by-bucket"]).await;
    });
}

#[test]
fn test_bucket_policy_put_object_version_acl_cross_account_allow() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        set_object_writer_ownership(&bucket).await;
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
        let put = client
            .put_object()
            .bucket(&bucket)
            .key("versioned")
            .body(ByteStream::from_static(b"versioned-body"))
            .send()
            .await
            .unwrap();
        let version_id = put
            .version_id()
            .expect("expected VersionId for versioned object")
            .to_string();

        let alt_id = client_canonical_id(alt_client).await;
        let grant_read_header = format!("id=\"{alt_id}\"");

        let denied = alt_client
            .put_object_acl()
            .bucket(&bucket)
            .key("versioned")
            .version_id(&version_id)
            .customize()
            .mutate_request({
                let grant_read_header = grant_read_header.clone();
                move |req| {
                    req.headers_mut()
                        .insert("x-amz-grant-read", grant_read_header.clone());
                }
            })
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:PutObjectVersionAcl",
                "Resource": bucket_wildcard_resource(&bucket),
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_ok_with_retry(
            "PutObjectAcl on version with bucket policy",
            60,
            std::time::Duration::from_millis(500),
            || {
                let grant_read_header = grant_read_header.clone();
                alt_client
                    .put_object_acl()
                    .bucket(&bucket)
                    .key("versioned")
                    .version_id(&version_id)
                    .customize()
                    .mutate_request(move |req| {
                        req.headers_mut()
                            .insert("x-amz-grant-read", grant_read_header.clone());
                    })
                    .send()
            },
        )
        .await;

        let owner_view = client
            .get_object_acl()
            .bucket(&bucket)
            .key("versioned")
            .version_id(&version_id)
            .send()
            .await
            .unwrap();
        assert!(
            has_grant(owner_view.grants(), Permission::Read, Some(&alt_id)),
            "expected READ grant for alternate account on version, got {:?}",
            owner_view.grants()
        );

        let alt_get = eventually_ok_with_retry(
            "GetObject version after PutObjectVersionAcl",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .get_object()
                    .bucket(&bucket)
                    .key("versioned")
                    .version_id(&version_id)
                    .send()
            },
        )
        .await;
        assert_eq!(
            alt_get.body.collect().await.unwrap().into_bytes().as_ref(),
            b"versioned-body"
        );

        client
            .delete_object()
            .bucket(&bucket)
            .key("versioned")
            .version_id(&version_id)
            .send()
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_version_id_condition_get_object_version() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
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

        let allowed_version_id = client
            .put_object()
            .bucket(&bucket)
            .key("versioned")
            .body(ByteStream::from_static(b"allowed-version"))
            .send()
            .await
            .unwrap()
            .version_id()
            .expect("expected VersionId for versioned object")
            .to_string();
        let denied_version_id = client
            .put_object()
            .bucket(&bucket)
            .key("versioned")
            .body(ByteStream::from_static(b"denied-version"))
            .send()
            .await
            .unwrap()
            .version_id()
            .expect("expected VersionId for versioned object")
            .to_string();

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:GetObjectVersion",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:versionid": allowed_version_id.clone()
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let allowed = eventually_ok_with_retry(
            "GetObjectVersion allowed by s3:versionid condition",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .get_object()
                    .bucket(&bucket)
                    .key("versioned")
                    .version_id(&allowed_version_id)
                    .send()
            },
        )
        .await;
        assert_eq!(
            allowed.body.collect().await.unwrap().into_bytes().as_ref(),
            b"allowed-version"
        );

        eventually_access_denied(
            "GetObjectVersion denied by nonmatching s3:versionid condition",
            || {
                alt_client
                    .get_object()
                    .bucket(&bucket)
                    .key("versioned")
                    .version_id(&denied_version_id)
                    .send()
            },
        )
        .await;

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_bucket_policy_version_id_condition_get_object_version_attributes() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
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

        let allowed_version_id = client
            .put_object()
            .bucket(&bucket)
            .key("attributes-version")
            .body(ByteStream::from_static(b"allowed-version"))
            .send()
            .await
            .unwrap()
            .version_id()
            .expect("expected VersionId for versioned object")
            .to_string();
        let denied_version_id = client
            .put_object()
            .bucket(&bucket)
            .key("attributes-version")
            .body(ByteStream::from_static(b"denied-version"))
            .send()
            .await
            .unwrap()
            .version_id()
            .expect("expected VersionId for versioned object")
            .to_string();

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": ["s3:GetObjectVersion", "s3:GetObjectVersionAttributes"],
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:versionid": allowed_version_id.clone()
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let attrs = eventually_ok_with_retry(
            "GetObjectVersionAttributes allowed by s3:versionid condition",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .get_object_attributes()
                    .bucket(&bucket)
                    .key("attributes-version")
                    .version_id(&allowed_version_id)
                    .object_attributes(ObjectAttributes::ObjectSize)
                    .send()
            },
        )
        .await;
        assert_eq!(attrs.object_size(), Some(15));

        eventually_access_denied(
            "GetObjectVersionAttributes denied by nonmatching s3:versionid condition",
            || {
                alt_client
                    .get_object_attributes()
                    .bucket(&bucket)
                    .key("attributes-version")
                    .version_id(&denied_version_id)
                    .object_attributes(ObjectAttributes::ObjectSize)
                    .send()
            },
        )
        .await;

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_bucket_policy_version_id_condition_object_version_acl() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        set_object_writer_ownership(&bucket).await;
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

        let allowed_version_id = client
            .put_object()
            .bucket(&bucket)
            .key("acl-version")
            .body(ByteStream::from_static(b"allowed-version"))
            .send()
            .await
            .unwrap()
            .version_id()
            .expect("expected VersionId for versioned object")
            .to_string();
        let denied_version_id = client
            .put_object()
            .bucket(&bucket)
            .key("acl-version")
            .body(ByteStream::from_static(b"denied-version"))
            .send()
            .await
            .unwrap()
            .version_id()
            .expect("expected VersionId for versioned object")
            .to_string();

        let alt_id = client_canonical_id(alt_client).await;
        let grant_read_header = format!("id=\"{alt_id}\"");
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": ["s3:GetObjectVersionAcl", "s3:PutObjectVersionAcl"],
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:versionid": allowed_version_id.clone()
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_ok_with_retry(
            "PutObjectVersionAcl allowed by s3:versionid condition",
            60,
            std::time::Duration::from_millis(500),
            || {
                let grant_read_header = grant_read_header.clone();
                alt_client
                    .put_object_acl()
                    .bucket(&bucket)
                    .key("acl-version")
                    .version_id(&allowed_version_id)
                    .customize()
                    .mutate_request(move |req| {
                        req.headers_mut()
                            .insert("x-amz-grant-read", grant_read_header.clone());
                    })
                    .send()
            },
        )
        .await;

        eventually_access_denied(
            "PutObjectVersionAcl denied by nonmatching s3:versionid condition",
            || {
                let grant_read_header = grant_read_header.clone();
                alt_client
                    .put_object_acl()
                    .bucket(&bucket)
                    .key("acl-version")
                    .version_id(&denied_version_id)
                    .customize()
                    .mutate_request(move |req| {
                        req.headers_mut()
                            .insert("x-amz-grant-read", grant_read_header.clone());
                    })
                    .send()
            },
        )
        .await;

        let acl = eventually_ok_with_retry(
            "GetObjectVersionAcl allowed by s3:versionid condition",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .get_object_acl()
                    .bucket(&bucket)
                    .key("acl-version")
                    .version_id(&allowed_version_id)
                    .send()
            },
        )
        .await;
        assert!(
            acl.owner().is_some(),
            "expected owner in GetObjectVersionAcl"
        );

        eventually_access_denied(
            "GetObjectVersionAcl denied by nonmatching s3:versionid condition",
            || {
                alt_client
                    .get_object_acl()
                    .bucket(&bucket)
                    .key("acl-version")
                    .version_id(&denied_version_id)
                    .send()
            },
        )
        .await;

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_bucket_policy_version_id_condition_object_version_tagging() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
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

        let allowed_version_id = client
            .put_object()
            .bucket(&bucket)
            .key("tagged-version")
            .body(ByteStream::from_static(b"allowed-version"))
            .send()
            .await
            .unwrap()
            .version_id()
            .expect("expected VersionId for versioned object")
            .to_string();
        let denied_version_id = client
            .put_object()
            .bucket(&bucket)
            .key("tagged-version")
            .body(ByteStream::from_static(b"denied-version"))
            .send()
            .await
            .unwrap()
            .version_id()
            .expect("expected VersionId for versioned object")
            .to_string();

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": [
                    "s3:PutObjectVersionTagging",
                    "s3:GetObjectVersionTagging",
                    "s3:DeleteObjectVersionTagging"
                ],
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:versionid": allowed_version_id.clone()
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_ok_with_retry(
            "PutObjectVersionTagging allowed by s3:versionid condition",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&bucket)
                    .key("tagged-version")
                    .version_id(&allowed_version_id)
                    .tagging(simple_bucket_tagging("security", "allow"))
                    .send()
            },
        )
        .await;

        eventually_access_denied(
            "PutObjectVersionTagging denied by nonmatching s3:versionid condition",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&bucket)
                    .key("tagged-version")
                    .version_id(&denied_version_id)
                    .tagging(simple_bucket_tagging("security", "deny"))
                    .send()
            },
        )
        .await;

        let tags = eventually_ok_with_retry(
            "GetObjectVersionTagging allowed by s3:versionid condition",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .get_object_tagging()
                    .bucket(&bucket)
                    .key("tagged-version")
                    .version_id(&allowed_version_id)
                    .send()
            },
        )
        .await;
        assert_eq!(
            tags.tag_set()
                .iter()
                .map(|tag| (tag.key(), tag.value()))
                .collect::<Vec<_>>(),
            vec![("security", "allow")]
        );

        eventually_access_denied(
            "GetObjectVersionTagging denied by nonmatching s3:versionid condition",
            || {
                alt_client
                    .get_object_tagging()
                    .bucket(&bucket)
                    .key("tagged-version")
                    .version_id(&denied_version_id)
                    .send()
            },
        )
        .await;

        eventually_ok_with_retry(
            "DeleteObjectVersionTagging allowed by s3:versionid condition",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .delete_object_tagging()
                    .bucket(&bucket)
                    .key("tagged-version")
                    .version_id(&allowed_version_id)
                    .send()
            },
        )
        .await;

        eventually_access_denied(
            "DeleteObjectVersionTagging denied by nonmatching s3:versionid condition",
            || {
                alt_client
                    .delete_object_tagging()
                    .bucket(&bucket)
                    .key("tagged-version")
                    .version_id(&denied_version_id)
                    .send()
            },
        )
        .await;

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_bucket_policy_version_id_condition_delete_object_version() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
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

        let allowed_version_id = client
            .put_object()
            .bucket(&bucket)
            .key("delete-version")
            .body(ByteStream::from_static(b"allowed-version"))
            .send()
            .await
            .unwrap()
            .version_id()
            .expect("expected VersionId for versioned object")
            .to_string();
        let denied_version_id = client
            .put_object()
            .bucket(&bucket)
            .key("delete-version")
            .body(ByteStream::from_static(b"denied-version"))
            .send()
            .await
            .unwrap()
            .version_id()
            .expect("expected VersionId for versioned object")
            .to_string();

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:DeleteObjectVersion",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:versionid": allowed_version_id.clone()
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_ok_with_retry(
            "DeleteObjectVersion allowed by s3:versionid condition",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .delete_object()
                    .bucket(&bucket)
                    .key("delete-version")
                    .version_id(&allowed_version_id)
                    .send()
            },
        )
        .await;

        eventually_ok_with_retry(
            "DeleteObjectVersion missing version allowed by s3:versionid request condition",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .delete_object()
                    .bucket(&bucket)
                    .key("delete-version")
                    .version_id(&allowed_version_id)
                    .send()
            },
        )
        .await;

        eventually_access_denied(
            "DeleteObjectVersion denied by nonmatching s3:versionid condition",
            || {
                alt_client
                    .delete_object()
                    .bucket(&bucket)
                    .key("delete-version")
                    .version_id(&denied_version_id)
                    .send()
            },
        )
        .await;

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_bucket_policy_get_object_version_acl_existing_tag_condition() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let public_key = "publictag-version-acl-get";
        let private_key = "privatetag-version-acl-get";

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
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
        let put = client
            .put_object()
            .bucket(&bucket)
            .key(public_key)
            .body(ByteStream::from_static(b"versioned-body"))
            .send()
            .await
            .unwrap();
        let public_version_id = put
            .version_id()
            .expect("expected VersionId for versioned object")
            .to_string();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(public_key)
            .version_id(&public_version_id)
            .tagging(simple_bucket_tagging("security", "public"))
            .send()
            .await
            .unwrap();
        let private_put = client
            .put_object()
            .bucket(&bucket)
            .key(private_key)
            .body(ByteStream::from_static(b"versioned-body"))
            .send()
            .await
            .unwrap();
        let private_version_id = private_put
            .version_id()
            .expect("expected VersionId for versioned object")
            .to_string();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(private_key)
            .version_id(&private_version_id)
            .tagging(simple_bucket_tagging("security", "private"))
            .send()
            .await
            .unwrap();

        let denied = alt_client
            .get_object_acl()
            .bucket(&bucket)
            .key(public_key)
            .version_id(&public_version_id)
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:GetObjectVersionAcl",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:ExistingObjectTag/security": "public"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let acl = eventually_ok_with_retry(
            "GetObjectVersionAcl with ExistingObjectTag bucket policy",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .get_object_acl()
                    .bucket(&bucket)
                    .key(public_key)
                    .version_id(&public_version_id)
                    .send()
            },
        )
        .await;
        assert!(
            acl.owner().is_some(),
            "expected owner in GetObjectVersionAcl"
        );

        let private_denied = alt_client
            .get_object_acl()
            .bucket(&bucket)
            .key(private_key)
            .version_id(&private_version_id)
            .send()
            .await;
        assert_eq!(err_status(&private_denied), 403);
        assert_s3_err_code(&private_denied, "AccessDenied");

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_bucket_policy_put_object_version_acl_existing_tag_condition() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let public_key = "publictag-version-acl-put";
        let private_key = "privatetag-version-acl-put";

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        set_object_writer_ownership(&bucket).await;
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
        let put = client
            .put_object()
            .bucket(&bucket)
            .key(public_key)
            .body(ByteStream::from_static(b"versioned-body"))
            .send()
            .await
            .unwrap();
        let public_version_id = put
            .version_id()
            .expect("expected VersionId for versioned object")
            .to_string();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(public_key)
            .version_id(&public_version_id)
            .tagging(simple_bucket_tagging("security", "public"))
            .send()
            .await
            .unwrap();
        let private_put = client
            .put_object()
            .bucket(&bucket)
            .key(private_key)
            .body(ByteStream::from_static(b"versioned-body"))
            .send()
            .await
            .unwrap();
        let private_version_id = private_put
            .version_id()
            .expect("expected VersionId for versioned object")
            .to_string();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(private_key)
            .version_id(&private_version_id)
            .tagging(simple_bucket_tagging("security", "private"))
            .send()
            .await
            .unwrap();

        let alt_id = client_canonical_id(alt_client).await;
        let grant_read_header = format!("id=\"{alt_id}\"");

        let denied = alt_client
            .put_object_acl()
            .bucket(&bucket)
            .key(public_key)
            .version_id(&public_version_id)
            .customize()
            .mutate_request({
                let grant_read_header = grant_read_header.clone();
                move |req| {
                    req.headers_mut()
                        .insert("x-amz-grant-read", grant_read_header.clone());
                }
            })
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:PutObjectVersionAcl",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:ExistingObjectTag/security": "public"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_ok_with_retry(
            "PutObjectVersionAcl with ExistingObjectTag bucket policy",
            60,
            std::time::Duration::from_millis(500),
            || {
                let grant_read_header = grant_read_header.clone();
                alt_client
                    .put_object_acl()
                    .bucket(&bucket)
                    .key(public_key)
                    .version_id(&public_version_id)
                    .customize()
                    .mutate_request(move |req| {
                        req.headers_mut()
                            .insert("x-amz-grant-read", grant_read_header.clone());
                    })
                    .send()
            },
        )
        .await;

        let owner_view = client
            .get_object_acl()
            .bucket(&bucket)
            .key(public_key)
            .version_id(&public_version_id)
            .send()
            .await
            .unwrap();
        assert!(
            has_grant(owner_view.grants(), Permission::Read, Some(&alt_id)),
            "expected READ grant for alternate account on version, got {:?}",
            owner_view.grants()
        );

        let private_denied = alt_client
            .put_object_acl()
            .bucket(&bucket)
            .key(private_key)
            .version_id(&private_version_id)
            .customize()
            .mutate_request({
                let grant_read_header = grant_read_header.clone();
                move |req| {
                    req.headers_mut()
                        .insert("x-amz-grant-read", grant_read_header.clone());
                }
            })
            .send()
            .await;
        assert_eq!(err_status(&private_denied), 403);
        assert_s3_err_code(&private_denied, "AccessDenied");

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_bucket_policy_delete_object_version_tagging_existing_tag_condition() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let public_key = "publictag-version-delete-tags";
        let private_key = "privatetag-version-delete-tags";

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
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

        let public_version_id = client
            .put_object()
            .bucket(&bucket)
            .key(public_key)
            .body(ByteStream::from_static(b"versioned-body"))
            .send()
            .await
            .unwrap()
            .version_id()
            .expect("expected VersionId for public object")
            .to_string();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(public_key)
            .version_id(&public_version_id)
            .tagging(simple_bucket_tagging("security", "public"))
            .send()
            .await
            .unwrap();

        let private_version_id = client
            .put_object()
            .bucket(&bucket)
            .key(private_key)
            .body(ByteStream::from_static(b"versioned-body"))
            .send()
            .await
            .unwrap()
            .version_id()
            .expect("expected VersionId for private object")
            .to_string();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(private_key)
            .version_id(&private_version_id)
            .tagging(simple_bucket_tagging("security", "private"))
            .send()
            .await
            .unwrap();

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:DeleteObjectVersionTagging",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:ExistingObjectTag/security": "public"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_ok_with_retry(
            "DeleteObjectVersionTagging with ExistingObjectTag bucket policy",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .delete_object_tagging()
                    .bucket(&bucket)
                    .key(public_key)
                    .version_id(&public_version_id)
                    .send()
            },
        )
        .await;

        let public_tags = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(public_key)
            .version_id(&public_version_id)
            .send()
            .await
            .unwrap();
        assert!(
            public_tags.tag_set().is_empty(),
            "expected public object version tags to be deleted, got {:?}",
            public_tags.tag_set()
        );

        eventually_access_denied(
            "DeleteObjectVersionTagging denied by nonmatching ExistingObjectTag bucket policy",
            || {
                alt_client
                    .delete_object_tagging()
                    .bucket(&bucket)
                    .key(private_key)
                    .version_id(&private_version_id)
                    .send()
            },
        )
        .await;

        let private_tags = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(private_key)
            .version_id(&private_version_id)
            .send()
            .await
            .unwrap();
        assert_eq!(
            private_tags
                .tag_set()
                .iter()
                .map(|tag| (tag.key(), tag.value()))
                .collect::<Vec<_>>(),
            vec![("security", "private")]
        );

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_bucket_policy_put_object_version_acl_null_treats_absent_acl_header_as_missing() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        set_object_writer_ownership(&bucket).await;
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
        let put = client
            .put_object()
            .bucket(&bucket)
            .key("versioned")
            .body(ByteStream::from_static(b"versioned-body"))
            .send()
            .await
            .unwrap();
        let version_id = put
            .version_id()
            .expect("expected VersionId for versioned object")
            .to_string();

        let alt_id = client_canonical_id(alt_client).await;
        let grant_read_header = format!("id=\"{alt_id}\"");
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": principal.clone(),
                    "Action": "s3:PutObjectVersionAcl",
                    "Resource": bucket_wildcard_resource(&bucket),
                },
                {
                    "Effect": "Deny",
                    "Principal": principal,
                    "Action": "s3:PutObjectVersionAcl",
                    "Resource": bucket_wildcard_resource(&bucket),
                    "Condition": {
                        "Null": {
                            "s3:x-amz-acl": "true"
                        }
                    }
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_result_matches(
            "PutObjectVersionAcl denied when x-amz-acl is absent under Null condition",
            60,
            std::time::Duration::from_millis(500),
            || {
                let grant_read_header = grant_read_header.clone();
                alt_client
                    .put_object_acl()
                    .bucket(&bucket)
                    .key("versioned")
                    .version_id(&version_id)
                    .customize()
                    .mutate_request(move |req| {
                        req.headers_mut()
                            .insert("x-amz-grant-read", grant_read_header.clone());
                    })
                    .send()
            },
            |result| {
                result.as_ref().err().is_some_and(|err| {
                    err.raw_response().map(|r| r.status().as_u16()) == Some(403)
                        && err.as_service_error().and_then(ProvideErrorMetadata::code)
                            == Some("AccessDenied")
                })
            },
        )
        .await;

        eventually_ok_with_retry(
            "PutObjectVersionAcl with explicit private ACL under Null condition",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object_acl()
                    .bucket(&bucket)
                    .key("versioned")
                    .version_id(&version_id)
                    .acl(ObjectCannedAcl::Private)
                    .send()
            },
        )
        .await;

        client
            .delete_object()
            .bucket(&bucket)
            .key("versioned")
            .version_id(&version_id)
            .send()
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_put_object_version_acl_string_not_equals_treats_absent_acl_header_as_not_equal(
) {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        set_object_writer_ownership(&bucket).await;
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
        let put = client
            .put_object()
            .bucket(&bucket)
            .key("versioned")
            .body(ByteStream::from_static(b"versioned-body"))
            .send()
            .await
            .unwrap();
        let version_id = put
            .version_id()
            .expect("expected VersionId for versioned object")
            .to_string();

        let alt_id = client_canonical_id(alt_client).await;
        let grant_read_header = format!("id=\"{alt_id}\"");
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": principal.clone(),
                    "Action": "s3:PutObjectVersionAcl",
                    "Resource": bucket_wildcard_resource(&bucket),
                },
                {
                    "Effect": "Deny",
                    "Principal": principal,
                    "Action": "s3:PutObjectVersionAcl",
                    "Resource": bucket_wildcard_resource(&bucket),
                    "Condition": {
                        "StringNotEquals": {
                            "s3:x-amz-acl": "private"
                        }
                    }
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_result_matches(
            "PutObjectVersionAcl denied when x-amz-acl is absent under StringNotEquals",
            60,
            std::time::Duration::from_millis(500),
            || {
                let grant_read_header = grant_read_header.clone();
                alt_client
                    .put_object_acl()
                    .bucket(&bucket)
                    .key("versioned")
                    .version_id(&version_id)
                    .customize()
                    .mutate_request(move |req| {
                        req.headers_mut()
                            .insert("x-amz-grant-read", grant_read_header.clone());
                    })
                    .send()
            },
            |result| {
                result.as_ref().err().is_some_and(|err| {
                    err.raw_response().map(|r| r.status().as_u16()) == Some(403)
                        && err.as_service_error().and_then(ProvideErrorMetadata::code)
                            == Some("AccessDenied")
                })
            },
        )
        .await;

        eventually_result_matches(
            "PutObjectVersionAcl denied when x-amz-acl does not equal private",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object_acl()
                    .bucket(&bucket)
                    .key("versioned")
                    .version_id(&version_id)
                    .acl(ObjectCannedAcl::BucketOwnerFullControl)
                    .send()
            },
            |result| {
                result.as_ref().err().is_some_and(|err| {
                    err.raw_response().map(|r| r.status().as_u16()) == Some(403)
                        && err.as_service_error().and_then(ProvideErrorMetadata::code)
                            == Some("AccessDenied")
                })
            },
        )
        .await;

        eventually_ok_with_retry(
            "PutObjectVersionAcl with x-amz-acl=private under StringNotEquals",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object_acl()
                    .bucket(&bucket)
                    .key("versioned")
                    .version_id(&version_id)
                    .acl(ObjectCannedAcl::Private)
                    .send()
            },
        )
        .await;

        client
            .delete_object()
            .bucket(&bucket)
            .key("versioned")
            .version_id(&version_id)
            .send()
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_put_object_version_acl_grant_read_condition_applies() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        set_object_writer_ownership(&bucket).await;
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
        let put = client
            .put_object()
            .bucket(&bucket)
            .key("versioned")
            .body(ByteStream::from_static(b"versioned-body"))
            .send()
            .await
            .unwrap();
        let version_id = put
            .version_id()
            .expect("expected VersionId for versioned object")
            .to_string();

        let alt_id = client_canonical_id(alt_client).await;
        let owner_id = client_canonical_id(client).await;
        let grant_read_header = format!("id=\"{alt_id}\"");
        let wrong_grant_read_header = format!("id=\"{owner_id}\"");
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:PutObjectVersionAcl",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:x-amz-grant-read": grant_read_header.clone()
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let denied = alt_client
            .put_object_acl()
            .bucket(&bucket)
            .key("versioned")
            .version_id(&version_id)
            .customize()
            .mutate_request({
                let wrong_grant_read_header = wrong_grant_read_header.clone();
                move |req| {
                    req.headers_mut()
                        .insert("x-amz-grant-read", wrong_grant_read_header.clone());
                }
            })
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        eventually_ok_with_retry(
            "PutObjectVersionAcl with grant-read condition",
            60,
            std::time::Duration::from_millis(500),
            || {
                let grant_read_header = grant_read_header.clone();
                alt_client
                    .put_object_acl()
                    .bucket(&bucket)
                    .key("versioned")
                    .version_id(&version_id)
                    .customize()
                    .mutate_request(move |req| {
                        req.headers_mut()
                            .insert("x-amz-grant-read", grant_read_header.clone());
                    })
                    .send()
            },
        )
        .await;

        let owner_view = client
            .get_object_acl()
            .bucket(&bucket)
            .key("versioned")
            .version_id(&version_id)
            .send()
            .await
            .unwrap();
        assert!(
            has_grant(owner_view.grants(), Permission::Read, Some(&alt_id)),
            "expected READ grant for alternate account on version, got {:?}",
            owner_view.grants()
        );

        client
            .delete_object()
            .bucket(&bucket)
            .key("versioned")
            .version_id(&version_id)
            .send()
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_put_obj_requires_sse_c_algorithm_header() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_sse_c(client).await;

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:PutObject",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "Null": {
                        "s3:x-amz-server-side-encryption-customer-algorithm": "true"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let denied = client
            .put_object()
            .bucket(&bucket)
            .key("denied")
            .body(ByteStream::from_static(b"plain"))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);
        client
            .put_object()
            .bucket(&bucket)
            .key("allowed")
            .sse_customer_algorithm("AES256")
            .sse_customer_key(key_b64.clone())
            .sse_customer_key_md5(key_md5_b64.clone())
            .body(ByteStream::from_static(b"secret"))
            .send()
            .await
            .unwrap();

        let get = client
            .get_object()
            .bucket(&bucket)
            .key("allowed")
            .sse_customer_algorithm("AES256")
            .sse_customer_key(key_b64)
            .sse_customer_key_md5(key_md5_b64)
            .send()
            .await
            .unwrap();
        assert_eq!(
            get.body.collect().await.unwrap().into_bytes().as_ref(),
            b"secret"
        );

        cleanup(&bucket, &["allowed", "denied"]).await;
    });
}

#[test]
fn test_bucket_policy_put_obj_requires_sse_s3_header() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:PutObject",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "Null": {
                        "s3:x-amz-server-side-encryption": "true"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let denied = client
            .put_object()
            .bucket(&bucket)
            .key("denied")
            .body(ByteStream::from_static(b"plain"))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        client
            .put_object()
            .bucket(&bucket)
            .key("allowed")
            .server_side_encryption(ServerSideEncryption::Aes256)
            .body(ByteStream::from_static(b"secret"))
            .send()
            .await
            .unwrap();

        let head = client
            .head_object()
            .bucket(&bucket)
            .key("allowed")
            .send()
            .await
            .unwrap();
        assert_eq!(
            head.server_side_encryption(),
            Some(&ServerSideEncryption::Aes256)
        );

        cleanup(&bucket, &["allowed", "denied"]).await;
    });
}

#[test]
fn test_bucket_policy_put_obj_sse_c_algorithm_string_not_equals() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_sse_c(client).await;

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:PutObject",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringNotEquals": {
                        "s3:x-amz-server-side-encryption-customer-algorithm": "AES256"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let denied = client
            .put_object()
            .bucket(&bucket)
            .key("denied")
            .body(ByteStream::from_static(b"plain"))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);
        client
            .put_object()
            .bucket(&bucket)
            .key("allowed")
            .sse_customer_algorithm("AES256")
            .sse_customer_key(key_b64)
            .sse_customer_key_md5(key_md5_b64)
            .body(ByteStream::from_static(b"secret"))
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &["allowed", "denied"]).await;
    });
}

#[test]
fn test_bucket_policy_put_obj_sse_s3_string_not_equals() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:PutObject",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringNotEquals": {
                        "s3:x-amz-server-side-encryption": "AES256"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let denied = client
            .put_object()
            .bucket(&bucket)
            .key("denied")
            .body(ByteStream::from_static(b"plain"))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        client
            .put_object()
            .bucket(&bucket)
            .key("allowed")
            .server_side_encryption(ServerSideEncryption::Aes256)
            .body(ByteStream::from_static(b"secret"))
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &["allowed", "denied"]).await;
    });
}

#[test]
fn test_bucket_policy_copy_object_recognizes_destination_sse_c_header() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_sse_c(client).await;

        client
            .put_object()
            .bucket(&bucket)
            .key("src")
            .body(ByteStream::from_static(b"copy-source"))
            .send()
            .await
            .unwrap();

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:PutObject",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "Null": {
                        "s3:x-amz-server-side-encryption-customer-algorithm": "true"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);
        with_sse_c_headers!(
            client
                .copy_object()
                .bucket(&bucket)
                .key("dst")
                .copy_source(format!("{bucket}/src")),
            "AES256",
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();

        let get = with_sse_c_headers!(
            client.get_object().bucket(&bucket).key("dst"),
            "AES256",
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        assert_eq!(
            get.body.collect().await.unwrap().into_bytes().as_ref(),
            b"copy-source"
        );

        cleanup(&bucket, &["src", "dst"]).await;
    });
}

#[test]
fn test_bucket_policy_copy_object_recognizes_destination_sse_s3_header() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        client
            .put_object()
            .bucket(&bucket)
            .key("src")
            .body(ByteStream::from_static(b"copy-source"))
            .send()
            .await
            .unwrap();

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:PutObject",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "Null": {
                        "s3:x-amz-server-side-encryption": "true"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        with_sse_s3_header!(client
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{bucket}/src")))
        .send()
        .await
        .unwrap();

        let get = client
            .get_object()
            .bucket(&bucket)
            .key("dst")
            .send()
            .await
            .unwrap();
        assert_eq!(
            get.server_side_encryption(),
            Some(&ServerSideEncryption::Aes256)
        );
        assert_eq!(
            get.body.collect().await.unwrap().into_bytes().as_ref(),
            b"copy-source"
        );

        cleanup(&bucket, &["src", "dst"]).await;
    });
}

#[test]
fn test_bucket_policy_complete_multipart_does_not_reuse_destination_sse_c_header() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_sse_c(client).await;

        client
            .put_object()
            .bucket(&bucket)
            .key("src")
            .body(ByteStream::from_static(b"copy-source"))
            .send()
            .await
            .unwrap();

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:PutObject",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "Null": {
                        "s3:x-amz-server-side-encryption-customer-algorithm": "true"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);
        let create = with_sse_c_headers!(
            client.create_multipart_upload().bucket(&bucket).key("dst"),
            "AES256",
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let copied_part = with_sse_c_headers!(
            client
                .upload_part_copy()
                .bucket(&bucket)
                .key("dst")
                .upload_id(&upload_id)
                .part_number(1)
                .copy_source(format!("{bucket}/src")),
            "AES256",
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        let complete = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key("dst")
            .upload_id(&upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(copied_part.copy_part_result().unwrap().e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await;
        assert_eq!(err_status(&complete), 403);
        assert_s3_err_code(&complete, "AccessDenied");
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("dst")
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();

        let get = with_sse_c_headers!(
            client.get_object().bucket(&bucket).key("dst"),
            "AES256",
            key_b64,
            key_md5_b64
        )
        .send()
        .await;
        assert_eq!(err_status(&get), 404);
        assert_s3_err_code(&get, "NoSuchKey");

        cleanup(&bucket, &["src", "dst"]).await;
    });
}

#[test]
fn test_bucket_policy_upload_part_copy_does_not_reuse_destination_sse_c_header() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let second_client = CTX.require_second_client();
        let principal = same_account_exact_principal().await;
        let bucket = create_bucket_allowing_sse_c(client).await;

        client
            .put_object()
            .bucket(&bucket)
            .key("src")
            .body(ByteStream::from_static(b"copy-source"))
            .send()
            .await
            .unwrap();

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": { "AWS": principal.clone() },
                    "Action": "s3:GetObject",
                    "Resource": bucket_wildcard_resource(&bucket),
                },
                {
                    "Effect": "Allow",
                    "Principal": { "AWS": principal.clone() },
                    "Action": "s3:PutObject",
                    "Resource": bucket_wildcard_resource(&bucket),
                },
                {
                    "Effect": "Deny",
                    "Principal": { "AWS": principal },
                    "Action": "s3:PutObject",
                    "Resource": bucket_wildcard_resource(&bucket),
                    "Condition": {
                        "Null": {
                            "s3:x-amz-server-side-encryption-customer-algorithm": "true"
                        }
                    }
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);
        let create = with_sse_c_headers!(
            second_client
                .create_multipart_upload()
                .bucket(&bucket)
                .key("dst"),
            "AES256",
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let copied_part = second_client
            .upload_part_copy()
            .bucket(&bucket)
            .key("dst")
            .upload_id(&upload_id)
            .part_number(1)
            .copy_source(format!("{bucket}/src"))
            .send()
            .await;
        assert_eq!(err_status(&copied_part), 403);
        assert_s3_err_code(&copied_part, "AccessDenied");

        second_client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("dst")
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &["src", "dst"]).await;
    });
}

#[test]
fn test_bucket_policy_multipart_copy_inherits_destination_sse_s3() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        client
            .put_object()
            .bucket(&bucket)
            .key("src")
            .body(ByteStream::from_static(b"copy-source"))
            .send()
            .await
            .unwrap();

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:PutObject",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "Null": {
                        "s3:x-amz-server-side-encryption": "true"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let create =
            with_sse_s3_header!(client.create_multipart_upload().bucket(&bucket).key("dst"))
                .send()
                .await
                .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let copied_part = client
            .upload_part_copy()
            .bucket(&bucket)
            .key("dst")
            .upload_id(&upload_id)
            .part_number(1)
            .copy_source(format!("{bucket}/src"))
            .send()
            .await
            .unwrap();
        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key("dst")
            .upload_id(&upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(copied_part.copy_part_result().unwrap().e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .unwrap();

        let get = client
            .get_object()
            .bucket(&bucket)
            .key("dst")
            .send()
            .await
            .unwrap();
        assert_eq!(
            get.server_side_encryption(),
            Some(&ServerSideEncryption::Aes256)
        );
        assert_eq!(
            get.body.collect().await.unwrap().into_bytes().as_ref(),
            b"copy-source"
        );

        cleanup(&bucket, &["src", "dst"]).await;
    });
}

#[test]
fn test_bucket_policy_upload_part_copy_reuses_destination_sse_s3_from_multipart_context() {
    s3_tests::run(async {
        let client = CTX.client();
        let second_client = CTX.require_second_client();
        let principal = same_account_exact_principal().await;
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        client
            .put_object()
            .bucket(&bucket)
            .key("src")
            .body(ByteStream::from_static(b"copy-source"))
            .send()
            .await
            .unwrap();

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": { "AWS": principal.clone() },
                    "Action": "s3:GetObject",
                    "Resource": bucket_wildcard_resource(&bucket),
                },
                {
                    "Effect": "Allow",
                    "Principal": { "AWS": principal.clone() },
                    "Action": "s3:PutObject",
                    "Resource": bucket_wildcard_resource(&bucket),
                },
                {
                    "Effect": "Deny",
                    "Principal": { "AWS": principal },
                    "Action": "s3:PutObject",
                    "Resource": bucket_wildcard_resource(&bucket),
                    "Condition": {
                        "Null": {
                            "s3:x-amz-server-side-encryption": "true"
                        }
                    }
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let create =
            with_sse_s3_header!(client.create_multipart_upload().bucket(&bucket).key("dst"))
                .send()
                .await
                .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let copied_part = second_client
            .upload_part_copy()
            .bucket(&bucket)
            .key("dst")
            .upload_id(&upload_id)
            .part_number(1)
            .copy_source(format!("{bucket}/src"))
            .send()
            .await
            .unwrap();
        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key("dst")
            .upload_id(&upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(copied_part.copy_part_result().unwrap().e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .unwrap();

        let get = client
            .get_object()
            .bucket(&bucket)
            .key("dst")
            .send()
            .await
            .unwrap();
        assert_eq!(
            get.server_side_encryption(),
            Some(&ServerSideEncryption::Aes256)
        );
        assert_eq!(
            get.body.collect().await.unwrap().into_bytes().as_ref(),
            b"copy-source"
        );

        cleanup(&bucket, &["src", "dst"]).await;
    });
}

#[test]
fn test_bucket_policy_complete_multipart_reuses_destination_sse_s3_from_multipart_context() {
    s3_tests::run(async {
        let client = CTX.client();
        let second_client = CTX.require_second_client();
        let principal = same_account_exact_principal().await;
        let bucket = unique_bucket();
        let key = "dst";
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": { "AWS": principal.clone() },
                    "Action": "s3:PutObject",
                    "Resource": bucket_wildcard_resource(&bucket),
                },
                {
                    "Effect": "Deny",
                    "Principal": { "AWS": principal },
                    "Action": "s3:PutObject",
                    "Resource": bucket_wildcard_resource(&bucket),
                    "Condition": {
                        "Null": {
                            "s3:x-amz-server-side-encryption": "true"
                        }
                    }
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let create = with_sse_s3_header!(client.create_multipart_upload().bucket(&bucket).key(key))
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let uploaded = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from_static(b"body"))
            .send()
            .await
            .unwrap();
        let etag = uploaded.e_tag().expect("expected upload part etag");

        second_client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(CompletedPart::builder().part_number(1).e_tag(etag).build())
                    .build(),
            )
            .send()
            .await
            .unwrap();

        let get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(
            get.server_side_encryption(),
            Some(&ServerSideEncryption::Aes256)
        );
        assert_eq!(
            get.body.collect().await.unwrap().into_bytes().as_ref(),
            b"body"
        );

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_streaming_put_rejects_lowercase_sse_c_algorithm() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_sse_c(client).await;

        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);
        let put = with_sse_c_headers!(
            client
                .put_object()
                .bucket(&bucket)
                .key("obj")
                .body(ByteStream::from_static(b"body")),
            "aes256",
            key_b64,
            key_md5_b64
        )
        .send()
        .await;
        assert_eq!(err_status(&put), 400);
        assert_s3_err_code(&put, "InvalidEncryptionAlgorithmError");

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_bucket_policy_put_obj_request_object_tag() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": ["s3:PutObject", "s3:PutObjectTagging"],
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:RequestObjectTag/security": "public"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();
        client
            .get_bucket_policy()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        let denied = alt_client
            .put_object()
            .bucket(&bucket)
            .key("denied")
            .body(ByteStream::from_static(b"denied"))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        eventually_ok("PutObject with request-object-tag condition", || {
            alt_client
                .put_object()
                .bucket(&bucket)
                .key("allowed")
                .tagging("security=public")
                .body(ByteStream::from_static(b"allowed"))
                .send()
        })
        .await;

        cleanup(&bucket, &["allowed", "denied"]).await;
    });
}

#[test]
fn test_bucket_policy_request_object_tag_condition_uses_decoded_tag_value() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Deny",
                        "Principal": "*",
                        "Action": "s3:PutObject",
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "StringNotEquals": {
                                "s3:RequestObjectTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let denied_url = object_url(CTX.endpoint(), &bucket, "denied-tag", None);
        eventually_result_matches(
            "PutObject denied with nonmatching request-object-tag condition",
            20,
            std::time::Duration::from_millis(200),
            || {
                let denied_url = denied_url.clone();
                async move {
                    Ok::<_, std::convert::Infallible>(send_signed_request(
                        "PUT",
                        &denied_url,
                        b"denied-tag",
                        [("x-amz-tagging", "security=private")],
                    ))
                }
            },
            |result| result.as_ref().is_ok_and(|response| response.status == 403),
        )
        .await;

        let url = object_url(CTX.endpoint(), &bucket, "encoded-tag", None);
        let response = send_signed_request(
            "PUT",
            &url,
            b"encoded-tag",
            [("x-amz-tagging", "security=pub%6Cic")],
        );
        let status = response.status;
        assert_eq!(status, 200, "expected 200, got {status}");

        let tags = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("encoded-tag")
            .send()
            .await
            .unwrap();
        assert_eq!(
            tags.tag_set()
                .iter()
                .map(|tag| (tag.key(), tag.value()))
                .collect::<Vec<_>>(),
            vec![("security", "public")]
        );

        cleanup(&bucket, &["encoded-tag", "denied-tag"]).await;
    });
}

#[test]
fn test_bucket_policy_request_object_tag_binary_equals_matches_base64_value() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": ["s3:PutObject", "s3:PutObjectTagging"],
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "BinaryEquals": {
                        "s3:RequestObjectTag/security": "cHVibGlj"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_ok(
            "PutObject with BinaryEquals request-object-tag condition",
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("binary-allowed")
                    .tagging("security=cHVibGlj")
                    .body(ByteStream::from_static(b"binary-allowed"))
                    .send()
            },
        )
        .await;

        let denied = alt_client
            .put_object()
            .bucket(&bucket)
            .key("binary-denied")
            .tagging("security=public")
            .body(ByteStream::from_static(b"binary-denied"))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        let denied = alt_client
            .put_object()
            .bucket(&bucket)
            .key("binary-private-denied")
            .tagging("security=cHJpdmF0ZQ%3D%3D")
            .body(ByteStream::from_static(b"binary-private-denied"))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        cleanup(
            &bucket,
            &["binary-allowed", "binary-denied", "binary-private-denied"],
        )
        .await;
    });
}

#[test]
fn test_bucket_policy_request_object_tag_binary_equals_deny_uses_base64_request_value() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Deny",
                    "Principal": principal,
                    "Action": ["s3:PutObject", "s3:PutObjectTagging"],
                    "Resource": bucket_wildcard_resource(&bucket),
                    "Condition": {
                        "BinaryEquals": {
                            "s3:RequestObjectTag/security": "cHVibGlj"
                        }
                    }
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": ["s3:PutObject", "s3:PutObjectTagging"],
                    "Resource": bucket_wildcard_resource(&bucket)
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_ok(
            "PutObject with raw non-base64 tag bypasses BinaryEquals Deny",
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("binary-deny-raw")
                    .tagging("security=public")
                    .body(ByteStream::from_static(b"binary-deny-raw"))
                    .send()
            },
        )
        .await;

        let encoded = alt_client
            .put_object()
            .bucket(&bucket)
            .key("binary-deny-encoded")
            .tagging("security=cHVibGlj")
            .body(ByteStream::from_static(b"binary-deny-encoded"))
            .send()
            .await;
        assert_eq!(err_status(&encoded), 403);
        assert_s3_err_code(&encoded, "AccessDenied");

        cleanup(&bucket, &["binary-deny-encoded", "binary-deny-raw"]).await;
    });
}

#[test]
fn test_bucket_policy_request_object_tag_binary_equals_does_not_wildcard_match() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": ["s3:PutObject", "s3:PutObjectTagging"],
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "BinaryEquals": {
                        "s3:RequestObjectTag/security": "Kg=="
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_ok(
            "PutObject with literal BinaryEquals asterisk tag value",
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("binary-wildcard-literal-allowed")
                    .tagging("security=Kg%3D%3D")
                    .body(ByteStream::from_static(b"binary-wildcard-literal-allowed"))
                    .send()
            },
        )
        .await;

        let denied = alt_client
            .put_object()
            .bucket(&bucket)
            .key("binary-wildcard-denied")
            .tagging("security=cHVibGlj")
            .body(ByteStream::from_static(b"binary-wildcard-denied"))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        cleanup(
            &bucket,
            &["binary-wildcard-literal-allowed", "binary-wildcard-denied"],
        )
        .await;
    });
}

#[test]
fn test_bucket_policy_list_bucket_prefix_string_equals_does_not_wildcard_match() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        for key in ["pub*/one", "public/one"] {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(b"body"))
                .send()
                .await
                .unwrap();
        }

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:ListBucket",
                "Resource": bucket_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:prefix": "pub*"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let listed = eventually_ok(
            "ListBucket with literal StringEquals asterisk prefix",
            || {
                alt_client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .prefix("pub*")
                    .send()
            },
        )
        .await;
        assert_eq!(listed.contents().len(), 1);

        let denied = alt_client
            .list_objects_v2()
            .bucket(&bucket)
            .prefix("public")
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        cleanup(&bucket, &["pub*/one", "public/one"]).await;
    });
}

#[test]
fn test_bucket_policy_list_bucket_prefix_string_not_equals_does_not_wildcard_match() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        for key in ["pub*/one", "public/one"] {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(b"body"))
                .send()
                .await
                .unwrap();
        }

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:ListBucket",
                "Resource": bucket_resource(&bucket),
                "Condition": {
                    "StringNotEquals": {
                        "s3:prefix": "pub*"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_ok(
            "ListBucket with StringNotEquals wildcard-looking operand and different prefix",
            || {
                alt_client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .prefix("public")
                    .send()
            },
        )
        .await;

        let denied = alt_client
            .list_objects_v2()
            .bucket(&bucket)
            .prefix("pub*")
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        cleanup(&bucket, &["pub*/one", "public/one"]).await;
    });
}

#[test]
fn test_bucket_policy_list_bucket_prefix_string_like_question_mark_matches_one_character() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        for key in ["question-a/one", "question-ab/one"] {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(b"body"))
                .send()
                .await
                .unwrap();
        }

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:ListBucket",
                "Resource": bucket_resource(&bucket),
                "Condition": {
                    "StringLike": {
                        "s3:prefix": "question-?/"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let listed = eventually_ok(
            "ListBucket with StringLike question-mark wildcard prefix",
            || {
                alt_client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .prefix("question-a/")
                    .send()
            },
        )
        .await;
        assert_eq!(listed.contents().len(), 1);

        eventually_access_denied(
            "ListBucket denied when StringLike question mark would need two characters",
            || {
                alt_client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .prefix("question-ab/")
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &["question-a/one", "question-ab/one"]).await;
    });
}

fn assert_anonymous_list_status(
    bucket: &str,
    encoded_prefix: &str,
    expected_status: u16,
) -> String {
    let response = raw_anonymous(
        "GET",
        bucket,
        "",
        Some(&format!("list-type=2&prefix={encoded_prefix}")),
    );
    assert_eq!(
        response.status, expected_status,
        "unexpected anonymous ListBucket status for prefix {encoded_prefix}: body={}",
        response.body
    );
    response.body
}

fn assert_anonymous_list_ok_contains(bucket: &str, encoded_prefix: &str, expected_key: &str) {
    let body = assert_anonymous_list_status(bucket, encoded_prefix, 200);
    assert!(
        body.contains(&format!("<Key>{expected_key}</Key>")),
        "expected ListBucket body to include {expected_key}: {body}"
    );
}

fn assert_anonymous_list_access_denied(bucket: &str, encoded_prefix: &str) {
    let body = assert_anonymous_list_status(bucket, encoded_prefix, 403);
    assert!(
        body.contains("<Code>AccessDenied</Code>"),
        "expected AccessDenied body: {body}"
    );
}

#[test]
fn test_bucket_policy_variables_anonymous_principal_values_defaults_and_special_literals() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let keys = [
            "anonymous/one",
            "anonymous/resource",
            "fallback/one",
            "star-*/one",
            "question-?/one",
            "dollar-$/one",
            "private/resource",
            "star-public/one",
            "question-a/one",
        ];
        for key in keys {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(b"body"))
                .send()
                .await
                .unwrap();
        }

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:ListBucket",
                    "Resource": bucket_resource(&bucket),
                    "Condition": {
                        "StringEquals": {
                            "aws:PrincipalType": "Anonymous",
                            "s3:prefix": "${AWS:UserId}"
                        }
                    }
                },
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:ListBucket",
                    "Resource": bucket_resource(&bucket),
                    "Condition": {
                        "StringEquals": {
                            "s3:prefix": "${aws:username, 'fallback'}"
                        }
                    }
                },
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:ListBucket",
                    "Resource": bucket_resource(&bucket),
                    "Condition": {
                        "StringLike": {
                            "s3:prefix": "star-${*}"
                        }
                    }
                },
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:ListBucket",
                    "Resource": bucket_resource(&bucket),
                    "Condition": {
                        "StringLike": {
                            "s3:prefix": "question-${?}"
                        }
                    }
                },
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:ListBucket",
                    "Resource": bucket_resource(&bucket),
                    "Condition": {
                        "StringEquals": {
                            "s3:prefix": "dollar-${$}"
                        }
                    }
                },
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": format!("arn:aws:s3:::{bucket}/${{aws:userid}}/*"),
                    "Condition": {
                        "StringEquals": {
                            "aws:PrincipalType": "Anonymous"
                        }
                    }
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        assert_anonymous_list_ok_contains(&bucket, "anonymous", "anonymous/one");
        assert_anonymous_list_ok_contains(&bucket, "fallback", "fallback/one");
        assert_anonymous_list_ok_contains(&bucket, "star-%2A", "star-*/one");
        assert_anonymous_list_ok_contains(&bucket, "question-%3F", "question-?/one");
        assert_anonymous_list_ok_contains(&bucket, "dollar-%24", "dollar-$/one");
        assert_anonymous_list_access_denied(&bucket, "star-public");
        assert_anonymous_list_access_denied(&bucket, "question-a");
        assert_eq!(
            raw_anonymous("GET", &bucket, "anonymous/resource", None).status,
            200
        );
        let private_resource = raw_anonymous("GET", &bucket, "private/resource", None);
        assert_eq!(
            private_resource.status, 403,
            "expected Resource variable to deny private object: {}",
            private_resource.body
        );
        assert!(
            private_resource.body.contains("<Code>AccessDenied</Code>"),
            "expected AccessDenied body: {}",
            private_resource.body
        );

        cleanup(&bucket, &keys).await;
    });
}

#[test]
fn test_bucket_policy_variables_authenticated_identity_defaults_do_not_apply() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let keys = ["control/one", "fallback/one"];
        for key in keys {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(b"body"))
                .send()
                .await
                .unwrap();
        }

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
                            "Action": "s3:ListBucket",
                            "Resource": bucket_resource(&bucket),
                            "Condition": {
                                "StringEquals": {
                                    "s3:prefix": "control"
                                }
                            }
                        },
                        {
                            "Effect": "Allow",
                            "Principal": alt_policy_principal(),
                            "Action": "s3:ListBucket",
                            "Resource": bucket_resource(&bucket),
                            "Condition": {
                                "StringEquals": {
                                    "s3:prefix": "${aws:userid, 'fallback'}"
                                }
                            }
                        }
                    ],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let listed = eventually_ok(
            "ListObjectsV2 authenticated variable control prefix",
            || {
                alt_client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .prefix("control")
                    .send()
            },
        )
        .await;
        assert_eq!(
            listed
                .contents()
                .first()
                .and_then(|object| object.key())
                .unwrap_or_default(),
            "control/one"
        );

        eventually_access_denied(
            "ListObjectsV2 authenticated variable default fallback prefix",
            || {
                alt_client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .prefix("fallback")
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &keys).await;
    });
}

#[test]
fn test_bucket_policy_variables_multivalued_context_defaults_do_not_apply() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let keys = ["control", "fallback"];

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
                            "Action": "s3:PutObject",
                            "Resource": format!("arn:aws:s3:::{bucket}/control")
                        },
                        {
                            "Effect": "Allow",
                            "Principal": alt_policy_principal(),
                            "Action": "s3:PutObject",
                            "Resource": format!("arn:aws:s3:::{bucket}/${{s3:RequestObjectTagKeys, 'fallback'}}")
                        }
                    ],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_ok("PutObject policy variable multivalue control", || {
            alt_client
                .put_object()
                .bucket(&bucket)
                .key("control")
                .body(ByteStream::from_static(b"control"))
                .send()
        })
        .await;

        eventually_access_denied("PutObject denied for multivalue variable default", || {
            alt_client
                .put_object()
                .bucket(&bucket)
                .key("fallback")
                .tagging("public=1&shared=2")
                .body(ByteStream::from_static(b"fallback"))
                .send()
        })
        .await;

        cleanup(&bucket, &keys).await;
    });
}

#[test]
fn test_bucket_policy_variables_require_2012_policy_version() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let keys = ["${*}/one", "*/one"];
        for key in keys {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(b"body"))
                .send()
                .await
                .unwrap();
        }

        let policy = json!({
            "Version": "2008-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:ListBucket",
                "Resource": bucket_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:prefix": "${*}"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        assert_anonymous_list_ok_contains(&bucket, "%24%7B%2A%7D", "${*}/one");
        assert_anonymous_list_access_denied(&bucket, "%2A");

        let policy = json!({
            "Statement": [{
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:ListBucket",
                "Resource": bucket_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:prefix": "${*}"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        assert_anonymous_list_ok_contains(&bucket, "%24%7B%2A%7D", "${*}/one");
        assert_anonymous_list_access_denied(&bucket, "%2A");

        cleanup(&bucket, &keys).await;
    });
}

#[test]
fn test_bucket_policy_variables_do_not_expand_for_numeric_operators() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let keys = ["allowed/one", "denied/one"];
        for key in keys {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(b"body"))
                .send()
                .await
                .unwrap();
        }

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:ListBucket",
                    "Resource": bucket_resource(&bucket),
                    "Condition": {
                        "StringEquals": {
                            "s3:prefix": "allowed"
                        }
                    }
                },
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:ListBucket",
                    "Resource": bucket_resource(&bucket),
                    "Condition": {
                        "StringEquals": {
                            "s3:prefix": "denied"
                        },
                        "NumericEquals": {
                            "s3:max-keys": "${aws:username, '2'}"
                        }
                    }
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        assert_anonymous_list_ok_contains(&bucket, "allowed", "allowed/one");
        let response = raw_anonymous(
            "GET",
            &bucket,
            "",
            Some("list-type=2&prefix=denied&max-keys=2"),
        );
        assert_eq!(
            response.status, 403,
            "expected NumericEquals variable-looking operand not to expand: {}",
            response.body
        );
        assert!(
            response.body.contains("<Code>AccessDenied</Code>"),
            "expected AccessDenied body: {}",
            response.body
        );

        cleanup(&bucket, &keys).await;
    });
}

#[test]
fn test_bucket_policy_current_time_date_condition_operators() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let allowed_keys = [
            "date-less-than-allowed",
            "date-less-than-fractional-allowed",
            "date-less-than-nanos-allowed",
            "date-less-than-equals-allowed",
            "date-greater-than-allowed",
            "date-greater-than-fractional-allowed",
            "date-greater-than-equals-allowed",
            "date-not-equals-allowed",
            "date-not-equals-invalid-allowed",
            "date-less-than-if-exists-allowed",
            "date-wildcard-fallback-allowed",
            "date-equals-false-fallback-allowed",
        ];
        let denied_keys = [
            "date-less-than-denied",
            "date-greater-than-denied",
            "date-equals-denied",
            "date-less-than-invalid-denied",
            "date-wildcard-denied",
        ];
        for key in allowed_keys.iter().chain(denied_keys.iter()) {
            client
                .put_object()
                .bucket(&bucket)
                .key(*key)
                .body(ByteStream::from((*key).as_bytes().to_vec()))
                .send()
                .await
                .unwrap();
        }

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, "date-less-than-allowed"),
                    "Condition": {"DateLessThan": {"aws:CurrentTime": "2999-01-01T00:00:00Z"}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, "date-less-than-fractional-allowed"),
                    "Condition": {"DateLessThan": {"aws:CurrentTime": "2999-01-01T00:00:00.0001Z"}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, "date-less-than-nanos-allowed"),
                    "Condition": {"DateLessThan": {"aws:CurrentTime": "2999-01-01T00:00:00.123456789Z"}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, "date-less-than-equals-allowed"),
                    "Condition": {"DateLessThanEquals": {"aws:CurrentTime": "2999-01-01T00:00:00Z"}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, "date-greater-than-allowed"),
                    "Condition": {"DateGreaterThan": {"aws:CurrentTime": "2000-01-01T00:00:00Z"}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, "date-greater-than-fractional-allowed"),
                    "Condition": {"DateGreaterThan": {"aws:CurrentTime": "2000-01-01T00:00:00.0001Z"}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, "date-greater-than-equals-allowed"),
                    "Condition": {"DateGreaterThanEquals": {"aws:CurrentTime": "2000-01-01T00:00:00Z"}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, "date-not-equals-allowed"),
                    "Condition": {"DateNotEquals": {"aws:CurrentTime": "2000-01-01T00:00:00Z"}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, "date-not-equals-invalid-allowed"),
                    "Condition": {"DateNotEquals": {"aws:CurrentTime": "not-a-date"}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, "date-less-than-if-exists-allowed"),
                    "Condition": {"DateLessThanIfExists": {"aws:CurrentTime": "2999-01-01T00:00:00Z"}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, "date-wildcard-fallback-allowed"),
                    "Condition": {"DateLessThan": {"aws:CurrentTime": "2999-01-01T00:00:00Z"}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, "date-equals-false-fallback-allowed")
                },
                {
                    "Effect": "Deny",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, "date-equals-false-fallback-allowed"),
                    "Condition": {"DateEquals": {"aws:CurrentTime": "2000-01-01T00:00:00Z"}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, "date-less-than-denied"),
                    "Condition": {"DateLessThan": {"aws:CurrentTime": "2000-01-01T00:00:00Z"}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, "date-greater-than-denied"),
                    "Condition": {"DateGreaterThan": {"aws:CurrentTime": "2999-01-01T00:00:00Z"}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, "date-equals-denied"),
                    "Condition": {"DateEquals": {"aws:CurrentTime": "2000-01-01T00:00:00Z"}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, "date-less-than-invalid-denied"),
                    "Condition": {"DateLessThan": {"aws:CurrentTime": "not-a-date"}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, "date-wildcard-denied"),
                    "Condition": {"DateEquals": {"aws:CurrentTime": "*"}}
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_ok(
            "GetObject with DateLessThan aws:CurrentTime condition",
            || {
                alt_client
                    .get_object()
                    .bucket(&bucket)
                    .key("date-less-than-allowed")
                    .send()
            },
        )
        .await;

        for key in allowed_keys {
            let response = alt_client
                .get_object()
                .bucket(&bucket)
                .key(key)
                .send()
                .await
                .unwrap();
            assert_eq!(
                response.body.collect().await.unwrap().into_bytes().as_ref(),
                key.as_bytes()
            );
        }

        for key in denied_keys {
            let denied = alt_client
                .get_object()
                .bucket(&bucket)
                .key(key)
                .send()
                .await;
            assert_eq!(err_status(&denied), 403, "{key}");
            assert_s3_err_code(&denied, "AccessDenied");
        }

        let cleanup_keys = [&allowed_keys[..], &denied_keys[..]].concat();
        cleanup(&bucket, &cleanup_keys).await;
    });
}

#[test]
fn test_bucket_policy_epoch_time_numeric_conditions() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let allowed_keys = ["epoch-less-than-allowed", "epoch-greater-than-allowed"];
        let denied_keys = ["epoch-less-than-denied", "epoch-greater-than-denied"];
        for key in allowed_keys.iter().chain(denied_keys.iter()) {
            client
                .put_object()
                .bucket(&bucket)
                .key(*key)
                .body(ByteStream::from((*key).as_bytes().to_vec()))
                .send()
                .await
                .unwrap();
        }

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, "epoch-less-than-allowed"),
                    "Condition": {"NumericLessThan": {"aws:EpochTime": 32503680000u64}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, "epoch-greater-than-allowed"),
                    "Condition": {"NumericGreaterThan": {"aws:EpochTime": 946684800}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, "epoch-less-than-denied"),
                    "Condition": {"NumericLessThan": {"aws:EpochTime": 946684800}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, "epoch-greater-than-denied"),
                    "Condition": {"NumericGreaterThan": {"aws:EpochTime": 32503680000u64}}
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_ok(
            "GetObject with NumericLessThan aws:EpochTime condition",
            || {
                alt_client
                    .get_object()
                    .bucket(&bucket)
                    .key("epoch-less-than-allowed")
                    .send()
            },
        )
        .await;

        for key in allowed_keys {
            let response = alt_client
                .get_object()
                .bucket(&bucket)
                .key(key)
                .send()
                .await
                .unwrap();
            assert_eq!(
                response.body.collect().await.unwrap().into_bytes().as_ref(),
                key.as_bytes()
            );
        }

        for key in denied_keys {
            let denied = alt_client
                .get_object()
                .bucket(&bucket)
                .key(key)
                .send()
                .await;
            assert_eq!(err_status(&denied), 403, "{key}");
            assert_s3_err_code(&denied, "AccessDenied");
        }

        let cleanup_keys = [&allowed_keys[..], &denied_keys[..]].concat();
        cleanup(&bucket, &cleanup_keys).await;
    });
}

#[test]
fn test_bucket_policy_secure_transport_bool_condition() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let allowed_key = "secure-transport-matches";
        let denied_key = "secure-transport-does-not-match";
        let secure_value = endpoint_is_https().to_string();
        let opposite_value = (!endpoint_is_https()).to_string();

        for key in [allowed_key, denied_key] {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(key.as_bytes()))
                .send()
                .await
                .unwrap();
        }

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, allowed_key),
                    "Condition": {"Bool": {"aws:SecureTransport": secure_value}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, denied_key),
                    "Condition": {"Bool": {"aws:SecureTransport": opposite_value}}
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let allowed = raw_alt_object_status_eventually(
            "GetObject allowed by aws:SecureTransport",
            RawAltObjectRequest::new("GET", &bucket, allowed_key),
            200,
        )
        .await;
        assert_eq!(allowed.body, allowed_key);

        let denied = raw_alt_object_status_eventually(
            "GetObject denied by aws:SecureTransport mismatch",
            RawAltObjectRequest::new("GET", &bucket, denied_key),
            403,
        )
        .await;
        assert_raw_access_denied("GetObject denied by aws:SecureTransport", &denied);

        cleanup(&bucket, &[allowed_key, denied_key]).await;
    });
}

#[test]
fn test_bucket_policy_requested_region_string_condition() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let allowed_key = "requested-region-matches";
        let denied_key = "requested-region-does-not-match";
        let expected_region = CTX.region();
        let wrong_region = if expected_region == "us-east-1" {
            "us-west-2"
        } else {
            "us-east-1"
        };

        for key in [allowed_key, denied_key] {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(key.as_bytes()))
                .send()
                .await
                .unwrap();
        }

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, allowed_key),
                    "Condition": {"StringEquals": {"aws:RequestedRegion": expected_region}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, denied_key),
                    "Condition": {"StringEquals": {"aws:RequestedRegion": wrong_region}}
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let allowed = raw_alt_object_status_eventually(
            "GetObject allowed by aws:RequestedRegion",
            RawAltObjectRequest::new("GET", &bucket, allowed_key),
            200,
        )
        .await;
        assert_eq!(allowed.body, allowed_key);

        let denied = raw_alt_object_status_eventually(
            "GetObject denied by aws:RequestedRegion mismatch",
            RawAltObjectRequest::new("GET", &bucket, denied_key),
            403,
        )
        .await;
        assert_raw_access_denied("GetObject denied by aws:RequestedRegion", &denied);

        cleanup(&bucket, &[allowed_key, denied_key]).await;
    });
}

#[test]
fn test_bucket_policy_referer_string_condition() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let allowed_key = "referer-matches";
        let missing_key = "referer-missing";
        let allowed_referer = "https://example.com/allowed";
        let wrong_referer = "https://example.com/denied";

        for key in [allowed_key, missing_key] {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(key.as_bytes()))
                .send()
                .await
                .unwrap();
        }

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:GetObject",
                "Resource": [
                    object_resource(&bucket, allowed_key),
                    object_resource(&bucket, missing_key)
                ],
                "Condition": {"StringEquals": {"aws:referer": allowed_referer}}
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let allowed_headers = [("referer", allowed_referer)];
        let allowed = raw_alt_object_status_eventually(
            "GetObject allowed by aws:referer",
            RawAltObjectRequest::new("GET", &bucket, allowed_key).extra_headers(&allowed_headers),
            200,
        )
        .await;
        assert_eq!(allowed.body, allowed_key);

        let wrong_headers = [("referer", wrong_referer)];
        let wrong = raw_alt_object_status_eventually(
            "GetObject denied by aws:referer mismatch",
            RawAltObjectRequest::new("GET", &bucket, allowed_key).extra_headers(&wrong_headers),
            403,
        )
        .await;
        assert_raw_access_denied("GetObject denied by aws:referer mismatch", &wrong);

        let missing = raw_alt_object_status_eventually(
            "GetObject denied by absent aws:referer",
            RawAltObjectRequest::new("GET", &bucket, missing_key),
            403,
        )
        .await;
        assert_raw_access_denied("GetObject denied by absent aws:referer", &missing);

        cleanup(&bucket, &[allowed_key, missing_key]).await;
    });
}

#[test]
fn test_bucket_policy_auth_request_context_condition_keys() {
    s3_tests::run(async {
        require_https_endpoint();

        let principal = alt_policy_principal();
        let client = CTX.client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let keys = [
            "auth-type-header",
            "auth-type-presigned",
            "auth-type-post",
            "auth-type-post-denied",
            "auth-type-denied",
            "signature-version",
            "signature-age-presigned-allowed",
            "signature-age-presigned-denied",
            "signature-age-post-fresh-allowed",
            "signature-age-post-old-denied",
            "tls-version-allowed",
            "tls-version-denied",
            "content-sha256-allowed",
            "content-sha256-denied",
            "website-redirect-allowed",
            "website-redirect-denied",
        ];

        for key in keys {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(key.as_bytes()))
                .send()
                .await
                .unwrap();
        }

        let empty_payload_sha256 =
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, "auth-type-header"),
                    "Condition": {"StringEquals": {"s3:authType": "REST-HEADER"}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, "auth-type-presigned"),
                    "Condition": {"StringEquals": {"s3:authType": "REST-QUERY-STRING"}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:PutObject",
                    "Resource": object_resource(&bucket, "auth-type-post"),
                    "Condition": {"StringEquals": {"s3:authType": "POST"}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:PutObject",
                    "Resource": object_resource(&bucket, "auth-type-post-denied"),
                    "Condition": {"StringEquals": {"s3:authType": "REST-POST"}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, "auth-type-denied"),
                    "Condition": {"StringEquals": {"s3:authType": "REST-QUERY-STRING"}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, "signature-version"),
                    "Condition": {"StringEquals": {"s3:signatureversion": "AWS4-HMAC-SHA256"}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, "signature-age-presigned-allowed"),
                    "Condition": {"NumericLessThan": {"s3:signatureAge": "600000"}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, "signature-age-presigned-denied"),
                    "Condition": {"NumericGreaterThan": {"s3:signatureAge": "604800000"}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:PutObject",
                    "Resource": object_resource(&bucket, "signature-age-post-fresh-allowed"),
                    "Condition": {"NumericLessThan": {"s3:signatureAge": "600000"}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:PutObject",
                    "Resource": object_resource(&bucket, "signature-age-post-old-denied"),
                    "Condition": {"NumericLessThan": {"s3:signatureAge": "600000"}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, "tls-version-allowed"),
                    "Condition": {"NumericGreaterThanEquals": {"s3:TlsVersion": "1.2"}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, "tls-version-denied"),
                    "Condition": {"NumericLessThan": {"s3:TlsVersion": "1.2"}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, "content-sha256-allowed"),
                    "Condition": {"StringEquals": {"s3:x-amz-content-sha256": empty_payload_sha256}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, "content-sha256-denied"),
                    "Condition": {"StringEquals": {"s3:x-amz-content-sha256": "0000000000000000000000000000000000000000000000000000000000000000"}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:PutObject",
                    "Resource": object_resource(&bucket, "website-redirect-allowed"),
                    "Condition": {"StringEquals": {"s3:x-amz-website-redirect-location": "/docs/allowed.html"}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:PutObject",
                    "Resource": object_resource(&bucket, "website-redirect-denied"),
                    "Condition": {"StringEquals": {"s3:x-amz-website-redirect-location": "/docs/allowed.html"}}
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let header_auth = raw_alt_object_status_eventually(
            "GetObject allowed by s3:authType REST-HEADER",
            RawAltObjectRequest::new("GET", &bucket, "auth-type-header"),
            200,
        )
        .await;
        assert_eq!(header_auth.body, "auth-type-header");

        let presigned = presign_url_with_credentials(
            "GET",
            &object_url(CTX.endpoint(), &bucket, "auth-type-presigned", None),
            std::time::Duration::from_secs(300),
            std::iter::empty::<(&str, &str)>(),
            None,
            raw_alt_credentials(),
        );
        let presigned_auth = raw_presigned_status_eventually(
            "GetObject allowed by s3:authType REST-QUERY-STRING",
            presigned.uri(),
            200,
        )
        .await;
        assert_eq!(presigned_auth.body, "auth-type-presigned");

        let post_credentials = raw_alt_credentials();
        let post_auth_type_fields = sigv4_post_fields_for_credentials(
            post_credentials.access_key,
            post_credentials.secret_key,
            post_credentials.region,
            &bucket,
            "auth-type-post",
            &[],
        );
        let post_auth_type = raw_alt_post_object_status_eventually(
            "PostObject allowed by s3:authType POST",
            &bucket,
            "auth-type-post",
            post_auth_type_fields,
            204,
        )
        .await;
        assert_eq!(post_auth_type.body, "");

        let post_auth_type_denied_fields = sigv4_post_fields_for_credentials(
            post_credentials.access_key,
            post_credentials.secret_key,
            post_credentials.region,
            &bucket,
            "auth-type-post-denied",
            &[],
        );
        let post_auth_type_denied = raw_alt_post_object_status_eventually(
            "PostObject denied by s3:authType REST-POST mismatch",
            &bucket,
            "auth-type-post-denied",
            post_auth_type_denied_fields,
            403,
        )
        .await;
        assert_raw_access_denied(
            "PostObject denied by s3:authType REST-POST mismatch",
            &post_auth_type_denied,
        );

        let denied_auth_type = raw_alt_object_status_eventually(
            "GetObject denied by s3:authType mismatch",
            RawAltObjectRequest::new("GET", &bucket, "auth-type-denied"),
            403,
        )
        .await;
        assert_raw_access_denied(
            "GetObject denied by s3:authType mismatch",
            &denied_auth_type,
        );

        let signature_version = raw_alt_object_status_eventually(
            "GetObject allowed by s3:signatureversion",
            RawAltObjectRequest::new("GET", &bucket, "signature-version"),
            200,
        )
        .await;
        assert_eq!(signature_version.body, "signature-version");

        let signature_age_allowed_url = presign_url_with_credentials(
            "GET",
            &object_url(
                CTX.endpoint(),
                &bucket,
                "signature-age-presigned-allowed",
                None,
            ),
            std::time::Duration::from_secs(300),
            std::iter::empty::<(&str, &str)>(),
            None,
            raw_alt_credentials(),
        );
        let signature_age_allowed = raw_presigned_status_eventually(
            "GetObject allowed by s3:signatureAge",
            signature_age_allowed_url.uri(),
            200,
        )
        .await;
        assert_eq!(
            signature_age_allowed.body,
            "signature-age-presigned-allowed"
        );

        let signature_age_denied_url = presign_url_with_credentials(
            "GET",
            &object_url(
                CTX.endpoint(),
                &bucket,
                "signature-age-presigned-denied",
                None,
            ),
            std::time::Duration::from_secs(300),
            std::iter::empty::<(&str, &str)>(),
            None,
            raw_alt_credentials(),
        );
        let signature_age_denied = raw_presigned_status_eventually(
            "GetObject denied by s3:signatureAge mismatch",
            signature_age_denied_url.uri(),
            403,
        )
        .await;
        assert_raw_access_denied(
            "GetObject denied by s3:signatureAge mismatch",
            &signature_age_denied,
        );

        let signature_age_post_allowed_fields = sigv4_post_fields_for_credentials(
            post_credentials.access_key,
            post_credentials.secret_key,
            post_credentials.region,
            &bucket,
            "signature-age-post-fresh-allowed",
            &[],
        );
        let signature_age_post_allowed = raw_alt_post_object_status_eventually(
            "PostObject allowed by fresh s3:signatureAge",
            &bucket,
            "signature-age-post-fresh-allowed",
            signature_age_post_allowed_fields,
            204,
        )
        .await;
        assert_eq!(signature_age_post_allowed.body, "");

        let old_post_signing_epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time after unix epoch")
            .as_secs()
            .saturating_sub(660);
        let signature_age_post_denied_fields = sigv4_post_fields_for_credentials_at_epoch(
            post_credentials.access_key,
            post_credentials.secret_key,
            post_credentials.region,
            &bucket,
            "signature-age-post-old-denied",
            old_post_signing_epoch,
            &[],
        );
        let signature_age_post_denied = raw_alt_post_object_status_eventually(
            "PostObject denied by old s3:signatureAge",
            &bucket,
            "signature-age-post-old-denied",
            signature_age_post_denied_fields,
            403,
        )
        .await;
        assert_raw_access_denied(
            "PostObject denied by old s3:signatureAge",
            &signature_age_post_denied,
        );

        let tls_version_allowed = raw_alt_object_status_eventually(
            "GetObject allowed by s3:TlsVersion",
            RawAltObjectRequest::new("GET", &bucket, "tls-version-allowed"),
            200,
        )
        .await;
        assert_eq!(tls_version_allowed.body, "tls-version-allowed");

        let tls_version_denied = raw_alt_object_status_eventually(
            "GetObject denied by s3:TlsVersion mismatch",
            RawAltObjectRequest::new("GET", &bucket, "tls-version-denied"),
            403,
        )
        .await;
        assert_raw_access_denied(
            "GetObject denied by s3:TlsVersion mismatch",
            &tls_version_denied,
        );

        let content_sha256_allowed = raw_alt_object_status_eventually(
            "GetObject allowed by s3:x-amz-content-sha256",
            RawAltObjectRequest::new("GET", &bucket, "content-sha256-allowed"),
            200,
        )
        .await;
        assert_eq!(content_sha256_allowed.body, "content-sha256-allowed");

        let content_sha256_denied = raw_alt_object_status_eventually(
            "GetObject denied by s3:x-amz-content-sha256 mismatch",
            RawAltObjectRequest::new("GET", &bucket, "content-sha256-denied"),
            403,
        )
        .await;
        assert_raw_access_denied(
            "GetObject denied by s3:x-amz-content-sha256 mismatch",
            &content_sha256_denied,
        );

        let redirect_allowed_headers = [("x-amz-website-redirect-location", "/docs/allowed.html")];
        let website_redirect_allowed = raw_alt_object_status_eventually(
            "PutObject allowed by s3:x-amz-website-redirect-location",
            RawAltObjectRequest::new("PUT", &bucket, "website-redirect-allowed")
                .extra_headers(&redirect_allowed_headers),
            200,
        )
        .await;
        assert_eq!(website_redirect_allowed.body, "");

        let redirect_denied_headers = [("x-amz-website-redirect-location", "/docs/denied.html")];
        let website_redirect_denied = raw_alt_object_status_eventually(
            "PutObject denied by s3:x-amz-website-redirect-location mismatch",
            RawAltObjectRequest::new("PUT", &bucket, "website-redirect-denied")
                .extra_headers(&redirect_denied_headers),
            403,
        )
        .await;
        assert_raw_access_denied(
            "PutObject denied by s3:x-amz-website-redirect-location mismatch",
            &website_redirect_denied,
        );

        cleanup(&bucket, &keys).await;
    });
}

#[test]
fn test_bucket_policy_condition_key_names_are_case_insensitive() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let secure_key = "mixed-case-secure-transport";
        let region_key = "mixed-case-requested-region";
        let referer_key = "mixed-case-referer";
        let secure_value = endpoint_is_https().to_string();
        let referer = "https://example.com/mixed-case";

        for key in [secure_key, region_key, referer_key] {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(key.as_bytes()))
                .send()
                .await
                .unwrap();
        }

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, secure_key),
                    "Condition": {"Bool": {"aws:securetransport": secure_value}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, region_key),
                    "Condition": {"StringEquals": {"AWS:RequestedRegion": CTX.region()}}
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, referer_key),
                    "Condition": {"StringEquals": {"AWS:Referer": referer}}
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let secure = raw_alt_object_status_eventually(
            "GetObject allowed by mixed-case aws:SecureTransport",
            RawAltObjectRequest::new("GET", &bucket, secure_key),
            200,
        )
        .await;
        assert_eq!(secure.body, secure_key);

        let region = raw_alt_object_status_eventually(
            "GetObject allowed by mixed-case aws:RequestedRegion",
            RawAltObjectRequest::new("GET", &bucket, region_key),
            200,
        )
        .await;
        assert_eq!(region.body, region_key);

        let referer_headers = [("referer", referer)];
        let referer_response = raw_alt_object_status_eventually(
            "GetObject allowed by mixed-case aws:referer",
            RawAltObjectRequest::new("GET", &bucket, referer_key).extra_headers(&referer_headers),
            200,
        )
        .await;
        assert_eq!(referer_response.body, referer_key);

        cleanup(&bucket, &[secure_key, region_key, referer_key]).await;
    });
}

#[test]
fn test_bucket_policy_existing_object_tag_prefix_key_is_case_insensitive() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let matching_key = "mixed-case-existing-tag-matches";
        let differently_cased_key = "mixed-case-existing-tag-different-case";
        let conflicting_denied_key = "mixed-case-existing-tag-conflicting-denied";
        let conflicting_allowed_key = "mixed-case-existing-tag-conflicting-allowed";

        for key in [
            matching_key,
            differently_cased_key,
            conflicting_denied_key,
            conflicting_allowed_key,
        ] {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(key.as_bytes()))
                .send()
                .await
                .unwrap();
        }

        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(matching_key)
            .tagging(simple_bucket_tagging("Classification", "public"))
            .send()
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(differently_cased_key)
            .tagging(simple_bucket_tagging("classification", "public"))
            .send()
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(conflicting_denied_key)
            .tagging(
                Tagging::builder()
                    .tag_set(
                        Tag::builder()
                            .key("Classification")
                            .value("private")
                            .build()
                            .unwrap(),
                    )
                    .tag_set(
                        Tag::builder()
                            .key("classification")
                            .value("public")
                            .build()
                            .unwrap(),
                    )
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(conflicting_allowed_key)
            .tagging(
                Tagging::builder()
                    .tag_set(
                        Tag::builder()
                            .key("Classification")
                            .value("public")
                            .build()
                            .unwrap(),
                    )
                    .tag_set(
                        Tag::builder()
                            .key("classification")
                            .value("private")
                            .build()
                            .unwrap(),
                    )
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .unwrap();

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:GetObject",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "S3:ExistingObjectTag/Classification": "public"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let allowed = eventually_ok(
            "GetObject with mixed-case ExistingObjectTag condition key prefix and exact tag case",
            || {
                alt_client
                    .get_object()
                    .bucket(&bucket)
                    .key(matching_key)
                    .send()
            },
        )
        .await;
        assert_eq!(
            allowed.body.collect().await.unwrap().into_bytes().as_ref(),
            matching_key.as_bytes()
        );

        let allowed = eventually_ok(
            "GetObject with mixed-case ExistingObjectTag condition key prefix and different tag case",
            || {
                alt_client
                    .get_object()
                    .bucket(&bucket)
                    .key(differently_cased_key)
                    .send()
            },
        )
        .await;
        assert_eq!(
            allowed.body.collect().await.unwrap().into_bytes().as_ref(),
            differently_cased_key.as_bytes()
        );

        eventually_access_denied(
            "GetObject with mixed-case ExistingObjectTag condition key prefix and conflicting denied tag cases",
            || {
                alt_client
                    .get_object()
                    .bucket(&bucket)
                    .key(conflicting_denied_key)
                    .send()
            },
        )
        .await;

        let allowed = eventually_ok(
            "GetObject with mixed-case ExistingObjectTag condition key prefix and conflicting allowed tag cases",
            || {
                alt_client
                    .get_object()
                    .bucket(&bucket)
                    .key(conflicting_allowed_key)
                    .send()
            },
        )
        .await;
        assert_eq!(
            allowed.body.collect().await.unwrap().into_bytes().as_ref(),
            conflicting_allowed_key.as_bytes()
        );

        cleanup(
            &bucket,
            &[
                matching_key,
                differently_cased_key,
                conflicting_denied_key,
                conflicting_allowed_key,
            ],
        )
        .await;
    });
}

#[test]
fn test_bucket_policy_request_object_tag_prefix_key_is_case_insensitive() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let matching_key = "mixed-case-request-tag-matches";
        let differently_cased_key = "mixed-case-request-tag-different-case";
        let conflicting_allowed_key = "mixed-case-request-tag-conflicting-allowed";
        let conflicting_reversed_key = "mixed-case-request-tag-conflicting-reversed";

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": ["s3:PutObject", "s3:PutObjectTagging"],
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "S3:RequestObjectTag/Classification": "public"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_ok(
            "PutObject with mixed-case RequestObjectTag condition key prefix and exact tag case",
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key(matching_key)
                    .tagging("Classification=public")
                    .body(ByteStream::from_static(matching_key.as_bytes()))
                    .send()
            },
        )
        .await;

        eventually_ok(
            "PutObject with mixed-case RequestObjectTag condition key prefix and different tag case",
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key(differently_cased_key)
                    .tagging("classification=public")
                    .body(ByteStream::from_static(differently_cased_key.as_bytes()))
                    .send()
            },
        )
        .await;

        eventually_ok_with_retry(
            "PutObject with mixed-case RequestObjectTag condition key prefix and conflicting tag cases",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key(conflicting_allowed_key)
                    .tagging("Classification=private&classification=public")
                    .body(ByteStream::from_static(conflicting_allowed_key.as_bytes()))
                    .send()
            },
        )
        .await;

        eventually_ok_with_retry(
            "PutObject with mixed-case RequestObjectTag condition key prefix and reversed conflicting tag cases",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key(conflicting_reversed_key)
                    .tagging("classification=public&Classification=private")
                    .body(ByteStream::from_static(conflicting_reversed_key.as_bytes()))
                    .send()
            },
        )
        .await;

        cleanup(
            &bucket,
            &[
                matching_key,
                differently_cased_key,
                conflicting_allowed_key,
                conflicting_reversed_key,
            ],
        )
        .await;
    });
}

#[test]
fn test_bucket_policy_request_object_tag_case_equivalent_values_preserve_set_operators() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let for_all_allowed_key = "request-tag-for-all-case-equivalent-allowed";
        let for_all_denied_key = "request-tag-for-all-case-equivalent-denied";
        let for_any_allowed_key = "request-tag-for-any-case-equivalent-allowed";
        let for_any_denied_key = "request-tag-for-any-case-equivalent-denied";

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": ["s3:PutObject", "s3:PutObjectTagging"],
                    "Resource": [
                        object_resource(&bucket, for_all_allowed_key),
                        object_resource(&bucket, for_all_denied_key)
                    ],
                    "Condition": {
                        "ForAllValues:StringEquals": {
                            "S3:RequestObjectTag/Classification": "public"
                        }
                    }
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": ["s3:PutObject", "s3:PutObjectTagging"],
                    "Resource": [
                        object_resource(&bucket, for_any_allowed_key),
                        object_resource(&bucket, for_any_denied_key)
                    ],
                    "Condition": {
                        "ForAnyValue:StringEquals": {
                            "S3:RequestObjectTag/Classification": "public"
                        }
                    }
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_ok_with_retry(
            "PutObject with ForAllValues:StringEquals and all case-equivalent request tag values matching",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key(for_all_allowed_key)
                    .tagging("Classification=public&classification=public")
                    .body(ByteStream::from_static(for_all_allowed_key.as_bytes()))
                    .send()
            },
        )
        .await;

        eventually_access_denied(
            "PutObject denied by ForAllValues:StringEquals when one case-equivalent request tag differs",
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key(for_all_denied_key)
                    .tagging("Classification=public&classification=private")
                    .body(ByteStream::from_static(for_all_denied_key.as_bytes()))
                    .send()
            },
        )
        .await;

        eventually_ok_with_retry(
            "PutObject with ForAnyValue:StringEquals and one case-equivalent request tag matching",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key(for_any_allowed_key)
                    .tagging("Classification=private&classification=public")
                    .body(ByteStream::from_static(for_any_allowed_key.as_bytes()))
                    .send()
            },
        )
        .await;

        eventually_access_denied(
            "PutObject denied by ForAnyValue:StringEquals when no case-equivalent request tag matches",
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key(for_any_denied_key)
                    .tagging("Classification=private&classification=internal")
                    .body(ByteStream::from_static(for_any_denied_key.as_bytes()))
                    .send()
            },
        )
        .await;

        cleanup(
            &bucket,
            &[
                for_all_allowed_key,
                for_all_denied_key,
                for_any_allowed_key,
                for_any_denied_key,
            ],
        )
        .await;
    });
}

#[test]
fn test_bucket_policy_request_object_tag_case_equivalent_values_preserve_binary_equals() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let binary_allowed_key = "request-tag-binary-case-equivalent-allowed";
        let binary_denied_key = "request-tag-binary-case-equivalent-denied";
        let for_all_allowed_key = "request-tag-binary-for-all-case-equivalent-allowed";
        let for_all_denied_key = "request-tag-binary-for-all-case-equivalent-denied";
        let for_any_allowed_key = "request-tag-binary-for-any-case-equivalent-allowed";
        let for_any_denied_key = "request-tag-binary-for-any-case-equivalent-denied";

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": ["s3:PutObject", "s3:PutObjectTagging"],
                    "Resource": [
                        object_resource(&bucket, binary_allowed_key),
                        object_resource(&bucket, binary_denied_key)
                    ],
                    "Condition": {
                        "BinaryEquals": {
                            "S3:RequestObjectTag/Classification": "cHVibGlj"
                        }
                    }
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": ["s3:PutObject", "s3:PutObjectTagging"],
                    "Resource": [
                        object_resource(&bucket, for_all_allowed_key),
                        object_resource(&bucket, for_all_denied_key)
                    ],
                    "Condition": {
                        "ForAllValues:BinaryEquals": {
                            "S3:RequestObjectTag/Classification": "cHVibGlj"
                        }
                    }
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": ["s3:PutObject", "s3:PutObjectTagging"],
                    "Resource": [
                        object_resource(&bucket, for_any_allowed_key),
                        object_resource(&bucket, for_any_denied_key)
                    ],
                    "Condition": {
                        "ForAnyValue:BinaryEquals": {
                            "S3:RequestObjectTag/Classification": "cHVibGlj"
                        }
                    }
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_ok_with_retry(
            "PutObject with BinaryEquals and one case-equivalent request tag value matching",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key(binary_allowed_key)
                    .tagging("Classification=cHJpdmF0ZQ==&classification=cHVibGlj")
                    .body(ByteStream::from_static(binary_allowed_key.as_bytes()))
                    .send()
            },
        )
        .await;

        eventually_access_denied(
            "PutObject denied by BinaryEquals when no case-equivalent request tag value matches",
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key(binary_denied_key)
                    .tagging("Classification=cHJpdmF0ZQ==&classification=aW50ZXJuYWw=")
                    .body(ByteStream::from_static(binary_denied_key.as_bytes()))
                    .send()
            },
        )
        .await;

        eventually_ok_with_retry(
            "PutObject with ForAllValues:BinaryEquals and all case-equivalent request tag values matching",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key(for_all_allowed_key)
                    .tagging("Classification=cHVibGlj&classification=cHVibGlj")
                    .body(ByteStream::from_static(for_all_allowed_key.as_bytes()))
                    .send()
            },
        )
        .await;

        eventually_access_denied(
            "PutObject denied by ForAllValues:BinaryEquals when one case-equivalent request tag differs",
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key(for_all_denied_key)
                    .tagging("Classification=cHVibGlj&classification=cHJpdmF0ZQ==")
                    .body(ByteStream::from_static(for_all_denied_key.as_bytes()))
                    .send()
            },
        )
        .await;

        eventually_ok_with_retry(
            "PutObject with ForAnyValue:BinaryEquals and one case-equivalent request tag matching",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key(for_any_allowed_key)
                    .tagging("Classification=cHJpdmF0ZQ==&classification=cHVibGlj")
                    .body(ByteStream::from_static(for_any_allowed_key.as_bytes()))
                    .send()
            },
        )
        .await;

        eventually_access_denied(
            "PutObject denied by ForAnyValue:BinaryEquals when no case-equivalent request tag matches",
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key(for_any_denied_key)
                    .tagging("Classification=cHJpdmF0ZQ==&classification=aW50ZXJuYWw=")
                    .body(ByteStream::from_static(for_any_denied_key.as_bytes()))
                    .send()
            },
        )
        .await;

        cleanup(
            &bucket,
            &[
                binary_allowed_key,
                binary_denied_key,
                for_all_allowed_key,
                for_all_denied_key,
                for_any_allowed_key,
                for_any_denied_key,
            ],
        )
        .await;
    });
}

#[test]
fn test_bucket_policy_request_object_tag_binary_equals_if_exists_matches_missing_tag() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": ["s3:PutObject", "s3:PutObjectTagging"],
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "BinaryEqualsIfExists": {
                        "s3:RequestObjectTag/security": "cHVibGlj"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_ok(
            "PutObject with missing BinaryEqualsIfExists request tag",
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("binary-if-exists-missing")
                    .body(ByteStream::from_static(b"binary-if-exists-missing"))
                    .send()
            },
        )
        .await;

        let denied = alt_client
            .put_object()
            .bucket(&bucket)
            .key("binary-if-exists-denied")
            .tagging("security=cHJpdmF0ZQ%3D%3D")
            .body(ByteStream::from_static(b"binary-if-exists-denied"))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        cleanup(
            &bucket,
            &["binary-if-exists-missing", "binary-if-exists-denied"],
        )
        .await;
    });
}

#[test]
fn test_bucket_policy_request_object_tag_keys_for_all_values_binary_equals() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let allowed_key = "binary-for-all-allowed";
        let denied_key = "binary-for-all-denied";

        for key in [allowed_key, denied_key] {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(b"object"))
                .send()
                .await
                .unwrap();
        }

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:PutObjectTagging",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "ForAllValues:BinaryEquals": {
                        "s3:RequestObjectTagKeys": ["YWJj", "ZGVm"]
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_ok(
            "PutObjectTagging with ForAllValues:BinaryEquals request tag keys",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&bucket)
                    .key(allowed_key)
                    .tagging(tagging(vec![tag("YWJj", "1"), tag("ZGVm", "2")]))
                    .send()
            },
        )
        .await;

        let denied = alt_client
            .put_object_tagging()
            .bucket(&bucket)
            .key(denied_key)
            .tagging(tagging(vec![tag("YWJj", "1"), tag("Z2hp", "3")]))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        cleanup(&bucket, &[allowed_key, denied_key]).await;
    });
}

#[test]
fn test_bucket_policy_request_object_tag_keys_for_any_value_binary_equals() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let allowed_key = "binary-for-any-allowed";
        let denied_key = "binary-for-any-denied";

        for key in [allowed_key, denied_key] {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(b"object"))
                .send()
                .await
                .unwrap();
        }

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:PutObjectTagging",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "ForAnyValue:BinaryEquals": {
                        "s3:RequestObjectTagKeys": ["YWJj", "ZGVm"]
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_ok(
            "PutObjectTagging with ForAnyValue:BinaryEquals request tag keys",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&bucket)
                    .key(allowed_key)
                    .tagging(tagging(vec![tag("Z2hp", "3"), tag("ZGVm", "2")]))
                    .send()
            },
        )
        .await;

        let denied = alt_client
            .put_object_tagging()
            .bucket(&bucket)
            .key(denied_key)
            .tagging(tagging(vec![tag("Z2hp", "3"), tag("amts", "4")]))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        cleanup(&bucket, &[allowed_key, denied_key]).await;
    });
}

#[test]
fn test_bucket_policy_request_object_tag_keys_for_all_values_string_not_equals() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let allowed_key = "string-not-equals-for-all-allowed";
        let denied_key = "string-not-equals-for-all-denied";

        for key in [allowed_key, denied_key] {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(b"object"))
                .send()
                .await
                .unwrap();
        }

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:PutObjectTagging",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "ForAllValues:StringNotEquals": {
                        "s3:RequestObjectTagKeys": ["blocked", "private"]
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_ok(
            "PutObjectTagging with ForAllValues:StringNotEquals request tag keys",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&bucket)
                    .key(allowed_key)
                    .tagging(tagging(vec![tag("public", "1"), tag("shared", "2")]))
                    .send()
            },
        )
        .await;

        let denied = alt_client
            .put_object_tagging()
            .bucket(&bucket)
            .key(denied_key)
            .tagging(tagging(vec![tag("public", "1"), tag("blocked", "2")]))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        cleanup(&bucket, &[allowed_key, denied_key]).await;
    });
}

#[test]
fn test_bucket_policy_request_object_tag_keys_for_any_value_string_not_equals() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let allowed_key = "string-not-equals-for-any-allowed";
        let denied_key = "string-not-equals-for-any-denied";

        for key in [allowed_key, denied_key] {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(b"object"))
                .send()
                .await
                .unwrap();
        }

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:PutObjectTagging",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "ForAnyValue:StringNotEquals": {
                        "s3:RequestObjectTagKeys": ["blocked", "private"]
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_ok(
            "PutObjectTagging with ForAnyValue:StringNotEquals request tag keys",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&bucket)
                    .key(allowed_key)
                    .tagging(tagging(vec![tag("blocked", "1"), tag("public", "2")]))
                    .send()
            },
        )
        .await;

        let denied = alt_client
            .put_object_tagging()
            .bucket(&bucket)
            .key(denied_key)
            .tagging(tagging(vec![tag("blocked", "1")]))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        cleanup(&bucket, &[allowed_key, denied_key]).await;
    });
}

#[test]
fn test_bucket_policy_put_object_no_tag_keys_for_all_values_string_not_equals() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let allowed_key = "string-not-equals-for-all-no-tags";
        let denied_key = "string-not-equals-for-all-blocked-tag";

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": ["s3:PutObject", "s3:PutObjectTagging"],
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "ForAllValues:StringNotEquals": {
                        "s3:RequestObjectTagKeys": ["blocked", "private"]
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_ok(
            "PutObject without tag keys under ForAllValues:StringNotEquals",
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key(allowed_key)
                    .body(ByteStream::from_static(b"for-all-no-tags"))
                    .send()
            },
        )
        .await;

        let denied = alt_client
            .put_object()
            .bucket(&bucket)
            .key(denied_key)
            .tagging("blocked=1")
            .body(ByteStream::from_static(b"for-all-blocked-tag"))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        cleanup(&bucket, &[allowed_key, denied_key]).await;
    });
}

#[test]
fn test_bucket_policy_put_object_no_tag_keys_for_any_value_string_not_equals() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let allowed_key = "string-not-equals-for-any-public-tag";
        let denied_key = "string-not-equals-for-any-no-tags";

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": ["s3:PutObject", "s3:PutObjectTagging"],
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "ForAnyValue:StringNotEquals": {
                        "s3:RequestObjectTagKeys": ["blocked", "private"]
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_ok(
            "PutObject with nonmatching tag key under ForAnyValue:StringNotEquals",
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key(allowed_key)
                    .tagging("public=1")
                    .body(ByteStream::from_static(b"for-any-public-tag"))
                    .send()
            },
        )
        .await;

        let denied = alt_client
            .put_object()
            .bucket(&bucket)
            .key(denied_key)
            .body(ByteStream::from_static(b"for-any-no-tags"))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        cleanup(&bucket, &[allowed_key, denied_key]).await;
    });
}

#[test]
fn test_bucket_policy_put_object_tagging_request_object_tag() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        let key = "request-tag-put-object-tagging";

        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"body"))
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
                        "Principal": principal,
                        "Action": "s3:PutObjectTagging",
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:RequestObjectTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_ok_with_retry(
            "PutObjectTagging with RequestObjectTag bucket policy",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&bucket)
                    .key(key)
                    .tagging(simple_bucket_tagging("security", "public"))
                    .send()
            },
        )
        .await;

        let tags = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(
            tags.tag_set()
                .iter()
                .map(|tag| (tag.key(), tag.value()))
                .collect::<Vec<_>>(),
            vec![("security", "public")]
        );

        eventually_access_denied(
            "PutObjectTagging denied by nonmatching RequestObjectTag bucket policy",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&bucket)
                    .key(key)
                    .tagging(simple_bucket_tagging("security", "private"))
                    .send()
            },
        )
        .await;

        let tags = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(
            tags.tag_set()
                .iter()
                .map(|tag| (tag.key(), tag.value()))
                .collect::<Vec<_>>(),
            vec![("security", "public")]
        );

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_put_object_version_tagging_request_object_tag() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        let key = "request-tag-put-object-version-tagging";

        s3_tests::create_bucket(client, &bucket).await.unwrap();
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
        let version_id = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"body"))
            .send()
            .await
            .unwrap()
            .version_id()
            .expect("expected VersionId for versioned object")
            .to_string();

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal,
                        "Action": "s3:PutObjectVersionTagging",
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:RequestObjectTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_ok_with_retry(
            "PutObjectVersionTagging with RequestObjectTag bucket policy",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&bucket)
                    .key(key)
                    .version_id(&version_id)
                    .tagging(simple_bucket_tagging("security", "public"))
                    .send()
            },
        )
        .await;

        let tags = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .send()
            .await
            .unwrap();
        assert_eq!(
            tags.tag_set()
                .iter()
                .map(|tag| (tag.key(), tag.value()))
                .collect::<Vec<_>>(),
            vec![("security", "public")]
        );

        eventually_access_denied(
            "PutObjectVersionTagging denied by nonmatching RequestObjectTag bucket policy",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&bucket)
                    .key(key)
                    .version_id(&version_id)
                    .tagging(simple_bucket_tagging("security", "private"))
                    .send()
            },
        )
        .await;

        let tags = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .send()
            .await
            .unwrap();
        assert_eq!(
            tags.tag_set()
                .iter()
                .map(|tag| (tag.key(), tag.value()))
                .collect::<Vec<_>>(),
            vec![("security", "public")]
        );

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_bucket_policy_string_equals_ignore_case_request_object_tag() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
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
                            "StringEqualsIgnoreCase": {
                                "s3:RequestObjectTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_ok(
            "PutObject with StringEqualsIgnoreCase request-object-tag condition",
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("allowed-uppercase")
                    .tagging("security=PUBLIC")
                    .body(ByteStream::from_static(b"allowed"))
                    .send()
            },
        )
        .await;

        eventually_access_denied(
            "PutObject with nonmatching StringEqualsIgnoreCase request-object-tag condition",
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("denied-private")
                    .tagging("security=private")
                    .body(ByteStream::from_static(b"denied"))
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &["allowed-uppercase", "denied-private"]).await;
    });
}

#[test]
fn test_bucket_policy_string_equals_ignore_case_request_object_tag_non_ascii() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
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
                            "StringEqualsIgnoreCase": {
                                "s3:RequestObjectTag/classification": "sëcret"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_ok(
            "PutObject with non-ASCII StringEqualsIgnoreCase request-object-tag condition",
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("allowed-non-ascii-uppercase")
                    .tagging("classification=S%C3%8BCRET")
                    .body(ByteStream::from_static(b"allowed"))
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &["allowed-non-ascii-uppercase"]).await;
    });
}

#[test]
fn test_bucket_policy_string_not_equals_ignore_case_request_object_tag() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
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
                            "Action": ["s3:PutObject", "s3:PutObjectTagging"],
                            "Resource": bucket_wildcard_resource(&bucket)
                        },
                        {
                            "Effect": "Deny",
                            "Principal": alt_policy_principal(),
                            "Action": ["s3:PutObject", "s3:PutObjectTagging"],
                            "Resource": bucket_wildcard_resource(&bucket),
                            "Condition": {
                                "StringNotEqualsIgnoreCase": {
                                    "s3:RequestObjectTag/security": "public"
                                }
                            }
                        }
                    ],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_ok(
            "PutObject exempted from StringNotEqualsIgnoreCase deny",
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("allowed-uppercase")
                    .tagging("security=PUBLIC")
                    .body(ByteStream::from_static(b"allowed"))
                    .send()
            },
        )
        .await;

        eventually_access_denied(
            "PutObject denied by StringNotEqualsIgnoreCase request-object-tag condition",
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("denied-private")
                    .tagging("security=private")
                    .body(ByteStream::from_static(b"denied"))
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &["allowed-uppercase", "denied-private"]).await;
    });
}

#[test]
fn test_bucket_policy_string_not_equals_ignore_case_request_object_tag_non_ascii() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
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
                            "Action": ["s3:PutObject", "s3:PutObjectTagging"],
                            "Resource": bucket_wildcard_resource(&bucket)
                        },
                        {
                            "Effect": "Deny",
                            "Principal": alt_policy_principal(),
                            "Action": ["s3:PutObject", "s3:PutObjectTagging"],
                            "Resource": bucket_wildcard_resource(&bucket),
                            "Condition": {
                                "StringNotEqualsIgnoreCase": {
                                    "s3:RequestObjectTag/classification": "sëcret"
                                }
                            }
                        }
                    ],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_ok(
            "PutObject exempted from non-ASCII StringNotEqualsIgnoreCase deny",
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("allowed-non-ascii-uppercase")
                    .tagging("classification=S%C3%8BCRET")
                    .body(ByteStream::from_static(b"allowed"))
                    .send()
            },
        )
        .await;

        eventually_access_denied(
            "PutObject denied by non-ASCII StringNotEqualsIgnoreCase request-object-tag condition",
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("denied-private")
                    .tagging("classification=private")
                    .body(ByteStream::from_static(b"denied"))
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &["allowed-non-ascii-uppercase", "denied-private"]).await;
    });
}

#[test]
fn test_bucket_policy_string_equals_ignore_case_if_exists_request_object_tag() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
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
                            "StringEqualsIgnoreCaseIfExists": {
                                "s3:RequestObjectTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_ok(
            "PutObject with absent StringEqualsIgnoreCaseIfExists request-object-tag condition",
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("allowed-absent")
                    .body(ByteStream::from_static(b"allowed"))
                    .send()
            },
        )
        .await;

        eventually_access_denied(
            "PutObject with nonmatching StringEqualsIgnoreCaseIfExists request-object-tag condition",
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("denied-private")
                    .tagging("security=private")
                    .body(ByteStream::from_static(b"denied"))
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &["allowed-absent", "denied-private"]).await;
    });
}

#[test]
fn test_bucket_policy_string_not_equals_ignore_case_if_exists_request_object_tag() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
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
                            "Action": ["s3:PutObject", "s3:PutObjectTagging"],
                            "Resource": bucket_wildcard_resource(&bucket)
                        },
                        {
                            "Effect": "Deny",
                            "Principal": alt_policy_principal(),
                            "Action": ["s3:PutObject", "s3:PutObjectTagging"],
                            "Resource": bucket_wildcard_resource(&bucket),
                            "Condition": {
                                "StringNotEqualsIgnoreCaseIfExists": {
                                    "s3:RequestObjectTag/security": "public"
                                }
                            }
                        }
                    ],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_ok(
            "PutObject exempted from StringNotEqualsIgnoreCaseIfExists deny",
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("allowed-uppercase")
                    .tagging("security=PUBLIC")
                    .body(ByteStream::from_static(b"allowed"))
                    .send()
            },
        )
        .await;

        eventually_access_denied(
            "PutObject with absent key denied by StringNotEqualsIgnoreCaseIfExists",
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("denied-absent")
                    .body(ByteStream::from_static(b"denied"))
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &["allowed-uppercase", "denied-absent"]).await;
    });
}

#[test]
fn test_bucket_policy_put_object_acl_request_object_tag_condition_is_rejected() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let key = "request-tag-acl";
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"body"))
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
                        "Action": "s3:PutObjectAcl",
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:RequestObjectTag/security": "public"
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

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_put_object_acl_and_tagging_request_object_tag_condition_is_rejected() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let key = "request-tag-mixed";
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"body"))
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
                        "Action": ["s3:PutObjectAcl", "s3:PutObjectTagging"],
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:RequestObjectTag/security": "public"
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

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_delete_object_tagging_existing_tag_condition() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let public_key = "publictag-delete-tags";
        let private_key = "privatetag-delete-tags";

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        for (key, tag_value) in [(public_key, "public"), (private_key, "private")] {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(b"tagged-body"))
                .send()
                .await
                .unwrap();
            client
                .put_object_tagging()
                .bucket(&bucket)
                .key(key)
                .tagging(simple_bucket_tagging("security", tag_value))
                .send()
                .await
                .unwrap();
        }

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:DeleteObjectTagging",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:ExistingObjectTag/security": "public"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_ok_with_retry(
            "DeleteObjectTagging with ExistingObjectTag bucket policy",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .delete_object_tagging()
                    .bucket(&bucket)
                    .key(public_key)
                    .send()
            },
        )
        .await;

        let public_tags = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(public_key)
            .send()
            .await
            .unwrap();
        assert!(
            public_tags.tag_set().is_empty(),
            "expected public object tags to be deleted, got {:?}",
            public_tags.tag_set()
        );

        eventually_access_denied(
            "DeleteObjectTagging denied by nonmatching ExistingObjectTag bucket policy",
            || {
                alt_client
                    .delete_object_tagging()
                    .bucket(&bucket)
                    .key(private_key)
                    .send()
            },
        )
        .await;

        let private_tags = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(private_key)
            .send()
            .await
            .unwrap();
        assert_eq!(
            private_tags
                .tag_set()
                .iter()
                .map(|tag| (tag.key(), tag.value()))
                .collect::<Vec<_>>(),
            vec![("security", "private")]
        );

        cleanup(&bucket, &[public_key, private_key]).await;
    });
}

#[test]
fn test_bucket_policy_copy_source_condition_is_rejected_for_mixed_get_and_put() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let key = "copy-source-mixed";
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"body"))
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
                        "Action": ["s3:GetObject", "s3:PutObject"],
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "StringLike": {
                                "s3:x-amz-copy-source": "src/public/*"
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

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_metadata_directive_condition_is_rejected_for_mixed_get_and_put() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let key = "metadata-directive-mixed";
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"body"))
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
                        "Action": ["s3:GetObject", "s3:PutObject"],
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:x-amz-metadata-directive": "COPY"
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

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_sse_s3_condition_is_rejected_for_mixed_get_and_put() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let key = "sse-s3-mixed";
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"body"))
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
                        "Action": ["s3:GetObject", "s3:PutObject"],
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "Null": {
                                "s3:x-amz-server-side-encryption": "true"
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

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_sse_c_condition_is_rejected_for_mixed_get_and_put() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let key = "sse-c-mixed";
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"body"))
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
                        "Action": ["s3:GetObject", "s3:PutObject"],
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "Null": {
                                "s3:x-amz-server-side-encryption-customer-algorithm": "true"
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

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_canned_acl_condition_is_rejected_for_mixed_get_and_put() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let key = "canned-acl-mixed";
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"body"))
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
                        "Action": ["s3:GetObject", "s3:PutObject"],
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:x-amz-acl": "private"
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

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_grant_read_condition_is_rejected_for_mixed_get_and_put() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let key = "grant-read-mixed";
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"body"))
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
                        "Action": ["s3:GetObject", "s3:PutObject"],
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:x-amz-grant-read": "uri=http://acs.amazonaws.com/groups/global/AllUsers"
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

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_copy_source_condition_is_rejected_for_mixed_get_version_and_put() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let key = "copy-source-mixed-version";
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"body"))
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
                        "Action": ["s3:GetObjectVersion", "s3:PutObject"],
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "StringLike": {
                                "s3:x-amz-copy-source": "src/public/*"
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

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_canned_acl_condition_is_rejected_for_mixed_get_version_and_put() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let key = "canned-acl-mixed-version";
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"body"))
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
                        "Action": ["s3:GetObjectVersion", "s3:PutObject"],
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:x-amz-acl": "private"
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

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_sse_s3_condition_is_rejected_for_mixed_get_version_and_put() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let key = "sse-s3-mixed-version";
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"body"))
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
                        "Action": ["s3:GetObjectVersion", "s3:PutObject"],
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "Null": {
                                "s3:x-amz-server-side-encryption": "true"
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

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_metadata_directive_condition_is_rejected_for_mixed_get_version_and_put() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let key = "metadata-directive-mixed-version";
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"body"))
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
                        "Action": ["s3:GetObjectVersion", "s3:PutObject"],
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:x-amz-metadata-directive": "COPY"
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

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_sse_c_condition_is_rejected_for_mixed_get_version_and_put() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let key = "sse-c-mixed-version";
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"body"))
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
                        "Action": ["s3:GetObjectVersion", "s3:PutObject"],
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "Null": {
                                "s3:x-amz-server-side-encryption-customer-algorithm": "true"
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

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_grant_read_acp_condition_is_rejected_for_mixed_get_and_put() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let key = "grant-read-acp-mixed";
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"body"))
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
                        "Action": ["s3:GetObject", "s3:PutObject"],
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:x-amz-grant-read-acp": "uri=http://acs.amazonaws.com/groups/global/AllUsers"
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

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_grant_full_control_condition_is_rejected_for_mixed_get_and_put() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let key = "grant-full-control-mixed";
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"body"))
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
                        "Action": ["s3:GetObject", "s3:PutObject"],
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:x-amz-grant-full-control": "uri=http://acs.amazonaws.com/groups/global/AllUsers"
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

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_grant_read_acp_condition_is_rejected_for_mixed_get_version_and_put() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let key = "grant-read-acp-mixed-version";
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"body"))
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
                        "Action": ["s3:GetObjectVersion", "s3:PutObject"],
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:x-amz-grant-read-acp": "uri=http://acs.amazonaws.com/groups/global/AllUsers"
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

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_grant_full_control_condition_is_rejected_for_mixed_get_version_and_put() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let key = "grant-full-control-mixed-version";
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"body"))
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
                        "Action": ["s3:GetObjectVersion", "s3:PutObject"],
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:x-amz-grant-full-control": "uri=http://acs.amazonaws.com/groups/global/AllUsers"
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

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_put_object_inline_tags_require_put_object_tagging() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                principal.clone(),
                "Allow",
                "s3:PutObject",
                bucket_wildcard_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        eventually_result_matches(
            "PutObject with inline tags denied without PutObjectTagging permission",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("denied")
                    .tagging("security=public")
                    .body(ByteStream::from_static(b"denied"))
                    .send()
            },
            |result| {
                result.as_ref().err().is_some_and(|err| {
                    err.raw_response().map(|r| r.status().as_u16()) == Some(403)
                        && err.as_service_error().and_then(ProvideErrorMetadata::code)
                            == Some("AccessDenied")
                })
            },
        )
        .await;

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": ["s3:PutObject", "s3:PutObjectTagging"],
                "Resource": bucket_wildcard_resource(&bucket),
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_ok_with_retry(
            "PutObject with inline tags under PutObjectTagging policy",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("allowed")
                    .tagging("security=public")
                    .body(ByteStream::from_static(b"allowed"))
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &["allowed", "denied"]).await;
    });
}

#[test]
fn test_bucket_policy_multipart_upload_requires_object_resource() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_bucket_allowing_public_policy(client).await;
        let key = "mpobj";

        let denied = alt_client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        let bucket_only = client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                principal.clone(),
                "Allow",
                "s3:PutObject",
                bucket_resource(&bucket),
            ))
            .send()
            .await;
        assert_eq!(err_status(&bucket_only), 400);
        assert_s3_err_code(&bucket_only, "MalformedPolicy");

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                principal,
                "Allow",
                "s3:PutObject",
                format!("arn:aws:s3:::{bucket}/{key}"),
            ))
            .send()
            .await
            .unwrap();

        let upload = eventually_ok("CreateMultipartUpload with object resource policy", || {
            alt_client
                .create_multipart_upload()
                .bucket(&bucket)
                .key(key)
                .send()
        })
        .await;
        alt_client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload.upload_id().unwrap())
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_multipart_upload_on_bucket_with_policy() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let key = "foo";

        let owner_principal = json!({
            "AWS": format!("arn:aws:iam::{}:root", CTX.account_id())
        });
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": owner_principal,
                "Action": "*",
                "Resource": [
                    bucket_resource(&bucket),
                    bucket_wildcard_resource(&bucket),
                ],
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let upload = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = upload.upload_id().unwrap().to_string();
        let part = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from_static(b"policy-body"))
            .send()
            .await
            .unwrap();
        complete_single_part_upload(client, &bucket, key, &upload_id, part.e_tag().unwrap()).await;

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"policy-body");

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_multipart_upload_request_object_tag() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": ["s3:PutObject", "s3:PutObjectTagging"],
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:RequestObjectTag/security": "public"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();
        client
            .get_bucket_policy()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        let denied = alt_client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("denied")
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        let upload = eventually_ok(
            "CreateMultipartUpload with request-object-tag condition",
            || {
                alt_client
                    .create_multipart_upload()
                    .bucket(&bucket)
                    .key("allowed")
                    .tagging("security=public")
                    .send()
            },
        )
        .await;
        let upload_id = upload.upload_id().unwrap().to_string();
        alt_client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("allowed")
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &["allowed", "denied"]).await;
    });
}

#[test]
fn test_bucket_policy_multipart_upload_inline_tags_require_put_object_tagging() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                principal.clone(),
                "Allow",
                "s3:PutObject",
                bucket_wildcard_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        eventually_result_matches(
            "CreateMultipartUpload with inline tags denied without PutObjectTagging permission",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .create_multipart_upload()
                    .bucket(&bucket)
                    .key("denied")
                    .tagging("security=public")
                    .send()
            },
            |result| {
                result.as_ref().err().is_some_and(|err| {
                    err.raw_response().map(|r| r.status().as_u16()) == Some(403)
                        && err.as_service_error().and_then(ProvideErrorMetadata::code)
                            == Some("AccessDenied")
                })
            },
        )
        .await;

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": ["s3:PutObject", "s3:PutObjectTagging"],
                "Resource": bucket_wildcard_resource(&bucket),
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let upload = eventually_ok_with_retry(
            "CreateMultipartUpload with inline tags under PutObjectTagging policy",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .create_multipart_upload()
                    .bucket(&bucket)
                    .key("allowed")
                    .tagging("security=public")
                    .send()
            },
        )
        .await;
        let upload_id = upload.upload_id().unwrap().to_string();

        alt_client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("allowed")
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &["allowed", "denied"]).await;
    });
}

#[test]
fn test_bucket_policy_canned_acl_condition_is_rejected_for_put_object_tagging() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let result = client
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
                            "StringEquals": {
                                "s3:x-amz-acl": "private"
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

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_canned_acl_condition_is_rejected_for_mixed_put_object_and_tagging() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let result = client
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
                            "StringEquals": {
                                "s3:x-amz-acl": "private"
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

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_sse_condition_is_rejected_for_put_object_tagging() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let result = client
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
                            "StringEquals": {
                                "s3:x-amz-server-side-encryption": "AES256"
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

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_sse_condition_is_rejected_for_mixed_put_object_and_tagging() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let result = client
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
                            "StringEquals": {
                                "s3:x-amz-server-side-encryption": "AES256"
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

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_boe_put_object_acl_condition_applies() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket_request(client, &bucket)
            .object_ownership(ObjectOwnership::BucketOwnerEnforced)
            .send()
            .await
            .unwrap();
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:PutObject",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:x-amz-acl": "bucket-owner-full-control"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_result_matches(
            "BOE PutObject denied when x-amz-acl is absent under StringEquals",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("denied")
                    .body(ByteStream::from_static(b"denied"))
                    .send()
            },
            |result| {
                result.as_ref().err().is_some_and(|err| {
                    err.raw_response().map(|r| r.status().as_u16()) == Some(403)
                        && err.as_service_error().and_then(ProvideErrorMetadata::code)
                            == Some("AccessDenied")
                })
            },
        )
        .await;

        eventually_ok_with_retry(
            "BOE PutObject with x-amz-acl=bucket-owner-full-control under StringEquals",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("allowed")
                    .acl(ObjectCannedAcl::BucketOwnerFullControl)
                    .body(ByteStream::from_static(b"allowed"))
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &["allowed", "denied"]).await;
    });
}

#[test]
fn test_bucket_policy_boe_multipart_upload_acl_condition_applies() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket_request(client, &bucket)
            .object_ownership(ObjectOwnership::BucketOwnerEnforced)
            .send()
            .await
            .unwrap();
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:PutObject",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:x-amz-acl": "bucket-owner-full-control"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_result_matches(
            "BOE CreateMultipartUpload denied when x-amz-acl is absent under StringEquals",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .create_multipart_upload()
                    .bucket(&bucket)
                    .key("denied")
                    .send()
            },
            |result| {
                result.as_ref().err().is_some_and(|err| {
                    err.raw_response().map(|r| r.status().as_u16()) == Some(403)
                        && err.as_service_error().and_then(ProvideErrorMetadata::code)
                            == Some("AccessDenied")
                })
            },
        )
        .await;

        let upload = eventually_ok_with_retry(
            "BOE CreateMultipartUpload with x-amz-acl=bucket-owner-full-control under StringEquals",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .create_multipart_upload()
                    .bucket(&bucket)
                    .key("allowed")
                    .acl(ObjectCannedAcl::BucketOwnerFullControl)
                    .send()
            },
        )
        .await;
        let upload_id = upload.upload_id().unwrap().to_string();

        alt_client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("allowed")
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &["allowed", "denied"]).await;
    });
}

#[test]
fn test_bucket_policy_boe_delete_object_requires_delete_object_policy() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket_request(client, &bucket)
            .object_ownership(ObjectOwnership::BucketOwnerEnforced)
            .send()
            .await
            .unwrap();

        client
            .put_object()
            .bucket(&bucket)
            .key("denied")
            .body(ByteStream::from_static(b"denied"))
            .send()
            .await
            .unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key("allowed")
            .body(ByteStream::from_static(b"allowed"))
            .send()
            .await
            .unwrap();

        eventually_access_denied(
            "BOE DeleteObject denied without DeleteObject policy",
            || {
                alt_client
                    .delete_object()
                    .bucket(&bucket)
                    .key("denied")
                    .send()
            },
        )
        .await;

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                principal,
                "Allow",
                "s3:DeleteObject",
                bucket_wildcard_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        eventually_ok_with_retry(
            "BOE DeleteObject allowed with DeleteObject policy",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .delete_object()
                    .bucket(&bucket)
                    .key("allowed")
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &["denied"]).await;
    });
}

#[test]
fn test_bucket_policy_boe_delete_object_version_requires_delete_object_version_policy() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket_request(client, &bucket)
            .object_ownership(ObjectOwnership::BucketOwnerEnforced)
            .send()
            .await
            .unwrap();
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

        let denied_version_id = client
            .put_object()
            .bucket(&bucket)
            .key("denied")
            .body(ByteStream::from_static(b"denied"))
            .send()
            .await
            .unwrap()
            .version_id()
            .expect("expected denied version id")
            .to_string();
        let allowed_version_id = client
            .put_object()
            .bucket(&bucket)
            .key("allowed")
            .body(ByteStream::from_static(b"allowed"))
            .send()
            .await
            .unwrap()
            .version_id()
            .expect("expected allowed version id")
            .to_string();

        eventually_access_denied(
            "BOE DeleteObjectVersion denied without DeleteObjectVersion policy",
            || {
                alt_client
                    .delete_object()
                    .bucket(&bucket)
                    .key("denied")
                    .version_id(&denied_version_id)
                    .send()
            },
        )
        .await;

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                principal,
                "Allow",
                "s3:DeleteObjectVersion",
                bucket_wildcard_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        eventually_ok_with_retry(
            "BOE DeleteObjectVersion allowed with DeleteObjectVersion policy",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .delete_object()
                    .bucket(&bucket)
                    .key("allowed")
                    .version_id(&allowed_version_id)
                    .send()
            },
        )
        .await;

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_bucket_policy_multipart_upload_acl_condition_applies() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        set_object_writer_ownership(&bucket).await;
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:PutObject",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:x-amz-acl": "private"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let denied = alt_client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("denied")
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        let upload = eventually_ok("CreateMultipartUpload with x-amz-acl=private", || {
            alt_client
                .create_multipart_upload()
                .bucket(&bucket)
                .key("allowed")
                .acl(ObjectCannedAcl::Private)
                .send()
        })
        .await;
        let upload_id = upload.upload_id().unwrap().to_string();

        alt_client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("allowed")
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &["allowed", "denied"]).await;
    });
}

#[test]
fn test_bucket_policy_multipart_upload_grant_full_control_condition() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        set_object_writer_ownership(&bucket).await;
        let owner_id = canonical_owner_id(client, &bucket).await;
        let full_control_header = format!("id=\"{owner_id}\"");
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:PutObject",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:x-amz-grant-full-control": full_control_header.clone()
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let denied = alt_client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("denied")
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        let upload = eventually_ok("CreateMultipartUpload with grant-full-control", || {
            let full_control_header = full_control_header.clone();
            alt_client
                .create_multipart_upload()
                .bucket(&bucket)
                .key("allowed")
                .customize()
                .mutate_request(move |req| {
                    req.headers_mut()
                        .insert("x-amz-grant-full-control", full_control_header.clone());
                })
                .send()
        })
        .await;
        let upload_id = upload.upload_id().unwrap().to_string();

        alt_client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("allowed")
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &["allowed", "denied"]).await;
    });
}

#[test]
fn test_bucket_policy_multipart_upload_grant_read_condition() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        set_object_writer_ownership(&bucket).await;
        let owner_id = canonical_owner_id(client, &bucket).await;
        let grant_read_header = format!("id=\"{owner_id}\"");
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:PutObject",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:x-amz-grant-read": grant_read_header.clone()
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let denied = alt_client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("denied")
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        let upload = eventually_ok("CreateMultipartUpload with grant-read", || {
            let grant_read_header = grant_read_header.clone();
            alt_client
                .create_multipart_upload()
                .bucket(&bucket)
                .key("allowed")
                .customize()
                .mutate_request(move |req| {
                    req.headers_mut()
                        .insert("x-amz-grant-read", grant_read_header.clone());
                })
                .send()
        })
        .await;
        let upload_id = upload.upload_id().unwrap().to_string();

        alt_client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("allowed")
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &["allowed", "denied"]).await;
    });
}

#[test]
fn test_bucket_policy_multipart_upload_grant_read_acp_condition() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        set_object_writer_ownership(&bucket).await;
        let owner_id = canonical_owner_id(client, &bucket).await;
        let grant_read_acp_header = format!("id=\"{owner_id}\"");
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:PutObject",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:x-amz-grant-read-acp": grant_read_acp_header.clone()
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let denied = alt_client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("denied")
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        let upload = eventually_ok("CreateMultipartUpload with grant-read-acp", || {
            let grant_read_acp_header = grant_read_acp_header.clone();
            alt_client
                .create_multipart_upload()
                .bucket(&bucket)
                .key("allowed")
                .customize()
                .mutate_request(move |req| {
                    req.headers_mut()
                        .insert("x-amz-grant-read-acp", grant_read_acp_header.clone());
                })
                .send()
        })
        .await;
        let upload_id = upload.upload_id().unwrap().to_string();

        alt_client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("allowed")
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &["allowed", "denied"]).await;
    });
}

#[test]
fn test_bucket_policy_multipart_upload_grant_write() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        set_object_writer_ownership(&bucket).await;
        let owner_id = canonical_owner_id(client, &bucket).await;
        let grant_write_header = format!("id=\"{owner_id}\"");
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:PutObject",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:x-amz-grant-write": grant_write_header.clone()
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let denied = alt_client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("denied")
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        let upload = eventually_ok("CreateMultipartUpload with grant-write", || {
            let grant_write_header = grant_write_header.clone();
            alt_client
                .create_multipart_upload()
                .bucket(&bucket)
                .key("allowed")
                .customize()
                .mutate_request(move |req| {
                    req.headers_mut()
                        .insert("x-amz-grant-write", grant_write_header.clone());
                })
                .send()
        })
        .await;
        let upload_id = upload.upload_id().unwrap().to_string();

        alt_client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("allowed")
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &["allowed", "denied"]).await;
    });
}

#[test]
fn test_bucket_policy_multipart_upload_grant_write_acp_condition() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        set_object_writer_ownership(&bucket).await;
        let owner_id = canonical_owner_id(client, &bucket).await;
        let grant_write_acp_header = format!("id=\"{owner_id}\"");
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:PutObject",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:x-amz-grant-write-acp": grant_write_acp_header.clone()
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let denied = alt_client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("denied")
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        let upload = eventually_ok("CreateMultipartUpload with grant-write-acp", || {
            let grant_write_acp_header = grant_write_acp_header.clone();
            alt_client
                .create_multipart_upload()
                .bucket(&bucket)
                .key("allowed")
                .customize()
                .mutate_request(move |req| {
                    req.headers_mut()
                        .insert("x-amz-grant-write-acp", grant_write_acp_header.clone());
                })
                .send()
        })
        .await;
        let upload_id = upload.upload_id().unwrap().to_string();

        alt_client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("allowed")
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &["allowed", "denied"]).await;
    });
}

#[test]
fn test_bucket_policy_abort_multipart_upload_initiator_only_requires_put_object() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_bucket_allowing_public_policy(client).await;
        let key = "cross-account-abort";
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                principal.clone(),
                "Allow",
                "s3:PutObject",
                bucket_wildcard_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        let upload = eventually_ok_with_retry(
            "CreateMultipartUpload allowed with PutObject only",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .create_multipart_upload()
                    .bucket(&bucket)
                    .key(key)
                    .send()
            },
        )
        .await;
        let upload_id = upload.upload_id().unwrap().to_string();

        alt_client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_list_parts_initiator_only_requires_put_object() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_bucket_allowing_public_policy(client).await;
        let key = "cross-account-list-parts";
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                principal.clone(),
                "Allow",
                "s3:PutObject",
                bucket_wildcard_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        let upload = eventually_ok_with_retry(
            "CreateMultipartUpload allowed with PutObject only",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .create_multipart_upload()
                    .bucket(&bucket)
                    .key(key)
                    .send()
            },
        )
        .await;
        let upload_id = upload.upload_id().unwrap().to_string();

        alt_client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![b'x'; 1024]))
            .send()
            .await
            .unwrap();

        let listed = alt_client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();
        assert_eq!(listed.parts().len(), 1);
        assert_eq!(listed.parts()[0].part_number(), Some(1));

        alt_client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_upload_part_and_complete_allow_same_account_non_initiator_with_put_object() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let second_client = CTX.require_second_client();
        let same_account_principal = same_account_exact_principal().await;

        let bucket = create_bucket_allowing_public_policy(client).await;
        let key = "same-account-non-initiator-write";
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(multipart_put_object_policy_for_alt_and_same_account(
                &bucket,
                &same_account_principal,
            ))
            .send()
            .await
            .unwrap();

        let upload = eventually_ok_with_retry(
            "CreateMultipartUpload with PutObject only",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .create_multipart_upload()
                    .bucket(&bucket)
                    .key(key)
                    .send()
            },
        )
        .await;
        let upload_id = upload.upload_id().unwrap().to_string();

        let uploaded = eventually_ok_with_retry(
            "UploadPart by same-account non-initiator with PutObject only",
            60,
            std::time::Duration::from_millis(500),
            || {
                second_client
                    .upload_part()
                    .bucket(&bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .part_number(1)
                    .body(ByteStream::from(vec![b'y'; 1024]))
                    .send()
            },
        )
        .await;
        let etag = uploaded.e_tag().expect("expected upload part etag");

        eventually_ok_with_retry(
            "CompleteMultipartUpload by same-account non-initiator with PutObject only",
            60,
            std::time::Duration::from_millis(500),
            || {
                second_client
                    .complete_multipart_upload()
                    .bucket(&bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .multipart_upload(
                        CompletedMultipartUpload::builder()
                            .parts(CompletedPart::builder().part_number(1).e_tag(etag).build())
                            .build(),
                    )
                    .send()
            },
        )
        .await;

        let object = get_object_eventually(client, &bucket, key).await;
        let body = object.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), vec![b'y'; 1024].as_slice());

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_boe_upload_part_and_complete_allow_same_account_non_initiator_with_put_object(
) {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let second_client = CTX.require_second_client();
        let same_account_principal = same_account_exact_principal().await;

        let bucket = unique_bucket();
        s3_tests::create_bucket_request(client, &bucket)
            .object_ownership(ObjectOwnership::BucketOwnerEnforced)
            .send()
            .await
            .unwrap();
        let key = "boe-same-account-non-initiator-write";
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(multipart_put_object_policy_for_alt_and_same_account(
                &bucket,
                &same_account_principal,
            ))
            .send()
            .await
            .unwrap();

        let upload = eventually_ok_with_retry(
            "BOE CreateMultipartUpload with PutObject only",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .create_multipart_upload()
                    .bucket(&bucket)
                    .key(key)
                    .send()
            },
        )
        .await;
        let upload_id = upload.upload_id().unwrap().to_string();

        let uploaded = eventually_ok_with_retry(
            "BOE UploadPart by same-account non-initiator with PutObject only",
            60,
            std::time::Duration::from_millis(500),
            || {
                second_client
                    .upload_part()
                    .bucket(&bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .part_number(1)
                    .body(ByteStream::from(vec![b'y'; 1024]))
                    .send()
            },
        )
        .await;
        let etag = uploaded.e_tag().expect("expected upload part etag");

        eventually_ok_with_retry(
            "BOE CompleteMultipartUpload by same-account non-initiator with PutObject only",
            60,
            std::time::Duration::from_millis(500),
            || {
                second_client
                    .complete_multipart_upload()
                    .bucket(&bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .multipart_upload(
                        CompletedMultipartUpload::builder()
                            .parts(CompletedPart::builder().part_number(1).e_tag(etag).build())
                            .build(),
                    )
                    .send()
            },
        )
        .await;

        let object = get_object_eventually(client, &bucket, key).await;
        let body = object.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), vec![b'y'; 1024].as_slice());

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_management_paths_deny_same_account_non_initiator_with_put_object() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let second_client = CTX.require_second_client();
        let same_account_principal = same_account_exact_principal().await;

        let bucket = create_bucket_allowing_public_policy(client).await;
        let key = "same-account-non-initiator-manage";
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(multipart_put_object_policy_for_alt_and_same_account(
                &bucket,
                &same_account_principal,
            ))
            .send()
            .await
            .unwrap();

        let upload = eventually_ok_with_retry(
            "CreateMultipartUpload with PutObject only",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .create_multipart_upload()
                    .bucket(&bucket)
                    .key(key)
                    .send()
            },
        )
        .await;
        let upload_id = upload.upload_id().unwrap().to_string();

        eventually_ok_with_retry(
            "UploadPart by initiator for management split setup",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .upload_part()
                    .bucket(&bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .part_number(1)
                    .body(ByteStream::from(vec![b'z'; 1024]))
                    .send()
            },
        )
        .await;

        eventually_access_denied(
            "ListParts by same-account non-initiator with PutObject only",
            || {
                second_client
                    .list_parts()
                    .bucket(&bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .send()
            },
        )
        .await;

        eventually_access_denied(
            "AbortMultipartUpload by same-account non-initiator with PutObject only",
            || {
                second_client
                    .abort_multipart_upload()
                    .bucket(&bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .send()
            },
        )
        .await;

        alt_client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_management_paths_deny_same_account_non_initiator_missing_upload_with_put_object(
) {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let second_client = CTX.require_second_client();
        let same_account_principal = same_account_exact_principal().await;

        let bucket = create_bucket_allowing_public_policy(client).await;
        let key = "same-account-non-initiator-missing-upload";
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(multipart_put_object_policy_for_alt_and_same_account(
                &bucket,
                &same_account_principal,
            ))
            .send()
            .await
            .unwrap();

        let upload = eventually_ok_with_retry(
            "CreateMultipartUpload with PutObject only",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .create_multipart_upload()
                    .bucket(&bucket)
                    .key(key)
                    .send()
            },
        )
        .await;
        let upload_id = upload.upload_id().unwrap().to_string();
        let mut mutated_upload_id = upload_id.clone().into_bytes();
        let last = mutated_upload_id
            .last_mut()
            .expect("AWS upload IDs are non-empty");
        *last = if *last == b'A' { b'B' } else { b'A' };
        let mutated_upload_id = String::from_utf8(mutated_upload_id).unwrap();

        alt_client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();

        eventually_result_matches(
            "ListParts by upload initiator should observe the aborted upload as missing",
            20,
            std::time::Duration::from_millis(200),
            || {
                alt_client
                    .list_parts()
                    .bucket(&bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .send()
            },
            |result| {
                result
                    .as_ref()
                    .err()
                    .and_then(|err| err.raw_response().map(|r| r.status().as_u16()))
                    == Some(404)
            },
        )
        .await;

        let list_denied = second_client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await;
        match err_status(&list_denied) {
            403 => assert_s3_err_code(&list_denied, "AccessDenied"),
            404 => assert_s3_err_code(&list_denied, "NoSuchUpload"),
            status => panic!("unexpected ListParts status for terminal hidden upload: {status}"),
        }

        let abort_denied = second_client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await;
        match err_status(&abort_denied) {
            403 => assert_s3_err_code(&abort_denied, "AccessDenied"),
            404 => assert_s3_err_code(&abort_denied, "NoSuchUpload"),
            status => {
                panic!(
                    "unexpected AbortMultipartUpload status for terminal hidden upload: {status}"
                )
            }
        }

        let wrong_key_list_missing = second_client
            .list_parts()
            .bucket(&bucket)
            .key("same-account-non-initiator-missing-upload-wrong-key")
            .upload_id(&upload_id)
            .send()
            .await;
        assert_eq!(err_status(&wrong_key_list_missing), 404);
        assert_s3_err_code(&wrong_key_list_missing, "NoSuchUpload");

        let mutated_list_missing = second_client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(&mutated_upload_id)
            .send()
            .await;
        assert_eq!(err_status(&mutated_list_missing), 404);
        assert_s3_err_code(&mutated_list_missing, "NoSuchUpload");

        let arbitrary_list_missing = second_client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id("missing-upload-id")
            .send()
            .await;
        assert_eq!(err_status(&arbitrary_list_missing), 404);
        assert_s3_err_code(&arbitrary_list_missing, "NoSuchUpload");

        let wrong_key_abort_missing = second_client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("same-account-non-initiator-missing-upload-wrong-key")
            .upload_id(&upload_id)
            .send()
            .await;
        assert_eq!(err_status(&wrong_key_abort_missing), 404);
        assert_s3_err_code(&wrong_key_abort_missing, "NoSuchUpload");

        let mutated_abort_missing = second_client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&mutated_upload_id)
            .send()
            .await;
        assert_eq!(err_status(&mutated_abort_missing), 404);
        assert_s3_err_code(&mutated_abort_missing, "NoSuchUpload");

        let arbitrary_abort_missing = second_client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id("missing-upload-id")
            .send()
            .await;
        assert_eq!(err_status(&arbitrary_abort_missing), 404);
        assert_s3_err_code(&arbitrary_abort_missing, "NoSuchUpload");

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_boe_management_paths_deny_same_account_non_initiator_with_put_object() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let second_client = CTX.require_second_client();
        let same_account_principal = same_account_exact_principal().await;

        let bucket = unique_bucket();
        s3_tests::create_bucket_request(client, &bucket)
            .object_ownership(ObjectOwnership::BucketOwnerEnforced)
            .send()
            .await
            .unwrap();
        let key = "boe-same-account-non-initiator-manage";
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(multipart_put_object_policy_for_alt_and_same_account(
                &bucket,
                &same_account_principal,
            ))
            .send()
            .await
            .unwrap();

        let upload = eventually_ok_with_retry(
            "BOE CreateMultipartUpload with PutObject only",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .create_multipart_upload()
                    .bucket(&bucket)
                    .key(key)
                    .send()
            },
        )
        .await;
        let upload_id = upload.upload_id().unwrap().to_string();

        eventually_ok_with_retry(
            "BOE UploadPart by initiator for management split setup",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .upload_part()
                    .bucket(&bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .part_number(1)
                    .body(ByteStream::from(vec![b'z'; 1024]))
                    .send()
            },
        )
        .await;

        eventually_access_denied(
            "BOE ListParts by same-account non-initiator with PutObject only",
            || {
                second_client
                    .list_parts()
                    .bucket(&bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .send()
            },
        )
        .await;

        eventually_access_denied(
            "BOE AbortMultipartUpload by same-account non-initiator with PutObject only",
            || {
                second_client
                    .abort_multipart_upload()
                    .bucket(&bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .send()
            },
        )
        .await;

        alt_client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_completed_abort_denies_same_account_non_initiator_with_put_object() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let second_client = CTX.require_second_client();
        let same_account_principal = same_account_exact_principal().await;

        let bucket = create_bucket_allowing_public_policy(client).await;
        let key = "same-account-non-initiator-completed-abort";
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(multipart_put_object_policy_for_alt_and_same_account(
                &bucket,
                &same_account_principal,
            ))
            .send()
            .await
            .unwrap();

        let upload = eventually_ok_with_retry(
            "CreateMultipartUpload with PutObject only",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .create_multipart_upload()
                    .bucket(&bucket)
                    .key(key)
                    .send()
            },
        )
        .await;
        let upload_id = upload.upload_id().unwrap().to_string();

        let uploaded = eventually_ok_with_retry(
            "UploadPart by initiator for completed abort setup",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .upload_part()
                    .bucket(&bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .part_number(1)
                    .body(ByteStream::from(vec![b'q'; 1024]))
                    .send()
            },
        )
        .await;
        let etag = uploaded.e_tag().expect("expected upload part etag");

        eventually_ok_with_retry(
            "CompleteMultipartUpload by initiator for completed abort setup",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .complete_multipart_upload()
                    .bucket(&bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .multipart_upload(
                        CompletedMultipartUpload::builder()
                            .parts(CompletedPart::builder().part_number(1).e_tag(etag).build())
                            .build(),
                    )
                    .send()
            },
        )
        .await;

        eventually_access_denied(
            "AbortMultipartUpload on completed upload by same-account non-initiator with PutObject only",
            || {
                second_client
                    .abort_multipart_upload()
                    .bucket(&bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .send()
            },
        )
        .await;

        let object = get_object_eventually(client, &bucket, key).await;
        let body = object.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), vec![b'q'; 1024].as_slice());

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_boe_abort_multipart_upload_initiator_only_requires_put_object() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket_request(client, &bucket)
            .object_ownership(ObjectOwnership::BucketOwnerEnforced)
            .send()
            .await
            .unwrap();
        let key = "boe-cross-account-abort";
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                principal.clone(),
                "Allow",
                "s3:PutObject",
                bucket_wildcard_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        let upload = eventually_ok_with_retry(
            "BOE CreateMultipartUpload allowed with PutObject only",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .create_multipart_upload()
                    .bucket(&bucket)
                    .key(key)
                    .send()
            },
        )
        .await;
        let upload_id = upload.upload_id().unwrap().to_string();

        alt_client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_upload_part_copy_copy_source() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let src_bucket = unique_bucket();
        let dst_bucket = unique_bucket();
        s3_tests::create_bucket(client, &src_bucket).await.unwrap();
        s3_tests::create_bucket(alt_client, &dst_bucket)
            .await
            .unwrap();

        for (key, body) in [
            ("public/foo", b"public/foo".as_slice()),
            ("public/bar", b"public/bar".as_slice()),
            ("private/foo", b"private/foo".as_slice()),
        ] {
            s3_tests::put_object_retrying_operation_aborted(
                client,
                &src_bucket,
                key,
                body.to_vec(),
            )
            .await;
        }

        let src_policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:GetObject",
                "Resource": format!("arn:aws:s3:::{src_bucket}/public/*"),
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&src_bucket)
            .policy(src_policy)
            .send_retrying_operation_aborted("put source bucket policy for upload part copy")
            .await
            .unwrap();
        client
            .get_bucket_policy()
            .bucket(&src_bucket)
            .send_retrying_operation_aborted("get source bucket policy for upload part copy")
            .await
            .unwrap();
        let source_probe = get_object_eventually(alt_client, &src_bucket, "public/foo").await;
        let source_probe_body = source_probe.body.collect().await.unwrap().into_bytes();
        assert_eq!(source_probe_body.as_ref(), b"public/foo");

        let upload = alt_client
            .create_multipart_upload()
            .bucket(&dst_bucket)
            .key("copied")
            .send_retrying_operation_aborted(
                "create destination multipart upload for upload part copy",
            )
            .await
            .unwrap();
        let upload_id = upload.upload_id().unwrap().to_string();

        let copied_part = upload_part_copy_eventually(
            alt_client,
            &dst_bucket,
            "copied",
            &upload_id,
            1,
            format!("{src_bucket}/public/foo"),
        )
        .await;
        complete_single_part_upload(
            alt_client,
            &dst_bucket,
            "copied",
            &upload_id,
            copied_part.copy_part_result().unwrap().e_tag().unwrap(),
        )
        .await;

        let second_upload = alt_client
            .create_multipart_upload()
            .bucket(&dst_bucket)
            .key("copied2")
            .send_retrying_operation_aborted(
                "create second destination multipart upload for upload part copy",
            )
            .await
            .unwrap();
        let second_upload_id = second_upload.upload_id().unwrap().to_string();
        let second_part = upload_part_copy_eventually(
            alt_client,
            &dst_bucket,
            "copied2",
            &second_upload_id,
            1,
            format!("{src_bucket}/public/bar"),
        )
        .await;
        complete_single_part_upload(
            alt_client,
            &dst_bucket,
            "copied2",
            &second_upload_id,
            second_part.copy_part_result().unwrap().e_tag().unwrap(),
        )
        .await;

        let denied_upload = alt_client
            .create_multipart_upload()
            .bucket(&dst_bucket)
            .key("copied-denied")
            .send_retrying_operation_aborted(
                "create denied destination multipart upload for upload part copy",
            )
            .await
            .unwrap();
        let denied_upload_id = denied_upload.upload_id().unwrap().to_string();
        let denied = alt_client
            .upload_part_copy()
            .bucket(&dst_bucket)
            .key("copied-denied")
            .upload_id(&denied_upload_id)
            .part_number(1)
            .copy_source(format!("{src_bucket}/private/foo"))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        let body = s3_tests::get_object_body_retrying_operation_aborted(
            alt_client,
            &dst_bucket,
            "copied",
            None,
            "get copied object in upload part copy test",
        )
        .await;
        assert_eq!(body.as_slice(), b"public/foo");

        let body = s3_tests::get_object_body_retrying_operation_aborted(
            alt_client,
            &dst_bucket,
            "copied2",
            None,
            "get second copied object in upload part copy test",
        )
        .await;
        assert_eq!(body.as_slice(), b"public/bar");

        cleanup_with_client(
            alt_client,
            &dst_bucket,
            &["copied", "copied2", "copied-denied"],
        )
        .await;
        cleanup(&src_bucket, &["public/foo", "public/bar", "private/foo"]).await;
    });
}

#[test]
fn test_bucket_policy_upload_part_copy_source_existing_tag_condition() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let src_bucket = unique_bucket();
        let dst_bucket = unique_bucket();
        let public_key = "public/foo";
        let private_key = "private/foo";
        s3_tests::create_bucket(client, &src_bucket).await.unwrap();
        s3_tests::create_bucket(alt_client, &dst_bucket)
            .await
            .unwrap();

        for key in [public_key, private_key] {
            s3_tests::put_object_retrying_operation_aborted(
                client,
                &src_bucket,
                key,
                b"copy-source".to_vec(),
            )
            .await;
        }

        client
            .put_object_tagging()
            .bucket(&src_bucket)
            .key(public_key)
            .tagging(simple_bucket_tagging("security", "public"))
            .send_retrying_operation_aborted(
                "put public source tag for upload part copy bucket policy test",
            )
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&src_bucket)
            .key(private_key)
            .tagging(simple_bucket_tagging("security", "private"))
            .send_retrying_operation_aborted(
                "put private source tag for upload part copy bucket policy test",
            )
            .await
            .unwrap();

        let src_policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:GetObject",
                "Resource": bucket_wildcard_resource(&src_bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:ExistingObjectTag/security": "public"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&src_bucket)
            .policy(src_policy)
            .send_retrying_operation_aborted("put source tag bucket policy for upload part copy")
            .await
            .unwrap();

        let source_read = eventually_ok_with_retry(
            "GetObject source read with ExistingObjectTag-conditioned bucket policy",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .get_object()
                    .bucket(&src_bucket)
                    .key(public_key)
                    .send()
            },
        )
        .await;
        assert_eq!(
            source_read
                .body
                .collect()
                .await
                .unwrap()
                .into_bytes()
                .as_ref(),
            b"copy-source"
        );

        let upload = alt_client
            .create_multipart_upload()
            .bucket(&dst_bucket)
            .key("copied")
            .send_retrying_operation_aborted("create tagged-source destination multipart upload")
            .await
            .unwrap();
        let upload_id = upload.upload_id().unwrap().to_string();

        let copied_part = upload_part_copy_eventually(
            alt_client,
            &dst_bucket,
            "copied",
            &upload_id,
            1,
            format!("{src_bucket}/{public_key}"),
        )
        .await;
        complete_single_part_upload(
            alt_client,
            &dst_bucket,
            "copied",
            &upload_id,
            copied_part.copy_part_result().unwrap().e_tag().unwrap(),
        )
        .await;

        let denied_upload = alt_client
            .create_multipart_upload()
            .bucket(&dst_bucket)
            .key("copied-denied")
            .send_retrying_operation_aborted(
                "create denied tagged-source destination multipart upload",
            )
            .await
            .unwrap();
        let denied_upload_id = denied_upload.upload_id().unwrap().to_string();

        let denied = alt_client
            .upload_part_copy()
            .bucket(&dst_bucket)
            .key("copied-denied")
            .upload_id(&denied_upload_id)
            .part_number(1)
            .copy_source(format!("{src_bucket}/{private_key}"))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        let body = s3_tests::get_object_body_retrying_operation_aborted(
            alt_client,
            &dst_bucket,
            "copied",
            None,
            "get tagged-source copied object",
        )
        .await;
        assert_eq!(body.as_slice(), b"copy-source");

        cleanup_with_client(alt_client, &dst_bucket, &["copied", "copied-denied"]).await;
        cleanup(&src_bucket, &[public_key, private_key]).await;
    });
}

#[test]
fn test_bucket_policy_upload_part_copy_destination_copy_source_condition() {
    s3_tests::run(async {
        let principal = same_account_exact_principal().await;
        let client = CTX.client();
        let second_client = CTX.require_second_client();

        let src_bucket = unique_bucket();
        let dst_bucket = unique_bucket();
        s3_tests::create_bucket(client, &src_bucket).await.unwrap();
        s3_tests::create_bucket(client, &dst_bucket).await.unwrap();

        for (key, body) in [
            ("public/foo", b"public/foo".as_slice()),
            ("private/foo", b"private/foo".as_slice()),
        ] {
            s3_tests::put_object_retrying_operation_aborted(
                client,
                &src_bucket,
                key,
                body.to_vec(),
            )
            .await;
        }

        let src_policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": { "AWS": principal.clone() },
                "Action": "s3:GetObject",
                "Resource": bucket_wildcard_resource(&src_bucket),
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&src_bucket)
            .policy(src_policy)
            .send_retrying_operation_aborted(
                "put source copy-source bucket policy for upload part copy",
            )
            .await
            .unwrap();

        let dst_policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": { "AWS": principal.clone() },
                    "Action": "s3:PutObject",
                    "Resource": bucket_wildcard_resource(&dst_bucket),
                },
                {
                    "Effect": "Deny",
                    "Principal": { "AWS": principal },
                    "Action": "s3:PutObject",
                    "Resource": bucket_wildcard_resource(&dst_bucket),
                    "Condition": {
                        "StringNotLike": {
                            "s3:x-amz-copy-source": format!("{src_bucket}/public/*")
                        }
                    }
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&dst_bucket)
            .policy(dst_policy)
            .send_retrying_operation_aborted(
                "put destination copy-source bucket policy for upload part copy",
            )
            .await
            .unwrap();

        let allowed_upload = eventually_ok(
            "CreateMultipartUpload by owner for UploadPartCopy policy probe",
            || {
                client
                    .create_multipart_upload()
                    .bucket(&dst_bucket)
                    .key("copied")
                    .send_retrying_operation_aborted(
                        "create copy-source-conditioned destination multipart upload",
                    )
            },
        )
        .await;
        let allowed_upload_id = allowed_upload.upload_id().unwrap().to_string();

        let copied_part = upload_part_copy_eventually(
            second_client,
            &dst_bucket,
            "copied",
            &allowed_upload_id,
            1,
            format!("{src_bucket}/public/foo"),
        )
        .await;
        complete_single_part_upload(
            client,
            &dst_bucket,
            "copied",
            &allowed_upload_id,
            copied_part.copy_part_result().unwrap().e_tag().unwrap(),
        )
        .await;

        let denied_upload = eventually_ok(
            "CreateMultipartUpload by owner for denied UploadPartCopy probe",
            || {
                client
                    .create_multipart_upload()
                    .bucket(&dst_bucket)
                    .key("copied-denied")
                    .send_retrying_operation_aborted(
                        "create denied copy-source-conditioned multipart upload",
                    )
            },
        )
        .await;
        let denied_upload_id = denied_upload.upload_id().unwrap().to_string();

        let denied = second_client
            .upload_part_copy()
            .bucket(&dst_bucket)
            .key("copied-denied")
            .upload_id(&denied_upload_id)
            .part_number(1)
            .copy_source(format!("{src_bucket}/private/foo"))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        let body = s3_tests::get_object_body_retrying_operation_aborted(
            client,
            &dst_bucket,
            "copied",
            None,
            "get copy-source-conditioned copied object",
        )
        .await;
        assert_eq!(body.as_slice(), b"public/foo");

        cleanup_with_client(client, &dst_bucket, &["copied", "copied-denied"]).await;
        cleanup(&src_bucket, &["public/foo", "private/foo"]).await;
    });
}

#[test]
fn test_bucket_policy_copy_source_condition_uses_leading_slash_encoded_header_value() {
    s3_tests::run(async {
        let client = CTX.client();
        let src_bucket = unique_bucket();
        let dst_bucket = unique_bucket();
        s3_tests::create_bucket(client, &src_bucket).await.unwrap();
        s3_tests::create_bucket(client, &dst_bucket).await.unwrap();

        let src_key = "public/space key+plus#hash";
        let encoded_src_key = "public/space%20key%2Bplus%23hash";
        let dst_key = "copied";
        let copy_source = format!("/{src_bucket}/{encoded_src_key}");

        s3_tests::put_object_retrying_operation_aborted(
            client,
            &src_bucket,
            src_key,
            b"copy-source-normalization".to_vec(),
        )
        .await;

        client
            .put_bucket_policy()
            .bucket(&dst_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Deny",
                        "Principal": "*",
                        "Action": "s3:PutObject",
                        "Resource": bucket_wildcard_resource(&dst_bucket),
                        "Condition": {
                            "StringNotEquals": {
                                "s3:x-amz-copy-source": copy_source
                            }
                        }
                    }]
                })
                .to_string(),
            )
            .send_retrying_operation_aborted("put copy-source normalization bucket policy")
            .await
            .unwrap();

        let dst_url = object_url(CTX.endpoint(), &dst_bucket, dst_key, None);
        let response = send_signed_request(
            "PUT",
            &dst_url,
            b"",
            [("x-amz-copy-source", copy_source.as_str())],
        );
        assert_eq!(
            response.status, 200,
            "expected copy with leading-slash encoded copy-source to match policy, got {} body={}",
            response.status, response.body
        );

        let copied = s3_tests::get_object_body_retrying_operation_aborted(
            client,
            &dst_bucket,
            dst_key,
            None,
            "get copied object after copy-source normalization policy",
        )
        .await;
        assert_eq!(copied.as_slice(), b"copy-source-normalization");

        cleanup_with_client(client, &dst_bucket, &[dst_key]).await;
        cleanup_with_client(client, &src_bucket, &[src_key]).await;
    });
}

#[test]
fn test_bucket_policy_copy_source_condition_percent_encoded_unreserved_bypasses_canonical_deny() {
    s3_tests::run(async {
        let client = CTX.client();
        let src_bucket = unique_bucket();
        let dst_bucket = unique_bucket();
        s3_tests::create_bucket(client, &src_bucket).await.unwrap();
        s3_tests::create_bucket(client, &dst_bucket).await.unwrap();

        s3_tests::put_object_retrying_operation_aborted(
            client,
            &src_bucket,
            "private/foo",
            b"private/foo".to_vec(),
        )
        .await;

        client
            .put_bucket_policy()
            .bucket(&dst_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Deny",
                        "Principal": "*",
                        "Action": "s3:PutObject",
                        "Resource": bucket_wildcard_resource(&dst_bucket),
                        "Condition": {
                            "StringLike": {
                                "s3:x-amz-copy-source": format!("{src_bucket}/private/*")
                            }
                        }
                    }]
                })
                .to_string(),
            )
            .send_retrying_operation_aborted("put canonical copy-source deny bucket policy")
            .await
            .unwrap();

        let canonical_deny_url = object_url(CTX.endpoint(), &dst_bucket, "canonical-denied", None);
        let canonical_copy_source = format!("{src_bucket}/private/foo");
        eventually_result_matches(
            "CopyObject denied with canonical copy-source header",
            20,
            std::time::Duration::from_millis(200),
            || {
                let canonical_deny_url = canonical_deny_url.clone();
                let canonical_copy_source = canonical_copy_source.clone();
                async move {
                    Ok::<_, std::convert::Infallible>(send_signed_request(
                        "PUT",
                        &canonical_deny_url,
                        b"",
                        [("x-amz-copy-source", canonical_copy_source.as_str())],
                    ))
                }
            },
            |result| result.as_ref().is_ok_and(|response| response.status == 403),
        )
        .await;

        let dst_url = object_url(CTX.endpoint(), &dst_bucket, "copied", None);
        let copy_source = format!("{src_bucket}/%70rivate/foo");
        let response = send_signed_request(
            "PUT",
            &dst_url,
            b"",
            [("x-amz-copy-source", copy_source.as_str())],
        );
        assert_eq!(
            response.status, 200,
            "expected encoded copy-source spelling to bypass canonical deny, got {} body={}",
            response.status, response.body
        );

        let copied = s3_tests::get_object_body_retrying_operation_aborted(
            client,
            &dst_bucket,
            "copied",
            None,
            "get copied object after encoded copy-source deny bypass",
        )
        .await;
        assert_eq!(copied.as_slice(), b"private/foo");

        cleanup_with_client(client, &dst_bucket, &["copied", "canonical-denied"]).await;
        cleanup_with_client(client, &src_bucket, &["private/foo"]).await;
    });
}

#[test]
fn test_bucket_policy_copy_source_percent_encoded_versionid_bypasses_canonical_deny() {
    s3_tests::run(async {
        let client = CTX.client();
        let src_bucket = unique_bucket();
        let dst_bucket = unique_bucket();
        s3_tests::create_bucket(client, &src_bucket).await.unwrap();
        s3_tests::create_bucket(client, &dst_bucket).await.unwrap();
        s3_tests::enable_bucket_versioning(client, &src_bucket).await;

        let src_key = "versioned/source";
        let first_put = s3_tests::put_object_retrying_operation_aborted(
            client,
            &src_bucket,
            src_key,
            b"denied-version".to_vec(),
        )
        .await;
        let first_version_id = first_put
            .version_id()
            .expect("versioned put should return version id")
            .to_string();
        assert_ne!(first_version_id, "null");

        s3_tests::put_object_retrying_operation_aborted(
            client,
            &src_bucket,
            src_key,
            b"latest-version".to_vec(),
        )
        .await;

        let canonical_copy_source = format!("{src_bucket}/{src_key}?versionId={first_version_id}");
        client
            .put_bucket_policy()
            .bucket(&dst_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Deny",
                        "Principal": "*",
                        "Action": "s3:PutObject",
                        "Resource": bucket_wildcard_resource(&dst_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:x-amz-copy-source": canonical_copy_source
                            }
                        }
                    }]
                })
                .to_string(),
            )
            .send_retrying_operation_aborted("put versionId copy-source deny bucket policy")
            .await
            .unwrap();

        let canonical_dst_url = object_url(
            CTX.endpoint(),
            &dst_bucket,
            "canonical-version-denied",
            None,
        );
        eventually_result_matches(
            "CopyObject denied with canonical versionId copy-source header",
            20,
            std::time::Duration::from_millis(200),
            || {
                let canonical_dst_url = canonical_dst_url.clone();
                let canonical_copy_source = canonical_copy_source.clone();
                async move {
                    Ok::<_, std::convert::Infallible>(send_signed_request(
                        "PUT",
                        &canonical_dst_url,
                        b"",
                        [("x-amz-copy-source", canonical_copy_source.as_str())],
                    ))
                }
            },
            |result| result.as_ref().is_ok_and(|response| response.status == 403),
        )
        .await;

        let encoded_dst_key = "encoded-version-copied";
        let encoded_dst_url = object_url(CTX.endpoint(), &dst_bucket, encoded_dst_key, None);
        let encoded_copy_source = format!(
            "{src_bucket}/{src_key}?versionId={}",
            percent_encode_first_byte(&first_version_id)
        );
        let response = send_signed_request(
            "PUT",
            &encoded_dst_url,
            b"",
            [("x-amz-copy-source", encoded_copy_source.as_str())],
        );
        assert_eq!(
            response.status, 200,
            "expected encoded versionId copy-source spelling to bypass canonical deny, got {} body={}",
            response.status, response.body
        );

        let copied = s3_tests::get_object_body_retrying_operation_aborted(
            client,
            &dst_bucket,
            encoded_dst_key,
            None,
            "get copied object after encoded versionId copy-source deny bypass",
        )
        .await;
        assert_eq!(copied.as_slice(), b"denied-version");

        cleanup_with_client(
            client,
            &dst_bucket,
            &[encoded_dst_key, "canonical-version-denied"],
        )
        .await;
        cleanup_versioned_bucket(client, &src_bucket).await;
    });
}

#[test]
fn test_bucket_policy_upload_part_copy_percent_encoded_versionid_bypasses_canonical_deny() {
    s3_tests::run(async {
        let client = CTX.client();
        let src_bucket = unique_bucket();
        let dst_bucket = unique_bucket();
        s3_tests::create_bucket(client, &src_bucket).await.unwrap();
        s3_tests::create_bucket(client, &dst_bucket).await.unwrap();
        s3_tests::enable_bucket_versioning(client, &src_bucket).await;

        let src_key = "versioned/source";
        let first_put = s3_tests::put_object_retrying_operation_aborted(
            client,
            &src_bucket,
            src_key,
            b"denied-version".to_vec(),
        )
        .await;
        let first_version_id = first_put
            .version_id()
            .expect("versioned put should return version id")
            .to_string();
        assert_ne!(first_version_id, "null");

        s3_tests::put_object_retrying_operation_aborted(
            client,
            &src_bucket,
            src_key,
            b"latest-version".to_vec(),
        )
        .await;

        let canonical_copy_source = format!("{src_bucket}/{src_key}?versionId={first_version_id}");
        client
            .put_bucket_policy()
            .bucket(&dst_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Deny",
                        "Principal": "*",
                        "Action": "s3:PutObject",
                        "Resource": bucket_wildcard_resource(&dst_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:x-amz-copy-source": canonical_copy_source
                            }
                        }
                    }]
                })
                .to_string(),
            )
            .send_retrying_operation_aborted(
                "put versionId upload-part-copy-source deny bucket policy",
            )
            .await
            .unwrap();

        let canonical_key = "canonical-version-denied";
        let canonical_upload = eventually_ok(
            "CreateMultipartUpload for canonical UploadPartCopy versionId denial",
            || {
                client
                    .create_multipart_upload()
                    .bucket(&dst_bucket)
                    .key(canonical_key)
                    .send_retrying_operation_aborted(
                        "create canonical-denied upload-part-copy destination upload",
                    )
            },
        )
        .await;
        let canonical_upload_id = canonical_upload.upload_id().unwrap().to_string();
        let canonical_url = object_url(
            CTX.endpoint(),
            &dst_bucket,
            canonical_key,
            Some(&format!("partNumber=1&uploadId={canonical_upload_id}")),
        );
        eventually_result_matches(
            "UploadPartCopy denied with canonical versionId copy-source header",
            20,
            std::time::Duration::from_millis(200),
            || {
                let canonical_url = canonical_url.clone();
                let canonical_copy_source = canonical_copy_source.clone();
                async move {
                    Ok::<_, std::convert::Infallible>(send_signed_request(
                        "PUT",
                        &canonical_url,
                        b"",
                        [("x-amz-copy-source", canonical_copy_source.as_str())],
                    ))
                }
            },
            |result| result.as_ref().is_ok_and(|response| response.status == 403),
        )
        .await;

        let encoded_key = "encoded-version-copied";
        let encoded_upload = eventually_ok(
            "CreateMultipartUpload for encoded UploadPartCopy versionId copy",
            || {
                client
                    .create_multipart_upload()
                    .bucket(&dst_bucket)
                    .key(encoded_key)
                    .send_retrying_operation_aborted(
                        "create encoded upload-part-copy destination upload",
                    )
            },
        )
        .await;
        let encoded_upload_id = encoded_upload.upload_id().unwrap().to_string();
        let encoded_url = object_url(
            CTX.endpoint(),
            &dst_bucket,
            encoded_key,
            Some(&format!("partNumber=1&uploadId={encoded_upload_id}")),
        );
        let encoded_copy_source = format!(
            "{src_bucket}/{src_key}?versionId={}",
            percent_encode_first_byte(&first_version_id)
        );
        let response = send_signed_request(
            "PUT",
            &encoded_url,
            b"",
            [("x-amz-copy-source", encoded_copy_source.as_str())],
        );
        assert_eq!(
            response.status, 200,
            "expected encoded versionId UploadPartCopy spelling to bypass canonical deny, got {} body={}",
            response.status, response.body
        );
        let etag = copy_part_etag_from_body(&response.body).to_string();
        complete_single_part_upload(client, &dst_bucket, encoded_key, &encoded_upload_id, &etag)
            .await;

        let copied = s3_tests::get_object_body_retrying_operation_aborted(
            client,
            &dst_bucket,
            encoded_key,
            None,
            "get completed object after encoded versionId upload-part-copy deny bypass",
        )
        .await;
        assert_eq!(copied.as_slice(), b"denied-version");

        cleanup_with_client(client, &dst_bucket, &[encoded_key, canonical_key]).await;
        cleanup_versioned_bucket(client, &src_bucket).await;
    });
}

#[test]
fn test_bucket_policy_upload_part_copy_destination_metadata_directive_condition() {
    s3_tests::run(async {
        let principal = same_account_exact_principal().await;
        let client = CTX.client();
        let second_client = CTX.require_second_client();

        let src_bucket = unique_bucket();
        let dst_bucket = unique_bucket();
        s3_tests::create_bucket(client, &src_bucket).await.unwrap();
        s3_tests::create_bucket(client, &dst_bucket).await.unwrap();

        s3_tests::put_object_retrying_operation_aborted(
            client,
            &src_bucket,
            "src",
            b"copy-source".to_vec(),
        )
        .await;

        let src_policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": { "AWS": principal.clone() },
                "Action": "s3:GetObject",
                "Resource": bucket_wildcard_resource(&src_bucket),
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&src_bucket)
            .policy(src_policy)
            .send_retrying_operation_aborted(
                "put source metadata-directive bucket policy for upload part copy",
            )
            .await
            .unwrap();

        let dst_policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": { "AWS": principal.clone() },
                    "Action": "s3:PutObject",
                    "Resource": bucket_wildcard_resource(&dst_bucket),
                },
                {
                    "Effect": "Deny",
                    "Principal": { "AWS": principal },
                    "Action": "s3:PutObject",
                    "Resource": bucket_wildcard_resource(&dst_bucket),
                    "Condition": {
                        "StringNotEquals": {
                            "s3:x-amz-metadata-directive": "COPY"
                        }
                    }
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&dst_bucket)
            .policy(dst_policy)
            .send_retrying_operation_aborted(
                "put destination metadata-directive bucket policy for upload part copy",
            )
            .await
            .unwrap();

        let upload = client
            .create_multipart_upload()
            .bucket(&dst_bucket)
            .key("copied")
            .send_retrying_operation_aborted(
                "create metadata-directive destination multipart upload",
            )
            .await
            .unwrap();
        let upload_id = upload.upload_id().unwrap().to_string();

        let denied = second_client
            .upload_part_copy()
            .bucket(&dst_bucket)
            .key("copied")
            .upload_id(&upload_id)
            .part_number(1)
            .copy_source(format!("{src_bucket}/src"))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        cleanup_with_client(client, &dst_bucket, &["copied"]).await;
        cleanup(&src_bucket, &["src"]).await;
    });
}

/// Apply the same policy to two different buckets and verify both work.
///
/// Matches Ceph: test_bucket_policy_another_bucket
#[test]
fn test_bucket_policy_another_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let principal = alt_policy_principal();

        let bucket1 = unique_bucket();
        let bucket2 = unique_bucket();
        s3_tests::create_bucket(client, &bucket1).await.unwrap();
        s3_tests::create_bucket(client, &bucket2).await.unwrap();

        client
            .put_object()
            .bucket(&bucket1)
            .key("obj1")
            .body(ByteStream::from_static(b"data1"))
            .send()
            .await
            .unwrap();
        client
            .put_object()
            .bucket(&bucket2)
            .key("obj2")
            .body(ByteStream::from_static(b"data2"))
            .send()
            .await
            .unwrap();

        client
            .put_bucket_policy()
            .bucket(&bucket1)
            .policy(bucket_policy_document(
                principal.clone(),
                "Allow",
                "s3:ListBucket",
                bucket_resource(&bucket1),
            ))
            .send()
            .await
            .unwrap();
        client
            .put_bucket_policy()
            .bucket(&bucket2)
            .policy(bucket_policy_document(
                principal,
                "Allow",
                "s3:ListBucket",
                bucket_resource(&bucket2),
            ))
            .send()
            .await
            .unwrap();

        let resp1 = alt_list_objects_v1_eventually(&bucket1).await;
        assert_eq!(resp1.contents().len(), 1);
        assert_eq!(resp1.contents()[0].key(), Some("obj1"));

        let resp2 = alt_list_objects_v1_eventually(&bucket2).await;
        assert_eq!(resp2.contents().len(), 1);
        assert_eq!(resp2.contents()[0].key(), Some("obj2"));

        cleanup(&bucket1, &["obj1"]).await;
        cleanup(&bucket2, &["obj2"]).await;
    });
}

// StringLikeIfExists condition operator: condition is satisfied when the key
// is absent from the request (IfExists semantics).
//
// Matches the Ceph IfExists compatibility gap on the currently supported
// condition-key subset.
#[test]
fn test_bucket_policy_condition_operator_if_exists() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": alt_policy_principal(),
                "Action": "s3:PutObject",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringLikeIfExists": {
                        "s3:x-amz-copy-source": "src/public/*"
                    }
                }
            }]
        })
        .to_string();

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_ok("PutObject with StringLikeIfExists condition", || {
            alt_client
                .put_object()
                .bucket(&bucket)
                .key("foo")
                .body(ByteStream::from_static(b"bar"))
                .send()
        })
        .await;

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();
        let body = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), b"bar");

        cleanup(&bucket, &["foo"]).await;
    });
}

#[test]
fn test_bucket_policy_string_not_like_if_exists_copy_source_condition() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let src_bucket = create_bucket_allowing_public_policy(client).await;
        let dst_bucket = create_bucket_allowing_public_policy(client).await;

        for key in ["public/foo", "blocked/foo"] {
            client
                .put_object()
                .bucket(&src_bucket)
                .key(key)
                .body(ByteStream::from_static(b"copy-source"))
                .send()
                .await
                .unwrap();
        }
        client
            .put_bucket_policy()
            .bucket(&src_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:GetObject",
                        "Resource": bucket_wildcard_resource(&src_bucket)
                    }]
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();
        client
            .put_bucket_policy()
            .bucket(&dst_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:PutObject",
                        "Resource": bucket_wildcard_resource(&dst_bucket),
                        "Condition": {
                            "StringNotLikeIfExists": {
                                "s3:x-amz-copy-source": format!("{src_bucket}/blocked/*")
                            }
                        }
                    }]
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_ok("PutObject with absent StringNotLikeIfExists key", || {
            alt_client
                .put_object()
                .bucket(&dst_bucket)
                .key("direct")
                .body(ByteStream::from_static(b"direct"))
                .send()
        })
        .await;
        eventually_ok(
            "CopyObject with StringNotLikeIfExists nonmatching copy-source",
            || {
                alt_client
                    .copy_object()
                    .bucket(&dst_bucket)
                    .key("copied-public")
                    .copy_source(format!("{src_bucket}/public/foo"))
                    .send()
            },
        )
        .await;
        eventually_access_denied(
            "CopyObject denied when StringNotLikeIfExists copy-source pattern matches",
            || {
                alt_client
                    .copy_object()
                    .bucket(&dst_bucket)
                    .key("copied-blocked")
                    .copy_source(format!("{src_bucket}/blocked/foo"))
                    .send()
            },
        )
        .await;

        cleanup(&dst_bucket, &["direct", "copied-public", "copied-blocked"]).await;
        cleanup(&src_bucket, &["public/foo", "blocked/foo"]).await;
    });
}

#[test]
fn test_bucket_policy_put_object_if_none_match_condition() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:PutObject",
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:if-none-match": "*"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_access_denied(
            "PutObject denied without If-None-Match condition header",
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("if-none-match-missing")
                    .body(ByteStream::from_static(b"missing"))
                    .send()
            },
        )
        .await;

        eventually_ok(
            "PutObject allowed with matching If-None-Match condition",
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("if-none-match-present")
                    .if_none_match("*")
                    .body(ByteStream::from_static(b"present"))
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &["if-none-match-present"]).await;
    });
}

#[test]
fn test_bucket_policy_put_object_if_match_condition() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let key = "if-match-object";
        let etag_header = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"old"))
            .send()
            .await
            .unwrap()
            .e_tag()
            .expect("expected ETag")
            .to_string();
        let entity_tag = etag_header.trim_matches('"').to_string();

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
                            "Action": "s3:PutObject",
                            "Resource": bucket_wildcard_resource(&bucket),
                            "Condition": {
                                "StringEquals": {
                                    "s3:if-match": entity_tag
                                }
                            }
                        },
                        {
                            "Effect": "Allow",
                            "Principal": alt_policy_principal(),
                            "Action": "s3:GetObject",
                            "Resource": bucket_wildcard_resource(&bucket)
                        }
                    ],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_access_denied("PutObject denied without If-Match condition header", || {
            alt_client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(b"missing"))
                .send()
        })
        .await;

        eventually_ok("PutObject allowed with matching If-Match condition", || {
            alt_client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .if_match(&etag_header)
                .body(ByteStream::from_static(b"new"))
                .send()
        })
        .await;

        let body = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap()
            .body
            .collect()
            .await
            .unwrap()
            .into_bytes();
        assert_eq!(body.as_ref(), b"new");

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_copy_object_if_match_condition() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let src_key = "copy-if-match-source";
        let dst_key = "copy-if-match-destination";
        client
            .put_object()
            .bucket(&bucket)
            .key(src_key)
            .body(ByteStream::from_static(b"source"))
            .send()
            .await
            .unwrap();
        let dst_etag = client
            .put_object()
            .bucket(&bucket)
            .key(dst_key)
            .body(ByteStream::from_static(b"destination"))
            .send()
            .await
            .unwrap()
            .e_tag()
            .unwrap()
            .to_string();

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
                            "Action": "s3:PutObject",
                            "Resource": bucket_wildcard_resource(&bucket),
                            "Condition": {
                                "Null": {
                                    "s3:if-match": "false"
                                },
                                "Bool": {
                                    "s3:ObjectCreationOperation": "true"
                                }
                            }
                        },
                        {
                            "Effect": "Allow",
                            "Principal": alt_policy_principal(),
                            "Action": "s3:GetObject",
                            "Resource": bucket_wildcard_resource(&bucket)
                        }
                    ],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_access_denied(
            "CopyObject denied without If-Match condition header",
            || {
                alt_client
                    .copy_object()
                    .bucket(&bucket)
                    .key(dst_key)
                    .copy_source(format!("{bucket}/{src_key}"))
                    .send()
            },
        )
        .await;

        let raw_copy_url = object_url(CTX.endpoint(), &bucket, dst_key, None);
        let conditional_copy = send_signed_request_with_credentials(
            "PUT",
            &raw_copy_url,
            b"",
            [
                ("x-amz-copy-source", format!("{bucket}/{src_key}")),
                ("if-match", dst_etag),
            ],
            raw_alt_credentials(),
        );
        assert_eq!(
            conditional_copy.status, 200,
            "unexpected CopyObject If-Match response: {}",
            conditional_copy.body
        );
        let copied = client
            .get_object()
            .bucket(&bucket)
            .key(dst_key)
            .send()
            .await
            .unwrap()
            .body
            .collect()
            .await
            .unwrap()
            .into_bytes();
        assert_eq!(copied.as_ref(), b"source");

        cleanup(&bucket, &[src_key, dst_key]).await;
    });
}

#[test]
fn test_bucket_policy_object_creation_operation_bool_variants_allow_put_object() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        for operator in ["BoolIfExists", "ForAllValues:Bool", "ForAnyValue:Bool"] {
            let bucket = create_bucket_allowing_public_policy(client).await;
            let key = format!(
                "object-creation-{}",
                operator.to_ascii_lowercase().replace(':', "-")
            );
            let policy = json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:PutObject",
                    "Resource": bucket_wildcard_resource(&bucket),
                    "Condition": {
                        operator: {
                            "s3:ObjectCreationOperation": "true"
                        }
                    }
                }],
            })
            .to_string();
            client
                .put_bucket_policy()
                .bucket(&bucket)
                .policy(policy)
                .send()
                .await
                .unwrap();

            eventually_ok(
                &format!("PutObject with {operator} s3:ObjectCreationOperation condition"),
                || {
                    alt_client
                        .put_object()
                        .bucket(&bucket)
                        .key(&key)
                        .body(ByteStream::from_static(b"object-creation-bool"))
                        .send()
                },
            )
            .await;

            cleanup(&bucket, &[&key]).await;
        }
    });
}

#[test]
fn test_bucket_policy_object_creation_operation_bool_does_not_wildcard_match() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let control_key = "object-creation-bool-wildcard-control";
        let denied_key = "object-creation-bool-wildcard-denied";

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:PutObject",
                    "Resource": object_resource(&bucket, control_key),
                    "Condition": {
                        "Bool": {
                            "s3:ObjectCreationOperation": "true"
                        }
                    }
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:PutObject",
                    "Resource": object_resource(&bucket, denied_key),
                    "Condition": {
                        "Bool": {
                            "s3:ObjectCreationOperation": "tr*"
                        }
                    }
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_ok(
            "PutObject with literal Bool true ObjectCreationOperation condition",
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key(control_key)
                    .body(ByteStream::from_static(b"bool-wildcard-control"))
                    .send()
            },
        )
        .await;

        let denied = alt_client
            .put_object()
            .bucket(&bucket)
            .key(denied_key)
            .body(ByteStream::from_static(b"bool-wildcard-denied"))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        cleanup(&bucket, &[control_key, denied_key]).await;
    });
}

#[test]
fn test_bucket_policy_copy_object_if_none_match_condition() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let src_key = "copy-if-none-match-source";
        let dst_key = "copy-if-none-match-destination";
        client
            .put_object()
            .bucket(&bucket)
            .key(src_key)
            .body(ByteStream::from_static(b"source"))
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
                            "Action": "s3:PutObject",
                            "Resource": bucket_wildcard_resource(&bucket),
                            "Condition": {
                                "Null": {
                                    "s3:if-none-match": "false"
                                },
                                "Bool": {
                                    "s3:ObjectCreationOperation": "true"
                                }
                            }
                        },
                        {
                            "Effect": "Allow",
                            "Principal": alt_policy_principal(),
                            "Action": "s3:GetObject",
                            "Resource": bucket_wildcard_resource(&bucket)
                        }
                    ],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_access_denied(
            "CopyObject denied without If-None-Match condition header",
            || {
                alt_client
                    .copy_object()
                    .bucket(&bucket)
                    .key(dst_key)
                    .copy_source(format!("{bucket}/{src_key}"))
                    .send()
            },
        )
        .await;

        let raw_copy_url = object_url(CTX.endpoint(), &bucket, dst_key, None);
        let conditional_copy = send_signed_request_with_credentials(
            "PUT",
            &raw_copy_url,
            b"",
            [
                ("x-amz-copy-source", format!("{bucket}/{src_key}")),
                ("if-none-match", "*".to_string()),
            ],
            raw_alt_credentials(),
        );
        assert_eq!(
            conditional_copy.status, 200,
            "unexpected CopyObject If-None-Match response: {}",
            conditional_copy.body
        );
        let copied = client
            .get_object()
            .bucket(&bucket)
            .key(dst_key)
            .send()
            .await
            .unwrap()
            .body
            .collect()
            .await
            .unwrap()
            .into_bytes();
        assert_eq!(copied.as_ref(), b"source");

        cleanup(&bucket, &[src_key, dst_key]).await;
    });
}

async fn prepare_single_part_multipart_upload(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    body: &'static [u8],
) -> (String, String) {
    let create = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    let upload_id = create.upload_id().unwrap().to_string();
    let part = client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from_static(body))
        .send()
        .await
        .unwrap();
    (upload_id, part.e_tag().unwrap().to_string())
}

fn single_part_complete_payload(part_etag: &str) -> CompletedMultipartUpload {
    CompletedMultipartUpload::builder()
        .parts(
            CompletedPart::builder()
                .part_number(1)
                .e_tag(part_etag)
                .build(),
        )
        .build()
}

#[test]
fn test_bucket_policy_complete_multipart_upload_if_none_match_condition() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let key = "conditional-complete-if-none-match";

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
                            "Action": "s3:PutObject",
                            "Resource": bucket_wildcard_resource(&bucket)
                        },
                        {
                            "Effect": "Deny",
                            "Principal": alt_policy_principal(),
                            "Action": "s3:PutObject",
                            "Resource": bucket_wildcard_resource(&bucket),
                            "Condition": {
                                "Null": {
                                    "s3:if-none-match": "true"
                                },
                                "Bool": {
                                    "s3:ObjectCreationOperation": "true"
                                }
                            }
                        }
                    ],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let (denied_upload_id, denied_part_etag) =
            prepare_single_part_multipart_upload(alt_client, &bucket, key, b"denied").await;
        eventually_access_denied(
            "CompleteMultipartUpload denied without required If-None-Match condition",
            || {
                alt_client
                    .complete_multipart_upload()
                    .bucket(&bucket)
                    .key(key)
                    .upload_id(&denied_upload_id)
                    .multipart_upload(single_part_complete_payload(&denied_part_etag))
                    .send()
            },
        )
        .await;
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&denied_upload_id)
            .send()
            .await
            .unwrap();

        let (upload_id, part_etag) =
            prepare_single_part_multipart_upload(alt_client, &bucket, key, b"created").await;
        eventually_ok(
            "CompleteMultipartUpload allowed with matching If-None-Match condition",
            || {
                alt_client
                    .complete_multipart_upload()
                    .bucket(&bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .if_none_match("*")
                    .multipart_upload(single_part_complete_payload(&part_etag))
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_complete_multipart_upload_if_match_condition() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let key = "conditional-complete-if-match";
        let etag = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"old"))
            .send()
            .await
            .unwrap()
            .e_tag()
            .unwrap()
            .to_string();

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
                            "Action": "s3:PutObject",
                            "Resource": bucket_wildcard_resource(&bucket)
                        },
                        {
                            "Effect": "Allow",
                            "Principal": alt_policy_principal(),
                            "Action": "s3:GetObject",
                            "Resource": bucket_wildcard_resource(&bucket)
                        },
                        {
                            "Effect": "Deny",
                            "Principal": alt_policy_principal(),
                            "Action": "s3:PutObject",
                            "Resource": bucket_wildcard_resource(&bucket),
                            "Condition": {
                                "Null": {
                                    "s3:if-match": "true"
                                },
                                "Bool": {
                                    "s3:ObjectCreationOperation": "true"
                                }
                            }
                        }
                    ],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let (denied_upload_id, denied_part_etag) =
            prepare_single_part_multipart_upload(alt_client, &bucket, key, b"denied").await;
        eventually_access_denied(
            "CompleteMultipartUpload denied without required If-Match condition",
            || {
                alt_client
                    .complete_multipart_upload()
                    .bucket(&bucket)
                    .key(key)
                    .upload_id(&denied_upload_id)
                    .multipart_upload(single_part_complete_payload(&denied_part_etag))
                    .send()
            },
        )
        .await;
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&denied_upload_id)
            .send()
            .await
            .unwrap();

        let (upload_id, part_etag) =
            prepare_single_part_multipart_upload(alt_client, &bucket, key, b"new").await;
        eventually_ok(
            "CompleteMultipartUpload allowed with matching If-Match condition",
            || {
                alt_client
                    .complete_multipart_upload()
                    .bucket(&bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .if_match(&etag)
                    .multipart_upload(single_part_complete_payload(&part_etag))
                    .send()
            },
        )
        .await;

        let body = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap()
            .body
            .collect()
            .await
            .unwrap()
            .into_bytes();
        assert_eq!(body.as_ref(), b"new");

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_list_bucket_prefix_condition() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        for key in ["allowed/one", "blocked/one"] {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(b"body"))
                .send()
                .await
                .unwrap();
        }

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:ListBucket",
                        "Resource": bucket_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:prefix": "allowed/"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let listed = eventually_ok(
            "ListObjectsV2 allowed with matching prefix condition",
            || {
                alt_client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .prefix("allowed/")
                    .send()
            },
        )
        .await;
        assert_eq!(listed.contents().len(), 1);
        assert_eq!(listed.contents()[0].key(), Some("allowed/one"));

        eventually_access_denied(
            "ListObjectsV2 denied without prefix condition value",
            || alt_client.list_objects_v2().bucket(&bucket).send(),
        )
        .await;
        eventually_access_denied(
            "ListObjectsV2 denied with nonmatching prefix condition",
            || {
                alt_client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .prefix("blocked/")
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &["allowed/one", "blocked/one"]).await;
    });
}

#[test]
fn test_bucket_policy_list_bucket_delimiter_condition() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        for key in ["allowed/one", "allowed/two"] {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(b"body"))
                .send()
                .await
                .unwrap();
        }

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:ListBucket",
                        "Resource": bucket_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:delimiter": "/"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let listed = eventually_ok(
            "ListObjectsV2 allowed with matching delimiter condition",
            || {
                alt_client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .delimiter("/")
                    .send()
            },
        )
        .await;
        assert!(listed
            .common_prefixes()
            .iter()
            .any(|prefix| prefix.prefix() == Some("allowed/")));

        eventually_access_denied(
            "ListObjectsV2 denied without delimiter condition value",
            || alt_client.list_objects_v2().bucket(&bucket).send(),
        )
        .await;
        eventually_access_denied(
            "ListObjectsV2 denied with nonmatching delimiter condition",
            || {
                alt_client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .delimiter(".")
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &["allowed/one", "allowed/two"]).await;
    });
}

#[test]
fn test_bucket_policy_list_prefix_and_delimiter_conditions_use_decoded_query_values() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        for key in ["allowed/one", "blocked/one"] {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(b"body"))
                .send()
                .await
                .unwrap();
        }

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Deny",
                            "Principal": "*",
                            "Action": "s3:ListBucket",
                            "Resource": bucket_resource(&bucket),
                            "Condition": {
                                "StringNotEquals": {
                                    "s3:prefix": "allowed/"
                                }
                            }
                        },
                        {
                            "Effect": "Deny",
                            "Principal": "*",
                            "Action": "s3:ListBucket",
                            "Resource": bucket_resource(&bucket),
                            "Condition": {
                                "StringNotEquals": {
                                    "s3:delimiter": "/"
                                }
                            }
                        }
                    ],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let denied_url = format!(
            "{}/{bucket}?list-type=2&prefix=blocked%2F&delimiter=%2F",
            CTX.endpoint()
        );
        eventually_result_matches(
            "ListObjectsV2 denied with nonmatching decoded prefix",
            20,
            std::time::Duration::from_millis(200),
            || {
                let denied_url = denied_url.clone();
                async move {
                    Ok::<_, std::convert::Infallible>(send_signed_request(
                        "GET",
                        &denied_url,
                        b"",
                        std::iter::empty::<(&str, &str)>(),
                    ))
                }
            },
            |result| result.as_ref().is_ok_and(|response| response.status == 403),
        )
        .await;

        let allowed_url = format!(
            "{}/{bucket}?list-type=2&prefix=allowed%2F&delimiter=%2F",
            CTX.endpoint()
        );
        let allowed =
            send_signed_request("GET", &allowed_url, b"", std::iter::empty::<(&str, &str)>());
        assert_eq!(
            allowed.status, 200,
            "expected decoded prefix/delimiter to avoid deny, got {} body={}",
            allowed.status, allowed.body
        );

        cleanup(&bucket, &["allowed/one", "blocked/one"]).await;
    });
}

#[test]
fn test_bucket_policy_list_bucket_max_keys_numeric_equals_condition() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        for key in ["one", "two", "three"] {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(b"body"))
                .send()
                .await
                .unwrap();
        }

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:ListBucket",
                        "Resource": bucket_resource(&bucket),
                        "Condition": {
                            "NumericEquals": {
                                "s3:max-keys": 2
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let listed = eventually_ok(
            "ListObjectsV2 allowed with matching max-keys condition",
            || {
                alt_client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .max_keys(2)
                    .send()
            },
        )
        .await;
        assert_eq!(listed.max_keys(), Some(2));

        eventually_access_denied(
            "ListObjectsV2 denied without max-keys condition value",
            || alt_client.list_objects_v2().bucket(&bucket).send(),
        )
        .await;
        eventually_access_denied(
            "ListObjectsV2 denied with nonmatching max-keys condition",
            || {
                alt_client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .max_keys(3)
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &["one", "two", "three"]).await;
    });
}

async fn put_max_keys_condition_policy(bucket: &str, operator: &str, policy_value: i32) {
    let mut condition = serde_json::Map::new();
    condition.insert(operator.to_string(), json!({ "s3:max-keys": policy_value }));

    CTX.client()
        .put_bucket_policy()
        .bucket(bucket)
        .policy(
            json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": alt_policy_principal(),
                    "Action": "s3:ListBucket",
                    "Resource": bucket_resource(bucket),
                    "Condition": condition,
                }],
            })
            .to_string(),
        )
        .send()
        .await
        .unwrap();
}

async fn assert_max_keys_condition_operator(
    operator: &str,
    policy_value: i32,
    allowed_max_keys: i32,
    denied_max_keys: i32,
    omitted_should_match: bool,
) {
    let client = CTX.client();
    let alt_client = CTX.alt_client();
    let bucket = create_bucket_allowing_public_policy(client).await;
    client
        .put_object()
        .bucket(&bucket)
        .key("one")
        .body(ByteStream::from_static(b"body"))
        .send()
        .await
        .unwrap();

    put_max_keys_condition_policy(&bucket, operator, policy_value).await;

    let allow_description =
        format!("ListObjectsV2 allowed for {operator} when max-keys is {allowed_max_keys}");
    let listed = eventually_ok(&allow_description, || {
        alt_client
            .list_objects_v2()
            .bucket(&bucket)
            .max_keys(allowed_max_keys)
            .send()
    })
    .await;
    assert_eq!(listed.max_keys(), Some(allowed_max_keys));

    if omitted_should_match {
        let omitted_description =
            format!("ListObjectsV2 allowed for {operator} when max-keys is omitted");
        eventually_ok(&omitted_description, || {
            alt_client.list_objects_v2().bucket(&bucket).send()
        })
        .await;
    }

    let deny_description =
        format!("ListObjectsV2 denied for {operator} when max-keys is {denied_max_keys}");
    eventually_access_denied(&deny_description, || {
        alt_client
            .list_objects_v2()
            .bucket(&bucket)
            .max_keys(denied_max_keys)
            .send()
    })
    .await;

    cleanup(&bucket, &["one"]).await;
}

#[test]
fn test_bucket_policy_list_bucket_max_keys_numeric_comparison_operators() {
    s3_tests::run(async {
        for (operator, policy_value, allowed_max_keys, denied_max_keys) in [
            ("NumericNotEquals", 2, 3, 2),
            ("NumericLessThan", 2, 1, 2),
            ("NumericLessThanEquals", 2, 2, 3),
            ("NumericGreaterThan", 2, 3, 2),
            ("NumericGreaterThanEquals", 2, 2, 1),
        ] {
            assert_max_keys_condition_operator(
                operator,
                policy_value,
                allowed_max_keys,
                denied_max_keys,
                false,
            )
            .await;
        }
    });
}

#[test]
fn test_bucket_policy_list_bucket_max_keys_numeric_if_exists_operators() {
    s3_tests::run(async {
        for (operator, policy_value, allowed_max_keys, denied_max_keys) in [
            ("NumericEqualsIfExists", 2, 2, 3),
            ("NumericNotEqualsIfExists", 2, 3, 2),
            ("NumericLessThanIfExists", 2, 1, 2),
            ("NumericLessThanEqualsIfExists", 2, 2, 3),
            ("NumericGreaterThanIfExists", 2, 3, 2),
            ("NumericGreaterThanEqualsIfExists", 2, 2, 1),
        ] {
            assert_max_keys_condition_operator(
                operator,
                policy_value,
                allowed_max_keys,
                denied_max_keys,
                true,
            )
            .await;
        }
    });
}

#[test]
fn test_bucket_policy_list_bucket_max_keys_string_equals_condition() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        for key in ["one", "two", "three"] {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(b"body"))
                .send()
                .await
                .unwrap();
        }

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:ListBucket",
                        "Resource": bucket_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:max-keys": "2"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let listed = eventually_ok(
            "ListObjectsV2 allowed with matching string max-keys condition",
            || {
                alt_client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .max_keys(2)
                    .send()
            },
        )
        .await;
        assert_eq!(listed.max_keys(), Some(2));

        eventually_access_denied(
            "ListObjectsV2 denied with nonmatching string max-keys condition",
            || {
                alt_client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .max_keys(3)
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &["one", "two", "three"]).await;
    });
}

#[test]
fn test_bucket_policy_list_bucket_max_keys_omitted_is_absent_for_numeric_condition() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        client
            .put_object()
            .bucket(&bucket)
            .key("one")
            .body(ByteStream::from_static(b"body"))
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
                        "Action": "s3:ListBucket",
                        "Resource": bucket_resource(&bucket),
                        "Condition": {
                            "NumericEquals": {
                                "s3:max-keys": 1000
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let listed = eventually_ok(
            "ListObjectsV2 allowed with explicit default max-keys condition",
            || {
                alt_client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .max_keys(1000)
                    .send()
            },
        )
        .await;
        assert_eq!(listed.max_keys(), Some(1000));

        eventually_access_denied("ListObjectsV2 denied when max-keys is omitted", || {
            alt_client.list_objects_v2().bucket(&bucket).send()
        })
        .await;
        eventually_access_denied(
            "ListObjectsV2 denied with explicit nonmatching max-keys condition",
            || {
                alt_client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .max_keys(999)
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &["one"]).await;
    });
}

#[test]
fn test_bucket_policy_list_bucket_max_keys_numeric_equals_nonnumeric_value_does_not_match() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        client
            .put_object()
            .bucket(&bucket)
            .key("allowed/one")
            .body(ByteStream::from_static(b"body"))
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
                            "Action": "s3:ListBucket",
                            "Resource": bucket_resource(&bucket),
                            "Condition": {
                                "StringEquals": {
                                    "s3:prefix": "allowed/"
                                }
                            }
                        },
                        {
                            "Effect": "Allow",
                            "Principal": alt_policy_principal(),
                            "Action": "s3:ListBucket",
                            "Resource": bucket_resource(&bucket),
                            "Condition": {
                                "StringEquals": {
                                    "s3:prefix": "denied/"
                                },
                                "NumericEquals": {
                                    "s3:max-keys": "not-a-number"
                                }
                            }
                        }
                    ],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let listed = eventually_ok(
            "ListObjectsV2 allowed by the control prefix statement",
            || {
                alt_client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .prefix("allowed/")
                    .send()
            },
        )
        .await;
        assert_eq!(listed.contents().len(), 1);

        eventually_access_denied(
            "ListObjectsV2 denied when NumericEquals operand is not numeric",
            || {
                alt_client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .prefix("denied/")
                    .max_keys(2)
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &["allowed/one"]).await;
    });
}

#[test]
fn test_bucket_policy_list_bucket_max_keys_numeric_equals_wildcard_value_does_not_match() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        for key in ["allowed/one", "denied/one"] {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(b"body"))
                .send()
                .await
                .unwrap();
        }

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
                            "Action": "s3:ListBucket",
                            "Resource": bucket_resource(&bucket),
                            "Condition": {
                                "StringEquals": {
                                    "s3:prefix": "allowed/"
                                }
                            }
                        },
                        {
                            "Effect": "Allow",
                            "Principal": alt_policy_principal(),
                            "Action": "s3:ListBucket",
                            "Resource": bucket_resource(&bucket),
                            "Condition": {
                                "StringEquals": {
                                    "s3:prefix": "denied/"
                                },
                                "NumericEquals": {
                                    "s3:max-keys": "*"
                                }
                            }
                        }
                    ],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let listed = eventually_ok(
            "ListObjectsV2 allowed by the control prefix statement",
            || {
                alt_client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .prefix("allowed/")
                    .send()
            },
        )
        .await;
        assert_eq!(listed.contents().len(), 1);

        eventually_access_denied(
            "ListObjectsV2 denied when NumericEquals operand is wildcard",
            || {
                alt_client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .prefix("denied/")
                    .max_keys(2)
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &["allowed/one", "denied/one"]).await;
    });
}

#[test]
fn test_bucket_policy_list_bucket_versions_prefix_condition() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;
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
        for key in ["allowed/versioned", "blocked/versioned"] {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(b"body"))
                .send()
                .await
                .unwrap();
        }

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:ListBucketVersions",
                        "Resource": bucket_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:prefix": "allowed/"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let listed = eventually_ok(
            "ListObjectVersions allowed with matching prefix condition",
            || {
                alt_client
                    .list_object_versions()
                    .bucket(&bucket)
                    .prefix("allowed/")
                    .send()
            },
        )
        .await;
        assert!(listed
            .versions()
            .iter()
            .any(|version| version.key() == Some("allowed/versioned")));

        eventually_access_denied(
            "ListObjectVersions denied without prefix condition value",
            || alt_client.list_object_versions().bucket(&bucket).send(),
        )
        .await;
        eventually_access_denied(
            "ListObjectVersions denied with nonmatching prefix condition",
            || {
                alt_client
                    .list_object_versions()
                    .bucket(&bucket)
                    .prefix("blocked/")
                    .send()
            },
        )
        .await;

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_bucket_policy_list_bucket_versions_delimiter_condition() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;
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
        for key in ["allowed/versioned", "allowed/again", "z-versioned"] {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(b"body"))
                .send()
                .await
                .unwrap();
        }

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:ListBucketVersions",
                        "Resource": bucket_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:delimiter": "/"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let listed = eventually_ok(
            "ListObjectVersions allowed with matching delimiter condition",
            || {
                alt_client
                    .list_object_versions()
                    .bucket(&bucket)
                    .delimiter("/")
                    .max_keys(1)
                    .send()
            },
        )
        .await;
        assert!(listed.versions().is_empty());
        assert_eq!(listed.common_prefixes().len(), 1);
        assert_eq!(listed.common_prefixes()[0].prefix(), Some("allowed/"));
        assert_eq!(listed.is_truncated(), Some(true));
        assert_eq!(listed.next_key_marker(), Some("allowed/"));
        assert_eq!(listed.next_version_id_marker(), None);

        let listed_second_page = alt_client
            .list_object_versions()
            .bucket(&bucket)
            .delimiter("/")
            .key_marker(listed.next_key_marker().unwrap())
            .send()
            .await
            .unwrap();
        assert!(listed_second_page.common_prefixes().is_empty());
        assert!(listed_second_page
            .versions()
            .iter()
            .any(|version| version.key() == Some("z-versioned")));

        let listed_after_marker_inside_prefix = alt_client
            .list_object_versions()
            .bucket(&bucket)
            .delimiter("/")
            .key_marker("allowed/again")
            .send()
            .await
            .unwrap();
        assert!(listed_after_marker_inside_prefix
            .common_prefixes()
            .is_empty());
        assert!(listed_after_marker_inside_prefix
            .versions()
            .iter()
            .any(|version| version.key() == Some("z-versioned")));

        eventually_access_denied(
            "ListObjectVersions denied without delimiter condition value",
            || alt_client.list_object_versions().bucket(&bucket).send(),
        )
        .await;
        eventually_access_denied(
            "ListObjectVersions denied with nonmatching delimiter condition",
            || {
                alt_client
                    .list_object_versions()
                    .bucket(&bucket)
                    .delimiter(".")
                    .send()
            },
        )
        .await;

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_bucket_policy_list_bucket_versions_max_keys_numeric_equals_condition() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;
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
        for key in ["one", "two", "three"] {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(b"body"))
                .send()
                .await
                .unwrap();
        }

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:ListBucketVersions",
                        "Resource": bucket_resource(&bucket),
                        "Condition": {
                            "NumericEquals": {
                                "s3:max-keys": 2
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let listed = eventually_ok(
            "ListObjectVersions allowed with matching max-keys condition",
            || {
                alt_client
                    .list_object_versions()
                    .bucket(&bucket)
                    .max_keys(2)
                    .send()
            },
        )
        .await;
        assert_eq!(listed.max_keys(), Some(2));

        eventually_access_denied(
            "ListObjectVersions denied without max-keys condition value",
            || alt_client.list_object_versions().bucket(&bucket).send(),
        )
        .await;
        eventually_access_denied(
            "ListObjectVersions denied with nonmatching max-keys condition",
            || {
                alt_client
                    .list_object_versions()
                    .bucket(&bucket)
                    .max_keys(3)
                    .send()
            },
        )
        .await;

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_bucket_policy_list_multipart_uploads_prefix_condition_is_rejected() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_public_policy(client).await;

        let result = client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:ListBucketMultipartUploads",
                        "Resource": bucket_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:prefix": "allowed/"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await;
        assert_s3_err_code(&result, "MalformedPolicy");

        cleanup(&bucket, &[]).await;
    });
}

fn ownership_controls(object_ownership: ObjectOwnership) -> OwnershipControls {
    let rule = OwnershipControlsRule::builder()
        .object_ownership(object_ownership)
        .build()
        .unwrap();
    OwnershipControls::builder().rules(rule).build().unwrap()
}

#[test]
fn test_bucket_policy_get_bucket_location_location_constraint_condition_is_rejected() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_public_policy(client).await;

        let result = client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:GetBucketLocation",
                        "Resource": bucket_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:locationconstraint": CTX.region()
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await;
        assert_s3_err_code(&result, "MalformedPolicy");

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_put_bucket_ownership_controls_object_ownership_condition() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:PutBucketOwnershipControls",
                        "Resource": bucket_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:x-amz-object-ownership": "BucketOwnerPreferred"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        eventually_access_denied(
            "PutBucketOwnershipControls denied with nonmatching s3:x-amz-object-ownership",
            || {
                alt_client
                    .put_bucket_ownership_controls()
                    .bucket(&bucket)
                    .ownership_controls(ownership_controls(ObjectOwnership::ObjectWriter))
                    .send()
            },
        )
        .await;

        eventually_ok(
            "PutBucketOwnershipControls allowed with matching s3:x-amz-object-ownership",
            || {
                alt_client
                    .put_bucket_ownership_controls()
                    .bucket(&bucket)
                    .ownership_controls(ownership_controls(ObjectOwnership::BucketOwnerPreferred))
                    .send()
            },
        )
        .await;

        let controls = client
            .get_bucket_ownership_controls()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(
            controls
                .ownership_controls()
                .and_then(|controls| controls.rules().first())
                .map(|rule| rule.object_ownership()),
            Some(&ObjectOwnership::BucketOwnerPreferred)
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_get_bucket_acl_requires_dedicated_action() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let denied = alt_client
            .get_bucket_acl()
            .bucket(&bucket)
            .send_retrying_operation_aborted("get bucket ACL as alternate account")
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                alt_policy_principal(),
                "Allow",
                "s3:GetBucketAcl",
                bucket_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        let acl = eventually_ok("GetBucketAcl with bucket policy", || {
            alt_client.get_bucket_acl().bucket(&bucket).send()
        })
        .await;
        assert!(acl.owner().is_some(), "expected owner in GetBucketAcl");

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_put_bucket_acl_requires_dedicated_action() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        set_object_writer_ownership(&bucket).await;

        let denied = alt_client
            .put_bucket_acl()
            .bucket(&bucket)
            .acl(BucketCannedAcl::Private)
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                alt_policy_principal(),
                "Allow",
                "s3:PutBucketAcl",
                bucket_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        eventually_ok("PutBucketAcl with bucket policy", || {
            alt_client
                .put_bucket_acl()
                .bucket(&bucket)
                .acl(BucketCannedAcl::Private)
                .send()
        })
        .await;

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_put_bucket_acl_null_treats_absent_acl_header_as_null() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        set_object_writer_ownership(&bucket).await;

        let alt_id = client_canonical_id(alt_client).await;
        let grant_read_header = format!("id=\"{alt_id}\"");
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": principal.clone(),
                    "Action": "s3:PutBucketAcl",
                    "Resource": bucket_resource(&bucket),
                },
                {
                    "Effect": "Deny",
                    "Principal": principal,
                    "Action": "s3:PutBucketAcl",
                    "Resource": bucket_resource(&bucket),
                    "Condition": {
                        "Null": {
                            "s3:x-amz-acl": "true"
                        }
                    }
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_result_matches(
            "PutBucketAcl denied when x-amz-acl is absent under Null condition",
            60,
            std::time::Duration::from_millis(500),
            || {
                let grant_read_header = grant_read_header.clone();
                alt_client
                    .put_bucket_acl()
                    .bucket(&bucket)
                    .customize()
                    .mutate_request(move |req| {
                        req.headers_mut()
                            .insert("x-amz-grant-read", grant_read_header.clone());
                    })
                    .send()
            },
            |result| {
                result.as_ref().err().is_some_and(|err| {
                    err.raw_response().map(|r| r.status().as_u16()) == Some(403)
                        && err.as_service_error().and_then(ProvideErrorMetadata::code)
                            == Some("AccessDenied")
                })
            },
        )
        .await;

        eventually_ok_with_retry(
            "PutBucketAcl with explicit private ACL under Null condition",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_bucket_acl()
                    .bucket(&bucket)
                    .acl(BucketCannedAcl::Private)
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_put_bucket_acl_string_not_equals_treats_absent_acl_header_as_not_equal() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        set_object_writer_ownership(&bucket).await;

        let alt_id = client_canonical_id(alt_client).await;
        let grant_read_header = format!("id=\"{alt_id}\"");
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": principal.clone(),
                    "Action": "s3:PutBucketAcl",
                    "Resource": bucket_resource(&bucket),
                },
                {
                    "Effect": "Deny",
                    "Principal": principal,
                    "Action": "s3:PutBucketAcl",
                    "Resource": bucket_resource(&bucket),
                    "Condition": {
                        "StringNotEquals": {
                            "s3:x-amz-acl": "private"
                        }
                    }
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_result_matches(
            "PutBucketAcl denied when x-amz-acl is absent under StringNotEquals",
            60,
            std::time::Duration::from_millis(500),
            || {
                let grant_read_header = grant_read_header.clone();
                alt_client
                    .put_bucket_acl()
                    .bucket(&bucket)
                    .customize()
                    .mutate_request(move |req| {
                        req.headers_mut()
                            .insert("x-amz-grant-read", grant_read_header.clone());
                    })
                    .send()
            },
            |result| {
                result.as_ref().err().is_some_and(|err| {
                    err.raw_response().map(|r| r.status().as_u16()) == Some(403)
                        && err.as_service_error().and_then(ProvideErrorMetadata::code)
                            == Some("AccessDenied")
                })
            },
        )
        .await;

        eventually_result_matches(
            "PutBucketAcl denied when x-amz-acl does not equal private",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_bucket_acl()
                    .bucket(&bucket)
                    .acl(BucketCannedAcl::PublicRead)
                    .send()
            },
            |result| {
                result.as_ref().err().is_some_and(|err| {
                    err.raw_response().map(|r| r.status().as_u16()) == Some(403)
                        && err.as_service_error().and_then(ProvideErrorMetadata::code)
                            == Some("AccessDenied")
                })
            },
        )
        .await;

        eventually_ok_with_retry(
            "PutBucketAcl with x-amz-acl=private under StringNotEquals",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_bucket_acl()
                    .bucket(&bucket)
                    .acl(BucketCannedAcl::Private)
                    .send()
            },
        )
        .await;

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_put_bucket_acl_grant_full_control_condition() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        set_object_writer_ownership(&bucket).await;

        let alt_id = client_canonical_id(alt_client).await;
        let grant_full_control_header = format!("id=\"{alt_id}\"");
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:PutBucketAcl",
                "Resource": bucket_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:x-amz-grant-full-control": grant_full_control_header.clone()
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_result_matches(
            "PutBucketAcl denied without matching grant-full-control header",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_bucket_acl()
                    .bucket(&bucket)
                    .acl(BucketCannedAcl::Private)
                    .send()
            },
            |result| {
                result.as_ref().err().is_some_and(|err| {
                    err.raw_response().map(|r| r.status().as_u16()) == Some(403)
                        && err.as_service_error().and_then(ProvideErrorMetadata::code)
                            == Some("AccessDenied")
                })
            },
        )
        .await;

        eventually_ok_with_retry(
            "PutBucketAcl with grant-full-control condition",
            60,
            std::time::Duration::from_millis(500),
            || {
                let grant_full_control_header = grant_full_control_header.clone();
                alt_client
                    .put_bucket_acl()
                    .bucket(&bucket)
                    .customize()
                    .mutate_request(move |req| {
                        req.headers_mut().insert(
                            "x-amz-grant-full-control",
                            grant_full_control_header.clone(),
                        );
                    })
                    .send()
            },
        )
        .await;

        let owner_view = eventually_ok_with_retry(
            "GetBucketAcl after grant-full-control PutBucketAcl",
            60,
            std::time::Duration::from_millis(500),
            || client.get_bucket_acl().bucket(&bucket).send(),
        )
        .await;
        assert!(
            has_grant(owner_view.grants(), Permission::FullControl, Some(&alt_id)),
            "expected FULL_CONTROL grant for alternate account, got {:?}",
            owner_view.grants()
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_put_bucket_acl_grant_read_condition() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        set_object_writer_ownership(&bucket).await;

        let alt_id = client_canonical_id(alt_client).await;
        let grant_read_header = format!("id=\"{alt_id}\"");
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:PutBucketAcl",
                "Resource": bucket_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:x-amz-grant-read": grant_read_header.clone()
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_result_matches(
            "PutBucketAcl denied without matching grant-read header",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_bucket_acl()
                    .bucket(&bucket)
                    .acl(BucketCannedAcl::Private)
                    .send()
            },
            |result| {
                result.as_ref().err().is_some_and(|err| {
                    err.raw_response().map(|r| r.status().as_u16()) == Some(403)
                        && err.as_service_error().and_then(ProvideErrorMetadata::code)
                            == Some("AccessDenied")
                })
            },
        )
        .await;

        eventually_ok_with_retry(
            "PutBucketAcl with grant-read condition",
            60,
            std::time::Duration::from_millis(500),
            || {
                let grant_read_header = grant_read_header.clone();
                alt_client
                    .put_bucket_acl()
                    .bucket(&bucket)
                    .customize()
                    .mutate_request(move |req| {
                        req.headers_mut()
                            .insert("x-amz-grant-read", grant_read_header.clone());
                    })
                    .send()
            },
        )
        .await;

        let owner_view = eventually_ok_with_retry(
            "GetBucketAcl after grant-read PutBucketAcl",
            60,
            std::time::Duration::from_millis(500),
            || client.get_bucket_acl().bucket(&bucket).send(),
        )
        .await;
        assert!(
            has_grant(owner_view.grants(), Permission::Read, Some(&alt_id)),
            "expected READ grant for alternate account, got {:?}",
            owner_view.grants()
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_put_bucket_acl_grant_read_wrong_header_body_mismatch_is_unexpected_content() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        set_object_writer_ownership(&bucket).await;

        let owner_id = client_canonical_id(client).await;
        let alt_id = client_canonical_id(alt_client).await;
        let grant_read_header = format!("id=\"{alt_id}\"");
        let grant_write_header = format!("id=\"{alt_id}\"");
        let acl = AccessControlPolicy::builder()
            .owner(Owner::builder().id(&owner_id).build())
            .set_grants(Some(vec![
                Grant::builder()
                    .grantee(
                        Grantee::builder()
                            .r#type(Type::CanonicalUser)
                            .id(&owner_id)
                            .build()
                            .unwrap(),
                    )
                    .permission(Permission::FullControl)
                    .build(),
                Grant::builder()
                    .grantee(
                        Grantee::builder()
                            .r#type(Type::CanonicalUser)
                            .id(&alt_id)
                            .build()
                            .unwrap(),
                    )
                    .permission(Permission::Read)
                    .build(),
            ]))
            .build();

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:PutBucketAcl",
                "Resource": bucket_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:x-amz-grant-read": grant_read_header.clone()
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let result = alt_client
            .put_bucket_acl()
            .bucket(&bucket)
            .access_control_policy(acl)
            .customize()
            .mutate_request(move |req| {
                req.headers_mut()
                    .insert("x-amz-grant-write", grant_write_header.clone());
            })
            .send()
            .await;
        assert_eq!(err_status(&result), 400, "unexpected result: {result:?}");
        assert_s3_err_code(&result, "UnexpectedContent");

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_put_bucket_acl_grant_read_acp_condition() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        set_object_writer_ownership(&bucket).await;

        let alt_id = client_canonical_id(alt_client).await;
        let grant_read_acp_header = format!("id=\"{alt_id}\"");
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:PutBucketAcl",
                "Resource": bucket_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:x-amz-grant-read-acp": grant_read_acp_header.clone()
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_result_matches(
            "PutBucketAcl denied without matching grant-read-acp header",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_bucket_acl()
                    .bucket(&bucket)
                    .acl(BucketCannedAcl::Private)
                    .send()
            },
            |result| {
                result.as_ref().err().is_some_and(|err| {
                    err.raw_response().map(|r| r.status().as_u16()) == Some(403)
                        && err.as_service_error().and_then(ProvideErrorMetadata::code)
                            == Some("AccessDenied")
                })
            },
        )
        .await;

        eventually_ok_with_retry(
            "PutBucketAcl with grant-read-acp condition",
            60,
            std::time::Duration::from_millis(500),
            || {
                let grant_read_acp_header = grant_read_acp_header.clone();
                alt_client
                    .put_bucket_acl()
                    .bucket(&bucket)
                    .customize()
                    .mutate_request(move |req| {
                        req.headers_mut()
                            .insert("x-amz-grant-read-acp", grant_read_acp_header.clone());
                    })
                    .send()
            },
        )
        .await;

        let owner_view = eventually_ok_with_retry(
            "GetBucketAcl after grant-read-acp PutBucketAcl",
            60,
            std::time::Duration::from_millis(500),
            || client.get_bucket_acl().bucket(&bucket).send(),
        )
        .await;
        assert!(
            has_grant(owner_view.grants(), Permission::ReadAcp, Some(&alt_id)),
            "expected READ_ACP grant for alternate account, got {:?}",
            owner_view.grants()
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_put_bucket_acl_grant_write_condition() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        set_object_writer_ownership(&bucket).await;

        let alt_id = client_canonical_id(alt_client).await;
        let grant_write_header = format!("id=\"{alt_id}\"");
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:PutBucketAcl",
                "Resource": bucket_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:x-amz-grant-write": grant_write_header.clone()
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_result_matches(
            "PutBucketAcl denied without matching grant-write header",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_bucket_acl()
                    .bucket(&bucket)
                    .acl(BucketCannedAcl::Private)
                    .send()
            },
            |result| {
                result.as_ref().err().is_some_and(|err| {
                    err.raw_response().map(|r| r.status().as_u16()) == Some(403)
                        && err.as_service_error().and_then(ProvideErrorMetadata::code)
                            == Some("AccessDenied")
                })
            },
        )
        .await;

        eventually_ok_with_retry(
            "PutBucketAcl with grant-write condition",
            60,
            std::time::Duration::from_millis(500),
            || {
                let grant_write_header = grant_write_header.clone();
                alt_client
                    .put_bucket_acl()
                    .bucket(&bucket)
                    .customize()
                    .mutate_request(move |req| {
                        req.headers_mut()
                            .insert("x-amz-grant-write", grant_write_header.clone());
                    })
                    .send()
            },
        )
        .await;

        let owner_view = eventually_ok_with_retry(
            "GetBucketAcl after grant-write PutBucketAcl",
            60,
            std::time::Duration::from_millis(500),
            || client.get_bucket_acl().bucket(&bucket).send(),
        )
        .await;
        assert!(
            has_grant(owner_view.grants(), Permission::Write, Some(&alt_id)),
            "expected WRITE grant for alternate account, got {:?}",
            owner_view.grants()
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_put_bucket_acl_grant_write_acp_condition() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        set_object_writer_ownership(&bucket).await;

        let alt_id = client_canonical_id(alt_client).await;
        let grant_write_acp_header = format!("id=\"{alt_id}\"");
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:PutBucketAcl",
                "Resource": bucket_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:x-amz-grant-write-acp": grant_write_acp_header.clone()
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        eventually_result_matches(
            "PutBucketAcl denied without matching grant-write-acp header",
            60,
            std::time::Duration::from_millis(500),
            || {
                alt_client
                    .put_bucket_acl()
                    .bucket(&bucket)
                    .acl(BucketCannedAcl::Private)
                    .send()
            },
            |result| {
                result.as_ref().err().is_some_and(|err| {
                    err.raw_response().map(|r| r.status().as_u16()) == Some(403)
                        && err.as_service_error().and_then(ProvideErrorMetadata::code)
                            == Some("AccessDenied")
                })
            },
        )
        .await;

        eventually_ok_with_retry(
            "PutBucketAcl with grant-write-acp condition",
            60,
            std::time::Duration::from_millis(500),
            || {
                let grant_write_acp_header = grant_write_acp_header.clone();
                alt_client
                    .put_bucket_acl()
                    .bucket(&bucket)
                    .customize()
                    .mutate_request(move |req| {
                        req.headers_mut()
                            .insert("x-amz-grant-write-acp", grant_write_acp_header.clone());
                    })
                    .send()
            },
        )
        .await;

        let owner_view = eventually_ok_with_retry(
            "GetBucketAcl after grant-write-acp PutBucketAcl",
            60,
            std::time::Duration::from_millis(500),
            || client.get_bucket_acl().bucket(&bucket).send(),
        )
        .await;
        assert!(
            has_grant(owner_view.grants(), Permission::WriteAcp, Some(&alt_id)),
            "expected WRITE_ACP grant for alternate account, got {:?}",
            owner_view.grants()
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_head_bucket_list_bucket_policy_is_not_sufficient() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let denied = alt_client
            .head_bucket()
            .bucket(&bucket)
            .send_retrying_operation_aborted("head bucket as alternate account")
            .await;
        assert_eq!(err_status(&denied), 403);

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                alt_policy_principal(),
                "Allow",
                "s3:ListBucket",
                bucket_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        eventually_access_denied("HeadBucket still denied with ListBucket policy", || {
            alt_client.head_bucket().bucket(&bucket).send()
        })
        .await;

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_get_bucket_location_requires_dedicated_action() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let denied = alt_client
            .get_bucket_location()
            .bucket(&bucket)
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                alt_policy_principal(),
                "Allow",
                "s3:ListBucket",
                bucket_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        eventually_access_denied("GetBucketLocation denied with ListBucket policy", || {
            alt_client.get_bucket_location().bucket(&bucket).send()
        })
        .await;

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                alt_policy_principal(),
                "Allow",
                "s3:GetBucketLocation",
                bucket_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        let response = eventually_raw_bucket_location(
            "GetBucketLocation allowed with GetBucketLocation policy",
            &bucket,
            SignedRequestCredentials {
                access_key: CTX.alt_access_key(),
                secret_key: CTX.alt_secret_key(),
                region: CTX.region(),
                tls_ca_pem: CTX.tls_ca_pem(),
            },
        )
        .await;
        s3_tests::assert_raw_bucket_location(&response, CTX.region());

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_get_bucket_versioning_requires_dedicated_action() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let denied = alt_client
            .get_bucket_versioning()
            .bucket(&bucket)
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                alt_policy_principal(),
                "Allow",
                "s3:GetBucketVersioning",
                bucket_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        let versioning = eventually_ok("GetBucketVersioning with bucket policy", || {
            alt_client.get_bucket_versioning().bucket(&bucket).send()
        })
        .await;
        assert!(
            versioning.status().is_none(),
            "expected unversioned bucket to report no status"
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_put_bucket_versioning_requires_dedicated_action() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let denied = alt_client
            .put_bucket_versioning()
            .bucket(&bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                alt_policy_principal(),
                "Allow",
                "s3:PutBucketVersioning",
                bucket_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        eventually_ok("PutBucketVersioning with bucket policy", || {
            alt_client
                .put_bucket_versioning()
                .bucket(&bucket)
                .versioning_configuration(
                    VersioningConfiguration::builder()
                        .status(BucketVersioningStatus::Enabled)
                        .build(),
                )
                .send()
        })
        .await;

        let versioning = client
            .get_bucket_versioning()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(versioning.status(), Some(&BucketVersioningStatus::Enabled));

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_bucket_policy_list_object_versions_requires_dedicated_action() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
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
            .put_object()
            .bucket(&bucket)
            .key("key")
            .body(ByteStream::from_static(b"one"))
            .send()
            .await
            .unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key("key")
            .body(ByteStream::from_static(b"two"))
            .send()
            .await
            .unwrap();

        let denied = alt_client
            .list_object_versions()
            .bucket(&bucket)
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                alt_policy_principal(),
                "Allow",
                "s3:ListBucketVersions",
                bucket_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        let versions = eventually_ok("ListObjectVersions with bucket policy", || {
            alt_client.list_object_versions().bucket(&bucket).send()
        })
        .await;
        assert!(
            versions
                .versions()
                .iter()
                .filter(|version| version.key() == Some("key"))
                .count()
                >= 2,
            "expected at least two versions for key"
        );

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_bucket_policy_list_multipart_uploads_requires_dedicated_action() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let upload = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("multipart-key")
            .send()
            .await
            .unwrap();
        let upload_id = upload.upload_id().unwrap().to_string();

        let denied = alt_client
            .list_multipart_uploads()
            .bucket(&bucket)
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                alt_policy_principal(),
                "Allow",
                "s3:ListBucketMultipartUploads",
                bucket_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        let uploads = eventually_ok("ListMultipartUploads with bucket policy", || {
            alt_client.list_multipart_uploads().bucket(&bucket).send()
        })
        .await;
        assert!(uploads
            .uploads()
            .iter()
            .any(|upload| upload.key() == Some("multipart-key")));

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("multipart-key")
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

// ── GetBucketPolicyStatus error shape ───────────────────────────────

#[test]
fn test_get_bucket_policy_status_no_policy_error_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let response = raw_bucket("GET", &bucket, Some("policyStatus="));
        assert_shape(
            "GetBucketPolicyStatus without policy",
            &response,
            &shape()
                .status(404)
                .headers(error_response_headers())
                .body(expected_error::no_such_bucket_policy(&bucket)),
        );

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── Response shapes ─────────────────────────────────────────────────

fn raw_put_policy(bucket: &str, body: &str) -> s3_tests::RawResponse {
    send_signed_request(
        "PUT",
        &format!("{}/{}?policy=", CTX.endpoint(), bucket),
        body.as_bytes(),
        std::iter::empty::<(&str, &str)>(),
    )
}

/// Full response shapes for the bucket-policy CRUD cycle: 204 acks for Put
/// and Delete (Delete is idempotent), and the exact normalized JSON echo
/// with `Content-Type: application/json` from Get.
#[test]
fn test_bucket_policy_crud_response_shapes() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let policy = format!(
            "{{\"Version\":\"2012-10-17\",\"Statement\":[{{\"Sid\":\"DenyAllGetObject\",\
             \"Effect\":\"Deny\",\"Principal\":\"*\",\"Action\":\"s3:GetObject\",\
             \"Resource\":\"arn:aws:s3:::{bucket}/*\"}}]}}"
        );

        assert_shape(
            "PutBucketPolicy",
            &raw_put_policy(&bucket, &policy),
            &shape().status(204).headers(id_headers()).body_empty(),
        );
        assert_shape(
            "GetBucketPolicy",
            &raw_bucket("GET", &bucket, Some("policy=")),
            &shape()
                .status(200)
                .headers(id_headers())
                .header("content-type", "application/json")
                .body(escape_literal(&policy)),
        );
        assert_shape(
            "DeleteBucketPolicy",
            &raw_bucket("DELETE", &bucket, Some("policy=")),
            &shape().status(204).headers(id_headers()).body_empty(),
        );
        assert_shape(
            "DeleteBucketPolicy idempotent",
            &raw_bucket("DELETE", &bucket, Some("policy=")),
            &shape().status(204).headers(id_headers()).body_empty(),
        );

        cleanup(&bucket, &[]).await;
    });
}

/// Full error shapes for the `MalformedPolicy` family. Parse-level failures
/// carry no `<Detail>`; action, resource, and principal validation echo the
/// offending value in one. Principal rejection here is format-level (AWS
/// additionally rejects principals that do not exist, which depends on the
/// account universe).
#[test]
fn test_put_bucket_policy_malformed_response_shapes() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let json_message = "Policies must be valid JSON and the first byte must be '{'";
        let assert_malformed = |context: &str, response: &s3_tests::RawResponse, body: String| {
            assert_shape(
                context,
                response,
                &shape()
                    .status(400)
                    .headers(error_response_headers())
                    .body(body),
            );
        };

        assert_malformed(
            "PutBucketPolicy not JSON",
            &raw_put_policy(&bucket, "not json at all"),
            expected_error::malformed_policy(json_message),
        );
        assert_malformed(
            "PutBucketPolicy JSON array",
            &raw_put_policy(&bucket, "[]"),
            expected_error::malformed_policy(json_message),
        );
        assert_malformed(
            "PutBucketPolicy missing Statement",
            &raw_put_policy(&bucket, "{\"Version\":\"2012-10-17\"}"),
            expected_error::malformed_policy("Missing required field Statement"),
        );
        assert_malformed(
            "PutBucketPolicy unsupported Version value",
            &raw_put_policy(
                &bucket,
                &format!(
                    "{{\"Version\":\"2026-07-11\",\"Statement\":[{{\"Effect\":\"Allow\",\
                     \"Principal\":\"*\",\"Action\":\"s3:ListBucket\",\
                     \"Resource\":\"arn:aws:s3:::{bucket}\"}}]}}"
                ),
            ),
            expected_error::malformed_policy("The policy must contain a valid version string"),
        );
        assert_malformed(
            "PutBucketPolicy empty Statement",
            &raw_put_policy(&bucket, "{\"Version\":\"2012-10-17\",\"Statement\":[]}"),
            expected_error::malformed_policy("Could not parse the policy: Statement is empty!"),
        );
        assert_malformed(
            "PutBucketPolicy invalid action",
            &raw_put_policy(
                &bucket,
                &format!(
                    "{{\"Version\":\"2012-10-17\",\"Statement\":[{{\"Effect\":\"Deny\",\
                     \"Principal\":\"*\",\"Action\":\"s3:NotARealAction\",\
                     \"Resource\":\"arn:aws:s3:::{bucket}/*\"}}]}}"
                ),
            ),
            expected_error::malformed_policy_with_detail(
                "Policy has invalid action",
                "s3:NotARealAction",
            ),
        );
        assert_malformed(
            "PutBucketPolicy foreign resource",
            &raw_put_policy(
                &bucket,
                "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Deny\",\
                 \"Principal\":\"*\",\"Action\":\"s3:GetObject\",\
                 \"Resource\":\"arn:aws:s3:::some-other-bucket-name/*\"}]}",
            ),
            expected_error::malformed_policy_with_detail(
                "Policy has invalid resource",
                "arn:aws:s3:::some-other-bucket-name/*",
            ),
        );
        assert_malformed(
            "PutBucketPolicy wildcard resource",
            &raw_put_policy(
                &bucket,
                "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Deny\",\
                 \"Principal\":\"*\",\"Action\":\"s3:GetObject\",\
                 \"Resource\":\"arn:aws:s3:::*\"}]}",
            ),
            expected_error::malformed_policy_with_detail(
                "Policy has invalid resource",
                "arn:aws:s3:::*",
            ),
        );
        assert_malformed(
            "PutBucketPolicy object action on bucket arn",
            &raw_put_policy(
                &bucket,
                &format!(
                    "{{\"Version\":\"2012-10-17\",\"Statement\":[{{\"Effect\":\"Deny\",\
                     \"Principal\":\"*\",\"Action\":\"s3:GetObject\",\
                     \"Resource\":\"arn:aws:s3:::{bucket}\"}}]}}"
                ),
            ),
            expected_error::malformed_policy_with_detail(
                "Action does not apply to any resource(s) in statement",
                "Action \"s3:GetObject\" in Statement \"NO_ID-0\"",
            ),
        );
        assert_malformed(
            "PutBucketPolicy string-form principal",
            &raw_put_policy(
                &bucket,
                &format!(
                    "{{\"Version\":\"2012-10-17\",\"Statement\":[{{\"Effect\":\"Deny\",\
                     \"Principal\":\"arn:aws:iam::123456789012:root\",\
                     \"Action\":\"s3:GetObject\",\"Resource\":\"arn:aws:s3:::{bucket}/*\"}}]}}"
                ),
            ),
            expected_error::malformed_policy("Invalid policy syntax."),
        );
        assert_malformed(
            "PutBucketPolicy invalid SourceIp operand",
            &raw_put_policy(
                &bucket,
                &format!(
                    "{{\"Version\":\"2012-10-17\",\"Statement\":[{{\"Effect\":\"Allow\",\
                     \"Principal\":\"*\",\"Action\":\"s3:GetObject\",\
                     \"Resource\":\"arn:aws:s3:::{bucket}/*\",\
                     \"Condition\":{{\"IpAddress\":{{\"aws:SourceIp\":\"not-an-ip\"}}}}}}]}}"
                ),
            ),
            expected_error::malformed_policy("Invalid IP address in Conditions"),
        );
        assert_malformed(
            "PutBucketPolicy wildcard SourceIp operand",
            &raw_put_policy(
                &bucket,
                &format!(
                    "{{\"Version\":\"2012-10-17\",\"Statement\":[{{\"Effect\":\"Allow\",\
                     \"Principal\":\"*\",\"Action\":\"s3:GetObject\",\
                     \"Resource\":\"arn:aws:s3:::{bucket}/*\",\
                     \"Condition\":{{\"IpAddress\":{{\"aws:SourceIp\":\"127.0.0.*\"}}}}}}]}}"
                ),
            ),
            expected_error::malformed_policy("Invalid IP address in Conditions"),
        );
        assert_malformed(
            "PutBucketPolicy invalid BinaryEquals operand",
            &raw_put_policy(
                &bucket,
                &format!(
                    "{{\"Version\":\"2012-10-17\",\"Statement\":[{{\"Effect\":\"Allow\",\
                     \"Principal\":\"*\",\"Action\":\"s3:PutObject\",\
                     \"Resource\":\"arn:aws:s3:::{bucket}/*\",\
                     \"Condition\":{{\"BinaryEquals\":{{\"s3:RequestObjectTag/security\":\
                     \"not-base64\"}}}}}}]}}"
                ),
            ),
            expected_error::malformed_policy("Invalid Base64 value for binary condition"),
        );
        assert_malformed(
            "PutBucketPolicy invalid principal",
            &raw_put_policy(
                &bucket,
                &format!(
                    "{{\"Version\":\"2012-10-17\",\"Statement\":[{{\"Effect\":\"Deny\",\
                     \"Principal\":{{\"AWS\":\"arn:aws:iam::123456789012:nonexistent-thing\"}},\
                     \"Action\":\"s3:GetObject\",\"Resource\":\"arn:aws:s3:::{bucket}/*\"}}]}}"
                ),
            ),
            expected_error::malformed_policy_with_detail(
                "Invalid principal in policy",
                "\"AWS\" : \"arn:aws:iam::123456789012:nonexistent-thing\"",
            ),
        );

        cleanup(&bucket, &[]).await;
    });
}
