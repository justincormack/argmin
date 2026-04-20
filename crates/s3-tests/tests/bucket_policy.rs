use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    AccessControlPolicy, BlockedEncryptionTypes, BucketCannedAcl, BucketLifecycleConfiguration,
    BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, CorsConfiguration, CorsRule,
    DefaultRetention, EncryptionType, ExpirationStatus, Grant, Grantee, LifecycleExpiration,
    LifecycleRule, LifecycleRuleFilter, ObjectCannedAcl, ObjectLockConfiguration,
    ObjectLockEnabled, ObjectLockRetentionMode, ObjectLockRule, ObjectOwnership, Owner,
    OwnershipControls, OwnershipControlsRule, Permission, PublicAccessBlockConfiguration,
    ServerSideEncryption, ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration,
    ServerSideEncryptionRule, Tag, Tagging, Type, VersioningConfiguration,
};
use s3_tests::{
    assert_s3_err_code, cleanup_versioned_bucket, create_public_bucket,
    disable_bucket_public_access_block, err_status, put_bucket_lifecycle_with_md5,
    sse_c_header_values, test_sse_c_key, unique_bucket, CTX,
};
use serde_json::json;
use std::future::Future;

fn agent() -> s3_tests::Agent {
    s3_tests::test_agent()
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

fn expected_bucket_location_constraint_for_sdk(region: &str) -> Option<&str> {
    match region {
        "us-east-1" => Some(""),
        "eu-west-1" => Some("EU"),
        other => Some(other),
    }
}

fn require_https_endpoint() {
    assert!(
        endpoint_is_https(),
        "bucket policy SSE-C coverage requires an https:// endpoint; got {}",
        CTX.endpoint()
    );
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
        let _ = client.delete_object().bucket(bucket).key(*key).send().await;
    }

    for _ in 0..10 {
        let uploads = client
            .list_multipart_uploads()
            .bucket(bucket)
            .send()
            .await
            .unwrap();
        for upload in uploads.uploads() {
            let _ = client
                .abort_multipart_upload()
                .bucket(bucket)
                .key(upload.key().unwrap())
                .upload_id(upload.upload_id().unwrap())
                .send()
                .await;
        }

        match client.delete_bucket().bucket(bucket).send().await {
            Ok(_) => return,
            Err(err) => {
                let raw = format!("{err:?}");
                if raw.contains("OperationAborted") || raw.contains("BucketNotEmpty") {
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    continue;
                }
                panic!("delete_bucket failed unexpectedly: {raw}");
            }
        }
    }

    client.delete_bucket().bucket(bucket).send().await.unwrap();
}

async fn cleanup(bucket: &str, keys: &[&str]) {
    cleanup_with_client(CTX.client(), bucket, keys).await;
}

async fn alt_list_objects_v1_eventually(
    bucket: &str,
) -> aws_sdk_s3::operation::list_objects::ListObjectsOutput {
    const MAX_ATTEMPTS: usize = 20;

    for attempt in 0..MAX_ATTEMPTS {
        match CTX.alt_client().list_objects().bucket(bucket).send().await {
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
            .send()
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
            .send()
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
        panic!("{description} did not converge");
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

async fn get_object_eventually(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
) -> aws_sdk_s3::operation::get_object::GetObjectOutput {
    eventually_ok("GetObject", || {
        client.get_object().bucket(bucket).key(key).send()
    })
    .await
}

async fn get_bucket_policy_status_eventually(
    client: &aws_sdk_s3::Client,
    bucket: &str,
) -> aws_sdk_s3::operation::get_bucket_policy_status::GetBucketPolicyStatusOutput {
    eventually_ok("GetBucketPolicyStatus", || {
        client.get_bucket_policy_status().bucket(bucket).send()
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
            .send()
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
        .send()
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
            .send()
            .await
            .unwrap();

        let result = client.get_bucket_policy().bucket(&bucket).send().await;
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
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:ListBucket",
                "Resource": bucket_resource(&bucket),
                "Condition": {
                    "IpAddress": {
                        "aws:SourceIp": "11.0.0.0/8"
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
            ("public/foo", ByteStream::from_static(b"public/foo")),
            ("public/bar", ByteStream::from_static(b"public/bar")),
            ("private/foo", ByteStream::from_static(b"private/foo")),
        ] {
            client
                .put_object()
                .bucket(&src_bucket)
                .key(key)
                .body(body)
                .send()
                .await
                .unwrap();
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
            .send()
            .await
            .unwrap();
        client
            .get_bucket_policy()
            .bucket(&src_bucket)
            .send()
            .await
            .unwrap();
        let source_probe = get_object_eventually(alt_client, &src_bucket, "public/foo").await;
        let source_probe_body = source_probe.body.collect().await.unwrap().into_bytes();
        assert_eq!(source_probe_body.as_ref(), b"public/foo");

        let upload = alt_client
            .create_multipart_upload()
            .bucket(&dst_bucket)
            .key("copied")
            .send()
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
            .send()
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
            .send()
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

        let response = alt_client
            .get_object()
            .bucket(&dst_bucket)
            .key("copied")
            .send()
            .await
            .unwrap();
        let body = response.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), b"public/foo");

        let response = alt_client
            .get_object()
            .bucket(&dst_bucket)
            .key("copied2")
            .send()
            .await
            .unwrap();
        let body = response.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), b"public/bar");

        cleanup_with_client(
            alt_client,
            &dst_bucket,
            &["copied", "copied2", "copied-denied"],
        )
        .await;
        cleanup(&src_bucket, &["public/foo", "public/bar", "private/foo"]).await;
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
fn test_bucket_policy_get_bucket_acl_requires_dedicated_action() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let denied = alt_client.get_bucket_acl().bucket(&bucket).send().await;
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

        let denied = alt_client.head_bucket().bucket(&bucket).send().await;
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

        let location = eventually_ok_with_retry(
            "GetBucketLocation allowed with GetBucketLocation policy",
            60,
            std::time::Duration::from_millis(500),
            || alt_client.get_bucket_location().bucket(&bucket).send(),
        )
        .await;
        assert_eq!(
            location.location_constraint().map(|v| v.as_str()),
            expected_bucket_location_constraint_for_sdk(CTX.region())
        );

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
