use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    AbacStatus, BlockedEncryptionTypes, BucketAbacStatus, BucketCannedAcl,
    BucketLifecycleConfiguration, BucketVersioningStatus, CompletedMultipartUpload, CompletedPart,
    CorsConfiguration, CorsRule, DefaultRetention, EncryptionType, ExpirationStatus, Grant,
    LifecycleExpiration, LifecycleRule, LifecycleRuleFilter, ObjectAttributes,
    ObjectLockConfiguration, ObjectLockEnabled, ObjectLockRetentionMode, ObjectLockRule,
    ObjectOwnership, OwnershipControls, OwnershipControlsRule, Permission,
    PublicAccessBlockConfiguration, ServerSideEncryption, ServerSideEncryptionByDefault,
    ServerSideEncryptionConfiguration, ServerSideEncryptionRule, Tag, Tagging,
    VersioningConfiguration,
};
use s3_tests::{
    assert_s3_err_code, cleanup_versioned_bucket, disable_bucket_public_access_block, err_status,
    put_bucket_lifecycle_with_md5, send_signed_request_to_endpoint_for_service_with_credentials,
    unique_bucket, SendRetryingOperationAborted, SignedRequestCredentials, CTX,
};
use serde_json::json;
use std::future::Future;

fn expected_bucket_location_constraint_for_sdk(region: &str) -> Option<&str> {
    match region {
        "us-east-1" => Some(""),
        "eu-west-1" => Some("EU"),
        other => Some(other),
    }
}
async fn cleanup_with_client(client: &aws_sdk_s3::Client, bucket: &str, keys: &[&str]) {
    for key in keys {
        let _ = s3_tests::delete_object_retrying_operation_aborted(client, bucket, *key).await;
    }

    for _ in 0..10 {
        let uploads = client
            .list_multipart_uploads()
            .bucket(bucket)
            .send_retrying_operation_aborted("list multipart uploads during ABAC cleanup")
            .await
            .unwrap();
        for upload in uploads.uploads() {
            let _ = client
                .abort_multipart_upload()
                .bucket(bucket)
                .key(upload.key().unwrap())
                .upload_id(upload.upload_id().unwrap())
                .send_retrying_operation_aborted("abort multipart upload during ABAC cleanup")
                .await;
        }

        match client
            .delete_bucket()
            .bucket(bucket)
            .send_retrying_operation_aborted("delete ABAC policy bucket")
            .await
        {
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

    s3_tests::delete_bucket_retrying_operation_aborted(client, bucket).await;
}

async fn cleanup(bucket: &str, keys: &[&str]) {
    cleanup_with_client(CTX.client(), bucket, keys).await;
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
async fn alt_get_object_access_denied_eventually(bucket: &str, key: &str) {
    // After a successful GetObject, AWS can keep honoring the old bucket-tag
    // decision for tens of seconds after TagResource has made the new tag
    // visible via GetBucketTagging. This pins the warm-read revocation path.
    const MAX_ATTEMPTS: usize = 400;

    for attempt in 0..MAX_ATTEMPTS {
        let result = CTX
            .alt_client()
            .get_object()
            .bucket(bucket)
            .key(key)
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
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            continue;
        }
        panic!(
            "GetObject did not converge to AccessDenied for bucket {bucket} key {key}: {result:?}"
        );
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

fn enabled_abac_status() -> AbacStatus {
    AbacStatus::builder()
        .status(BucketAbacStatus::Enabled)
        .build()
}

async fn enable_bucket_abac_with_security_tag(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    value: &str,
) {
    client
        .put_bucket_tagging()
        .bucket(bucket)
        .tagging(simple_bucket_tagging("security", value))
        .send()
        .await
        .unwrap();
    client
        .put_bucket_abac()
        .bucket(bucket)
        .abac_status(enabled_abac_status())
        .send()
        .await
        .unwrap();
}

async fn put_bucket_tag_condition_policy_for_alt(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    action: &str,
) {
    client
        .put_bucket_policy()
        .bucket(bucket)
        .policy(
            json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": alt_policy_principal(),
                    "Action": action,
                    "Resource": bucket_resource(bucket),
                    "Condition": {
                        "StringEquals": {
                            "s3:BucketTag/security": "public"
                        }
                    }
                }],
            })
            .to_string(),
        )
        .send()
        .await
        .unwrap();
}

async fn create_bucket_pair_allowing_bucket_tag_action(action: &str) -> (String, String) {
    let client = CTX.client();

    let public_bucket = create_bucket_allowing_public_policy(client).await;
    enable_bucket_abac_with_security_tag(client, &public_bucket, "public").await;
    put_bucket_tag_condition_policy_for_alt(client, &public_bucket, action).await;

    let private_bucket = create_bucket_allowing_public_policy(client).await;
    enable_bucket_abac_with_security_tag(client, &private_bucket, "private").await;
    put_bucket_tag_condition_policy_for_alt(client, &private_bucket, action).await;

    (public_bucket, private_bucket)
}

async fn create_versioned_bucket_allowing_public_policy(client: &aws_sdk_s3::Client) -> String {
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
    bucket
}

async fn put_versioned_object(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    body: &'static [u8],
) -> String {
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(body))
        .send()
        .await
        .unwrap()
        .version_id()
        .expect("expected VersionId for versioned object")
        .to_string()
}

fn bucket_resource_arn(bucket: &str) -> String {
    format!("arn:aws:s3:::{bucket}")
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

async fn eventually_get_object_succeeds(
    description: &str,
    mut op: impl FnMut() -> aws_sdk_s3::operation::get_object::builders::GetObjectFluentBuilder,
) {
    const MAX_ATTEMPTS: usize = 20;

    for attempt in 0..MAX_ATTEMPTS {
        match op()
            .send_retrying_operation_aborted("eventual ABAC get object")
            .await
        {
            Ok(_) => return,
            Err(_) if attempt + 1 < MAX_ATTEMPTS => {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            Err(err) => panic!("{description} failed unexpectedly: {err:?}"),
        }
    }

    unreachable!()
}

fn bucket_abac_connect_endpoint() -> String {
    if CTX.tls_ca_pem().is_some() || !CTX.endpoint().contains("amazonaws.com") {
        CTX.endpoint().to_string()
    } else {
        bucket_abac_control_endpoint()
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

fn tag_resource_body(tags: &[(&str, &str)]) -> String {
    let mut body =
        String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?><TagResourceRequest xmlns=\"http://awss3control.amazonaws.com/doc/2018-08-20/\"><Tags>");
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

fn tag_resource(bucket: &str, tags: &[(&str, &str)]) -> s3_tests::RawResponse {
    let endpoint = bucket_abac_control_endpoint();
    let connect_endpoint = bucket_abac_connect_endpoint();
    let resource = percent_encode_path_segment(&bucket_resource_arn(bucket));
    let signed_url = format!("{endpoint}/v20180820/tags/{resource}");
    let connect_url = format!("{connect_endpoint}/v20180820/tags/{resource}");
    send_signed_request_to_endpoint_for_service_with_credentials(
        "POST",
        &connect_url,
        &signed_url,
        tag_resource_body(tags).as_bytes(),
        [("x-amz-account-id", CTX.account_id())],
        "s3",
        raw_primary_credentials(),
    )
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
fn test_bucket_policy_get_bucket_tagging_bucket_tag_condition() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_bucket_allowing_sse_c(client).await;
        client
            .put_bucket_tagging()
            .bucket(&bucket)
            .tagging(simple_bucket_tagging("security", "public"))
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
                        "Action": "s3:GetBucketTagging",
                        "Resource": bucket_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
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
            "GetBucketTagging denied for public bucket tag while bucket ABAC is disabled",
            || alt_client.get_bucket_tagging().bucket(&bucket).send(),
        )
        .await;

        client
            .put_bucket_tagging()
            .bucket(&bucket)
            .tagging(simple_bucket_tagging("security", "private"))
            .send()
            .await
            .unwrap();

        eventually_access_denied(
            "GetBucketTagging denied for private bucket tag while bucket ABAC is disabled",
            || alt_client.get_bucket_tagging().bucket(&bucket).send(),
        )
        .await;

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_get_object_bucket_tag_condition() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_bucket_allowing_sse_c(client).await;
        let key = "bucket-tag-get";
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"bucket-tag-body"))
            .send()
            .await
            .unwrap();
        client
            .put_bucket_tagging()
            .bucket(&bucket)
            .tagging(simple_bucket_tagging("security", "public"))
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
                        "Action": "s3:GetObject",
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
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
            "GetObject denied for public bucket tag while bucket ABAC is disabled",
            || alt_client.get_object().bucket(&bucket).key(key).send(),
        )
        .await;

        client
            .put_bucket_tagging()
            .bucket(&bucket)
            .tagging(simple_bucket_tagging("security", "private"))
            .send()
            .await
            .unwrap();

        eventually_access_denied(
            "GetObject denied for private bucket tag while bucket ABAC is disabled",
            || alt_client.get_object().bucket(&bucket).key(key).send(),
        )
        .await;

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_put_object_bucket_tag_condition() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_bucket_allowing_sse_c(client).await;
        client
            .put_bucket_tagging()
            .bucket(&bucket)
            .tagging(simple_bucket_tagging("security", "public"))
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
                        "Action": "s3:PutObject",
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
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
            "PutObject denied for public bucket tag while bucket ABAC is disabled",
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("bucket-tag-put-public")
                    .body(ByteStream::from_static(b"public"))
                    .send()
            },
        )
        .await;

        client
            .put_bucket_tagging()
            .bucket(&bucket)
            .tagging(simple_bucket_tagging("security", "private"))
            .send()
            .await
            .unwrap();

        eventually_access_denied(
            "PutObject denied for private bucket tag while bucket ABAC is disabled",
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("bucket-tag-put-private")
                    .body(ByteStream::from_static(b"private"))
                    .send()
            },
        )
        .await;

        cleanup(
            &bucket,
            &["bucket-tag-put-public", "bucket-tag-put-private"],
        )
        .await;
    });
}

#[test]
fn test_bucket_policy_get_object_bucket_tag_deny_condition_when_abac_disabled() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_bucket_allowing_sse_c(client).await;
        let key = "bucket-tag-deny-disabled";
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"bucket-tag-body"))
            .send()
            .await
            .unwrap();
        client
            .put_bucket_tagging()
            .bucket(&bucket)
            .tagging(simple_bucket_tagging("security", "private"))
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
                            "Principal": principal,
                            "Action": "s3:GetObject",
                            "Resource": bucket_wildcard_resource(&bucket),
                        },
                        {
                            "Effect": "Deny",
                            "Principal": principal,
                            "Action": "s3:GetObject",
                            "Resource": bucket_wildcard_resource(&bucket),
                            "Condition": {
                                "StringEquals": {
                                    "s3:BucketTag/security": "private"
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

        eventually_get_object_succeeds(
            "GetObject unexpectedly denied by bucket-tag deny while ABAC is disabled",
            || alt_client.get_object().bucket(&bucket).key(key),
        )
        .await;

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_get_bucket_tagging_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_bucket_allowing_sse_c(client).await;
        client
            .put_bucket_tagging()
            .bucket(&bucket)
            .tagging(simple_bucket_tagging("security", "public"))
            .send()
            .await
            .unwrap();
        client
            .put_bucket_abac()
            .bucket(&bucket)
            .abac_status(enabled_abac_status())
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
                        "Action": "s3:GetBucketTagging",
                        "Resource": bucket_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let response = eventually_ok(
            "GetBucketTagging allowed for public bucket tag when ABAC is enabled",
            || alt_client.get_bucket_tagging().bucket(&bucket).send(),
        )
        .await;
        assert_eq!(response.tag_set().len(), 1);
        assert_eq!(response.tag_set()[0].key(), "security");
        assert_eq!(response.tag_set()[0].value(), "public");

        let tag = tag_resource(&bucket, &[("security", "private")]);
        assert_eq!(tag.status, 204, "TagResource failed: {:?}", tag);
        let get = eventually_ok("Owner GetBucketTagging after TagResource private", || {
            client.get_bucket_tagging().bucket(&bucket).send()
        })
        .await;
        assert_eq!(get.tag_set().len(), 1);
        assert_eq!(get.tag_set()[0].key(), "security");
        assert_eq!(get.tag_set()[0].value(), "private");

        eventually_access_denied(
            "GetBucketTagging denied for private bucket tag when ABAC is enabled",
            || alt_client.get_bucket_tagging().bucket(&bucket).send(),
        )
        .await;

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_get_bucket_tagging_resource_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_bucket_allowing_sse_c(client).await;
        client
            .put_bucket_tagging()
            .bucket(&bucket)
            .tagging(simple_bucket_tagging("security", "allow"))
            .send()
            .await
            .unwrap();
        client
            .put_bucket_abac()
            .bucket(&bucket)
            .abac_status(enabled_abac_status())
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
                        "Action": "s3:GetBucketTagging",
                        "Resource": bucket_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "aws:ResourceTag/security": "allow"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let response = eventually_ok(
            "GetBucketTagging allowed by aws:ResourceTag/security=allow",
            || alt_client.get_bucket_tagging().bucket(&bucket).send(),
        )
        .await;
        assert_eq!(response.tag_set().len(), 1);
        assert_eq!(response.tag_set()[0].key(), "security");
        assert_eq!(response.tag_set()[0].value(), "allow");

        let tag = tag_resource(&bucket, &[("security", "deny")]);
        assert_eq!(tag.status, 204, "TagResource failed: {:?}", tag);

        eventually_access_denied(
            "GetBucketTagging denied after aws:ResourceTag/security changes to deny",
            || alt_client.get_bucket_tagging().bucket(&bucket).send(),
        )
        .await;

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_list_bucket_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_bucket_allowing_sse_c(client).await;
        client
            .put_bucket_tagging()
            .bucket(&bucket)
            .tagging(simple_bucket_tagging("security", "public"))
            .send()
            .await
            .unwrap();
        client
            .put_bucket_abac()
            .bucket(&bucket)
            .abac_status(enabled_abac_status())
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
                        "Action": "s3:ListBucket",
                        "Resource": bucket_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
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
            "ListBucket allowed for public bucket tag when ABAC is enabled",
            || alt_client.list_objects_v2().bucket(&bucket).send(),
        )
        .await;

        let tag = tag_resource(&bucket, &[("security", "private")]);
        assert_eq!(tag.status, 204, "TagResource failed: {:?}", tag);
        let get = eventually_ok("Owner GetBucketTagging after TagResource private", || {
            client.get_bucket_tagging().bucket(&bucket).send()
        })
        .await;
        assert_eq!(get.tag_set().len(), 1);
        assert_eq!(get.tag_set()[0].key(), "security");
        assert_eq!(get.tag_set()[0].value(), "private");

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_get_object_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_bucket_allowing_sse_c(client).await;
        let key = "bucket-tag-get-enabled";
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"bucket-tag-body"))
            .send()
            .await
            .unwrap();
        client
            .put_bucket_tagging()
            .bucket(&bucket)
            .tagging(simple_bucket_tagging("security", "public"))
            .send()
            .await
            .unwrap();
        client
            .put_bucket_abac()
            .bucket(&bucket)
            .abac_status(enabled_abac_status())
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
                        "Action": "s3:GetObject",
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let response = eventually_ok(
            "GetObject allowed for public bucket tag when ABAC is enabled",
            || alt_client.get_object().bucket(&bucket).key(key).send(),
        )
        .await;
        assert_eq!(response.content_length(), Some(15));

        let tag = tag_resource(&bucket, &[("security", "private")]);
        assert_eq!(tag.status, 204, "TagResource failed: {:?}", tag);
        let get = eventually_ok("Owner GetBucketTagging after TagResource private", || {
            client.get_bucket_tagging().bucket(&bucket).send()
        })
        .await;
        assert_eq!(get.tag_set().len(), 1);
        assert_eq!(get.tag_set()[0].key(), "security");
        assert_eq!(get.tag_set()[0].value(), "private");

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_get_object_bucket_tag_revocation_when_abac_enabled() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_bucket_allowing_sse_c(client).await;
        let key = "bucket-tag-get-revocation";
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"bucket-tag-body"))
            .send()
            .await
            .unwrap();
        client
            .put_bucket_tagging()
            .bucket(&bucket)
            .tagging(simple_bucket_tagging("security", "public"))
            .send()
            .await
            .unwrap();
        client
            .put_bucket_abac()
            .bucket(&bucket)
            .abac_status(enabled_abac_status())
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
                        "Action": "s3:GetObject",
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        {
            let response = eventually_ok(
                "GetObject allowed for public bucket tag before revocation",
                || alt_client.get_object().bucket(&bucket).key(key).send(),
            )
            .await;
            assert_eq!(response.content_length(), Some(15));
        }

        let tag = tag_resource(&bucket, &[("security", "private")]);
        assert_eq!(tag.status, 204, "TagResource failed: {:?}", tag);
        let get = eventually_ok("Owner GetBucketTagging after TagResource private", || {
            client.get_bucket_tagging().bucket(&bucket).send()
        })
        .await;
        assert_eq!(get.tag_set().len(), 1);
        assert_eq!(get.tag_set()[0].key(), "security");
        assert_eq!(get.tag_set()[0].value(), "private");

        alt_get_object_access_denied_eventually(&bucket, key).await;

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_put_object_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_bucket_allowing_sse_c(client).await;
        client
            .put_bucket_tagging()
            .bucket(&bucket)
            .tagging(simple_bucket_tagging("security", "public"))
            .send()
            .await
            .unwrap();
        client
            .put_bucket_abac()
            .bucket(&bucket)
            .abac_status(enabled_abac_status())
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
                        "Action": "s3:PutObject",
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
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
            "PutObject allowed for public bucket tag when ABAC is enabled",
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("bucket-tag-put-public-enabled")
                    .body(ByteStream::from_static(b"public"))
                    .send()
            },
        )
        .await;

        let tag = tag_resource(&bucket, &[("security", "private")]);
        assert_eq!(tag.status, 204, "TagResource failed: {:?}", tag);
        let get = eventually_ok("Owner GetBucketTagging after TagResource private", || {
            client.get_bucket_tagging().bucket(&bucket).send()
        })
        .await;
        assert_eq!(get.tag_set().len(), 1);
        assert_eq!(get.tag_set()[0].key(), "security");
        assert_eq!(get.tag_set()[0].value(), "private");

        eventually_access_denied(
            "PutObject denied for private bucket tag when ABAC is enabled",
            || {
                alt_client
                    .put_object()
                    .bucket(&bucket)
                    .key("bucket-tag-put-private-enabled")
                    .body(ByteStream::from_static(b"private"))
                    .send()
            },
        )
        .await;

        cleanup(
            &bucket,
            &[
                "bucket-tag-put-public-enabled",
                "bucket-tag-put-private-enabled",
            ],
        )
        .await;
    });
}

#[test]
fn test_bucket_policy_create_bucket_bucket_tag_condition_is_rejected() {
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
                        "Action": "s3:CreateBucket",
                        "Resource": bucket_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await;

        cleanup(&bucket, &[]).await;

        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "MalformedPolicy");
    });
}

#[test]
fn test_bucket_policy_delete_bucket_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let public_bucket = create_bucket_allowing_public_policy(client).await;
        enable_bucket_abac_with_security_tag(client, &public_bucket, "public").await;
        let public_policy = client
            .put_bucket_policy()
            .bucket(&public_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:DeleteBucket",
                        "Resource": bucket_resource(&public_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await;

        let private_bucket = create_bucket_allowing_public_policy(client).await;
        enable_bucket_abac_with_security_tag(client, &private_bucket, "private").await;
        let private_policy = client
            .put_bucket_policy()
            .bucket(&private_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:DeleteBucket",
                        "Resource": bucket_resource(&private_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await;

        public_policy.unwrap();
        private_policy.unwrap();

        eventually_ok(
            "DeleteBucket allowed for public bucket tag when ABAC is enabled",
            || alt_client.delete_bucket().bucket(&public_bucket).send(),
        )
        .await;

        eventually_access_denied(
            "DeleteBucket denied for private bucket tag when ABAC is enabled",
            || alt_client.delete_bucket().bucket(&private_bucket).send(),
        )
        .await;

        cleanup(&private_bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_get_bucket_policy_status_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let alt_client = CTX.alt_client();
        let (public_bucket, private_bucket) =
            create_bucket_pair_allowing_bucket_tag_action("s3:GetBucketPolicyStatus").await;

        let public = eventually_ok(
            "GetBucketPolicyStatus allowed for public bucket tag when ABAC is enabled",
            || {
                alt_client
                    .get_bucket_policy_status()
                    .bucket(&public_bucket)
                    .send()
            },
        )
        .await;
        assert_eq!(
            public.policy_status().and_then(|status| status.is_public()),
            Some(false)
        );

        eventually_access_denied(
            "GetBucketPolicyStatus denied for private bucket tag when ABAC is enabled",
            || {
                alt_client
                    .get_bucket_policy_status()
                    .bucket(&private_bucket)
                    .send()
            },
        )
        .await;

        cleanup(&public_bucket, &[]).await;
        cleanup(&private_bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_get_bucket_acl_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let alt_client = CTX.alt_client();
        let (public_bucket, private_bucket) =
            create_bucket_pair_allowing_bucket_tag_action("s3:GetBucketAcl").await;

        let acl = eventually_ok(
            "GetBucketAcl allowed for public bucket tag when ABAC is enabled",
            || alt_client.get_bucket_acl().bucket(&public_bucket).send(),
        )
        .await;
        assert!(acl.owner().is_some(), "expected owner in GetBucketAcl");

        eventually_access_denied(
            "GetBucketAcl denied for private bucket tag when ABAC is enabled",
            || alt_client.get_bucket_acl().bucket(&private_bucket).send(),
        )
        .await;

        cleanup(&public_bucket, &[]).await;
        cleanup(&private_bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_put_bucket_acl_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let alt_client = CTX.alt_client();
        let (public_bucket, private_bucket) =
            create_bucket_pair_allowing_bucket_tag_action("s3:PutBucketAcl").await;
        set_object_writer_ownership(&public_bucket).await;
        set_object_writer_ownership(&private_bucket).await;

        eventually_ok(
            "PutBucketAcl allowed for public bucket tag when ABAC is enabled",
            || {
                alt_client
                    .put_bucket_acl()
                    .bucket(&public_bucket)
                    .acl(BucketCannedAcl::Private)
                    .send()
            },
        )
        .await;

        eventually_access_denied(
            "PutBucketAcl denied for private bucket tag when ABAC is enabled",
            || {
                alt_client
                    .put_bucket_acl()
                    .bucket(&private_bucket)
                    .acl(BucketCannedAcl::Private)
                    .send()
            },
        )
        .await;

        cleanup(&public_bucket, &[]).await;
        cleanup(&private_bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_get_bucket_versioning_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let alt_client = CTX.alt_client();
        let (public_bucket, private_bucket) =
            create_bucket_pair_allowing_bucket_tag_action("s3:GetBucketVersioning").await;

        let versioning = eventually_ok(
            "GetBucketVersioning allowed for public bucket tag when ABAC is enabled",
            || {
                alt_client
                    .get_bucket_versioning()
                    .bucket(&public_bucket)
                    .send()
            },
        )
        .await;
        assert!(
            versioning.status().is_none(),
            "expected unversioned bucket to report no status"
        );

        eventually_access_denied(
            "GetBucketVersioning denied for private bucket tag when ABAC is enabled",
            || {
                alt_client
                    .get_bucket_versioning()
                    .bucket(&private_bucket)
                    .send()
            },
        )
        .await;

        cleanup(&public_bucket, &[]).await;
        cleanup(&private_bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_put_bucket_versioning_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let (public_bucket, private_bucket) =
            create_bucket_pair_allowing_bucket_tag_action("s3:PutBucketVersioning").await;

        eventually_ok(
            "PutBucketVersioning allowed for public bucket tag when ABAC is enabled",
            || {
                alt_client
                    .put_bucket_versioning()
                    .bucket(&public_bucket)
                    .versioning_configuration(
                        VersioningConfiguration::builder()
                            .status(BucketVersioningStatus::Enabled)
                            .build(),
                    )
                    .send()
            },
        )
        .await;

        eventually_access_denied(
            "PutBucketVersioning denied for private bucket tag when ABAC is enabled",
            || {
                alt_client
                    .put_bucket_versioning()
                    .bucket(&private_bucket)
                    .versioning_configuration(
                        VersioningConfiguration::builder()
                            .status(BucketVersioningStatus::Enabled)
                            .build(),
                    )
                    .send()
            },
        )
        .await;

        let public_versioning = client
            .get_bucket_versioning()
            .bucket(&public_bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(
            public_versioning.status(),
            Some(&BucketVersioningStatus::Enabled)
        );

        cleanup_versioned_bucket(client, &public_bucket).await;
        cleanup(&private_bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_list_bucket_versions_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let (public_bucket, private_bucket) =
            create_bucket_pair_allowing_bucket_tag_action("s3:ListBucketVersions").await;

        for bucket in [&public_bucket, &private_bucket] {
            client
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
            client
                .put_object()
                .bucket(bucket)
                .key("key")
                .body(ByteStream::from_static(b"one"))
                .send()
                .await
                .unwrap();
            client
                .put_object()
                .bucket(bucket)
                .key("key")
                .body(ByteStream::from_static(b"two"))
                .send()
                .await
                .unwrap();
        }

        let versions = eventually_ok(
            "ListBucketVersions allowed for public bucket tag when ABAC is enabled",
            || {
                alt_client
                    .list_object_versions()
                    .bucket(&public_bucket)
                    .send()
            },
        )
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

        eventually_access_denied(
            "ListBucketVersions denied for private bucket tag when ABAC is enabled",
            || {
                alt_client
                    .list_object_versions()
                    .bucket(&private_bucket)
                    .send()
            },
        )
        .await;

        cleanup_versioned_bucket(client, &public_bucket).await;
        cleanup_versioned_bucket(client, &private_bucket).await;
    });
}

#[test]
fn test_bucket_policy_list_bucket_multipart_uploads_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let (public_bucket, private_bucket) =
            create_bucket_pair_allowing_bucket_tag_action("s3:ListBucketMultipartUploads").await;

        let public_upload = client
            .create_multipart_upload()
            .bucket(&public_bucket)
            .key("multipart-key")
            .send()
            .await
            .unwrap();
        let private_upload = client
            .create_multipart_upload()
            .bucket(&private_bucket)
            .key("multipart-key")
            .send()
            .await
            .unwrap();

        let uploads = eventually_ok(
            "ListBucketMultipartUploads allowed for public bucket tag when ABAC is enabled",
            || {
                alt_client
                    .list_multipart_uploads()
                    .bucket(&public_bucket)
                    .send()
            },
        )
        .await;
        assert!(uploads
            .uploads()
            .iter()
            .any(|upload| upload.key() == Some("multipart-key")));

        eventually_access_denied(
            "ListBucketMultipartUploads denied for private bucket tag when ABAC is enabled",
            || {
                alt_client
                    .list_multipart_uploads()
                    .bucket(&private_bucket)
                    .send()
            },
        )
        .await;

        client
            .abort_multipart_upload()
            .bucket(&public_bucket)
            .key("multipart-key")
            .upload_id(public_upload.upload_id().unwrap())
            .send()
            .await
            .unwrap();
        client
            .abort_multipart_upload()
            .bucket(&private_bucket)
            .key("multipart-key")
            .upload_id(private_upload.upload_id().unwrap())
            .send()
            .await
            .unwrap();
        cleanup(&public_bucket, &[]).await;
        cleanup(&private_bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_get_bucket_lifecycle_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let (public_bucket, private_bucket) =
            create_bucket_pair_allowing_bucket_tag_action("s3:GetLifecycleConfiguration").await;

        put_bucket_lifecycle_with_md5(
            client,
            &public_bucket,
            simple_lifecycle_configuration("logs/", 30),
        )
        .send()
        .await
        .unwrap();
        put_bucket_lifecycle_with_md5(
            client,
            &private_bucket,
            simple_lifecycle_configuration("logs/", 30),
        )
        .send()
        .await
        .unwrap();

        let response = eventually_ok(
            "GetLifecycleConfiguration allowed for public bucket tag when ABAC is enabled",
            || {
                alt_client
                    .get_bucket_lifecycle_configuration()
                    .bucket(&public_bucket)
                    .send()
            },
        )
        .await;
        assert_eq!(response.rules().len(), 1);
        assert_eq!(response.rules()[0].id(), Some("expire-current"));

        eventually_access_denied(
            "GetLifecycleConfiguration denied for private bucket tag when ABAC is enabled",
            || {
                alt_client
                    .get_bucket_lifecycle_configuration()
                    .bucket(&private_bucket)
                    .send()
            },
        )
        .await;

        cleanup(&public_bucket, &[]).await;
        cleanup(&private_bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_put_bucket_lifecycle_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let (public_bucket, private_bucket) =
            create_bucket_pair_allowing_bucket_tag_action("s3:PutLifecycleConfiguration").await;

        eventually_ok(
            "PutLifecycleConfiguration allowed for public bucket tag when ABAC is enabled",
            || {
                put_bucket_lifecycle_with_md5(
                    alt_client,
                    &public_bucket,
                    simple_lifecycle_configuration("archive/", 14),
                )
                .send()
            },
        )
        .await;

        eventually_access_denied(
            "PutLifecycleConfiguration denied for private bucket tag when ABAC is enabled",
            || {
                put_bucket_lifecycle_with_md5(
                    alt_client,
                    &private_bucket,
                    simple_lifecycle_configuration("archive/", 14),
                )
                .send()
            },
        )
        .await;

        let read_back = client
            .get_bucket_lifecycle_configuration()
            .bucket(&public_bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(read_back.rules().len(), 1);
        assert_eq!(
            read_back.rules()[0].filter().and_then(|f| f.prefix()),
            Some("archive/")
        );

        cleanup(&public_bucket, &[]).await;
        cleanup(&private_bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_get_bucket_ownership_controls_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let (public_bucket, private_bucket) =
            create_bucket_pair_allowing_bucket_tag_action("s3:GetBucketOwnershipControls").await;

        for bucket in [&public_bucket, &private_bucket] {
            client
                .put_bucket_ownership_controls()
                .bucket(bucket)
                .ownership_controls(simple_bucket_ownership_controls(
                    ObjectOwnership::BucketOwnerPreferred,
                ))
                .send()
                .await
                .unwrap();
        }

        let response = eventually_ok(
            "GetBucketOwnershipControls allowed for public bucket tag when ABAC is enabled",
            || {
                alt_client
                    .get_bucket_ownership_controls()
                    .bucket(&public_bucket)
                    .send()
            },
        )
        .await;
        assert_eq!(
            response.ownership_controls().unwrap().rules()[0].object_ownership,
            ObjectOwnership::BucketOwnerPreferred
        );

        eventually_access_denied(
            "GetBucketOwnershipControls denied for private bucket tag when ABAC is enabled",
            || {
                alt_client
                    .get_bucket_ownership_controls()
                    .bucket(&private_bucket)
                    .send()
            },
        )
        .await;

        cleanup(&public_bucket, &[]).await;
        cleanup(&private_bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_put_bucket_ownership_controls_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let (public_bucket, private_bucket) =
            create_bucket_pair_allowing_bucket_tag_action("s3:PutBucketOwnershipControls").await;

        eventually_ok(
            "PutBucketOwnershipControls allowed for public bucket tag when ABAC is enabled",
            || {
                alt_client
                    .put_bucket_ownership_controls()
                    .bucket(&public_bucket)
                    .ownership_controls(simple_bucket_ownership_controls(
                        ObjectOwnership::BucketOwnerPreferred,
                    ))
                    .send()
            },
        )
        .await;

        eventually_access_denied(
            "PutBucketOwnershipControls denied for private bucket tag when ABAC is enabled",
            || {
                alt_client
                    .put_bucket_ownership_controls()
                    .bucket(&private_bucket)
                    .ownership_controls(simple_bucket_ownership_controls(
                        ObjectOwnership::BucketOwnerPreferred,
                    ))
                    .send()
            },
        )
        .await;

        let read_back = client
            .get_bucket_ownership_controls()
            .bucket(&public_bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(
            read_back.ownership_controls().unwrap().rules()[0].object_ownership,
            ObjectOwnership::BucketOwnerPreferred
        );

        cleanup(&public_bucket, &[]).await;
        cleanup(&private_bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_get_bucket_encryption_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let (public_bucket, private_bucket) =
            create_bucket_pair_allowing_bucket_tag_action("s3:GetEncryptionConfiguration").await;

        for bucket in [&public_bucket, &private_bucket] {
            client
                .put_bucket_encryption()
                .bucket(bucket)
                .server_side_encryption_configuration(simple_bucket_encryption(
                    EncryptionType::SseC,
                ))
                .send()
                .await
                .unwrap();
        }

        let response = eventually_ok(
            "GetEncryptionConfiguration allowed for public bucket tag when ABAC is enabled",
            || {
                alt_client
                    .get_bucket_encryption()
                    .bucket(&public_bucket)
                    .send()
            },
        )
        .await;
        let rules = response
            .server_side_encryption_configuration()
            .unwrap()
            .rules();
        assert_eq!(
            blocked_encryption_types(&rules[0]),
            vec!["SSE-C".to_string()]
        );

        eventually_access_denied(
            "GetEncryptionConfiguration denied for private bucket tag when ABAC is enabled",
            || {
                alt_client
                    .get_bucket_encryption()
                    .bucket(&private_bucket)
                    .send()
            },
        )
        .await;

        cleanup(&public_bucket, &[]).await;
        cleanup(&private_bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_put_bucket_encryption_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let (public_bucket, private_bucket) =
            create_bucket_pair_allowing_bucket_tag_action("s3:PutEncryptionConfiguration").await;

        eventually_ok(
            "PutEncryptionConfiguration allowed for public bucket tag when ABAC is enabled",
            || {
                alt_client
                    .put_bucket_encryption()
                    .bucket(&public_bucket)
                    .server_side_encryption_configuration(simple_bucket_encryption(
                        EncryptionType::SseC,
                    ))
                    .send()
            },
        )
        .await;

        eventually_access_denied(
            "PutEncryptionConfiguration denied for private bucket tag when ABAC is enabled",
            || {
                alt_client
                    .put_bucket_encryption()
                    .bucket(&private_bucket)
                    .server_side_encryption_configuration(simple_bucket_encryption(
                        EncryptionType::SseC,
                    ))
                    .send()
            },
        )
        .await;

        let read_back = client
            .get_bucket_encryption()
            .bucket(&public_bucket)
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

        cleanup(&public_bucket, &[]).await;
        cleanup(&private_bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_get_bucket_location_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let alt_client = CTX.alt_client();
        let (public_bucket, private_bucket) =
            create_bucket_pair_allowing_bucket_tag_action("s3:GetBucketLocation").await;

        let location = eventually_ok(
            "GetBucketLocation allowed for public bucket tag when ABAC is enabled",
            || {
                alt_client
                    .get_bucket_location()
                    .bucket(&public_bucket)
                    .send()
            },
        )
        .await;
        assert_eq!(
            location.location_constraint().map(|v| v.as_str()),
            expected_bucket_location_constraint_for_sdk(CTX.region())
        );

        eventually_access_denied(
            "GetBucketLocation denied for private bucket tag when ABAC is enabled",
            || {
                alt_client
                    .get_bucket_location()
                    .bucket(&private_bucket)
                    .send()
            },
        )
        .await;

        cleanup(&public_bucket, &[]).await;
        cleanup(&private_bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_get_bucket_cors_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let (public_bucket, private_bucket) =
            create_bucket_pair_allowing_bucket_tag_action("s3:GetBucketCors").await;

        for bucket in [&public_bucket, &private_bucket] {
            client
                .put_bucket_cors()
                .bucket(bucket)
                .cors_configuration(simple_cors_configuration("https://example.com", "GET"))
                .send()
                .await
                .unwrap();
        }

        let response = eventually_ok(
            "GetBucketCors allowed for public bucket tag when ABAC is enabled",
            || alt_client.get_bucket_cors().bucket(&public_bucket).send(),
        )
        .await;
        assert_eq!(response.cors_rules().len(), 1);

        eventually_access_denied(
            "GetBucketCors denied for private bucket tag when ABAC is enabled",
            || alt_client.get_bucket_cors().bucket(&private_bucket).send(),
        )
        .await;

        cleanup(&public_bucket, &[]).await;
        cleanup(&private_bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_put_bucket_cors_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let (public_bucket, private_bucket) =
            create_bucket_pair_allowing_bucket_tag_action("s3:PutBucketCors").await;

        eventually_ok(
            "PutBucketCors allowed for public bucket tag when ABAC is enabled",
            || {
                alt_client
                    .put_bucket_cors()
                    .bucket(&public_bucket)
                    .cors_configuration(simple_cors_configuration("https://example.com", "GET"))
                    .send()
            },
        )
        .await;

        eventually_access_denied(
            "PutBucketCors denied for private bucket tag when ABAC is enabled",
            || {
                alt_client
                    .put_bucket_cors()
                    .bucket(&private_bucket)
                    .cors_configuration(simple_cors_configuration("https://example.com", "GET"))
                    .send()
            },
        )
        .await;

        let read_back = client
            .get_bucket_cors()
            .bucket(&public_bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(read_back.cors_rules().len(), 1);

        cleanup(&public_bucket, &[]).await;
        cleanup(&private_bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_get_bucket_public_access_block_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let (public_bucket, private_bucket) =
            create_bucket_pair_allowing_bucket_tag_action("s3:GetBucketPublicAccessBlock").await;

        for bucket in [&public_bucket, &private_bucket] {
            client
                .put_public_access_block()
                .bucket(bucket)
                .public_access_block_configuration(simple_public_access_block())
                .send()
                .await
                .unwrap();
        }

        let response = eventually_ok(
            "GetBucketPublicAccessBlock allowed for public bucket tag when ABAC is enabled",
            || {
                alt_client
                    .get_public_access_block()
                    .bucket(&public_bucket)
                    .send()
            },
        )
        .await;
        let config = response.public_access_block_configuration().unwrap();
        assert_eq!(config.block_public_acls(), Some(true));

        eventually_access_denied(
            "GetBucketPublicAccessBlock denied for private bucket tag when ABAC is enabled",
            || {
                alt_client
                    .get_public_access_block()
                    .bucket(&private_bucket)
                    .send()
            },
        )
        .await;

        cleanup(&public_bucket, &[]).await;
        cleanup(&private_bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_put_bucket_public_access_block_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let (public_bucket, private_bucket) =
            create_bucket_pair_allowing_bucket_tag_action("s3:PutBucketPublicAccessBlock").await;

        eventually_ok(
            "PutBucketPublicAccessBlock allowed for public bucket tag when ABAC is enabled",
            || {
                alt_client
                    .put_public_access_block()
                    .bucket(&public_bucket)
                    .public_access_block_configuration(simple_public_access_block())
                    .send()
            },
        )
        .await;

        eventually_access_denied(
            "PutBucketPublicAccessBlock denied for private bucket tag when ABAC is enabled",
            || {
                alt_client
                    .put_public_access_block()
                    .bucket(&private_bucket)
                    .public_access_block_configuration(simple_public_access_block())
                    .send()
            },
        )
        .await;

        let read_back = client
            .get_public_access_block()
            .bucket(&public_bucket)
            .send()
            .await
            .unwrap();
        let config = read_back.public_access_block_configuration().unwrap();
        assert_eq!(config.block_public_acls(), Some(true));

        cleanup(&public_bucket, &[]).await;
        cleanup(&private_bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_get_bucket_object_lock_configuration_bucket_tag_condition_when_abac_enabled()
{
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let public_bucket = create_object_lock_bucket(client).await;
        let public_config = simple_object_lock_configuration();
        client
            .put_object_lock_configuration()
            .bucket(&public_bucket)
            .object_lock_configuration(public_config.clone())
            .send()
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(client, &public_bucket, "public").await;
        put_bucket_tag_condition_policy_for_alt(
            client,
            &public_bucket,
            "s3:GetBucketObjectLockConfiguration",
        )
        .await;

        let private_bucket = create_object_lock_bucket(client).await;
        let private_config = simple_object_lock_configuration();
        client
            .put_object_lock_configuration()
            .bucket(&private_bucket)
            .object_lock_configuration(private_config.clone())
            .send()
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(client, &private_bucket, "private").await;
        put_bucket_tag_condition_policy_for_alt(
            client,
            &private_bucket,
            "s3:GetBucketObjectLockConfiguration",
        )
        .await;

        let response = eventually_ok(
            "GetBucketObjectLockConfiguration allowed for public bucket tag when ABAC is enabled",
            || {
                alt_client
                    .get_object_lock_configuration()
                    .bucket(&public_bucket)
                    .send()
            },
        )
        .await;
        assert_eq!(response.object_lock_configuration(), Some(&public_config));

        eventually_access_denied(
            "GetBucketObjectLockConfiguration denied for private bucket tag when ABAC is enabled",
            || {
                alt_client
                    .get_object_lock_configuration()
                    .bucket(&private_bucket)
                    .send()
            },
        )
        .await;

        cleanup(&public_bucket, &[]).await;
        cleanup(&private_bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_put_bucket_object_lock_configuration_bucket_tag_condition_when_abac_enabled()
{
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let public_bucket = create_object_lock_bucket(client).await;
        let public_config = simple_object_lock_configuration();
        enable_bucket_abac_with_security_tag(client, &public_bucket, "public").await;
        put_bucket_tag_condition_policy_for_alt(
            client,
            &public_bucket,
            "s3:PutBucketObjectLockConfiguration",
        )
        .await;

        let private_bucket = create_object_lock_bucket(client).await;
        let private_config = simple_object_lock_configuration();
        enable_bucket_abac_with_security_tag(client, &private_bucket, "private").await;
        put_bucket_tag_condition_policy_for_alt(
            client,
            &private_bucket,
            "s3:PutBucketObjectLockConfiguration",
        )
        .await;

        eventually_ok(
            "PutBucketObjectLockConfiguration allowed for public bucket tag when ABAC is enabled",
            || {
                alt_client
                    .put_object_lock_configuration()
                    .bucket(&public_bucket)
                    .object_lock_configuration(public_config.clone())
                    .send()
            },
        )
        .await;

        eventually_access_denied(
            "PutBucketObjectLockConfiguration denied for private bucket tag when ABAC is enabled",
            || {
                alt_client
                    .put_object_lock_configuration()
                    .bucket(&private_bucket)
                    .object_lock_configuration(private_config.clone())
                    .send()
            },
        )
        .await;

        let read_back = client
            .get_object_lock_configuration()
            .bucket(&public_bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(read_back.object_lock_configuration(), Some(&public_config));

        cleanup(&public_bucket, &[]).await;
        cleanup(&private_bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_head_object_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let key = "bucket-tag-head-enabled";

        let public_bucket = create_bucket_allowing_sse_c(client).await;
        client
            .put_object()
            .bucket(&public_bucket)
            .key(key)
            .body(ByteStream::from_static(b"bucket-tag-body"))
            .send()
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(client, &public_bucket, "public").await;
        client
            .put_bucket_policy()
            .bucket(&public_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal.clone(),
                        "Action": "s3:GetObject",
                        "Resource": bucket_wildcard_resource(&public_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let private_bucket = create_bucket_allowing_sse_c(client).await;
        client
            .put_object()
            .bucket(&private_bucket)
            .key(key)
            .body(ByteStream::from_static(b"bucket-tag-body"))
            .send()
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(client, &private_bucket, "private").await;
        client
            .put_bucket_policy()
            .bucket(&private_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal,
                        "Action": "s3:GetObject",
                        "Resource": bucket_wildcard_resource(&private_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let head = eventually_ok(
            "HeadObject allowed for public bucket tag when ABAC is enabled",
            || {
                alt_client
                    .head_object()
                    .bucket(&public_bucket)
                    .key(key)
                    .send()
            },
        )
        .await;
        assert_eq!(head.content_length(), Some(15));

        eventually_access_denied(
            "HeadObject denied for private bucket tag when ABAC is enabled",
            || {
                alt_client
                    .head_object()
                    .bucket(&private_bucket)
                    .key(key)
                    .send()
            },
        )
        .await;

        cleanup(&public_bucket, &[key]).await;
        cleanup(&private_bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_get_object_attributes_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let key = "bucket-tag-attrs-enabled";

        let public_bucket = create_bucket_allowing_sse_c(client).await;
        client
            .put_object()
            .bucket(&public_bucket)
            .key(key)
            .body(ByteStream::from_static(b"bucket-tag-body"))
            .send()
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(client, &public_bucket, "public").await;
        client
            .put_bucket_policy()
            .bucket(&public_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": principal.clone(),
                            "Action": "s3:GetObject",
                            "Resource": bucket_wildcard_resource(&public_bucket),
                            "Condition": {
                                "StringEquals": {
                                    "s3:BucketTag/security": "public"
                                }
                            }
                        },
                        {
                            "Effect": "Allow",
                            "Principal": principal.clone(),
                            "Action": "s3:GetObjectAttributes",
                            "Resource": bucket_wildcard_resource(&public_bucket)
                        }
                    ],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let private_bucket = create_bucket_allowing_sse_c(client).await;
        client
            .put_object()
            .bucket(&private_bucket)
            .key(key)
            .body(ByteStream::from_static(b"bucket-tag-body"))
            .send()
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(client, &private_bucket, "private").await;
        client
            .put_bucket_policy()
            .bucket(&private_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": principal.clone(),
                            "Action": "s3:GetObject",
                            "Resource": bucket_wildcard_resource(&private_bucket),
                            "Condition": {
                                "StringEquals": {
                                    "s3:BucketTag/security": "public"
                                }
                            }
                        },
                        {
                            "Effect": "Allow",
                            "Principal": principal,
                            "Action": "s3:GetObjectAttributes",
                            "Resource": bucket_wildcard_resource(&private_bucket)
                        }
                    ],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let attrs = eventually_ok(
            "GetObjectAttributes allowed for public bucket tag when ABAC is enabled",
            || {
                alt_client
                    .get_object_attributes()
                    .bucket(&public_bucket)
                    .key(key)
                    .object_attributes(ObjectAttributes::ObjectSize)
                    .send()
            },
        )
        .await;
        assert_eq!(attrs.object_size(), Some(15));

        eventually_access_denied(
            "GetObjectAttributes denied for private bucket tag when ABAC is enabled",
            || {
                alt_client
                    .get_object_attributes()
                    .bucket(&private_bucket)
                    .key(key)
                    .object_attributes(ObjectAttributes::ObjectSize)
                    .send()
            },
        )
        .await;

        cleanup(&public_bucket, &[key]).await;
        cleanup(&private_bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_create_multipart_upload_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let key = "bucket-tag-mpu-create-enabled";

        let public_bucket = create_bucket_allowing_sse_c(client).await;
        enable_bucket_abac_with_security_tag(client, &public_bucket, "public").await;
        client
            .put_bucket_policy()
            .bucket(&public_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal.clone(),
                        "Action": "s3:PutObject",
                        "Resource": bucket_wildcard_resource(&public_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let private_bucket = create_bucket_allowing_sse_c(client).await;
        enable_bucket_abac_with_security_tag(client, &private_bucket, "private").await;
        client
            .put_bucket_policy()
            .bucket(&private_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal,
                        "Action": "s3:PutObject",
                        "Resource": bucket_wildcard_resource(&private_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let upload = eventually_ok(
            "CreateMultipartUpload allowed for public bucket tag when ABAC is enabled",
            || {
                alt_client
                    .create_multipart_upload()
                    .bucket(&public_bucket)
                    .key(key)
                    .send()
            },
        )
        .await;
        alt_client
            .abort_multipart_upload()
            .bucket(&public_bucket)
            .key(key)
            .upload_id(upload.upload_id().unwrap())
            .send()
            .await
            .unwrap();

        eventually_access_denied(
            "CreateMultipartUpload denied for private bucket tag when ABAC is enabled",
            || {
                alt_client
                    .create_multipart_upload()
                    .bucket(&private_bucket)
                    .key(key)
                    .send()
            },
        )
        .await;

        cleanup(&public_bucket, &[]).await;
        cleanup(&private_bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_upload_part_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let key = "bucket-tag-upload-part-enabled";

        let bucket = create_bucket_allowing_sse_c(client).await;
        enable_bucket_abac_with_security_tag(client, &bucket, "public").await;
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal,
                        "Action": "s3:PutObject",
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let upload = eventually_ok(
            "CreateMultipartUpload for UploadPart bucket-tag probe",
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

        let uploaded = eventually_ok(
            "UploadPart allowed for public bucket tag when ABAC is enabled",
            || {
                alt_client
                    .upload_part()
                    .bucket(&bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .part_number(1)
                    .body(ByteStream::from_static(b"bucket-tag-part"))
                    .send()
            },
        )
        .await;
        assert!(uploaded.e_tag().is_some());

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
fn test_bucket_policy_complete_multipart_upload_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let key = "bucket-tag-complete-enabled";

        let bucket = create_bucket_allowing_sse_c(client).await;
        enable_bucket_abac_with_security_tag(client, &bucket, "public").await;
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal,
                        "Action": "s3:PutObject",
                        "Resource": bucket_wildcard_resource(&bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let upload = eventually_ok(
            "CreateMultipartUpload for CompleteMultipartUpload bucket-tag probe",
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
        let uploaded = eventually_ok(
            "UploadPart for CompleteMultipartUpload bucket-tag probe",
            || {
                alt_client
                    .upload_part()
                    .bucket(&bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .part_number(1)
                    .body(ByteStream::from_static(b"bucket-tag-part"))
                    .send()
            },
        )
        .await;

        eventually_ok(
            "CompleteMultipartUpload allowed for public bucket tag when ABAC is enabled",
            || {
                alt_client
                    .complete_multipart_upload()
                    .bucket(&bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .multipart_upload(
                        CompletedMultipartUpload::builder()
                            .parts(
                                CompletedPart::builder()
                                    .part_number(1)
                                    .e_tag(uploaded.e_tag().unwrap())
                                    .build(),
                            )
                            .build(),
                    )
                    .send()
            },
        )
        .await;

        let object = eventually_ok(
            "GetObject after CompleteMultipartUpload bucket-tag probe",
            || client.get_object().bucket(&bucket).key(key).send(),
        )
        .await;
        let body = object.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), b"bucket-tag-part");

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_copy_object_source_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let public_key = "source-public";
        let private_key = "source-private";

        let public_src_bucket = create_bucket_allowing_sse_c(client).await;
        client
            .put_object()
            .bucket(&public_src_bucket)
            .key(public_key)
            .body(ByteStream::from_static(b"public-source"))
            .send()
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(client, &public_src_bucket, "public").await;
        client
            .put_bucket_policy()
            .bucket(&public_src_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal.clone(),
                        "Action": "s3:GetObject",
                        "Resource": bucket_wildcard_resource(&public_src_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let private_src_bucket = create_bucket_allowing_sse_c(client).await;
        client
            .put_object()
            .bucket(&private_src_bucket)
            .key(private_key)
            .body(ByteStream::from_static(b"private-source"))
            .send()
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(client, &private_src_bucket, "private").await;
        client
            .put_bucket_policy()
            .bucket(&private_src_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal,
                        "Action": "s3:GetObject",
                        "Resource": bucket_wildcard_resource(&private_src_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let dst_bucket = unique_bucket();
        s3_tests::create_bucket(alt_client, &dst_bucket)
            .await
            .unwrap();

        eventually_ok(
            "CopyObject source read allowed for public bucket tag when ABAC is enabled",
            || {
                alt_client
                    .copy_object()
                    .bucket(&dst_bucket)
                    .key("copied-public")
                    .copy_source(format!("{public_src_bucket}/{public_key}"))
                    .send()
            },
        )
        .await;

        eventually_access_denied(
            "CopyObject source read denied for private bucket tag when ABAC is enabled",
            || {
                alt_client
                    .copy_object()
                    .bucket(&dst_bucket)
                    .key("copied-private")
                    .copy_source(format!("{private_src_bucket}/{private_key}"))
                    .send()
            },
        )
        .await;

        cleanup_with_client(
            alt_client,
            &dst_bucket,
            &["copied-public", "copied-private"],
        )
        .await;
        cleanup(&public_src_bucket, &[public_key]).await;
        cleanup(&private_src_bucket, &[private_key]).await;
    });
}

#[test]
fn test_bucket_policy_upload_part_copy_source_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let public_key = "source-public";
        let private_key = "source-private";

        let public_src_bucket = create_bucket_allowing_sse_c(client).await;
        client
            .put_object()
            .bucket(&public_src_bucket)
            .key(public_key)
            .body(ByteStream::from_static(b"public-source"))
            .send()
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(client, &public_src_bucket, "public").await;
        client
            .put_bucket_policy()
            .bucket(&public_src_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal.clone(),
                        "Action": "s3:GetObject",
                        "Resource": bucket_wildcard_resource(&public_src_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let private_src_bucket = create_bucket_allowing_sse_c(client).await;
        client
            .put_object()
            .bucket(&private_src_bucket)
            .key(private_key)
            .body(ByteStream::from_static(b"private-source"))
            .send()
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(client, &private_src_bucket, "private").await;
        client
            .put_bucket_policy()
            .bucket(&private_src_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal,
                        "Action": "s3:GetObject",
                        "Resource": bucket_wildcard_resource(&private_src_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let dst_bucket = unique_bucket();
        s3_tests::create_bucket(alt_client, &dst_bucket)
            .await
            .unwrap();
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
            format!("{public_src_bucket}/{public_key}"),
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
            .copy_source(format!("{private_src_bucket}/{private_key}"))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        cleanup_with_client(alt_client, &dst_bucket, &["copied", "copied-denied"]).await;
        cleanup(&public_src_bucket, &[public_key]).await;
        cleanup(&private_src_bucket, &[private_key]).await;
    });
}

#[test]
fn test_bucket_policy_copy_object_destination_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let src_bucket = unique_bucket();
        let src_key = "source";

        s3_tests::create_bucket(alt_client, &src_bucket)
            .await
            .unwrap();
        alt_client
            .put_object()
            .bucket(&src_bucket)
            .key(src_key)
            .body(ByteStream::from_static(b"copy-body"))
            .send()
            .await
            .unwrap();

        let public_dst_bucket = create_bucket_allowing_sse_c(client).await;
        enable_bucket_abac_with_security_tag(client, &public_dst_bucket, "public").await;
        client
            .put_bucket_policy()
            .bucket(&public_dst_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal.clone(),
                        "Action": "s3:PutObject",
                        "Resource": bucket_wildcard_resource(&public_dst_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let private_dst_bucket = create_bucket_allowing_sse_c(client).await;
        enable_bucket_abac_with_security_tag(client, &private_dst_bucket, "private").await;
        client
            .put_bucket_policy()
            .bucket(&private_dst_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal,
                        "Action": "s3:PutObject",
                        "Resource": bucket_wildcard_resource(&private_dst_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
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
            "CopyObject destination write allowed for public bucket tag when ABAC is enabled",
            || {
                alt_client
                    .copy_object()
                    .bucket(&public_dst_bucket)
                    .key("copied-public")
                    .copy_source(format!("{src_bucket}/{src_key}"))
                    .send()
            },
        )
        .await;

        eventually_access_denied(
            "CopyObject destination write denied for private bucket tag when ABAC is enabled",
            || {
                alt_client
                    .copy_object()
                    .bucket(&private_dst_bucket)
                    .key("copied-private")
                    .copy_source(format!("{src_bucket}/{src_key}"))
                    .send()
            },
        )
        .await;

        cleanup_with_client(alt_client, &src_bucket, &[src_key]).await;
        cleanup(&public_dst_bucket, &["copied-public"]).await;
        cleanup(&private_dst_bucket, &["copied-private"]).await;
    });
}

#[test]
fn test_bucket_policy_upload_part_copy_destination_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let src_bucket = unique_bucket();
        let src_key = "source";

        s3_tests::create_bucket(alt_client, &src_bucket)
            .await
            .unwrap();
        alt_client
            .put_object()
            .bucket(&src_bucket)
            .key(src_key)
            .body(ByteStream::from_static(b"copy-body"))
            .send()
            .await
            .unwrap();

        let dst_bucket = create_bucket_allowing_sse_c(client).await;
        enable_bucket_abac_with_security_tag(client, &dst_bucket, "public").await;
        client
            .put_bucket_policy()
            .bucket(&dst_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal,
                        "Action": "s3:PutObject",
                        "Resource": bucket_wildcard_resource(&dst_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let upload = eventually_ok(
            "CreateMultipartUpload for UploadPartCopy destination bucket-tag probe",
            || {
                alt_client
                    .create_multipart_upload()
                    .bucket(&dst_bucket)
                    .key("copied")
                    .send()
            },
        )
        .await;
        let upload_id = upload.upload_id().unwrap().to_string();

        let copied_part = upload_part_copy_eventually(
            alt_client,
            &dst_bucket,
            "copied",
            &upload_id,
            1,
            format!("{src_bucket}/{src_key}"),
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

        cleanup_with_client(alt_client, &src_bucket, &[src_key]).await;
        cleanup(&dst_bucket, &["copied"]).await;
    });
}

#[test]
fn test_bucket_policy_get_object_tagging_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let key = "bucket-tag-get-tagging-enabled";

        let public_bucket = create_bucket_allowing_sse_c(client).await;
        client
            .put_object()
            .bucket(&public_bucket)
            .key(key)
            .body(ByteStream::from_static(b"bucket-tag-body"))
            .send()
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&public_bucket)
            .key(key)
            .tagging(simple_bucket_tagging("object", "public"))
            .send()
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(client, &public_bucket, "public").await;
        client
            .put_bucket_policy()
            .bucket(&public_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal.clone(),
                        "Action": "s3:GetObjectTagging",
                        "Resource": bucket_wildcard_resource(&public_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let private_bucket = create_bucket_allowing_sse_c(client).await;
        client
            .put_object()
            .bucket(&private_bucket)
            .key(key)
            .body(ByteStream::from_static(b"bucket-tag-body"))
            .send()
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&private_bucket)
            .key(key)
            .tagging(simple_bucket_tagging("object", "private"))
            .send()
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(client, &private_bucket, "private").await;
        client
            .put_bucket_policy()
            .bucket(&private_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal,
                        "Action": "s3:GetObjectTagging",
                        "Resource": bucket_wildcard_resource(&private_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let tagging = eventually_ok(
            "GetObjectTagging allowed for public bucket tag when ABAC is enabled",
            || {
                alt_client
                    .get_object_tagging()
                    .bucket(&public_bucket)
                    .key(key)
                    .send()
            },
        )
        .await;
        assert_eq!(tagging.tag_set().len(), 1);
        assert_eq!(tagging.tag_set()[0].key(), "object");
        assert_eq!(tagging.tag_set()[0].value(), "public");

        eventually_access_denied(
            "GetObjectTagging denied for private bucket tag when ABAC is enabled",
            || {
                alt_client
                    .get_object_tagging()
                    .bucket(&private_bucket)
                    .key(key)
                    .send()
            },
        )
        .await;

        cleanup(&public_bucket, &[key]).await;
        cleanup(&private_bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_put_object_tagging_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let key = "bucket-tag-put-tagging-enabled";

        let public_bucket = create_bucket_allowing_sse_c(client).await;
        client
            .put_object()
            .bucket(&public_bucket)
            .key(key)
            .body(ByteStream::from_static(b"bucket-tag-body"))
            .send()
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(client, &public_bucket, "public").await;
        client
            .put_bucket_policy()
            .bucket(&public_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal.clone(),
                        "Action": "s3:PutObjectTagging",
                        "Resource": bucket_wildcard_resource(&public_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let private_bucket = create_bucket_allowing_sse_c(client).await;
        client
            .put_object()
            .bucket(&private_bucket)
            .key(key)
            .body(ByteStream::from_static(b"bucket-tag-body"))
            .send()
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(client, &private_bucket, "private").await;
        client
            .put_bucket_policy()
            .bucket(&private_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal,
                        "Action": "s3:PutObjectTagging",
                        "Resource": bucket_wildcard_resource(&private_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
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
            "PutObjectTagging allowed for public bucket tag when ABAC is enabled",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&public_bucket)
                    .key(key)
                    .tagging(simple_bucket_tagging("updated", "public"))
                    .send()
            },
        )
        .await;

        let updated = client
            .get_object_tagging()
            .bucket(&public_bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(updated.tag_set().len(), 1);
        assert_eq!(updated.tag_set()[0].key(), "updated");
        assert_eq!(updated.tag_set()[0].value(), "public");

        eventually_access_denied(
            "PutObjectTagging denied for private bucket tag when ABAC is enabled",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&private_bucket)
                    .key(key)
                    .tagging(simple_bucket_tagging("updated", "private"))
                    .send()
            },
        )
        .await;

        cleanup(&public_bucket, &[key]).await;
        cleanup(&private_bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_get_object_acl_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let key = "bucket-tag-get-acl-enabled";

        let public_bucket = create_bucket_allowing_sse_c(client).await;
        client
            .put_object()
            .bucket(&public_bucket)
            .key(key)
            .body(ByteStream::from_static(b"bucket-tag-body"))
            .send()
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(client, &public_bucket, "public").await;
        client
            .put_bucket_policy()
            .bucket(&public_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal.clone(),
                        "Action": "s3:GetObjectAcl",
                        "Resource": bucket_wildcard_resource(&public_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let private_bucket = create_bucket_allowing_sse_c(client).await;
        client
            .put_object()
            .bucket(&private_bucket)
            .key(key)
            .body(ByteStream::from_static(b"bucket-tag-body"))
            .send()
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(client, &private_bucket, "private").await;
        client
            .put_bucket_policy()
            .bucket(&private_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal,
                        "Action": "s3:GetObjectAcl",
                        "Resource": bucket_wildcard_resource(&private_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let acl = eventually_ok(
            "GetObjectAcl allowed for public bucket tag when ABAC is enabled",
            || {
                alt_client
                    .get_object_acl()
                    .bucket(&public_bucket)
                    .key(key)
                    .send()
            },
        )
        .await;
        assert!(acl.owner().is_some(), "expected owner in GetObjectAcl");

        eventually_access_denied(
            "GetObjectAcl denied for private bucket tag when ABAC is enabled",
            || {
                alt_client
                    .get_object_acl()
                    .bucket(&private_bucket)
                    .key(key)
                    .send()
            },
        )
        .await;

        cleanup(&public_bucket, &[key]).await;
        cleanup(&private_bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_put_object_acl_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let key = "bucket-tag-put-acl-enabled";
        let alt_id = client_canonical_id(alt_client).await;
        let grant_read_header = format!("id=\"{alt_id}\"");

        let public_bucket = create_bucket_allowing_sse_c(client).await;
        set_object_writer_ownership(&public_bucket).await;
        client
            .put_object()
            .bucket(&public_bucket)
            .key(key)
            .body(ByteStream::from_static(b"bucket-tag-body"))
            .send()
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(client, &public_bucket, "public").await;
        client
            .put_bucket_policy()
            .bucket(&public_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal.clone(),
                        "Action": "s3:PutObjectAcl",
                        "Resource": bucket_wildcard_resource(&public_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let private_bucket = create_bucket_allowing_sse_c(client).await;
        set_object_writer_ownership(&private_bucket).await;
        client
            .put_object()
            .bucket(&private_bucket)
            .key(key)
            .body(ByteStream::from_static(b"bucket-tag-body"))
            .send()
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(client, &private_bucket, "private").await;
        client
            .put_bucket_policy()
            .bucket(&private_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal,
                        "Action": "s3:PutObjectAcl",
                        "Resource": bucket_wildcard_resource(&private_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
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
            "PutObjectAcl allowed for public bucket tag when ABAC is enabled",
            || {
                let grant_read_header = grant_read_header.clone();
                alt_client
                    .put_object_acl()
                    .bucket(&public_bucket)
                    .key(key)
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
            .bucket(&public_bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert!(
            has_grant(owner_view.grants(), Permission::Read, Some(&alt_id)),
            "expected READ grant for alternate account, got {:?}",
            owner_view.grants()
        );

        eventually_access_denied(
            "PutObjectAcl denied for private bucket tag when ABAC is enabled",
            || {
                let grant_read_header = grant_read_header.clone();
                alt_client
                    .put_object_acl()
                    .bucket(&private_bucket)
                    .key(key)
                    .customize()
                    .mutate_request(move |req| {
                        req.headers_mut()
                            .insert("x-amz-grant-read", grant_read_header.clone());
                    })
                    .send()
            },
        )
        .await;

        cleanup(&public_bucket, &[key]).await;
        cleanup(&private_bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_delete_object_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let key = "bucket-tag-delete-enabled";

        let public_bucket = create_bucket_allowing_sse_c(client).await;
        client
            .put_object()
            .bucket(&public_bucket)
            .key(key)
            .body(ByteStream::from_static(b"bucket-tag-body"))
            .send()
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(client, &public_bucket, "public").await;
        client
            .put_bucket_policy()
            .bucket(&public_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal.clone(),
                        "Action": "s3:DeleteObject",
                        "Resource": bucket_wildcard_resource(&public_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let private_bucket = create_bucket_allowing_sse_c(client).await;
        client
            .put_object()
            .bucket(&private_bucket)
            .key(key)
            .body(ByteStream::from_static(b"bucket-tag-body"))
            .send()
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(client, &private_bucket, "private").await;
        client
            .put_bucket_policy()
            .bucket(&private_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal,
                        "Action": "s3:DeleteObject",
                        "Resource": bucket_wildcard_resource(&private_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
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
            "DeleteObject allowed for public bucket tag when ABAC is enabled",
            || {
                alt_client
                    .delete_object()
                    .bucket(&public_bucket)
                    .key(key)
                    .send()
            },
        )
        .await;

        let missing = client
            .get_object()
            .bucket(&public_bucket)
            .key(key)
            .send()
            .await;
        assert_eq!(err_status(&missing), 404);
        assert_s3_err_code(&missing, "NoSuchKey");

        eventually_access_denied(
            "DeleteObject denied for private bucket tag when ABAC is enabled",
            || {
                alt_client
                    .delete_object()
                    .bucket(&private_bucket)
                    .key(key)
                    .send()
            },
        )
        .await;

        cleanup(&public_bucket, &[]).await;
        cleanup(&private_bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_get_object_version_acl_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let key = "bucket-tag-version-acl-get";

        let public_bucket = create_versioned_bucket_allowing_public_policy(client).await;
        let public_version_id = put_versioned_object(client, &public_bucket, key, b"body").await;
        enable_bucket_abac_with_security_tag(client, &public_bucket, "public").await;
        client
            .put_bucket_policy()
            .bucket(&public_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal.clone(),
                        "Action": "s3:GetObjectVersionAcl",
                        "Resource": bucket_wildcard_resource(&public_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let private_bucket = create_versioned_bucket_allowing_public_policy(client).await;
        let private_version_id = put_versioned_object(client, &private_bucket, key, b"body").await;
        enable_bucket_abac_with_security_tag(client, &private_bucket, "private").await;
        client
            .put_bucket_policy()
            .bucket(&private_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal,
                        "Action": "s3:GetObjectVersionAcl",
                        "Resource": bucket_wildcard_resource(&private_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let acl = eventually_ok(
            "GetObjectVersionAcl allowed for public bucket tag when ABAC is enabled",
            || {
                alt_client
                    .get_object_acl()
                    .bucket(&public_bucket)
                    .key(key)
                    .version_id(&public_version_id)
                    .send()
            },
        )
        .await;
        assert!(
            acl.owner().is_some(),
            "expected owner in GetObjectVersionAcl"
        );

        eventually_access_denied(
            "GetObjectVersionAcl denied for private bucket tag when ABAC is enabled",
            || {
                alt_client
                    .get_object_acl()
                    .bucket(&private_bucket)
                    .key(key)
                    .version_id(&private_version_id)
                    .send()
            },
        )
        .await;

        cleanup_versioned_bucket(client, &public_bucket).await;
        cleanup_versioned_bucket(client, &private_bucket).await;
    });
}

#[test]
fn test_bucket_policy_put_object_version_acl_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let key = "bucket-tag-version-acl-put";
        let alt_id = client_canonical_id(alt_client).await;
        let grant_read_header = format!("id=\"{alt_id}\"");

        let public_bucket = create_versioned_bucket_allowing_public_policy(client).await;
        set_object_writer_ownership(&public_bucket).await;
        let public_version_id = put_versioned_object(client, &public_bucket, key, b"body").await;
        enable_bucket_abac_with_security_tag(client, &public_bucket, "public").await;
        client
            .put_bucket_policy()
            .bucket(&public_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal.clone(),
                        "Action": "s3:PutObjectVersionAcl",
                        "Resource": bucket_wildcard_resource(&public_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let private_bucket = create_versioned_bucket_allowing_public_policy(client).await;
        set_object_writer_ownership(&private_bucket).await;
        let private_version_id = put_versioned_object(client, &private_bucket, key, b"body").await;
        enable_bucket_abac_with_security_tag(client, &private_bucket, "private").await;
        client
            .put_bucket_policy()
            .bucket(&private_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal,
                        "Action": "s3:PutObjectVersionAcl",
                        "Resource": bucket_wildcard_resource(&private_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
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
            "PutObjectVersionAcl allowed for public bucket tag when ABAC is enabled",
            || {
                let grant_read_header = grant_read_header.clone();
                alt_client
                    .put_object_acl()
                    .bucket(&public_bucket)
                    .key(key)
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

        eventually_access_denied(
            "PutObjectVersionAcl denied for private bucket tag when ABAC is enabled",
            || {
                let grant_read_header = grant_read_header.clone();
                alt_client
                    .put_object_acl()
                    .bucket(&private_bucket)
                    .key(key)
                    .version_id(&private_version_id)
                    .customize()
                    .mutate_request(move |req| {
                        req.headers_mut()
                            .insert("x-amz-grant-read", grant_read_header.clone());
                    })
                    .send()
            },
        )
        .await;

        cleanup_versioned_bucket(client, &public_bucket).await;
        cleanup_versioned_bucket(client, &private_bucket).await;
    });
}

#[test]
fn test_bucket_policy_get_object_version_tagging_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let key = "bucket-tag-version-tagging-get";

        let public_bucket = create_versioned_bucket_allowing_public_policy(client).await;
        let public_version_id = put_versioned_object(client, &public_bucket, key, b"body").await;
        client
            .put_object_tagging()
            .bucket(&public_bucket)
            .key(key)
            .version_id(&public_version_id)
            .tagging(simple_bucket_tagging("color", "blue"))
            .send()
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(client, &public_bucket, "public").await;
        client
            .put_bucket_policy()
            .bucket(&public_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal.clone(),
                        "Action": "s3:GetObjectVersionTagging",
                        "Resource": bucket_wildcard_resource(&public_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let private_bucket = create_versioned_bucket_allowing_public_policy(client).await;
        let private_version_id = put_versioned_object(client, &private_bucket, key, b"body").await;
        client
            .put_object_tagging()
            .bucket(&private_bucket)
            .key(key)
            .version_id(&private_version_id)
            .tagging(simple_bucket_tagging("color", "blue"))
            .send()
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(client, &private_bucket, "private").await;
        client
            .put_bucket_policy()
            .bucket(&private_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal,
                        "Action": "s3:GetObjectVersionTagging",
                        "Resource": bucket_wildcard_resource(&private_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let tagging = eventually_ok(
            "GetObjectVersionTagging allowed for public bucket tag when ABAC is enabled",
            || {
                alt_client
                    .get_object_tagging()
                    .bucket(&public_bucket)
                    .key(key)
                    .version_id(&public_version_id)
                    .send()
            },
        )
        .await;
        assert_eq!(tagging.tag_set().len(), 1);
        assert_eq!(tagging.tag_set()[0].key(), "color");
        assert_eq!(tagging.tag_set()[0].value(), "blue");

        eventually_access_denied(
            "GetObjectVersionTagging denied for private bucket tag when ABAC is enabled",
            || {
                alt_client
                    .get_object_tagging()
                    .bucket(&private_bucket)
                    .key(key)
                    .version_id(&private_version_id)
                    .send()
            },
        )
        .await;

        cleanup_versioned_bucket(client, &public_bucket).await;
        cleanup_versioned_bucket(client, &private_bucket).await;
    });
}

#[test]
fn test_bucket_policy_put_object_version_tagging_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let key = "bucket-tag-version-tagging-put";

        let public_bucket = create_versioned_bucket_allowing_public_policy(client).await;
        let public_version_id = put_versioned_object(client, &public_bucket, key, b"body").await;
        enable_bucket_abac_with_security_tag(client, &public_bucket, "public").await;
        client
            .put_bucket_policy()
            .bucket(&public_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal.clone(),
                        "Action": "s3:PutObjectVersionTagging",
                        "Resource": bucket_wildcard_resource(&public_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let private_bucket = create_versioned_bucket_allowing_public_policy(client).await;
        let private_version_id = put_versioned_object(client, &private_bucket, key, b"body").await;
        enable_bucket_abac_with_security_tag(client, &private_bucket, "private").await;
        client
            .put_bucket_policy()
            .bucket(&private_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal,
                        "Action": "s3:PutObjectVersionTagging",
                        "Resource": bucket_wildcard_resource(&private_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
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
            "PutObjectVersionTagging allowed for public bucket tag when ABAC is enabled",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&public_bucket)
                    .key(key)
                    .version_id(&public_version_id)
                    .tagging(simple_bucket_tagging("color", "blue"))
                    .send()
            },
        )
        .await;

        let owner_tagging = client
            .get_object_tagging()
            .bucket(&public_bucket)
            .key(key)
            .version_id(&public_version_id)
            .send()
            .await
            .unwrap();
        assert_eq!(owner_tagging.tag_set().len(), 1);
        assert_eq!(owner_tagging.tag_set()[0].key(), "color");
        assert_eq!(owner_tagging.tag_set()[0].value(), "blue");

        eventually_access_denied(
            "PutObjectVersionTagging denied for private bucket tag when ABAC is enabled",
            || {
                alt_client
                    .put_object_tagging()
                    .bucket(&private_bucket)
                    .key(key)
                    .version_id(&private_version_id)
                    .tagging(simple_bucket_tagging("color", "blue"))
                    .send()
            },
        )
        .await;

        cleanup_versioned_bucket(client, &public_bucket).await;
        cleanup_versioned_bucket(client, &private_bucket).await;
    });
}

#[test]
fn test_bucket_policy_delete_object_version_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let key = "bucket-tag-version-delete";

        let public_bucket = create_versioned_bucket_allowing_public_policy(client).await;
        let public_version_id = put_versioned_object(client, &public_bucket, key, b"body").await;
        enable_bucket_abac_with_security_tag(client, &public_bucket, "public").await;
        client
            .put_bucket_policy()
            .bucket(&public_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal.clone(),
                        "Action": "s3:DeleteObjectVersion",
                        "Resource": bucket_wildcard_resource(&public_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let private_bucket = create_versioned_bucket_allowing_public_policy(client).await;
        let private_version_id = put_versioned_object(client, &private_bucket, key, b"body").await;
        enable_bucket_abac_with_security_tag(client, &private_bucket, "private").await;
        client
            .put_bucket_policy()
            .bucket(&private_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal,
                        "Action": "s3:DeleteObjectVersion",
                        "Resource": bucket_wildcard_resource(&private_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
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
            "DeleteObjectVersion allowed for public bucket tag when ABAC is enabled",
            || {
                alt_client
                    .delete_object()
                    .bucket(&public_bucket)
                    .key(key)
                    .version_id(&public_version_id)
                    .send()
            },
        )
        .await;

        eventually_access_denied(
            "DeleteObjectVersion denied for private bucket tag when ABAC is enabled",
            || {
                alt_client
                    .delete_object()
                    .bucket(&private_bucket)
                    .key(key)
                    .version_id(&private_version_id)
                    .send()
            },
        )
        .await;

        cleanup_versioned_bucket(client, &public_bucket).await;
        cleanup_versioned_bucket(client, &private_bucket).await;
    });
}

#[test]
fn test_bucket_policy_delete_object_tagging_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let key = "bucket-tag-delete-tagging";

        let public_bucket = create_versioned_bucket_allowing_public_policy(client).await;
        let _public_version_id = put_versioned_object(client, &public_bucket, key, b"body").await;
        client
            .put_object_tagging()
            .bucket(&public_bucket)
            .key(key)
            .tagging(simple_bucket_tagging("color", "blue"))
            .send()
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(client, &public_bucket, "public").await;
        client
            .put_bucket_policy()
            .bucket(&public_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal.clone(),
                        "Action": "s3:DeleteObjectTagging",
                        "Resource": bucket_wildcard_resource(&public_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let private_bucket = create_versioned_bucket_allowing_public_policy(client).await;
        let _private_version_id = put_versioned_object(client, &private_bucket, key, b"body").await;
        client
            .put_object_tagging()
            .bucket(&private_bucket)
            .key(key)
            .tagging(simple_bucket_tagging("color", "blue"))
            .send()
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(client, &private_bucket, "private").await;
        client
            .put_bucket_policy()
            .bucket(&private_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal,
                        "Action": "s3:DeleteObjectTagging",
                        "Resource": bucket_wildcard_resource(&private_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
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
            "DeleteObjectTagging allowed for public bucket tag when ABAC is enabled",
            || {
                alt_client
                    .delete_object_tagging()
                    .bucket(&public_bucket)
                    .key(key)
                    .send()
            },
        )
        .await;

        let owner_tagging = client
            .get_object_tagging()
            .bucket(&public_bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert!(owner_tagging.tag_set().is_empty());

        eventually_access_denied(
            "DeleteObjectTagging denied for private bucket tag when ABAC is enabled",
            || {
                alt_client
                    .delete_object_tagging()
                    .bucket(&private_bucket)
                    .key(key)
                    .send()
            },
        )
        .await;

        cleanup_versioned_bucket(client, &public_bucket).await;
        cleanup_versioned_bucket(client, &private_bucket).await;
    });
}

#[test]
fn test_bucket_policy_delete_object_version_tagging_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let key = "bucket-tag-version-delete-tagging";

        let public_bucket = create_versioned_bucket_allowing_public_policy(client).await;
        let public_version_id = put_versioned_object(client, &public_bucket, key, b"body").await;
        client
            .put_object_tagging()
            .bucket(&public_bucket)
            .key(key)
            .version_id(&public_version_id)
            .tagging(simple_bucket_tagging("color", "blue"))
            .send()
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(client, &public_bucket, "public").await;
        client
            .put_bucket_policy()
            .bucket(&public_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal.clone(),
                        "Action": "s3:DeleteObjectVersionTagging",
                        "Resource": bucket_wildcard_resource(&public_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();

        let private_bucket = create_versioned_bucket_allowing_public_policy(client).await;
        let private_version_id = put_versioned_object(client, &private_bucket, key, b"body").await;
        client
            .put_object_tagging()
            .bucket(&private_bucket)
            .key(key)
            .version_id(&private_version_id)
            .tagging(simple_bucket_tagging("color", "blue"))
            .send()
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(client, &private_bucket, "private").await;
        client
            .put_bucket_policy()
            .bucket(&private_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": principal,
                        "Action": "s3:DeleteObjectVersionTagging",
                        "Resource": bucket_wildcard_resource(&private_bucket),
                        "Condition": {
                            "StringEquals": {
                                "s3:BucketTag/security": "public"
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
            "DeleteObjectVersionTagging allowed for public bucket tag when ABAC is enabled",
            || {
                alt_client
                    .delete_object_tagging()
                    .bucket(&public_bucket)
                    .key(key)
                    .version_id(&public_version_id)
                    .send()
            },
        )
        .await;

        let owner_tagging = client
            .get_object_tagging()
            .bucket(&public_bucket)
            .key(key)
            .version_id(&public_version_id)
            .send()
            .await
            .unwrap();
        assert!(owner_tagging.tag_set().is_empty());

        eventually_access_denied(
            "DeleteObjectVersionTagging denied for private bucket tag when ABAC is enabled",
            || {
                alt_client
                    .delete_object_tagging()
                    .bucket(&private_bucket)
                    .key(key)
                    .version_id(&private_version_id)
                    .send()
            },
        )
        .await;

        cleanup_versioned_bucket(client, &public_bucket).await;
        cleanup_versioned_bucket(client, &private_bucket).await;
    });
}

#[test]
fn test_bucket_policy_head_bucket_bucket_tag_conditions_with_list_and_location_when_abac_enabled() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let public_bucket = create_bucket_allowing_public_policy(client).await;
        enable_bucket_abac_with_security_tag(client, &public_bucket, "public").await;
        client
            .put_bucket_policy()
            .bucket(&public_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": alt_policy_principal(),
                            "Action": "s3:ListBucket",
                            "Resource": bucket_resource(&public_bucket),
                            "Condition": {
                                "StringEquals": {
                                    "s3:BucketTag/security": "public"
                                }
                            }
                        },
                        {
                            "Effect": "Allow",
                            "Principal": alt_policy_principal(),
                            "Action": "s3:GetBucketLocation",
                            "Resource": bucket_resource(&public_bucket),
                            "Condition": {
                                "StringEquals": {
                                    "s3:BucketTag/security": "public"
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

        let private_bucket = create_bucket_allowing_public_policy(client).await;
        enable_bucket_abac_with_security_tag(client, &private_bucket, "private").await;
        client
            .put_bucket_policy()
            .bucket(&private_bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": alt_policy_principal(),
                            "Action": "s3:ListBucket",
                            "Resource": bucket_resource(&private_bucket),
                            "Condition": {
                                "StringEquals": {
                                    "s3:BucketTag/security": "public"
                                }
                            }
                        },
                        {
                            "Effect": "Allow",
                            "Principal": alt_policy_principal(),
                            "Action": "s3:GetBucketLocation",
                            "Resource": bucket_resource(&private_bucket),
                            "Condition": {
                                "StringEquals": {
                                    "s3:BucketTag/security": "public"
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

        let result = eventually_ok(
            "HeadBucket allowed with bucket-tag-conditioned ListBucket and GetBucketLocation",
            || alt_client.head_bucket().bucket(&public_bucket).send(),
        )
        .await;
        assert_eq!(result.bucket_region(), Some(CTX.region()));

        eventually_access_denied(
            "HeadBucket denied for private bucket tag with ListBucket and GetBucketLocation",
            || alt_client.head_bucket().bucket(&private_bucket).send(),
        )
        .await;

        cleanup(&public_bucket, &[]).await;
        cleanup(&private_bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_head_bucket_bucket_tag_condition_with_location_only_when_abac_enabled() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_bucket_allowing_public_policy(client).await;
        enable_bucket_abac_with_security_tag(client, &bucket, "public").await;
        client
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
                                "s3:BucketTag/security": "public"
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
            "HeadBucket denied with bucket-tag-conditioned GetBucketLocation alone",
            || alt_client.head_bucket().bucket(&bucket).send(),
        )
        .await;

        cleanup(&bucket, &[]).await;
    });
}
