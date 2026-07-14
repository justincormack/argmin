use std::future::Future;
use std::time::Duration;

use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::types::{
    AbacStatus, BucketAbacStatus, BucketLifecycleConfiguration, BucketVersioningStatus,
    CorsConfiguration, CorsRule, DefaultRetention, ExpirationStatus, LifecycleExpiration,
    LifecycleRule, LifecycleRuleFilter, ObjectLockConfiguration, ObjectLockEnabled,
    ObjectLockRetentionMode, ObjectLockRule, ObjectOwnership, OwnershipControls,
    OwnershipControlsRule, PublicAccessBlockConfiguration, ServerSideEncryption,
    ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration, ServerSideEncryptionRule,
    Tag, Tagging, VersioningConfiguration,
};
use s3_tests::{
    put_bucket_lifecycle_with_md5, send_signed_request_for_service_with_credentials, unique_bucket,
    SendRetryingOperationAborted, SignedRequestCredentials, CTX,
};

fn owner_root_client() -> &'static aws_sdk_s3::Client {
    CTX.require_owner_root_client()
}

async fn eventually_ok<T, E, F, Fut>(description: &str, mut op: F) -> T
where
    E: std::fmt::Debug,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, aws_sdk_s3::error::SdkError<E>>>,
{
    const MAX_ATTEMPTS: usize = 20;

    for attempt in 0..MAX_ATTEMPTS {
        match op().await {
            Ok(output) => return output,
            Err(_) if attempt + 1 < MAX_ATTEMPTS => {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(err) => panic!("{description} failed unexpectedly: {err:?}"),
        }
    }

    unreachable!()
}

async fn eventually_err_status<T, E, F, Fut>(
    description: &str,
    expected_status: u16,
    expected_code: Option<&str>,
    mut op: F,
) where
    E: std::fmt::Debug + ProvideErrorMetadata,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, aws_sdk_s3::error::SdkError<E>>>,
{
    const MAX_ATTEMPTS: usize = 20;

    for attempt in 0..MAX_ATTEMPTS {
        let result = op().await;
        match &result {
            Err(err)
                if err
                    .raw_response()
                    .map(|response| response.status().as_u16())
                    == Some(expected_status)
                    && expected_code.is_none_or(|code| {
                        err.as_service_error().and_then(ProvideErrorMetadata::code) == Some(code)
                    }) =>
            {
                return;
            }
            _ if attempt + 1 < MAX_ATTEMPTS => {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            _ => panic!(
                "{description} did not converge to HTTP {expected_status} ({expected_code:?})"
            ),
        }
    }

    unreachable!()
}

async fn eventually_err_status_with_message<T, E, F, Fut>(
    description: &str,
    expected_status: u16,
    expected_code: Option<&str>,
    expected_message: &str,
    mut op: F,
) where
    E: std::fmt::Debug + ProvideErrorMetadata,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, aws_sdk_s3::error::SdkError<E>>>,
{
    const MAX_ATTEMPTS: usize = 20;

    for attempt in 0..MAX_ATTEMPTS {
        let result = op().await;
        match &result {
            Err(err)
                if err
                    .raw_response()
                    .map(|response| response.status().as_u16())
                    == Some(expected_status)
                    && expected_code.is_none_or(|code| {
                        err.as_service_error().and_then(ProvideErrorMetadata::code) == Some(code)
                    })
                    && err
                        .as_service_error()
                        .and_then(ProvideErrorMetadata::message)
                        == Some(expected_message) =>
            {
                return;
            }
            _ if attempt + 1 < MAX_ATTEMPTS => {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            _ => {
                let last_result = match &result {
                    Ok(_) => "Ok".to_string(),
                    Err(err) => format!(
                        "status={:?} code={:?} message={:?}",
                        err.raw_response()
                            .map(|response| response.status().as_u16()),
                        err.as_service_error().and_then(ProvideErrorMetadata::code),
                        err.as_service_error()
                            .and_then(ProvideErrorMetadata::message),
                    ),
                };
                panic!(
                    "{description} did not converge to HTTP {expected_status} ({expected_code:?}) with message {expected_message:?}; last result: {}",
                    last_result
                );
            }
        }
    }

    unreachable!()
}

async fn eventually_list_contains(
    client: &aws_sdk_s3::Client,
    expected_buckets: &[&str],
    description: &str,
) {
    const MAX_ATTEMPTS: usize = 20;

    for attempt in 0..MAX_ATTEMPTS {
        let result = client
            .list_buckets()
            .send_retrying_operation_aborted("list buckets for admin root test")
            .await;
        match result {
            Ok(output) => {
                let names: Vec<&str> = output
                    .buckets()
                    .iter()
                    .filter_map(|bucket| bucket.name())
                    .collect();
                if expected_buckets
                    .iter()
                    .all(|expected| names.contains(expected))
                {
                    return;
                }
            }
            Err(err) if attempt + 1 < MAX_ATTEMPTS => {
                let _ = err;
            }
            Err(err) => panic!("{description} failed unexpectedly: {err:?}"),
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    panic!("{description} did not converge to include {expected_buckets:?}");
}

async fn create_standard_bucket(client: &aws_sdk_s3::Client) -> String {
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    bucket
}

async fn create_acl_enabled_bucket(client: &aws_sdk_s3::Client) -> String {
    let bucket = unique_bucket();
    s3_tests::create_bucket_request(client, &bucket)
        .object_ownership(ObjectOwnership::ObjectWriter)
        .send()
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

async fn cleanup_bucket(
    root_client: &aws_sdk_s3::Client,
    non_root_client: &aws_sdk_s3::Client,
    bucket: &str,
) {
    for client in [root_client, non_root_client] {
        let _ = client
            .delete_bucket_policy()
            .bucket(bucket)
            .send_retrying_operation_aborted("delete bucket policy during admin cleanup")
            .await;
        let _ = client
            .delete_bucket_cors()
            .bucket(bucket)
            .send_retrying_operation_aborted("delete bucket CORS during admin cleanup")
            .await;
        let _ = client
            .delete_bucket_tagging()
            .bucket(bucket)
            .send_retrying_operation_aborted("delete bucket tagging during admin cleanup")
            .await;
        let _ = client
            .delete_bucket_lifecycle()
            .bucket(bucket)
            .send_retrying_operation_aborted("delete bucket lifecycle during admin cleanup")
            .await;
        let _ = client
            .delete_public_access_block()
            .bucket(bucket)
            .send_retrying_operation_aborted("delete public access block during admin cleanup")
            .await;
        let _ = client
            .delete_bucket_ownership_controls()
            .bucket(bucket)
            .send_retrying_operation_aborted("delete ownership controls during admin cleanup")
            .await;
        let _ = client
            .delete_bucket_encryption()
            .bucket(bucket)
            .send_retrying_operation_aborted("delete bucket encryption during admin cleanup")
            .await;
    }

    let mut last_error = None;
    for attempt in 0..20 {
        let mut saw_retryable = false;
        for client in [root_client, non_root_client] {
            match client
                .delete_bucket()
                .bucket(bucket)
                .send_retrying_operation_aborted("delete bucket during admin cleanup")
                .await
            {
                Ok(_) => return,
                Err(err)
                    if err.as_service_error().and_then(ProvideErrorMetadata::code)
                        == Some("NoSuchBucket") =>
                {
                    return;
                }
                Err(err) => {
                    let raw = format!("{err:?}");
                    saw_retryable |= raw.contains("OperationAborted")
                        || raw.contains("BucketNotEmpty")
                        || raw.contains("NoSuchBucket");
                    last_error = Some(raw);
                }
            }
        }
        if saw_retryable && attempt < 19 {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }
        break;
    }

    panic!(
        "bucket cleanup delete failed for {bucket}: {}",
        last_error.unwrap_or_else(|| "no delete attempt was made".to_string())
    );
}

fn versioning_enabled() -> VersioningConfiguration {
    VersioningConfiguration::builder()
        .status(BucketVersioningStatus::Enabled)
        .build()
}

fn simple_public_access_block() -> PublicAccessBlockConfiguration {
    PublicAccessBlockConfiguration::builder()
        .block_public_acls(true)
        .ignore_public_acls(true)
        .block_public_policy(true)
        .restrict_public_buckets(false)
        .build()
}

fn simple_ownership_controls() -> OwnershipControls {
    let rule = OwnershipControlsRule::builder()
        .object_ownership(ObjectOwnership::BucketOwnerPreferred)
        .build()
        .unwrap();
    OwnershipControls::builder().rules(rule).build().unwrap()
}

fn simple_tagging() -> Tagging {
    Tagging::builder()
        .tag_set(
            Tag::builder()
                .key("env")
                .value("root-admin")
                .build()
                .unwrap(),
        )
        .build()
        .unwrap()
}

fn enabled_abac_status() -> AbacStatus {
    AbacStatus::builder()
        .status(BucketAbacStatus::Enabled)
        .build()
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
    let resource = percent_encode_path_segment(&bucket_resource_arn(bucket));
    let url = format!("{}/v20180820/tags/{resource}", CTX.s3_control_endpoint());
    send_signed_request_for_service_with_credentials(
        "POST",
        &url,
        tag_resource_body(tags).as_bytes(),
        [("x-amz-account-id", CTX.account_id())],
        "s3",
        raw_primary_credentials(),
    )
}

fn untag_resource(bucket: &str, tag_keys: &[&str]) -> s3_tests::RawResponse {
    let resource = percent_encode_path_segment(&bucket_resource_arn(bucket));
    let query = tag_keys
        .iter()
        .map(|key| format!("tagKeys={}", percent_encode_path_segment(key)))
        .collect::<Vec<_>>()
        .join("&");
    let url = format!(
        "{}/v20180820/tags/{resource}?{query}",
        CTX.s3_control_endpoint()
    );
    send_signed_request_for_service_with_credentials(
        "DELETE",
        &url,
        &[],
        [("x-amz-account-id", CTX.account_id())],
        "s3",
        raw_primary_credentials(),
    )
}

fn simple_cors_config() -> CorsConfiguration {
    CorsConfiguration::builder()
        .cors_rules(
            CorsRule::builder()
                .allowed_origins("https://example.com")
                .allowed_methods("GET")
                .allowed_headers("*")
                .build()
                .unwrap(),
        )
        .build()
        .unwrap()
}

fn simple_lifecycle_config() -> BucketLifecycleConfiguration {
    BucketLifecycleConfiguration::builder()
        .rules(
            LifecycleRule::builder()
                .id("expire-logs")
                .filter(LifecycleRuleFilter::builder().prefix("logs/").build())
                .status(ExpirationStatus::Enabled)
                .expiration(LifecycleExpiration::builder().days(30).build())
                .build()
                .unwrap(),
        )
        .build()
        .unwrap()
}

fn simple_bucket_encryption() -> ServerSideEncryptionConfiguration {
    let default = ServerSideEncryptionByDefault::builder()
        .sse_algorithm(ServerSideEncryption::Aes256)
        .build()
        .unwrap();
    ServerSideEncryptionConfiguration::builder()
        .rules(
            ServerSideEncryptionRule::builder()
                .apply_server_side_encryption_by_default(default)
                .build(),
        )
        .build()
        .unwrap()
}

fn simple_object_lock_config() -> ObjectLockConfiguration {
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

#[test]
fn test_same_account_root_and_non_root_list_buckets_share_visibility() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let non_root_client = CTX.client();
        let root_bucket = create_standard_bucket(root_client).await;
        let non_root_bucket = create_standard_bucket(non_root_client).await;

        eventually_list_contains(
            root_client,
            &[&root_bucket, &non_root_bucket],
            "root ListBuckets",
        )
        .await;
        eventually_list_contains(
            non_root_client,
            &[&root_bucket, &non_root_bucket],
            "non-root ListBuckets",
        )
        .await;

        cleanup_bucket(root_client, non_root_client, &root_bucket).await;
        cleanup_bucket(root_client, non_root_client, &non_root_bucket).await;
    });
}

#[test]
fn test_same_account_root_and_non_root_delete_bucket() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let non_root_client = CTX.client();
        let root_bucket = create_standard_bucket(root_client).await;
        let non_root_bucket = create_standard_bucket(non_root_client).await;

        eventually_ok("non-root DeleteBucket on root-created bucket", || {
            non_root_client.delete_bucket().bucket(&root_bucket).send()
        })
        .await;
        eventually_ok("root DeleteBucket on non-root-created bucket", || {
            root_client.delete_bucket().bucket(&non_root_bucket).send()
        })
        .await;
    });
}

#[test]
fn test_same_account_root_and_non_root_bucket_acl_admin() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let non_root_client = CTX.client();
        let root_bucket = create_acl_enabled_bucket(root_client).await;
        let non_root_bucket = create_acl_enabled_bucket(non_root_client).await;

        for (description, bucket, admin_client) in [
            (
                "non-root manages root-created bucket ACL",
                root_bucket.as_str(),
                non_root_client,
            ),
            (
                "root manages non-root-created bucket ACL",
                non_root_bucket.as_str(),
                root_client,
            ),
        ] {
            let acl = eventually_ok(&format!("{description} GetBucketAcl"), || {
                admin_client.get_bucket_acl().bucket(bucket).send()
            })
            .await;
            assert!(
                acl.owner().and_then(|owner| owner.id()).is_some(),
                "{description} returned GetBucketAcl without owner"
            );

            eventually_ok(&format!("{description} PutBucketAcl"), || {
                admin_client
                    .put_bucket_acl()
                    .bucket(bucket)
                    .acl(aws_sdk_s3::types::BucketCannedAcl::Private)
                    .send()
            })
            .await;
        }

        cleanup_bucket(root_client, non_root_client, &root_bucket).await;
        cleanup_bucket(root_client, non_root_client, &non_root_bucket).await;
    });
}

#[test]
fn test_same_account_root_and_non_root_bucket_ownership_controls_admin() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let non_root_client = CTX.client();
        let root_bucket = create_standard_bucket(root_client).await;
        let non_root_bucket = create_standard_bucket(non_root_client).await;
        let controls = simple_ownership_controls();

        for (description, bucket, admin_client) in [
            (
                "non-root manages root-created ownership controls",
                root_bucket.as_str(),
                non_root_client,
            ),
            (
                "root manages non-root-created ownership controls",
                non_root_bucket.as_str(),
                root_client,
            ),
        ] {
            eventually_ok(&format!("{description} PutBucketOwnershipControls"), || {
                admin_client
                    .put_bucket_ownership_controls()
                    .bucket(bucket)
                    .ownership_controls(controls.clone())
                    .send()
            })
            .await;

            let get = eventually_ok(&format!("{description} GetBucketOwnershipControls"), || {
                admin_client
                    .get_bucket_ownership_controls()
                    .bucket(bucket)
                    .send()
            })
            .await;
            assert_eq!(
                get.ownership_controls().unwrap().rules()[0].object_ownership,
                ObjectOwnership::BucketOwnerPreferred
            );

            eventually_ok(
                &format!("{description} DeleteBucketOwnershipControls"),
                || {
                    admin_client
                        .delete_bucket_ownership_controls()
                        .bucket(bucket)
                        .send()
                },
            )
            .await;

            eventually_err_status(
                &format!("{description} GetBucketOwnershipControls after delete"),
                404,
                Some("OwnershipControlsNotFoundError"),
                || {
                    admin_client
                        .get_bucket_ownership_controls()
                        .bucket(bucket)
                        .send()
                },
            )
            .await;
        }

        cleanup_bucket(root_client, non_root_client, &root_bucket).await;
        cleanup_bucket(root_client, non_root_client, &non_root_bucket).await;
    });
}

#[test]
fn test_same_account_root_and_non_root_public_access_block_admin() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let non_root_client = CTX.client();
        let root_bucket = create_standard_bucket(root_client).await;
        let non_root_bucket = create_standard_bucket(non_root_client).await;
        let config = simple_public_access_block();

        for (description, bucket, admin_client) in [
            (
                "non-root manages root-created public access block",
                root_bucket.as_str(),
                non_root_client,
            ),
            (
                "root manages non-root-created public access block",
                non_root_bucket.as_str(),
                root_client,
            ),
        ] {
            eventually_ok(&format!("{description} PutPublicAccessBlock"), || {
                admin_client
                    .put_public_access_block()
                    .bucket(bucket)
                    .public_access_block_configuration(config.clone())
                    .send()
            })
            .await;

            let get = eventually_ok(&format!("{description} GetPublicAccessBlock"), || {
                admin_client.get_public_access_block().bucket(bucket).send()
            })
            .await;
            assert_eq!(
                get.public_access_block_configuration()
                    .unwrap()
                    .block_public_acls(),
                Some(true)
            );

            eventually_ok(&format!("{description} DeletePublicAccessBlock"), || {
                admin_client
                    .delete_public_access_block()
                    .bucket(bucket)
                    .send()
            })
            .await;

            eventually_err_status(
                &format!("{description} GetPublicAccessBlock after delete"),
                404,
                Some("NoSuchPublicAccessBlockConfiguration"),
                || admin_client.get_public_access_block().bucket(bucket).send(),
            )
            .await;
        }

        cleanup_bucket(root_client, non_root_client, &root_bucket).await;
        cleanup_bucket(root_client, non_root_client, &non_root_bucket).await;
    });
}

#[test]
fn test_same_account_root_and_non_root_bucket_versioning_admin() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let non_root_client = CTX.client();
        let root_bucket = create_standard_bucket(root_client).await;
        let non_root_bucket = create_standard_bucket(non_root_client).await;
        let config = versioning_enabled();

        for (description, bucket, admin_client) in [
            (
                "non-root manages root-created versioning",
                root_bucket.as_str(),
                non_root_client,
            ),
            (
                "root manages non-root-created versioning",
                non_root_bucket.as_str(),
                root_client,
            ),
        ] {
            eventually_ok(&format!("{description} PutBucketVersioning"), || {
                admin_client
                    .put_bucket_versioning()
                    .bucket(bucket)
                    .versioning_configuration(config.clone())
                    .send()
            })
            .await;

            let get = eventually_ok(&format!("{description} GetBucketVersioning"), || {
                admin_client.get_bucket_versioning().bucket(bucket).send()
            })
            .await;
            assert_eq!(get.status(), Some(&BucketVersioningStatus::Enabled));
        }

        cleanup_bucket(root_client, non_root_client, &root_bucket).await;
        cleanup_bucket(root_client, non_root_client, &non_root_bucket).await;
    });
}

#[test]
fn test_same_account_root_and_non_root_bucket_tagging_admin() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let non_root_client = CTX.client();
        let root_bucket = create_standard_bucket(root_client).await;
        let non_root_bucket = create_standard_bucket(non_root_client).await;
        let tags = simple_tagging();

        for (description, bucket, admin_client) in [
            (
                "non-root manages root-created bucket tagging",
                root_bucket.as_str(),
                non_root_client,
            ),
            (
                "root manages non-root-created bucket tagging",
                non_root_bucket.as_str(),
                root_client,
            ),
        ] {
            eventually_ok(&format!("{description} PutBucketTagging"), || {
                admin_client
                    .put_bucket_tagging()
                    .bucket(bucket)
                    .tagging(tags.clone())
                    .send()
            })
            .await;

            let get = eventually_ok(&format!("{description} GetBucketTagging"), || {
                admin_client.get_bucket_tagging().bucket(bucket).send()
            })
            .await;
            assert_eq!(get.tag_set().len(), 1);
            assert!(get
                .tag_set()
                .iter()
                .any(|tag| tag.key() == "env" && tag.value() == "root-admin"));

            eventually_ok(&format!("{description} DeleteBucketTagging"), || {
                admin_client.delete_bucket_tagging().bucket(bucket).send()
            })
            .await;

            eventually_err_status(
                &format!("{description} GetBucketTagging after delete"),
                404,
                Some("NoSuchTagSet"),
                || admin_client.get_bucket_tagging().bucket(bucket).send(),
            )
            .await;
        }

        cleanup_bucket(root_client, non_root_client, &root_bucket).await;
        cleanup_bucket(root_client, non_root_client, &non_root_bucket).await;
    });
}

#[test]
fn test_same_account_root_and_non_root_bucket_abac_admin() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let non_root_client = CTX.client();
        let root_bucket = create_standard_bucket(root_client).await;
        let non_root_bucket = create_standard_bucket(non_root_client).await;
        let tags = simple_tagging();

        for (description, bucket, admin_client) in [
            (
                "non-root manages root-created bucket ABAC",
                root_bucket.as_str(),
                non_root_client,
            ),
            (
                "root manages non-root-created bucket ABAC",
                non_root_bucket.as_str(),
                root_client,
            ),
        ] {
            let get = eventually_ok(&format!("{description} GetBucketAbac disabled"), || {
                admin_client.get_bucket_abac().bucket(bucket).send()
            })
            .await;
            assert_eq!(
                get.abac_status().and_then(|status| status.status()),
                Some(&BucketAbacStatus::Disabled)
            );

            eventually_ok(
                &format!("{description} PutBucketTagging before enable"),
                || {
                    admin_client
                        .put_bucket_tagging()
                        .bucket(bucket)
                        .tagging(tags.clone())
                        .send()
                },
            )
            .await;

            eventually_ok(&format!("{description} PutBucketAbac enabled"), || {
                admin_client
                    .put_bucket_abac()
                    .bucket(bucket)
                    .abac_status(enabled_abac_status())
                    .send()
            })
            .await;

            let get = eventually_ok(&format!("{description} GetBucketAbac enabled"), || {
                admin_client.get_bucket_abac().bucket(bucket).send()
            })
            .await;
            assert_eq!(
                get.abac_status().and_then(|status| status.status()),
                Some(&BucketAbacStatus::Enabled)
            );

            let get = eventually_ok(
                &format!("{description} GetBucketTagging after enable"),
                || admin_client.get_bucket_tagging().bucket(bucket).send(),
            )
            .await;
            assert_eq!(get.tag_set().len(), 1);
            assert_eq!(get.tag_set()[0].key(), "env");
            assert_eq!(get.tag_set()[0].value(), "root-admin");

            eventually_err_status_with_message(
                &format!("{description} PutBucketTagging after enable"),
                400,
                Some("BadRequest"),
                "This S3 general purpose bucket has attribute-based access control (ABAC) enabled. To add tags to this bucket, initiate a TagResource request. To delete tags from this bucket, initiate an UntagResource request.",
                || {
                    admin_client
                        .put_bucket_tagging()
                        .bucket(bucket)
                        .tagging(tags.clone())
                        .send()
                },
            )
            .await;

            eventually_err_status_with_message(
                &format!("{description} DeleteBucketTagging after enable"),
                400,
                Some("BadRequest"),
                "This S3 general purpose bucket has attribute-based access control (ABAC) enabled. To delete tags from this bucket, initiate an UntagResource request.",
                || admin_client.delete_bucket_tagging().bucket(bucket).send(),
            )
            .await;
        }

        cleanup_bucket(root_client, non_root_client, &root_bucket).await;
        cleanup_bucket(root_client, non_root_client, &non_root_bucket).await;
    });
}

#[test]
fn test_bucket_abac_tag_resource_and_untag_resource() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_standard_bucket(client).await;

        eventually_ok("PutBucketTagging before enable", || {
            client
                .put_bucket_tagging()
                .bucket(&bucket)
                .tagging(simple_tagging())
                .send()
        })
        .await;

        eventually_ok("PutBucketAbac enabled", || {
            client
                .put_bucket_abac()
                .bucket(&bucket)
                .abac_status(enabled_abac_status())
                .send()
        })
        .await;

        let tag = tag_resource(&bucket, &[("security", "public")]);
        assert_eq!(tag.status, 204, "TagResource failed: {:?}", tag);

        let get = eventually_ok("GetBucketTagging after TagResource", || {
            client.get_bucket_tagging().bucket(&bucket).send()
        })
        .await;
        assert_eq!(get.tag_set().len(), 2);
        assert!(get
            .tag_set()
            .iter()
            .any(|tag| tag.key() == "env" && tag.value() == "root-admin"));
        assert!(get
            .tag_set()
            .iter()
            .any(|tag| tag.key() == "security" && tag.value() == "public"));

        let untag = untag_resource(&bucket, &["env"]);
        assert_eq!(untag.status, 204, "UntagResource failed: {:?}", untag);

        let get = eventually_ok("GetBucketTagging after UntagResource", || {
            client.get_bucket_tagging().bucket(&bucket).send()
        })
        .await;
        assert_eq!(get.tag_set().len(), 1);
        assert_eq!(get.tag_set()[0].key(), "security");
        assert_eq!(get.tag_set()[0].value(), "public");

        cleanup_bucket(owner_root_client(), client, &bucket).await;
    });
}

#[test]
fn test_same_account_root_and_non_root_bucket_cors_admin() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let non_root_client = CTX.client();
        let root_bucket = create_standard_bucket(root_client).await;
        let non_root_bucket = create_standard_bucket(non_root_client).await;
        let config = simple_cors_config();

        for (description, bucket, admin_client) in [
            (
                "non-root manages root-created bucket CORS",
                root_bucket.as_str(),
                non_root_client,
            ),
            (
                "root manages non-root-created bucket CORS",
                non_root_bucket.as_str(),
                root_client,
            ),
        ] {
            eventually_ok(&format!("{description} PutBucketCors"), || {
                admin_client
                    .put_bucket_cors()
                    .bucket(bucket)
                    .cors_configuration(config.clone())
                    .send()
            })
            .await;

            let get = eventually_ok(&format!("{description} GetBucketCors"), || {
                admin_client.get_bucket_cors().bucket(bucket).send()
            })
            .await;
            assert_eq!(get.cors_rules().len(), 1);
            assert_eq!(
                get.cors_rules()[0].allowed_origins(),
                ["https://example.com"]
            );

            eventually_ok(&format!("{description} DeleteBucketCors"), || {
                admin_client.delete_bucket_cors().bucket(bucket).send()
            })
            .await;

            eventually_err_status(
                &format!("{description} GetBucketCors after delete"),
                404,
                Some("NoSuchCORSConfiguration"),
                || admin_client.get_bucket_cors().bucket(bucket).send(),
            )
            .await;
        }

        cleanup_bucket(root_client, non_root_client, &root_bucket).await;
        cleanup_bucket(root_client, non_root_client, &non_root_bucket).await;
    });
}

#[test]
fn test_same_account_root_and_non_root_bucket_lifecycle_admin() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let non_root_client = CTX.client();
        let root_bucket = create_standard_bucket(root_client).await;
        let non_root_bucket = create_standard_bucket(non_root_client).await;
        let config = simple_lifecycle_config();

        for (description, bucket, admin_client) in [
            (
                "non-root manages root-created bucket lifecycle",
                root_bucket.as_str(),
                non_root_client,
            ),
            (
                "root manages non-root-created bucket lifecycle",
                non_root_bucket.as_str(),
                root_client,
            ),
        ] {
            eventually_ok(
                &format!("{description} PutBucketLifecycleConfiguration"),
                || put_bucket_lifecycle_with_md5(admin_client, bucket, config.clone()).send(),
            )
            .await;

            let get = eventually_ok(
                &format!("{description} GetBucketLifecycleConfiguration"),
                || {
                    admin_client
                        .get_bucket_lifecycle_configuration()
                        .bucket(bucket)
                        .send()
                },
            )
            .await;
            assert_eq!(get.rules().len(), 1);
            assert_eq!(get.rules()[0].id(), Some("expire-logs"));

            eventually_ok(&format!("{description} DeleteBucketLifecycle"), || {
                admin_client.delete_bucket_lifecycle().bucket(bucket).send()
            })
            .await;

            eventually_err_status(
                &format!("{description} GetBucketLifecycleConfiguration after delete"),
                404,
                Some("NoSuchLifecycleConfiguration"),
                || {
                    admin_client
                        .get_bucket_lifecycle_configuration()
                        .bucket(bucket)
                        .send()
                },
            )
            .await;
        }

        cleanup_bucket(root_client, non_root_client, &root_bucket).await;
        cleanup_bucket(root_client, non_root_client, &non_root_bucket).await;
    });
}

#[test]
fn test_same_account_root_and_non_root_bucket_encryption_admin() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let non_root_client = CTX.client();
        let root_bucket = create_standard_bucket(root_client).await;
        let non_root_bucket = create_standard_bucket(non_root_client).await;
        let config = simple_bucket_encryption();

        for (description, bucket, admin_client) in [
            (
                "non-root manages root-created bucket encryption",
                root_bucket.as_str(),
                non_root_client,
            ),
            (
                "root manages non-root-created bucket encryption",
                non_root_bucket.as_str(),
                root_client,
            ),
        ] {
            eventually_ok(&format!("{description} PutBucketEncryption"), || {
                admin_client
                    .put_bucket_encryption()
                    .bucket(bucket)
                    .server_side_encryption_configuration(config.clone())
                    .send()
            })
            .await;

            let get = eventually_ok(&format!("{description} GetBucketEncryption"), || {
                admin_client.get_bucket_encryption().bucket(bucket).send()
            })
            .await;
            assert_eq!(
                get.server_side_encryption_configuration().unwrap().rules()[0]
                    .apply_server_side_encryption_by_default()
                    .unwrap()
                    .sse_algorithm(),
                &ServerSideEncryption::Aes256
            );

            eventually_ok(&format!("{description} DeleteBucketEncryption"), || {
                admin_client
                    .delete_bucket_encryption()
                    .bucket(bucket)
                    .send()
            })
            .await;
        }

        cleanup_bucket(root_client, non_root_client, &root_bucket).await;
        cleanup_bucket(root_client, non_root_client, &non_root_bucket).await;
    });
}

#[test]
fn test_same_account_root_and_non_root_object_lock_configuration_admin() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let non_root_client = CTX.client();
        let root_bucket = create_object_lock_bucket(root_client).await;
        let non_root_bucket = create_object_lock_bucket(non_root_client).await;
        let config = simple_object_lock_config();

        for (description, bucket, admin_client) in [
            (
                "non-root manages root-created object lock configuration",
                root_bucket.as_str(),
                non_root_client,
            ),
            (
                "root manages non-root-created object lock configuration",
                non_root_bucket.as_str(),
                root_client,
            ),
        ] {
            eventually_ok(&format!("{description} PutObjectLockConfiguration"), || {
                admin_client
                    .put_object_lock_configuration()
                    .bucket(bucket)
                    .object_lock_configuration(config.clone())
                    .send()
            })
            .await;

            let get = eventually_ok(&format!("{description} GetObjectLockConfiguration"), || {
                admin_client
                    .get_object_lock_configuration()
                    .bucket(bucket)
                    .send()
            })
            .await;
            assert_eq!(get.object_lock_configuration(), Some(&config));
        }

        cleanup_bucket(root_client, non_root_client, &root_bucket).await;
        cleanup_bucket(root_client, non_root_client, &non_root_bucket).await;
    });
}
