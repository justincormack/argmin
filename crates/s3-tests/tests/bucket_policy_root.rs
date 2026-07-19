use std::future::Future;
use std::time::Duration;

use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    AbacStatus, BucketAbacStatus, PublicAccessBlockConfiguration, Tag, Tagging,
};
use s3_tests::{unique_bucket, CTX};
use serde_json::json;

fn owner_root_client() -> &'static aws_sdk_s3::Client {
    CTX.require_owner_root_client()
}

fn bucket_owner_client(_root_client: &aws_sdk_s3::Client) -> &aws_sdk_s3::Client {
    CTX.client()
}

fn owner_root_principal() -> serde_json::Value {
    json!({ "AWS": format!("arn:aws:iam::{}:root", CTX.account_id()) })
}

fn bucket_resource(bucket: &str) -> String {
    format!("arn:aws:s3:::{bucket}")
}

fn deny_policy(bucket: &str, action: &str) -> String {
    json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Deny",
            "Principal": owner_root_principal(),
            "Action": action,
            "Resource": bucket_resource(bucket),
        }],
    })
    .to_string()
}

fn deny_root_bucket_policy_crud_policy(bucket: &str) -> String {
    json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Deny",
            "Principal": owner_root_principal(),
            "Action": [
                "s3:GetBucketPolicy",
                "s3:PutBucketPolicy",
                "s3:DeleteBucketPolicy",
            ],
            "Resource": bucket_resource(bucket),
        }],
    })
    .to_string()
}

fn public_list_policy(bucket: &str) -> String {
    json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:ListBucket",
            "Resource": bucket_resource(bucket),
        }],
    })
    .to_string()
}

fn non_public_root_get_policy(bucket: &str) -> String {
    json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": owner_root_principal(),
            "Action": "s3:GetBucketPolicy",
            "Resource": bucket_resource(bucket),
        }],
    })
    .to_string()
}

fn enabled_abac_status() -> AbacStatus {
    AbacStatus::builder()
        .status(BucketAbacStatus::Enabled)
        .build()
}

fn simple_bucket_tagging(key: &str, value: &str) -> Tagging {
    Tagging::builder()
        .tag_set(Tag::builder().key(key).value(value).build().unwrap())
        .build()
        .unwrap()
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

async fn same_account_exact_principal(root_client: &aws_sdk_s3::Client) -> String {
    if std::env::var_os("S3_TEST_ENDPOINT").is_none() {
        return format!("arn:aws:iam::{}:user/limited", CTX.account_id());
    }
    if let Ok(principal) = std::env::var("AWS_TEST_SECOND_PRINCIPAL") {
        assert!(
            principal.starts_with("arn:aws:iam::"),
            "AWS_TEST_SECOND_PRINCIPAL must be an IAM ARN"
        );
        return principal;
    }

    let client = CTX.require_second_client();
    let bucket = unique_bucket();
    s3_tests::create_bucket(root_client, &bucket).await.unwrap();
    let denied = client
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
    cleanup_bucket(root_client, &bucket).await;
    principal
}

fn bucket_tag_condition_policy(bucket: &str, principal: &str, action: &str, value: &str) -> String {
    json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": { "AWS": principal },
            "Action": action,
            "Resource": bucket_resource(bucket),
            "Condition": {
                "StringEquals": {
                    "s3:BucketTag/security": value
                }
            }
        }],
    })
    .to_string()
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

async fn eventually_access_denied<T, E, F, Fut>(description: &str, mut op: F)
where
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
                    == Some(403) =>
            {
                return;
            }
            _ if attempt + 1 < MAX_ATTEMPTS => {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            _ => panic!("{description} did not converge to AccessDenied"),
        }
    }

    unreachable!()
}

async fn eventually_no_such_bucket_policy<F, Fut>(description: &str, mut op: F)
where
    F: FnMut() -> Fut,
    Fut: Future<
        Output = Result<
            aws_sdk_s3::operation::get_bucket_policy::GetBucketPolicyOutput,
            aws_sdk_s3::error::SdkError<
                aws_sdk_s3::operation::get_bucket_policy::GetBucketPolicyError,
            >,
        >,
    >,
{
    const MAX_ATTEMPTS: usize = 20;

    for attempt in 0..MAX_ATTEMPTS {
        let result = op().await;
        match &result {
            Err(err)
                if err
                    .raw_response()
                    .map(|response| response.status().as_u16())
                    == Some(404)
                    && err.as_service_error().and_then(ProvideErrorMetadata::code)
                        == Some("NoSuchBucketPolicy") =>
            {
                return;
            }
            _ if attempt + 1 < MAX_ATTEMPTS => {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            _ => panic!("{description} did not converge to NoSuchBucketPolicy: {result:?}"),
        }
    }

    unreachable!()
}

async fn eventually_block_public_policy_denied(
    bucket: &str,
    policy: &str,
    root_client: &aws_sdk_s3::Client,
) {
    const MAX_ATTEMPTS: usize = 20;

    for attempt in 0..MAX_ATTEMPTS {
        let result = root_client
            .put_bucket_policy()
            .bucket(bucket)
            .policy(policy)
            .send()
            .await;
        match &result {
            Err(err)
                if err
                    .raw_response()
                    .map(|response| response.status().as_u16())
                    == Some(403)
                    && err.as_service_error().and_then(ProvideErrorMetadata::code)
                        == Some("AccessDenied")
                    && format!("{err:?}").contains("BlockPublicPolicy") =>
            {
                return;
            }
            _ if attempt + 1 < MAX_ATTEMPTS => {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            _ => panic!("PutBucketPolicy did not converge to BlockPublicPolicy denial: {result:?}"),
        }
    }

    unreachable!()
}

async fn cleanup_bucket(root_client: &aws_sdk_s3::Client, bucket: &str) {
    let _ = root_client
        .delete_bucket_policy()
        .bucket(bucket)
        .send()
        .await;
    let _ = root_client
        .delete_public_access_block()
        .bucket(bucket)
        .send()
        .await;
    s3_tests::delete_bucket_retrying_operation_aborted(root_client, bucket).await;
}

#[test]
fn test_owner_root_get_bucket_policy_bypasses_explicit_deny() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let client = CTX.client();
        let owner_client = bucket_owner_client(root_client);
        let bucket = unique_bucket();
        s3_tests::create_bucket(owner_client, &bucket)
            .await
            .unwrap();

        let policy = deny_policy(&bucket, "s3:GetBucketPolicy");
        root_client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy.clone())
            .send()
            .await
            .unwrap();

        let fetched = eventually_ok("owner-root GetBucketPolicy", || {
            root_client.get_bucket_policy().bucket(&bucket).send()
        })
        .await;
        let fetched_policy: serde_json::Value =
            serde_json::from_str(fetched.policy().unwrap()).unwrap();
        let expected_policy: serde_json::Value = serde_json::from_str(&policy).unwrap();
        assert_eq!(fetched_policy, expected_policy);

        eventually_access_denied("same-account non-root GetBucketPolicy", || {
            client.get_bucket_policy().bucket(&bucket).send()
        })
        .await;

        cleanup_bucket(root_client, &bucket).await;
    });
}

#[test]
fn test_owner_root_put_bucket_policy_bypasses_explicit_deny() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let client = CTX.client();
        let owner_client = bucket_owner_client(root_client);
        let bucket = unique_bucket();
        s3_tests::create_bucket(owner_client, &bucket)
            .await
            .unwrap();

        root_client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(deny_policy(&bucket, "s3:PutBucketPolicy"))
            .send()
            .await
            .unwrap();

        let replacement = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": owner_root_principal(),
                "Action": "s3:GetBucketPolicy",
                "Resource": bucket_resource(&bucket),
            }],
        })
        .to_string();

        eventually_access_denied("same-account non-root PutBucketPolicy", || {
            client
                .put_bucket_policy()
                .bucket(&bucket)
                .policy(replacement.clone())
                .send()
        })
        .await;

        eventually_ok("owner-root PutBucketPolicy", || {
            root_client
                .put_bucket_policy()
                .bucket(&bucket)
                .policy(replacement.clone())
                .send()
        })
        .await;

        let fetched = eventually_ok("owner-root GetBucketPolicy after PutBucketPolicy", || {
            root_client.get_bucket_policy().bucket(&bucket).send()
        })
        .await;
        let fetched_policy: serde_json::Value =
            serde_json::from_str(fetched.policy().unwrap()).unwrap();
        let expected_policy: serde_json::Value = serde_json::from_str(&replacement).unwrap();
        assert_eq!(fetched_policy, expected_policy);

        cleanup_bucket(root_client, &bucket).await;
    });
}

#[test]
fn test_owner_root_delete_bucket_policy_bypasses_explicit_deny() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let client = CTX.client();
        let owner_client = bucket_owner_client(root_client);
        let bucket = unique_bucket();
        s3_tests::create_bucket(owner_client, &bucket)
            .await
            .unwrap();

        root_client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(deny_policy(&bucket, "s3:DeleteBucketPolicy"))
            .send()
            .await
            .unwrap();

        eventually_access_denied("same-account non-root DeleteBucketPolicy", || {
            client.delete_bucket_policy().bucket(&bucket).send()
        })
        .await;

        eventually_ok("owner-root DeleteBucketPolicy", || {
            root_client.delete_bucket_policy().bucket(&bucket).send()
        })
        .await;

        eventually_no_such_bucket_policy(
            "GetBucketPolicy after owner-root DeleteBucketPolicy",
            || root_client.get_bucket_policy().bucket(&bucket).send(),
        )
        .await;

        cleanup_bucket(root_client, &bucket).await;
    });
}

#[test]
fn test_owner_root_get_bucket_policy_status_has_no_carveout() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let owner_client = bucket_owner_client(root_client);
        let bucket = unique_bucket();
        s3_tests::create_bucket(owner_client, &bucket)
            .await
            .unwrap();

        root_client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(deny_policy(&bucket, "s3:GetBucketPolicyStatus"))
            .send()
            .await
            .unwrap();

        eventually_access_denied("owner-root GetBucketPolicyStatus", || {
            root_client
                .get_bucket_policy_status()
                .bucket(&bucket)
                .send()
        })
        .await;

        cleanup_bucket(root_client, &bucket).await;
    });
}

#[test]
fn test_owner_root_put_bucket_policy_still_blocked_by_block_public_policy() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let owner_client = bucket_owner_client(root_client);
        let bucket = unique_bucket();
        s3_tests::create_bucket(owner_client, &bucket)
            .await
            .unwrap();

        root_client
            .put_public_access_block()
            .bucket(&bucket)
            .public_access_block_configuration(
                PublicAccessBlockConfiguration::builder()
                    .block_public_acls(false)
                    .ignore_public_acls(false)
                    .block_public_policy(true)
                    .restrict_public_buckets(false)
                    .build(),
            )
            .send()
            .await
            .unwrap();

        eventually_block_public_policy_denied(&bucket, &public_list_policy(&bucket), root_client)
            .await;

        cleanup_bucket(root_client, &bucket).await;
    });
}

#[test]
fn test_owner_root_confirm_remove_self_bucket_access_does_not_disable_root_carveout() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let client = CTX.client();
        let owner_client = bucket_owner_client(root_client);
        let bucket = unique_bucket();
        s3_tests::create_bucket(owner_client, &bucket)
            .await
            .unwrap();

        let deny_policy = deny_root_bucket_policy_crud_policy(&bucket);
        eventually_ok(
            "owner-root PutBucketPolicy with ConfirmRemoveSelfBucketAccess",
            || {
                root_client
                    .put_bucket_policy()
                    .bucket(&bucket)
                    .policy(deny_policy.clone())
                    .confirm_remove_self_bucket_access(true)
                    .send()
            },
        )
        .await;

        eventually_access_denied(
            "same-account non-root GetBucketPolicy after confirm header",
            || client.get_bucket_policy().bucket(&bucket).send(),
        )
        .await;

        eventually_ok("owner-root GetBucketPolicy after confirm header", || {
            root_client.get_bucket_policy().bucket(&bucket).send()
        })
        .await;

        eventually_access_denied(
            "same-account non-root DeleteBucketPolicy after confirm header",
            || client.delete_bucket_policy().bucket(&bucket).send(),
        )
        .await;

        eventually_ok("owner-root DeleteBucketPolicy after confirm header", || {
            root_client.delete_bucket_policy().bucket(&bucket).send()
        })
        .await;

        eventually_ok("owner-root PutBucketPolicy after confirm header", || {
            root_client
                .put_bucket_policy()
                .bucket(&bucket)
                .policy(non_public_root_get_policy(&bucket))
                .send()
        })
        .await;

        cleanup_bucket(root_client, &bucket).await;
    });
}

#[test]
fn test_owner_root_confirm_remove_self_bucket_access_accepts_non_locking_policy() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let owner_client = bucket_owner_client(root_client);
        let bucket = unique_bucket();
        s3_tests::create_bucket(owner_client, &bucket)
            .await
            .unwrap();

        let policy = non_public_root_get_policy(&bucket);
        let put = root_client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy.clone())
            .confirm_remove_self_bucket_access(true)
            .send()
            .await;
        put.unwrap();

        let fetched = eventually_ok(
            "owner-root GetBucketPolicy after ConfirmRemoveSelfBucketAccess non-locking policy",
            || root_client.get_bucket_policy().bucket(&bucket).send(),
        )
        .await;
        let fetched_policy: serde_json::Value =
            serde_json::from_str(fetched.policy().unwrap()).unwrap();
        let expected_policy: serde_json::Value = serde_json::from_str(&policy).unwrap();
        assert_eq!(fetched_policy, expected_policy);

        cleanup_bucket(root_client, &bucket).await;
    });
}

#[test]
fn test_same_account_non_root_get_bucket_policy_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let client = CTX.require_second_client();
        let principal = same_account_exact_principal(root_client).await;

        let public_bucket = unique_bucket();
        s3_tests::create_bucket(root_client, &public_bucket)
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(root_client, &public_bucket, "public").await;
        let public_policy =
            bucket_tag_condition_policy(&public_bucket, &principal, "s3:GetBucketPolicy", "public");
        root_client
            .put_bucket_policy()
            .bucket(&public_bucket)
            .policy(public_policy.clone())
            .send()
            .await
            .unwrap();

        let private_bucket = unique_bucket();
        s3_tests::create_bucket(root_client, &private_bucket)
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(root_client, &private_bucket, "private").await;
        let private_policy = bucket_tag_condition_policy(
            &private_bucket,
            &principal,
            "s3:GetBucketPolicy",
            "public",
        );
        root_client
            .put_bucket_policy()
            .bucket(&private_bucket)
            .policy(private_policy)
            .send()
            .await
            .unwrap();

        let fetched = eventually_ok(
            "same-account non-root GetBucketPolicy with public bucket tag",
            || client.get_bucket_policy().bucket(&public_bucket).send(),
        )
        .await;
        let fetched_policy: serde_json::Value =
            serde_json::from_str(fetched.policy().unwrap()).unwrap();
        let expected_policy: serde_json::Value = serde_json::from_str(&public_policy).unwrap();
        assert_eq!(fetched_policy, expected_policy);

        eventually_access_denied(
            "same-account non-root GetBucketPolicy denied for private bucket tag",
            || client.get_bucket_policy().bucket(&private_bucket).send(),
        )
        .await;

        cleanup_bucket(root_client, &public_bucket).await;
        cleanup_bucket(root_client, &private_bucket).await;
    });
}

#[test]
fn test_same_account_non_root_put_bucket_policy_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let client = CTX.require_second_client();
        let principal = same_account_exact_principal(root_client).await;

        let public_bucket = unique_bucket();
        s3_tests::create_bucket(root_client, &public_bucket)
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(root_client, &public_bucket, "public").await;
        root_client
            .put_bucket_policy()
            .bucket(&public_bucket)
            .policy(bucket_tag_condition_policy(
                &public_bucket,
                &principal,
                "s3:PutBucketPolicy",
                "public",
            ))
            .send()
            .await
            .unwrap();

        let private_bucket = unique_bucket();
        s3_tests::create_bucket(root_client, &private_bucket)
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(root_client, &private_bucket, "private").await;
        root_client
            .put_bucket_policy()
            .bucket(&private_bucket)
            .policy(bucket_tag_condition_policy(
                &private_bucket,
                &principal,
                "s3:PutBucketPolicy",
                "public",
            ))
            .send()
            .await
            .unwrap();

        let replacement = non_public_root_get_policy(&public_bucket);
        eventually_ok(
            "same-account non-root PutBucketPolicy with public bucket tag",
            || {
                client
                    .put_bucket_policy()
                    .bucket(&public_bucket)
                    .policy(replacement.clone())
                    .send()
            },
        )
        .await;

        let fetched = eventually_ok(
            "owner-root GetBucketPolicy after non-root PutBucketPolicy",
            || {
                root_client
                    .get_bucket_policy()
                    .bucket(&public_bucket)
                    .send()
            },
        )
        .await;
        let fetched_policy: serde_json::Value =
            serde_json::from_str(fetched.policy().unwrap()).unwrap();
        let expected_policy: serde_json::Value = serde_json::from_str(&replacement).unwrap();
        assert_eq!(fetched_policy, expected_policy);

        eventually_access_denied(
            "same-account non-root PutBucketPolicy denied for private bucket tag",
            || {
                client
                    .put_bucket_policy()
                    .bucket(&private_bucket)
                    .policy(non_public_root_get_policy(&private_bucket))
                    .send()
            },
        )
        .await;

        cleanup_bucket(root_client, &public_bucket).await;
        cleanup_bucket(root_client, &private_bucket).await;
    });
}

#[test]
fn test_same_account_non_root_delete_bucket_policy_bucket_tag_condition_when_abac_enabled() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let client = CTX.require_second_client();
        let principal = same_account_exact_principal(root_client).await;

        let public_bucket = unique_bucket();
        s3_tests::create_bucket(root_client, &public_bucket)
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(root_client, &public_bucket, "public").await;
        root_client
            .put_bucket_policy()
            .bucket(&public_bucket)
            .policy(bucket_tag_condition_policy(
                &public_bucket,
                &principal,
                "s3:DeleteBucketPolicy",
                "public",
            ))
            .send()
            .await
            .unwrap();

        let private_bucket = unique_bucket();
        s3_tests::create_bucket(root_client, &private_bucket)
            .await
            .unwrap();
        enable_bucket_abac_with_security_tag(root_client, &private_bucket, "private").await;
        root_client
            .put_bucket_policy()
            .bucket(&private_bucket)
            .policy(bucket_tag_condition_policy(
                &private_bucket,
                &principal,
                "s3:DeleteBucketPolicy",
                "public",
            ))
            .send()
            .await
            .unwrap();

        eventually_ok(
            "same-account non-root DeleteBucketPolicy with public bucket tag",
            || client.delete_bucket_policy().bucket(&public_bucket).send(),
        )
        .await;

        eventually_no_such_bucket_policy(
            "owner-root GetBucketPolicy after non-root DeleteBucketPolicy",
            || {
                root_client
                    .get_bucket_policy()
                    .bucket(&public_bucket)
                    .send()
            },
        )
        .await;

        eventually_access_denied(
            "same-account non-root DeleteBucketPolicy denied for private bucket tag",
            || client.delete_bucket_policy().bucket(&private_bucket).send(),
        )
        .await;

        cleanup_bucket(root_client, &public_bucket).await;
        cleanup_bucket(root_client, &private_bucket).await;
    });
}
