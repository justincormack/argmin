use std::future::Future;
use std::time::Duration;

use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::types::{
    BucketLifecycleConfiguration, BucketVersioningStatus, CorsConfiguration, CorsRule,
    DefaultRetention, ExpirationStatus, LifecycleExpiration, LifecycleRule, LifecycleRuleFilter,
    ObjectLockConfiguration, ObjectLockEnabled, ObjectLockRetentionMode, ObjectLockRule,
    ObjectOwnership, OwnershipControls, OwnershipControlsRule, PublicAccessBlockConfiguration,
    ServerSideEncryption, ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration,
    ServerSideEncryptionRule, Tag, Tagging, VersioningConfiguration,
};
use s3_tests::{put_bucket_lifecycle_with_md5, unique_bucket, CTX};

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

async fn eventually_list_contains(
    client: &aws_sdk_s3::Client,
    expected_buckets: &[&str],
    description: &str,
) {
    const MAX_ATTEMPTS: usize = 20;

    for attempt in 0..MAX_ATTEMPTS {
        let result = client.list_buckets().send().await;
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
        let _ = client.delete_bucket_policy().bucket(bucket).send().await;
        let _ = client.delete_bucket_cors().bucket(bucket).send().await;
        let _ = client.delete_bucket_tagging().bucket(bucket).send().await;
        let _ = client.delete_bucket_lifecycle().bucket(bucket).send().await;
        let _ = client
            .delete_public_access_block()
            .bucket(bucket)
            .send()
            .await;
        let _ = client
            .delete_bucket_ownership_controls()
            .bucket(bucket)
            .send()
            .await;
        let _ = client
            .delete_bucket_encryption()
            .bucket(bucket)
            .send()
            .await;
    }

    for client in [root_client, non_root_client] {
        if client.delete_bucket().bucket(bucket).send().await.is_ok() {
            return;
        }
    }

    panic!("bucket cleanup delete failed for {bucket}");
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
