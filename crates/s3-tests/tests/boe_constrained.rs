// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::time::Duration;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, ObjectAttributes, ObjectOwnership, Tag, Tagging,
    VersioningConfiguration,
};
use s3_tests::{
    assert_s3_err_code, cleanup_versioned_bucket, err_status, unique_bucket,
    SendRetryingOperationAborted, CTX,
};

fn owner_root_client() -> &'static aws_sdk_s3::Client {
    CTX.require_owner_root_client()
}

fn constrained_client() -> &'static aws_sdk_s3::Client {
    CTX.require_second_client()
}

async fn eventually_access_denied<T, E, F, Fut>(description: &str, mut op: F)
where
    T: std::fmt::Debug,
    E: std::fmt::Debug,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, aws_sdk_s3::error::SdkError<E>>>,
{
    const MAX_ATTEMPTS: usize = 20;

    for attempt in 0..MAX_ATTEMPTS {
        let result = op().await;
        if result.is_err() && err_status(&result) == 403 {
            assert_s3_err_code(&result, "AccessDenied");
            return;
        }
        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }
        panic!("{description} did not converge to AccessDenied: {result:?}");
    }

    unreachable!()
}

async fn create_boe_bucket(client: &aws_sdk_s3::Client) -> String {
    let bucket = unique_bucket();
    s3_tests::create_bucket_request(client, &bucket)
        .object_ownership(ObjectOwnership::BucketOwnerEnforced)
        .send()
        .await
        .unwrap();
    bucket
}

async fn create_versioned_boe_bucket(client: &aws_sdk_s3::Client) -> String {
    let bucket = create_boe_bucket(client).await;
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

fn tagging(key: &str, value: &str) -> Tagging {
    Tagging::builder()
        .tag_set(Tag::builder().key(key).value(value).build().unwrap())
        .build()
        .unwrap()
}

async fn cleanup_bucket(root_client: &aws_sdk_s3::Client, bucket: &str, keys: &[&str]) {
    for key in keys {
        let _ = root_client
            .delete_object()
            .bucket(bucket)
            .key(*key)
            .send()
            .await;
    }

    s3_tests::delete_bucket_retrying_operation_aborted(root_client, bucket).await;
}

async fn cleanup_maybe_versioned_bucket(
    root_client: &aws_sdk_s3::Client,
    bucket: &str,
    keys: &[&str],
    versioned: bool,
) {
    if versioned {
        cleanup_versioned_bucket(root_client, bucket).await;
    } else {
        cleanup_bucket(root_client, bucket, keys).await;
    }
}

#[test]
fn test_same_account_constrained_user_cannot_read_boe_object() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let limited_client = constrained_client();
        let bucket = create_boe_bucket(root_client).await;
        let key = "root-owned";

        root_client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        eventually_access_denied("constrained HeadObject on BOE object", || {
            limited_client.head_object().bucket(&bucket).key(key).send()
        })
        .await;
        eventually_access_denied("constrained GetObject on BOE object", || {
            limited_client.get_object().bucket(&bucket).key(key).send()
        })
        .await;

        cleanup_bucket(root_client, &bucket, &[key]).await;
    });
}

#[test]
fn test_same_account_constrained_user_cannot_manage_boe_object_tags() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let limited_client = constrained_client();
        let bucket = create_boe_bucket(root_client).await;
        let key = "root-owned";

        root_client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();
        root_client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .tagging(tagging("owner", "set"))
            .send_retrying_operation_aborted("set BOE object tags during constrained setup")
            .await
            .unwrap();

        eventually_access_denied("constrained GetObjectTagging on BOE object", || {
            limited_client
                .get_object_tagging()
                .bucket(&bucket)
                .key(key)
                .send()
        })
        .await;
        eventually_access_denied("constrained PutObjectTagging on BOE object", || {
            limited_client
                .put_object_tagging()
                .bucket(&bucket)
                .key(key)
                .tagging(tagging("limited", "denied"))
                .send()
        })
        .await;
        eventually_access_denied("constrained DeleteObjectTagging on BOE object", || {
            limited_client
                .delete_object_tagging()
                .bucket(&bucket)
                .key(key)
                .send()
        })
        .await;

        let current_tags = root_client
            .get_object_tagging()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(current_tags.tag_set().len(), 1);
        assert_eq!(current_tags.tag_set()[0].key(), "owner");
        assert_eq!(current_tags.tag_set()[0].value(), "set");

        cleanup_bucket(root_client, &bucket, &[key]).await;
    });
}

#[test]
fn test_same_account_constrained_user_cannot_get_boe_bucket_acl() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let limited_client = constrained_client();
        let bucket = create_boe_bucket(root_client).await;

        eventually_access_denied("constrained GetBucketAcl on BOE bucket", || {
            limited_client.get_bucket_acl().bucket(&bucket).send()
        })
        .await;

        cleanup_bucket(root_client, &bucket, &[]).await;
    });
}

#[test]
fn test_same_account_constrained_user_cannot_get_boe_object_acl() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let limited_client = constrained_client();
        let bucket = create_boe_bucket(root_client).await;
        let key = "root-owned";

        root_client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        eventually_access_denied("constrained GetObjectAcl on BOE object", || {
            limited_client
                .get_object_acl()
                .bucket(&bucket)
                .key(key)
                .send()
        })
        .await;

        cleanup_bucket(root_client, &bucket, &[key]).await;
    });
}

#[test]
fn test_same_account_constrained_user_cannot_discover_missing_boe_object() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let limited_client = constrained_client();
        let bucket = create_boe_bucket(root_client).await;
        let missing_key = "missing";

        eventually_access_denied("constrained HeadObject on missing BOE object", || {
            limited_client
                .head_object()
                .bucket(&bucket)
                .key(missing_key)
                .send()
        })
        .await;
        eventually_access_denied("constrained GetObject on missing BOE object", || {
            limited_client
                .get_object()
                .bucket(&bucket)
                .key(missing_key)
                .send()
        })
        .await;
        eventually_access_denied("constrained GetObjectAcl on missing BOE object", || {
            limited_client
                .get_object_acl()
                .bucket(&bucket)
                .key(missing_key)
                .send()
        })
        .await;
        eventually_access_denied(
            "constrained GetObjectAttributes on missing BOE object",
            || {
                limited_client
                    .get_object_attributes()
                    .bucket(&bucket)
                    .key(missing_key)
                    .object_attributes(ObjectAttributes::ObjectSize)
                    .send()
            },
        )
        .await;

        cleanup_bucket(root_client, &bucket, &[]).await;
    });
}

#[test]
fn test_same_account_constrained_user_cannot_get_boe_object_attributes() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let limited_client = constrained_client();
        let bucket = create_versioned_boe_bucket(root_client).await;
        let key = "root-owned";

        let put = root_client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"attributes"))
            .send()
            .await
            .unwrap();
        let version_id = put.version_id().unwrap().to_string();

        eventually_access_denied("constrained GetObjectAttributes on BOE object", || {
            limited_client
                .get_object_attributes()
                .bucket(&bucket)
                .key(key)
                .object_attributes(ObjectAttributes::ObjectSize)
                .send()
        })
        .await;
        eventually_access_denied(
            "constrained GetObjectAttributes on BOE object version",
            || {
                limited_client
                    .get_object_attributes()
                    .bucket(&bucket)
                    .key(key)
                    .version_id(&version_id)
                    .object_attributes(ObjectAttributes::ObjectSize)
                    .send()
            },
        )
        .await;

        cleanup_maybe_versioned_bucket(root_client, &bucket, &[key], true).await;
    });
}

#[test]
fn test_same_account_constrained_user_cannot_read_boe_object_version() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let limited_client = constrained_client();
        let bucket = create_versioned_boe_bucket(root_client).await;
        let key = "root-owned";

        let put = root_client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"versioned"))
            .send()
            .await
            .unwrap();
        let version_id = put.version_id().unwrap().to_string();

        eventually_access_denied("constrained HeadObject on BOE object version", || {
            limited_client
                .head_object()
                .bucket(&bucket)
                .key(key)
                .version_id(&version_id)
                .send()
        })
        .await;
        eventually_access_denied("constrained GetObject on BOE object version", || {
            limited_client
                .get_object()
                .bucket(&bucket)
                .key(key)
                .version_id(&version_id)
                .send()
        })
        .await;

        cleanup_maybe_versioned_bucket(root_client, &bucket, &[key], true).await;
    });
}

#[test]
fn test_same_account_constrained_user_cannot_manage_boe_object_version_tags() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let limited_client = constrained_client();
        let bucket = create_versioned_boe_bucket(root_client).await;
        let key = "root-owned";

        let put = root_client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"versioned"))
            .send()
            .await
            .unwrap();
        let version_id = put.version_id().unwrap().to_string();

        root_client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .tagging(tagging("owner", "version"))
            .send_retrying_operation_aborted("set BOE object version tags during constrained setup")
            .await
            .unwrap();

        eventually_access_denied("constrained GetObjectTagging on BOE object version", || {
            limited_client
                .get_object_tagging()
                .bucket(&bucket)
                .key(key)
                .version_id(&version_id)
                .send()
        })
        .await;
        eventually_access_denied("constrained PutObjectTagging on BOE object version", || {
            limited_client
                .put_object_tagging()
                .bucket(&bucket)
                .key(key)
                .version_id(&version_id)
                .tagging(tagging("limited", "denied"))
                .send()
        })
        .await;
        eventually_access_denied(
            "constrained DeleteObjectTagging on BOE object version",
            || {
                limited_client
                    .delete_object_tagging()
                    .bucket(&bucket)
                    .key(key)
                    .version_id(&version_id)
                    .send()
            },
        )
        .await;

        let current_tags = root_client
            .get_object_tagging()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .send()
            .await
            .unwrap();
        assert_eq!(current_tags.tag_set().len(), 1);
        assert_eq!(current_tags.tag_set()[0].key(), "owner");
        assert_eq!(current_tags.tag_set()[0].value(), "version");

        cleanup_maybe_versioned_bucket(root_client, &bucket, &[key], true).await;
    });
}

#[test]
fn test_same_account_constrained_user_cannot_get_boe_object_version_acl() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let limited_client = constrained_client();
        let bucket = create_versioned_boe_bucket(root_client).await;
        let key = "root-owned";

        let put = root_client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"versioned"))
            .send()
            .await
            .unwrap();
        let version_id = put.version_id().unwrap().to_string();

        eventually_access_denied("constrained GetObjectAcl on BOE object version", || {
            limited_client
                .get_object_acl()
                .bucket(&bucket)
                .key(key)
                .version_id(&version_id)
                .send()
        })
        .await;

        cleanup_maybe_versioned_bucket(root_client, &bucket, &[key], true).await;
    });
}
