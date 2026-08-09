// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::{ByteStream, DateTime};
use aws_sdk_s3::types::{
    ObjectCannedAcl, ObjectLockLegalHold, ObjectLockLegalHoldStatus, ObjectLockRetention,
    ObjectLockRetentionMode, ObjectOwnership, Tag, Tagging,
};
use s3_tests::{
    cleanup_versioned_bucket, retrying_operation_aborted, unique_bucket,
    SendRetryingOperationAborted, CTX,
};

fn owner_root_client() -> &'static aws_sdk_s3::Client {
    CTX.require_owner_root_client()
}

async fn create_acl_enabled_bucket(client: &aws_sdk_s3::Client) -> String {
    let bucket = unique_bucket();
    s3_tests::create_bucket_request(client, &bucket)
        .object_ownership(ObjectOwnership::ObjectWriter)
        .send_retrying_operation_aborted("create object admin ACL-enabled bucket")
        .await
        .unwrap();
    bucket
}

async fn create_standard_bucket(client: &aws_sdk_s3::Client) -> String {
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    bucket
}

async fn create_object_lock_bucket(client: &aws_sdk_s3::Client) -> String {
    let bucket = unique_bucket();
    s3_tests::create_bucket_request(client, &bucket)
        .object_lock_enabled_for_bucket(true)
        .send_retrying_operation_aborted("create object admin object-lock bucket")
        .await
        .unwrap();
    bucket
}

async fn put_object(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    body: &'static [u8],
) -> aws_sdk_s3::operation::put_object::PutObjectOutput {
    retrying_operation_aborted("put object admin test object", || async move {
        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(body))
            .send()
            .await
    })
    .await
}

fn future_date(seconds_from_now: u64) -> DateTime {
    DateTime::from_secs(
        (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + seconds_from_now) as i64,
    )
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

fn simple_tagging(value: &str) -> Tagging {
    Tagging::builder()
        .tag_set(Tag::builder().key("env").value(value).build().unwrap())
        .build()
        .unwrap()
}

async fn cleanup_plain_bucket(
    root_client: &aws_sdk_s3::Client,
    non_root_client: &aws_sdk_s3::Client,
    bucket: &str,
    keys: &[&str],
) {
    for client in [root_client, non_root_client] {
        for key in keys {
            let _ = s3_tests::delete_object_retrying_operation_aborted(client, bucket, key).await;
        }
    }

    let mut last_error = None;
    for attempt in 0..20 {
        let mut saw_retryable = false;
        for client in [root_client, non_root_client] {
            match client
                .delete_bucket()
                .bucket(bucket)
                .send_retrying_operation_aborted("delete object admin bucket during cleanup")
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
                    saw_retryable |= s3_tests::is_retryable_operation_contention(&err)
                        || raw.contains("BucketNotEmpty")
                        || raw.contains("NoSuchBucket");
                    last_error = Some(raw);
                }
            }
        }
        if saw_retryable && attempt < 19 {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            continue;
        }
        break;
    }

    panic!(
        "bucket cleanup delete failed for {bucket}: {}",
        last_error.unwrap_or_else(|| "no delete attempt was made".to_string())
    );
}

async fn cleanup_object_lock_version(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    version_id: &str,
) {
    let _ = client
        .put_object_legal_hold()
        .bucket(bucket)
        .key(key)
        .version_id(version_id)
        .legal_hold(legal_hold(ObjectLockLegalHoldStatus::Off))
        .send()
        .await;
    let _ = client
        .delete_object()
        .bucket(bucket)
        .key(key)
        .version_id(version_id)
        .bypass_governance_retention(true)
        .send()
        .await;
}

#[test]
fn test_same_account_root_and_non_root_object_acl_admin() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let client = CTX.client();
        let bucket = create_acl_enabled_bucket(client).await;

        put_object(client, &bucket, "non-root-object", b"one").await;
        root_client
            .get_object_acl()
            .bucket(&bucket)
            .key("non-root-object")
            .send()
            .await
            .unwrap();
        root_client
            .put_object_acl()
            .bucket(&bucket)
            .key("non-root-object")
            .acl(ObjectCannedAcl::Private)
            .send()
            .await
            .unwrap();

        put_object(root_client, &bucket, "root-object", b"two").await;
        client
            .get_object_acl()
            .bucket(&bucket)
            .key("root-object")
            .send()
            .await
            .unwrap();
        client
            .put_object_acl()
            .bucket(&bucket)
            .key("root-object")
            .acl(ObjectCannedAcl::Private)
            .send()
            .await
            .unwrap();

        cleanup_plain_bucket(
            root_client,
            client,
            &bucket,
            &["non-root-object", "root-object"],
        )
        .await;
    });
}

#[test]
fn test_same_account_root_and_non_root_object_tagging_admin() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let client = CTX.client();
        let bucket = create_standard_bucket(client).await;

        client
            .put_object()
            .bucket(&bucket)
            .key("non-root-object")
            .tagging("env=owner")
            .body(ByteStream::from_static(b"one"))
            .send()
            .await
            .unwrap();
        let root_view = root_client
            .get_object_tagging()
            .bucket(&bucket)
            .key("non-root-object")
            .send()
            .await
            .unwrap();
        assert_eq!(root_view.tag_set().len(), 1);
        assert_eq!(root_view.tag_set()[0].key(), "env");
        assert_eq!(root_view.tag_set()[0].value(), "owner");
        root_client
            .put_object_tagging()
            .bucket(&bucket)
            .key("non-root-object")
            .tagging(simple_tagging("root"))
            .send()
            .await
            .unwrap();
        let updated = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("non-root-object")
            .send()
            .await
            .unwrap();
        assert_eq!(updated.tag_set()[0].value(), "root");

        root_client
            .put_object()
            .bucket(&bucket)
            .key("root-object")
            .tagging("env=root")
            .body(ByteStream::from_static(b"two"))
            .send()
            .await
            .unwrap();
        let non_root_view = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("root-object")
            .send()
            .await
            .unwrap();
        assert_eq!(non_root_view.tag_set()[0].value(), "root");
        client
            .delete_object_tagging()
            .bucket(&bucket)
            .key("root-object")
            .send()
            .await
            .unwrap();
        let cleared = root_client
            .get_object_tagging()
            .bucket(&bucket)
            .key("root-object")
            .send()
            .await
            .unwrap();
        assert!(cleared.tag_set().is_empty());

        cleanup_plain_bucket(
            root_client,
            client,
            &bucket,
            &["non-root-object", "root-object"],
        )
        .await;
    });
}

#[test]
fn test_same_account_root_and_non_root_object_retention_admin() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let client = CTX.client();
        let bucket = create_object_lock_bucket(client).await;

        let non_root_version = put_object(client, &bucket, "non-root-object", b"one")
            .await
            .version_id()
            .expect("expected version id")
            .to_string();
        let root_retention = retention(
            ObjectLockRetentionMode::Governance,
            future_date(24 * 60 * 60),
        );
        root_client
            .put_object_retention()
            .bucket(&bucket)
            .key("non-root-object")
            .version_id(&non_root_version)
            .retention(root_retention.clone())
            .send()
            .await
            .unwrap();
        let retained = root_client
            .get_object_retention()
            .bucket(&bucket)
            .key("non-root-object")
            .version_id(&non_root_version)
            .send()
            .await
            .unwrap();
        assert_eq!(retained.retention(), Some(&root_retention));

        let root_version = put_object(root_client, &bucket, "root-object", b"two")
            .await
            .version_id()
            .expect("expected version id")
            .to_string();
        let non_root_retention = retention(
            ObjectLockRetentionMode::Governance,
            future_date(2 * 24 * 60 * 60),
        );
        client
            .put_object_retention()
            .bucket(&bucket)
            .key("root-object")
            .version_id(&root_version)
            .retention(non_root_retention.clone())
            .send()
            .await
            .unwrap();
        let updated = client
            .get_object_retention()
            .bucket(&bucket)
            .key("root-object")
            .version_id(&root_version)
            .send()
            .await
            .unwrap();
        assert_eq!(updated.retention(), Some(&non_root_retention));

        cleanup_object_lock_version(root_client, &bucket, "non-root-object", &non_root_version)
            .await;
        cleanup_object_lock_version(client, &bucket, "root-object", &root_version).await;
        cleanup_versioned_bucket(root_client, &bucket).await;
    });
}

#[test]
fn test_same_account_root_and_non_root_object_legal_hold_admin() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let client = CTX.client();
        let bucket = create_object_lock_bucket(client).await;

        let non_root_version = put_object(client, &bucket, "non-root-object", b"one")
            .await
            .version_id()
            .expect("expected version id")
            .to_string();
        root_client
            .put_object_legal_hold()
            .bucket(&bucket)
            .key("non-root-object")
            .version_id(&non_root_version)
            .legal_hold(legal_hold(ObjectLockLegalHoldStatus::On))
            .send()
            .await
            .unwrap();
        let root_view = root_client
            .get_object_legal_hold()
            .bucket(&bucket)
            .key("non-root-object")
            .version_id(&non_root_version)
            .send()
            .await
            .unwrap();
        assert_eq!(
            root_view.legal_hold().and_then(|hold| hold.status()),
            Some(&ObjectLockLegalHoldStatus::On)
        );

        let root_version = put_object(root_client, &bucket, "root-object", b"two")
            .await
            .version_id()
            .expect("expected version id")
            .to_string();
        client
            .put_object_legal_hold()
            .bucket(&bucket)
            .key("root-object")
            .version_id(&root_version)
            .legal_hold(legal_hold(ObjectLockLegalHoldStatus::On))
            .send()
            .await
            .unwrap();
        let non_root_view = client
            .get_object_legal_hold()
            .bucket(&bucket)
            .key("root-object")
            .version_id(&root_version)
            .send()
            .await
            .unwrap();
        assert_eq!(
            non_root_view.legal_hold().and_then(|hold| hold.status()),
            Some(&ObjectLockLegalHoldStatus::On)
        );

        cleanup_object_lock_version(root_client, &bucket, "non-root-object", &non_root_version)
            .await;
        cleanup_object_lock_version(client, &bucket, "root-object", &root_version).await;
        cleanup_versioned_bucket(root_client, &bucket).await;
    });
}

#[test]
fn test_same_account_root_and_non_root_bypass_governance_delete_object_version() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let client = CTX.client();
        let bucket = create_object_lock_bucket(client).await;

        let non_root_version = put_object(client, &bucket, "non-root-object", b"one")
            .await
            .version_id()
            .expect("expected version id")
            .to_string();
        client
            .put_object_retention()
            .bucket(&bucket)
            .key("non-root-object")
            .version_id(&non_root_version)
            .retention(retention(
                ObjectLockRetentionMode::Governance,
                future_date(2 * 24 * 60 * 60),
            ))
            .send()
            .await
            .unwrap();
        root_client
            .delete_object()
            .bucket(&bucket)
            .key("non-root-object")
            .version_id(&non_root_version)
            .bypass_governance_retention(true)
            .send()
            .await
            .unwrap();

        let root_version = put_object(root_client, &bucket, "root-object", b"two")
            .await
            .version_id()
            .expect("expected version id")
            .to_string();
        root_client
            .put_object_retention()
            .bucket(&bucket)
            .key("root-object")
            .version_id(&root_version)
            .retention(retention(
                ObjectLockRetentionMode::Governance,
                future_date(2 * 24 * 60 * 60),
            ))
            .send()
            .await
            .unwrap();
        client
            .delete_object()
            .bucket(&bucket)
            .key("root-object")
            .version_id(&root_version)
            .bypass_governance_retention(true)
            .send()
            .await
            .unwrap();

        cleanup_versioned_bucket(root_client, &bucket).await;
    });
}
