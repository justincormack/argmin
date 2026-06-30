use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_sdk_s3::primitives::{ByteStream, DateTime};
use aws_sdk_s3::types::{
    CompletedMultipartUpload, CompletedPart, ObjectLockLegalHold, ObjectLockLegalHoldStatus,
    ObjectLockMode, ObjectLockRetention, ObjectLockRetentionMode, ObjectOwnership,
};
use s3_tests::{
    err_status, retrying_operation_aborted, unique_bucket, SendRetryingOperationAborted, CTX,
};

const GOVERNANCE_RETENTION_SECS: u64 = 24 * 60 * 60;

fn now_epoch_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn future_date(seconds_from_now: u64) -> DateTime {
    DateTime::from_secs(now_epoch_secs() + seconds_from_now as i64)
}

fn governance_retain_until() -> DateTime {
    future_date(GOVERNANCE_RETENTION_SECS)
}

async fn setup_acl_object_lock_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    s3_tests::create_bucket_request(client, &bucket)
        .object_lock_enabled_for_bucket(true)
        .object_ownership(ObjectOwnership::ObjectWriter)
        .send_retrying_operation_aborted("create ACL object-lock bucket")
        .await
        .unwrap();
    bucket
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

async fn put_object_bytes(bucket: &str, key: &str, body: &[u8]) -> String {
    retrying_operation_aborted("put ACL object-lock object", || {
        let body = body.to_vec();
        async move {
            CTX.client()
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(ByteStream::from(body))
                .send()
                .await
        }
    })
    .await
    .version_id()
    .expect("expected version_id on object lock bucket")
    .to_string()
}

async fn delete_version_with_bypass(bucket: &str, key: &str, version_id: &str) {
    CTX.client()
        .delete_object()
        .bucket(bucket)
        .key(key)
        .version_id(version_id)
        .bypass_governance_retention(true)
        .send_retrying_operation_aborted("delete ACL object-lock version with bypass")
        .await
        .unwrap();
}

async fn cleanup_object_lock_bucket(bucket: &str) {
    let client = CTX.client();

    'retry: loop {
        let resp = client
            .list_object_versions()
            .bucket(bucket)
            .send_retrying_operation_aborted("list ACL object-lock versions during cleanup")
            .await
            .unwrap();

        if resp.versions().is_empty() && resp.delete_markers().is_empty() {
            match client
                .delete_bucket()
                .bucket(bucket)
                .send_retrying_operation_aborted("delete ACL object-lock bucket")
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

#[test]
fn test_object_lock_acl_put_object_headers_persist() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_acl_object_lock_bucket().await;
        let key = "acl-put-object-headers";
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
        delete_version_with_bypass(&bucket, key, &version_id).await;
        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_acl_put_get_retention() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_acl_object_lock_bucket().await;
        let key = "acl-retention";
        let version_id = put_object_bytes(&bucket, key, b"abc").await;
        let object_retention = retention(
            ObjectLockRetentionMode::Governance,
            governance_retain_until(),
        );

        client
            .put_object_retention()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .retention(object_retention.clone())
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
        assert_eq!(response.retention(), Some(&object_retention));

        delete_version_with_bypass(&bucket, key, &version_id).await;
        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_acl_put_get_legal_hold() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_acl_object_lock_bucket().await;
        let key = "acl-legal-hold";
        let version_id = put_object_bytes(&bucket, key, b"abc").await;

        client
            .put_object_legal_hold()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .legal_hold(legal_hold(ObjectLockLegalHoldStatus::On))
            .send()
            .await
            .unwrap();
        let response = client
            .get_object_legal_hold()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
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
            .version_id(&version_id)
            .legal_hold(legal_hold(ObjectLockLegalHoldStatus::Off))
            .send()
            .await
            .unwrap();
        let response = client
            .get_object_legal_hold()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
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
fn test_object_lock_acl_multipart_headers_persist() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_acl_object_lock_bucket().await;
        let key = "acl-multipart";
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
