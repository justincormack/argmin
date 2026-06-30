use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use aws_sdk_s3::operation::delete_objects::DeleteObjectsOutput;
use aws_sdk_s3::primitives::{DateTime, DateTimeFormat};
use aws_sdk_s3::types::{
    BucketCannedAcl, Delete, DeletedObject, Error as DeleteObjectError, ObjectIdentifier,
    ObjectLockLegalHold, ObjectLockLegalHoldStatus, ObjectLockRetention, ObjectLockRetentionMode,
    ObjectOwnership, OwnershipControls, OwnershipControlsRule,
};
use base64::Engine;
use md5_legacy::Digest;
use s3_tests::{
    assert_s3_err_code, content_md5_header, disable_bucket_public_access_block, err_status,
    unique_bucket, SendRetryingOperationAborted, CTX,
};

const GOVERNANCE_RETENTION_SECS: u64 = 24 * 60 * 60;

fn agent() -> s3_tests::Agent {
    s3_tests::test_agent()
}

fn anonymous_get(url: &str) -> (u16, String) {
    let mut resp = agent().get(url).call().expect("transport error");
    let status = resp.status().as_u16();
    let body = resp.body_mut().read_to_string().unwrap_or_default();
    (status, body)
}

fn anonymous_put_with_headers(
    url: &str,
    body: &[u8],
    headers: &[(String, String)],
) -> (u16, String) {
    let request = headers
        .iter()
        .fold(agent().put(url), |request, (name, value)| {
            request.header(name, value)
        });
    let mut resp = request.send(body).expect("transport error");
    let status = resp.status().as_u16();
    let body = resp.body_mut().read_to_string().unwrap_or_default();
    (status, body)
}

fn future_date(seconds_from_now: u64) -> DateTime {
    DateTime::from_secs(now_epoch_secs() + seconds_from_now as i64)
}

fn now_epoch_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

async fn setup_public_write_object_lock_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    s3_tests::create_bucket_request(client, &bucket)
        .object_lock_enabled_for_bucket(true)
        .send_retrying_operation_aborted("create public object-lock bucket")
        .await
        .unwrap();

    disable_bucket_public_access_block(client, &bucket).await;

    let ownership_rule = OwnershipControlsRule::builder()
        .object_ownership(ObjectOwnership::BucketOwnerPreferred)
        .build()
        .unwrap();
    let ownership = OwnershipControls::builder()
        .rules(ownership_rule)
        .build()
        .unwrap();
    client
        .put_bucket_ownership_controls()
        .bucket(&bucket)
        .ownership_controls(ownership)
        .send_retrying_operation_aborted("put public object-lock ownership controls")
        .await
        .unwrap();
    client
        .put_bucket_acl()
        .bucket(&bucket)
        .acl(BucketCannedAcl::PublicReadWrite)
        .send_retrying_operation_aborted("put public object-lock bucket ACL")
        .await
        .unwrap();

    client
        .get_public_access_block()
        .bucket(&bucket)
        .send_retrying_operation_aborted("get public object-lock public access block")
        .await
        .unwrap();
    client
        .get_bucket_ownership_controls()
        .bucket(&bucket)
        .send_retrying_operation_aborted("get public object-lock ownership controls")
        .await
        .unwrap();
    client
        .get_bucket_acl()
        .bucket(&bucket)
        .send_retrying_operation_aborted("get public object-lock bucket ACL")
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

fn object_id(key: &str, version_id: &str) -> ObjectIdentifier {
    ObjectIdentifier::builder()
        .key(key)
        .version_id(version_id)
        .build()
        .unwrap()
}

fn configured_retry_timeout() -> Duration {
    let timeout_secs = std::env::var("S3_TEST_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(30);
    Duration::from_secs(timeout_secs)
}

async fn put_object_bytes(bucket: &str, key: &str, body: &[u8]) -> String {
    s3_tests::put_object_retrying_operation_aborted(CTX.client(), bucket, key, body.to_vec())
        .await
        .version_id()
        .expect("expected version_id on object lock bucket")
        .to_string()
}

async fn delete_objects_with_bypass_retrying_operation_aborted(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    delete: Delete,
) -> DeleteObjectsOutput {
    const RETRY_DELAY: Duration = Duration::from_millis(100);
    let deadline = Instant::now() + configured_retry_timeout();
    let quiet = delete.quiet();
    let all_objects = delete.objects().to_vec();
    let mut pending = all_objects.clone();
    let mut deleted = Vec::new();
    let mut errors = Vec::new();

    loop {
        let request_delete = Delete::builder()
            .set_objects(Some(pending.clone()))
            .set_quiet(quiet)
            .build()
            .unwrap();

        let response = s3_tests::retrying_operation_aborted(
            "delete public object-lock objects with bypass",
            || {
                let request_delete = request_delete.clone();
                client
                    .delete_objects()
                    .bucket(bucket)
                    .delete(request_delete)
                    .bypass_governance_retention(true)
                    .customize()
                    .mutate_request(|req| {
                        let body = req.body().bytes().expect("DeleteObjects body in memory");
                        let digest = md5_legacy::Md5::digest(body);
                        let content_md5 =
                            base64::engine::general_purpose::STANDARD.encode(&digest[..]);
                        req.headers_mut().insert("content-md5", content_md5);
                    })
                    .send()
            },
        )
        .await;

        deleted.extend(response.deleted().iter().cloned());

        let mut retry = Vec::new();
        for error in response.errors() {
            if matches!(error.code(), Some("OperationAborted" | "SlowDown"))
                && Instant::now() < deadline
            {
                retry.push(matching_delete_object(&all_objects, error));
            } else {
                errors.push(error.clone());
            }
        }

        if retry.is_empty() {
            return build_delete_objects_output(deleted, errors);
        }

        pending = retry;
        tokio::time::sleep(RETRY_DELAY).await;
    }
}

fn matching_delete_object(
    objects: &[ObjectIdentifier],
    error: &DeleteObjectError,
) -> ObjectIdentifier {
    let key = error.key().unwrap_or_default();
    let version_id = error.version_id();
    objects
        .iter()
        .find(|object| object.key() == key && object.version_id() == version_id)
        .cloned()
        .unwrap_or_else(|| {
            let mut builder = ObjectIdentifier::builder().key(key);
            if let Some(version_id) = version_id {
                builder = builder.version_id(version_id);
            }
            builder.build().unwrap()
        })
}

fn build_delete_objects_output(
    deleted: Vec<DeletedObject>,
    errors: Vec<DeleteObjectError>,
) -> DeleteObjectsOutput {
    DeleteObjectsOutput::builder()
        .set_deleted((!deleted.is_empty()).then_some(deleted))
        .set_errors((!errors.is_empty()).then_some(errors))
        .build()
}

async fn delete_version_with_bypass(bucket: &str, key: &str, version_id: &str) {
    CTX.client()
        .delete_object()
        .bucket(bucket)
        .key(key)
        .version_id(version_id)
        .bypass_governance_retention(true)
        .send_retrying_operation_aborted("delete public object-lock version with bypass")
        .await
        .unwrap();
}

async fn cleanup_object_lock_bucket(bucket: &str) {
    let client = CTX.client();

    'retry: loop {
        let resp = client
            .list_object_versions()
            .bucket(bucket)
            .send_retrying_operation_aborted("list public object-lock versions during cleanup")
            .await
            .unwrap();

        if resp.versions().is_empty() && resp.delete_markers().is_empty() {
            match client
                .delete_bucket()
                .bucket(bucket)
                .send_retrying_operation_aborted("delete public object-lock bucket")
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
                .send_retrying_operation_aborted("delete public object-lock delete marker")
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
                .send_retrying_operation_aborted("head public object-lock version during cleanup")
                .await
                .unwrap();

            if head.object_lock_legal_hold_status() == Some(&ObjectLockLegalHoldStatus::On) {
                client
                    .put_object_legal_hold()
                    .bucket(bucket)
                    .key(key)
                    .version_id(version_id)
                    .legal_hold(legal_hold(ObjectLockLegalHoldStatus::Off))
                    .send_retrying_operation_aborted("clear public object-lock legal hold")
                    .await
                    .unwrap();
            }

            let delete = client
                .delete_object()
                .bucket(bucket)
                .key(key)
                .version_id(version_id)
                .bypass_governance_retention(true)
                .send_retrying_operation_aborted("delete public object-lock version during cleanup")
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
fn test_object_lock_delete_object_bypass_requires_bucket_admin() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = setup_public_write_object_lock_bucket().await;

        s3_tests::put_object_retrying_operation_aborted(
            client,
            &bucket,
            "plain",
            b"plain".to_vec(),
        )
        .await;
        let delete_marker = alt_client
            .delete_object()
            .bucket(&bucket)
            .key("plain")
            .send_retrying_operation_aborted("create public object-lock delete marker")
            .await
            .unwrap();
        assert_eq!(delete_marker.delete_marker(), Some(true));

        let key = "locked";
        let version_id = put_object_bytes(&bucket, key, b"locked").await;
        client
            .put_object_retention()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .retention(retention(
                ObjectLockRetentionMode::Governance,
                future_date(GOVERNANCE_RETENTION_SECS),
            ))
            .send_retrying_operation_aborted("put public object-lock retention")
            .await
            .unwrap();

        let result = alt_client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .bypass_governance_retention(true)
            .send_retrying_operation_aborted("attempt public object-lock bypass delete")
            .await;
        assert_eq!(err_status(&result), 403);
        assert_s3_err_code(&result, "AccessDenied");

        delete_version_with_bypass(&bucket, key, &version_id).await;
        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_lock_anonymous_get_object_retention_denied_message() {
    s3_tests::run(async {
        let bucket = setup_public_write_object_lock_bucket().await;
        let key = "anon-get-retention";
        put_object_bytes(&bucket, key, b"locked").await;

        let url = format!("{}/{bucket}/{key}?retention", CTX.endpoint());
        let response = anonymous_get(&url);

        cleanup_object_lock_bucket(&bucket).await;

        assert_eq!(response.0, 403, "unexpected body: {}", response.1);
        assert!(
            response.1.contains("AccessDenied"),
            "unexpected body: {}",
            response.1
        );
    });
}

#[test]
fn test_object_lock_anonymous_put_object_retention_denied_message() {
    s3_tests::run(async {
        let bucket = setup_public_write_object_lock_bucket().await;
        let key = "anon-put-retention";
        put_object_bytes(&bucket, key, b"locked").await;

        let retention_body = format!(
            "<ObjectLockRetention xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Mode>GOVERNANCE</Mode><RetainUntilDate>{}</RetainUntilDate></ObjectLockRetention>",
            future_date(GOVERNANCE_RETENTION_SECS)
                .fmt(DateTimeFormat::DateTime)
                .unwrap()
        );
        let url = format!("{}/{bucket}/{key}?retention", CTX.endpoint());
        let response = anonymous_put_with_headers(
            &url,
            retention_body.as_bytes(),
            &[content_md5_header(retention_body.as_bytes())],
        );

        cleanup_object_lock_bucket(&bucket).await;

        assert_eq!(response.0, 403, "unexpected body: {}", response.1);
        assert!(
            response.1.contains("AccessDenied"),
            "unexpected body: {}",
            response.1
        );
    });
}

#[test]
fn test_object_lock_anonymous_get_object_legal_hold_denied_message() {
    s3_tests::run(async {
        let bucket = setup_public_write_object_lock_bucket().await;
        let key = "anon-get-legal-hold";
        put_object_bytes(&bucket, key, b"locked").await;

        let url = format!("{}/{bucket}/{key}?legal-hold", CTX.endpoint());
        let response = anonymous_get(&url);

        cleanup_object_lock_bucket(&bucket).await;

        assert_eq!(response.0, 403, "unexpected body: {}", response.1);
        assert!(
            response.1.contains("AccessDenied"),
            "unexpected body: {}",
            response.1
        );
    });
}

#[test]
fn test_object_lock_anonymous_put_object_legal_hold_denied_message() {
    s3_tests::run(async {
        let bucket = setup_public_write_object_lock_bucket().await;
        let key = "anon-put-legal-hold";
        put_object_bytes(&bucket, key, b"locked").await;

        let legal_hold_body = br#"<LegalHold xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>ON</Status></LegalHold>"#;
        let url = format!("{}/{bucket}/{key}?legal-hold", CTX.endpoint());
        let response = anonymous_put_with_headers(
            &url,
            legal_hold_body,
            &[content_md5_header(legal_hold_body)],
        );

        cleanup_object_lock_bucket(&bucket).await;

        assert_eq!(response.0, 403, "unexpected body: {}", response.1);
        assert!(
            response.1.contains("AccessDenied"),
            "unexpected body: {}",
            response.1
        );
    });
}

#[test]
fn test_object_lock_multi_delete_bypass_requires_bucket_admin() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = setup_public_write_object_lock_bucket().await;

        let plain_key = "plain";
        put_object_bytes(&bucket, plain_key, b"plain").await;

        let locked_key = "locked";
        let locked_version_id = put_object_bytes(&bucket, locked_key, b"locked").await;
        client
            .put_object_retention()
            .bucket(&bucket)
            .key(locked_key)
            .version_id(&locked_version_id)
            .retention(retention(
                ObjectLockRetentionMode::Governance,
                future_date(GOVERNANCE_RETENTION_SECS),
            ))
            .send_retrying_operation_aborted("put public object-lock retention")
            .await
            .unwrap();

        let delete = Delete::builder()
            .objects(ObjectIdentifier::builder().key(plain_key).build().unwrap())
            .objects(object_id(locked_key, &locked_version_id))
            .build()
            .unwrap();
        let response =
            delete_objects_with_bypass_retrying_operation_aborted(alt_client, &bucket, delete)
                .await;

        assert_eq!(
            response.deleted().len(),
            1,
            "deleted={:?} errors={:?}",
            response.deleted(),
            response.errors()
        );
        assert_eq!(
            response.errors().len(),
            1,
            "deleted={:?} errors={:?}",
            response.deleted(),
            response.errors()
        );
        let deleted = &response.deleted()[0];
        assert_eq!(deleted.key(), Some(plain_key));
        assert_eq!(deleted.delete_marker(), Some(true));
        assert!(deleted.delete_marker_version_id().is_some());
        let failed = &response.errors()[0];
        assert_eq!(failed.code(), Some("AccessDenied"));
        assert_eq!(failed.key(), Some(locked_key));
        assert_eq!(failed.version_id(), Some(locked_version_id.as_str()));

        delete_version_with_bypass(&bucket, locked_key, &locked_version_id).await;
        cleanup_object_lock_bucket(&bucket).await;
    });
}
