use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_sdk_s3::client::customize::CustomizableOperation;
use aws_sdk_s3::operation::delete_objects::builders::DeleteObjectsFluentBuilder;
use aws_sdk_s3::operation::delete_objects::{DeleteObjectsError, DeleteObjectsOutput};
use aws_sdk_s3::primitives::{ByteStream, DateTime};
use aws_sdk_s3::types::{
    BucketCannedAcl, BucketVersioningStatus, CompletedMultipartUpload, CompletedPart,
    CorsConfiguration, CorsRule, DefaultRetention, Delete, ObjectAttributes, ObjectCannedAcl,
    ObjectIdentifier, ObjectLockConfiguration, ObjectLockEnabled, ObjectLockLegalHold,
    ObjectLockLegalHoldStatus, ObjectLockRetention, ObjectLockRetentionMode, ObjectLockRule,
    ObjectOwnership, OwnershipControls, OwnershipControlsRule, PublicAccessBlockConfiguration,
    ServerSideEncryption, ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration,
    ServerSideEncryptionRule, Tag, Tagging, VersioningConfiguration,
};
use base64::Engine;
use md5_legacy::Digest;
use s3_tests::{assert_s3_err_code, cleanup_versioned_bucket, err_status, unique_bucket, CTX};
use serde_json::json;

const PART_SIZE: usize = 5 * 1024 * 1024;
const WRONG_OWNER: &str = "000000000000";

fn assert_expected_bucket_owner_denied<T: std::fmt::Debug, E: std::fmt::Debug>(
    result: &Result<T, aws_sdk_s3::error::SdkError<E>>,
) {
    assert_eq!(err_status(result), 403, "unexpected result: {result:?}");

    let debug = format!("{result:?}");
    if debug.contains("AccessDenied") {
        assert_s3_err_code(result, "AccessDenied");
        return;
    }

    let body = result
        .as_ref()
        .err()
        .and_then(|sdk_err| sdk_err.raw_response())
        .and_then(|response| response.body().bytes());
    assert!(
        body.is_some_and(|body| body.is_empty()),
        "expected AccessDenied code or empty 403 body, got {debug}"
    );
}

macro_rules! expect_owner_ok {
    ($op:expr) => {{
        $op.expected_bucket_owner(CTX.account_id())
            .send()
            .await
            .unwrap()
    }};
}

macro_rules! expect_owner_denied {
    ($op:expr) => {{
        let result = $op.expected_bucket_owner(WRONG_OWNER).send().await;
        assert_expected_bucket_owner_denied(&result);
    }};
}

fn versioning_enabled() -> VersioningConfiguration {
    VersioningConfiguration::builder()
        .status(BucketVersioningStatus::Enabled)
        .build()
}

fn simple_cors_config() -> CorsConfiguration {
    let rule = CorsRule::builder()
        .allowed_origins("https://example.com")
        .allowed_methods("GET")
        .allowed_headers("*")
        .build()
        .unwrap();
    CorsConfiguration::builder()
        .cors_rules(rule)
        .build()
        .unwrap()
}

fn simple_tagging() -> Tagging {
    Tagging::builder()
        .tag_set(Tag::builder().key("env").value("test").build().unwrap())
        .build()
        .unwrap()
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

fn bucket_owner_preferred_controls() -> OwnershipControls {
    let rule = OwnershipControlsRule::builder()
        .object_ownership(ObjectOwnership::BucketOwnerPreferred)
        .build()
        .unwrap();
    OwnershipControls::builder().rules(rule).build().unwrap()
}

fn object_writer_ownership_controls() -> OwnershipControls {
    let rule = OwnershipControlsRule::builder()
        .object_ownership(ObjectOwnership::ObjectWriter)
        .build()
        .unwrap();
    OwnershipControls::builder().rules(rule).build().unwrap()
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

fn owner_only_bucket_policy(bucket: &str) -> String {
    json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {
                "AWS": format!("arn:aws:iam::{}:root", CTX.account_id())
            },
            "Action": "s3:GetBucketPolicy",
            "Resource": format!("arn:aws:s3:::{bucket}"),
        }],
    })
    .to_string()
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

fn governance_retention_tomorrow() -> ObjectLockRetention {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    ObjectLockRetention::builder()
        .mode(ObjectLockRetentionMode::Governance)
        .retain_until_date(DateTime::from_secs(now + 24 * 60 * 60))
        .build()
}

fn legal_hold(status: ObjectLockLegalHoldStatus) -> ObjectLockLegalHold {
    ObjectLockLegalHold::builder().status(status).build()
}

async fn create_bucket() -> String {
    let bucket = unique_bucket();
    s3_tests::create_bucket(CTX.client(), &bucket)
        .await
        .unwrap();
    bucket
}

async fn create_versioned_bucket() -> String {
    let bucket = create_bucket().await;
    CTX.client()
        .put_bucket_versioning()
        .bucket(&bucket)
        .versioning_configuration(versioning_enabled())
        .send()
        .await
        .unwrap();
    bucket
}

async fn create_object_lock_bucket() -> String {
    let bucket = unique_bucket();
    s3_tests::create_bucket_request(CTX.client(), &bucket)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();
    bucket
}

async fn set_object_writer_ownership(bucket: &str) {
    CTX.client()
        .put_bucket_ownership_controls()
        .bucket(bucket)
        .ownership_controls(object_writer_ownership_controls())
        .send()
        .await
        .unwrap();
}

async fn put_object_bytes(bucket: &str, key: &str, body: &[u8]) {
    CTX.client()
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(body.to_vec()))
        .send()
        .await
        .unwrap();
}

async fn cleanup_bucket(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = client.delete_object().bucket(bucket).key(*key).send().await;
    }
    client.delete_bucket().bucket(bucket).send().await.unwrap();
}

async fn cleanup_multipart_bucket(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
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

        for key in keys {
            let _ = client.delete_object().bucket(bucket).key(*key).send().await;
        }

        match client.delete_bucket().bucket(bucket).send().await {
            Ok(_) => return,
            Err(err) => {
                let raw = format!("{err:?}");
                if raw.contains("BucketNotEmpty") || raw.contains("OperationAborted") {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
                panic!("unexpected delete_bucket failure: {raw}");
            }
        }
    }

    client.delete_bucket().bucket(bucket).send().await.unwrap();
}

async fn cleanup_object_lock_bucket(bucket: &str) {
    let client = CTX.client();

    'retry: loop {
        let resp = client
            .list_object_versions()
            .bucket(bucket)
            .send()
            .await
            .unwrap();

        if resp.versions().is_empty() && resp.delete_markers().is_empty() {
            match client.delete_bucket().bucket(bucket).send().await {
                Ok(_) => return,
                Err(err) => {
                    let raw = format!("{err:?}");
                    if raw.contains("BucketNotEmpty") || raw.contains("OperationAborted") {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        continue;
                    }
                    panic!("unexpected delete_bucket failure: {raw}");
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

fn now_epoch_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn delete_objects_with_md5_and_expected_owner(
    bucket: &str,
    delete: Delete,
    expected_owner: &str,
) -> CustomizableOperation<DeleteObjectsOutput, DeleteObjectsError, DeleteObjectsFluentBuilder> {
    CTX.client()
        .delete_objects()
        .bucket(bucket)
        .delete(delete)
        .expected_bucket_owner(expected_owner)
        .customize()
        .mutate_request(|req| {
            let body = req
                .body()
                .bytes()
                .expect("DeleteObjects body must be in-memory");
            let digest = md5_legacy::Md5::digest(body);
            let content_md5 = base64::engine::general_purpose::STANDARD.encode(&digest[..]);
            req.headers_mut().insert("content-md5", content_md5);
        })
}

#[test]
fn test_create_bucket_ignores_expected_bucket_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();

        let result = s3_tests::create_bucket_request(client, &bucket)
            .customize()
            .mutate_request(|req| {
                req.headers_mut()
                    .insert("x-amz-expected-bucket-owner", WRONG_OWNER);
            })
            .send()
            .await;

        match result {
            Ok(_) => {
                client.head_bucket().bucket(&bucket).send().await.unwrap();
                client.delete_bucket().bucket(&bucket).send().await.unwrap();
            }
            Err(err) => {
                let result = Err::<(), _>(err);
                assert_ne!(
                    err_status(&result),
                    403,
                    "unexpected owner check failure: {result:?}"
                );
                let msg = format!("{result:?}");
                assert!(
                    !msg.contains("AccessDenied"),
                    "create bucket unexpectedly enforced expected owner: {msg}"
                );
            }
        }
    });
}

#[test]
fn test_head_bucket_expected_bucket_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket().await;

        expect_owner_denied!(client.head_bucket().bucket(&bucket));
        expect_owner_ok!(client.head_bucket().bucket(&bucket));

        cleanup_bucket(&bucket, &[]).await;
    });
}

#[test]
fn test_delete_bucket_expected_bucket_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket().await;

        expect_owner_denied!(client.delete_bucket().bucket(&bucket));
        expect_owner_ok!(client.delete_bucket().bucket(&bucket));
    });
}

#[test]
fn test_object_crud_expected_bucket_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket().await;
        let key = "obj";
        let body = b"hello expected owner";

        expect_owner_denied!(client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(body)));
        expect_owner_ok!(client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(body)));

        expect_owner_denied!(client.head_object().bucket(&bucket).key(key));
        expect_owner_ok!(client.head_object().bucket(&bucket).key(key));

        expect_owner_denied!(client.get_object().bucket(&bucket).key(key));
        let resp = expect_owner_ok!(client.get_object().bucket(&bucket).key(key));
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], body);

        expect_owner_denied!(client.delete_object().bucket(&bucket).key(key));
        expect_owner_ok!(client.delete_object().bucket(&bucket).key(key));

        cleanup_bucket(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_listing_expected_bucket_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_versioned_bucket().await;

        put_object_bytes(&bucket, "alpha", b"one").await;
        put_object_bytes(&bucket, "alpha", b"two").await;
        put_object_bytes(&bucket, "beta", b"three").await;

        expect_owner_denied!(client.list_objects().bucket(&bucket));
        let list_v1 = expect_owner_ok!(client.list_objects().bucket(&bucket));
        assert_eq!(list_v1.contents().len(), 2);

        expect_owner_denied!(client.list_objects_v2().bucket(&bucket));
        let list_v2 = expect_owner_ok!(client.list_objects_v2().bucket(&bucket));
        assert_eq!(list_v2.contents().len(), 2);

        expect_owner_denied!(client.list_object_versions().bucket(&bucket));
        let versions = expect_owner_ok!(client.list_object_versions().bucket(&bucket));
        assert!(versions.versions().len() >= 3);

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_bucket_versioning_expected_bucket_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket().await;

        expect_owner_denied!(client
            .put_bucket_versioning()
            .bucket(&bucket)
            .versioning_configuration(versioning_enabled()));
        expect_owner_ok!(client
            .put_bucket_versioning()
            .bucket(&bucket)
            .versioning_configuration(versioning_enabled()));

        expect_owner_denied!(client.get_bucket_versioning().bucket(&bucket));
        let resp = expect_owner_ok!(client.get_bucket_versioning().bucket(&bucket));
        assert_eq!(resp.status(), Some(&BucketVersioningStatus::Enabled));

        cleanup_bucket(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_cors_expected_bucket_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket().await;
        let config = simple_cors_config();

        expect_owner_denied!(client
            .put_bucket_cors()
            .bucket(&bucket)
            .cors_configuration(config.clone()));
        expect_owner_ok!(client
            .put_bucket_cors()
            .bucket(&bucket)
            .cors_configuration(config.clone()));

        expect_owner_denied!(client.get_bucket_cors().bucket(&bucket));
        let resp = expect_owner_ok!(client.get_bucket_cors().bucket(&bucket));
        assert_eq!(resp.cors_rules().len(), 1);

        expect_owner_denied!(client.delete_bucket_cors().bucket(&bucket));
        expect_owner_ok!(client.delete_bucket_cors().bucket(&bucket));

        cleanup_bucket(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_tagging_expected_bucket_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket().await;
        let tags = simple_tagging();

        expect_owner_denied!(client
            .put_bucket_tagging()
            .bucket(&bucket)
            .tagging(tags.clone()));
        expect_owner_ok!(client
            .put_bucket_tagging()
            .bucket(&bucket)
            .tagging(tags.clone()));

        expect_owner_denied!(client.get_bucket_tagging().bucket(&bucket));
        let resp = expect_owner_ok!(client.get_bucket_tagging().bucket(&bucket));
        assert_eq!(resp.tag_set().len(), 1);

        expect_owner_denied!(client.delete_bucket_tagging().bucket(&bucket));
        expect_owner_ok!(client.delete_bucket_tagging().bucket(&bucket));

        cleanup_bucket(&bucket, &[]).await;
    });
}

#[test]
fn test_public_access_block_expected_bucket_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket().await;
        let config = simple_public_access_block();

        expect_owner_denied!(client
            .put_public_access_block()
            .bucket(&bucket)
            .public_access_block_configuration(config.clone()));
        expect_owner_ok!(client
            .put_public_access_block()
            .bucket(&bucket)
            .public_access_block_configuration(config.clone()));

        expect_owner_denied!(client.get_public_access_block().bucket(&bucket));
        let resp = expect_owner_ok!(client.get_public_access_block().bucket(&bucket));
        let got = resp.public_access_block_configuration().unwrap();
        assert_eq!(got.block_public_acls(), Some(true));

        expect_owner_denied!(client.delete_public_access_block().bucket(&bucket));
        expect_owner_ok!(client.delete_public_access_block().bucket(&bucket));

        cleanup_bucket(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_ownership_controls_expected_bucket_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket().await;
        let controls = simple_ownership_controls();

        expect_owner_denied!(client
            .put_bucket_ownership_controls()
            .bucket(&bucket)
            .ownership_controls(controls.clone()));
        expect_owner_ok!(client
            .put_bucket_ownership_controls()
            .bucket(&bucket)
            .ownership_controls(controls.clone()));

        expect_owner_denied!(client.get_bucket_ownership_controls().bucket(&bucket));
        let resp = expect_owner_ok!(client.get_bucket_ownership_controls().bucket(&bucket));
        assert_eq!(
            resp.ownership_controls().unwrap().rules()[0].object_ownership,
            ObjectOwnership::BucketOwnerPreferred
        );

        expect_owner_denied!(client.delete_bucket_ownership_controls().bucket(&bucket));
        expect_owner_ok!(client.delete_bucket_ownership_controls().bucket(&bucket));

        cleanup_bucket(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_encryption_expected_bucket_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket().await;
        let config = simple_bucket_encryption();

        expect_owner_denied!(client
            .put_bucket_encryption()
            .bucket(&bucket)
            .server_side_encryption_configuration(config.clone()));
        expect_owner_ok!(client
            .put_bucket_encryption()
            .bucket(&bucket)
            .server_side_encryption_configuration(config.clone()));

        expect_owner_denied!(client.get_bucket_encryption().bucket(&bucket));
        let resp = expect_owner_ok!(client.get_bucket_encryption().bucket(&bucket));
        assert_eq!(
            resp.server_side_encryption_configuration().unwrap().rules()[0]
                .apply_server_side_encryption_by_default()
                .unwrap()
                .sse_algorithm(),
            &ServerSideEncryption::Aes256
        );

        cleanup_bucket(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_expected_bucket_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket().await;
        let policy = owner_only_bucket_policy(&bucket);

        expect_owner_denied!(client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy.clone()));
        expect_owner_ok!(client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy.clone()));

        expect_owner_denied!(client.get_bucket_policy().bucket(&bucket));
        let resp = expect_owner_ok!(client.get_bucket_policy().bucket(&bucket));
        let got: serde_json::Value = serde_json::from_str(resp.policy().unwrap()).unwrap();
        let expected: serde_json::Value = serde_json::from_str(&policy).unwrap();
        assert_eq!(got, expected);

        expect_owner_denied!(client.get_bucket_policy_status().bucket(&bucket));
        let status = expect_owner_ok!(client.get_bucket_policy_status().bucket(&bucket));
        assert_eq!(
            status.policy_status().and_then(|value| value.is_public()),
            Some(false)
        );

        expect_owner_denied!(client.delete_bucket_policy().bucket(&bucket));
        expect_owner_ok!(client.delete_bucket_policy().bucket(&bucket));

        cleanup_bucket(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_acl_expected_bucket_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket().await;

        client
            .put_bucket_ownership_controls()
            .bucket(&bucket)
            .ownership_controls(bucket_owner_preferred_controls())
            .send()
            .await
            .unwrap();

        expect_owner_denied!(client
            .put_bucket_acl()
            .bucket(&bucket)
            .acl(BucketCannedAcl::Private));
        expect_owner_ok!(client
            .put_bucket_acl()
            .bucket(&bucket)
            .acl(BucketCannedAcl::Private));

        expect_owner_denied!(client.get_bucket_acl().bucket(&bucket));
        expect_owner_ok!(client.get_bucket_acl().bucket(&bucket));

        cleanup_bucket(&bucket, &[]).await;
    });
}

#[test]
fn test_object_acl_expected_bucket_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket().await;
        let key = "obj";

        set_object_writer_ownership(&bucket).await;
        put_object_bytes(&bucket, key, b"acl").await;

        expect_owner_denied!(client
            .put_object_acl()
            .bucket(&bucket)
            .key(key)
            .acl(ObjectCannedAcl::Private));
        expect_owner_ok!(client
            .put_object_acl()
            .bucket(&bucket)
            .key(key)
            .acl(ObjectCannedAcl::Private));

        expect_owner_denied!(client.get_object_acl().bucket(&bucket).key(key));
        expect_owner_ok!(client.get_object_acl().bucket(&bucket).key(key));

        cleanup_bucket(&bucket, &[key]).await;
    });
}

#[test]
fn test_object_tagging_expected_bucket_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket().await;
        let key = "obj";
        let tags = simple_tagging();

        put_object_bytes(&bucket, key, b"tagged").await;

        expect_owner_denied!(client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .tagging(tags.clone()));
        expect_owner_ok!(client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .tagging(tags.clone()));

        expect_owner_denied!(client.get_object_tagging().bucket(&bucket).key(key));
        let resp = expect_owner_ok!(client.get_object_tagging().bucket(&bucket).key(key));
        assert_eq!(resp.tag_set().len(), 1);

        expect_owner_denied!(client.delete_object_tagging().bucket(&bucket).key(key));
        expect_owner_ok!(client.delete_object_tagging().bucket(&bucket).key(key));

        cleanup_bucket(&bucket, &[key]).await;
    });
}

#[test]
fn test_get_object_attributes_expected_bucket_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket().await;
        let key = "obj";

        put_object_bytes(&bucket, key, b"attrs").await;

        expect_owner_denied!(client
            .get_object_attributes()
            .bucket(&bucket)
            .key(key)
            .object_attributes(ObjectAttributes::ObjectSize));
        let resp = expect_owner_ok!(client
            .get_object_attributes()
            .bucket(&bucket)
            .key(key)
            .object_attributes(ObjectAttributes::ObjectSize));
        assert_eq!(resp.object_size(), Some(5));

        cleanup_bucket(&bucket, &[key]).await;
    });
}

#[test]
fn test_delete_objects_expected_bucket_owner() {
    s3_tests::run(async {
        let bucket = create_bucket().await;

        put_object_bytes(&bucket, "a", b"1").await;
        put_object_bytes(&bucket, "b", b"2").await;

        let delete = Delete::builder()
            .objects(ObjectIdentifier::builder().key("a").build().unwrap())
            .objects(ObjectIdentifier::builder().key("b").build().unwrap())
            .quiet(true)
            .build()
            .unwrap();

        let result =
            delete_objects_with_md5_and_expected_owner(&bucket, delete.clone(), WRONG_OWNER)
                .send()
                .await;
        assert_eq!(err_status(&result), 403, "unexpected result: {result:?}");

        delete_objects_with_md5_and_expected_owner(&bucket, delete, CTX.account_id())
            .send()
            .await
            .unwrap();

        let list = CTX
            .client()
            .list_objects_v2()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert!(list.contents().is_empty());

        cleanup_bucket(&bucket, &[]).await;
    });
}

#[test]
fn test_multipart_expected_bucket_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket().await;
        let key = "multipart-complete";
        let abort_key = "multipart-abort";
        let part = vec![b'a'; PART_SIZE];

        expect_owner_denied!(client.create_multipart_upload().bucket(&bucket).key(key));
        let create = expect_owner_ok!(client.create_multipart_upload().bucket(&bucket).key(key));
        let upload_id = create.upload_id().unwrap().to_string();

        expect_owner_denied!(client.list_multipart_uploads().bucket(&bucket));
        let uploads = expect_owner_ok!(client.list_multipart_uploads().bucket(&bucket));
        assert!(uploads
            .uploads()
            .iter()
            .any(|upload| upload.key() == Some(key)));

        expect_owner_denied!(client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from_static(b"denied")));
        let upload = expect_owner_ok!(client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from(part.clone())));

        expect_owner_denied!(client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id));
        let parts = expect_owner_ok!(client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id));
        assert_eq!(parts.parts().len(), 1);

        let completed = CompletedMultipartUpload::builder()
            .parts(
                CompletedPart::builder()
                    .part_number(1)
                    .e_tag(upload.e_tag().unwrap())
                    .build(),
            )
            .build();
        expect_owner_denied!(client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(completed.clone()));
        expect_owner_ok!(client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(completed));

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(data.len(), PART_SIZE);

        let abort_create = expect_owner_ok!(client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(abort_key));
        let abort_upload_id = abort_create.upload_id().unwrap().to_string();

        expect_owner_denied!(client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(abort_key)
            .upload_id(&abort_upload_id));
        expect_owner_ok!(client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(abort_key)
            .upload_id(&abort_upload_id));

        cleanup_multipart_bucket(&bucket, &[key]).await;
    });
}

#[test]
fn test_copy_object_expected_bucket_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket().await;
        let src = "src";
        let dst = "dst";

        put_object_bytes(&bucket, src, b"copy-source").await;

        let result = client
            .copy_object()
            .bucket(&bucket)
            .key(dst)
            .copy_source(format!("{bucket}/{src}"))
            .expected_bucket_owner(WRONG_OWNER)
            .expected_source_bucket_owner(CTX.account_id())
            .send()
            .await;
        assert_eq!(err_status(&result), 403, "unexpected result: {result:?}");

        let result = client
            .copy_object()
            .bucket(&bucket)
            .key(dst)
            .copy_source(format!("{bucket}/{src}"))
            .expected_bucket_owner(CTX.account_id())
            .expected_source_bucket_owner(WRONG_OWNER)
            .send()
            .await;
        assert_eq!(err_status(&result), 403, "unexpected result: {result:?}");

        client
            .copy_object()
            .bucket(&bucket)
            .key(dst)
            .copy_source(format!("{bucket}/{src}"))
            .expected_bucket_owner(CTX.account_id())
            .expected_source_bucket_owner(CTX.account_id())
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(dst)
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"copy-source");

        cleanup_bucket(&bucket, &[src, dst]).await;
    });
}

#[test]
fn test_upload_part_copy_expected_bucket_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket().await;
        let src = "src";
        let dst = "dst";
        let source_body = vec![b'b'; PART_SIZE];

        put_object_bytes(&bucket, src, &source_body).await;
        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(dst)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let result = client
            .upload_part_copy()
            .bucket(&bucket)
            .key(dst)
            .upload_id(&upload_id)
            .part_number(1)
            .copy_source(format!("{bucket}/{src}"))
            .expected_bucket_owner(WRONG_OWNER)
            .expected_source_bucket_owner(CTX.account_id())
            .send()
            .await;
        assert_eq!(err_status(&result), 403, "unexpected result: {result:?}");

        let result = client
            .upload_part_copy()
            .bucket(&bucket)
            .key(dst)
            .upload_id(&upload_id)
            .part_number(1)
            .copy_source(format!("{bucket}/{src}"))
            .expected_bucket_owner(CTX.account_id())
            .expected_source_bucket_owner(WRONG_OWNER)
            .send()
            .await;
        assert_eq!(err_status(&result), 403, "unexpected result: {result:?}");

        let part = client
            .upload_part_copy()
            .bucket(&bucket)
            .key(dst)
            .upload_id(&upload_id)
            .part_number(1)
            .copy_source(format!("{bucket}/{src}"))
            .expected_bucket_owner(CTX.account_id())
            .expected_source_bucket_owner(CTX.account_id())
            .send()
            .await
            .unwrap();

        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(dst)
            .upload_id(&upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .part_number(1)
                            .e_tag(part.copy_part_result().unwrap().e_tag().unwrap())
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(dst)
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(data.len(), PART_SIZE);

        cleanup_multipart_bucket(&bucket, &[src, dst]).await;
    });
}

#[test]
fn test_object_lock_configuration_expected_bucket_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_object_lock_bucket().await;
        let config = simple_object_lock_configuration();

        expect_owner_denied!(client
            .put_object_lock_configuration()
            .bucket(&bucket)
            .object_lock_configuration(config.clone()));
        expect_owner_ok!(client
            .put_object_lock_configuration()
            .bucket(&bucket)
            .object_lock_configuration(config.clone()));

        expect_owner_denied!(client.get_object_lock_configuration().bucket(&bucket));
        let resp = expect_owner_ok!(client.get_object_lock_configuration().bucket(&bucket));
        assert_eq!(resp.object_lock_configuration(), Some(&config));

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_retention_expected_bucket_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_object_lock_bucket().await;
        let key = "retention";
        let retention = governance_retention_tomorrow();

        put_object_bytes(&bucket, key, b"retained").await;

        expect_owner_denied!(client
            .put_object_retention()
            .bucket(&bucket)
            .key(key)
            .retention(retention.clone()));
        expect_owner_ok!(client
            .put_object_retention()
            .bucket(&bucket)
            .key(key)
            .retention(retention.clone()));

        expect_owner_denied!(client.get_object_retention().bucket(&bucket).key(key));
        let resp = expect_owner_ok!(client.get_object_retention().bucket(&bucket).key(key));
        assert_eq!(resp.retention(), Some(&retention));

        cleanup_object_lock_bucket(&bucket).await;
    });
}

#[test]
fn test_object_legal_hold_expected_bucket_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_object_lock_bucket().await;
        let key = "hold";

        put_object_bytes(&bucket, key, b"held").await;

        expect_owner_denied!(client
            .put_object_legal_hold()
            .bucket(&bucket)
            .key(key)
            .legal_hold(legal_hold(ObjectLockLegalHoldStatus::On)));
        expect_owner_ok!(client
            .put_object_legal_hold()
            .bucket(&bucket)
            .key(key)
            .legal_hold(legal_hold(ObjectLockLegalHoldStatus::On)));

        expect_owner_denied!(client.get_object_legal_hold().bucket(&bucket).key(key));
        let resp = expect_owner_ok!(client.get_object_legal_hold().bucket(&bucket).key(key));
        assert_eq!(
            resp.legal_hold(),
            Some(&legal_hold(ObjectLockLegalHoldStatus::On))
        );

        cleanup_object_lock_bucket(&bucket).await;
    });
}
