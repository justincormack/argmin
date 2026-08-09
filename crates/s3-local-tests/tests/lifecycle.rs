// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::time::{SystemTime, UNIX_EPOCH};

use s3_tests::server::{TEST_ACCESS_KEY, TEST_REGION, TEST_SECRET_KEY};
use s3_tests::{
    assert_s3_err_code,
    aws_sdk_s3::{
        self,
        primitives::{ByteStream, DateTime},
        types::{
            AbortIncompleteMultipartUpload, BucketLifecycleConfiguration, BucketVersioningStatus,
            ExpirationStatus, LifecycleExpiration, LifecycleRule, LifecycleRuleFilter,
            NoncurrentVersionExpiration, ObjectLockMode, Tag, VersioningConfiguration,
        },
    },
    build_client_with_ca, err_status, put_bucket_lifecycle_with_md5, TestServer, RT,
};

const DAY_MILLIS: u64 = 24 * 60 * 60 * 1000;

fn run_local<F: std::future::Future>(f: F) -> F::Output {
    RT.block_on(f)
}

async fn test_client(server: &TestServer) -> aws_sdk_s3::Client {
    build_client_with_ca(
        server.endpoint(),
        TEST_ACCESS_KEY,
        TEST_SECRET_KEY,
        TEST_REGION,
        server.tls_ca_pem(),
    )
}

async fn create_bucket(client: &aws_sdk_s3::Client, bucket: &str) {
    client.create_bucket().bucket(bucket).send().await.unwrap();
}

async fn create_object_lock_bucket(client: &aws_sdk_s3::Client, bucket: &str) {
    client
        .create_bucket()
        .bucket(bucket)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();
}

async fn enable_versioning(client: &aws_sdk_s3::Client, bucket: &str) {
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
}

async fn suspend_versioning(client: &aws_sdk_s3::Client, bucket: &str) {
    client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Suspended)
                .build(),
        )
        .send()
        .await
        .unwrap();
}

async fn put_expiration_lifecycle(client: &aws_sdk_s3::Client, bucket: &str, prefix: &str) {
    let rule = LifecycleRule::builder()
        .id("expire-current")
        .filter(LifecycleRuleFilter::builder().prefix(prefix).build())
        .status(ExpirationStatus::Enabled)
        .expiration(LifecycleExpiration::builder().days(1).build())
        .build()
        .expect("valid expiration lifecycle rule");
    let config = BucketLifecycleConfiguration::builder()
        .rules(rule)
        .build()
        .expect("valid lifecycle configuration");
    put_bucket_lifecycle_with_md5(client, bucket, config)
        .send()
        .await
        .unwrap();
}

async fn put_expiration_date_lifecycle(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    date: DateTime,
) {
    let rule = LifecycleRule::builder()
        .id("expire-by-date")
        .filter(LifecycleRuleFilter::builder().prefix(prefix).build())
        .status(ExpirationStatus::Enabled)
        .expiration(LifecycleExpiration::builder().date(date).build())
        .build()
        .expect("valid date expiration lifecycle rule");
    let config = BucketLifecycleConfiguration::builder()
        .rules(rule)
        .build()
        .expect("valid lifecycle configuration");
    put_bucket_lifecycle_with_md5(client, bucket, config)
        .send()
        .await
        .unwrap();
}

async fn put_expiration_size_filter_lifecycle(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    id: &str,
    object_size_greater_than: Option<i64>,
    object_size_less_than: Option<i64>,
) {
    let rule = LifecycleRule::builder()
        .id(id)
        .filter(
            LifecycleRuleFilter::builder()
                .set_object_size_greater_than(object_size_greater_than)
                .set_object_size_less_than(object_size_less_than)
                .build(),
        )
        .status(ExpirationStatus::Enabled)
        .expiration(LifecycleExpiration::builder().days(1).build())
        .build()
        .expect("valid size-filter expiration lifecycle rule");
    let config = BucketLifecycleConfiguration::builder()
        .rules(rule)
        .build()
        .expect("valid lifecycle configuration");
    put_bucket_lifecycle_with_md5(client, bucket, config)
        .send()
        .await
        .unwrap();
}

async fn put_tag_expiration_lifecycle(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    value: &str,
) {
    let rule = LifecycleRule::builder()
        .id("expire-tagged")
        .filter(
            LifecycleRuleFilter::builder()
                .tag(Tag::builder().key(key).value(value).build().unwrap())
                .build(),
        )
        .status(ExpirationStatus::Enabled)
        .expiration(LifecycleExpiration::builder().days(1).build())
        .build()
        .expect("valid tag expiration lifecycle rule");
    let config = BucketLifecycleConfiguration::builder()
        .rules(rule)
        .build()
        .expect("valid lifecycle configuration");
    put_bucket_lifecycle_with_md5(client, bucket, config)
        .send()
        .await
        .unwrap();
}

async fn put_noncurrent_expiration_lifecycle(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    newer_noncurrent_versions: Option<i32>,
) {
    let rule = LifecycleRule::builder()
        .id("expire-noncurrent")
        .filter(LifecycleRuleFilter::builder().prefix(prefix).build())
        .status(ExpirationStatus::Enabled)
        .noncurrent_version_expiration(
            NoncurrentVersionExpiration::builder()
                .noncurrent_days(1)
                .set_newer_noncurrent_versions(newer_noncurrent_versions)
                .build(),
        )
        .build()
        .expect("valid noncurrent expiration lifecycle rule");
    let config = BucketLifecycleConfiguration::builder()
        .rules(rule)
        .build()
        .expect("valid lifecycle configuration");
    put_bucket_lifecycle_with_md5(client, bucket, config)
        .send()
        .await
        .unwrap();
}

async fn put_noncurrent_tag_expiration_lifecycle(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    value: &str,
) {
    let rule = LifecycleRule::builder()
        .id("expire-noncurrent-tagged")
        .filter(
            LifecycleRuleFilter::builder()
                .tag(Tag::builder().key(key).value(value).build().unwrap())
                .build(),
        )
        .status(ExpirationStatus::Enabled)
        .noncurrent_version_expiration(
            NoncurrentVersionExpiration::builder()
                .noncurrent_days(1)
                .build(),
        )
        .build()
        .expect("valid tagged noncurrent expiration lifecycle rule");
    let config = BucketLifecycleConfiguration::builder()
        .rules(rule)
        .build()
        .expect("valid lifecycle configuration");
    put_bucket_lifecycle_with_md5(client, bucket, config)
        .send()
        .await
        .unwrap();
}

async fn put_abort_incomplete_multipart_lifecycle(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
) {
    let rule = LifecycleRule::builder()
        .id("abort-incomplete")
        .filter(LifecycleRuleFilter::builder().prefix(prefix).build())
        .status(ExpirationStatus::Enabled)
        .abort_incomplete_multipart_upload(
            AbortIncompleteMultipartUpload::builder()
                .days_after_initiation(1)
                .build(),
        )
        .build()
        .expect("valid abort lifecycle rule");
    let config = BucketLifecycleConfiguration::builder()
        .rules(rule)
        .build()
        .expect("valid lifecycle configuration");
    put_bucket_lifecycle_with_md5(client, bucket, config)
        .send()
        .await
        .unwrap();
}

async fn put_expired_delete_marker_lifecycle(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
) {
    let rule = LifecycleRule::builder()
        .id("expire-marker")
        .filter(LifecycleRuleFilter::builder().prefix(prefix).build())
        .status(ExpirationStatus::Enabled)
        .expiration(
            LifecycleExpiration::builder()
                .expired_object_delete_marker(true)
                .build(),
        )
        .build()
        .expect("valid expired delete marker lifecycle rule");
    let config = BucketLifecycleConfiguration::builder()
        .rules(rule)
        .build()
        .expect("valid lifecycle configuration");
    put_bucket_lifecycle_with_md5(client, bucket, config)
        .send()
        .await
        .unwrap();
}

fn smithy_millis(dt: &DateTime) -> u64 {
    u64::try_from(dt.secs()).expect("non-negative timestamp") * 1000
}

fn lifecycle_day_deadline(start_millis: u64, days: u32) -> u64 {
    ((start_millis / DAY_MILLIS) + u64::from(days) + 1) * DAY_MILLIS
}

fn future_retention_date() -> DateTime {
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    DateTime::from_secs(now_secs + 7 * 24 * 60 * 60)
}

async fn current_object_deadline_millis(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    days: u32,
) -> u64 {
    let head = client
        .head_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    lifecycle_day_deadline(smithy_millis(head.last_modified().unwrap()), days)
}

async fn list_object_keys_v2(client: &aws_sdk_s3::Client, bucket: &str) -> Vec<String> {
    client
        .list_objects_v2()
        .bucket(bucket)
        .send()
        .await
        .unwrap()
        .contents()
        .iter()
        .filter_map(|entry| entry.key().map(ToString::to_string))
        .collect()
}

async fn list_object_keys(client: &aws_sdk_s3::Client, bucket: &str) -> Vec<String> {
    client
        .list_objects()
        .bucket(bucket)
        .send()
        .await
        .unwrap()
        .contents()
        .iter()
        .filter_map(|entry| entry.key().map(ToString::to_string))
        .collect()
}

#[test]
fn test_lifecycle_expiration_deletes_nonversioned_objects_on_manual_sweep() {
    run_local(async {
        let server = TestServer::start().await;
        let client = test_client(&server).await;
        let bucket = "lifecycle-nonversioned";

        create_bucket(&client, bucket).await;
        put_expiration_lifecycle(&client, bucket, "logs/").await;

        client
            .put_object()
            .bucket(bucket)
            .key("logs/expire-me")
            .body(ByteStream::from_static(b"expired"))
            .send()
            .await
            .unwrap();
        client
            .put_object()
            .bucket(bucket)
            .key("keep/me")
            .body(ByteStream::from_static(b"keep"))
            .send()
            .await
            .unwrap();
        let sweep_at = current_object_deadline_millis(&client, bucket, "logs/expire-me", 1).await;

        server.run_lifecycle_sweep_at(sweep_at).unwrap();

        let expired = client
            .get_object()
            .bucket(bucket)
            .key("logs/expire-me")
            .send()
            .await;
        assert_eq!(err_status(&expired), 404);
        assert_s3_err_code(&expired, "NoSuchKey");

        let kept = client
            .get_object()
            .bucket(bucket)
            .key("keep/me")
            .send()
            .await
            .unwrap();
        let kept_body = kept.body.collect().await.unwrap().into_bytes();
        assert_eq!(&kept_body[..], b"keep");

        assert_eq!(list_object_keys_v2(&client, bucket).await, vec!["keep/me"]);
    });
}

#[test]
fn test_lifecycle_expiration_updates_list_objects_v1_after_manual_sweep() {
    run_local(async {
        let server = TestServer::start().await;
        let client = test_client(&server).await;
        let bucket = "lifecycle-list-objects-v1";

        create_bucket(&client, bucket).await;
        put_expiration_lifecycle(&client, bucket, "logs/").await;

        client
            .put_object()
            .bucket(bucket)
            .key("logs/expire-me")
            .body(ByteStream::from_static(b"expired"))
            .send()
            .await
            .unwrap();
        client
            .put_object()
            .bucket(bucket)
            .key("keep/me")
            .body(ByteStream::from_static(b"keep"))
            .send()
            .await
            .unwrap();
        let sweep_at = current_object_deadline_millis(&client, bucket, "logs/expire-me", 1).await;

        server.run_lifecycle_sweep_at(sweep_at).unwrap();

        assert_eq!(list_object_keys(&client, bucket).await, vec!["keep/me"]);
    });
}

#[test]
fn test_lifecycle_expiration_creates_delete_marker_in_versioned_bucket() {
    run_local(async {
        let server = TestServer::start().await;
        let client = test_client(&server).await;
        let bucket = "lifecycle-versioned";
        let key = "logs/object";

        create_bucket(&client, bucket).await;
        enable_versioning(&client, bucket).await;
        put_expiration_lifecycle(&client, bucket, "logs/").await;

        let put = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(b"versioned"))
            .send()
            .await
            .unwrap();
        let live_version_id = put.version_id().unwrap().to_string();
        let sweep_at = current_object_deadline_millis(&client, bucket, key, 1).await;

        server.run_lifecycle_sweep_at(sweep_at).unwrap();

        let current = client.get_object().bucket(bucket).key(key).send().await;
        assert_eq!(err_status(&current), 404);
        assert_s3_err_code(&current, "NoSuchKey");

        let versions = client
            .list_object_versions()
            .bucket(bucket)
            .prefix(key)
            .send()
            .await
            .unwrap();
        assert_eq!(versions.versions().len(), 1);
        assert_eq!(versions.delete_markers().len(), 1);
        assert_eq!(
            versions.versions()[0].version_id(),
            Some(live_version_id.as_str())
        );
        assert_eq!(versions.versions()[0].is_latest(), Some(false));
        assert_eq!(versions.delete_markers()[0].is_latest(), Some(true));
        assert_ne!(versions.delete_markers()[0].version_id(), Some("null"));
    });
}

#[test]
fn test_lifecycle_expiration_replaces_suspended_null_current_with_null_delete_marker() {
    run_local(async {
        let server = TestServer::start().await;
        let client = test_client(&server).await;
        let bucket = "lifecycle-suspended";
        let key = "logs/object";

        create_bucket(&client, bucket).await;
        enable_versioning(&client, bucket).await;
        suspend_versioning(&client, bucket).await;
        put_expiration_lifecycle(&client, bucket, "logs/").await;

        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(b"null-current"))
            .send()
            .await
            .unwrap();
        let sweep_at = current_object_deadline_millis(&client, bucket, key, 1).await;

        server.run_lifecycle_sweep_at(sweep_at).unwrap();

        let current = client.get_object().bucket(bucket).key(key).send().await;
        assert_eq!(err_status(&current), 404);
        assert_s3_err_code(&current, "NoSuchKey");

        let versions = client
            .list_object_versions()
            .bucket(bucket)
            .prefix(key)
            .send()
            .await
            .unwrap();
        assert!(versions.versions().is_empty());
        assert_eq!(versions.delete_markers().len(), 1);
        assert_eq!(versions.delete_markers()[0].version_id(), Some("null"));
        assert_eq!(versions.delete_markers()[0].is_latest(), Some(true));
    });
}

#[test]
fn test_lifecycle_tag_expiration_creates_delete_marker_for_matching_versioned_object() {
    run_local(async {
        let server = TestServer::start().await;
        let client = test_client(&server).await;
        let bucket = "lifecycle-tagged-versioned";
        let matching_key = "match";
        let keep_key = "keep";

        create_bucket(&client, bucket).await;
        enable_versioning(&client, bucket).await;
        put_tag_expiration_lifecycle(&client, bucket, "env", "prod").await;

        client
            .put_object()
            .bucket(bucket)
            .key(matching_key)
            .tagging("env=prod")
            .body(ByteStream::from_static(b"match"))
            .send()
            .await
            .unwrap();
        client
            .put_object()
            .bucket(bucket)
            .key(keep_key)
            .tagging("env=dev")
            .body(ByteStream::from_static(b"keep"))
            .send()
            .await
            .unwrap();
        let sweep_at = current_object_deadline_millis(&client, bucket, matching_key, 1).await;

        server.run_lifecycle_sweep_at(sweep_at).unwrap();

        let current = client
            .get_object()
            .bucket(bucket)
            .key(matching_key)
            .send()
            .await;
        assert_eq!(err_status(&current), 404);
        assert_s3_err_code(&current, "NoSuchKey");

        let kept = client
            .get_object()
            .bucket(bucket)
            .key(keep_key)
            .send()
            .await
            .unwrap();
        let kept_body = kept.body.collect().await.unwrap().into_bytes();
        assert_eq!(&kept_body[..], b"keep");

        let versions = client
            .list_object_versions()
            .bucket(bucket)
            .prefix(matching_key)
            .send()
            .await
            .unwrap();
        assert_eq!(versions.versions().len(), 1);
        assert_eq!(versions.delete_markers().len(), 1);
        assert_eq!(versions.versions()[0].is_latest(), Some(false));
        assert_eq!(versions.delete_markers()[0].is_latest(), Some(true));

        assert_eq!(
            list_object_keys_v2(&client, bucket).await,
            vec![keep_key.to_string()]
        );
    });
}

#[test]
fn test_lifecycle_abort_incomplete_multipart_upload_runs_on_manual_sweep() {
    run_local(async {
        let server = TestServer::start().await;
        let client = test_client(&server).await;
        let bucket = "lifecycle-abort-mpu";
        let key = "uploads/incomplete";

        create_bucket(&client, bucket).await;
        put_abort_incomplete_multipart_lifecycle(&client, bucket, "uploads/").await;

        let create = client
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();
        let sweep_at = smithy_millis(create.abort_date().unwrap());

        let before = client
            .list_multipart_uploads()
            .bucket(bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(before.uploads().len(), 1);

        server.run_lifecycle_sweep_at(sweep_at).unwrap();

        let after = client
            .list_multipart_uploads()
            .bucket(bucket)
            .send()
            .await
            .unwrap();
        assert!(after.uploads().is_empty());

        let list_parts = client
            .list_parts()
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await;
        assert_eq!(err_status(&list_parts), 404);
        assert_s3_err_code(&list_parts, "NoSuchUpload");
    });
}

#[test]
fn test_lifecycle_noncurrent_expiration_deletes_due_noncurrent_version() {
    run_local(async {
        let server = TestServer::start().await;
        let client = test_client(&server).await;
        let bucket = "lifecycle-noncurrent";
        let key = "logs/object";

        create_bucket(&client, bucket).await;
        enable_versioning(&client, bucket).await;
        put_noncurrent_expiration_lifecycle(&client, bucket, "logs/", None).await;

        let first = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(b"v1"))
            .send()
            .await
            .unwrap();
        let first_version_id = first.version_id().unwrap().to_string();

        let second = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(b"v2"))
            .send()
            .await
            .unwrap();
        let second_version_id = second.version_id().unwrap().to_string();
        let sweep_at = current_object_deadline_millis(&client, bucket, key, 1).await;

        server.run_lifecycle_sweep_at(sweep_at).unwrap();

        let current = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let current_body = current.body.collect().await.unwrap().into_bytes();
        assert_eq!(&current_body[..], b"v2");

        let old = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .version_id(&first_version_id)
            .send()
            .await;
        assert_eq!(err_status(&old), 404);
        assert_s3_err_code(&old, "NoSuchVersion");

        let versions = client
            .list_object_versions()
            .bucket(bucket)
            .prefix(key)
            .send()
            .await
            .unwrap();
        assert_eq!(versions.versions().len(), 1);
        assert!(versions.delete_markers().is_empty());
        assert_eq!(
            versions.versions()[0].version_id(),
            Some(second_version_id.as_str())
        );
        assert_eq!(versions.versions()[0].is_latest(), Some(true));
    });
}

#[test]
fn test_lifecycle_noncurrent_expiration_retains_newest_required_noncurrent_versions() {
    run_local(async {
        let server = TestServer::start().await;
        let client = test_client(&server).await;
        let bucket = "lifecycle-retain-newer";
        let key = "logs/object";

        create_bucket(&client, bucket).await;
        enable_versioning(&client, bucket).await;
        put_noncurrent_expiration_lifecycle(&client, bucket, "logs/", Some(1)).await;

        let first = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(b"v1"))
            .send()
            .await
            .unwrap();
        let first_version_id = first.version_id().unwrap().to_string();

        let second = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(b"v2"))
            .send()
            .await
            .unwrap();
        let second_version_id = second.version_id().unwrap().to_string();

        let third = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(b"v3"))
            .send()
            .await
            .unwrap();
        let third_version_id = third.version_id().unwrap().to_string();
        let sweep_at = current_object_deadline_millis(&client, bucket, key, 1).await;

        server.run_lifecycle_sweep_at(sweep_at).unwrap();

        let first = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .version_id(&first_version_id)
            .send()
            .await;
        assert_eq!(err_status(&first), 404);
        assert_s3_err_code(&first, "NoSuchVersion");

        let retained = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .version_id(&second_version_id)
            .send()
            .await
            .unwrap();
        let retained_body = retained.body.collect().await.unwrap().into_bytes();
        assert_eq!(&retained_body[..], b"v2");

        let versions = client
            .list_object_versions()
            .bucket(bucket)
            .prefix(key)
            .send()
            .await
            .unwrap();
        assert_eq!(versions.versions().len(), 2);
        assert_eq!(
            versions.versions()[0].version_id(),
            Some(third_version_id.as_str())
        );
        assert_eq!(versions.versions()[0].is_latest(), Some(true));
        assert_eq!(
            versions.versions()[1].version_id(),
            Some(second_version_id.as_str())
        );
        assert_eq!(versions.versions()[1].is_latest(), Some(false));
    });
}

#[test]
fn test_lifecycle_noncurrent_tag_expiration_only_deletes_matching_noncurrent_versions() {
    run_local(async {
        let server = TestServer::start().await;
        let client = test_client(&server).await;
        let bucket = "lifecycle-noncurrent-tagged";
        let matching_key = "match";
        let keep_key = "keep";

        create_bucket(&client, bucket).await;
        enable_versioning(&client, bucket).await;
        put_noncurrent_tag_expiration_lifecycle(&client, bucket, "env", "prod").await;

        let matching_first = client
            .put_object()
            .bucket(bucket)
            .key(matching_key)
            .tagging("env=prod")
            .body(ByteStream::from_static(b"v1"))
            .send()
            .await
            .unwrap();
        let matching_first_version_id = matching_first.version_id().unwrap().to_string();
        client
            .put_object()
            .bucket(bucket)
            .key(matching_key)
            .body(ByteStream::from_static(b"v2"))
            .send()
            .await
            .unwrap();

        let keep_first = client
            .put_object()
            .bucket(bucket)
            .key(keep_key)
            .tagging("env=dev")
            .body(ByteStream::from_static(b"k1"))
            .send()
            .await
            .unwrap();
        let keep_first_version_id = keep_first.version_id().unwrap().to_string();
        let keep_second = client
            .put_object()
            .bucket(bucket)
            .key(keep_key)
            .body(ByteStream::from_static(b"k2"))
            .send()
            .await
            .unwrap();
        let keep_second_version_id = keep_second.version_id().unwrap().to_string();

        let sweep_at = current_object_deadline_millis(&client, bucket, matching_key, 1).await;
        server.run_lifecycle_sweep_at(sweep_at).unwrap();

        let expired = client
            .get_object()
            .bucket(bucket)
            .key(matching_key)
            .version_id(&matching_first_version_id)
            .send()
            .await;
        assert_eq!(err_status(&expired), 404);
        assert_s3_err_code(&expired, "NoSuchVersion");

        let retained = client
            .get_object()
            .bucket(bucket)
            .key(keep_key)
            .version_id(&keep_first_version_id)
            .send()
            .await
            .unwrap();
        let retained_body = retained.body.collect().await.unwrap().into_bytes();
        assert_eq!(&retained_body[..], b"k1");

        let versions = client
            .list_object_versions()
            .bucket(bucket)
            .prefix(keep_key)
            .send()
            .await
            .unwrap();
        assert_eq!(versions.versions().len(), 2);
        assert_eq!(
            versions.versions()[0].version_id(),
            Some(keep_second_version_id.as_str())
        );
        assert_eq!(
            versions.versions()[1].version_id(),
            Some(keep_first_version_id.as_str())
        );
    });
}

#[test]
fn test_lifecycle_expiration_days_removes_sole_current_delete_marker() {
    run_local(async {
        let server = TestServer::start().await;
        let client = test_client(&server).await;
        let bucket = "lifecycle-delete-marker-cleanup";
        let key = "logs/object";

        create_bucket(&client, bucket).await;
        enable_versioning(&client, bucket).await;
        put_expiration_lifecycle(&client, bucket, "logs/").await;

        let put = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(b"current"))
            .send()
            .await
            .unwrap();
        let live_version_id = put.version_id().unwrap().to_string();
        let first_sweep_at = current_object_deadline_millis(&client, bucket, key, 1).await;

        server.run_lifecycle_sweep_at(first_sweep_at).unwrap();

        client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .version_id(&live_version_id)
            .send()
            .await
            .unwrap();

        let versions = client
            .list_object_versions()
            .bucket(bucket)
            .prefix(key)
            .send()
            .await
            .unwrap();
        assert!(versions.versions().is_empty());
        assert_eq!(versions.delete_markers().len(), 1);
        let delete_marker = versions.delete_markers()[0].clone();
        let second_sweep_at =
            lifecycle_day_deadline(smithy_millis(delete_marker.last_modified().unwrap()), 1);

        server.run_lifecycle_sweep_at(second_sweep_at).unwrap();

        let versions = client
            .list_object_versions()
            .bucket(bucket)
            .prefix(key)
            .send()
            .await
            .unwrap();
        assert!(versions.versions().is_empty());
        assert!(versions.delete_markers().is_empty());
    });
}

#[test]
fn test_lifecycle_expired_object_delete_marker_rule_removes_sole_delete_marker() {
    run_local(async {
        let server = TestServer::start().await;
        let client = test_client(&server).await;
        let bucket = "lifecycle-expired-delete-marker";
        let key = "logs/object";

        create_bucket(&client, bucket).await;
        enable_versioning(&client, bucket).await;
        put_expired_delete_marker_lifecycle(&client, bucket, "logs/").await;

        let put = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(b"current"))
            .send()
            .await
            .unwrap();
        let live_version_id = put.version_id().unwrap().to_string();

        client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .version_id(&live_version_id)
            .send()
            .await
            .unwrap();

        let before = client
            .list_object_versions()
            .bucket(bucket)
            .prefix(key)
            .send()
            .await
            .unwrap();
        assert!(before.versions().is_empty());
        assert_eq!(before.delete_markers().len(), 1);

        server.run_lifecycle_sweep_at(0).unwrap();

        let after = client
            .list_object_versions()
            .bucket(bucket)
            .prefix(key)
            .send()
            .await
            .unwrap();
        assert!(after.versions().is_empty());
        assert!(after.delete_markers().is_empty());
    });
}

#[test]
fn test_lifecycle_noncurrent_expiration_respects_object_lock_retention() {
    run_local(async {
        let server = TestServer::start().await;
        let client = test_client(&server).await;
        let bucket = "lifecycle-object-lock";
        let key = "logs/object";

        create_object_lock_bucket(&client, bucket).await;
        put_noncurrent_expiration_lifecycle(&client, bucket, "logs/", None).await;

        let first = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .object_lock_mode(ObjectLockMode::Governance)
            .object_lock_retain_until_date(future_retention_date())
            .body(ByteStream::from_static(b"locked"))
            .send()
            .await
            .unwrap();
        let first_version_id = first.version_id().unwrap().to_string();

        let second = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(b"current"))
            .send()
            .await
            .unwrap();
        let second_version_id = second.version_id().unwrap().to_string();
        let sweep_at = current_object_deadline_millis(&client, bucket, key, 1).await;

        server.run_lifecycle_sweep_at(sweep_at).unwrap();

        let locked = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .version_id(&first_version_id)
            .send()
            .await
            .unwrap();
        let locked_body = locked.body.collect().await.unwrap().into_bytes();
        assert_eq!(&locked_body[..], b"locked");

        let versions = client
            .list_object_versions()
            .bucket(bucket)
            .prefix(key)
            .send()
            .await
            .unwrap();
        assert_eq!(versions.versions().len(), 2);
        assert_eq!(
            versions.versions()[0].version_id(),
            Some(second_version_id.as_str())
        );
        assert_eq!(versions.versions()[0].is_latest(), Some(true));
        assert_eq!(
            versions.versions()[1].version_id(),
            Some(first_version_id.as_str())
        );
        assert_eq!(versions.versions()[1].is_latest(), Some(false));
    });
}

#[test]
fn test_lifecycle_expiration_date_expires_due_prefix_and_updates_list_objects_v2() {
    run_local(async {
        let server = TestServer::start().await;
        let client = test_client(&server).await;
        let bucket = "lifecycle-date-expiration";

        create_bucket(&client, bucket).await;

        let past_date = DateTime::from_secs(0);
        put_expiration_date_lifecycle(&client, bucket, "past/", past_date).await;

        client
            .put_object()
            .bucket(bucket)
            .key("past/foo")
            .body(ByteStream::from_static(b"expire"))
            .send()
            .await
            .unwrap();
        client
            .put_object()
            .bucket(bucket)
            .key("future/bar")
            .body(ByteStream::from_static(b"keep"))
            .send()
            .await
            .unwrap();

        server
            .run_lifecycle_sweep_at(smithy_millis(&past_date))
            .unwrap();

        let expired = client
            .get_object()
            .bucket(bucket)
            .key("past/foo")
            .send()
            .await;
        assert_eq!(err_status(&expired), 404);
        assert_s3_err_code(&expired, "NoSuchKey");

        assert_eq!(
            list_object_keys_v2(&client, bucket).await,
            vec!["future/bar"]
        );
    });
}

#[test]
fn test_lifecycle_expiration_size_greater_than_expires_matching_objects() {
    run_local(async {
        let server = TestServer::start().await;
        let client = test_client(&server).await;
        let bucket = "lifecycle-size-gt";

        create_bucket(&client, bucket).await;
        put_expiration_size_filter_lifecycle(&client, bucket, "size-gt", Some(2000), None).await;

        client
            .put_object()
            .bucket(bucket)
            .key("small")
            .body(ByteStream::from(vec![b'a'; 1000]))
            .send()
            .await
            .unwrap();
        client
            .put_object()
            .bucket(bucket)
            .key("big")
            .body(ByteStream::from(vec![b'b'; 3000]))
            .send()
            .await
            .unwrap();

        let sweep_at = current_object_deadline_millis(&client, bucket, "big", 1).await;
        server.run_lifecycle_sweep_at(sweep_at).unwrap();

        assert_eq!(list_object_keys_v2(&client, bucket).await, vec!["small"]);
    });
}

#[test]
fn test_lifecycle_expiration_size_less_than_expires_matching_objects() {
    run_local(async {
        let server = TestServer::start().await;
        let client = test_client(&server).await;
        let bucket = "lifecycle-size-lt";

        create_bucket(&client, bucket).await;
        put_expiration_size_filter_lifecycle(&client, bucket, "size-lt", None, Some(2000)).await;

        client
            .put_object()
            .bucket(bucket)
            .key("small")
            .body(ByteStream::from(vec![b'a'; 1000]))
            .send()
            .await
            .unwrap();
        client
            .put_object()
            .bucket(bucket)
            .key("big")
            .body(ByteStream::from(vec![b'b'; 3000]))
            .send()
            .await
            .unwrap();

        let sweep_at = current_object_deadline_millis(&client, bucket, "small", 1).await;
        server.run_lifecycle_sweep_at(sweep_at).unwrap();

        assert_eq!(list_object_keys_v2(&client, bucket).await, vec!["big"]);
    });
}
