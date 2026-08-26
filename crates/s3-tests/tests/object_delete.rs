// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, Delete, DeletedObject, Error as DeleteObjectError, ObjectIdentifier,
    VersioningConfiguration,
};
use s3_tests::{
    assert_s3_err_code, content_md5_header, create_objects, create_objects_with_keys,
    delete_all_and_bucket, delete_objects_retrying_exact_operation_aborted_result,
    delete_objects_with_md5, err_status, send_signed_request,
    shape::{assert_body_with_unordered_blocks, assert_shape, shape, xml_response_headers},
    unique_bucket, SendRetryingOperationAborted, CTX,
};
use serde_json::json;
use std::sync::Arc;

// ── Local helpers ───────────────────────────────────────────────────

fn make_object_id(key: &str) -> ObjectIdentifier {
    ObjectIdentifier::builder()
        .key(key)
        .build()
        .expect("build ObjectIdentifier")
}

fn make_object_id_with_etag(key: &str, etag: &str) -> ObjectIdentifier {
    ObjectIdentifier::builder()
        .key(key)
        .e_tag(etag)
        .build()
        .expect("build ObjectIdentifier")
}

fn make_object_id_with_version_and_etag(
    key: &str,
    version_id: &str,
    etag: &str,
) -> ObjectIdentifier {
    ObjectIdentifier::builder()
        .key(key)
        .version_id(version_id)
        .e_tag(etag)
        .build()
        .expect("build ObjectIdentifier")
}

fn make_delete_request(keys: &[&str], quiet: bool) -> Delete {
    let objects: Vec<ObjectIdentifier> = keys.iter().map(|k| make_object_id(k)).collect();
    Delete::builder()
        .set_objects(Some(objects))
        .quiet(quiet)
        .build()
        .expect("build Delete")
}

fn get_keys(objects: &[aws_sdk_s3::types::Object]) -> Vec<String> {
    objects
        .iter()
        .filter_map(|o| o.key().map(str::to_string))
        .collect()
}

fn object_resource(bucket: &str, key: &str) -> String {
    format!("arn:aws:s3:::{bucket}/{key}")
}

fn alt_policy_principal() -> serde_json::Value {
    json!({ "AWS": format!("arn:aws:iam::{}:root", CTX.alt_account_id()) })
}

fn object_tagging(key: &str, value: &str) -> aws_sdk_s3::types::Tagging {
    aws_sdk_s3::types::Tagging::builder()
        .tag_set(
            aws_sdk_s3::types::Tag::builder()
                .key(key)
                .value(value)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap()
}

async fn put_object(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    body: impl Into<Vec<u8>>,
) -> aws_sdk_s3::operation::put_object::PutObjectOutput {
    s3_tests::put_object_retrying_operation_aborted(client, bucket, key, body.into()).await
}

type DeleteObjectsCallResult = Result<
    aws_sdk_s3::operation::delete_objects::DeleteObjectsOutput,
    aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::delete_objects::DeleteObjectsError>,
>;

type PutObjectCallResult = Result<
    aws_sdk_s3::operation::put_object::PutObjectOutput,
    aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::put_object::PutObjectError>,
>;

#[derive(Debug)]
enum ConditionalDeleteObjectsEntryOutcome {
    Deleted {
        delete_marker_version_id: Option<String>,
    },
    Rejected {
        code: String,
    },
}

async fn race_conditional_delete_objects_with_put(
    bucket: &str,
    key: &str,
    etag: &str,
    canary_key: &str,
    canary_etag: &str,
    replacement_body: &'static [u8],
    replacement_state: &'static str,
) -> (DeleteObjectsCallResult, PutObjectCallResult) {
    let delete = Delete::builder()
        .set_objects(Some(vec![
            make_object_id_with_etag(key, etag),
            make_object_id_with_etag(canary_key, canary_etag),
        ]))
        .quiet(false)
        .build()
        .unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let delete_client = CTX.client().clone();
    let delete_bucket = bucket.to_string();
    let delete_barrier = Arc::clone(&barrier);
    let delete_task = tokio::spawn(async move {
        delete_barrier.wait().await;
        delete_objects_retrying_exact_operation_aborted_result(
            &delete_client,
            &delete_bucket,
            delete,
        )
        .await
    });
    let put_client = CTX.client().clone();
    let put_bucket = bucket.to_string();
    let put_key = key.to_string();
    let put_task = tokio::spawn(async move {
        barrier.wait().await;
        // Either side of the deliberate overlap can receive AWS's transient
        // whole-request contention response. Retrying only OperationAborted
        // preserves the race while ensuring the unconditional replacement
        // eventually establishes the state inspected below.
        s3_tests::retrying_exact_operation_aborted_result(|| {
            let request = put_client
                .put_object()
                .bucket(put_bucket.clone())
                .key(put_key.clone())
                .metadata("replacement-state", replacement_state)
                .body(ByteStream::from_static(replacement_body));
            async move { request.send().await }
        })
        .await
    });
    let (delete, put) = tokio::join!(delete_task, put_task);
    let delete = match delete.unwrap() {
        Ok(output) => {
            Ok(
                retry_delete_objects_canary_slow_down(bucket, canary_key, canary_etag, output)
                    .await,
            )
        }
        Err(error) => Err(error),
    };
    (delete, put.unwrap())
}

async fn retry_delete_objects_canary_slow_down(
    bucket: &str,
    canary_key: &str,
    canary_etag: &str,
    output: aws_sdk_s3::operation::delete_objects::DeleteObjectsOutput,
) -> aws_sdk_s3::operation::delete_objects::DeleteObjectsOutput {
    if !validate_canary_slow_down_for_retry(&output, canary_key) {
        return output;
    }

    // Preserve the conditional target's exact race result. Only replay the
    // independent canary entry whose SlowDown means it was not admitted.
    let retry = delete_objects_with_md5(
        CTX.client(),
        bucket,
        Delete::builder()
            .objects(make_object_id_with_etag(canary_key, canary_etag))
            .quiet(false)
            .build()
            .unwrap(),
    )
    .send()
    .await
    .unwrap_or_else(|error| panic!("retried multi-delete canary failed: {error:?}"));
    assert!(
        retry.errors().is_empty(),
        "retried multi-delete canary failed: {retry:?}"
    );
    assert_eq!(retry.deleted().len(), 1, "{retry:?}");
    assert_eq!(retry.deleted()[0].key(), Some(canary_key));

    let mut deleted = output.deleted().to_vec();
    deleted.extend(retry.deleted().iter().cloned());
    let errors = output
        .errors()
        .iter()
        .filter(|error| error.key() != Some(canary_key))
        .cloned()
        .collect::<Vec<_>>();
    aws_sdk_s3::operation::delete_objects::DeleteObjectsOutput::builder()
        .set_deleted(Some(deleted))
        .set_errors((!errors.is_empty()).then_some(errors))
        .build()
}

fn validate_canary_slow_down_for_retry(
    output: &aws_sdk_s3::operation::delete_objects::DeleteObjectsOutput,
    canary_key: &str,
) -> bool {
    let canary_errors = output
        .errors()
        .iter()
        .filter(|error| error.key() == Some(canary_key))
        .collect::<Vec<_>>();
    if !canary_errors
        .iter()
        .any(|error| error.code() == Some("SlowDown"))
    {
        return false;
    }

    assert!(
        output
            .deleted()
            .iter()
            .all(|deleted| deleted.key() != Some(canary_key)),
        "canary cannot be both deleted and SlowDown before retry: {output:?}"
    );
    assert_eq!(
        canary_errors.len(),
        1,
        "canary must have exactly one error before retry: {output:?}"
    );
    assert_eq!(canary_errors[0].code(), Some("SlowDown"));
    true
}

fn test_delete_objects_output(
    deleted: Vec<DeletedObject>,
    errors: Vec<DeleteObjectError>,
) -> aws_sdk_s3::operation::delete_objects::DeleteObjectsOutput {
    aws_sdk_s3::operation::delete_objects::DeleteObjectsOutput::builder()
        .set_deleted((!deleted.is_empty()).then_some(deleted))
        .set_errors((!errors.is_empty()).then_some(errors))
        .build()
}

fn test_delete_objects_error(key: &str, code: &str) -> DeleteObjectError {
    DeleteObjectError::builder().key(key).code(code).build()
}

#[test]
fn canary_slow_down_retry_accepts_one_undeleted_error() {
    let output = test_delete_objects_output(
        Vec::new(),
        vec![test_delete_objects_error("canary", "SlowDown")],
    );
    assert!(validate_canary_slow_down_for_retry(&output, "canary"));
}

#[test]
#[should_panic(expected = "canary cannot be both deleted and SlowDown before retry")]
fn canary_slow_down_retry_rejects_deleted_canary() {
    let output = test_delete_objects_output(
        vec![DeletedObject::builder().key("canary").build()],
        vec![test_delete_objects_error("canary", "SlowDown")],
    );
    validate_canary_slow_down_for_retry(&output, "canary");
}

#[test]
#[should_panic(expected = "canary must have exactly one error before retry")]
fn canary_slow_down_retry_rejects_duplicate_errors() {
    let output = test_delete_objects_output(
        Vec::new(),
        vec![
            test_delete_objects_error("canary", "SlowDown"),
            test_delete_objects_error("canary", "SlowDown"),
        ],
    );
    validate_canary_slow_down_for_retry(&output, "canary");
}

fn classify_conditional_delete_objects_entry(
    output: &aws_sdk_s3::operation::delete_objects::DeleteObjectsOutput,
    key: &str,
    canary_key: &str,
) -> ConditionalDeleteObjectsEntryOutcome {
    let canary = output
        .deleted()
        .iter()
        .find(|deleted| deleted.key() == Some(canary_key))
        .unwrap_or_else(|| panic!("missing successful canary deletion: {output:?}"));
    let _ = canary;
    assert!(
        output
            .errors()
            .iter()
            .all(|error| error.key() != Some(canary_key)),
        "canary unexpectedly failed: {output:?}"
    );

    let deleted = output
        .deleted()
        .iter()
        .find(|deleted| deleted.key() == Some(key));
    let error = output
        .errors()
        .iter()
        .find(|error| error.key() == Some(key));
    assert_ne!(
        deleted.is_some(),
        error.is_some(),
        "conditional entry must appear exactly once: {output:?}"
    );
    if let Some(deleted) = deleted {
        ConditionalDeleteObjectsEntryOutcome::Deleted {
            delete_marker_version_id: deleted.delete_marker_version_id().map(str::to_string),
        }
    } else {
        let error = error.unwrap();
        let code = error.code().unwrap_or_default();
        assert!(
            matches!(
                code,
                "ConditionalRequestConflict" | "PreconditionFailed" | "NoSuchKey"
            ),
            "unexpected conditional batch-delete error: {error:?}"
        );
        ConditionalDeleteObjectsEntryOutcome::Rejected {
            code: code.to_string(),
        }
    }
}

// ── Multi-object delete ─────────────────────────────────────────────

#[test]
fn test_multi_object_delete() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects(client, "", 3).await;

        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let delete = make_delete_request(&key_refs, false);
        let resp =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, delete).await;

        assert_eq!(resp.deleted().len(), 3);
        assert!(resp.errors().is_empty());

        // Verify objects are gone via V1 list
        let list = client
            .list_objects()
            .bucket(&bucket)
            .send_retrying_operation_aborted("list objects after multi-object delete")
            .await
            .unwrap();
        assert!(list.contents().is_empty());

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_bucket_policy_delete_object_existing_tag_condition_is_rejected() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let public_key = "public-delete";
        let private_key = "private-delete";
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        for key in [public_key, private_key] {
            put_object(client, &bucket, key, b"data").await;
        }

        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(public_key)
            .tagging(object_tagging("security", "public"))
            .send_retrying_operation_aborted("put object tagging during delete tests")
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(private_key)
            .tagging(object_tagging("security", "private"))
            .send_retrying_operation_aborted("put object tagging during delete tests")
            .await
            .unwrap();

        let result = client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:DeleteObject",
                        "Resource": [object_resource(&bucket, public_key), object_resource(&bucket, private_key)],
                        "Condition": {
                            "StringEquals": {
                                "s3:ExistingObjectTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send_retrying_operation_aborted("put bucket policy during delete tests")
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "MalformedPolicy");

        client
            .delete_object()
            .bucket(&bucket)
            .key(private_key)
            .send_retrying_operation_aborted("delete object during delete tests")
            .await
            .unwrap();
        client
            .delete_object()
            .bucket(&bucket)
            .key(public_key)
            .send_retrying_operation_aborted("delete object during delete tests")
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_bucket_policy_delete_object_version_existing_tag_condition_is_rejected() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let public_key = "public-version-delete";
        let private_key = "private-version-delete";
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_bucket_versioning()
            .bucket(&bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send_retrying_operation_aborted("put bucket versioning during delete tests")
            .await
            .unwrap();

        let public_version = put_object(client, &bucket, public_key, b"data")
            .await
            .version_id()
            .expect("expected version id")
            .to_string();
        let private_version = put_object(client, &bucket, private_key, b"data")
            .await
            .version_id()
            .expect("expected version id")
            .to_string();

        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(public_key)
            .version_id(&public_version)
            .tagging(object_tagging("security", "public"))
            .send_retrying_operation_aborted("put object tagging during delete tests")
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(private_key)
            .version_id(&private_version)
            .tagging(object_tagging("security", "private"))
            .send_retrying_operation_aborted("put object tagging during delete tests")
            .await
            .unwrap();

        let result = client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:DeleteObjectVersion",
                        "Resource": [object_resource(&bucket, public_key), object_resource(&bucket, private_key)],
                        "Condition": {
                            "StringEquals": {
                                "s3:ExistingObjectTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send_retrying_operation_aborted("put bucket policy during delete tests")
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "MalformedPolicy");

        cleanup_versioned_bucket(&bucket).await;
    });
}

#[test]
fn test_bucket_policy_delete_and_delete_tagging_existing_tag_condition_is_rejected() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let key = "mixed-delete-action";
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        put_object(client, &bucket, key, b"data").await;
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .tagging(object_tagging("security", "public"))
            .send_retrying_operation_aborted("put object tagging during delete tests")
            .await
            .unwrap();

        let result = client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": ["s3:DeleteObject", "s3:DeleteObjectTagging"],
                        "Resource": object_resource(&bucket, key),
                        "Condition": {
                            "StringEquals": {
                                "s3:ExistingObjectTag/security": "public"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send_retrying_operation_aborted("put bucket policy during delete tests")
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "MalformedPolicy");

        delete_all_and_bucket(client, &bucket, &[key.to_string()]).await;
    });
}

#[test]
fn test_multi_objectv2_delete() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects(client, "", 3).await;

        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let delete = make_delete_request(&key_refs, false);
        s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, delete).await;

        // Verify objects are gone via V2 list
        let list = client
            .list_objects_v2()
            .bucket(&bucket)
            .send_retrying_operation_aborted("list objects during delete tests")
            .await
            .unwrap();
        assert_eq!(list.key_count(), Some(0));
        assert!(list.contents().is_empty());

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_multi_object_delete_quiet() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects(client, "", 3).await;

        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let delete = make_delete_request(&key_refs, true);
        let resp =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, delete).await;

        // Quiet mode: successfully deleted items not listed in response
        assert!(resp.deleted().is_empty());
        assert!(resp.errors().is_empty());

        // But objects should actually be deleted
        let list = client
            .list_objects_v2()
            .bucket(&bucket)
            .send_retrying_operation_aborted("list objects during delete tests")
            .await
            .unwrap();
        assert!(list.contents().is_empty());

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_multi_object_delete_large() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects(client, "", 35).await;

        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let delete = make_delete_request(&key_refs, false);
        let resp =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, delete).await;

        assert_eq!(resp.deleted().len(), 35);
        assert!(resp.errors().is_empty());

        let list = client
            .list_objects_v2()
            .bucket(&bucket)
            .send_retrying_operation_aborted("list objects during delete tests")
            .await
            .unwrap();
        assert!(list.contents().is_empty());

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_multi_object_delete_nonexistent_keys() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        // Delete keys that were never created — should succeed (idempotent)
        let delete = make_delete_request(&["nokey1", "nokey2", "nokey3"], false);
        let resp =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, delete).await;

        assert_eq!(resp.deleted().len(), 3);
        assert!(resp.errors().is_empty());

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_multi_object_delete_mixed() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, &["existing1", "existing2"]).await;

        // Delete mix of existing and nonexistent keys
        let delete = make_delete_request(&["existing1", "nonexistent", "existing2"], false);
        let resp =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, delete).await;

        // All should be reported as deleted (including nonexistent)
        assert_eq!(resp.deleted().len(), 3);
        assert!(resp.errors().is_empty());

        // Verify existing objects are gone
        let list = client
            .list_objects_v2()
            .bucket(&bucket)
            .send_retrying_operation_aborted("list objects during delete tests")
            .await
            .unwrap();
        assert!(list.contents().is_empty());

        // keys vec only has the originally created ones; bucket is already empty
        let _ = keys;
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_multi_object_delete_special_keys() {
    s3_tests::run(async {
        let client = CTX.client();
        let special = &["a/b/c", "hello world", "foo&bar", "key with spaces"];
        let (bucket, keys) = create_objects_with_keys(client, special).await;

        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let delete = make_delete_request(&key_refs, false);
        let resp =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, delete).await;

        assert_eq!(resp.deleted().len(), 4);
        assert!(resp.errors().is_empty());

        let list = client
            .list_objects_v2()
            .bucket(&bucket)
            .send_retrying_operation_aborted("list objects during delete tests")
            .await
            .unwrap();
        assert!(list.contents().is_empty());

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_multi_object_delete_single() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, _keys) = create_objects_with_keys(client, &["only", "survivor"]).await;

        // Delete just one key via multi-delete API
        let delete = make_delete_request(&["only"], false);
        let resp =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, delete).await;

        assert_eq!(resp.deleted().len(), 1);
        assert!(resp.errors().is_empty());

        // "survivor" should still exist
        let list = client
            .list_objects_v2()
            .bucket(&bucket)
            .send_retrying_operation_aborted("list objects during delete tests")
            .await
            .unwrap();
        assert_eq!(get_keys(list.contents()), vec!["survivor"]);

        delete_all_and_bucket(client, &bucket, &["survivor".to_string()]).await;
    });
}

#[test]
fn test_multi_object_delete_verify_response() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, _keys) = create_objects_with_keys(client, &["alpha", "beta", "gamma"]).await;

        let delete = make_delete_request(&["alpha", "beta", "gamma"], false);
        let resp =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, delete).await;

        // Verify response lists the correct deleted keys
        let mut deleted_keys: Vec<String> = resp
            .deleted()
            .iter()
            .filter_map(|d| d.key().map(str::to_string))
            .collect();
        deleted_keys.sort();
        assert_eq!(deleted_keys, vec!["alpha", "beta", "gamma"]);
        assert!(resp.errors().is_empty());

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_multi_object_delete_per_object_if_match() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let ok = put_object(client, &bucket, "ok", b"ok").await;
        put_object(client, &bucket, "stale", b"stale").await;

        let delete = Delete::builder()
            .set_objects(Some(vec![
                make_object_id_with_etag("ok", ok.e_tag().unwrap()),
                make_object_id_with_etag("stale", "\"0000000000000000\""),
            ]))
            .quiet(false)
            .build()
            .unwrap();
        let resp =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, delete).await;

        assert_eq!(resp.deleted().len(), 1);
        assert_eq!(resp.deleted()[0].key(), Some("ok"));
        assert_eq!(resp.errors().len(), 1);
        assert_eq!(resp.errors()[0].key(), Some("stale"));
        assert_eq!(resp.errors()[0].code(), Some("PreconditionFailed"));

        client
            .head_object()
            .bucket(&bucket)
            .key("stale")
            .send_retrying_operation_aborted("head object after conditional multi-delete")
            .await
            .unwrap();

        delete_all_and_bucket(client, &bucket, &["stale".to_string()]).await;
    });
}

#[test]
fn test_multi_object_delete_ifmatch_races_current_replacement_across_versioning_states() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let different_key = "batch-delete-race-unversioned-different-etag";
        let different_canary = "batch-delete-race-unversioned-different-canary";
        let different_original = put_object(client, &bucket, different_key, b"original").await;
        let different_canary_put = put_object(client, &bucket, different_canary, b"canary").await;
        let (different_delete, different_put) = race_conditional_delete_objects_with_put(
            &bucket,
            different_key,
            different_original.e_tag().unwrap(),
            different_canary,
            different_canary_put.e_tag().unwrap(),
            b"different replacement",
            "different-etag",
        )
        .await;
        let different_delete = different_delete.unwrap_or_else(|error| {
            panic!("unversioned conditional batch delete failed: {error:?}")
        });
        different_put.unwrap_or_else(|error| {
            panic!("unversioned batch-delete replacement failed: {error:?}")
        });
        match classify_conditional_delete_objects_entry(
            &different_delete,
            different_key,
            different_canary,
        ) {
            ConditionalDeleteObjectsEntryOutcome::Deleted {
                delete_marker_version_id,
            } => assert_eq!(delete_marker_version_id, None),
            ConditionalDeleteObjectsEntryOutcome::Rejected { code } => {
                assert!(matches!(
                    code.as_str(),
                    "ConditionalRequestConflict" | "PreconditionFailed"
                ));
            }
        }
        let different_current = client
            .get_object()
            .bucket(&bucket)
            .key(different_key)
            .send_retrying_operation_aborted("get unversioned conditional batch-delete replacement")
            .await
            .unwrap();
        assert_eq!(
            different_current
                .metadata()
                .and_then(|metadata| metadata.get("replacement-state"))
                .map(String::as_str),
            Some("different-etag")
        );
        assert_eq!(
            different_current
                .body
                .collect()
                .await
                .unwrap()
                .into_bytes()
                .as_ref(),
            b"different replacement"
        );

        let same_key = "batch-delete-race-unversioned-same-etag";
        let same_canary = "batch-delete-race-unversioned-same-canary";
        let same_original = put_object(client, &bucket, same_key, b"same bytes").await;
        let same_canary_put = put_object(client, &bucket, same_canary, b"canary").await;
        let (same_delete, same_put) = race_conditional_delete_objects_with_put(
            &bucket,
            same_key,
            same_original.e_tag().unwrap(),
            same_canary,
            same_canary_put.e_tag().unwrap(),
            b"same bytes",
            "same-etag",
        )
        .await;
        let same_delete = same_delete
            .unwrap_or_else(|error| panic!("same-ETag conditional batch delete failed: {error:?}"));
        let same_put = same_put
            .unwrap_or_else(|error| panic!("same-ETag batch-delete replacement failed: {error:?}"));
        assert_eq!(same_put.e_tag(), same_original.e_tag());
        let same_outcome =
            classify_conditional_delete_objects_entry(&same_delete, same_key, same_canary);
        let same_delete_succeeded = match same_outcome {
            ConditionalDeleteObjectsEntryOutcome::Deleted {
                delete_marker_version_id,
            } => {
                assert_eq!(delete_marker_version_id, None);
                true
            }
            ConditionalDeleteObjectsEntryOutcome::Rejected { code } => {
                assert_eq!(code, "ConditionalRequestConflict");
                false
            }
        };
        match client
            .get_object()
            .bucket(&bucket)
            .key(same_key)
            .send_retrying_operation_aborted("get same-ETag conditional batch-delete result")
            .await
        {
            Ok(current) => {
                assert_eq!(current.e_tag(), same_original.e_tag());
                assert_eq!(
                    current
                        .metadata()
                        .and_then(|metadata| metadata.get("replacement-state"))
                        .map(String::as_str),
                    Some("same-etag")
                );
            }
            Err(error) => {
                assert!(
                    same_delete_succeeded,
                    "a rejected batch delete cannot remove the successful replacement"
                );
                assert_eq!(
                    error
                        .raw_response()
                        .map(|response| response.status().as_u16()),
                    Some(404),
                    "{error:?}"
                );
            }
        }

        s3_tests::enable_bucket_versioning(client, &bucket).await;

        let versioned_key = "batch-delete-race-versioned-different-etag";
        let versioned_canary = "batch-delete-race-versioned-canary";
        let versioned_original =
            put_object(client, &bucket, versioned_key, b"versioned original").await;
        let original_version = versioned_original.version_id().unwrap().to_string();
        let versioned_canary_put = put_object(client, &bucket, versioned_canary, b"canary").await;
        let (versioned_delete, versioned_put) = race_conditional_delete_objects_with_put(
            &bucket,
            versioned_key,
            versioned_original.e_tag().unwrap(),
            versioned_canary,
            versioned_canary_put.e_tag().unwrap(),
            b"versioned replacement",
            "versioned-different-etag",
        )
        .await;
        let versioned_delete = versioned_delete
            .unwrap_or_else(|error| panic!("versioned conditional batch delete failed: {error:?}"));
        let versioned_put = versioned_put
            .unwrap_or_else(|error| panic!("versioned batch-delete replacement failed: {error:?}"));
        let replacement_version = versioned_put.version_id().unwrap().to_string();
        let versioned_outcome = classify_conditional_delete_objects_entry(
            &versioned_delete,
            versioned_key,
            versioned_canary,
        );
        let expected_marker_version = match &versioned_outcome {
            ConditionalDeleteObjectsEntryOutcome::Deleted {
                delete_marker_version_id,
            } => Some(
                delete_marker_version_id
                    .as_deref()
                    .expect("versioned conditional delete must return a marker version"),
            ),
            ConditionalDeleteObjectsEntryOutcome::Rejected { code } => {
                assert!(matches!(
                    code.as_str(),
                    "ConditionalRequestConflict" | "PreconditionFailed"
                ));
                None
            }
        };
        let versioned_versions = client
            .list_object_versions()
            .bucket(&bucket)
            .prefix(versioned_key)
            .send_retrying_operation_aborted("list versioned conditional batch-delete race")
            .await
            .unwrap();
        assert_eq!(versioned_versions.versions().len(), 2);
        assert!(versioned_versions.versions().iter().any(|version| {
            version.version_id() == Some(original_version.as_str())
                && !version.is_latest().unwrap_or(false)
        }));
        assert!(versioned_versions.versions().iter().any(|version| {
            version.version_id() == Some(replacement_version.as_str())
                && version.is_latest().unwrap_or(false)
        }));
        assert_eq!(
            versioned_versions.delete_markers().len(),
            usize::from(expected_marker_version.is_some())
        );
        if let Some(expected_marker_version) = expected_marker_version {
            assert_eq!(
                versioned_versions.delete_markers()[0].version_id(),
                Some(expected_marker_version)
            );
            assert!(!versioned_versions.delete_markers()[0]
                .is_latest()
                .unwrap_or(false));
        }

        let marker_key = "batch-delete-race-versioned-current-marker";
        let marker_canary = "batch-delete-race-versioned-marker-canary";
        let marker_original = put_object(client, &bucket, marker_key, b"marker original").await;
        let marker_original_version = marker_original.version_id().unwrap().to_string();
        let marker_canary_put = put_object(client, &bucket, marker_canary, b"canary").await;
        let marker_delete = Delete::builder()
            .set_objects(Some(vec![
                make_object_id_with_etag(marker_key, marker_original.e_tag().unwrap()),
                make_object_id_with_etag(marker_canary, marker_canary_put.e_tag().unwrap()),
            ]))
            .quiet(false)
            .build()
            .unwrap();
        let marker_barrier = Arc::new(tokio::sync::Barrier::new(2));
        let batch_client = client.clone();
        let batch_bucket = bucket.clone();
        let batch_barrier = Arc::clone(&marker_barrier);
        let batch = tokio::spawn(async move {
            batch_barrier.wait().await;
            delete_objects_retrying_exact_operation_aborted_result(
                &batch_client,
                &batch_bucket,
                marker_delete,
            )
            .await
        });
        let competing_client = client.clone();
        let competing_bucket = bucket.clone();
        let competing = tokio::spawn(async move {
            marker_barrier.wait().await;
            competing_client
                .delete_object()
                .bucket(competing_bucket)
                .key(marker_key)
                .send_retrying_exact_operation_aborted(
                    "insert competing marker during conditional batch-delete race",
                )
                .await
        });
        let (batch, competing) = tokio::join!(batch, competing);
        let batch = batch
            .unwrap()
            .unwrap_or_else(|error| panic!("conditional marker batch delete failed: {error:?}"));
        let competing = competing
            .unwrap()
            .unwrap_or_else(|error| panic!("competing marker insertion failed: {error:?}"));
        assert_eq!(competing.delete_marker(), Some(true));
        let competing_marker_version = competing.version_id().unwrap().to_string();
        let marker_outcome =
            classify_conditional_delete_objects_entry(&batch, marker_key, marker_canary);
        let batch_marker_version = match &marker_outcome {
            ConditionalDeleteObjectsEntryOutcome::Deleted {
                delete_marker_version_id,
            } => Some(
                delete_marker_version_id
                    .as_deref()
                    .expect("versioned conditional batch delete must return marker version"),
            ),
            ConditionalDeleteObjectsEntryOutcome::Rejected { code } => {
                assert!(matches!(
                    code.as_str(),
                    "ConditionalRequestConflict" | "NoSuchKey"
                ));
                None
            }
        };
        let marker_versions = client
            .list_object_versions()
            .bucket(&bucket)
            .prefix(marker_key)
            .send_retrying_operation_aborted("list competing batch-delete markers")
            .await
            .unwrap();
        assert_eq!(marker_versions.versions().len(), 1);
        assert_eq!(
            marker_versions.versions()[0].version_id(),
            Some(marker_original_version.as_str())
        );
        let mut expected_marker_versions =
            std::collections::HashSet::from([competing_marker_version.as_str()]);
        if let Some(batch_marker_version) = batch_marker_version {
            expected_marker_versions.insert(batch_marker_version);
        }
        let actual_marker_versions: std::collections::HashSet<&str> = marker_versions
            .delete_markers()
            .iter()
            .filter_map(|marker| marker.version_id())
            .collect();
        assert_eq!(actual_marker_versions, expected_marker_versions);
        assert_eq!(
            marker_versions
                .delete_markers()
                .iter()
                .filter(|marker| marker.is_latest().unwrap_or(false))
                .count(),
            1
        );

        let suspended_key = "batch-delete-race-suspended-different-etag";
        let suspended_canary = "batch-delete-race-suspended-canary";
        let suspended_same_key = "batch-delete-race-suspended-same-etag";
        let suspended_same_canary = "batch-delete-race-suspended-same-canary";
        let suspended_numbered =
            put_object(client, &bucket, suspended_key, b"numbered history").await;
        let suspended_numbered_version = suspended_numbered.version_id().unwrap().to_string();
        let suspended_same_numbered = put_object(
            client,
            &bucket,
            suspended_same_key,
            b"numbered same history",
        )
        .await;
        let suspended_same_numbered_version =
            suspended_same_numbered.version_id().unwrap().to_string();
        client
            .put_bucket_versioning()
            .bucket(&bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Suspended)
                    .build(),
            )
            .send_retrying_operation_aborted("suspend versioning for conditional batch-delete race")
            .await
            .unwrap();
        let suspended_original =
            put_object(client, &bucket, suspended_key, b"suspended original").await;
        assert_eq!(suspended_original.version_id(), None);
        let suspended_canary_put = put_object(client, &bucket, suspended_canary, b"canary").await;
        let (suspended_delete, suspended_put) = race_conditional_delete_objects_with_put(
            &bucket,
            suspended_key,
            suspended_original.e_tag().unwrap(),
            suspended_canary,
            suspended_canary_put.e_tag().unwrap(),
            b"suspended replacement",
            "suspended-different-etag",
        )
        .await;
        let suspended_delete = suspended_delete
            .unwrap_or_else(|error| panic!("suspended conditional batch delete failed: {error:?}"));
        suspended_put
            .unwrap_or_else(|error| panic!("suspended batch-delete replacement failed: {error:?}"));
        match classify_conditional_delete_objects_entry(
            &suspended_delete,
            suspended_key,
            suspended_canary,
        ) {
            ConditionalDeleteObjectsEntryOutcome::Deleted {
                delete_marker_version_id,
            } => assert_eq!(delete_marker_version_id.as_deref(), Some("null")),
            ConditionalDeleteObjectsEntryOutcome::Rejected { code } => {
                assert!(matches!(
                    code.as_str(),
                    "ConditionalRequestConflict" | "PreconditionFailed"
                ));
            }
        }
        let suspended_versions = client
            .list_object_versions()
            .bucket(&bucket)
            .prefix(suspended_key)
            .send_retrying_operation_aborted("list suspended conditional batch-delete race")
            .await
            .unwrap();
        assert_eq!(suspended_versions.versions().len(), 2);
        assert!(suspended_versions.delete_markers().is_empty());
        assert!(suspended_versions.versions().iter().any(|version| {
            version.version_id() == Some(suspended_numbered_version.as_str())
                && !version.is_latest().unwrap_or(false)
        }));
        assert!(suspended_versions.versions().iter().any(|version| {
            version.version_id() == Some("null") && version.is_latest().unwrap_or(false)
        }));
        let suspended_current = client
            .get_object()
            .bucket(&bucket)
            .key(suspended_key)
            .send_retrying_operation_aborted("get suspended conditional batch-delete replacement")
            .await
            .unwrap();
        assert_eq!(
            suspended_current
                .metadata()
                .and_then(|metadata| metadata.get("replacement-state"))
                .map(String::as_str),
            Some("suspended-different-etag")
        );

        let suspended_same_original =
            put_object(client, &bucket, suspended_same_key, b"suspended same bytes").await;
        assert_eq!(suspended_same_original.version_id(), None);
        let suspended_same_canary_put =
            put_object(client, &bucket, suspended_same_canary, b"canary").await;
        let (suspended_same_delete, suspended_same_put) = race_conditional_delete_objects_with_put(
            &bucket,
            suspended_same_key,
            suspended_same_original.e_tag().unwrap(),
            suspended_same_canary,
            suspended_same_canary_put.e_tag().unwrap(),
            b"suspended same bytes",
            "suspended-same-etag",
        )
        .await;
        let suspended_same_delete = suspended_same_delete.unwrap_or_else(|error| {
            panic!("suspended same-ETag conditional batch delete failed: {error:?}")
        });
        let suspended_same_put = suspended_same_put.unwrap_or_else(|error| {
            panic!("suspended same-ETag batch-delete replacement failed: {error:?}")
        });
        assert_eq!(suspended_same_put.e_tag(), suspended_same_original.e_tag());
        let suspended_same_outcome = classify_conditional_delete_objects_entry(
            &suspended_same_delete,
            suspended_same_key,
            suspended_same_canary,
        );
        match &suspended_same_outcome {
            ConditionalDeleteObjectsEntryOutcome::Deleted {
                delete_marker_version_id,
            } => assert_eq!(delete_marker_version_id.as_deref(), Some("null")),
            ConditionalDeleteObjectsEntryOutcome::Rejected { code } => {
                assert_eq!(code, "ConditionalRequestConflict");
            }
        }
        let suspended_same_versions = client
            .list_object_versions()
            .bucket(&bucket)
            .prefix(suspended_same_key)
            .send_retrying_operation_aborted(
                "list suspended same-ETag conditional batch-delete race",
            )
            .await
            .unwrap();
        assert!(suspended_same_versions.versions().len() <= 2);
        assert!(suspended_same_versions.delete_markers().len() <= 1);
        assert_eq!(
            suspended_same_versions.versions().len()
                + suspended_same_versions.delete_markers().len(),
            2
        );
        assert!(suspended_same_versions.versions().iter().any(|version| {
            version.version_id() == Some(suspended_same_numbered_version.as_str())
                && !version.is_latest().unwrap_or(false)
        }));
        if let Some(null_version) = suspended_same_versions
            .versions()
            .iter()
            .find(|version| version.version_id() == Some("null"))
        {
            assert!(null_version.is_latest().unwrap_or(false));
            assert!(suspended_same_versions.delete_markers().is_empty());
            let current = client
                .get_object()
                .bucket(&bucket)
                .key(suspended_same_key)
                .send_retrying_operation_aborted(
                    "get suspended same-ETag conditional batch-delete replacement",
                )
                .await
                .unwrap();
            assert_eq!(
                current
                    .metadata()
                    .and_then(|metadata| metadata.get("replacement-state"))
                    .map(String::as_str),
                Some("suspended-same-etag")
            );
            assert_eq!(
                current.body.collect().await.unwrap().into_bytes().as_ref(),
                b"suspended same bytes"
            );
        } else {
            assert!(matches!(
                suspended_same_outcome,
                ConditionalDeleteObjectsEntryOutcome::Deleted { .. }
            ));
            assert_eq!(suspended_same_versions.delete_markers().len(), 1);
            assert_eq!(
                suspended_same_versions.delete_markers()[0].version_id(),
                Some("null")
            );
            assert!(suspended_same_versions.delete_markers()[0]
                .is_latest()
                .unwrap_or(false));
            let current = client
                .get_object()
                .bucket(&bucket)
                .key(suspended_same_key)
                .send_retrying_operation_aborted(
                    "get suspended same-ETag conditional batch-delete marker",
                )
                .await;
            assert_eq!(err_status(&current), 404);
        }

        cleanup_versioned_bucket(&bucket).await;
    });
}

#[test]
fn test_multi_object_delete_etag_wildcard_matches_any_existing_object() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        put_object(client, &bucket, "obj", b"body").await;

        let delete = Delete::builder()
            .set_objects(Some(vec![make_object_id_with_etag("obj", "*")]))
            .quiet(false)
            .build()
            .unwrap();
        let resp =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, delete).await;

        assert_eq!(resp.deleted().len(), 1);
        assert_eq!(resp.deleted()[0].key(), Some("obj"));
        assert!(resp.errors().is_empty());

        let head = client
            .head_object()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("head object after wildcard-etag multi-delete")
            .await;
        assert_eq!(err_status(&head), 404);

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_multi_object_delete_etag_comma_list_is_not_if_match_list() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        let put = put_object(client, &bucket, "obj", b"body").await;
        let etag_list = format!("\"0000000000000000\", {}", put.e_tag().unwrap());

        let delete = Delete::builder()
            .set_objects(Some(vec![make_object_id_with_etag("obj", &etag_list)]))
            .quiet(false)
            .build()
            .unwrap();
        let resp =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, delete).await;

        assert!(resp.deleted().is_empty());
        assert_eq!(resp.errors().len(), 1);
        assert_eq!(resp.errors()[0].key(), Some("obj"));
        assert_eq!(resp.errors()[0].code(), Some("PreconditionFailed"));

        client
            .head_object()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("head object after comma-etag multi-delete")
            .await
            .unwrap();

        delete_all_and_bucket(client, &bucket, &["obj".to_string()]).await;
    });
}

#[test]
fn test_multi_object_delete_key_limit() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        // Build a request with >1000 keys (server limit)
        let key_strs: Vec<String> = (0..1001).map(|i| format!("key{}", i)).collect();
        let key_refs: Vec<&str> = key_strs.iter().map(|s| s.as_str()).collect();
        let delete = make_delete_request(&key_refs, false);

        let result = delete_objects_with_md5(client, &bucket, delete)
            .send()
            .await;
        assert_eq!(err_status(&result), 400);

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── Single delete edge cases ────────────────────────────────────────

#[test]
fn test_object_delete_nonexistent_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();

        // Single delete from nonexistent bucket should error
        let result = client
            .delete_object()
            .bucket(&bucket)
            .key("somekey")
            .send_retrying_operation_aborted("delete object during delete tests")
            .await;
        assert!(result.is_err());
    });
}

#[test]
fn test_multi_object_delete_nonexistent_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();

        let delete = make_delete_request(&["key1", "key2"], false);
        let result = delete_objects_with_md5(client, &bucket, delete)
            .send()
            .await;
        assert!(result.is_err());
    });
}

#[test]
fn test_multi_objectv2_delete_key_limit() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        // Build a request with >1000 keys (server limit), verify via V2 list
        let key_strs: Vec<String> = (0..1001).map(|i| format!("key{}", i)).collect();
        let key_refs: Vec<&str> = key_strs.iter().map(|s| s.as_str()).collect();
        let delete = make_delete_request(&key_refs, false);

        let result = delete_objects_with_md5(client, &bucket, delete)
            .send()
            .await;
        assert_eq!(err_status(&result), 400);

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_object_delete_key_bucket_gone() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;

        // Try to delete an object from the now-deleted bucket
        let result = client
            .delete_object()
            .bucket(&bucket)
            .key("somekey")
            .send_retrying_operation_aborted("delete object during delete tests")
            .await;
        assert_eq!(err_status(&result), 404);
    });
}

// ── Versioning helpers ──────────────────────────────────────────────

async fn setup_versioned_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    client
        .put_bucket_versioning()
        .bucket(&bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send_retrying_operation_aborted("put bucket versioning during delete tests")
        .await
        .unwrap();
    bucket
}

async fn create_multiple_versions(
    bucket: &str,
    key: &str,
    num: usize,
) -> (Vec<String>, Vec<String>) {
    let client = CTX.client();
    let mut version_ids = Vec::new();
    let mut contents = Vec::new();
    for i in 0..num {
        let body = format!("content-{}", i);
        let resp = put_object(client, bucket, key, body.clone().into_bytes()).await;
        version_ids.push(resp.version_id().unwrap().to_string());
        contents.push(body);
    }
    (version_ids, contents)
}

/// Clean up a versioned bucket by deleting all versions and delete markers.
async fn cleanup_versioned_bucket(bucket: &str) {
    let client = CTX.client();
    // List all versions and delete markers, delete them all
    let resp = client
        .list_object_versions()
        .bucket(bucket)
        .send_retrying_operation_aborted("list object versions during delete tests")
        .await
        .unwrap();
    for v in resp.versions() {
        client
            .delete_object()
            .bucket(bucket)
            .key(v.key().unwrap())
            .version_id(v.version_id().unwrap())
            .send_retrying_operation_aborted("delete object during delete tests")
            .await
            .unwrap();
    }
    for dm in resp.delete_markers() {
        client
            .delete_object()
            .bucket(bucket)
            .key(dm.key().unwrap())
            .version_id(dm.version_id().unwrap())
            .send_retrying_operation_aborted("delete object during delete tests")
            .await
            .unwrap();
    }
    s3_tests::delete_bucket_retrying_operation_aborted(client, bucket).await;
}

// ── Versioning + multi-object delete ────────────────────────────────

/// Batch-delete specific version IDs and verify they are removed.
#[test]
fn test_versioning_multi_object_delete() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "testobj";
        let num_versions = 5;

        let (version_ids, _contents) = create_multiple_versions(&bucket, key, num_versions).await;

        // Batch delete all versions by specifying their version IDs
        let objects: Vec<ObjectIdentifier> = version_ids
            .iter()
            .map(|vid| {
                ObjectIdentifier::builder()
                    .key(key)
                    .version_id(vid)
                    .build()
                    .unwrap()
            })
            .collect();
        let delete = Delete::builder()
            .set_objects(Some(objects))
            .quiet(false)
            .build()
            .unwrap();
        let resp =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, delete).await;

        assert_eq!(resp.deleted().len(), num_versions);
        assert!(resp.errors().is_empty());

        // Verify: no versions remain
        let list = client
            .list_object_versions()
            .bucket(&bucket)
            .send_retrying_operation_aborted("list object versions during delete tests")
            .await
            .unwrap();
        assert!(
            list.versions().is_empty(),
            "expected no versions after batch delete"
        );
        assert!(
            list.delete_markers().is_empty(),
            "expected no delete markers after batch delete"
        );

        // Idempotent: deleting the same version IDs again should succeed
        let objects2: Vec<ObjectIdentifier> = version_ids
            .iter()
            .map(|vid| {
                ObjectIdentifier::builder()
                    .key(key)
                    .version_id(vid)
                    .build()
                    .unwrap()
            })
            .collect();
        let delete2 = Delete::builder()
            .set_objects(Some(objects2))
            .quiet(false)
            .build()
            .unwrap();
        let resp2 =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, delete2).await;
        assert_eq!(resp2.deleted().len(), num_versions);
        assert!(resp2.errors().is_empty());

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

/// Batch-delete versions plus a delete marker.
#[test]
fn test_versioning_multi_object_delete_with_marker() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "testobj";

        // Create 3 versions
        let (version_ids, _contents) = create_multiple_versions(&bucket, key, 3).await;

        // Create a delete marker by deleting without specifying versionId
        let del_resp = client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("delete object during delete tests")
            .await
            .unwrap();
        assert!(del_resp.delete_marker().unwrap_or(false));
        let marker_vid = del_resp.version_id().unwrap().to_string();

        // Now batch-delete all versions + the delete marker
        let mut all_vids = version_ids.clone();
        all_vids.push(marker_vid);

        let objects: Vec<ObjectIdentifier> = all_vids
            .iter()
            .map(|vid| {
                ObjectIdentifier::builder()
                    .key(key)
                    .version_id(vid)
                    .build()
                    .unwrap()
            })
            .collect();
        let delete = Delete::builder()
            .set_objects(Some(objects))
            .quiet(false)
            .build()
            .unwrap();
        let resp =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, delete).await;

        assert_eq!(resp.deleted().len(), 4);
        assert!(resp.errors().is_empty());

        // The entry for the delete marker should have delete_marker=true
        let marker_entry = resp
            .deleted()
            .iter()
            .find(|d| d.delete_marker().unwrap_or(false));
        assert!(
            marker_entry.is_some(),
            "expected a delete marker entry in response"
        );

        // Verify: bucket should be completely clean
        let list = client
            .list_object_versions()
            .bucket(&bucket)
            .send_retrying_operation_aborted("list object versions during delete tests")
            .await
            .unwrap();
        assert!(list.versions().is_empty());
        assert!(list.delete_markers().is_empty());

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

/// Use delete_objects (without versionId) on a versioned bucket to create a delete marker.
#[test]
fn test_versioning_multi_object_delete_marker_create() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "testobj";

        // Put one version
        put_object(client, &bucket, key, b"data").await;

        // Batch-delete WITHOUT specifying versionId → should create a delete marker
        let objects = vec![ObjectIdentifier::builder().key(key).build().unwrap()];
        let delete = Delete::builder()
            .set_objects(Some(objects))
            .quiet(false)
            .build()
            .unwrap();
        let resp =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, delete).await;

        assert_eq!(resp.deleted().len(), 1);
        assert!(resp.errors().is_empty());
        let d = &resp.deleted()[0];
        assert!(
            d.delete_marker().unwrap_or(false),
            "expected delete_marker=true when deleting without versionId in versioned bucket"
        );
        assert!(
            d.delete_marker_version_id().is_some(),
            "expected delete_marker_version_id in response"
        );

        // The object should now be inaccessible (404) via normal GET
        let get_result = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("get object after versioned multi-object delete")
            .await;
        assert!(get_result.is_err());

        // But list_object_versions should show both the version and the delete marker
        let list = client
            .list_object_versions()
            .bucket(&bucket)
            .send_retrying_operation_aborted("list object versions during delete tests")
            .await
            .unwrap();
        assert_eq!(
            list.versions().len(),
            1,
            "original version should still exist"
        );
        assert_eq!(list.delete_markers().len(), 1, "delete marker should exist");

        cleanup_versioned_bucket(&bucket).await;
    });
}

#[test]
fn test_versioning_multi_object_delete_current_if_match() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let put = put_object(client, &bucket, "obj", b"data").await;

        let bad_delete = Delete::builder()
            .set_objects(Some(vec![make_object_id_with_etag(
                "obj",
                "\"0000000000000000\"",
            )]))
            .quiet(false)
            .build()
            .unwrap();
        let bad_resp =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, bad_delete).await;
        assert!(bad_resp.deleted().is_empty());
        assert_eq!(bad_resp.errors().len(), 1);
        assert_eq!(bad_resp.errors()[0].key(), Some("obj"));
        assert_eq!(bad_resp.errors()[0].code(), Some("PreconditionFailed"));

        let delete = Delete::builder()
            .set_objects(Some(vec![make_object_id_with_etag(
                "obj",
                put.e_tag().unwrap(),
            )]))
            .quiet(false)
            .build()
            .unwrap();
        let resp =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, delete).await;

        assert_eq!(resp.deleted().len(), 1);
        let deleted = &resp.deleted()[0];
        assert_eq!(deleted.key(), Some("obj"));
        assert_eq!(deleted.delete_marker(), Some(true));
        assert!(deleted.delete_marker_version_id().is_some());

        let get_result = client
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("get object after versioned multi-object delete")
            .await;
        assert!(get_result.is_err());

        cleanup_versioned_bucket(&bucket).await;
    });
}

#[test]
fn test_versioning_multi_object_delete_current_marker_if_match_returns_no_such_key() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "current-marker";
        let canary_key = "batch-canary";
        let put = put_object(client, &bucket, key, b"data").await;
        let canary = put_object(client, &bucket, canary_key, b"canary").await;
        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("create current batch-delete marker")
            .await
            .unwrap();

        let delete = Delete::builder()
            .set_objects(Some(vec![
                make_object_id_with_etag(key, put.e_tag().unwrap()),
                make_object_id_with_etag(canary_key, canary.e_tag().unwrap()),
            ]))
            .quiet(false)
            .build()
            .unwrap();
        let response =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, delete).await;
        assert_eq!(response.deleted().len(), 1);
        assert_eq!(response.deleted()[0].key(), Some(canary_key));
        assert_eq!(response.deleted()[0].delete_marker(), Some(true));
        assert_eq!(response.errors().len(), 1);
        assert_eq!(response.errors()[0].key(), Some(key));
        assert_eq!(response.errors()[0].code(), Some("NoSuchKey"));

        let versions = client
            .list_object_versions()
            .bucket(&bucket)
            .prefix(key)
            .send_retrying_operation_aborted("list current marker after conditional batch delete")
            .await
            .unwrap();
        assert_eq!(versions.versions().len(), 1);
        assert_eq!(versions.delete_markers().len(), 1);
        assert!(versions.delete_markers()[0].is_latest().unwrap_or(false));

        cleanup_versioned_bucket(&bucket).await;
    });
}

#[test]
fn test_versioning_multi_object_delete_version_id_with_etag_not_implemented() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "obj";

        let first = put_object(client, &bucket, key, b"v1").await;
        let first_version = first.version_id().unwrap().to_string();
        let first_etag = first.e_tag().unwrap().to_string();

        let second = put_object(client, &bucket, key, b"v2").await;
        let second_version = second.version_id().unwrap().to_string();

        // AWS does not support DeleteObjects entries that combine VersionId
        // and ETag on general-purpose buckets. Both matching and mismatching
        // ETags return per-object NotImplemented and leave the version intact.
        let bad_delete = Delete::builder()
            .set_objects(Some(vec![make_object_id_with_version_and_etag(
                key,
                &first_version,
                "\"0000000000000000\"",
            )]))
            .quiet(false)
            .build()
            .unwrap();
        let bad_resp =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, bad_delete).await;

        assert!(bad_resp.deleted().is_empty());
        assert_eq!(bad_resp.errors().len(), 1);
        assert_eq!(bad_resp.errors()[0].key(), Some(key));
        assert_eq!(
            bad_resp.errors()[0].version_id(),
            Some(first_version.as_str())
        );
        assert_eq!(bad_resp.errors()[0].code(), Some("NotImplemented"));

        let good_delete = Delete::builder()
            .set_objects(Some(vec![make_object_id_with_version_and_etag(
                key,
                &first_version,
                &first_etag,
            )]))
            .quiet(false)
            .build()
            .unwrap();
        let good_resp =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, good_delete).await;

        assert!(good_resp.deleted().is_empty());
        assert_eq!(good_resp.errors().len(), 1);
        assert_eq!(good_resp.errors()[0].key(), Some(key));
        assert_eq!(
            good_resp.errors()[0].version_id(),
            Some(first_version.as_str())
        );
        assert_eq!(good_resp.errors()[0].code(), Some("NotImplemented"));

        let first_get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&first_version)
            .send_retrying_operation_aborted("get first version after conditional multi-delete")
            .await
            .unwrap();
        let first_data = first_get.body.collect().await.unwrap().into_bytes();
        assert_eq!(&first_data[..], b"v1");

        let second_get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&second_version)
            .send_retrying_operation_aborted("get second version after conditional multi-delete")
            .await
            .unwrap();
        let data = second_get.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"v2");

        cleanup_versioned_bucket(&bucket).await;
    });
}

/// Batch-delete on a non-existent key in a versioned bucket creates a delete marker.
#[test]
fn test_versioning_multi_object_delete_nonexistent_creates_marker() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;

        // Batch-delete a key that was never created
        let objects = vec![ObjectIdentifier::builder()
            .key("never-existed")
            .build()
            .unwrap()];
        let delete = Delete::builder()
            .set_objects(Some(objects))
            .quiet(false)
            .build()
            .unwrap();
        let resp =
            s3_tests::delete_objects_retrying_operation_aborted(client, &bucket, delete).await;

        assert_eq!(resp.deleted().len(), 1);
        assert!(resp.errors().is_empty());
        let d = &resp.deleted()[0];
        assert!(
            d.delete_marker().unwrap_or(false),
            "expected delete_marker=true for nonexistent key in versioned bucket"
        );
        assert!(
            d.delete_marker_version_id().is_some(),
            "expected delete_marker_version_id for nonexistent key"
        );

        // Verify delete marker was actually created
        let list = client
            .list_object_versions()
            .bucket(&bucket)
            .send_retrying_operation_aborted("list object versions during delete tests")
            .await
            .unwrap();
        assert!(list.versions().is_empty());
        assert_eq!(list.delete_markers().len(), 1);
        assert_eq!(list.delete_markers()[0].key().unwrap(), "never-existed");

        cleanup_versioned_bucket(&bucket).await;
    });
}

// ── Response shapes ─────────────────────────────────────────────────

#[test]
fn test_delete_objects_response_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        let keys = ["shape-delete-objects-a.txt", "shape-delete-objects-b.txt"];
        for key in keys {
            s3_tests::put_object_retrying_operation_aborted(
                client,
                &bucket,
                key,
                b"delete-objects".to_vec(),
            )
            .await;
        }

        let delete_body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <Delete>\
             <Object><Key>{}</Key></Object>\
             <Object><Key>{}</Key></Object>\
             </Delete>",
            keys[0], keys[1]
        );
        let response = send_signed_request(
            "POST",
            &format!("{}/{}?delete=", CTX.endpoint(), bucket),
            delete_body.as_bytes(),
            [content_md5_header(delete_body.as_bytes())],
        );
        // Deleted entries may appear in any order; the body must be exactly
        // the envelope plus those entries, so any extra sibling element
        // (including Error entries) fails the envelope match.
        assert_shape(
            "DeleteObjects shape",
            &response,
            &shape().status(200).headers(xml_response_headers()),
        );
        assert_body_with_unordered_blocks(
            "DeleteObjects shape",
            &response.body,
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"></DeleteResult>",
            "Deleted",
            &[
                format!("<Deleted><Key>{}</Key></Deleted>", keys[0]),
                format!("<Deleted><Key>{}</Key></Deleted>", keys[1]),
            ],
            &std::collections::BTreeMap::new(),
        );

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}
