// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    AccessControlPolicy, BucketVersioningStatus, Grant, Grantee, ObjectOwnership, Owner,
    OwnershipControls, OwnershipControlsRule, Permission, Type, VersioningConfiguration,
};
use s3_tests::{
    assert_s3_err_code, cleanup_versioned_bucket, retrying_operation_aborted, unique_bucket,
    SendRetryingOperationAborted, CTX,
};

fn assert_canonical_owner_id(id: &str) {
    assert_eq!(
        id.len(),
        64,
        "expected 64-char canonical owner ID, got {id}"
    );
    assert!(
        id.bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
        "expected lowercase hex canonical owner ID, got {id}"
    );
}

async fn put_bucket_ownership_controls_retrying_operation_aborted(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    controls: OwnershipControls,
) {
    retrying_operation_aborted(
        "put bucket ownership controls during versioning ACL setup",
        || {
            client
                .put_bucket_ownership_controls()
                .bucket(bucket)
                .ownership_controls(controls.clone())
                .send()
        },
    )
    .await;
}

async fn put_bucket_versioning_retrying_operation_aborted(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    status: BucketVersioningStatus,
) {
    retrying_operation_aborted("put bucket versioning during versioning ACL setup", || {
        client
            .put_bucket_versioning()
            .bucket(bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(status.clone())
                    .build(),
            )
            .send()
    })
    .await;
}

async fn setup_versioned_acl_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    let ownership_rule = OwnershipControlsRule::builder()
        .object_ownership(ObjectOwnership::ObjectWriter)
        .build()
        .unwrap();
    let controls = OwnershipControls::builder()
        .rules(ownership_rule)
        .build()
        .unwrap();
    put_bucket_ownership_controls_retrying_operation_aborted(client, &bucket, controls).await;
    put_bucket_versioning_retrying_operation_aborted(
        client,
        &bucket,
        BucketVersioningStatus::Enabled,
    )
    .await;
    bucket
}

async fn canonical_owner_id(client: &aws_sdk_s3::Client) -> String {
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    let owner_id = client
        .get_bucket_acl()
        .bucket(&bucket)
        .send_retrying_operation_aborted("get bucket ACL during versioning ACL setup")
        .await
        .unwrap()
        .owner()
        .and_then(|owner| owner.id())
        .expect("expected owner ID in GetBucketAcl")
        .to_string();
    s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    owner_id
}

fn canonical_user_grant(canonical_user_id: &str, permission: Permission) -> Grant {
    Grant::builder()
        .grantee(
            Grantee::builder()
                .id(canonical_user_id)
                .r#type(Type::CanonicalUser)
                .build()
                .expect("canonical grantee"),
        )
        .permission(permission)
        .build()
}

fn access_control_policy(owner_id: &str, grants: Vec<Grant>) -> AccessControlPolicy {
    AccessControlPolicy::builder()
        .owner(Owner::builder().id(owner_id).build())
        .set_grants(Some(grants))
        .build()
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
        let resp =
            retrying_operation_aborted("put object during versioned object ACL setup", || {
                client
                    .put_object()
                    .bucket(bucket)
                    .key(key)
                    .body(ByteStream::from(body.clone().into_bytes()))
                    .send()
            })
            .await;
        version_ids.push(resp.version_id().unwrap().to_string());
        contents.push(body);
    }
    (version_ids, contents)
}

#[test]
fn test_versioned_object_acl() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = setup_versioned_acl_bucket().await;
        let key = "xyz";
        let (version_ids, contents) = create_multiple_versions(&bucket, key, 3).await;
        let older_version_id = version_ids[0].clone();
        let target_version_id = version_ids[1].clone();
        let current_version_id = version_ids[2].clone();

        let owner_id = client
            .get_object_acl()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("get object ACL during versioning ACL test")
            .await
            .unwrap()
            .owner()
            .and_then(|owner| owner.id())
            .expect("expected owner ID in GetObjectAcl")
            .to_string();
        assert_canonical_owner_id(&owner_id);
        let alt_owner_id = canonical_owner_id(alt_client).await;
        assert_canonical_owner_id(&alt_owner_id);

        retrying_operation_aborted("put object ACL on versioned target", || {
            client
                .put_object_acl()
                .bucket(&bucket)
                .key(key)
                .version_id(&target_version_id)
                .access_control_policy(access_control_policy(
                    &owner_id,
                    vec![
                        canonical_user_grant(&owner_id, Permission::FullControl),
                        canonical_user_grant(&alt_owner_id, Permission::Read),
                    ],
                ))
                .send()
        })
        .await;

        let target = alt_client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&target_version_id)
            .send_retrying_operation_aborted("get object during versioning ACL test")
            .await
            .unwrap();
        let target_body = target.body.collect().await.unwrap().into_bytes();
        assert!(
            std::str::from_utf8(&target_body).unwrap() == contents[1],
            "expected alternate client to read target version content {}, got {:?}",
            contents[1],
            target_body,
        );

        let current = alt_client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&current_version_id)
            .send_retrying_operation_aborted("get object during versioning ACL test")
            .await;
        assert_s3_err_code(&current, "AccessDenied");

        let older = alt_client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&older_version_id)
            .send_retrying_operation_aborted("get object during versioning ACL test")
            .await;
        assert_s3_err_code(&older, "AccessDenied");

        let head = alt_client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("get object during versioning ACL test")
            .await;
        assert_s3_err_code(&head, "AccessDenied");

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_versioned_object_acl_no_version_specified() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = setup_versioned_acl_bucket().await;
        let key = "xyz";
        let (version_ids, contents) = create_multiple_versions(&bucket, key, 3).await;
        let older_version_id = version_ids[0].clone();
        let current_version_id = version_ids[2].clone();
        let owner_id = client
            .get_object_acl()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("get object ACL during versioning ACL test")
            .await
            .unwrap()
            .owner()
            .and_then(|owner| owner.id())
            .expect("expected owner ID in GetObjectAcl")
            .to_string();
        let alt_owner_id = canonical_owner_id(alt_client).await;

        retrying_operation_aborted("put current object ACL on versioned object", || {
            client
                .put_object_acl()
                .bucket(&bucket)
                .key(key)
                .access_control_policy(access_control_policy(
                    &owner_id,
                    vec![
                        canonical_user_grant(&owner_id, Permission::FullControl),
                        canonical_user_grant(&alt_owner_id, Permission::Read),
                    ],
                ))
                .send()
        })
        .await;

        let current = alt_client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("get object during versioning ACL test")
            .await
            .unwrap();
        let current_body = current.body.collect().await.unwrap().into_bytes();
        assert_eq!(std::str::from_utf8(&current_body).unwrap(), contents[2]);

        let current_version = alt_client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&current_version_id)
            .send_retrying_operation_aborted("get object during versioning ACL test")
            .await
            .unwrap();
        let current_version_body = current_version.body.collect().await.unwrap().into_bytes();
        assert_eq!(
            std::str::from_utf8(&current_version_body).unwrap(),
            contents[2]
        );

        let older = alt_client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&older_version_id)
            .send_retrying_operation_aborted("get object during versioning ACL test")
            .await;
        assert_s3_err_code(&older, "AccessDenied");

        cleanup_versioned_bucket(client, &bucket).await;
    });
}
