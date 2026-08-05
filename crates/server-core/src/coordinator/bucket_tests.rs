use super::test_helpers::{self, UploadPartRequest};
use super::test_support::*;
use super::*;
use crate::conditional::DeleteCondition;
use s3_types::{
    AbortIncompleteMultipartUpload, BucketLifecycleConfiguration, LifecycleExpiration,
    LifecycleObjectSizeRange, LifecycleRule, LifecycleRuleFilter, LifecycleRuleStatus,
    LifecycleTag,
};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use storage::test_support::{install_bucket_scoped_test_hooks, BucketScopedTestHooks};
use storage::test_support::{
    StorageClusterLifecycleTestSupport as _, StorageClusterPayloadTestSupport as _,
};
use storage::PutBucketSubresource;

fn delete_bucket_test(coord: &Coordinator, name: &str) -> Result<(), ServerError> {
    coord.delete_bucket(&bucket_request_with_expected_owner(
        name,
        test_requester(),
        None,
    ))
}

fn bucket_request_with_expected_owner<'a>(
    name: &'a str,
    requester: Requester,
    expected_bucket_owner: Option<&'a str>,
) -> BucketRequest<'a> {
    BucketRequest::new(trusted_bucket_name(name), requester, expected_bucket_owner)
}

fn put_bucket_config_request_with_expected_owner<'a>(
    name: &'a str,
    config: &'a str,
    requester: Requester,
    expected_bucket_owner: Option<&'a str>,
) -> PutBucketConfigRequest<'a> {
    PutBucketConfigRequest {
        bucket: bucket_request_with_expected_owner(name, requester, expected_bucket_owner),
        config,
    }
}

fn put_bucket_policy_request_with_expected_owner<'a>(
    name: &'a str,
    config: &'a str,
    confirm_remove_self_bucket_access: bool,
    requester: Requester,
    expected_bucket_owner: Option<&'a str>,
) -> PutBucketPolicyRequest<'a> {
    PutBucketPolicyRequest {
        bucket: bucket_request_with_expected_owner(name, requester, expected_bucket_owner),
        config,
        confirm_remove_self_bucket_access,
    }
}

trait MultipartUploadIdArg {
    fn into_test_upload_id(self) -> UploadId;
}

impl MultipartUploadIdArg for &str {
    fn into_test_upload_id(self) -> UploadId {
        UploadId::try_from(self).unwrap_or_else(|_| trusted_upload_id(self))
    }
}

impl MultipartUploadIdArg for &UploadId {
    fn into_test_upload_id(self) -> UploadId {
        self.clone()
    }
}

impl MultipartUploadIdArg for String {
    fn into_test_upload_id(self) -> UploadId {
        UploadId::try_from(self.clone()).unwrap_or_else(|_| trusted_upload_id(&self))
    }
}

impl MultipartUploadIdArg for &String {
    fn into_test_upload_id(self) -> UploadId {
        UploadId::try_from(self.as_str()).unwrap_or_else(|_| trusted_upload_id(self))
    }
}

fn multipart_object_request_with_expected_owner<'a, I: MultipartUploadIdArg>(
    bucket: &'a str,
    key: &'a str,
    upload_id: I,
    requester: Requester,
    expected_bucket_owner: Option<&'a str>,
) -> MultipartObjectRequest<'a> {
    MultipartObjectRequest::new(
        trusted_bucket_name(bucket),
        trusted_object_key(key),
        upload_id.into_test_upload_id(),
        requester,
        expected_bucket_owner,
    )
}

fn delete_object_request<'a>(
    bucket: &'a str,
    key: &'a str,
    version_id: Option<VersionId>,
    requester: Requester,
    bypass_governance: bool,
    cond: &'a DeleteCondition,
) -> DeleteObjectRequest<'a> {
    delete_object_request_with_expected_owner(
        bucket,
        key,
        version_id,
        requester,
        None,
        bypass_governance,
        cond,
    )
}

fn delete_object_request_with_expected_owner<'a>(
    bucket: &'a str,
    key: &'a str,
    version_id: Option<VersionId>,
    requester: Requester,
    expected_bucket_owner: Option<&'a str>,
    bypass_governance: bool,
    cond: &'a DeleteCondition,
) -> DeleteObjectRequest<'a> {
    DeleteObjectRequest {
        object: object_version_request_with_expected_owner(
            bucket,
            key,
            version_id,
            requester,
            expected_bucket_owner,
        ),
        bypass_governance,
        cond,
    }
}

fn listed_key_versions(coord: &Coordinator, bucket: &str, key: &str) -> Vec<VersionEntry> {
    listed_key_versions_as(coord, bucket, key, test_requester())
}

fn listed_key_versions_as(
    coord: &Coordinator,
    bucket: &str,
    key: &str,
    requester: Requester,
) -> Vec<VersionEntry> {
    let result = coord
        .list_object_versions(&ListObjectVersionsRequest {
            bucket: bucket_request_with_expected_owner(bucket, requester, None),
            prefix: Some(key),
            delimiter: None,
            key_marker: None,
            version_id_marker: None,
            max_keys: 100,
            requested_max_keys: Some(100),
        })
        .unwrap();
    assert!(!result.is_truncated);
    result
        .versions
        .into_iter()
        .filter(|version| version.key == key)
        .collect()
}

fn put_bucket_versioning_test(
    coord: &Coordinator,
    name: &str,
    state: BucketVersioningState,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<(), ServerError> {
    coord.put_bucket_versioning(&PutBucketVersioningRequest {
        bucket: bucket_request_with_expected_owner(name, requester, expected_bucket_owner),
        state,
    })
}

fn put_bucket_encryption_test(
    coord: &Coordinator,
    name: &str,
    config: BucketEncryptionConfig,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<(), ServerError> {
    coord.put_bucket_encryption(&PutBucketEncryptionRequest {
        bucket: bucket_request_with_expected_owner(name, requester, expected_bucket_owner),
        config,
    })
}

fn get_bucket_encryption_test(
    coord: &Coordinator,
    name: &str,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<EffectiveBucketEncryptionConfig, ServerError> {
    coord.get_bucket_encryption(&bucket_request_with_expected_owner(
        name,
        requester,
        expected_bucket_owner,
    ))
}

fn delete_bucket_encryption_test(
    coord: &Coordinator,
    name: &str,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<(), ServerError> {
    coord.delete_bucket_encryption(&bucket_request_with_expected_owner(
        name,
        requester,
        expected_bucket_owner,
    ))
}

fn put_bucket_cors_test(
    coord: &Coordinator,
    name: &str,
    config: &str,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<(), ServerError> {
    coord.put_bucket_cors(&put_bucket_config_request_with_expected_owner(
        name,
        config,
        requester,
        expected_bucket_owner,
    ))
}

fn get_bucket_cors_test(
    coord: &Coordinator,
    name: &str,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<Option<String>, ServerError> {
    coord.get_bucket_cors(&bucket_request_with_expected_owner(
        name,
        requester,
        expected_bucket_owner,
    ))
}

fn delete_bucket_cors_test(
    coord: &Coordinator,
    name: &str,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<(), ServerError> {
    coord.delete_bucket_cors(&bucket_request_with_expected_owner(
        name,
        requester,
        expected_bucket_owner,
    ))
}

fn put_bucket_tags_test(
    coord: &Coordinator,
    name: &str,
    config: &str,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<(), ServerError> {
    let tags = bucket_tag_set(config);
    coord.put_bucket_tags(&PutBucketTagsRequest {
        bucket: bucket_request_with_expected_owner(name, requester, expected_bucket_owner),
        tags,
    })
}

fn get_bucket_tags_test(
    coord: &Coordinator,
    name: &str,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<Option<s3_types::TagSet>, ServerError> {
    coord.get_bucket_tags(&bucket_request_with_expected_owner(
        name,
        requester,
        expected_bucket_owner,
    ))
}

fn delete_bucket_tags_test(
    coord: &Coordinator,
    name: &str,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<(), ServerError> {
    coord.delete_bucket_tags(&bucket_request_with_expected_owner(
        name,
        requester,
        expected_bucket_owner,
    ))
}

fn put_bucket_tags_for_tag_resource_test(
    coord: &Coordinator,
    name: &str,
    config: &str,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<(), ServerError> {
    let tags = bucket_tag_set(config);
    coord.put_bucket_tags_for_tag_resource(&PutBucketTagControlRequest {
        control: BucketTagControlRequest {
            bucket: bucket_request_with_expected_owner(name, requester, expected_bucket_owner),
        },
        tags,
        request_tags: &[],
    })
}

fn set_bucket_abac_enabled_test(coord: &Coordinator, name: &str, enabled: bool) {
    coord
        .set_bucket_abac_enabled_for_test(name, enabled)
        .unwrap();
}

fn put_bucket_policy_test(
    coord: &Coordinator,
    name: &str,
    policy: &str,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<(), ServerError> {
    coord.put_bucket_policy(&put_bucket_policy_request_with_expected_owner(
        name,
        policy,
        false,
        requester,
        expected_bucket_owner,
    ))
}

fn get_bucket_policy_test(
    coord: &Coordinator,
    name: &str,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<Option<String>, ServerError> {
    coord.get_bucket_policy(&bucket_request_with_expected_owner(
        name,
        requester,
        expected_bucket_owner,
    ))
}

fn delete_bucket_policy_test(
    coord: &Coordinator,
    name: &str,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<(), ServerError> {
    coord.delete_bucket_policy(&bucket_request_with_expected_owner(
        name,
        requester,
        expected_bucket_owner,
    ))
}

fn deny_bucket_policy_action_for_principal(bucket: &str, principal: &str, action: &str) -> String {
    format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Deny","Principal":{{"AWS":"{principal}"}},"Action":"{action}","Resource":"arn:aws:s3:::{bucket}"}}]}}"#
    )
}

fn put_bucket_lifecycle_test(
    coord: &Coordinator,
    name: &str,
    config: &str,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<(), ServerError> {
    coord.put_bucket_lifecycle(&put_bucket_config_request_with_expected_owner(
        name,
        config,
        requester,
        expected_bucket_owner,
    ))
}

fn get_bucket_lifecycle_test(
    coord: &Coordinator,
    name: &str,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<Option<String>, ServerError> {
    coord.get_bucket_lifecycle(&bucket_request_with_expected_owner(
        name,
        requester,
        expected_bucket_owner,
    ))
}

fn delete_bucket_lifecycle_test(
    coord: &Coordinator,
    name: &str,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<(), ServerError> {
    coord.delete_bucket_lifecycle(&bucket_request_with_expected_owner(
        name,
        requester,
        expected_bucket_owner,
    ))
}

fn get_bucket_policy_status_test(
    coord: &Coordinator,
    name: &str,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<bool, ServerError> {
    coord.get_bucket_policy_status(&bucket_request_with_expected_owner(
        name,
        requester,
        expected_bucket_owner,
    ))
}

fn put_bucket_abac_test(
    coord: &Coordinator,
    name: &str,
    enabled: bool,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<(), ServerError> {
    coord.put_bucket_abac(&PutBucketAbacRequest {
        bucket: bucket_request_with_expected_owner(name, requester, expected_bucket_owner),
        enabled,
    })
}

fn get_bucket_abac_test(
    coord: &Coordinator,
    name: &str,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<bool, ServerError> {
    coord.get_bucket_abac(&bucket_request_with_expected_owner(
        name,
        requester,
        expected_bucket_owner,
    ))
}

fn parse_test_public_access_block_config(config: &str) -> PublicAccessBlockConfig {
    PublicAccessBlockConfig {
        block_public_acls: config.contains("<BlockPublicAcls>true</BlockPublicAcls>"),
        ignore_public_acls: config.contains("<IgnorePublicAcls>true</IgnorePublicAcls>"),
        block_public_policy: config.contains("<BlockPublicPolicy>true</BlockPublicPolicy>"),
        restrict_public_buckets: config
            .contains("<RestrictPublicBuckets>true</RestrictPublicBuckets>"),
    }
}

fn put_bucket_public_access_block_test(
    coord: &Coordinator,
    name: &str,
    config: &str,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<(), ServerError> {
    coord.put_bucket_public_access_block(&PutBucketPublicAccessBlockRequest {
        bucket: bucket_request_with_expected_owner(name, requester, expected_bucket_owner),
        config: parse_test_public_access_block_config(config),
    })
}

fn get_bucket_public_access_block_test(
    coord: &Coordinator,
    name: &str,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<Option<PublicAccessBlockConfig>, ServerError> {
    coord.get_bucket_public_access_block(&bucket_request_with_expected_owner(
        name,
        requester,
        expected_bucket_owner,
    ))
}

fn delete_bucket_public_access_block_test(
    coord: &Coordinator,
    name: &str,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<(), ServerError> {
    coord.delete_bucket_public_access_block(&bucket_request_with_expected_owner(
        name,
        requester,
        expected_bucket_owner,
    ))
}

fn parse_test_ownership_controls(config: &str) -> BucketOwnershipControls {
    let object_ownership =
        if config.contains("<ObjectOwnership>BucketOwnerEnforced</ObjectOwnership>") {
            BucketObjectOwnership::BucketOwnerEnforced
        } else if config.contains("<ObjectOwnership>BucketOwnerPreferred</ObjectOwnership>") {
            BucketObjectOwnership::BucketOwnerPreferred
        } else if config.contains("<ObjectOwnership>ObjectWriter</ObjectOwnership>") {
            BucketObjectOwnership::ObjectWriter
        } else {
            panic!("unknown ownership controls test config: {config}");
        };
    BucketOwnershipControls { object_ownership }
}

fn put_bucket_ownership_controls_test(
    coord: &Coordinator,
    name: &str,
    config: &str,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<(), ServerError> {
    coord.put_bucket_ownership_controls(&PutBucketOwnershipControlsRequest {
        bucket: bucket_request_with_expected_owner(name, requester, expected_bucket_owner),
        config: parse_test_ownership_controls(config),
    })
}

fn get_bucket_ownership_controls_test(
    coord: &Coordinator,
    name: &str,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<Option<BucketOwnershipControls>, ServerError> {
    coord.get_bucket_ownership_controls(&bucket_request_with_expected_owner(
        name,
        requester,
        expected_bucket_owner,
    ))
}

fn delete_bucket_ownership_controls_test(
    coord: &Coordinator,
    name: &str,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<(), ServerError> {
    coord.delete_bucket_ownership_controls(&bucket_request_with_expected_owner(
        name,
        requester,
        expected_bucket_owner,
    ))
}

fn put_bucket_canned_acl_test(
    coord: &Coordinator,
    name: &str,
    acl: BucketAcl,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<(), ServerError> {
    coord.put_bucket_acl(&PutBucketAclRequest {
        bucket: bucket_request_with_expected_owner(name, requester, expected_bucket_owner),
        acl: PutBucketAclInput::Canned(acl),
        policy_context: PutObjectPolicyContext::default(),
    })
}

fn get_bucket_acl_test(
    coord: &Coordinator,
    name: &str,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<GetBucketAclResult, ServerError> {
    coord.get_bucket_acl(&bucket_request_with_expected_owner(
        name,
        requester,
        expected_bucket_owner,
    ))
}

fn put_object_retention_test(
    coord: &Coordinator,
    bucket: &str,
    key: &str,
    version_id: Option<VersionId>,
    retention: ObjectRetention,
    bypass_governance: bool,
    requester: Requester,
) -> Result<(), ServerError> {
    coord
        .put_object_retention(&PutObjectRetentionRequest {
            object: object_version_request(bucket, key, version_id, requester),
            retention,
            bypass_governance,
        })
        .map(|_| ())
}

fn grants_contain(acl_grants: &AclGrants, grantee: &AclGrantee, permission: AclPermission) -> bool {
    acl_grants
        .iter()
        .any(|grant| grant.grantee() == grantee && grant.permission() == permission)
}

#[test]
fn bucket_crud() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    // Create
    coord
        .create_bucket_for_owner("default-owner", "test-bucket", false)
        .unwrap();

    // Head
    let info = coord
        .unchecked_active_bucket_summary("test-bucket")
        .unwrap();
    assert_eq!(info.name, "test-bucket");

    // List
    let buckets = coord
        .list_buckets(&ListBucketsRequest {
            requester: test_requester(),
        })
        .unwrap();
    assert_eq!(buckets.len(), 1);

    // Delete
    delete_bucket_test(&coord, "test-bucket").unwrap();
    assert!(coord
        .unchecked_active_bucket_summary("test-bucket")
        .is_err());
}

#[test]
fn create_bucket_reuse_finalizes_deleting_bucket_inline() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_without_reclaim_sweeper(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    delete_bucket_test(&coord, "bucket").unwrap();

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let info = coord.unchecked_active_bucket_summary("bucket").unwrap();
    assert_eq!(info.name, "bucket");
}

#[test]
fn create_bucket_reloads_when_deleting_bucket_is_recreated_by_racer() {
    let _storage_serial = super::test_hooks::STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap();
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_without_reclaim_sweeper(tmp.path());
    let bucket = trusted_bucket_name("bucket");
    let owner = storage::OwnerIdentity::from_principal("default-owner");
    let acl_grants = Coordinator::bucket_acl_grants_from_flags(&owner, false, false);

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    delete_bucket_test(&coord, "bucket").unwrap();

    let storage = Arc::clone(&coord.storage_node());
    let hook_bucket = bucket.clone();
    let hook_owner = owner.clone();
    let hook_acl_grants = acl_grants.clone();
    let raced = Arc::new(AtomicBool::new(false));
    let _hook_guard = install_bucket_scoped_test_hooks(BucketScopedTestHooks {
        target: Some(bucket.clone()),
        after_bucket_delete_finalize: Some(Arc::new({
            let raced = Arc::clone(&raced);
            move || {
                if raced.swap(true, Ordering::SeqCst) {
                    return;
                }
                storage
                    .create_bucket_with_config_and_load_info(&storage::CreateBucketConfig {
                        name: hook_bucket.as_str(),
                        owner_principal: hook_owner.principal.as_str(),
                        owner_canonical_id: &hook_owner.canonical_id,
                        acl_grants: &hook_acl_grants,
                        public_read: false,
                        public_write: false,
                        versioning: BucketVersioningState::Disabled,
                        object_lock: storage::BucketObjectLockConfig {
                            enabled: false,
                            default_retention: None,
                        },
                        ownership_controls: storage::BucketOwnershipControls {
                            object_ownership: storage::BucketObjectOwnership::ObjectWriter,
                        },
                    })
                    .unwrap();
            }
        })),
        ..BucketScopedTestHooks::default()
    });

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    assert!(raced.load(Ordering::SeqCst));
}

#[test]
fn create_bucket_idempotent() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    // Second create should succeed (idempotent for same owner)
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    // Only one bucket should exist
    let buckets = coord
        .list_buckets(&ListBucketsRequest {
            requester: test_helpers::requester("default-owner"),
        })
        .unwrap();
    assert_eq!(buckets.len(), 1);
}

#[test]
fn create_bucket_idempotent_in_us_east_1_resets_acl() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("owner-a"),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::BucketOwnerEnforced,
            object_lock_enabled: false,
        })
        .unwrap();

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("owner-a"),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::BucketOwnerEnforced,
            object_lock_enabled: false,
        })
        .unwrap();

    let acl =
        get_bucket_acl_test(&coord, "bucket", test_helpers::requester("owner-a"), None).unwrap();
    assert!(grants_contain(
        &acl.acl_grants,
        &AclGrantee::CanonicalUser(CanonicalUserId::from_principal("owner-a")),
        AclPermission::FullControl,
    ));
    assert!(!grants_contain(
        &acl.acl_grants,
        &AclGrantee::AllUsers,
        AclPermission::Read,
    ));
}

#[test]
fn create_bucket_persists_requested_ownership_controls_atomically() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("owner-a"),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::BucketOwnerPreferred,
            object_lock_enabled: false,
        })
        .unwrap();

    let info = coord
        .storage_node()
        .head_bucket_info(&trusted_bucket_name("bucket"))
        .unwrap();
    assert_eq!(
        info.ownership_controls,
        Some(BucketOwnershipControls {
            object_ownership: BucketObjectOwnership::BucketOwnerPreferred,
        })
    );
    assert_eq!(
        get_bucket_ownership_controls_test(
            &coord,
            "bucket",
            test_helpers::requester("owner-a"),
            None,
        )
        .unwrap(),
        Some(BucketOwnershipControls {
            object_ownership: BucketObjectOwnership::BucketOwnerPreferred,
        })
    );
}

#[test]
fn create_bucket_same_owner_acl_enabled_us_east_1_returns_bucket_already_owned_by_you() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("owner-a"),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();
    put_bucket_canned_acl_test(
        &coord,
        "bucket",
        BucketAcl::PublicRead,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();

    let err = coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("owner-a"),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::BucketOwnerEnforced,
            object_lock_enabled: false,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::BucketAlreadyOwnedByYou));

    let acl =
        get_bucket_acl_test(&coord, "bucket", test_helpers::requester("owner-a"), None).unwrap();
    assert!(grants_contain(
        &acl.acl_grants,
        &AclGrantee::CanonicalUser(CanonicalUserId::from_principal("owner-a")),
        AclPermission::FullControl,
    ));
    assert!(grants_contain(
        &acl.acl_grants,
        &AclGrantee::AllUsers,
        AclPermission::Read,
    ));

    let controls = get_bucket_ownership_controls_test(
        &coord,
        "bucket",
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        controls,
        BucketOwnershipControls {
            object_ownership: BucketObjectOwnership::ObjectWriter,
        }
    );
}

#[test]
fn create_bucket_same_owner_non_us_east_1_returns_bucket_already_owned_by_you() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_in_region(tmp.path(), "us-west-2");

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("default-owner"),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::BucketOwnerEnforced,
            object_lock_enabled: false,
        })
        .unwrap();
    let err = coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("default-owner"),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::BucketOwnerEnforced,
            object_lock_enabled: false,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::BucketAlreadyOwnedByYou));
}

#[test]
fn create_bucket_account_regional_rejects_mismatched_account_suffix() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    let err = coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket-444455556666-us-east-1-an"),
            requester: test_helpers::requester("111122223333"),
            namespace: BucketNamespace::AccountRegional,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap_err();
    match err {
        ServerError::InvalidBucketNamespace {
            bucket_namespace,
            reason,
        } => {
            assert_eq!(bucket_namespace, "bucket-444455556666-us-east-1-an");
            assert!(reason.contains("requested AWS Account ID '444455556666'"));
            assert!(reason.contains("caller's AWS Account ID '111122223333'"));
        }
        other => panic!("expected InvalidBucketNamespace, got {other:?}"),
    }
}

#[test]
fn create_bucket_account_regional_rejects_mismatched_region_suffix() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_in_region(tmp.path(), "eu-central-1");

    let err = coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket-111122223333-us-east-1-an"),
            requester: test_helpers::requester("111122223333"),
            namespace: BucketNamespace::AccountRegional,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap_err();
    match err {
        ServerError::InvalidBucketNamespace {
            bucket_namespace,
            reason,
        } => {
            assert_eq!(bucket_namespace, "bucket-111122223333-us-east-1-an");
            assert!(reason.contains("requested region 'us-east-1'"));
            assert!(reason.contains("current region 'eu-central-1'"));
        }
        other => panic!("expected InvalidBucketNamespace, got {other:?}"),
    }
}

#[test]
fn create_bucket_account_regional_rejects_name_without_suffix() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    let err = coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("ordinary-bucket"),
            requester: test_helpers::requester("111122223333"),
            namespace: BucketNamespace::AccountRegional,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap_err();
    assert!(matches!(
        err,
        ServerError::AccountRegionalNamespaceHeaderRequiresSuffix { bucket }
            if bucket == "ordinary-bucket"
    ));
}

#[test]
fn create_bucket_global_namespace_rejects_account_regional_name() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    let err = coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket-111122223333-us-east-1-an"),
            requester: test_helpers::requester("111122223333"),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::MissingNamespaceHeader));
}

#[test]
fn create_bucket_account_regional_accepts_iam_arn_principal() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket-111122223333-us-east-1-an"),
            requester: test_helpers::requester("arn:aws:iam::111122223333:user/reader"),
            namespace: BucketNamespace::AccountRegional,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();
}

#[test]
fn create_bucket_different_owner_conflicts() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    let err = coord
        .create_bucket_for_owner("owner-b", "bucket", false)
        .unwrap_err();
    assert!(matches!(err, ServerError::BucketAlreadyExists));
}

#[test]
fn create_bucket_different_owner_acl_enabled_conflicts() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("owner-a"),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();

    let err = coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("owner-b"),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::BucketAlreadyExists));
}

#[test]
fn authorize_create_bucket_rejects_anonymous() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    let err = coord
        .authorize_create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::anonymous(),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn authorize_create_bucket_rejects_explicit_private_with_owner_enforced() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    let err = coord
        .authorize_create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("owner-a"),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::Canned(BucketAcl::Private),
            ownership: BucketObjectOwnership::BucketOwnerEnforced,
            object_lock_enabled: false,
        })
        .unwrap_err();
    assert!(matches!(
        err,
        ServerError::InvalidBucketAclWithObjectOwnership
    ));
}

#[test]
fn list_buckets_scoped_by_owner() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("owner-a", "bucket-a", false)
        .unwrap();
    coord
        .create_bucket_for_owner("owner-b", "bucket-b", false)
        .unwrap();

    let a = coord
        .list_buckets(&ListBucketsRequest {
            requester: test_helpers::requester("owner-a"),
        })
        .unwrap();
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].name, "bucket-a");
    assert_eq!(a[0].owner_principal, "owner-a");

    let b = coord
        .list_buckets(&ListBucketsRequest {
            requester: test_helpers::requester("owner-b"),
        })
        .unwrap();
    assert_eq!(b.len(), 1);
    assert_eq!(b[0].name, "bucket-b");
    assert_eq!(b[0].owner_principal, "owner-b");
}

#[test]
fn list_buckets_scoped_by_owner_account_canonical_id() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let same_account_canonical_id = CanonicalUserId::from_principal("111122223333");
    let owner_root = AccountIdentity::new(
        "arn:aws:iam::111122223333:root",
        same_account_canonical_id.clone(),
        "Owner Root",
    );
    let owner_user = AccountIdentity::new(
        "arn:aws:iam::111122223333:user/admin",
        same_account_canonical_id,
        "Owner User",
    );

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket-root"),
            requester: Requester::authenticated(owner_root.clone()),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::BucketOwnerEnforced,
            object_lock_enabled: false,
        })
        .unwrap();
    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket-user"),
            requester: Requester::authenticated(owner_user.clone()),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::BucketOwnerEnforced,
            object_lock_enabled: false,
        })
        .unwrap();

    let root_names: Vec<String> = coord
        .list_buckets(&ListBucketsRequest {
            requester: Requester::authenticated(owner_root),
        })
        .unwrap()
        .into_iter()
        .map(|bucket| bucket.name.into_string())
        .collect();
    assert_eq!(root_names, vec!["bucket-root", "bucket-user"]);

    let user_names: Vec<String> = coord
        .list_buckets(&ListBucketsRequest {
            requester: Requester::authenticated(owner_user),
        })
        .unwrap()
        .into_iter()
        .map(|bucket| bucket.name.into_string())
        .collect();
    assert_eq!(user_names, vec!["bucket-root", "bucket-user"]);
}

#[test]
fn list_buckets_globally_sorted() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "zz-top", false)
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", "alpha", false)
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", "mango", false)
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", "beta", false)
        .unwrap();

    let names: Vec<String> = coord
        .list_buckets(&ListBucketsRequest {
            requester: test_helpers::requester("default-owner"),
        })
        .unwrap()
        .into_iter()
        .map(|b| b.name.into_string())
        .collect();
    assert_eq!(names, vec!["alpha", "beta", "mango", "zz-top"]);
}

#[test]
fn list_buckets_rejects_anonymous() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    let err = coord
        .list_buckets(&ListBucketsRequest {
            requester: Requester::anonymous(),
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn authorize_list_buckets_rejects_anonymous() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    let err = coord
        .authorize_list_buckets(&ListBucketsRequest {
            requester: Requester::anonymous(),
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn authorize_delete_bucket_allows_same_account_owner_account() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let same_account_canonical_id = CanonicalUserId::from_principal("111122223333");
    let bucket_owner = AccountIdentity::new(
        "arn:aws:iam::111122223333:root",
        same_account_canonical_id.clone(),
        "Bucket Owner",
    );
    let same_account_account_principal = AccountIdentity::new(
        "111122223333",
        same_account_canonical_id,
        "Same Account Owner Principal",
    );

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::authenticated(bucket_owner),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::BucketOwnerEnforced,
            object_lock_enabled: false,
        })
        .unwrap();

    coord
        .authorize_delete_bucket(&bucket_request_with_expected_owner(
            "bucket",
            Requester::authenticated_owner_account_admin(same_account_account_principal),
            None,
        ))
        .unwrap();
}

#[test]
fn create_bucket_sets_ownership_controls() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("owner-a"),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();

    let controls = get_bucket_ownership_controls_test(
        &coord,
        "bucket",
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        controls,
        BucketOwnershipControls {
            object_ownership: BucketObjectOwnership::ObjectWriter,
        }
    );
}

#[test]
fn bucket_policy_round_trips() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("owner-a"),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();

    let policy = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Deny\",\"Principal\":\"*\",\"Action\":\"s3:GetObject\",\"Resource\":\"arn:aws:s3:::bucket/*\"}]}";
    put_bucket_policy_test(
        &coord,
        "bucket",
        policy,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();

    assert_eq!(
        get_bucket_policy_test(&coord, "bucket", test_helpers::requester("owner-a"), None).unwrap(),
        Some(policy.to_string())
    );

    delete_bucket_policy_test(&coord, "bucket", test_helpers::requester("owner-a"), None).unwrap();
    delete_bucket_policy_test(&coord, "bucket", test_helpers::requester("owner-a"), None).unwrap();

    assert_eq!(
        get_bucket_policy_test(&coord, "bucket", test_helpers::requester("owner-a"), None).unwrap(),
        None
    );
}

#[test]
fn bucket_lifecycle_round_trips() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("owner-a"),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();

    let lifecycle = "<LifecycleConfiguration>\
            <Rule>\
                <ID>expire-current</ID>\
                <Filter><Prefix>logs/</Prefix></Filter>\
                <Status>Enabled</Status>\
                <Expiration><Days>3</Days></Expiration>\
            </Rule>\
        </LifecycleConfiguration>";
    put_bucket_lifecycle_test(
        &coord,
        "bucket",
        lifecycle,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();

    assert_eq!(
        get_bucket_lifecycle_test(&coord, "bucket", test_helpers::requester("owner-a"), None)
            .unwrap(),
        Some(lifecycle.to_string())
    );

    delete_bucket_lifecycle_test(&coord, "bucket", test_helpers::requester("owner-a"), None)
        .unwrap();
    delete_bucket_lifecycle_test(&coord, "bucket", test_helpers::requester("owner-a"), None)
        .unwrap();

    assert_eq!(
        get_bucket_lifecycle_test(&coord, "bucket", test_helpers::requester("owner-a"), None)
            .unwrap(),
        None
    );
}

#[test]
fn evaluate_current_object_lifecycle_expiration_selects_earliest_matching_rule() {
    let config = BucketLifecycleConfiguration {
        rules: vec![
            LifecycleRule {
                id: Some("later".to_string()),
                status: LifecycleRuleStatus::Enabled,
                filter: LifecycleRuleFilter::legacy_prefix("logs/").unwrap(),
                expiration: Some(LifecycleExpiration::Days(
                    std::num::NonZeroU32::new(30).unwrap(),
                )),
                noncurrent_version_expiration: None,
                abort_incomplete_multipart_upload: None,
            },
            LifecycleRule {
                id: Some("earlier".to_string()),
                status: LifecycleRuleStatus::Enabled,
                filter: LifecycleRuleFilter::explicit_with_predicates(
                    Some("logs/".to_string()),
                    vec![LifecycleTag {
                        key: "env".to_string(),
                        value: "prod".to_string(),
                    }],
                    LifecycleObjectSizeRange::greater_than(10),
                )
                .unwrap(),
                expiration: Some(LifecycleExpiration::Days(
                    std::num::NonZeroU32::new(1).unwrap(),
                )),
                noncurrent_version_expiration: None,
                abort_incomplete_multipart_upload: None,
            },
        ],
    };
    let tags = vec![("env".to_string(), "prod".to_string())];
    let header = Coordinator::evaluate_current_object_lifecycle_expiration(
        &config,
        "logs/app.txt",
        &tags,
        20,
        1_700_000_000_000,
    )
    .unwrap();
    assert_eq!(header.rule_id.as_deref(), Some("earlier"));
    assert_eq!(
        header.expiry_time_millis,
        Coordinator::lifecycle_day_based_deadline(1_700_000_000_000, 1).unwrap()
    );
}

#[test]
fn requested_version_is_current_live_requires_implicit_current_request() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_without_lifecycle_sweeper(tmp.path());
    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_requester(),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();
    let put = coord
        .put_object(&PutObjectRequest {
            object: object_request("bucket", "key", test_requester()),
            data: b"body",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap();

    assert!(
        Coordinator::requested_version_is_current_live("bucket", "key", None, put.version_id)
            .unwrap()
    );
    assert!(!Coordinator::requested_version_is_current_live(
        "bucket",
        "key",
        Some(put.version_id),
        put.version_id
    )
    .unwrap());
}

#[test]
fn evaluate_multipart_lifecycle_abort_headers_matches_prefix_rule() {
    let config = BucketLifecycleConfiguration {
        rules: vec![
            LifecycleRule {
                id: Some("skip-tagged".to_string()),
                status: LifecycleRuleStatus::Enabled,
                filter: LifecycleRuleFilter::explicit_with_predicates(
                    Some("uploads/".to_string()),
                    vec![LifecycleTag {
                        key: "env".to_string(),
                        value: "prod".to_string(),
                    }],
                    LifecycleObjectSizeRange::default(),
                )
                .unwrap(),
                expiration: None,
                noncurrent_version_expiration: None,
                abort_incomplete_multipart_upload: Some(AbortIncompleteMultipartUpload {
                    days_after_initiation: std::num::NonZeroU32::new(2).unwrap(),
                }),
            },
            LifecycleRule {
                id: Some("abort-prefix".to_string()),
                status: LifecycleRuleStatus::Enabled,
                filter: LifecycleRuleFilter::legacy_prefix("uploads/").unwrap(),
                expiration: None,
                noncurrent_version_expiration: None,
                abort_incomplete_multipart_upload: Some(AbortIncompleteMultipartUpload {
                    days_after_initiation: std::num::NonZeroU32::new(7).unwrap(),
                }),
            },
        ],
    };
    let header = Coordinator::evaluate_multipart_lifecycle_abort_headers(
        &config,
        "uploads/archive.bin",
        1_700_000_000_000,
    )
    .unwrap();
    assert_eq!(header.rule_id.as_deref(), Some("abort-prefix"));
    assert_eq!(
        header.abort_time_millis,
        Coordinator::lifecycle_day_based_deadline(1_700_000_000_000, 7).unwrap()
    );
}

mod lifecycle_prop_tests {
    use super::*;
    use proptest::prelude::*;
    use proptest::test_runner::{Config as ProptestConfig, TestCaseError, TestCaseResult};
    use std::collections::{BTreeMap, BTreeSet};
    use std::fmt::Write as _;

    const PROP_BUCKET: &str = "bucket";
    const PROP_LIFECYCLE_XML: &str = "<LifecycleConfiguration><Rule><ID>expire</ID><Filter><Prefix/></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>";
    const PROP_MAX_KEYS: usize = 3;
    const PROP_MAX_OPS: usize = 8;
    const PROP_DAY_MILLIS: u64 = 86_400_000;
    const UNPAGINATED_MAX_KEYS: u32 = 100;
    const EXPIRATION_DAYS: u32 = 1;

    const DISABLED_VERSIONING_TARGETS: [BucketVersioningState; 2] = [
        BucketVersioningState::Disabled,
        BucketVersioningState::Enabled,
    ];
    const ENABLED_VERSIONING_TARGETS: [BucketVersioningState; 2] = [
        BucketVersioningState::Enabled,
        BucketVersioningState::Suspended,
    ];
    const SUSPENDED_VERSIONING_TARGETS: [BucketVersioningState; 2] = [
        BucketVersioningState::Enabled,
        BucketVersioningState::Suspended,
    ];

    #[derive(Debug, Clone)]
    enum LifecycleTraceSeed {
        Transition { choice: u8 },
        PutLive { key_index: usize, size: u8 },
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum LifecycleTraceOp {
        SetVersioning(BucketVersioningState),
        PutLive { key: String, size: u8 },
    }

    impl std::fmt::Display for LifecycleTraceOp {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::SetVersioning(state) => write!(f, "set-versioning({state:?})"),
                Self::PutLive { key, size } => {
                    write!(f, "put-live(key={key}, size={size})")
                }
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum LifecycleVersionKind {
        Live,
        DeleteMarker,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct LifecycleLiveSnapshot {
        key: String,
        size: u64,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct LifecycleVersionSnapshot {
        key: String,
        version_id: VersionId,
        kind: LifecycleVersionKind,
        size: Option<u64>,
        is_latest: bool,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct LifecycleVersion {
        version_id: VersionId,
        kind: LifecycleVersionKind,
        size: Option<u64>,
        last_modified: u64,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct LifecycleCurrentExpirationSweep {
        expired_current: u64,
        enabled_delete_marker_min_version_ids: BTreeMap<String, VersionId>,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct LifecycleModel {
        versioning: BucketVersioningState,
        objects: BTreeMap<String, Vec<LifecycleVersion>>,
    }

    impl LifecycleModel {
        fn new() -> Self {
            Self {
                versioning: BucketVersioningState::Disabled,
                objects: BTreeMap::new(),
            }
        }

        fn versioning(&self) -> BucketVersioningState {
            self.versioning
        }

        fn legal_versioning_targets(&self) -> &'static [BucketVersioningState] {
            match self.versioning {
                BucketVersioningState::Disabled => &DISABLED_VERSIONING_TARGETS,
                BucketVersioningState::Enabled => &ENABLED_VERSIONING_TARGETS,
                BucketVersioningState::Suspended => &SUSPENDED_VERSIONING_TARGETS,
            }
        }

        fn set_versioning(&mut self, next: BucketVersioningState) {
            assert!(self.legal_versioning_targets().contains(&next));
            self.versioning = next;
        }

        fn apply_put(&mut self, key: String, version_id: VersionId, size: u64, last_modified: u64) {
            self.insert_version(
                key,
                version_id,
                LifecycleVersionKind::Live,
                Some(size),
                last_modified,
            );
        }

        fn insert_version(
            &mut self,
            key: String,
            version_id: VersionId,
            kind: LifecycleVersionKind,
            size: Option<u64>,
            last_modified: u64,
        ) {
            let versions = self.objects.entry(key).or_default();
            if version_id.is_null() {
                versions.retain(|version| version.version_id != VersionId::Null);
            }
            versions.push(LifecycleVersion {
                version_id,
                kind,
                size,
                last_modified,
            });
        }

        fn next_numbered_version_id(&self, key: &str) -> VersionId {
            let next = self
                .objects
                .get(key)
                .into_iter()
                .flat_map(|versions| versions.iter())
                .filter_map(|version| match version.version_id {
                    VersionId::Null => None,
                    VersionId::Versioned(value) => Some(value.get()),
                })
                .max()
                .unwrap_or(0)
                + 1;
            VersionId::Versioned(
                std::num::NonZeroU64::new(next)
                    .expect("next numbered lifecycle version id is non-zero"),
            )
        }

        fn live_listing(&self) -> Vec<LifecycleLiveSnapshot> {
            self.objects
                .iter()
                .filter_map(|(key, versions)| versions.last().map(|current| (key, current)))
                .filter(|(_, current)| current.kind == LifecycleVersionKind::Live)
                .map(|(key, current)| LifecycleLiveSnapshot {
                    key: key.clone(),
                    size: current.size.unwrap_or(0),
                })
                .collect()
        }

        fn version_listing(&self) -> Vec<LifecycleVersionSnapshot> {
            let mut snapshots = Vec::new();
            for (key, versions) in &self.objects {
                for (index, version) in versions.iter().rev().enumerate() {
                    snapshots.push(LifecycleVersionSnapshot {
                        key: key.clone(),
                        version_id: version.version_id,
                        kind: version.kind,
                        size: version.size,
                        is_latest: index == 0,
                    });
                }
            }
            snapshots
        }

        fn sweep_candidates(&self) -> Vec<u64> {
            let mut candidates = BTreeSet::from([0u64]);
            for versions in self.objects.values() {
                let Some(current) = versions.last() else {
                    continue;
                };
                if current.kind != LifecycleVersionKind::Live {
                    continue;
                }
                let deadline = Coordinator::lifecycle_day_based_deadline(
                    current.last_modified,
                    EXPIRATION_DAYS,
                )
                .expect("static expiration days should produce a deadline");
                if deadline > 0 {
                    candidates.insert(deadline - 1);
                }
                candidates.insert(deadline);
            }
            candidates.into_iter().collect()
        }

        fn apply_current_expiration_sweep(
            &mut self,
            now_millis: u64,
        ) -> LifecycleCurrentExpirationSweep {
            let eligible_keys: Vec<String> = self
                .objects
                .iter()
                .filter_map(|(key, versions)| {
                    let current = versions.last()?;
                    if current.kind != LifecycleVersionKind::Live {
                        return None;
                    }
                    let deadline = Coordinator::lifecycle_day_based_deadline(
                        current.last_modified,
                        EXPIRATION_DAYS,
                    )
                    .expect("static expiration days should produce a deadline");
                    (deadline <= now_millis).then(|| key.clone())
                })
                .collect();

            let mut enabled_delete_marker_min_version_ids = BTreeMap::new();
            for key in &eligible_keys {
                match self.versioning {
                    BucketVersioningState::Disabled => {
                        self.objects.remove(key);
                    }
                    BucketVersioningState::Enabled => {
                        let next = self.next_numbered_version_id(key);
                        enabled_delete_marker_min_version_ids.insert(key.clone(), next);
                        self.insert_version(
                            key.clone(),
                            next,
                            LifecycleVersionKind::DeleteMarker,
                            None,
                            now_millis,
                        );
                    }
                    BucketVersioningState::Suspended => {
                        self.insert_version(
                            key.clone(),
                            VersionId::Null,
                            LifecycleVersionKind::DeleteMarker,
                            None,
                            now_millis,
                        );
                    }
                }
            }

            LifecycleCurrentExpirationSweep {
                expired_current: eligible_keys.len() as u64,
                enabled_delete_marker_min_version_ids,
            }
        }
    }

    fn render_trace(ops: &[LifecycleTraceOp]) -> String {
        let mut rendered = String::new();
        for (index, op) in ops.iter().enumerate() {
            let _ = writeln!(&mut rendered, "{index}: {op}");
        }
        rendered
    }

    fn lifecycle_key_strategy() -> BoxedStrategy<String> {
        proptest::string::string_regex(r"[a-z][a-z0-9/_-]{0,7}")
            .expect("static lifecycle key regex should compile")
            .boxed()
    }

    fn lifecycle_key_set_strategy() -> BoxedStrategy<Vec<String>> {
        proptest::collection::btree_set(lifecycle_key_strategy(), 1..=PROP_MAX_KEYS)
            .prop_map(|keys| keys.into_iter().collect())
            .boxed()
    }

    fn lifecycle_trace_seed_strategy(key_count: usize) -> BoxedStrategy<LifecycleTraceSeed> {
        prop_oneof![
            2 => any::<u8>().prop_map(|choice| LifecycleTraceSeed::Transition { choice }),
            5 => (0usize..key_count, 0u8..=8u8)
                .prop_map(|(key_index, size)| LifecycleTraceSeed::PutLive { key_index, size }),
        ]
        .boxed()
    }

    fn lifecycle_trace_strategy() -> BoxedStrategy<(Vec<String>, Vec<LifecycleTraceOp>, u8)> {
        lifecycle_key_set_strategy()
            .prop_flat_map(|keys| {
                let key_count = keys.len();
                let op_keys = keys.clone();
                let ops = proptest::collection::vec(
                    lifecycle_trace_seed_strategy(key_count),
                    0..=PROP_MAX_OPS,
                )
                .prop_map(move |seeds| {
                    let mut versioning = BucketVersioningState::Disabled;
                    let mut ops = Vec::with_capacity(seeds.len());

                    for seed in seeds {
                        match seed {
                            LifecycleTraceSeed::Transition { choice } => {
                                let legal_targets = match versioning {
                                    BucketVersioningState::Disabled => &DISABLED_VERSIONING_TARGETS,
                                    BucketVersioningState::Enabled => &ENABLED_VERSIONING_TARGETS,
                                    BucketVersioningState::Suspended => {
                                        &SUSPENDED_VERSIONING_TARGETS
                                    }
                                };
                                let next = legal_targets[(choice as usize) % legal_targets.len()];
                                versioning = next;
                                ops.push(LifecycleTraceOp::SetVersioning(next));
                            }
                            LifecycleTraceSeed::PutLive { key_index, size } => {
                                ops.push(LifecycleTraceOp::PutLive {
                                    key: op_keys[key_index].clone(),
                                    size,
                                });
                            }
                        }
                    }

                    ops
                });

                (Just(keys), ops, any::<u8>())
            })
            .boxed()
    }

    fn lifecycle_write_time(write_index: usize) -> u64 {
        (u64::try_from(write_index).expect("write index should fit in u64") + 1)
            .checked_mul(PROP_DAY_MILLIS)
            .and_then(|millis| millis.checked_sub(1))
            .expect("bounded lifecycle write times should not overflow")
    }

    fn install_lifecycle_rule(coord: &Coordinator) -> Result<(), TestCaseError> {
        put_bucket_lifecycle_test(
            coord,
            PROP_BUCKET,
            PROP_LIFECYCLE_XML,
            test_requester(),
            None,
        )
        .map_err(|err| TestCaseError::fail(format!("put_bucket_lifecycle failed: {err:?}")))
    }

    fn put_live_at(
        coord: &Coordinator,
        key: &str,
        size: u8,
        now_millis: u64,
    ) -> Result<PutObjectResult, TestCaseError> {
        let data = vec![size; usize::from(size)];
        let metadata = MetadataBlob::default();
        let system_metadata = SystemMetadata::default();
        storage::clock::with_time_override(now_millis, || {
            test_helpers::put_object(
                coord,
                &PutObjectRequest {
                    encryption: WriteEncryptionRequest::none(),
                    policy_context: PutObjectPolicyContext::default(),
                    object_lock: ObjectLockState::default(),
                    object: object_request_with_expected_owner(
                        PROP_BUCKET,
                        key,
                        test_requester(),
                        None,
                    ),
                    data: &data,
                    metadata: &metadata,
                    system_metadata: &system_metadata,
                    tags: None,
                    cond: NO_WRITE,
                    acl: NO_PUT_OBJECT_ACL.into(),
                },
            )
        })
        .map_err(|err| TestCaseError::fail(format!("put_object failed for key {key}: {err:?}")))
    }

    fn list_live_snapshots(
        coord: &Coordinator,
    ) -> Result<Vec<LifecycleLiveSnapshot>, TestCaseError> {
        let result = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: bucket_request_with_expected_owner(PROP_BUCKET, test_requester(), None),
                prefix: None,
                delimiter: None,
                continuation_token: None,
                max_keys: UNPAGINATED_MAX_KEYS,
                requested_max_keys: Some(UNPAGINATED_MAX_KEYS),
            })
            .map_err(|err| TestCaseError::fail(format!("list_objects_v2 failed: {err:?}")))?;

        if result.is_truncated {
            return Err(TestCaseError::fail(format!(
                "list_objects_v2 unexpectedly truncated with {} keys",
                UNPAGINATED_MAX_KEYS
            )));
        }
        if !result.common_prefixes.is_empty() {
            return Err(TestCaseError::fail(
                "list_objects_v2 unexpectedly returned common prefixes".to_string(),
            ));
        }

        Ok(result
            .objects
            .into_iter()
            .map(|entry| LifecycleLiveSnapshot {
                key: entry.key,
                size: entry.size,
            })
            .collect())
    }

    fn list_version_snapshots(
        coord: &Coordinator,
    ) -> Result<Vec<LifecycleVersionSnapshot>, TestCaseError> {
        let result = coord
            .list_object_versions(&ListObjectVersionsRequest {
                bucket: bucket_request_with_expected_owner(PROP_BUCKET, test_requester(), None),
                prefix: None,
                delimiter: None,
                key_marker: None,
                version_id_marker: None,
                max_keys: UNPAGINATED_MAX_KEYS,
                requested_max_keys: Some(UNPAGINATED_MAX_KEYS),
            })
            .map_err(|err| TestCaseError::fail(format!("list_object_versions failed: {err:?}")))?;

        if result.is_truncated {
            return Err(TestCaseError::fail(format!(
                "list_object_versions unexpectedly truncated with {} keys",
                UNPAGINATED_MAX_KEYS
            )));
        }

        Ok(result
            .versions
            .into_iter()
            .map(|entry| LifecycleVersionSnapshot {
                key: entry.key,
                version_id: entry.version_id,
                kind: if entry.is_delete_marker {
                    LifecycleVersionKind::DeleteMarker
                } else {
                    LifecycleVersionKind::Live
                },
                size: (!entry.is_delete_marker).then_some(entry.size),
                is_latest: entry.is_latest,
            })
            .collect())
    }

    fn assert_namespace_matches(
        coord: &Coordinator,
        model: &LifecycleModel,
        context: &str,
    ) -> TestCaseResult {
        let actual_live = list_live_snapshots(coord)?;
        let expected_live = model.live_listing();
        prop_assert_eq!(actual_live, expected_live, "{}", context);

        let actual_versions = list_version_snapshots(coord)?;
        let expected_versions = model.version_listing();
        prop_assert_eq!(actual_versions, expected_versions, "{}", context);
        Ok(())
    }

    fn assert_namespace_matches_after_current_expiration_sweep(
        coord: &Coordinator,
        model: &LifecycleModel,
        sweep: &LifecycleCurrentExpirationSweep,
        context: &str,
    ) -> TestCaseResult {
        let actual_live = list_live_snapshots(coord)?;
        let expected_live = model.live_listing();
        prop_assert_eq!(actual_live, expected_live, "{}", context);

        let actual_versions = list_version_snapshots(coord)?;
        let expected_versions = model.version_listing();
        prop_assert_eq!(
            actual_versions.len(),
            expected_versions.len(),
            "{}\nactual_versions={:?}\nexpected_versions={:?}",
            context,
            actual_versions,
            expected_versions
        );
        for (actual, expected) in actual_versions.iter().zip(expected_versions.iter()) {
            let lifecycle_marker_min_version_id = sweep
                .enabled_delete_marker_min_version_ids
                .get(&expected.key)
                .filter(|min_version_id| {
                    expected.kind == LifecycleVersionKind::DeleteMarker
                        && expected.is_latest
                        && expected.size.is_none()
                        && expected.version_id == **min_version_id
                });
            if let Some(min_version_id) = lifecycle_marker_min_version_id {
                prop_assert_eq!(&actual.key, &expected.key, "{}", context);
                prop_assert_eq!(actual.kind, expected.kind, "{}", context);
                prop_assert_eq!(actual.size, expected.size, "{}", context);
                prop_assert_eq!(actual.is_latest, expected.is_latest, "{}", context);
                prop_assert!(
                    actual.version_id.is_versioned()
                        && actual.version_id.to_u64() >= min_version_id.to_u64(),
                    "{}\nactual lifecycle marker version {:?} should be versioned and at least {:?}",
                    context,
                    actual.version_id,
                    min_version_id
                );
            } else {
                prop_assert_eq!(actual, expected, "{}", context);
            }
        }
        Ok(())
    }

    fn apply_trace_op(
        coord: &Coordinator,
        model: &mut LifecycleModel,
        op: &LifecycleTraceOp,
        write_index: &mut usize,
        context: &str,
    ) -> TestCaseResult {
        match op {
            LifecycleTraceOp::SetVersioning(state) => {
                put_bucket_versioning_test(coord, PROP_BUCKET, *state, test_requester(), None)
                    .map_err(|err| {
                        TestCaseError::fail(format!(
                            "{context}\nput_bucket_versioning failed: {err:?}"
                        ))
                    })?;
                model.set_versioning(*state);
            }
            LifecycleTraceOp::PutLive { key, size } => {
                let now_millis = lifecycle_write_time(*write_index);
                *write_index += 1;
                let result = put_live_at(coord, key, *size, now_millis)?;

                match model.versioning() {
                    BucketVersioningState::Enabled => {
                        prop_assert!(
                            result.version_id.is_versioned(),
                            "{context}\nexpected numbered version id for enabled bucket, got {:?}",
                            result.version_id
                        );
                    }
                    BucketVersioningState::Disabled | BucketVersioningState::Suspended => {
                        prop_assert_eq!(result.version_id, VersionId::Null, "{}", context);
                    }
                }

                model.apply_put(key.clone(), result.version_id, u64::from(*size), now_millis);
            }
        }

        Ok(())
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(24))]

        #[test]
        fn prop_lifecycle_current_expiration_matches_model(
            (keys, ops, sweep_selector) in lifecycle_trace_strategy(),
        ) {
            let trace = render_trace(&ops);
            let tmp = test_util::tempdir();
            let coord = setup_coordinator_with_pg_count_without_background_sweepers(
                tmp.path(),
                DEFAULT_TEST_PG_COUNT,
            );
            coord
                .create_bucket_for_owner("default-owner", PROP_BUCKET, false)
                .unwrap();
            let mut model = LifecycleModel::new();
            let mut write_index = 0usize;

            let initial_context = format!("initial state\nkeys={keys:?}\nfull trace:\n{trace}");
            assert_namespace_matches(&coord, &model, &initial_context)?;

            for (index, op) in ops.iter().enumerate() {
                let step_context = format!(
                    "after step {index}: {op}\nkeys={keys:?}\nfull trace:\n{trace}"
                );
                apply_trace_op(&coord, &mut model, op, &mut write_index, &step_context)?;
                assert_namespace_matches(&coord, &model, &step_context)?;
            }

            install_lifecycle_rule(&coord)?;

            let sweep_candidates = model.sweep_candidates();
            let sweep_at = sweep_candidates[(sweep_selector as usize) % sweep_candidates.len()];
            let mut expected_after_sweep = model.clone();
            let expected_sweep = expected_after_sweep.apply_current_expiration_sweep(sweep_at);

            let sweep_context = format!(
                "after lifecycle sweep at {sweep_at}\nkeys={keys:?}\nsweep_candidates={sweep_candidates:?}\nfull trace:\n{trace}"
            );
            let stats = coord.run_lifecycle_sweep_at(sweep_at).map_err(|err| {
                TestCaseError::fail(format!(
                    "{sweep_context}\nrun_lifecycle_sweep_at failed: {err:?}"
                ))
            })?;

            prop_assert_eq!(stats.scanned_buckets, 1, "{}", sweep_context);
            prop_assert_eq!(
                stats.expired_current_objects,
                expected_sweep.expired_current,
                "{}",
                sweep_context
            );
            prop_assert_eq!(stats.expired_noncurrent_versions, 0, "{}", sweep_context);
            prop_assert_eq!(stats.expired_delete_markers, 0, "{}", sweep_context);
            prop_assert_eq!(stats.aborted_multipart_uploads, 0, "{}", sweep_context);
            assert_namespace_matches_after_current_expiration_sweep(
                &coord,
                &expected_after_sweep,
                &expected_sweep,
                &sweep_context,
            )?;
        }
    }
}

#[test]
fn lifecycle_sweep_expires_nonversioned_current_object() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let metadata = MetadataBlob::default();
    let system_metadata = SystemMetadata::default();
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    put_bucket_lifecycle_test(
            &coord,
            "bucket",
            "<LifecycleConfiguration><Rule><ID>expire</ID><Filter><Prefix/></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>",
            test_requester(),
            None,
        )
        .unwrap();

    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"hello",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let lifecycle_object = storage::test_support::capture_lifecycle_object_observation(
        &coord.storage_node(),
        &trusted_bucket_name("bucket"),
        &trusted_object_key("key"),
        VersionId::Null,
    )
    .unwrap();
    let lease = storage::test_support::acquire_lifecycle_object_payload_lease(
        &coord.storage_node(),
        &lifecycle_object,
    )
    .unwrap();
    let deadline =
        Coordinator::lifecycle_day_based_deadline(lifecycle_object.last_modified(), 1).unwrap();

    let stats = coord.run_lifecycle_sweep_at(deadline).unwrap();
    assert_eq!(stats.scanned_buckets, 1);
    assert_eq!(stats.expired_current_objects, 1);

    assert!(listed_key_versions(&coord, "bucket", "key").is_empty());
    assert!(storage::test_support::lifecycle_object_has_reclaim_root(
        &coord.storage_node(),
        &lifecycle_object,
    )
    .unwrap());
    assert!(coord
        .storage_node()
        .test_capture_object_payload(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            VersionId::Null
        )
        .unwrap()
        .is_empty());
    drop(lease);
}

#[test]
fn lifecycle_sweep_skips_bucket_with_live_durable_claim() {
    let tmp = test_util::tempdir();
    let (first, second) =
        setup_same_process_coordinators_with_single_pg_without_lifecycle_sweeper(tmp.path());
    let bucket = trusted_bucket_name("bucket");
    first
        .create_bucket_for_owner("default-owner", bucket.as_str(), false)
        .unwrap();
    put_bucket_lifecycle_test(
        &first,
        bucket.as_str(),
        "<LifecycleConfiguration><Rule><ID>expire</ID><Filter><Prefix/></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>",
        test_requester(),
        None,
    )
    .unwrap();

    let bucket_info = first.storage_node().head_bucket_info(&bucket).unwrap();
    let claim_now = storage::clock::wall_time_millis();
    let claim = first
        .storage_node()
        .acquire_lifecycle_sweep_claim(
            &bucket,
            bucket_info.bucket_incarnation_generation,
            claim_now,
        )
        .unwrap()
        .expect("first coordinator should acquire lifecycle sweep claim");

    let future_lifecycle_evaluation_time = claim_now.saturating_add(120_000);
    let stats = second
        .run_lifecycle_sweep_at(future_lifecycle_evaluation_time)
        .unwrap();
    assert_eq!(stats.scanned_buckets, 0);
    assert_eq!(stats.expired_current_objects, 0);
    assert_eq!(stats.expired_noncurrent_versions, 0);
    assert_eq!(stats.expired_delete_markers, 0);
    assert_eq!(stats.aborted_multipart_uploads, 0);

    first
        .storage_node()
        .release_lifecycle_sweep_claim(&claim)
        .unwrap();
}

#[test]
fn lifecycle_sweep_failure_records_durable_claim_error() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_without_lifecycle_sweeper(tmp.path());
    let bucket = trusted_bucket_name("bucket");
    coord
        .create_bucket_for_owner("default-owner", bucket.as_str(), false)
        .unwrap();
    coord
        .storage_node()
        .put_bucket_subresource_and_load_info(
            &bucket,
            PutBucketSubresource::lifecycle("<LifecycleConfiguration><Rule><ID>broken"),
        )
        .unwrap();

    let err = coord.run_lifecycle_sweep_at(0).unwrap_err();
    assert!(
        format!("{err:?}").contains("lifecycle configuration"),
        "expected lifecycle parse failure, got {err:?}"
    );

    let steal_now = storage::clock::wall_time_millis().saturating_add(120_000);
    let roots = coord
        .storage_node()
        .list_lifecycle_sweep_roots(steal_now)
        .unwrap();
    let root = roots
        .iter()
        .find(|root| root.bucket == bucket)
        .expect("failed lifecycle claim should be rediscovered after lease expiry");
    let claim = coord
        .storage_node()
        .acquire_lifecycle_sweep_claim(&bucket, root.bucket_incarnation_generation, steal_now)
        .unwrap()
        .expect("expired failed lifecycle claim should be stealable");
    assert!(
        claim
            .last_error
            .as_deref()
            .is_some_and(|last_error| last_error.contains("lifecycle configuration")),
        "stolen claim should carry retry context, got {:?}",
        claim.last_error
    );
    coord
        .storage_node()
        .release_lifecycle_sweep_claim(&claim)
        .unwrap();
}

#[test]
fn lifecycle_sweep_old_incarnation_expired_claim_does_not_skip_current_root() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_without_lifecycle_sweeper(tmp.path());
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    coord
        .create_bucket_for_owner("default-owner", bucket.as_str(), false)
        .unwrap();
    put_bucket_lifecycle_test(
        &coord,
        bucket.as_str(),
        "<LifecycleConfiguration><Rule><ID>expire</ID><Filter><Prefix/></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>",
        test_requester(),
        None,
    )
    .unwrap();

    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                bucket.as_str(),
                key.as_str(),
                test_requester(),
                None,
            ),
            data: b"hello",
            metadata: &MetadataBlob::default(),
            system_metadata: &SystemMetadata::default(),
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let lifecycle_object = storage::test_support::capture_lifecycle_object_observation(
        &coord.storage_node(),
        &bucket,
        &key,
        VersionId::Null,
    )
    .unwrap();
    coord
        .storage_node()
        .test_seed_stale_lifecycle_sweep_claim(&bucket)
        .unwrap();

    let deadline =
        Coordinator::lifecycle_day_based_deadline(lifecycle_object.last_modified(), 1).unwrap();
    let stats = coord.run_lifecycle_sweep_at(deadline).unwrap();
    assert_eq!(stats.scanned_buckets, 1);
    assert_eq!(stats.expired_current_objects, 1);
}

#[test]
fn lifecycle_sweep_expires_versioned_current_with_delete_marker() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let metadata = MetadataBlob::default();
    let system_metadata = SystemMetadata::default();
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        test_requester(),
        None,
    )
    .unwrap();
    put_bucket_lifecycle_test(
            &coord,
            "bucket",
            "<LifecycleConfiguration><Rule><ID>expire</ID><Filter><Prefix/></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>",
            test_requester(),
            None,
        )
        .unwrap();

    let put = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"hello",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let lifecycle_object = storage::test_support::capture_lifecycle_object_observation(
        &coord.storage_node(),
        &trusted_bucket_name("bucket"),
        &trusted_object_key("key"),
        put.version_id,
    )
    .unwrap();
    let deadline =
        Coordinator::lifecycle_day_based_deadline(lifecycle_object.last_modified(), 1).unwrap();

    let stats = coord.run_lifecycle_sweep_at(deadline).unwrap();
    assert_eq!(stats.scanned_buckets, 1);
    assert_eq!(stats.expired_current_objects, 1);

    let versions = listed_key_versions(&coord, "bucket", "key");
    assert_eq!(versions.len(), 2);
    assert!(versions[0].is_latest);
    assert!(versions[0].is_delete_marker);
    assert_eq!(versions[1].version_id, put.version_id);
    assert!(!versions[1].is_latest);
    assert!(!versions[1].is_delete_marker);
    assert!(!storage::test_support::lifecycle_object_has_reclaim_root(
        &coord.storage_node(),
        &lifecycle_object,
    )
    .unwrap());
}

#[test]
fn lifecycle_sweep_expires_suspended_null_current_with_null_delete_marker() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let metadata = MetadataBlob::default();
    let system_metadata = SystemMetadata::default();
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        test_requester(),
        None,
    )
    .unwrap();
    let older = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"older",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Suspended,
        test_requester(),
        None,
    )
    .unwrap();
    put_bucket_lifecycle_test(
            &coord,
            "bucket",
            "<LifecycleConfiguration><Rule><ID>expire</ID><Filter><Prefix/></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>",
            test_requester(),
            None,
        )
        .unwrap();

    let put = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"current",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    assert_eq!(put.version_id, VersionId::Null);

    let lifecycle_object = storage::test_support::capture_lifecycle_object_observation(
        &coord.storage_node(),
        &trusted_bucket_name("bucket"),
        &trusted_object_key("key"),
        VersionId::Null,
    )
    .unwrap();
    let lease = storage::test_support::acquire_lifecycle_object_payload_lease(
        &coord.storage_node(),
        &lifecycle_object,
    )
    .unwrap();
    let deadline =
        Coordinator::lifecycle_day_based_deadline(lifecycle_object.last_modified(), 1).unwrap();

    let stats = coord.run_lifecycle_sweep_at(deadline).unwrap();
    assert_eq!(stats.scanned_buckets, 1);
    assert_eq!(stats.expired_current_objects, 1);

    assert!(storage::test_support::lifecycle_object_has_reclaim_root(
        &coord.storage_node(),
        &lifecycle_object,
    )
    .unwrap());

    let versions = coord
        .list_object_versions(&ListObjectVersionsRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            key_marker: None,
            version_id_marker: None,
            max_keys: 100,
            requested_max_keys: Some(100),
        })
        .unwrap();
    assert_eq!(versions.versions.len(), 2);
    assert_eq!(versions.versions[0].version_id, VersionId::Null);
    assert!(versions.versions[0].is_latest);
    assert!(versions.versions[0].is_delete_marker);
    assert_eq!(versions.versions[1].version_id, older.version_id);
    assert!(!versions.versions[1].is_latest);
    assert!(!versions.versions[1].is_delete_marker);

    drop(lease);
}

#[test]
fn lifecycle_sweep_expires_noncurrent_versioned_live_object() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let metadata = MetadataBlob::default();
    let system_metadata = SystemMetadata::default();
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        test_requester(),
        None,
    )
    .unwrap();
    put_bucket_lifecycle_test(
            &coord,
            "bucket",
            "<LifecycleConfiguration><Rule><ID>expire-noncurrent</ID><Filter><Prefix/></Filter><Status>Enabled</Status><NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays></NoncurrentVersionExpiration></Rule></LifecycleConfiguration>",
            test_requester(),
            None,
        )
        .unwrap();

    let older = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"older",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let current = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"current",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let lifecycle_object = storage::test_support::capture_lifecycle_object_observation(
        &coord.storage_node(),
        &trusted_bucket_name("bucket"),
        &trusted_object_key("key"),
        older.version_id,
    )
    .unwrap();
    let lease = storage::test_support::acquire_lifecycle_object_payload_lease(
        &coord.storage_node(),
        &lifecycle_object,
    )
    .unwrap();
    let deadline = Coordinator::lifecycle_day_based_deadline(
        lifecycle_object.became_noncurrent_at().unwrap(),
        1,
    )
    .unwrap();

    let stats = coord.run_lifecycle_sweep_at(deadline).unwrap();
    assert_eq!(stats.scanned_buckets, 1);
    assert_eq!(stats.expired_current_objects, 0);
    assert_eq!(stats.expired_noncurrent_versions, 1);

    let versions = listed_key_versions(&coord, "bucket", "key");
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].version_id, current.version_id);
    assert!(versions[0].is_latest);
    assert!(!versions[0].is_delete_marker);
    assert!(storage::test_support::lifecycle_object_has_reclaim_root(
        &coord.storage_node(),
        &lifecycle_object,
    )
    .unwrap());
    drop(lease);
}

#[test]
fn lifecycle_sweep_expires_suspended_noncurrent_numbered_version() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let metadata = MetadataBlob::default();
    let system_metadata = SystemMetadata::default();
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        test_requester(),
        None,
    )
    .unwrap();
    let numbered = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"older",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Suspended,
        test_requester(),
        None,
    )
    .unwrap();
    put_bucket_lifecycle_test(
            &coord,
            "bucket",
            "<LifecycleConfiguration><Rule><ID>expire-noncurrent</ID><Filter><Prefix/></Filter><Status>Enabled</Status><NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays></NoncurrentVersionExpiration></Rule></LifecycleConfiguration>",
            test_requester(),
            None,
        )
        .unwrap();
    let null_current = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"current",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    assert_eq!(null_current.version_id, VersionId::Null);

    let lifecycle_object = storage::test_support::capture_lifecycle_object_observation(
        &coord.storage_node(),
        &trusted_bucket_name("bucket"),
        &trusted_object_key("key"),
        numbered.version_id,
    )
    .unwrap();
    assert!(
        !storage::test_support::lifecycle_object_has_reclaim_root(
            &coord.storage_node(),
            &lifecycle_object,
        )
        .unwrap(),
        "suspended null-version write must not enqueue reclaim for the numbered version"
    );
    let lease = storage::test_support::acquire_lifecycle_object_payload_lease(
        &coord.storage_node(),
        &lifecycle_object,
    )
    .unwrap();
    let deadline = Coordinator::lifecycle_day_based_deadline(
        lifecycle_object.became_noncurrent_at().unwrap(),
        1,
    )
    .unwrap();

    let stats = coord.run_lifecycle_sweep_at(deadline).unwrap();
    assert_eq!(stats.scanned_buckets, 1);
    assert_eq!(stats.expired_current_objects, 0);
    assert_eq!(stats.expired_noncurrent_versions, 1);

    let versions = listed_key_versions(&coord, "bucket", "key");
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].version_id, VersionId::Null);
    assert!(versions[0].is_latest);
    assert!(!versions[0].is_delete_marker);
    assert!(storage::test_support::lifecycle_object_has_reclaim_root(
        &coord.storage_node(),
        &lifecycle_object,
    )
    .unwrap());
    drop(lease);
}

#[test]
fn suspended_direct_put_reclaims_replaced_null_under_numbered_current() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_without_reclaim_sweeper(tmp.path());
    let metadata = MetadataBlob::default();
    let system_metadata = SystemMetadata::default();
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Suspended,
        test_requester(),
        None,
    )
    .unwrap();
    let first_null = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"old-null",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    assert_eq!(first_null.version_id, VersionId::Null);

    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        test_requester(),
        None,
    )
    .unwrap();
    let numbered = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"numbered",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    assert_ne!(numbered.version_id, VersionId::Null);

    let old_null = storage::test_support::capture_lifecycle_object_observation(
        &coord.storage_node(),
        &trusted_bucket_name("bucket"),
        &trusted_object_key("key"),
        VersionId::Null,
    )
    .unwrap();
    let numbered = storage::test_support::capture_lifecycle_object_observation(
        &coord.storage_node(),
        &trusted_bucket_name("bucket"),
        &trusted_object_key("key"),
        numbered.version_id,
    )
    .unwrap();

    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Suspended,
        test_requester(),
        None,
    )
    .unwrap();
    let replacement_null = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"new-null",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    assert_eq!(replacement_null.version_id, VersionId::Null);

    assert!(
        storage::test_support::lifecycle_object_has_reclaim_root(&coord.storage_node(), &old_null,)
            .unwrap(),
        "suspended null-version direct PUT must reclaim the replaced null payload"
    );
    assert!(
        !storage::test_support::lifecycle_object_has_reclaim_root(
            &coord.storage_node(),
            &numbered,
        )
        .unwrap(),
        "suspended null-version direct PUT must not reclaim the displaced numbered payload"
    );
}

#[test]
fn lifecycle_sweep_noncurrent_expiration_respects_newer_noncurrent_versions() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let metadata = MetadataBlob::default();
    let system_metadata = SystemMetadata::default();
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        test_requester(),
        None,
    )
    .unwrap();
    put_bucket_lifecycle_test(
            &coord,
            "bucket",
            "<LifecycleConfiguration><Rule><ID>retain-one-newer</ID><Filter><Prefix/></Filter><Status>Enabled</Status><NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays><NewerNoncurrentVersions>1</NewerNoncurrentVersions></NoncurrentVersionExpiration></Rule></LifecycleConfiguration>",
            test_requester(),
            None,
        )
        .unwrap();

    let oldest = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"v1",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let middle = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"v2",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let current = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"v3",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let oldest_lifecycle = storage::test_support::capture_lifecycle_object_observation(
        &coord.storage_node(),
        &trusted_bucket_name("bucket"),
        &trusted_object_key("key"),
        oldest.version_id,
    )
    .unwrap();
    let middle_lifecycle = storage::test_support::capture_lifecycle_object_observation(
        &coord.storage_node(),
        &trusted_bucket_name("bucket"),
        &trusted_object_key("key"),
        middle.version_id,
    )
    .unwrap();
    let lease = storage::test_support::acquire_lifecycle_object_payload_lease(
        &coord.storage_node(),
        &oldest_lifecycle,
    )
    .unwrap();
    let deadline = Coordinator::lifecycle_day_based_deadline(
        oldest_lifecycle.became_noncurrent_at().unwrap(),
        1,
    )
    .unwrap();

    let stats = coord.run_lifecycle_sweep_at(deadline).unwrap();
    assert_eq!(stats.scanned_buckets, 1);
    assert_eq!(stats.expired_current_objects, 0);
    assert_eq!(stats.expired_noncurrent_versions, 1);

    let versions = listed_key_versions(&coord, "bucket", "key");
    assert_eq!(versions.len(), 2);
    assert_eq!(versions[0].version_id, current.version_id);
    assert!(versions[0].is_latest);
    assert_eq!(versions[1].version_id, middle.version_id);
    assert!(!versions[1].is_latest);
    assert!(versions.iter().all(|version| !version.is_delete_marker));
    assert!(storage::test_support::lifecycle_object_has_reclaim_root(
        &coord.storage_node(),
        &oldest_lifecycle,
    )
    .unwrap());
    assert!(!storage::test_support::lifecycle_object_has_reclaim_root(
        &coord.storage_node(),
        &middle_lifecycle,
    )
    .unwrap());
    drop(lease);
}

#[test]
fn lifecycle_sweep_skips_object_locked_noncurrent_version() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let requester = test_helpers::requester("owner-a");

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::authenticated(AccountIdentity::from_principal("owner-a")),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: true,
        })
        .unwrap();
    put_bucket_lifecycle_test(
            &coord,
            "bucket",
            "<LifecycleConfiguration><Rule><ID>expire-noncurrent</ID><Filter><Prefix/></Filter><Status>Enabled</Status><NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays></NoncurrentVersionExpiration></Rule></LifecycleConfiguration>",
            requester.clone(),
            None,
        )
        .unwrap();

    let older = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", requester.clone(), None),
            data: b"older",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    put_object_retention_test(
        &coord,
        "bucket",
        "key",
        Some(older.version_id),
        ObjectRetention {
            mode: ObjectLockMode::Governance,
            retain_until_unix_seconds: Coordinator::current_unix_seconds().unwrap() + 7 * 86_400,
        },
        false,
        requester.clone(),
    )
    .unwrap();
    let current = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", requester.clone(), None),
            data: b"current",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let lifecycle_object = storage::test_support::capture_lifecycle_object_observation(
        &coord.storage_node(),
        &trusted_bucket_name("bucket"),
        &trusted_object_key("key"),
        older.version_id,
    )
    .unwrap();
    let deadline = Coordinator::lifecycle_day_based_deadline(
        lifecycle_object.became_noncurrent_at().unwrap(),
        1,
    )
    .unwrap();

    let stats = coord.run_lifecycle_sweep_at(deadline).unwrap();
    assert_eq!(stats.scanned_buckets, 1);
    assert_eq!(stats.expired_current_objects, 0);
    assert_eq!(stats.expired_noncurrent_versions, 0);

    let versions = listed_key_versions_as(&coord, "bucket", "key", requester);
    assert_eq!(versions.len(), 2);
    assert_eq!(versions[0].version_id, current.version_id);
    assert!(versions[0].is_latest);
    assert_eq!(versions[1].version_id, older.version_id);
    assert!(!versions[1].is_latest);
    assert!(versions.iter().all(|version| !version.is_delete_marker));
}

#[test]
fn deleting_current_version_clears_repromoted_version_noncurrent_timestamp_for_lifecycle() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_without_lifecycle_sweeper(tmp.path());
    let metadata = MetadataBlob::default();
    let system_metadata = SystemMetadata::default();
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        test_requester(),
        None,
    )
    .unwrap();
    put_bucket_lifecycle_test(
            &coord,
            "bucket",
            "<LifecycleConfiguration><Rule><ID>expire-noncurrent</ID><Filter><Prefix/></Filter><Status>Enabled</Status><NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays></NoncurrentVersionExpiration></Rule></LifecycleConfiguration>",
            test_requester(),
            None,
        )
        .unwrap();

    let v1 = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"v1",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let v2 = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"v2",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let aged_v1 = coord
        .storage_node()
        .test_age_noncurrent_lifecycle_version(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            v1.version_id,
        )
        .unwrap();

    coord
        .delete_object(&delete_object_request(
            "bucket",
            "key",
            Some(v2.version_id),
            test_requester(),
            false,
            NO_DELETE,
        ))
        .unwrap();

    let repromoted = storage::test_support::capture_lifecycle_object_observation(
        &coord.storage_node(),
        &trusted_bucket_name("bucket"),
        &trusted_object_key("key"),
        v1.version_id,
    )
    .unwrap();
    assert_eq!(repromoted.became_noncurrent_at(), None);
    let versions = listed_key_versions(&coord, "bucket", "key");
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].version_id, v1.version_id);
    assert!(versions[0].is_latest);

    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"v3",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let old_deadline =
        Coordinator::lifecycle_day_based_deadline(aged_v1.became_noncurrent_at().unwrap(), 1)
            .unwrap();
    let stats = coord.run_lifecycle_sweep_at(old_deadline).unwrap();
    assert_eq!(stats.expired_current_objects, 0);
    assert_eq!(stats.expired_noncurrent_versions, 0);

    assert!(listed_key_versions(&coord, "bucket", "key")
        .iter()
        .any(|version| version.version_id == v1.version_id && !version.is_delete_marker));
}

#[test]
fn lifecycle_sweep_expires_explicit_expired_object_delete_marker() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_without_lifecycle_sweeper(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        test_requester(),
        None,
    )
    .unwrap();
    put_bucket_lifecycle_test(
            &coord,
            "bucket",
            "<LifecycleConfiguration><Rule><ID>expire-marker</ID><Filter><Prefix/></Filter><Status>Enabled</Status><Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration><NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays></NoncurrentVersionExpiration></Rule></LifecycleConfiguration>",
            test_requester(),
            None,
        )
        .unwrap();

    let put = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"v1",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let delete = coord
        .delete_object(&delete_object_request(
            "bucket",
            "key",
            None,
            test_requester(),
            false,
            NO_DELETE,
        ))
        .unwrap();
    assert!(delete.delete_marker);

    let lifecycle_object = storage::test_support::capture_lifecycle_object_observation(
        &coord.storage_node(),
        &trusted_bucket_name("bucket"),
        &trusted_object_key("key"),
        put.version_id,
    )
    .unwrap();
    let deadline = Coordinator::lifecycle_day_based_deadline(
        lifecycle_object.became_noncurrent_at().unwrap(),
        1,
    )
    .unwrap();

    let stats = coord.run_lifecycle_sweep_at(deadline).unwrap();
    assert_eq!(stats.expired_current_objects, 0);
    assert_eq!(stats.expired_noncurrent_versions, 1);
    assert_eq!(stats.expired_delete_markers, 1);
    assert_eq!(stats.aborted_multipart_uploads, 0);

    let versions = coord
        .list_object_versions(&ListObjectVersionsRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            key_marker: None,
            version_id_marker: None,
            max_keys: 100,
            requested_max_keys: Some(100),
        })
        .unwrap();
    assert!(versions.versions.is_empty());
}

#[test]
fn lifecycle_sweep_expires_delete_marker_after_expiration_days_deadline() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        test_requester(),
        None,
    )
    .unwrap();
    put_bucket_lifecycle_test(
            &coord,
            "bucket",
            "<LifecycleConfiguration><Rule><ID>expire-marker-by-days</ID><Filter><Prefix/></Filter><Status>Enabled</Status><Expiration><Days>2</Days></Expiration><NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays></NoncurrentVersionExpiration></Rule></LifecycleConfiguration>",
            test_requester(),
            None,
        )
        .unwrap();

    let put = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"v1",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let delete = coord
        .delete_object(&delete_object_request(
            "bucket",
            "key",
            None,
            test_requester(),
            false,
            NO_DELETE,
        ))
        .unwrap();
    assert!(delete.delete_marker);

    let live_lifecycle = storage::test_support::capture_lifecycle_object_observation(
        &coord.storage_node(),
        &trusted_bucket_name("bucket"),
        &trusted_object_key("key"),
        put.version_id,
    )
    .unwrap();
    let marker_version_id = delete
        .version_id
        .expect("versioned delete marker must return a version ID");
    let marker = listed_key_versions(&coord, "bucket", "key")
        .into_iter()
        .find(|version| version.version_id == marker_version_id)
        .expect("current delete marker should be listed");
    assert!(marker.is_delete_marker);
    let noncurrent_deadline = Coordinator::lifecycle_day_based_deadline(
        live_lifecycle.became_noncurrent_at().unwrap(),
        1,
    )
    .unwrap();
    let marker_deadline =
        Coordinator::lifecycle_day_based_deadline(marker.last_modified, 2).unwrap();

    let first_stats = coord.run_lifecycle_sweep_at(noncurrent_deadline).unwrap();
    assert_eq!(first_stats.expired_noncurrent_versions, 1);
    assert_eq!(first_stats.expired_delete_markers, 0);

    let versions = listed_key_versions(&coord, "bucket", "key");
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].version_id, marker_version_id);
    assert!(versions[0].is_latest);
    assert!(versions[0].is_delete_marker);

    let second_stats = coord.run_lifecycle_sweep_at(marker_deadline).unwrap();
    assert_eq!(second_stats.expired_noncurrent_versions, 0);
    assert_eq!(second_stats.expired_delete_markers, 1);
    assert_eq!(second_stats.aborted_multipart_uploads, 0);

    assert!(listed_key_versions(&coord, "bucket", "key").is_empty());
}

#[test]
fn lifecycle_sweep_aborts_due_incomplete_multipart_upload() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    put_bucket_lifecycle_test(
            &coord,
            "bucket",
            "<LifecycleConfiguration><Rule><ID>abort-mpu</ID><Filter><Prefix>logs/</Prefix></Filter><Status>Enabled</Status><AbortIncompleteMultipartUpload><DaysAfterInitiation>1</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>",
            test_requester(),
            None,
        )
        .unwrap();

    let matching = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                "bucket",
                "logs/app",
                test_requester(),
                None,
            ),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();
    let retained = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                "bucket",
                "tmp/keep",
                test_requester(),
                None,
            ),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    test_helpers::upload_part(
        &coord,
        &UploadPartRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "logs/app",
                &matching.upload_id,
                test_requester(),
                None,
            ),
            part_number: 1,
            data: b"hello multipart",
            claimed_checksum: None,
            sse_customer: None,
        },
    )
    .unwrap();

    let deadline = Coordinator::lifecycle_day_based_deadline(
        storage::test_support::multipart_upload_initiated_at(
            &coord.storage_node(),
            &trusted_bucket_name("bucket"),
            &trusted_object_key("logs/app"),
            &matching.upload_id,
        )
        .unwrap(),
        1,
    )
    .unwrap();

    let stats = coord.run_lifecycle_sweep_at(deadline).unwrap();
    assert_eq!(stats.expired_current_objects, 0);
    assert_eq!(stats.expired_noncurrent_versions, 0);
    assert_eq!(stats.expired_delete_markers, 0);
    assert_eq!(stats.aborted_multipart_uploads, 1);

    assert!(!storage::test_support::multipart_upload_exists(
        &coord.storage_node(),
        &trusted_bucket_name("bucket"),
        &trusted_object_key("logs/app"),
        &matching.upload_id,
    )
    .unwrap());
    assert!(coord
        .storage_node()
        .test_capture_multipart_upload_payload(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("logs/app"),
            &matching.upload_id
        )
        .unwrap()
        .is_empty());

    assert!(storage::test_support::multipart_upload_exists(
        &coord.storage_node(),
        &trusted_bucket_name("bucket"),
        &trusted_object_key("tmp/keep"),
        &retained.upload_id,
    )
    .unwrap());
}

#[test]
fn lifecycle_abort_rechecks_current_bucket_lifecycle_before_aborting_upload() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    put_bucket_lifecycle_test(
            &coord,
            "bucket",
            "<LifecycleConfiguration><Rule><ID>abort-mpu</ID><Filter><Prefix>logs/</Prefix></Filter><Status>Enabled</Status><AbortIncompleteMultipartUpload><DaysAfterInitiation>1</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>",
            test_requester(),
            None,
        )
        .unwrap();

    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                "bucket",
                "logs/app",
                test_requester(),
                None,
            ),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let deadline = Coordinator::lifecycle_day_based_deadline(
        storage::test_support::multipart_upload_initiated_at(
            &coord.storage_node(),
            &trusted_bucket_name("bucket"),
            &trusted_object_key("logs/app"),
            &upload.upload_id,
        )
        .unwrap(),
        1,
    )
    .unwrap();

    delete_bucket_lifecycle_test(&coord, "bucket", test_requester(), None).unwrap();
    let bucket_incarnation_generation = coord
        .storage_node()
        .head_bucket_info(&trusted_bucket_name("bucket"))
        .unwrap()
        .bucket_incarnation_generation;

    assert!(!coord
        .read_runtime()
        .abort_multipart_upload_if_due(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("logs/app"),
            &upload.upload_id,
            bucket_incarnation_generation,
            deadline,
        )
        .unwrap());

    assert!(storage::test_support::multipart_upload_exists(
        &coord.storage_node(),
        &trusted_bucket_name("bucket"),
        &trusted_object_key("logs/app"),
        &upload.upload_id,
    )
    .unwrap());
}

#[test]
fn lifecycle_abort_stops_when_delete_drain_starts_after_claim() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_without_lifecycle_sweeper(tmp.path());
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("logs/app");
    coord
        .create_bucket_for_owner("default-owner", bucket.as_str(), false)
        .unwrap();
    put_bucket_lifecycle_test(
            &coord,
            bucket.as_str(),
            "<LifecycleConfiguration><Rule><ID>abort-mpu</ID><Filter><Prefix>logs/</Prefix></Filter><Status>Enabled</Status><AbortIncompleteMultipartUpload><DaysAfterInitiation>1</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>",
            test_requester(),
            None,
        )
        .unwrap();

    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                bucket.as_str(),
                key.as_str(),
                test_requester(),
                None,
            ),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let deadline = Coordinator::lifecycle_day_based_deadline(
        storage::test_support::multipart_upload_initiated_at(
            &coord.storage_node(),
            &bucket,
            &key,
            &upload.upload_id,
        )
        .unwrap(),
        1,
    )
    .unwrap();
    let bucket_info = coord.storage_node().head_bucket_info(&bucket).unwrap();
    let claim = coord
        .storage_node()
        .acquire_lifecycle_sweep_claim(&bucket, bucket_info.bucket_incarnation_generation, deadline)
        .unwrap()
        .expect("lifecycle claim should be acquirable before delete drain");
    coord
        .storage_node()
        .test_begin_durable_bucket_delete_drain(&bucket)
        .unwrap();

    assert!(!coord
        .read_runtime()
        .abort_multipart_upload_if_due(
            &bucket,
            &key,
            &upload.upload_id,
            claim.bucket_incarnation_generation,
            deadline,
        )
        .unwrap());
    assert!(storage::test_support::multipart_upload_exists(
        &coord.storage_node(),
        &bucket,
        &key,
        &upload.upload_id,
    )
    .unwrap());

    coord
        .storage_node()
        .release_lifecycle_sweep_claim(&claim)
        .unwrap();
}

#[test]
fn lifecycle_current_expiry_stops_when_delete_drain_starts_after_claim() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_without_lifecycle_sweeper(tmp.path());
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    coord
        .create_bucket_for_owner("default-owner", bucket.as_str(), false)
        .unwrap();
    put_bucket_lifecycle_test(
        &coord,
        bucket.as_str(),
        "<LifecycleConfiguration><Rule><ID>expire</ID><Filter><Prefix/></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>",
        test_requester(),
        None,
    )
    .unwrap();
    let put = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                bucket.as_str(),
                key.as_str(),
                test_requester(),
                None,
            ),
            data: b"hello",
            metadata: &MetadataBlob::default(),
            system_metadata: &SystemMetadata::default(),
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let version_id = put.version_id;
    let bucket_info = coord.storage_node().head_bucket_info(&bucket).unwrap();
    let claim = coord
        .storage_node()
        .acquire_lifecycle_sweep_claim(
            &bucket,
            bucket_info.bucket_incarnation_generation,
            storage::clock::wall_time_millis(),
        )
        .unwrap()
        .expect("lifecycle claim should be acquirable before delete drain");
    coord
        .storage_node()
        .test_begin_durable_bucket_delete_drain(&bucket)
        .unwrap();

    let outcome = coord
        .storage_node()
        .expire_current_object_if_due(
            &bucket,
            &key,
            version_id,
            claim.bucket_incarnation_generation,
            |_, _| Ok::<bool, ServerError>(true),
        )
        .unwrap()
        .unwrap();
    assert!(
        outcome.is_none(),
        "lifecycle current expiry should stop behind DeleteBucket drain"
    );
    let versions = listed_key_versions(&coord, bucket.as_str(), key.as_str());
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].version_id, version_id);
    assert!(versions[0].is_latest);
    assert!(!versions[0].is_delete_marker);

    coord
        .storage_node()
        .release_lifecycle_sweep_claim(&claim)
        .unwrap();
}

#[test]
fn lifecycle_noncurrent_expiry_stops_when_delete_drain_starts_after_claim() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_without_lifecycle_sweeper(tmp.path());
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    coord
        .create_bucket_for_owner("default-owner", bucket.as_str(), false)
        .unwrap();
    put_bucket_lifecycle_test(
            &coord,
            bucket.as_str(),
            "<LifecycleConfiguration><Rule><ID>expire-noncurrent</ID><Filter><Prefix/></Filter><Status>Enabled</Status><NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays></NoncurrentVersionExpiration></Rule></LifecycleConfiguration>",
            test_requester(),
            None,
        )
        .unwrap();
    coord
        .put_bucket_versioning(&PutBucketVersioningRequest {
            bucket: bucket_request_with_expected_owner(bucket.as_str(), test_requester(), None),
            state: BucketVersioningState::Enabled,
        })
        .unwrap();
    let mut put_results = Vec::new();
    for data in [b"old".as_slice(), b"new".as_slice()] {
        put_results.push(
            test_helpers::put_object(
                &coord,
                &PutObjectRequest {
                    encryption: WriteEncryptionRequest::none(),
                    policy_context: PutObjectPolicyContext::default(),
                    object_lock: ObjectLockState::default(),
                    object: object_request_with_expected_owner(
                        bucket.as_str(),
                        key.as_str(),
                        test_requester(),
                        None,
                    ),
                    data,
                    metadata: &MetadataBlob::default(),
                    system_metadata: &SystemMetadata::default(),
                    tags: None,
                    cond: NO_WRITE,
                    acl: NO_PUT_OBJECT_ACL.into(),
                },
            )
            .unwrap(),
        );
    }
    let noncurrent_version_id = put_results[0].version_id;
    let bucket_info = coord.storage_node().head_bucket_info(&bucket).unwrap();
    let claim = coord
        .storage_node()
        .acquire_lifecycle_sweep_claim(
            &bucket,
            bucket_info.bucket_incarnation_generation,
            storage::clock::wall_time_millis(),
        )
        .unwrap()
        .expect("lifecycle claim should be acquirable before delete drain");
    coord
        .storage_node()
        .test_begin_durable_bucket_delete_drain(&bucket)
        .unwrap();

    let reclaimed = coord
        .storage_node()
        .delete_noncurrent_live_versions_if_due(
            &bucket,
            &key,
            claim.bucket_incarnation_generation,
            |_, _| Ok::<HashSet<VersionId>, ServerError>(HashSet::from([noncurrent_version_id])),
        )
        .unwrap()
        .unwrap();
    assert!(
        reclaimed.is_empty(),
        "lifecycle noncurrent expiry should stop behind DeleteBucket drain"
    );
    assert!(listed_key_versions(&coord, bucket.as_str(), key.as_str())
        .iter()
        .any(|version| {
            version.version_id == noncurrent_version_id
                && !version.is_latest
                && !version.is_delete_marker
        }));

    coord
        .storage_node()
        .release_lifecycle_sweep_claim(&claim)
        .unwrap();
}

#[test]
fn lifecycle_delete_marker_cleanup_stops_when_delete_drain_starts_after_claim() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_without_lifecycle_sweeper(tmp.path());
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    coord
        .create_bucket_for_owner("default-owner", bucket.as_str(), false)
        .unwrap();
    put_bucket_lifecycle_test(
            &coord,
            bucket.as_str(),
            "<LifecycleConfiguration><Rule><ID>expire-marker</ID><Filter><Prefix/></Filter><Status>Enabled</Status><Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration></Rule></LifecycleConfiguration>",
            test_requester(),
            None,
        )
        .unwrap();
    coord
        .put_bucket_versioning(&PutBucketVersioningRequest {
            bucket: bucket_request_with_expected_owner(bucket.as_str(), test_requester(), None),
            state: BucketVersioningState::Enabled,
        })
        .unwrap();
    let deleted = coord
        .delete_object(&delete_object_request(
            bucket.as_str(),
            key.as_str(),
            None,
            test_requester(),
            false,
            NO_DELETE,
        ))
        .unwrap();
    let marker_version_id = deleted
        .version_id
        .expect("versioned delete marker must return a version ID");
    let bucket_info = coord.storage_node().head_bucket_info(&bucket).unwrap();
    let claim = coord
        .storage_node()
        .acquire_lifecycle_sweep_claim(
            &bucket,
            bucket_info.bucket_incarnation_generation,
            storage::clock::wall_time_millis(),
        )
        .unwrap()
        .expect("lifecycle claim should be acquirable before delete drain");
    coord
        .storage_node()
        .test_begin_durable_bucket_delete_drain(&bucket)
        .unwrap();

    let deleted = coord
        .storage_node()
        .delete_expired_delete_marker_if_due(
            &bucket,
            &key,
            marker_version_id,
            claim.bucket_incarnation_generation,
            |_, _| Ok::<bool, ServerError>(true),
        )
        .unwrap()
        .unwrap();
    assert!(
        !deleted,
        "lifecycle delete-marker cleanup should stop behind DeleteBucket drain"
    );
    assert!(listed_key_versions(&coord, bucket.as_str(), key.as_str())
        .iter()
        .any(|version| {
            version.version_id == marker_version_id && version.is_latest && version.is_delete_marker
        }));

    coord
        .storage_node()
        .release_lifecycle_sweep_claim(&claim)
        .unwrap();
}

#[test]
fn lifecycle_aborting_upload_finish_stops_when_delete_drain_starts_after_claim() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_without_lifecycle_sweeper(tmp.path());
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("logs/app");
    coord
        .create_bucket_for_owner("default-owner", bucket.as_str(), false)
        .unwrap();
    put_bucket_lifecycle_test(
            &coord,
            bucket.as_str(),
            "<LifecycleConfiguration><Rule><ID>abort-mpu</ID><Filter><Prefix>logs/</Prefix></Filter><Status>Enabled</Status><AbortIncompleteMultipartUpload><DaysAfterInitiation>1</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>",
            test_requester(),
            None,
        )
        .unwrap();

    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                bucket.as_str(),
                key.as_str(),
                test_requester(),
                None,
            ),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();
    test_helpers::upload_part(
        &coord,
        &UploadPartRequest {
            upload: multipart_object_request_with_expected_owner(
                bucket.as_str(),
                key.as_str(),
                &upload.upload_id,
                test_requester(),
                None,
            ),
            part_number: 1,
            data: b"hello multipart",
            claimed_checksum: None,
            sse_customer: None,
        },
    )
    .unwrap();
    coord
        .storage_node()
        .test_mark_multipart_upload_aborting(&bucket, &key, &upload.upload_id)
        .unwrap();

    let bucket_info = coord.storage_node().head_bucket_info(&bucket).unwrap();
    let claim = coord
        .storage_node()
        .acquire_lifecycle_sweep_claim(
            &bucket,
            bucket_info.bucket_incarnation_generation,
            storage::clock::wall_time_millis(),
        )
        .unwrap()
        .expect("lifecycle claim should be acquirable before delete drain");
    coord
        .storage_node()
        .test_begin_durable_bucket_delete_drain(&bucket)
        .unwrap();

    assert!(!coord
        .read_runtime()
        .abort_multipart_upload_for_lifecycle_sweep(
            &bucket,
            &key,
            &upload.upload_id,
            claim.bucket_incarnation_generation,
        )
        .unwrap());
    assert_eq!(
        storage::test_support::multipart_upload_state(
            &coord.storage_node(),
            &bucket,
            &key,
            &upload.upload_id,
        )
        .unwrap(),
        UploadState::Aborting
    );

    coord
        .storage_node()
        .release_lifecycle_sweep_claim(&claim)
        .unwrap();
}

#[test]
fn lifecycle_sweep_finishes_aborting_multipart_upload_without_current_lifecycle_config() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_without_lifecycle_sweeper(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    put_bucket_lifecycle_test(
            &coord,
            "bucket",
            "<LifecycleConfiguration><Rule><ID>abort-mpu</ID><Filter><Prefix>logs/</Prefix></Filter><Status>Enabled</Status><AbortIncompleteMultipartUpload><DaysAfterInitiation>1</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>",
            test_requester(),
            None,
        )
        .unwrap();

    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                "bucket",
                "logs/app",
                test_requester(),
                None,
            ),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    test_helpers::upload_part(
        &coord,
        &UploadPartRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "logs/app",
                &upload.upload_id,
                test_requester(),
                None,
            ),
            part_number: 1,
            data: b"hello multipart",
            claimed_checksum: None,
            sse_customer: None,
        },
    )
    .unwrap();

    {
        coord
            .storage_node()
            .test_mark_multipart_upload_aborting(
                &trusted_bucket_name("bucket"),
                &trusted_object_key("logs/app"),
                &upload.upload_id,
            )
            .unwrap();
    }
    delete_bucket_lifecycle_test(&coord, "bucket", test_requester(), None).unwrap();

    let stats = coord.run_lifecycle_sweep_at(0).unwrap();
    assert_eq!(stats.aborted_multipart_uploads, 1);

    assert!(!storage::test_support::multipart_upload_exists(
        &coord.storage_node(),
        &trusted_bucket_name("bucket"),
        &trusted_object_key("logs/app"),
        &upload.upload_id,
    )
    .unwrap());
}

#[test]
fn put_bucket_policy_rejects_malformed_policy() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("owner-a"),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();

    let err = put_bucket_policy_test(
        &coord,
        "bucket",
        "{",
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::MalformedPolicy { .. }));
}

#[test]
fn put_bucket_policy_rejects_public_policy_when_block_public_policy_enabled() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("owner-a"),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();
    put_bucket_public_access_block_test(&coord,
                "bucket",
                "<PublicAccessBlockConfiguration><BlockPublicAcls>false</BlockPublicAcls><IgnorePublicAcls>false</IgnorePublicAcls><BlockPublicPolicy>true</BlockPublicPolicy><RestrictPublicBuckets>false</RestrictPublicBuckets></PublicAccessBlockConfiguration>",
                test_helpers::requester("owner-a"), None)
            .unwrap();

    let err = put_bucket_policy_test(&coord,
                "bucket",
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}"#,
                test_helpers::requester("owner-a"), None)
            .unwrap_err();
    assert!(matches!(
        err,
        ServerError::BlockPublicPolicyAccessDenied { .. }
    ));
    assert_eq!(
        get_bucket_policy_test(&coord, "bucket", test_helpers::requester("owner-a"), None).unwrap(),
        None
    );
}

#[test]
fn authorize_put_bucket_policy_rejects_public_policy_when_block_public_policy_enabled() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("owner-a"),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();
    put_bucket_public_access_block_test(&coord,
                "bucket",
                "<PublicAccessBlockConfiguration><BlockPublicAcls>false</BlockPublicAcls><IgnorePublicAcls>false</IgnorePublicAcls><BlockPublicPolicy>true</BlockPublicPolicy><RestrictPublicBuckets>false</RestrictPublicBuckets></PublicAccessBlockConfiguration>",
                test_helpers::requester("owner-a"), None)
            .unwrap();

    let err = coord
            .authorize_put_bucket_policy(&put_bucket_policy_request_with_expected_owner(
                "bucket",
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}"#,
                false,
                test_helpers::requester("owner-a"),
                None,
            ))
            .unwrap_err();
    assert!(matches!(
        err,
        ServerError::BlockPublicPolicyAccessDenied { .. }
    ));
}

#[test]
fn put_bucket_policy_rejects_broad_source_ip_policy_when_block_public_policy_enabled() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("owner-a"),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();
    put_bucket_public_access_block_test(&coord,
                "bucket",
                "<PublicAccessBlockConfiguration><BlockPublicAcls>false</BlockPublicAcls><IgnorePublicAcls>false</IgnorePublicAcls><BlockPublicPolicy>true</BlockPublicPolicy><RestrictPublicBuckets>false</RestrictPublicBuckets></PublicAccessBlockConfiguration>",
                test_helpers::requester("owner-a"), None)
            .unwrap();

    let err = put_bucket_policy_test(&coord,
                "bucket",
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket","Condition":{"IpAddress":{"aws:SourceIp":"0.0.0.0/0"}}}]}"#,
                test_helpers::requester("owner-a"), None)
            .unwrap_err();
    assert!(matches!(
        err,
        ServerError::BlockPublicPolicyAccessDenied { .. }
    ));
    assert_eq!(
        get_bucket_policy_test(&coord, "bucket", test_helpers::requester("owner-a"), None).unwrap(),
        None
    );
}

#[test]
fn put_bucket_policy_rejects_source_vpc_condition_on_enforced_object_action() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("owner-a"),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();
    let policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"owner-a"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringNotEquals":{"aws:SourceVpc":"vpc-12345678"}}}]}"#;
    let err = put_bucket_policy_test(
        &coord,
        "bucket",
        policy,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::MalformedPolicy { .. }));
}

#[test]
fn put_bucket_policy_rejects_principal_arn_condition_on_enforced_object_action() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("owner-a"),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();

    let policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":{"AWS":"owner-a"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringNotEquals":{"aws:PrincipalArn":"arn:aws:iam::444455556666:user/other"}}}]}"#;
    let err = put_bucket_policy_test(
        &coord,
        "bucket",
        policy,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::MalformedPolicy { .. }));
}

#[test]
fn put_bucket_policy_rejects_list_bucket_object_only_resource() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("owner-a"),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();

    let err = put_bucket_policy_test(&coord,
                "bucket",
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket/*"}]}"#,
                test_helpers::requester("owner-a"), None)
            .unwrap_err();
    assert!(matches!(err, ServerError::MalformedPolicy { .. }));
    assert_eq!(
        get_bucket_policy_test(&coord, "bucket", test_helpers::requester("owner-a"), None).unwrap(),
        None
    );
}

#[test]
fn put_bucket_policy_allows_fixed_principal_when_block_public_policy_enabled() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("owner-a"),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();
    put_bucket_public_access_block_test(&coord,
                "bucket",
                "<PublicAccessBlockConfiguration><BlockPublicAcls>false</BlockPublicAcls><IgnorePublicAcls>false</IgnorePublicAcls><BlockPublicPolicy>true</BlockPublicPolicy><RestrictPublicBuckets>false</RestrictPublicBuckets></PublicAccessBlockConfiguration>",
                test_helpers::requester("owner-a"), None)
            .unwrap();

    let policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::123456789012:root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}"#;
    put_bucket_policy_test(
        &coord,
        "bucket",
        policy,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();

    assert_eq!(
        get_bucket_policy_test(&coord, "bucket", test_helpers::requester("owner-a"), None).unwrap(),
        Some(policy.to_string())
    );
}

#[test]
fn get_object_restrict_public_buckets_blocks_anonymous_public_policy_access() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "foo",
                test_helpers::requester("111122223333"),
                None,
            ),
            data: b"bar",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    put_bucket_policy_test(&coord,
                "bucket",
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}"#,
                test_helpers::requester("111122223333"), None)
            .unwrap();
    put_bucket_public_access_block_test(&coord,
                "bucket",
                "<PublicAccessBlockConfiguration><BlockPublicAcls>false</BlockPublicAcls><IgnorePublicAcls>false</IgnorePublicAcls><BlockPublicPolicy>false</BlockPublicPolicy><RestrictPublicBuckets>true</RestrictPublicBuckets></PublicAccessBlockConfiguration>",
                test_helpers::requester("111122223333"), None)
            .unwrap();

    let err = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "foo",
                None,
                Requester::anonymous(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));

    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "foo",
                None,
                test_helpers::requester("111122223333"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), b"bar");
}

#[test]
fn get_object_restrict_public_buckets_allows_same_account_iam_principal() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "foo",
                test_helpers::requester("111122223333"),
                None,
            ),
            data: b"bar",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    put_bucket_policy_test(&coord,
                "bucket",
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}"#,
                test_helpers::requester("111122223333"), None)
            .unwrap();
    put_bucket_public_access_block_test(&coord,
                "bucket",
                "<PublicAccessBlockConfiguration><BlockPublicAcls>false</BlockPublicAcls><IgnorePublicAcls>false</IgnorePublicAcls><BlockPublicPolicy>false</BlockPublicPolicy><RestrictPublicBuckets>true</RestrictPublicBuckets></PublicAccessBlockConfiguration>",
                test_helpers::requester("111122223333"), None)
            .unwrap();

    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "foo",
                None,
                test_helpers::requester("arn:aws:iam::111122223333:user/reader"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), b"bar");
}

#[test]
fn get_object_restrict_public_buckets_blocks_cross_account_allow_when_policy_is_public() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "foo",
                test_helpers::requester("111122223333"),
                None,
            ),
            data: b"bar",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    put_bucket_policy_test(&coord,
                "bucket",
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"},{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}"#,
                test_helpers::requester("111122223333"), None)
            .unwrap();
    put_bucket_public_access_block_test(&coord,
                "bucket",
                "<PublicAccessBlockConfiguration><BlockPublicAcls>false</BlockPublicAcls><IgnorePublicAcls>false</IgnorePublicAcls><BlockPublicPolicy>false</BlockPublicPolicy><RestrictPublicBuckets>true</RestrictPublicBuckets></PublicAccessBlockConfiguration>",
                test_helpers::requester("111122223333"), None)
            .unwrap();

    let err = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "foo",
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn get_object_restrict_public_buckets_blocks_spoofed_service_principal_name() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "foo",
                test_helpers::requester("111122223333"),
                None,
            ),
            data: b"bar",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    put_bucket_policy_test(&coord,
                "bucket",
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}"#,
                test_helpers::requester("111122223333"), None)
            .unwrap();
    put_bucket_public_access_block_test(&coord,
                "bucket",
                "<PublicAccessBlockConfiguration><BlockPublicAcls>false</BlockPublicAcls><IgnorePublicAcls>false</IgnorePublicAcls><BlockPublicPolicy>false</BlockPublicPolicy><RestrictPublicBuckets>true</RestrictPublicBuckets></PublicAccessBlockConfiguration>",
                test_helpers::requester("111122223333"), None)
            .unwrap();

    let err = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "foo",
                None,
                test_helpers::requester("evil.amazonaws.com"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn get_bucket_public_access_block_bucket_policy_deny_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_public_access_block_test(&coord,
                "bucket",
                "<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls><IgnorePublicAcls>true</IgnorePublicAcls><BlockPublicPolicy>true</BlockPublicPolicy><RestrictPublicBuckets>false</RestrictPublicBuckets></PublicAccessBlockConfiguration>",
                test_helpers::requester("111122223333"), None)
            .unwrap();
    put_bucket_policy_test(&coord,
                "bucket",
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetBucketPublicAccessBlock","Resource":"arn:aws:s3:::bucket"}]}"#,
                test_helpers::requester("111122223333"), None)
            .unwrap();

    let err = get_bucket_public_access_block_test(
        &coord,
        "bucket",
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn authorize_get_bucket_public_access_block_bucket_policy_deny_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_public_access_block_test(&coord,
                "bucket",
                "<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls><IgnorePublicAcls>true</IgnorePublicAcls><BlockPublicPolicy>true</BlockPublicPolicy><RestrictPublicBuckets>false</RestrictPublicBuckets></PublicAccessBlockConfiguration>",
                test_helpers::requester("111122223333"), None)
            .unwrap();
    put_bucket_policy_test(&coord,
                "bucket",
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetBucketPublicAccessBlock","Resource":"arn:aws:s3:::bucket"}]}"#,
                test_helpers::requester("111122223333"), None)
            .unwrap();

    let err = coord
        .authorize_get_bucket_public_access_block(&bucket_request_with_expected_owner(
            "bucket",
            test_helpers::requester("111122223333"),
            None,
        ))
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn get_bucket_public_access_block_bucket_policy_allow_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let config = "<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls><IgnorePublicAcls>true</IgnorePublicAcls><BlockPublicPolicy>true</BlockPublicPolicy><RestrictPublicBuckets>false</RestrictPublicBuckets></PublicAccessBlockConfiguration>";
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_public_access_block_test(
        &coord,
        "bucket",
        config,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    put_bucket_policy_test(
            &coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetBucketPublicAccessBlock","Resource":"arn:aws:s3:::bucket"}]}"#,
            test_helpers::requester("111122223333"),
            None,
        )
        .unwrap();

    let read_back = get_bucket_public_access_block_test(
        &coord,
        "bucket",
        test_helpers::requester("444455556666"),
        None,
    )
    .unwrap();
    assert_eq!(
        read_back,
        Some(parse_test_public_access_block_config(config))
    );
}

#[test]
fn put_and_delete_bucket_public_access_block_bucket_policy_allow_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let config = "<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls><IgnorePublicAcls>false</IgnorePublicAcls><BlockPublicPolicy>true</BlockPublicPolicy><RestrictPublicBuckets>false</RestrictPublicBuckets></PublicAccessBlockConfiguration>";
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_policy_test(
            &coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:PutBucketPublicAccessBlock","Resource":"arn:aws:s3:::bucket"}]}"#,
            test_helpers::requester("111122223333"),
            None,
        )
        .unwrap();

    put_bucket_public_access_block_test(
        &coord,
        "bucket",
        config,
        test_helpers::requester("444455556666"),
        None,
    )
    .unwrap();
    assert_eq!(
        get_bucket_public_access_block_test(
            &coord,
            "bucket",
            test_helpers::requester("111122223333"),
            None
        )
        .unwrap(),
        Some(parse_test_public_access_block_config(config))
    );

    delete_bucket_public_access_block_test(
        &coord,
        "bucket",
        test_helpers::requester("444455556666"),
        None,
    )
    .unwrap();
    assert_eq!(
        get_bucket_public_access_block_test(
            &coord,
            "bucket",
            test_helpers::requester("111122223333"),
            None
        )
        .unwrap(),
        None
    );
}

#[test]
fn get_bucket_cors_bucket_policy_allow_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let cors = "<CORSConfiguration><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>";
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_cors_test(
        &coord,
        "bucket",
        cors,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    put_bucket_policy_test(
            &coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetBucketCORS","Resource":"arn:aws:s3:::bucket"}]}"#,
            test_helpers::requester("111122223333"),
            None,
        )
        .unwrap();

    let config = get_bucket_cors_test(
        &coord,
        "bucket",
        test_helpers::requester("444455556666"),
        None,
    )
    .unwrap();
    assert_eq!(config, Some(cors.to_string()));
}

#[test]
fn authorize_get_bucket_cors_bucket_policy_allow_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_policy_test(
            &coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetBucketCORS","Resource":"arn:aws:s3:::bucket"}]}"#,
            test_helpers::requester("111122223333"),
            None,
        )
        .unwrap();

    let authorized = coord
        .authorize_get_bucket_cors(&bucket_request_with_expected_owner(
            "bucket",
            test_helpers::requester("444455556666"),
            None,
        ))
        .unwrap();
    assert_eq!(authorized.body, None);
}

#[test]
fn put_and_delete_bucket_cors_bucket_policy_allow_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let cors = "<CORSConfiguration><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>PUT</AllowedMethod></CORSRule></CORSConfiguration>";
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_policy_test(
            &coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:PutBucketCORS","Resource":"arn:aws:s3:::bucket"}]}"#,
            test_helpers::requester("111122223333"),
            None,
        )
        .unwrap();

    put_bucket_cors_test(
        &coord,
        "bucket",
        cors,
        test_helpers::requester("444455556666"),
        None,
    )
    .unwrap();
    assert_eq!(
        get_bucket_cors_test(
            &coord,
            "bucket",
            test_helpers::requester("111122223333"),
            None
        )
        .unwrap(),
        Some(cors.to_string())
    );

    delete_bucket_cors_test(
        &coord,
        "bucket",
        test_helpers::requester("444455556666"),
        None,
    )
    .unwrap();
    assert_eq!(
        get_bucket_cors_test(
            &coord,
            "bucket",
            test_helpers::requester("111122223333"),
            None
        )
        .unwrap(),
        None
    );
}

#[test]
fn get_bucket_cors_bucket_policy_deny_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let cors = "<CORSConfiguration><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>";
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_cors_test(
        &coord,
        "bucket",
        cors,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    put_bucket_policy_test(
            &coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetBucketCORS","Resource":"arn:aws:s3:::bucket"}]}"#,
            test_helpers::requester("111122223333"),
            None,
        )
        .unwrap();

    let err = get_bucket_cors_test(
        &coord,
        "bucket",
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn get_bucket_tags_bucket_policy_allow_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let tags = "<Tagging><TagSet><Tag><Key>env</Key><Value>test</Value></Tag></TagSet></Tagging>";
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_tags_test(
        &coord,
        "bucket",
        tags,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    put_bucket_policy_test(
            &coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetBucketTagging","Resource":"arn:aws:s3:::bucket"}]}"#,
            test_helpers::requester("111122223333"),
            None,
        )
        .unwrap();

    let config = get_bucket_tags_test(
        &coord,
        "bucket",
        test_helpers::requester("444455556666"),
        None,
    )
    .unwrap();
    assert_eq!(config, Some(bucket_tag_set(tags)));
}

#[test]
fn get_bucket_tags_bucket_tag_policy_denied_when_abac_disabled() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let tags =
        "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>";
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_tags_test(
        &coord,
        "bucket",
        tags,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    put_bucket_policy_test(
        &coord,
        "bucket",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetBucketTagging","Resource":"arn:aws:s3:::bucket","Condition":{"StringEquals":{"s3:BucketTag/security":"public"}}}]}"#,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();

    let err = get_bucket_tags_test(
        &coord,
        "bucket",
        test_helpers::requester("444455556666"),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn put_and_get_bucket_abac_round_trip() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();

    assert!(!get_bucket_abac_test(
        &coord,
        "bucket",
        test_helpers::requester("111122223333"),
        None
    )
    .unwrap());

    put_bucket_abac_test(
        &coord,
        "bucket",
        true,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    assert!(get_bucket_abac_test(
        &coord,
        "bucket",
        test_helpers::requester("111122223333"),
        None
    )
    .unwrap());

    put_bucket_abac_test(
        &coord,
        "bucket",
        false,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    assert!(!get_bucket_abac_test(
        &coord,
        "bucket",
        test_helpers::requester("111122223333"),
        None
    )
    .unwrap());
}

#[test]
fn bucket_tagging_rejected_when_abac_enabled() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let tags =
        "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>";
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_abac_test(
        &coord,
        "bucket",
        true,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();

    let err = put_bucket_tags_test(
        &coord,
        "bucket",
        tags,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap_err();
    match err {
        ServerError::BadRequest { reason } => assert_eq!(
            reason,
            "This S3 general purpose bucket has attribute-based access control (ABAC) enabled. To add tags to this bucket, initiate a TagResource request. To delete tags from this bucket, initiate an UntagResource request."
        ),
        other => panic!("expected BadRequest, got {other:?}"),
    }

    let err = delete_bucket_tags_test(
        &coord,
        "bucket",
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap_err();
    match err {
        ServerError::BadRequest { reason } => assert_eq!(
            reason,
            "This S3 general purpose bucket has attribute-based access control (ABAC) enabled. To delete tags from this bucket, initiate an UntagResource request."
        ),
        other => panic!("expected BadRequest, got {other:?}"),
    }
}

#[test]
fn get_bucket_policy_status_bucket_tag_policy_applies_when_abac_enabled() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let public_tags =
        "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>";
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_tags_test(
        &coord,
        "bucket",
        public_tags,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    set_bucket_abac_enabled_test(&coord, "bucket", true);
    put_bucket_policy_test(
        &coord,
        "bucket",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetBucketPolicyStatus","Resource":"arn:aws:s3:::bucket","Condition":{"StringEquals":{"s3:BucketTag/security":"public"}}}]}"#,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();

    let is_public = get_bucket_policy_status_test(
        &coord,
        "bucket",
        test_helpers::requester("444455556666"),
        None,
    )
    .unwrap();
    assert!(!is_public);

    let private_tags =
        "<Tagging><TagSet><Tag><Key>security</Key><Value>private</Value></Tag></TagSet></Tagging>";
    put_bucket_tags_for_tag_resource_test(
        &coord,
        "bucket",
        private_tags,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();

    let err = get_bucket_policy_status_test(
        &coord,
        "bucket",
        test_helpers::requester("444455556666"),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn authorize_list_bucket_bucket_tag_policy_applies_when_abac_enabled() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let public_tags =
        "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>";
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_tags_test(
        &coord,
        "bucket",
        public_tags,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    set_bucket_abac_enabled_test(&coord, "bucket", true);
    put_bucket_policy_test(
        &coord,
        "bucket",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket","Condition":{"StringEquals":{"s3:BucketTag/security":"public"}}}]}"#,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();

    coord
        .authorize_list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner(
                "bucket",
                test_helpers::requester("444455556666"),
                None,
            ),
            prefix: None,
            delimiter: None,
            continuation_token: None,
            max_keys: 1000,
            requested_max_keys: Some(1000),
        })
        .unwrap();

    let private_tags =
        "<Tagging><TagSet><Tag><Key>security</Key><Value>private</Value></Tag></TagSet></Tagging>";
    put_bucket_tags_for_tag_resource_test(
        &coord,
        "bucket",
        private_tags,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();

    let err = coord
        .authorize_list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner(
                "bucket",
                test_helpers::requester("444455556666"),
                None,
            ),
            prefix: None,
            delimiter: None,
            continuation_token: None,
            max_keys: 1000,
            requested_max_keys: Some(1000),
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn authorize_head_bucket_bucket_tag_policy_requires_list_and_location_when_abac_enabled() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let public_tags =
        "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>";
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_tags_test(
        &coord,
        "bucket",
        public_tags,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    set_bucket_abac_enabled_test(&coord, "bucket", true);
    put_bucket_policy_test(
        &coord,
        "bucket",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket","Condition":{"StringEquals":{"s3:BucketTag/security":"public"}}},{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetBucketLocation","Resource":"arn:aws:s3:::bucket","Condition":{"StringEquals":{"s3:BucketTag/security":"public"}}}]}"#,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();

    coord
        .authorize_head_bucket(&bucket_request_with_expected_owner(
            "bucket",
            test_helpers::requester("444455556666"),
            None,
        ))
        .unwrap();

    let private_tags =
        "<Tagging><TagSet><Tag><Key>security</Key><Value>private</Value></Tag></TagSet></Tagging>";
    put_bucket_tags_for_tag_resource_test(
        &coord,
        "bucket",
        private_tags,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();

    let err = coord
        .authorize_head_bucket(&bucket_request_with_expected_owner(
            "bucket",
            test_helpers::requester("444455556666"),
            None,
        ))
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn put_and_delete_bucket_tags_bucket_policy_allow_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let tags =
        "<Tagging><TagSet><Tag><Key>team</Key><Value>storage</Value></Tag></TagSet></Tagging>";
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_policy_test(
            &coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:PutBucketTagging","Resource":"arn:aws:s3:::bucket"}]}"#,
            test_helpers::requester("111122223333"),
            None,
        )
        .unwrap();

    put_bucket_tags_test(
        &coord,
        "bucket",
        tags,
        test_helpers::requester("444455556666"),
        None,
    )
    .unwrap();
    assert_eq!(
        get_bucket_tags_test(
            &coord,
            "bucket",
            test_helpers::requester("111122223333"),
            None
        )
        .unwrap(),
        Some(bucket_tag_set(tags))
    );

    delete_bucket_tags_test(
        &coord,
        "bucket",
        test_helpers::requester("444455556666"),
        None,
    )
    .unwrap();
    assert_eq!(
        get_bucket_tags_test(
            &coord,
            "bucket",
            test_helpers::requester("111122223333"),
            None
        )
        .unwrap(),
        None
    );
}

#[test]
fn get_bucket_tags_bucket_policy_deny_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let tags = "<Tagging><TagSet><Tag><Key>env</Key><Value>test</Value></Tag></TagSet></Tagging>";
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_tags_test(
        &coord,
        "bucket",
        tags,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    put_bucket_policy_test(
            &coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetBucketTagging","Resource":"arn:aws:s3:::bucket"}]}"#,
            test_helpers::requester("111122223333"),
            None,
        )
        .unwrap();

    let err = get_bucket_tags_test(
        &coord,
        "bucket",
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn get_bucket_lifecycle_bucket_policy_allow_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let lifecycle = "<LifecycleConfiguration>\
            <Rule>\
                <ID>expire</ID>\
                <Filter><Prefix>logs/</Prefix></Filter>\
                <Status>Enabled</Status>\
                <Expiration><Days>3</Days></Expiration>\
            </Rule>\
        </LifecycleConfiguration>";
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_lifecycle_test(
        &coord,
        "bucket",
        lifecycle,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    put_bucket_policy_test(
            &coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetLifecycleConfiguration","Resource":"arn:aws:s3:::bucket"}]}"#,
            test_helpers::requester("111122223333"),
            None,
        )
        .unwrap();

    let config = get_bucket_lifecycle_test(
        &coord,
        "bucket",
        test_helpers::requester("444455556666"),
        None,
    )
    .unwrap();
    assert_eq!(config, Some(lifecycle.to_string()));
}

#[test]
fn put_and_delete_bucket_lifecycle_bucket_policy_allow_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let lifecycle = "<LifecycleConfiguration>\
            <Rule>\
                <ID>expire</ID>\
                <Filter><Prefix>archive/</Prefix></Filter>\
                <Status>Enabled</Status>\
                <Expiration><Days>7</Days></Expiration>\
            </Rule>\
        </LifecycleConfiguration>";
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_policy_test(
            &coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:PutLifecycleConfiguration","Resource":"arn:aws:s3:::bucket"}]}"#,
            test_helpers::requester("111122223333"),
            None,
        )
        .unwrap();

    put_bucket_lifecycle_test(
        &coord,
        "bucket",
        lifecycle,
        test_helpers::requester("444455556666"),
        None,
    )
    .unwrap();
    assert_eq!(
        get_bucket_lifecycle_test(
            &coord,
            "bucket",
            test_helpers::requester("111122223333"),
            None
        )
        .unwrap(),
        Some(lifecycle.to_string())
    );

    delete_bucket_lifecycle_test(
        &coord,
        "bucket",
        test_helpers::requester("444455556666"),
        None,
    )
    .unwrap();
    assert_eq!(
        get_bucket_lifecycle_test(
            &coord,
            "bucket",
            test_helpers::requester("111122223333"),
            None
        )
        .unwrap(),
        None
    );
}

#[test]
fn get_bucket_lifecycle_bucket_policy_deny_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let lifecycle = "<LifecycleConfiguration>\
            <Rule>\
                <ID>expire</ID>\
                <Filter><Prefix>logs/</Prefix></Filter>\
                <Status>Enabled</Status>\
                <Expiration><Days>3</Days></Expiration>\
            </Rule>\
        </LifecycleConfiguration>";
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_lifecycle_test(
        &coord,
        "bucket",
        lifecycle,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    put_bucket_policy_test(
            &coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetLifecycleConfiguration","Resource":"arn:aws:s3:::bucket"}]}"#,
            test_helpers::requester("111122223333"),
            None,
        )
        .unwrap();

    let err = get_bucket_lifecycle_test(
        &coord,
        "bucket",
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn authorize_get_bucket_lifecycle_bucket_policy_deny_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let lifecycle = "<LifecycleConfiguration>\
            <Rule>\
                <ID>expire</ID>\
                <Filter><Prefix>logs/</Prefix></Filter>\
                <Status>Enabled</Status>\
                <Expiration><Days>3</Days></Expiration>\
            </Rule>\
        </LifecycleConfiguration>";
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_lifecycle_test(
        &coord,
        "bucket",
        lifecycle,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    put_bucket_policy_test(
            &coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetLifecycleConfiguration","Resource":"arn:aws:s3:::bucket"}]}"#,
            test_helpers::requester("111122223333"),
            None,
        )
        .unwrap();

    let err = coord
        .authorize_get_bucket_lifecycle(&bucket_request_with_expected_owner(
            "bucket",
            test_helpers::requester("111122223333"),
            None,
        ))
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn get_bucket_ownership_controls_bucket_policy_allow_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let controls = "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerPreferred</ObjectOwnership></Rule></OwnershipControls>";
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &coord,
        "bucket",
        controls,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    put_bucket_policy_test(
            &coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetBucketOwnershipControls","Resource":"arn:aws:s3:::bucket"}]}"#,
            test_helpers::requester("111122223333"),
            None,
        )
        .unwrap();

    let config = get_bucket_ownership_controls_test(
        &coord,
        "bucket",
        test_helpers::requester("444455556666"),
        None,
    )
    .unwrap();
    assert_eq!(config, Some(parse_test_ownership_controls(controls)));
}

#[test]
fn put_and_delete_bucket_ownership_controls_bucket_policy_allow_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let controls = "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerPreferred</ObjectOwnership></Rule></OwnershipControls>";
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_policy_test(
            &coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:PutBucketOwnershipControls","Resource":"arn:aws:s3:::bucket"}]}"#,
            test_helpers::requester("111122223333"),
            None,
        )
        .unwrap();

    put_bucket_ownership_controls_test(
        &coord,
        "bucket",
        controls,
        test_helpers::requester("444455556666"),
        None,
    )
    .unwrap();
    assert_eq!(
        get_bucket_ownership_controls_test(
            &coord,
            "bucket",
            test_helpers::requester("111122223333"),
            None
        )
        .unwrap(),
        Some(parse_test_ownership_controls(controls))
    );

    delete_bucket_ownership_controls_test(
        &coord,
        "bucket",
        test_helpers::requester("444455556666"),
        None,
    )
    .unwrap();
    assert_eq!(
        get_bucket_ownership_controls_test(
            &coord,
            "bucket",
            test_helpers::requester("111122223333"),
            None
        )
        .unwrap(),
        None
    );
}

#[test]
fn get_bucket_ownership_controls_bucket_policy_deny_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let controls = "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerPreferred</ObjectOwnership></Rule></OwnershipControls>";
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &coord,
        "bucket",
        controls,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    put_bucket_policy_test(
            &coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetBucketOwnershipControls","Resource":"arn:aws:s3:::bucket"}]}"#,
            test_helpers::requester("111122223333"),
            None,
        )
        .unwrap();

    let err = get_bucket_ownership_controls_test(
        &coord,
        "bucket",
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn authorize_get_bucket_ownership_controls_bucket_policy_deny_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let controls = "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerPreferred</ObjectOwnership></Rule></OwnershipControls>";
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &coord,
        "bucket",
        controls,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    put_bucket_policy_test(
            &coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetBucketOwnershipControls","Resource":"arn:aws:s3:::bucket"}]}"#,
            test_helpers::requester("111122223333"),
            None,
        )
        .unwrap();

    let err = coord
        .authorize_get_bucket_ownership_controls(&bucket_request_with_expected_owner(
            "bucket",
            test_helpers::requester("111122223333"),
            None,
        ))
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn get_bucket_encryption_bucket_policy_allow_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_encryption_test(
        &coord,
        "bucket",
        BucketEncryptionConfig {
            default_encryption: None,
            sse_c_blocked: true,
        },
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    put_bucket_policy_test(
            &coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetEncryptionConfiguration","Resource":"arn:aws:s3:::bucket"}]}"#,
            test_helpers::requester("111122223333"),
            None,
        )
        .unwrap();

    let config = get_bucket_encryption_test(
        &coord,
        "bucket",
        test_helpers::requester("444455556666"),
        None,
    )
    .unwrap();
    assert!(config.sse_c_blocked);
}

#[test]
fn put_and_delete_bucket_encryption_bucket_policy_allow_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_policy_test(
            &coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:PutEncryptionConfiguration","Resource":"arn:aws:s3:::bucket"}]}"#,
            test_helpers::requester("111122223333"),
            None,
        )
        .unwrap();

    put_bucket_encryption_test(
        &coord,
        "bucket",
        BucketEncryptionConfig {
            default_encryption: None,
            sse_c_blocked: true,
        },
        test_helpers::requester("444455556666"),
        None,
    )
    .unwrap();
    assert!(
        get_bucket_encryption_test(
            &coord,
            "bucket",
            test_helpers::requester("111122223333"),
            None
        )
        .unwrap()
        .sse_c_blocked
    );

    delete_bucket_encryption_test(
        &coord,
        "bucket",
        test_helpers::requester("444455556666"),
        None,
    )
    .unwrap();
    assert_eq!(
        get_bucket_encryption_test(
            &coord,
            "bucket",
            test_helpers::requester("111122223333"),
            None
        )
        .unwrap(),
        EffectiveBucketEncryptionConfig::default()
    );
}

#[test]
fn get_bucket_encryption_bucket_policy_deny_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_policy_test(
            &coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetEncryptionConfiguration","Resource":"arn:aws:s3:::bucket"}]}"#,
            test_helpers::requester("111122223333"),
            None,
        )
        .unwrap();

    let err = get_bucket_encryption_test(
        &coord,
        "bucket",
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn authorize_get_bucket_encryption_bucket_policy_deny_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_policy_test(
            &coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetEncryptionConfiguration","Resource":"arn:aws:s3:::bucket"}]}"#,
            test_helpers::requester("111122223333"),
            None,
        )
        .unwrap();

    let err = coord
        .authorize_get_bucket_encryption(&bucket_request_with_expected_owner(
            "bucket",
            test_helpers::requester("111122223333"),
            None,
        ))
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn get_bucket_policy_status_defaults_private() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();

    let err = get_bucket_policy_status_test(
        &coord,
        "bucket",
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::NoSuchBucketPolicy { .. }));
}

#[test]
fn get_bucket_policy_status_public_bucket_acl_without_policy_returns_no_such_bucket_policy() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner = test_helpers::requester("111122223333");
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_canned_acl_test(&coord, "bucket", BucketAcl::PublicRead, owner.clone(), None)
        .unwrap();

    let err = get_bucket_policy_status_test(&coord, "bucket", owner, None).unwrap_err();
    assert!(matches!(err, ServerError::NoSuchBucketPolicy { .. }));
}

#[test]
fn get_bucket_policy_owner_root_bypasses_explicit_deny() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let (root, root_requester, _user, user_requester) = same_account_root_and_user();

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: user_requester.clone(),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();

    let deny_policy =
        deny_bucket_policy_action_for_principal("bucket", root.principal(), "s3:GetBucketPolicy");
    put_bucket_policy_test(&coord, "bucket", &deny_policy, user_requester, None).unwrap();

    assert_eq!(
        get_bucket_policy_test(&coord, "bucket", root_requester, None).unwrap(),
        Some(deny_policy),
    );
}

#[test]
fn get_bucket_policy_explicit_deny_blocks_same_account_non_root() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let (_root, _root_requester, user, user_requester) = same_account_root_and_user();

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: user_requester.clone(),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();

    let deny_policy =
        deny_bucket_policy_action_for_principal("bucket", user.principal(), "s3:GetBucketPolicy");
    put_bucket_policy_test(&coord, "bucket", &deny_policy, user_requester.clone(), None).unwrap();

    let err = get_bucket_policy_test(&coord, "bucket", user_requester, None).unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn put_bucket_policy_owner_root_bypasses_explicit_deny() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let (root, root_requester, _user, user_requester) = same_account_root_and_user();

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: user_requester.clone(),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();

    let deny_policy =
        deny_bucket_policy_action_for_principal("bucket", root.principal(), "s3:PutBucketPolicy");
    put_bucket_policy_test(&coord, "bucket", &deny_policy, user_requester.clone(), None).unwrap();

    let replacement = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket"}]}"#;
    put_bucket_policy_test(&coord, "bucket", replacement, root_requester, None).unwrap();
    assert_eq!(
        get_bucket_policy_test(&coord, "bucket", user_requester, None).unwrap(),
        Some(replacement.to_string()),
    );
}

#[test]
fn delete_bucket_policy_owner_root_bypasses_explicit_deny() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let (root, root_requester, _user, user_requester) = same_account_root_and_user();

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: user_requester.clone(),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();

    let deny_policy = deny_bucket_policy_action_for_principal(
        "bucket",
        root.principal(),
        "s3:DeleteBucketPolicy",
    );
    put_bucket_policy_test(&coord, "bucket", &deny_policy, user_requester.clone(), None).unwrap();

    delete_bucket_policy_test(&coord, "bucket", root_requester, None).unwrap();
    assert_eq!(
        get_bucket_policy_test(&coord, "bucket", user_requester, None).unwrap(),
        None,
    );
}

#[test]
fn get_bucket_policy_status_owner_root_has_no_carveout() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let (root, root_requester, _user, user_requester) = same_account_root_and_user();

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: user_requester.clone(),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();

    let deny_policy = deny_bucket_policy_action_for_principal(
        "bucket",
        root.principal(),
        "s3:GetBucketPolicyStatus",
    );
    put_bucket_policy_test(&coord, "bucket", &deny_policy, user_requester, None).unwrap();

    let err = get_bucket_policy_status_test(&coord, "bucket", root_requester, None).unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn put_bucket_policy_owner_root_still_blocked_by_block_public_policy() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let (_root, root_requester, _user, user_requester) = same_account_root_and_user();

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: user_requester.clone(),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();
    put_bucket_public_access_block_test(&coord,
                "bucket",
                "<PublicAccessBlockConfiguration><BlockPublicAcls>false</BlockPublicAcls><IgnorePublicAcls>false</IgnorePublicAcls><BlockPublicPolicy>true</BlockPublicPolicy><RestrictPublicBuckets>false</RestrictPublicBuckets></PublicAccessBlockConfiguration>",
                user_requester, None)
            .unwrap();

    let err = put_bucket_policy_test(&coord,
                "bucket",
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}"#,
                root_requester, None)
            .unwrap_err();
    assert!(matches!(
        err,
        ServerError::BlockPublicPolicyAccessDenied { .. }
    ));
}

#[test]
fn put_bucket_policy_confirm_remove_self_bucket_access_does_not_disable_root_carveout() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let (_root, root_requester, _user, _user_requester) = same_account_root_and_user();

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: root_requester.clone(),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();

    coord.put_bucket_policy(&put_bucket_policy_request_with_expected_owner(
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":["s3:PutBucketPolicy","s3:GetBucketPolicy","s3:DeleteBucketPolicy"],"Resource":"arn:aws:s3:::bucket"}]}"#,
            true,
            root_requester.clone(),
            None,
        ))
        .unwrap();

    let fetched = get_bucket_policy_test(&coord, "bucket", root_requester.clone(), None).unwrap();
    assert!(fetched.unwrap().contains(r#""s3:GetBucketPolicy""#));

    delete_bucket_policy_test(&coord, "bucket", root_requester.clone(), None).unwrap();

    put_bucket_policy_test(
            &coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket"}]}"#,
            root_requester,
            None,
        )
        .unwrap();
}

#[test]
fn get_bucket_policy_status_reports_public_bucket_policy() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_policy_test(&coord,
                "bucket",
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket"}]}"#,
                test_helpers::requester("111122223333"), None)
            .unwrap();

    let is_public = get_bucket_policy_status_test(
        &coord,
        "bucket",
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    assert!(is_public);
}

#[test]
fn get_bucket_policy_status_reports_broad_source_ip_policy_as_public() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_policy_test(&coord,
                "bucket",
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket","Condition":{"IpAddress":{"aws:SourceIp":"0.0.0.0/0"}}}]}"#,
                test_helpers::requester("111122223333"), None)
            .unwrap();

    let is_public = get_bucket_policy_status_test(
        &coord,
        "bucket",
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    assert!(is_public);
}

#[test]
fn get_bucket_policy_status_bucket_policy_allow_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_policy_test(&coord,
                "bucket",
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"444455556666"},"Action":"s3:GetBucketPolicyStatus","Resource":"arn:aws:s3:::bucket"}]}"#,
                test_helpers::requester("111122223333"), None)
            .unwrap();

    let is_public = get_bucket_policy_status_test(
        &coord,
        "bucket",
        test_helpers::requester("444455556666"),
        None,
    )
    .unwrap();
    assert!(!is_public);
}

#[test]
fn get_bucket_policy_status_bucket_policy_deny_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_policy_test(&coord,
                "bucket",
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetBucketPolicyStatus","Resource":"arn:aws:s3:::bucket"}]}"#,
                test_helpers::requester("111122223333"), None)
            .unwrap();

    let err = get_bucket_policy_status_test(
        &coord,
        "bucket",
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn authorize_get_bucket_policy_status_bucket_policy_deny_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_policy_test(&coord,
                "bucket",
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetBucketPolicyStatus","Resource":"arn:aws:s3:::bucket"}]}"#,
                test_helpers::requester("111122223333"), None)
            .unwrap();

    let err = coord
        .authorize_get_bucket_policy_status(&bucket_request_with_expected_owner(
            "bucket",
            test_helpers::requester("111122223333"),
            None,
        ))
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn create_bucket_persists_explicit_owner_canonical_id() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner_canonical_id = CanonicalUserId::from_principal("custom-account-id");
    let owner = AccountIdentity::new("owner-a", owner_canonical_id.clone(), "Owner A");

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::authenticated(owner),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();

    let bucket = coord.unchecked_active_bucket_summary("bucket").unwrap();
    assert_eq!(bucket.owner_principal, "owner-a");
    assert_eq!(bucket.owner_canonical_id, owner_canonical_id);
}
