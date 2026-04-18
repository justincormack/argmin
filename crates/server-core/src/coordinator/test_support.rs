use super::test_helpers;
use super::*;
use crate::conditional::{DeleteCondition, ReadCondition, WriteCondition};
use crate::sse::{ManagedWrappingKeyConfig, StaticManagedKeyProvider, SSE_C_CUSTOMER_KEY_LEN};
use std::path::Path;
use std::sync::Arc;

pub(crate) const NO_READ: &ReadCondition = &ReadCondition {
    if_match: None,
    if_none_match: None,
    if_modified_since: None,
    if_unmodified_since: None,
};
pub(crate) const NO_WRITE: &WriteCondition = &WriteCondition::None;
pub(crate) const NO_DELETE: &DeleteCondition = &DeleteCondition::None;
pub(crate) const NO_PUT_OBJECT_ACL: PutObjectAcl<'static> = PutObjectAcl::None;

pub(crate) fn test_requester() -> Requester {
    test_helpers::requester("default-owner")
}

pub(crate) fn same_account_root_and_user(
) -> (AccountIdentity, Requester, AccountIdentity, Requester) {
    let canonical_id = CanonicalUserId::from_principal("111122223333");
    let root = AccountIdentity::new(
        "arn:aws:iam::111122223333:root",
        canonical_id.clone(),
        "Owner Root",
    );
    let user = AccountIdentity::new(
        "arn:aws:iam::111122223333:user/admin",
        canonical_id,
        "Owner User",
    );
    let root_requester = Requester::authenticated_owner_account_admin(root.clone());
    let user_requester = Requester::authenticated_owner_account_admin(user.clone());
    (root, root_requester, user, user_requester)
}

pub(crate) fn read_all_body(mut body: ReadHandle) -> Result<Vec<u8>, ServerError> {
    let mut out = Vec::new();
    while let Some(chunk) = body.next_chunk(INTERNAL_SEGMENT_SIZE)? {
        out.extend_from_slice(&chunk);
    }
    Ok(out)
}

pub(crate) trait ReadHandleTestExt {
    fn read_all(self) -> Result<Vec<u8>, ServerError>;
}

impl ReadHandleTestExt for ReadHandle {
    fn read_all(self) -> Result<Vec<u8>, ServerError> {
        read_all_body(self)
    }
}

pub(crate) fn compute_shard_size(size: u64, ec_k: u8) -> usize {
    let k = u64::from(ec_k);
    let padded = size.div_ceil(k) * k;
    (padded / k) as usize
}

pub(crate) fn shards_for_byte_range(
    start: usize,
    end: usize,
    shard_size: usize,
    ec_k: u8,
) -> Vec<usize> {
    if shard_size == 0 {
        return vec![];
    }
    let first = start / shard_size;
    let last = (end / shard_size).min(ec_k as usize - 1);
    (first..=last).collect()
}

pub(crate) const TEST_SSE_S3_WRAPPING_KEY_B64: &str =
    "YWJjZGVmMDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODk=";

pub(crate) fn test_sse_s3_provider() -> StaticManagedKeyProvider {
    StaticManagedKeyProvider::single(
        ManagedWrappingKeyConfig::from_base64(1, TEST_SSE_S3_WRAPPING_KEY_B64).unwrap(),
    )
}

pub(crate) fn setup_coordinator(dir: &Path) -> Coordinator {
    setup_coordinator_with_pg_count(dir, 4)
}

pub(crate) fn setup_coordinator_in_region(dir: &Path, region: &str) -> Coordinator {
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_node = Arc::new(SharedStorageNode::open(dir, &pg_ids).unwrap());
    let ec_config = EcConfig::default();
    Coordinator::new_with_managed_key_provider(
        storage_node,
        ec_config,
        region.to_string(),
        None,
        test_sse_s3_provider(),
    )
    .unwrap()
}

pub(crate) fn setup_coordinator_with_pg_count(dir: &Path, pg_count: u32) -> Coordinator {
    let pg_ids: Vec<u32> = (0..pg_count).collect();
    let storage_node = Arc::new(SharedStorageNode::open(dir, &pg_ids).unwrap());
    let ec_config = EcConfig::default();
    Coordinator::new_with_managed_key_provider(
        storage_node,
        ec_config,
        "us-east-1".to_string(),
        None,
        test_sse_s3_provider(),
    )
    .unwrap()
}

pub(crate) fn setup_coordinator_without_managed_key_provider(dir: &Path) -> Coordinator {
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_node = Arc::new(SharedStorageNode::open(dir, &pg_ids).unwrap());
    let ec_config = EcConfig::default();
    Coordinator::new(storage_node, ec_config, "us-east-1".to_string(), None).unwrap()
}

pub(crate) fn setup_coordinator_without_lifecycle_sweeper(dir: &Path) -> Coordinator {
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_node = Arc::new(SharedStorageNode::open(dir, &pg_ids).unwrap());
    let ec_config = EcConfig::default();
    Coordinator::new_with_lifecycle_sweeper_factory(
        storage_node,
        ec_config,
        "us-east-1".to_string(),
        None,
        Some(test_sse_s3_provider()),
        |_, _| Ok(LifecycleSweeper::disabled()),
    )
    .unwrap()
}

pub(crate) fn setup_coordinator_with_shared_storage(
    storage_node: Arc<SharedStorageNode>,
) -> Coordinator {
    let ec_config = EcConfig::default();
    Coordinator::new_with_managed_key_provider(
        storage_node,
        ec_config,
        "us-east-1".to_string(),
        None,
        test_sse_s3_provider(),
    )
    .unwrap()
}

pub(crate) fn setup_coordinators_with_pg_count(
    dir: &Path,
    pg_count: u32,
) -> (Coordinator, Coordinator) {
    let pg_ids: Vec<u32> = (0..pg_count).collect();
    let storage_node = Arc::new(SharedStorageNode::open(dir, &pg_ids).unwrap());
    (
        setup_coordinator_with_shared_storage(Arc::clone(&storage_node)),
        setup_coordinator_with_shared_storage(storage_node),
    )
}

pub(crate) fn setup_coordinators_with_single_pg(dir: &Path) -> (Coordinator, Coordinator) {
    setup_coordinators_with_pg_count(dir, 1)
}

pub(crate) fn setup_coordinator_with_sse_c(dir: &Path) -> Coordinator {
    use base64::Engine;

    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_node = Arc::new(SharedStorageNode::open(dir, &pg_ids).unwrap());
    let ec_config = EcConfig::default();
    let validator = SseCustomerValidatorConfig::from_base64(
        1,
        &base64::engine::general_purpose::STANDARD.encode([9u8; 32]),
    )
    .unwrap();
    Coordinator::new_with_managed_key_provider(
        storage_node,
        ec_config,
        "us-east-1".to_string(),
        Some(validator),
        test_sse_s3_provider(),
    )
    .unwrap()
}

pub(crate) fn backend_supports_parity_recovery() -> bool {
    EcConfig::default().parity_shards > 0
}

pub(crate) fn test_sse_customer_request() -> SseCustomerRequest {
    SseCustomerRequest::new([7u8; SSE_C_CUSTOMER_KEY_LEN], "dummy-md5".to_string())
}

pub(crate) fn object_request<'a>(
    bucket: &'a str,
    key: &'a str,
    requester: Requester,
) -> ObjectRequest<'a> {
    object_request_with_expected_owner(bucket, key, requester, None)
}

pub(crate) fn object_request_with_expected_owner<'a>(
    bucket: &'a str,
    key: &'a str,
    requester: Requester,
    expected_bucket_owner: Option<&'a str>,
) -> ObjectRequest<'a> {
    ObjectRequest::new(
        trusted_bucket_name(bucket),
        trusted_object_key(key),
        requester,
        expected_bucket_owner,
    )
}

pub(crate) fn object_version_request<'a>(
    bucket: &'a str,
    key: &'a str,
    version_id: Option<VersionId>,
    requester: Requester,
) -> ObjectVersionRequest<'a> {
    object_version_request_with_expected_owner(bucket, key, version_id, requester, None)
}

pub(crate) fn object_version_request_with_expected_owner<'a>(
    bucket: &'a str,
    key: &'a str,
    version_id: Option<VersionId>,
    requester: Requester,
    expected_bucket_owner: Option<&'a str>,
) -> ObjectVersionRequest<'a> {
    ObjectVersionRequest::new(
        trusted_bucket_name(bucket),
        trusted_object_key(key),
        version_id,
        requester,
        expected_bucket_owner,
    )
}

pub(crate) fn begin_stream_put_test(
    coord: &Coordinator,
    bucket: &str,
    key: &str,
) -> Result<SessionId, ServerError> {
    begin_stream_put_with_authorized_request_test(
        coord,
        object_request(bucket, key, test_requester()),
        NO_PUT_OBJECT_ACL.into(),
        PutObjectPolicyContext::default(),
        WriteEncryptionRequest::none(),
        ObjectLockState::default(),
    )
}

pub(crate) fn begin_stream_put_with_authorized_request_test<'a>(
    coord: &Coordinator,
    object: ObjectRequest<'a>,
    acl: PutObjectWriteAcl<'a>,
    policy_context: PutObjectPolicyContext<'a>,
    encryption: WriteEncryptionRequest<'a>,
    object_lock: ObjectLockState,
) -> Result<SessionId, ServerError> {
    let authorized = coord.authorize_put_object_write(&AuthorizePutObjectRequest {
        object: ObjectRequest::new(
            object.bucket.name_typed().clone(),
            object.key_typed().clone(),
            object.requester().clone(),
            object.expected_bucket_owner(),
        ),
        acl: acl.clone(),
        policy_context: encryption.with_policy_context(
            policy_context.with_default_canned_acl(acl.policy_condition_value()),
        ),
        object_lock,
        tags: None,
        encryption,
    })?;
    coord.begin_stream_put_session(&authorized)
}

pub(crate) fn delete_bucket_test(coord: &Coordinator, name: &str) -> Result<(), ServerError> {
    coord.delete_bucket(&bucket_request_with_expected_owner(
        name,
        test_requester(),
        None,
    ))
}

pub(crate) fn bucket_request_with_expected_owner<'a>(
    name: &'a str,
    requester: Requester,
    expected_bucket_owner: Option<&'a str>,
) -> BucketRequest<'a> {
    BucketRequest::new(trusted_bucket_name(name), requester, expected_bucket_owner)
}

pub(crate) fn put_bucket_config_request_with_expected_owner<'a>(
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

pub(crate) fn put_bucket_policy_request_with_expected_owner<'a>(
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

pub(crate) trait MultipartUploadIdArg {
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

pub(crate) fn multipart_object_request<'a, I: MultipartUploadIdArg>(
    bucket: &'a str,
    key: &'a str,
    upload_id: I,
    requester: Requester,
) -> MultipartObjectRequest<'a> {
    multipart_object_request_with_expected_owner(bucket, key, upload_id, requester, None)
}

pub(crate) fn multipart_object_request_with_expected_owner<'a, I: MultipartUploadIdArg>(
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

pub(crate) fn copy_source<'a>(
    bucket: &'a str,
    key: &'a str,
    version_id: Option<VersionId>,
) -> CopySource<'a> {
    copy_source_with_condition_and_expected_owner(bucket, key, version_id, NO_READ, None)
}

pub(crate) fn copy_source_with_condition_and_expected_owner<'a>(
    bucket: &'a str,
    key: &'a str,
    version_id: Option<VersionId>,
    condition: &'a ReadCondition,
    expected_bucket_owner: Option<&'a str>,
) -> CopySource<'a> {
    CopySource {
        bucket: trusted_bucket_name(bucket),
        key: trusted_object_key(key),
        version_id,
        condition,
        expected_bucket_owner,
    }
}

pub(crate) fn delete_object_request<'a>(
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

pub(crate) fn delete_object_request_with_expected_owner<'a>(
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

pub(crate) fn put_bucket_versioning_test(
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

pub(crate) fn get_bucket_versioning_test(
    coord: &Coordinator,
    name: &str,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<BucketVersioningState, ServerError> {
    coord.get_bucket_versioning(&bucket_request_with_expected_owner(
        name,
        requester,
        expected_bucket_owner,
    ))
}

pub(crate) fn put_bucket_object_lock_configuration_test(
    coord: &Coordinator,
    name: &str,
    config: BucketObjectLockConfigurationUpdate,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<(), ServerError> {
    coord.put_bucket_object_lock_configuration(&PutBucketObjectLockConfigurationRequest {
        bucket: bucket_request_with_expected_owner(name, requester, expected_bucket_owner),
        config,
    })
}

pub(crate) fn get_bucket_object_lock_configuration_test(
    coord: &Coordinator,
    name: &str,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<BucketObjectLockConfig, ServerError> {
    coord.get_bucket_object_lock_configuration(&bucket_request_with_expected_owner(
        name,
        requester,
        expected_bucket_owner,
    ))
}

pub(crate) fn put_bucket_encryption_test(
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

pub(crate) fn get_bucket_encryption_test(
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

pub(crate) fn put_bucket_policy_test(
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

pub(crate) fn put_bucket_lifecycle_test(
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

pub(crate) fn parse_test_public_access_block_config(config: &str) -> PublicAccessBlockConfig {
    PublicAccessBlockConfig {
        block_public_acls: config.contains("<BlockPublicAcls>true</BlockPublicAcls>"),
        ignore_public_acls: config.contains("<IgnorePublicAcls>true</IgnorePublicAcls>"),
        block_public_policy: config.contains("<BlockPublicPolicy>true</BlockPublicPolicy>"),
        restrict_public_buckets: config
            .contains("<RestrictPublicBuckets>true</RestrictPublicBuckets>"),
    }
}

pub(crate) fn put_bucket_public_access_block_test(
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

pub(crate) fn parse_test_ownership_controls(config: &str) -> BucketOwnershipControls {
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

pub(crate) fn put_bucket_ownership_controls_test(
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

pub(crate) fn get_bucket_ownership_controls_test(
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

pub(crate) fn put_bucket_acl_test(
    coord: &Coordinator,
    name: &str,
    acl_grants: AclGrants,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<(), ServerError> {
    coord.put_bucket_acl(&PutBucketAclRequest {
        bucket: bucket_request_with_expected_owner(name, requester, expected_bucket_owner),
        acl: PutBucketAclInput::Grants(acl_grants),
        policy_context: PutObjectPolicyContext::default(),
    })
}

pub(crate) fn put_bucket_canned_acl_test(
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

pub(crate) fn get_bucket_acl_test(
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

pub(crate) fn put_object_tags_test(
    coord: &Coordinator,
    bucket: &str,
    key: &str,
    version_id: Option<VersionId>,
    tags: &str,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<(), ServerError> {
    coord.put_object_tags(&PutObjectTagsRequest {
        object: object_version_request_with_expected_owner(
            bucket,
            key,
            version_id,
            requester,
            expected_bucket_owner,
        ),
        tags,
    })
}

pub(crate) fn get_object_tags_test(
    coord: &Coordinator,
    bucket: &str,
    key: &str,
    version_id: Option<VersionId>,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<Option<String>, ServerError> {
    coord.get_object_tags(&object_version_request_with_expected_owner(
        bucket,
        key,
        version_id,
        requester,
        expected_bucket_owner,
    ))
}

pub(crate) fn put_object_retention_test(
    coord: &Coordinator,
    bucket: &str,
    key: &str,
    version_id: Option<VersionId>,
    retention: ObjectRetention,
    bypass_governance: bool,
    requester: Requester,
) -> Result<(), ServerError> {
    coord.put_object_retention(&PutObjectRetentionRequest {
        object: object_version_request(bucket, key, version_id, requester),
        retention,
        bypass_governance,
    })
}

pub(crate) fn get_object_retention_test(
    coord: &Coordinator,
    bucket: &str,
    key: &str,
    version_id: Option<VersionId>,
    requester: Requester,
) -> Result<Option<ObjectRetention>, ServerError> {
    coord.get_object_retention(&object_version_request(bucket, key, version_id, requester))
}

pub(crate) fn put_object_legal_hold_test(
    coord: &Coordinator,
    bucket: &str,
    key: &str,
    version_id: Option<VersionId>,
    legal_hold: LegalHoldStatus,
    requester: Requester,
) -> Result<(), ServerError> {
    coord.put_object_legal_hold(&PutObjectLegalHoldRequest {
        object: object_version_request(bucket, key, version_id, requester),
        legal_hold,
    })
}

pub(crate) fn get_object_legal_hold_test(
    coord: &Coordinator,
    bucket: &str,
    key: &str,
    version_id: Option<VersionId>,
    requester: Requester,
) -> Result<Option<LegalHoldStatus>, ServerError> {
    coord.get_object_legal_hold(&object_version_request(bucket, key, version_id, requester))
}

pub(crate) fn put_object_acl_test(
    coord: &Coordinator,
    bucket: &str,
    key: &str,
    version_id: Option<VersionId>,
    acl_grants: AclGrants,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<VersionId, ServerError> {
    coord.put_object_acl(&PutObjectAclRequest {
        object: object_version_request_with_expected_owner(
            bucket,
            key,
            version_id,
            requester,
            expected_bucket_owner,
        ),
        acl: PutObjectAclInput::Grants(acl_grants),
        policy_context: PutObjectPolicyContext::default(),
    })
}

pub(crate) fn put_object_canned_acl_test(
    coord: &Coordinator,
    bucket: &str,
    key: &str,
    version_id: Option<VersionId>,
    acl: PutObjectAcl<'_>,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<VersionId, ServerError> {
    coord.put_object_acl(&PutObjectAclRequest {
        object: object_version_request_with_expected_owner(
            bucket,
            key,
            version_id,
            requester,
            expected_bucket_owner,
        ),
        acl: PutObjectAclInput::Canned(acl),
        policy_context: PutObjectPolicyContext::default()
            .with_default_canned_acl(acl.policy_condition_value()),
    })
}

pub(crate) fn get_object_acl_test(
    coord: &Coordinator,
    bucket: &str,
    key: &str,
    version_id: Option<VersionId>,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<GetObjectAclResult, ServerError> {
    coord.get_object_acl(&object_version_request_with_expected_owner(
        bucket,
        key,
        version_id,
        requester,
        expected_bucket_owner,
    ))
}

pub(crate) fn create_bucket_for_owner_with_flags(
    coord: &Coordinator,
    owner_principal: &str,
    owner_canonical_id: &CanonicalUserId,
    name: &str,
    public_read: bool,
    public_write: bool,
    object_lock_enabled: bool,
) -> Result<(), ServerError> {
    let owner = OwnerIdentity::new(owner_principal, owner_canonical_id.clone());
    let acl_grants = Coordinator::bucket_acl_grants_from_flags(&owner, public_read, public_write);
    coord.create_bucket_with_acl_grants(&owner, name, acl_grants, object_lock_enabled)?;
    Ok(())
}

pub(crate) fn grants_contain(
    acl_grants: &AclGrants,
    grantee: &AclGrantee,
    permission: AclPermission,
) -> bool {
    acl_grants
        .iter()
        .any(|grant| grant.grantee() == grantee && grant.permission() == permission)
}
