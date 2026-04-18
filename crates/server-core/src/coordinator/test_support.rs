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
