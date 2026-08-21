// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::test_helpers;
use super::*;
use crate::conditional::{DeleteCondition, ReadCondition, WriteCondition};
use crate::sse::{ManagedWrappingKeyConfig, StaticManagedKeyProvider, SSE_C_CUSTOMER_KEY_LEN};
use ec::EcConfig;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use storage::test_support::{
    StorageClusterLifecycleTestSupport as _, StorageReclaimSweeperTestSupport as _,
};
use storage::{
    NodeId, StorageCluster, StorageClusterRouteHandle, StorageClusterRuntimeMapHandle,
    StorageClusterRuntimeMapRefreshError,
};

fn test_support_storage_route_handle(
    storage_cluster: Arc<StorageCluster>,
) -> StorageClusterRouteHandle {
    match StorageClusterRuntimeMapHandle::new(Arc::clone(&storage_cluster)) {
        Ok(runtime) => runtime.route_handle(),
        Err(StorageClusterRuntimeMapRefreshError::StaticRouteAuthorityRefresh) => {
            StorageClusterRouteHandle::from_static_cluster(storage_cluster).unwrap()
        }
        Err(error) => panic!("invalid test storage cluster route authority: {error}"),
    }
}

pub(crate) const NO_READ: &ReadCondition = &ReadCondition {
    if_match: None,
    if_none_match: None,
    if_modified_since: None,
    if_unmodified_since: None,
};
pub(crate) const NO_WRITE: &WriteCondition = &WriteCondition::None;
pub(crate) const NO_DELETE: &DeleteCondition = &DeleteCondition::None;
pub(crate) const NO_PUT_OBJECT_ACL: PutObjectAcl<'static> = PutObjectAcl::None;

pub(crate) fn server_error_is_retryable_operation_contention(error: &ServerError) -> bool {
    matches!(error, ServerError::OperationAborted | ServerError::SlowDown)
}

#[test]
fn operation_contention_test_retries_include_slow_down_without_broadening_conflicts() {
    assert!(server_error_is_retryable_operation_contention(
        &ServerError::OperationAborted
    ));
    assert!(server_error_is_retryable_operation_contention(
        &ServerError::SlowDown
    ));
    assert!(!server_error_is_retryable_operation_contention(
        &ServerError::InvalidBucketState
    ));
}

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
pub(crate) const DEFAULT_TEST_PG_COUNT: u32 = 1;
pub(crate) const METADATA_FANOUT_TEST_PG_COUNT: u32 = 2;

pub(crate) fn test_sse_s3_provider() -> StaticManagedKeyProvider {
    StaticManagedKeyProvider::single(
        ManagedWrappingKeyConfig::from_base64(1, TEST_SSE_S3_WRAPPING_KEY_B64).unwrap(),
    )
}

pub(crate) fn setup_coordinator(dir: &Path) -> Coordinator {
    setup_coordinator_with_pg_count(dir, DEFAULT_TEST_PG_COUNT)
}

pub(crate) fn setup_coordinator_without_reclaim_sweeper(dir: &Path) -> Coordinator {
    let mut coord = setup_coordinator(dir);
    stop_reclaim_sweeper_for_test(&mut coord);
    coord
}

fn stop_reclaim_sweeper_for_test(coord: &mut Coordinator) {
    coord._reclaim_sweeper.test_stop();
}

pub(crate) fn setup_coordinator_in_region(dir: &Path, region: &str) -> Coordinator {
    let pg_ids: Vec<u32> = (0..DEFAULT_TEST_PG_COUNT).collect();
    let storage_cluster = open_test_storage_cluster(dir, &pg_ids);
    Coordinator::new_with_managed_key_provider_for_storage_cluster(
        storage_cluster,
        region.to_string(),
        None,
        test_sse_s3_provider(),
    )
    .unwrap()
}

pub(crate) fn setup_coordinator_with_pg_count(dir: &Path, pg_count: u32) -> Coordinator {
    let pg_ids: Vec<u32> = (0..pg_count).collect();
    let storage_cluster = open_test_storage_cluster(dir, &pg_ids);
    Coordinator::new_with_managed_key_provider_for_storage_cluster(
        storage_cluster,
        "us-east-1".to_string(),
        None,
        test_sse_s3_provider(),
    )
    .unwrap()
}

pub(crate) fn setup_coordinator_with_pg_count_without_background_sweepers(
    dir: &Path,
    pg_count: u32,
) -> Coordinator {
    let pg_ids: Vec<u32> = (0..pg_count).collect();
    let storage_cluster = open_test_storage_cluster(dir, &pg_ids);
    Coordinator::new_with_background_sweeper_factories_for_storage_cluster(
        storage_cluster,
        "us-east-1".to_string(),
        None,
        Some(test_sse_s3_provider()),
        (
            false,
            |_, _| Ok(LifecycleSweeper::disabled()),
            |storage_handle| Ok(ShardScavengerSweeper::disabled(storage_handle.clone())),
            |storage_handle| Ok(ShardRepairSweeper::disabled(storage_handle.clone())),
            |storage_handle| Ok(ShardBackfillSweeper::disabled(storage_handle.clone())),
            |storage_handle| Ok(StreamSessionSweeper::disabled(storage_handle.clone())),
        ),
    )
    .unwrap()
}

pub(crate) fn setup_coordinator_without_managed_key_provider(dir: &Path) -> Coordinator {
    let pg_ids: Vec<u32> = (0..DEFAULT_TEST_PG_COUNT).collect();
    let storage_cluster = open_test_storage_cluster(dir, &pg_ids);
    Coordinator::new_with_storage_cluster(storage_cluster, "us-east-1".to_string(), None).unwrap()
}

pub(crate) fn setup_coordinator_without_lifecycle_sweeper(dir: &Path) -> Coordinator {
    let pg_ids: Vec<u32> = (0..DEFAULT_TEST_PG_COUNT).collect();
    let storage_cluster = open_test_storage_cluster(dir, &pg_ids);
    Coordinator::new_with_lifecycle_sweeper_factory_for_storage_cluster(
        storage_cluster,
        "us-east-1".to_string(),
        None,
        Some(test_sse_s3_provider()),
        |_, _| Ok(LifecycleSweeper::disabled()),
    )
    .unwrap()
}

/// Build a same-process frontend over an existing storage cluster.
///
/// Coordinators created this way share the process-local cache registry for
/// that storage cluster. Use
/// `setup_process_isolated_cache_coordinator_with_storage_cluster` when a test
/// needs process-shaped cache isolation.
pub(crate) fn setup_same_process_coordinator_with_storage_cluster(
    storage_cluster: Arc<StorageCluster>,
) -> Coordinator {
    let shared_caches = shared_caches_for_storage_cluster(&storage_cluster);
    Coordinator::new_with_shared_caches_and_lifecycle_sweeper_factory(
        storage_cluster,
        shared_caches,
        "us-east-1".to_string(),
        None,
        Some(test_sse_s3_provider()),
        LifecycleSweeper::acquire_shared,
    )
    .unwrap()
}

/// Build a second same-process frontend over an existing storage cluster.
///
/// Coordinators created this way share the process-local cache registry for
/// that storage cluster. Use
/// `setup_process_isolated_cache_coordinator_with_storage_cluster` when a test
/// needs process-shaped cache isolation.
pub(crate) fn setup_same_process_coordinator_with_storage_cluster_without_lifecycle_sweeper(
    storage_cluster: Arc<StorageCluster>,
) -> Coordinator {
    let shared_caches = shared_caches_for_storage_cluster(&storage_cluster);
    Coordinator::new_with_shared_caches_and_lifecycle_sweeper_factory(
        storage_cluster,
        shared_caches,
        "us-east-1".to_string(),
        None,
        Some(test_sse_s3_provider()),
        |_, _| Ok(LifecycleSweeper::disabled()),
    )
    .unwrap()
}

pub(crate) fn setup_same_process_coordinator_with_storage_cluster_without_background_sweepers(
    storage_cluster: Arc<StorageCluster>,
) -> Coordinator {
    let shared_caches = shared_caches_for_storage_cluster(&storage_cluster);
    Coordinator::new_with_shared_caches_and_background_sweeper_factories(
        test_support_storage_route_handle(Arc::clone(&storage_cluster)),
        storage_cluster,
        shared_caches,
        "us-east-1".to_string(),
        None,
        Some(test_sse_s3_provider()),
        (
            false,
            |_, _| Ok(LifecycleSweeper::disabled()),
            |storage_handle| Ok(ShardScavengerSweeper::disabled(storage_handle.clone())),
            |storage_handle| Ok(ShardRepairSweeper::disabled(storage_handle.clone())),
            |storage_handle| Ok(ShardBackfillSweeper::disabled(storage_handle.clone())),
            |storage_handle| Ok(StreamSessionSweeper::disabled(storage_handle.clone())),
        ),
    )
    .unwrap()
}

/// Build a coordinator with a fresh cache domain over an existing storage
/// cluster.
///
/// This models watcher-disabled separate-process cache state for tests where
/// request-time cache freshness is the invariant under test. It still shares the
/// in-process storage cluster handle, so use it only when shared storage is
/// intentional and the reader cache must be isolated from writer-side hints.
pub(crate) fn setup_process_isolated_cache_coordinator_with_storage_cluster(
    storage_cluster: Arc<StorageCluster>,
) -> Coordinator {
    Coordinator::new_with_shared_caches_and_lifecycle_sweeper_factory(
        storage_cluster,
        Arc::new(CoordinatorSharedCaches::default()),
        "us-east-1".to_string(),
        None,
        Some(test_sse_s3_provider()),
        |_, _| Ok(LifecycleSweeper::disabled()),
    )
    .unwrap()
}

pub(crate) fn setup_same_process_coordinators_with_pg_count(
    dir: &Path,
    pg_count: u32,
) -> (Coordinator, Coordinator) {
    let pg_ids: Vec<u32> = (0..pg_count).collect();
    let storage_cluster = open_test_storage_cluster(dir, &pg_ids);
    (
        setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster)),
        setup_same_process_coordinator_with_storage_cluster(storage_cluster),
    )
}

pub(crate) fn setup_same_process_coordinators_with_pg_count_without_lifecycle_sweeper(
    dir: &Path,
    pg_count: u32,
) -> (Coordinator, Coordinator) {
    let pg_ids: Vec<u32> = (0..pg_count).collect();
    let storage_cluster = open_test_storage_cluster(dir, &pg_ids);
    (
        setup_same_process_coordinator_with_storage_cluster_without_lifecycle_sweeper(Arc::clone(
            &storage_cluster,
        )),
        setup_same_process_coordinator_with_storage_cluster_without_lifecycle_sweeper(
            storage_cluster,
        ),
    )
}

pub(crate) fn setup_same_process_coordinators_with_single_pg_without_lifecycle_sweeper(
    dir: &Path,
) -> (Coordinator, Coordinator) {
    setup_same_process_coordinators_with_pg_count_without_lifecycle_sweeper(dir, 1)
}

pub(crate) fn setup_same_process_coordinators_with_pg_count_without_background_sweepers(
    dir: &Path,
    pg_count: u32,
) -> (Coordinator, Coordinator) {
    let pg_ids: Vec<u32> = (0..pg_count).collect();
    let storage_cluster = open_test_storage_cluster(dir, &pg_ids);
    (
        setup_same_process_coordinator_with_storage_cluster_without_background_sweepers(
            Arc::clone(&storage_cluster),
        ),
        setup_same_process_coordinator_with_storage_cluster_without_background_sweepers(
            storage_cluster,
        ),
    )
}

pub(crate) fn setup_same_process_coordinators_with_single_pg_without_background_sweepers(
    dir: &Path,
) -> (Coordinator, Coordinator) {
    setup_same_process_coordinators_with_pg_count_without_background_sweepers(dir, 1)
}

pub(crate) fn setup_coordinator_with_sse_c(dir: &Path) -> Coordinator {
    use base64::Engine;

    let validator = SseCustomerValidatorConfig::from_base64(
        1,
        &base64::engine::general_purpose::STANDARD.encode([9u8; 32]),
    )
    .unwrap();
    setup_coordinator_with_sse_c_validator(dir, validator)
}

pub(crate) fn setup_coordinator_with_sse_c_validator(
    dir: &Path,
    validator: SseCustomerValidatorConfig,
) -> Coordinator {
    let pg_ids: Vec<u32> = (0..DEFAULT_TEST_PG_COUNT).collect();
    let storage_cluster = open_test_storage_cluster(dir, &pg_ids);
    Coordinator::new_with_managed_key_provider_for_storage_cluster(
        storage_cluster,
        "us-east-1".to_string(),
        Some(validator),
        test_sse_s3_provider(),
    )
    .unwrap()
}

pub(crate) fn open_test_storage_cluster(dir: &Path, pg_ids: &[u32]) -> Arc<StorageCluster> {
    let ec_config = EcConfig::default();
    open_test_storage_cluster_with_ec_shape(
        dir,
        pg_ids,
        storage::EcShape {
            k: ec_config.data_shards(),
            m: ec_config.parity_shards(),
        },
    )
}

pub(crate) fn open_test_storage_cluster_with_ec_shape(
    dir: &Path,
    pg_ids: &[u32],
    ec_shape: storage::EcShape,
) -> Arc<StorageCluster> {
    let node_count = u32::from(ec_shape.k) + u32::from(ec_shape.m);
    let node_ids: Vec<NodeId> = (0..node_count).map(NodeId::new).collect();
    StorageCluster::open_static_local_nodes(dir, &node_ids, pg_ids, ec_shape)
        .expect("open local storage cluster")
}

pub(crate) fn backend_supports_parity_recovery() -> bool {
    EcConfig::default().parity_shards() > 0
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
    Ok(coord
        .begin_stream_put(&AuthorizePutObjectRequest {
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
        })?
        .session_id)
}

pub(crate) fn wait_until_bucket_gone(coord: &Coordinator, name: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        coord
            .read_runtime()
            .try_finalize_bucket_delete_for(&trusted_bucket_name(name))
            .unwrap();
        if matches!(
            coord.unchecked_active_bucket_summary(name),
            Err(ServerError::BucketNotFound { .. })
        ) && coord
            .storage_node()
            .test_bucket_presence(&trusted_bucket_name(name))
            .is_ok_and(|presence| presence == storage::test_support::TestBucketPresence::Missing)
        {
            return;
        }
        if Instant::now() >= deadline {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("bucket {name} was not fully removed");
}

pub(crate) fn reclaim_object_payload(
    coord: &Coordinator,
    subject: &storage::test_support::TestObjectPayloadReclaimSubject,
) {
    coord
        .read_runtime()
        .try_reclaim_object_payload(subject)
        .unwrap();
}

pub(crate) fn begin_stream_part_test<I: MultipartUploadIdArg>(
    coord: &Coordinator,
    bucket: &str,
    key: &str,
    upload_id: I,
    part_number: u32,
) -> Result<BeginStreamPartResult, ServerError> {
    coord.begin_stream_part(&BeginStreamPartRequest {
        upload: multipart_object_request(bucket, key, upload_id, test_requester()),
        part_number,
        policy_context: PutObjectPolicyContext::default(),
        sse_customer: None,
    })
}

pub(crate) fn create_upload_with_parts(
    coord: &Coordinator,
    bucket: &str,
    key: &str,
    part_data: &[(u32, &[u8])],
) -> (UploadId, Vec<CompletePart>) {
    let metadata = MetadataBlob::new();
    let create = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request(bucket, key, test_requester()),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();
    let mut complete_parts = Vec::new();
    for &(part_number, data) in part_data {
        let result = test_helpers::upload_part(
            coord,
            &test_helpers::UploadPartRequest {
                upload: multipart_object_request(bucket, key, &create.upload_id, test_requester()),
                part_number,
                data,
                claimed_checksum: None,
                sse_customer: None,
            },
        )
        .unwrap();
        complete_parts.push(CompletePart {
            part_number,
            etag: result.etag,
            checksum: None,
        });
    }
    (create.upload_id, complete_parts)
}

pub(crate) const MIN_PART: usize = 5 * 1024 * 1024;

fn make_part(fill: u8, size: usize) -> Vec<u8> {
    vec![fill; size]
}

pub(crate) fn create_completed_multipart_with_streamed_tail(
    coord: &Coordinator,
    bucket: &str,
    key: &str,
) -> (CompleteMultipartUploadResult, Vec<u8>) {
    let metadata = MetadataBlob::new();
    let create = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request(bucket, key, test_requester()),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let part1 = make_part(0xAA, MIN_PART);
    let part1_result = test_helpers::upload_part(
        coord,
        &test_helpers::UploadPartRequest {
            upload: multipart_object_request(bucket, key, &create.upload_id, test_requester()),
            part_number: 1,
            data: &part1,
            claimed_checksum: None,
            sse_customer: None,
        },
    )
    .unwrap();

    let session = begin_stream_part_test(coord, bucket, key, &create.upload_id, 2).unwrap();
    let part2 = b"streamed-tail-data".to_vec();
    coord
        .append_plaintext_stream_segment_for_test(bucket, key, &session.session_id, 0, &part2)
        .unwrap();
    let part2_crc = checksum::crc64::checksum(&part2);
    let part2_result = coord
        .finalize_stream_part(FinalizeStreamPartRequest {
            upload: multipart_object_request(bucket, key, &create.upload_id, test_requester()),
            session_id: &session.session_id,
            part_number: 2,
            crc64: part2_crc,
            total_size: part2.len() as u64,
            claimed_checksum: None,
            computed_checksum: None,
        })
        .unwrap();

    let complete = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request(bucket, key, &create.upload_id, test_requester()),
            parts: &[
                CompletePart {
                    part_number: 1,
                    etag: part1_result.etag,
                    checksum: None,
                },
                CompletePart {
                    part_number: 2,
                    etag: part2_result.etag,
                    checksum: None,
                },
            ],
            sse_customer: None,
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
        })
        .unwrap();

    let expected = [part1, part2].concat();
    (complete, expected)
}

pub(crate) fn delete_bucket_test(coord: &Coordinator, name: &str) -> Result<(), ServerError> {
    coord.delete_bucket(&bucket_request_with_expected_owner(
        name,
        test_requester(),
        None,
    ))
}

pub(crate) fn delete_bucket_eventually_test(
    coord: &Coordinator,
    name: &str,
) -> Result<(), ServerError> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match delete_bucket_test(coord, name) {
            Ok(()) => return Ok(()),
            Err(error)
                if server_error_is_retryable_operation_contention(&error)
                    && Instant::now() < deadline =>
            {
                let _ = coord
                    .read_runtime()
                    .try_finalize_bucket_delete_for(&trusted_bucket_name(name));
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(err) => return Err(err),
        }
    }
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
    CopySource::new(
        trusted_bucket_name(bucket),
        trusted_object_key(key),
        version_id,
        condition,
        expected_bucket_owner,
    )
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

pub(crate) fn enable_bucket_sse_c_test(
    coord: &Coordinator,
    name: &str,
    requester: Requester,
    expected_bucket_owner: Option<&str>,
) -> Result<(), ServerError> {
    put_bucket_encryption_test(
        coord,
        name,
        BucketEncryptionConfig {
            default_encryption: Some(ManagedEncryptionAlgorithm::Aes256),
            sse_c_blocked: false,
        },
        requester,
        expected_bucket_owner,
    )
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
    let tags = object_tag_set(tags);
    coord.put_object_tags(&PutObjectTagsRequest {
        object: object_version_request_with_expected_owner(
            bucket,
            key,
            version_id,
            requester,
            expected_bucket_owner,
        ),
        tags: &tags,
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
    coord
        .get_object_tags(&object_version_request_with_expected_owner(
            bucket,
            key,
            version_id,
            requester,
            expected_bucket_owner,
        ))
        .map(|tags| tags.map(|tags| tags.to_xml()))
}

pub(crate) fn object_tag_set(xml: &str) -> s3_types::TagSet {
    s3_types::TagSet::parse_canonical_xml(xml, s3_types::MAX_OBJECT_TAGS)
        .expect("server-core tests must use valid object tags")
}

pub(crate) fn bucket_tag_set(xml: &str) -> s3_types::TagSet {
    s3_types::TagSet::parse_canonical_xml(xml, s3_types::MAX_BUCKET_TAGS)
        .expect("test bucket tags must be valid")
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
    coord
        .put_object_retention(&PutObjectRetentionRequest {
            object: object_version_request(bucket, key, version_id, requester),
            retention,
            bypass_governance,
        })
        .map(|_| ())
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
    coord
        .put_object_legal_hold(&PutObjectLegalHoldRequest {
            object: object_version_request(bucket, key, version_id, requester),
            legal_hold,
        })
        .map(|_| ())
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
