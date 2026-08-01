/// Coordinator: orchestrates S3 operations across EC, storage, and metadata layers.
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{
    Arc, Mutex, MutexGuard, OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard, Weak,
};

use checksum::{ChecksumAlgorithm, RawChecksum};
#[cfg(test)]
use checksum::{ChecksumType, MultipartChecksumConfig};
#[cfg(test)]
pub(crate) use s3_types::{
    AccountIdentity, BucketNamespace, LegalHoldStatus, ObjectLockDefaultRetention, ObjectLockMode,
    ObjectRetention, RetentionPeriod,
};
#[cfg(test)]
use s3_types::{
    AclGrant, AclGrantee, AclGrants, AclPermission, BucketVersioningState, CanonicalUserId,
    StoredLegalHoldStatus, VersionId,
};
#[cfg(test)]
use storage::ObjectEncryption;
#[cfg(test)]
use storage::ObjectLockState;
#[cfg(test)]
use storage::ShardKey;
#[cfg(test)]
use storage::{BucketEncryptionConfig, EffectiveBucketEncryptionConfig, ObjectLayout};
use storage::{
    BucketName, ObjectKey, ProcessLocalRegistryKey, StorageCluster, StorageClusterRouteHandle,
};
#[cfg(test)]
use storage::{
    BucketObjectLockConfig, BucketOwnershipControls, BucketState, EcShape, GenerationId,
    ManagedEncryptionAlgorithm, PublicAccessBlockConfig, SessionId, StoredObject,
    StreamUploadTarget, UploadId, UploadState,
};

use self::authz_results::*;
pub use self::authz_types::{
    ActiveWriteEncryption, ActiveWriteEncryptionRef, AuthorizedPutObjectWrite,
};
pub use self::infra::BackgroundWorkerMode;
#[cfg(test)]
use self::payload::encode_parity_scratch_len;
use self::payload::PayloadBufferPool;
use self::read_core::{
    segment_payloads_from_object_segments, ReadObjectContext, SegmentPayloadRecord,
};
#[cfg(test)]
use self::read_core::{PayloadLease, ReadRuntime, SegmentListReader};
pub use self::read_core::{ReadChunk, ReadHandle};
pub use self::request_types::*;
use self::request_types::{AuthorizedWriteTags, BucketCreateOutcome};
pub use self::response_types::*;
use self::response_types::{DeleteMarkerLifecycleExpiration, NoncurrentLifecycleExpiration};
use self::runtime::{
    LifecycleSweeper, ReclaimSweeper, ShardBackfillSweeper, ShardRepairSweeper,
    ShardScavengerSweeper, StreamSessionSweeper,
};
#[cfg(test)]
use self::test_hooks::*;
pub use crate::checksum_claim::{ChecksumClaim, EncodedChecksumClaim};
#[cfg(test)]
use crate::conditional::DeleteCondition;
#[cfg(test)]
use crate::conditional::ReadCondition;
#[cfg(test)]
use crate::conditional::WriteCondition;
use crate::error::ServerError;
#[cfg(test)]
use crate::range::ByteRange;
#[cfg(test)]
use crate::sse::SseCustomerRequest;
pub use storage::BucketObjectOwnership;
pub use storage::OwnerIdentity;
#[cfg(test)]
use storage::TestReclaimWorkItem as ReclaimWorkItem;

fn lock_mutex_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|err| err.into_inner())
}

pub(super) fn map_store_error(error: storage::StoreError) -> ServerError {
    if store_error_is_retryable_contention(&error) {
        ServerError::OperationAborted
    } else if store_error_is_resource_exhausted(&error) {
        ServerError::SlowDown
    } else {
        ServerError::Store(error)
    }
}

pub(crate) struct StreamSegmentAppendPayload<'a> {
    pub(crate) storage_bytes: &'a [u8],
    pub(crate) payload_crc64: u64,
}

impl<'a> StreamSegmentAppendPayload<'a> {
    pub(crate) fn new(storage_bytes: &'a [u8], payload_crc64: u64) -> Self {
        Self {
            storage_bytes,
            payload_crc64,
        }
    }
}

pub(super) fn metadata_error_is_command_contention(error: &storage::MetadataError) -> bool {
    matches!(
        error,
        storage::MetadataError::ObjectGenerationReservationConflict { .. }
            | storage::MetadataError::ObjectVersionReservationConflict { .. }
            | storage::MetadataError::BucketWriteReservationConflict { .. }
            | storage::MetadataError::BucketWriteReservationNotFound { .. }
            | storage::MetadataError::StaleBucketMetadataCommand { .. }
            | storage::MetadataError::StaleObjectWriteCommand { .. }
    )
}

fn store_error_is_metadata_command_contention(error: &storage::StoreError) -> bool {
    if error.storage_node_failure_class()
        == Some(storage::StorageNodeFailureClass::MetadataCommandContention)
    {
        return true;
    }
    match error {
        storage::StoreError::MetadataCommandContention { .. }
        | storage::StoreError::MetadataCommandLogConflict { .. }
        | storage::StoreError::MetadataCommandLogGap { .. }
        | storage::StoreError::MetadataCommandPendingConflict { .. } => true,
        storage::StoreError::ShardStore { source, .. } => {
            store_error_is_metadata_command_contention(source)
        }
        _ => false,
    }
}

pub(super) fn object_pg_action_error_is_metadata_command_contention(
    error: &storage::ObjectPgActionError,
) -> bool {
    match error {
        storage::ObjectPgActionError::Store(error) => {
            store_error_is_metadata_command_contention(error)
        }
        storage::ObjectPgActionError::Metadata(error) => {
            metadata_error_is_command_contention(error)
        }
        _ => false,
    }
}

fn store_error_is_retryable_contention(error: &storage::StoreError) -> bool {
    if error
        .storage_node_failure_class()
        .is_some_and(storage_node_failure_is_retryable_contention)
    {
        return true;
    }
    match error {
        storage::StoreError::MetadataCommandContention { .. }
        | storage::StoreError::MetadataCommandLogConflict { .. }
        | storage::StoreError::MetadataCommandLogGap { .. }
        | storage::StoreError::StalePayloadOperation { .. }
        | storage::StoreError::StaleMetadataCommand { .. }
        | storage::StoreError::StaleMetadataPrimaryBridge { .. }
        | storage::StoreError::StaleMetadataOperation { .. }
        | storage::StoreError::StaleMetadataRoute { .. }
        | storage::StoreError::RouteMapExpired { .. }
        | storage::StoreError::RouteAdmissionClusterMismatch { .. }
        | storage::StoreError::StaleShardOperation { .. }
        | storage::StoreError::StaleShardLocation { .. }
        | storage::StoreError::PgNotActive { .. }
        | storage::StoreError::ShardPgNotActive { .. } => true,
        storage::StoreError::ShardStore { source, .. } => {
            store_error_is_retryable_contention(source)
        }
        _ => false,
    }
}

fn storage_node_failure_is_retryable_contention(failure: storage::StorageNodeFailureClass) -> bool {
    match failure {
        storage::StorageNodeFailureClass::ShardLocationStale
        | storage::StorageNodeFailureClass::PgRouteUnavailable
        | storage::StorageNodeFailureClass::MetadataCommandContention
        | storage::StorageNodeFailureClass::TransportInterrupted => true,
        storage::StorageNodeFailureClass::MetadataTransferHistoricalRouteActive => false,
    }
}

fn store_error_is_resource_exhausted(error: &storage::StoreError) -> bool {
    match error {
        storage::StoreError::StorageRpcResourceExhausted { .. } => true,
        storage::StoreError::ShardStore { source, .. } => store_error_is_resource_exhausted(source),
        _ => false,
    }
}

#[cfg(test)]
fn trusted_bucket_name(name: impl Into<String>) -> BucketName {
    BucketName::try_from(name.into())
        .expect("coordinator must only construct BucketName from validated values")
}

#[cfg(test)]
fn trusted_object_key(key: impl Into<String>) -> ObjectKey {
    ObjectKey::try_from(key.into())
        .expect("coordinator must only construct ObjectKey from validated values")
}

#[cfg(test)]
fn trusted_upload_id(seed: &str) -> UploadId {
    UploadId::for_test(seed)
}

fn parse_list_object_key(value: &str) -> Result<ObjectKey, ServerError> {
    ObjectKey::try_from(value).map_err(|error| ServerError::InvalidArgument {
        reason: error.to_string(),
    })
}

fn optional_list_object_key(value: Option<&str>) -> Result<Option<ObjectKey>, ServerError> {
    value
        .filter(|value| !value.is_empty())
        .map(parse_list_object_key)
        .transpose()
}

fn read_rwlock_unpoisoned<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|err| err.into_inner())
}

fn write_rwlock_unpoisoned<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(|err| err.into_inner())
}
#[cfg(test)]
use crate::etag::format_etag;
#[cfg(test)]
use crate::metadata_blob::MetadataBlob;
#[cfg(test)]
use crate::sse::SSE_C_SEGMENT_TAG_LEN;
use crate::sse::{SseCustomerValidatorConfig, StaticManagedKeyProvider};
#[cfg(test)]
use crate::system_metadata::SystemMetadata;
#[cfg(test)]
use storage::object_key_hash;

const TRACE_TARGET: &str = "server_core";

/// Maximum object size for single PUT or upload part (5 GiB, matches AWS S3).
pub const MAX_OBJECT_SIZE: u64 = 5 * 1024 * 1024 * 1024;

/// Fixed internal segment size for newly committed segmented payloads.
pub const INTERNAL_SEGMENT_SIZE: usize = 8 * 1024 * 1024;

const LIFECYCLE_SWEEP_INTERVAL_MILLIS: u64 = 1000;
const BUCKET_FAST_PATH_MAX_ENTRIES: usize = 1024;
#[cfg(test)]
const BUCKET_FAST_PATH_WATCH_INTERVAL_MILLIS: u64 = 50;
#[cfg(not(test))]
const BUCKET_FAST_PATH_WATCH_INTERVAL_MILLIS: u64 = 1000;

const S3_MAX_LIST_KEYS: u32 = 1_000;

/// Minimum part size for non-final parts (5 MiB).
const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;

/// Maximum number of parts in a multipart upload (matches AWS S3).
pub const MAX_MULTIPART_PARTS: usize = 10_000;

#[derive(Debug, Default)]
struct BucketFastPathCache {
    entries: HashMap<BucketName, BucketFastPathCacheEntry>,
    next_touch: AtomicU64,
}

#[derive(Debug)]
struct BucketFastPathCacheEntry {
    info: storage::BucketFastPathInfo,
    parsed_policy: Option<Arc<auth::BucketPolicy>>,
    known_generation: AtomicU64,
    last_used_tick: AtomicU64,
}

impl BucketFastPathCacheEntry {
    fn parse_policy(
        info: &storage::BucketFastPathInfo,
    ) -> Result<Option<Arc<auth::BucketPolicy>>, ServerError> {
        match &info.policy {
            storage::BucketFastPathPolicy::Absent => Ok(None),
            storage::BucketFastPathPolicy::Loaded(policy) => auth::parse_bucket_policy(policy)
                .map(Arc::new)
                .map(Some)
                .map_err(|e| ServerError::InternalError {
                    reason: format!(
                        "stored bucket policy for {} failed to parse at cache insert time: {}",
                        info.name,
                        e.reason()
                    ),
                }),
        }
    }

    fn new(
        info: storage::BucketFastPathInfo,
        known_generation: u64,
        last_used_tick: u64,
    ) -> Result<Self, ServerError> {
        Ok(Self {
            parsed_policy: Self::parse_policy(&info)?,
            known_generation: AtomicU64::new(known_generation),
            info,
            last_used_tick: AtomicU64::new(last_used_tick),
        })
    }

    fn record_hit(&self, tick: u64) {
        let mut observed = self.last_used_tick.load(Ordering::Relaxed);
        while observed < tick {
            match self.last_used_tick.compare_exchange_weak(
                observed,
                tick,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => observed = actual,
            }
        }
    }

    fn is_fresh(&self) -> bool {
        // This is only a process-local watcher hint. Request paths must still
        // validate the cached identity against durable bucket metadata before
        // using any fast-path state for authorization or bucket configuration.
        self.info.bucket_execution_generation == self.known_generation.load(Ordering::Relaxed)
    }

    fn observe_known_generation(&self, generation: u64) {
        let mut observed = self.known_generation.load(Ordering::Relaxed);
        while observed < generation {
            match self.known_generation.compare_exchange_weak(
                observed,
                generation,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => observed = actual,
            }
        }
    }
}

impl BucketFastPathCache {
    fn next_tick(&self) -> u64 {
        self.next_touch
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1)
    }

    #[cfg(test)]
    fn get(&self, bucket: &BucketName) -> Option<storage::BucketFastPathInfo> {
        let entry = self.entries.get(bucket)?;
        entry.record_hit(self.next_tick());
        Some(entry.info.clone())
    }

    fn get_if_fresh(&self, bucket: &BucketName) -> Option<storage::BucketFastPathInfo> {
        let entry = self.entries.get(bucket)?;
        entry.record_hit(self.next_tick());
        entry.is_fresh().then(|| entry.info.clone())
    }

    #[cfg(test)]
    fn is_fresh(&self, bucket: &BucketName) -> Option<bool> {
        self.entries
            .get(bucket)
            .map(BucketFastPathCacheEntry::is_fresh)
    }

    #[cfg(test)]
    fn parsed_policy_if_fresh(
        &self,
        bucket: &BucketName,
        bucket_policy_generation: u64,
    ) -> Option<(Arc<auth::BucketPolicy>, storage::BucketFastPathIdentity)> {
        let entry = self.entries.get(bucket)?;
        // Local freshness only prevents obviously stale entries from reaching
        // the parsed-policy fast path. The returned identity must be validated
        // durably by the caller before the parsed policy is trusted.
        if !(entry.is_fresh()
            && entry.info.bucket_policy_present
            && entry.info.bucket_policy_generation == bucket_policy_generation)
        {
            return None;
        }
        Some((
            entry.parsed_policy.as_ref().cloned()?,
            entry.info.identity(),
        ))
    }

    fn parsed_policy_for_identity_if_fresh(
        &self,
        bucket: &BucketName,
        identity: storage::BucketFastPathIdentity,
        bucket_policy_generation: u64,
    ) -> Option<Arc<auth::BucketPolicy>> {
        let entry = self.entries.get(bucket)?;
        if !(entry.is_fresh()
            && entry.info.identity() == identity
            && entry.info.bucket_policy_present
            && entry.info.bucket_policy_generation == bucket_policy_generation)
        {
            return None;
        }
        entry.record_hit(self.next_tick());
        entry.parsed_policy.as_ref().cloned()
    }

    fn insert(&mut self, info: storage::BucketFastPathInfo) -> Result<(), ServerError> {
        let tick = self.next_tick();
        let bucket = info.name.clone();
        let (known_generation, last_used_tick) = self
            .entries
            .get(&bucket)
            .map(|entry| {
                (
                    entry
                        .known_generation
                        .load(Ordering::Relaxed)
                        .max(info.bucket_execution_generation),
                    entry.last_used_tick.load(Ordering::Relaxed).max(tick),
                )
            })
            .unwrap_or((info.bucket_execution_generation, tick));
        let entry = BucketFastPathCacheEntry::new(info, known_generation, last_used_tick)?;
        self.entries.insert(bucket, entry);
        self.evict_if_needed();
        Ok(())
    }

    fn remove(&mut self, bucket: &BucketName) {
        self.entries.remove(bucket);
    }

    fn observe_known_generation(&self, bucket: &BucketName, generation: u64) {
        if let Some(entry) = self.entries.get(bucket) {
            entry.observe_known_generation(generation);
        }
    }

    fn snapshot_bucket_names(&self) -> Vec<BucketName> {
        self.entries.keys().cloned().collect()
    }

    fn evict_if_needed(&mut self) {
        while self.entries.len() > BUCKET_FAST_PATH_MAX_ENTRIES {
            let Some(lru_bucket) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used_tick.load(Ordering::Relaxed))
                .map(|(bucket, _)| bucket.clone())
            else {
                break;
            };
            self.entries.remove(&lru_bucket);
        }
    }
}

#[derive(Default)]
pub(super) struct CoordinatorSharedCaches {
    bucket_fast_path: RwLock<BucketFastPathCache>,
    #[cfg(test)]
    stream_append_test_hooks: Arc<Mutex<test_hooks::StreamAppendTestHooks>>,
    #[cfg(test)]
    bucket_write_handle_test_hooks: Arc<Mutex<test_hooks::BucketWriteHandleTestHooks>>,
}

fn shared_caches_for_storage_cluster(
    storage_cluster: &Arc<StorageCluster>,
) -> Arc<CoordinatorSharedCaches> {
    static SHARED_COORDINATOR_CACHES: OnceLock<
        Mutex<HashMap<ProcessLocalRegistryKey, Weak<CoordinatorSharedCaches>>>,
    > = OnceLock::new();

    let key = storage_cluster.process_local_registry_key();
    let registry = SHARED_COORDINATOR_CACHES.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = lock_mutex_unpoisoned(registry);
    if let Some(existing) = guard.get(&key).and_then(Weak::upgrade) {
        return existing;
    }
    let shared = Arc::new(CoordinatorSharedCaches::default());
    spawn_bucket_fast_path_watcher(&shared, storage_cluster);
    guard.insert(key, Arc::downgrade(&shared));
    shared
}

fn spawn_bucket_fast_path_watcher(
    shared: &Arc<CoordinatorSharedCaches>,
    storage_cluster: &Arc<StorageCluster>,
) {
    let shared = Arc::downgrade(shared);
    let storage_cluster = Arc::clone(storage_cluster);
    std::thread::Builder::new()
        .name("argmin-bucket-fast-path-watch".to_string())
        .spawn(move || loop {
            std::thread::sleep(std::time::Duration::from_millis(
                BUCKET_FAST_PATH_WATCH_INTERVAL_MILLIS,
            ));
            let Some(shared) = shared.upgrade() else {
                break;
            };
            let buckets =
                { read_rwlock_unpoisoned(&shared.bucket_fast_path).snapshot_bucket_names() };
            if buckets.is_empty() {
                continue;
            }
            for (buckets, generations) in
                storage_cluster.load_available_bucket_execution_generation_batches(&buckets)
            {
                let mut cache = write_rwlock_unpoisoned(&shared.bucket_fast_path);
                for bucket in buckets {
                    match generations.get(&bucket) {
                        Some(&generation) => cache.observe_known_generation(&bucket, generation),
                        None => cache.remove(&bucket),
                    }
                }
            }
        })
        .expect("coordinator should spawn bucket fast path watcher");
}

pub struct Coordinator {
    storage_node: StorageClusterRouteHandle,
    shared_caches: Arc<CoordinatorSharedCaches>,
    payload_buffer_pool: Arc<PayloadBufferPool>,
    region: String,
    sse_c_validator: Option<SseCustomerValidatorConfig>,
    managed_key_provider: Option<StaticManagedKeyProvider>,
    _reclaim_sweeper: Arc<ReclaimSweeper>,
    _shard_scavenger_sweeper: Arc<ShardScavengerSweeper>,
    _shard_repair_sweeper: Arc<ShardRepairSweeper>,
    _shard_backfill_sweeper: Arc<ShardBackfillSweeper>,
    _stream_session_sweeper: Arc<StreamSessionSweeper>,
    _lifecycle_sweeper: Arc<LifecycleSweeper>,
}

impl Coordinator {
    fn now_millis() -> u64 {
        storage::clock::current_time_millis()
    }

    #[cfg(test)]
    pub(super) fn get_bucket_fast_path(
        &self,
        bucket: &BucketName,
    ) -> Option<storage::BucketFastPathInfo> {
        read_rwlock_unpoisoned(&self.shared_caches.bucket_fast_path).get(bucket)
    }

    pub(super) fn get_bucket_fast_path_if_fresh(
        &self,
        bucket: &BucketName,
    ) -> Option<storage::BucketFastPathInfo> {
        let info =
            read_rwlock_unpoisoned(&self.shared_caches.bucket_fast_path).get_if_fresh(bucket)?;
        #[cfg(test)]
        if test_hooks::should_fail_bucket_fast_path_identity_load(bucket.as_str()) {
            self.remove_bucket_fast_path(bucket);
            return None;
        }
        match self.storage_node().load_bucket_fast_path_identity(bucket) {
            Ok(Some(identity)) if identity == info.identity() => Some(info),
            Ok(_) | Err(_) => {
                self.remove_bucket_fast_path(bucket);
                None
            }
        }
    }

    #[cfg(test)]
    pub(super) fn parsed_bucket_fast_path_policy_if_fresh(
        &self,
        bucket: &BucketName,
        bucket_policy_generation: u64,
    ) -> Option<Arc<auth::BucketPolicy>> {
        let (parsed, cached_identity) =
            read_rwlock_unpoisoned(&self.shared_caches.bucket_fast_path)
                .parsed_policy_if_fresh(bucket, bucket_policy_generation)?;
        #[cfg(test)]
        if test_hooks::should_fail_bucket_fast_path_identity_load(bucket.as_str()) {
            self.remove_bucket_fast_path(bucket);
            return None;
        }
        match self.storage_node().load_bucket_fast_path_identity(bucket) {
            Ok(Some(identity)) if identity == cached_identity => Some(parsed),
            Ok(_) | Err(_) => {
                self.remove_bucket_fast_path(bucket);
                None
            }
        }
    }

    pub(super) fn parsed_bucket_fast_path_policy_for_identity_if_fresh(
        &self,
        bucket: &BucketName,
        identity: storage::BucketFastPathIdentity,
        bucket_policy_generation: u64,
    ) -> Option<Arc<auth::BucketPolicy>> {
        read_rwlock_unpoisoned(&self.shared_caches.bucket_fast_path)
            .parsed_policy_for_identity_if_fresh(bucket, identity, bucket_policy_generation)
    }

    pub(super) fn upsert_bucket_fast_path(
        &self,
        info: storage::BucketFastPathInfo,
    ) -> Result<(), ServerError> {
        write_rwlock_unpoisoned(&self.shared_caches.bucket_fast_path).insert(info)
    }

    pub(super) fn remove_bucket_fast_path(&self, bucket: &BucketName) {
        write_rwlock_unpoisoned(&self.shared_caches.bucket_fast_path).remove(bucket);
    }

    pub(super) fn observe_bucket_fast_path_generation(&self, bucket: &BucketName, generation: u64) {
        read_rwlock_unpoisoned(&self.shared_caches.bucket_fast_path)
            .observe_known_generation(bucket, generation);
    }
    #[cfg(test)]
    pub(super) fn bucket_fast_path_is_fresh_for_test(&self, bucket: &BucketName) -> Option<bool> {
        read_rwlock_unpoisoned(&self.shared_caches.bucket_fast_path).is_fresh(bucket)
    }
}

#[cfg(test)]
mod bucket_fast_path_cache_tests {
    use super::test_panic::SuppressExpectedTestPanic;
    use super::*;
    use std::panic::AssertUnwindSafe;

    fn bucket_fast_path_info(name: &str) -> storage::BucketFastPathInfo {
        storage::BucketFastPathInfo {
            name: trusted_bucket_name(name),
            owner_principal: "owner".to_string(),
            owner_canonical_id: s3_types::CanonicalUserId::from_principal("owner"),
            created_at: 0,
            state: storage::BucketState::Active,
            versioning: s3_types::BucketVersioningState::Disabled,
            object_lock: s3_types::BucketObjectLockConfig {
                enabled: false,
                default_retention: None,
            },
            public_access_block: None,
            ownership_controls: None,
            bucket_policy_present: false,
            bucket_policy_public: false,
            bucket_policy_generation: 0,
            policy: storage::BucketFastPathPolicy::Absent,
            bucket_lifecycle_present: false,
            bucket_lifecycle_generation: 0,
            bucket_execution_generation: 0,
            bucket_incarnation_generation: 0,
            multipart_upload_id_authority: storage::MultipartUploadIdAuthority::for_test(),
            bucket_abac_enabled: false,
            tags: storage::BucketFastPathTags::NotApplicable,
            encryption: storage::EffectiveBucketEncryptionConfig::default(),
        }
    }

    #[test]
    fn shared_bucket_fast_path_recovers_from_poisoned_lock() {
        let shared = CoordinatorSharedCaches::default();
        let poison_result = {
            let _panic_guard = SuppressExpectedTestPanic::enter();
            std::panic::catch_unwind(AssertUnwindSafe(|| {
                let _guard = shared.bucket_fast_path.write().unwrap();
                panic!("poison bucket fast path lock");
            }))
        };
        assert!(poison_result.is_err());

        write_rwlock_unpoisoned(&shared.bucket_fast_path)
            .insert(bucket_fast_path_info("bucket"))
            .unwrap();
        assert_eq!(
            read_rwlock_unpoisoned(&shared.bucket_fast_path)
                .get(&trusted_bucket_name("bucket"))
                .as_ref()
                .map(|entry| entry.name.as_str()),
            Some("bucket")
        );
        write_rwlock_unpoisoned(&shared.bucket_fast_path).remove(&trusted_bucket_name("bucket"));
        assert!(read_rwlock_unpoisoned(&shared.bucket_fast_path)
            .get(&trusted_bucket_name("bucket"))
            .is_none());
    }

    #[test]
    fn shared_bucket_fast_path_is_count_bounded_with_read_hit_recency() {
        let mut cache = BucketFastPathCache::default();

        for idx in 0..BUCKET_FAST_PATH_MAX_ENTRIES {
            cache
                .insert(bucket_fast_path_info(&format!("bucket-{idx:04}")))
                .unwrap();
        }

        assert!(cache.get(&trusted_bucket_name("bucket-0000")).is_some());

        cache
            .insert(bucket_fast_path_info(&format!(
                "bucket-{:04}",
                BUCKET_FAST_PATH_MAX_ENTRIES
            )))
            .unwrap();

        assert!(cache.get(&trusted_bucket_name("bucket-0000")).is_some());
        assert!(cache.get(&trusted_bucket_name("bucket-0001")).is_none());
        assert!(cache
            .get(&trusted_bucket_name(format!(
                "bucket-{:04}",
                BUCKET_FAST_PATH_MAX_ENTRIES
            )))
            .is_some());
    }

    #[test]
    fn shared_bucket_fast_path_insert_preserves_newer_known_generation() {
        let mut cache = BucketFastPathCache::default();
        let bucket = trusted_bucket_name("bucket");
        cache.insert(bucket_fast_path_info("bucket")).unwrap();
        cache.observe_known_generation(&bucket, 7);

        let mut reloaded = bucket_fast_path_info("bucket");
        reloaded.bucket_execution_generation = 6;
        cache.insert(reloaded).unwrap();

        assert_eq!(cache.is_fresh(&bucket), Some(false));
        let cached = cache.get(&bucket).expect("bucket should remain cached");
        assert_eq!(cached.bucket_execution_generation, 6);
    }

    #[test]
    fn parsed_policy_cache_requires_exact_loaded_snapshot_generations() {
        let mut cache = BucketFastPathCache::default();
        let bucket = trusted_bucket_name("bucket");
        let mut info = bucket_fast_path_info("bucket");
        info.bucket_execution_generation = 11;
        info.bucket_incarnation_generation = 13;
        info.bucket_policy_present = true;
        info.bucket_policy_generation = 17;
        info.policy = storage::BucketFastPathPolicy::Loaded(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket"}]}"#
                .to_string(),
        );
        cache.insert(info).unwrap();

        let identity = storage::BucketFastPathIdentity {
            bucket_execution_generation: 11,
            bucket_incarnation_generation: 13,
        };
        assert!(cache
            .parsed_policy_for_identity_if_fresh(&bucket, identity, 17)
            .is_some());
        assert!(cache
            .parsed_policy_for_identity_if_fresh(
                &bucket,
                storage::BucketFastPathIdentity {
                    bucket_execution_generation: 12,
                    ..identity
                },
                17,
            )
            .is_none());
        assert!(cache
            .parsed_policy_for_identity_if_fresh(
                &bucket,
                storage::BucketFastPathIdentity {
                    bucket_incarnation_generation: 14,
                    ..identity
                },
                17,
            )
            .is_none());
        assert!(cache
            .parsed_policy_for_identity_if_fresh(&bucket, identity, 18)
            .is_none());

        cache.observe_known_generation(&bucket, 12);
        assert!(cache
            .parsed_policy_for_identity_if_fresh(&bucket, identity, 17)
            .is_none());
    }
}

/// Compute an inline checksum value for the given algorithm and data.
fn compute_checksum(algo: ChecksumAlgorithm, data: &[u8]) -> RawChecksum {
    checksum::compute_checksum(algo, data)
}

type StreamingChecksumAccumulator = checksum::ChecksumHasher;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_helpers;

#[cfg(test)]
mod access_control_tests;
mod authz;
#[cfg(test)]
mod authz_model_tests;
mod authz_results;
mod authz_types;
mod bucket;
mod bucket_handles;
#[cfg(test)]
mod bucket_tests;
mod copy;
#[cfg(test)]
mod core_tests;
mod delete;
mod infra;
mod lifecycle;
mod listing;
mod multipart;
#[cfg(test)]
mod multipart_reclaim_trace_tests;
#[cfg(test)]
mod multipart_stateful_tests;
#[cfg(test)]
mod multipart_tests;
#[cfg(test)]
mod multipart_trace_tests;
mod object_metadata;
mod object_state;
#[cfg(test)]
mod object_state_tests;
mod payload;
mod put;
mod read;
mod read_core;
#[cfg(test)]
mod read_tests;
mod request_types;
mod response_types;
mod runtime;
mod streaming;
#[cfg(test)]
mod test_hooks;
#[cfg(test)]
mod test_panic;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod test_topology;
