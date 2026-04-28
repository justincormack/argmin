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
use storage::SimplePayloadReclaimRecord;
#[cfg(test)]
use storage::{BucketEncryptionConfig, EffectiveBucketEncryptionConfig, ObjectLayout};
use storage::{BucketName, ObjectKey, SharedStorageNode, StorageCluster};
#[cfg(test)]
use storage::{
    BucketObjectLockConfig, BucketOwnershipControls, BucketState, CreateStreamUploadReq, EcShape,
    GenerationId, ManagedEncryptionAlgorithm, OwnerIdentity, PublicAccessBlockConfig, SessionId,
    StoredObject, StreamUploadTarget, UploadId, UploadState, UPLOAD_ID_LEN,
};

use self::authz_results::*;
pub use self::authz_types::{
    ActiveWriteEncryption, ActiveWriteEncryptionRef, AuthorizedPutObjectWrite,
};
#[cfg(test)]
use self::payload::encode_parity_scratch_len;
use self::payload::PayloadBufferPool;
#[cfg(test)]
use self::payload::SharedPayloadBuffer;
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
use self::runtime::{LifecycleSweeper, ReclaimSweeper};
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
#[cfg(test)]
use storage::ReclaimWorkItem;

fn lock_mutex_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|err| err.into_inner())
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
    let mut bytes = [b'.'; UPLOAD_ID_LEN];
    let mut encoded = String::with_capacity(seed.len() * 2);
    for byte in seed.bytes() {
        use std::fmt::Write;
        write!(encoded, "{byte:02x}").unwrap();
    }
    let take = encoded.len().min(UPLOAD_ID_LEN);
    bytes[..take].copy_from_slice(&encoded.as_bytes()[..take]);
    UploadId::try_from(String::from_utf8(bytes.to_vec()).unwrap())
        .expect("coordinator tests must use valid upload IDs")
}

#[cfg(test)]
fn trusted_session_id(seed: &str) -> SessionId {
    let mut bytes = [b'0'; storage::SESSION_ID_LEN];
    let mut encoded = String::with_capacity(seed.len() * 2);
    for byte in seed.bytes() {
        use std::fmt::Write;
        write!(encoded, "{byte:02x}").unwrap();
    }
    let take = encoded.len().min(storage::SESSION_ID_LEN);
    bytes[..take].copy_from_slice(&encoded.as_bytes()[..take]);
    SessionId::try_from(String::from_utf8(bytes.to_vec()).unwrap())
        .expect("coordinator tests must use valid session IDs")
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
use crate::pg::object_key_hash;
#[cfg(test)]
use crate::sse::SSE_C_SEGMENT_TAG_LEN;
use crate::sse::{SseCustomerValidatorConfig, StaticManagedKeyProvider};
#[cfg(test)]
use crate::system_metadata::SystemMetadata;

const TRACE_TARGET: &str = "server_core";
const COMPLETED_MULTIPART_UPLOADS_PER_BUCKET_LIMIT: usize = 10_000;

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

/// Hard cap on total records fetched across all PGs for a single list query.
/// Prevents unbounded memory when delimiter causes u32::MAX per-PG limits.
const MAX_LIST_RECORDS: usize = 100_000;
const S3_MAX_LIST_KEYS: u32 = 1_000;

#[cfg(test)]
struct WrittenShard {
    key: ShardKey,
    ack: storage::WriteAck,
}
/// Minimum part size for non-final parts (5 MiB).
const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;

/// Maximum number of parts in a multipart upload (matches AWS S3).
const MAX_PARTS: usize = 10_000;

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

    fn parsed_policy_if_fresh(
        &self,
        bucket: &BucketName,
        bucket_policy_generation: u64,
    ) -> Option<Arc<auth::BucketPolicy>> {
        let entry = self.entries.get(bucket)?;
        if !(entry.is_fresh()
            && entry.info.bucket_policy_present
            && entry.info.bucket_policy_generation == bucket_policy_generation)
        {
            return None;
        }
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
}

fn shared_caches_for_storage_cluster(
    storage_cluster: &Arc<StorageCluster>,
) -> Arc<CoordinatorSharedCaches> {
    static SHARED_COORDINATOR_CACHES: OnceLock<
        Mutex<HashMap<usize, Weak<CoordinatorSharedCaches>>>,
    > = OnceLock::new();

    let key = storage_cluster.single_node_compat_key();
    let registry = SHARED_COORDINATOR_CACHES.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = lock_mutex_unpoisoned(registry);
    if let Some(existing) = guard.get(&key).and_then(Weak::upgrade) {
        return existing;
    }
    let shared = Arc::new(CoordinatorSharedCaches::default());
    spawn_bucket_fast_path_watcher(&shared, storage_cluster.single_node_compat_handle());
    guard.insert(key, Arc::downgrade(&shared));
    shared
}

#[cfg(test)]
fn shared_caches_for_storage_node(
    storage_node: &Arc<SharedStorageNode>,
) -> Arc<CoordinatorSharedCaches> {
    shared_caches_for_storage_cluster(&StorageCluster::shared_single_node(Arc::clone(
        storage_node,
    )))
}

fn spawn_bucket_fast_path_watcher(
    shared: &Arc<CoordinatorSharedCaches>,
    storage_node: &Arc<SharedStorageNode>,
) {
    let shared = Arc::downgrade(shared);
    let storage_node = Arc::downgrade(storage_node);
    std::thread::Builder::new()
        .name("argmin-bucket-fast-path-watch".to_string())
        .spawn(move || loop {
            std::thread::sleep(std::time::Duration::from_millis(
                BUCKET_FAST_PATH_WATCH_INTERVAL_MILLIS,
            ));
            let Some(shared) = shared.upgrade() else {
                break;
            };
            let Some(storage_node) = storage_node.upgrade() else {
                break;
            };
            let buckets =
                { read_rwlock_unpoisoned(&shared.bucket_fast_path).snapshot_bucket_names() };
            if buckets.is_empty() {
                continue;
            }
            let mut buckets_by_pg = HashMap::<u32, Vec<BucketName>>::new();
            for bucket in buckets {
                buckets_by_pg
                    .entry(storage_node.bucket_pg_id_for(&bucket))
                    .or_default()
                    .push(bucket);
            }
            for (pg_id, buckets) in buckets_by_pg {
                let generations =
                    match storage_node.load_bucket_execution_generations_for_pg(pg_id, &buckets) {
                        Ok(generations) => generations,
                        Err(_) => continue,
                    };
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
    storage_node: Arc<StorageCluster>,
    shared_caches: Arc<CoordinatorSharedCaches>,
    payload_buffer_pool: Arc<PayloadBufferPool>,
    region: String,
    sse_c_validator: Option<SseCustomerValidatorConfig>,
    managed_key_provider: Option<StaticManagedKeyProvider>,
    _reclaim_sweeper: ReclaimSweeper,
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
        read_rwlock_unpoisoned(&self.shared_caches.bucket_fast_path).get_if_fresh(bucket)
    }

    pub(super) fn parsed_bucket_fast_path_policy_if_fresh(
        &self,
        bucket: &BucketName,
        bucket_policy_generation: u64,
    ) -> Option<Arc<auth::BucketPolicy>> {
        read_rwlock_unpoisoned(&self.shared_caches.bucket_fast_path)
            .parsed_policy_if_fresh(bucket, bucket_policy_generation)
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
            bucket_abac_enabled: false,
            tags: storage::BucketFastPathTags::NotApplicable,
            encryption: storage::EffectiveBucketEncryptionConfig::default(),
        }
    }

    #[test]
    fn shared_bucket_fast_path_recovers_from_poisoned_lock() {
        let shared = CoordinatorSharedCaches::default();
        let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let _guard = shared.bucket_fast_path.write().unwrap();
            panic!("poison bucket fast path lock");
        }));

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
}

/// Compute an inline checksum value for the given algorithm and data.
fn compute_checksum(algo: ChecksumAlgorithm, data: &[u8]) -> RawChecksum {
    match algo {
        ChecksumAlgorithm::Crc32 => {
            RawChecksum::new(algo, checksum::crc32::checksum(data).to_be_bytes())
        }
        ChecksumAlgorithm::Crc32c => {
            RawChecksum::new(algo, checksum::crc32c::checksum(data).to_be_bytes())
        }
        ChecksumAlgorithm::Crc64nvme => {
            RawChecksum::new(algo, checksum::crc64::checksum(data).to_be_bytes())
        }
        ChecksumAlgorithm::Sha256 => RawChecksum::new(
            algo,
            ring::digest::digest(&ring::digest::SHA256, data).as_ref(),
        ),
        ChecksumAlgorithm::Sha1 => RawChecksum::new(
            algo,
            ring::digest::digest(&ring::digest::SHA1_FOR_LEGACY_USE_ONLY, data).as_ref(),
        ),
    }
    .expect("checksum helper produces bytes matching the requested algorithm")
}

enum StreamingChecksumAccumulator {
    Crc32(checksum::crc32::Hasher),
    Crc32c(checksum::crc32c::Hasher),
    Crc64(checksum::crc64::Hasher),
    Sha1(ring::digest::Context),
    Sha256(ring::digest::Context),
}

impl StreamingChecksumAccumulator {
    fn new(algo: ChecksumAlgorithm) -> Self {
        match algo {
            ChecksumAlgorithm::Crc32 => Self::Crc32(checksum::crc32::Hasher::new()),
            ChecksumAlgorithm::Crc32c => Self::Crc32c(checksum::crc32c::Hasher::new()),
            ChecksumAlgorithm::Crc64nvme => Self::Crc64(checksum::crc64::Hasher::new()),
            ChecksumAlgorithm::Sha1 => Self::Sha1(ring::digest::Context::new(
                &ring::digest::SHA1_FOR_LEGACY_USE_ONLY,
            )),
            ChecksumAlgorithm::Sha256 => {
                Self::Sha256(ring::digest::Context::new(&ring::digest::SHA256))
            }
        }
    }

    fn algorithm(&self) -> ChecksumAlgorithm {
        match self {
            Self::Crc32(_) => ChecksumAlgorithm::Crc32,
            Self::Crc32c(_) => ChecksumAlgorithm::Crc32c,
            Self::Crc64(_) => ChecksumAlgorithm::Crc64nvme,
            Self::Sha1(_) => ChecksumAlgorithm::Sha1,
            Self::Sha256(_) => ChecksumAlgorithm::Sha256,
        }
    }

    fn update(&mut self, data: &[u8]) {
        match self {
            Self::Crc32(hasher) => hasher.update(data),
            Self::Crc32c(hasher) => hasher.update(data),
            Self::Crc64(hasher) => hasher.update(data),
            Self::Sha1(hasher) => hasher.update(data),
            Self::Sha256(hasher) => hasher.update(data),
        }
    }

    fn finalize(self) -> RawChecksum {
        match self {
            Self::Crc32(hasher) => {
                RawChecksum::new(ChecksumAlgorithm::Crc32, hasher.finalize().to_be_bytes())
            }
            Self::Crc32c(hasher) => {
                RawChecksum::new(ChecksumAlgorithm::Crc32c, hasher.finalize().to_be_bytes())
            }
            Self::Crc64(hasher) => RawChecksum::new(
                ChecksumAlgorithm::Crc64nvme,
                hasher.finalize().to_be_bytes(),
            ),
            Self::Sha1(hasher) => {
                RawChecksum::new(ChecksumAlgorithm::Sha1, hasher.finish().as_ref())
            }
            Self::Sha256(hasher) => {
                RawChecksum::new(ChecksumAlgorithm::Sha256, hasher.finish().as_ref())
            }
        }
        .expect("streaming checksum accumulator produces bytes matching the algorithm")
    }
}

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
mod test_support;
#[cfg(test)]
mod test_topology;
