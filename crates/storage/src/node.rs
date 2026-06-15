/// LocalStorageNode and SharedStorageNode — manage multiple PgStores on a single node.
use ec::{EcConfig, ErasureCodec};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(any(test, feature = "test-hooks"))]
use std::sync::OnceLock;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
#[cfg(any(test, feature = "test-hooks"))]
use std::time::Instant;

#[cfg(any(test, feature = "test-hooks"))]
use rapidhash::v3::{rapidhash_v3_micro_inline, RapidSecrets};
#[cfg(any(test, feature = "test-hooks"))]
use s3_types::VersionId;
#[cfg(test)]
use s3_types::{AclGrants, BucketObjectLockConfig, BucketVersioningState, CanonicalUserId};

use crate::control_plane::{NodePgHeartbeatObservation, PgMetadataProof};
#[cfg(test)]
use crate::error::BucketWriteDrainError;
use crate::error::{BucketSnapshotLoadError, ObjectPgActionError, StoreError};
use crate::pg_store::{PgStore, ScavengerShardFileScan};
use crate::pg_topology::PgTopology;
use crate::traits::{PgMetadataStore, ShardStore, StorageNode};
#[cfg(test)]
use crate::types::CreateBucketConfig;
#[cfg(any(test, feature = "test-hooks"))]
use crate::types::ListMultipartUploadsReq;
#[cfg(test)]
use crate::types::{
    AuthorizedMultipartUploadRecord, BucketState, ListObjectVersionsReq, ListedMultipartParts,
    MultipartCompletionPreflight, MultipartCompletionSnapshot, MultipartUploadManagementLookup,
    ObjectReadSnapshotOutcome,
};
use crate::types::{
    BucketInfo, BucketName, BucketSnapshot, BucketSnapshotRequest, BucketSubresourceKind, EcShape,
    GenerationId, LoadedBucketSubresource, ObjectKey, ObjectReadAuthSubject,
    ObjectReadAuthSubjectIdentity, ObjectReadSnapshot, ShardKey, StoredObject, WriteAck,
};
#[cfg(any(test, feature = "test-hooks"))]
use crate::types::{
    CreateStreamUploadReq, ListPartsReq, ListPartsResp, MultipartPartRecord,
    MultipartPartSegmentRecord, MultipartReclaimRecord, MultipartUploadRecord, ObjectPartRecord,
    ObjectSegmentRecord, ObjectSegmentsReclaimRecord, PayloadReclaimRoot, PutLiveObjectReq,
    SessionId, StreamUploadRecord, StreamUploadSegmentRecord, UploadId, UploadState,
};
#[cfg(test)]
use crate::types::{StreamUploadState, StreamUploadTarget};
use crate::{PgId, PgState};

const TRACE_TARGET: &str = "storage";
#[cfg(any(test, feature = "test-hooks"))]
const RAPIDHASH_SECRETS: RapidSecrets = RapidSecrets::seed(0);
#[cfg(any(test, feature = "test-hooks"))]
const LOCK_WAIT_EVENT_THRESHOLD_US: u128 = 1_000;
const RECLAIM_WORKER_WAIT_POLL_MILLIS: u64 = 100;
pub(crate) const OBJECT_PAYLOAD_RECLAIM_MAX_OUTSTANDING_PER_PG: usize = 2;
mod bucket_ops;
mod multipart_ops;
mod object_metadata_ops;
mod object_read_ops;
mod stream_ops;

struct PgDataPaths {
    shards_dir: PathBuf,
    tmp_dir: PathBuf,
}

struct EncodeScratchPool {
    max_cached: usize,
    cached: Mutex<Vec<Vec<u8>>>,
    #[cfg(any(test, feature = "test-hooks"))]
    allocations: std::sync::atomic::AtomicUsize,
}

struct EncodeScratch {
    pool: Arc<EncodeScratchPool>,
    buf: Option<Vec<u8>>,
}

struct StorageEcWriteState {
    codec: ErasureCodec,
    scratch: Arc<EncodeScratchPool>,
}

#[cfg(any(test, feature = "test-hooks"))]
pub struct BucketLockGuard<'a> {
    guard: MutexGuard<'a, ()>,
}

#[cfg(any(test, feature = "test-hooks"))]
pub struct BucketPgTestGuard<'a> {
    guard: MutexGuard<'a, PgStore>,
}

#[cfg(any(test, feature = "test-hooks"))]
pub struct DirectPutMetadataPublishTestHookGuard {
    scope_id: usize,
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for BucketLockGuard<'_> {
    fn drop(&mut self) {
        let _ = &self.guard;
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for BucketPgTestGuard<'_> {
    fn drop(&mut self) {
        let _ = &self.guard;
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for DirectPutMetadataPublishTestHookGuard {
    fn drop(&mut self) {
        let hooks =
            AFTER_DIRECT_PUT_METADATA_PUBLISH_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        hooks.lock().unwrap().remove(&self.scope_id);
    }
}

#[cfg(any(test, feature = "test-hooks"))]
#[derive(Default, Clone)]
pub struct BucketScopedTestHooks {
    pub target: Option<BucketName>,
    pub before_bucket_lock_acquire: Option<Arc<dyn Fn() + Send + Sync>>,
    pub before_bucket_write_drain_wait: Option<Arc<dyn Fn() + Send + Sync>>,
    pub before_lifecycle_context_load: Option<Arc<dyn Fn() + Send + Sync>>,
    pub before_lifecycle_bucket_write_proof_acquire: Option<Arc<dyn Fn() + Send + Sync>>,
    pub after_begin_bucket_delete_drain: Option<Arc<dyn Fn() + Send + Sync>>,
    pub after_bucket_delete_finalize: Option<Arc<dyn Fn() + Send + Sync>>,
    pub before_completed_multipart_prune:
        Option<Arc<dyn Fn() -> Result<(), ObjectPgActionError> + Send + Sync>>,
}

#[cfg(any(test, feature = "test-hooks"))]
static BUCKET_SCOPED_TEST_HOOKS: OnceLock<Mutex<BucketScopedTestHooks>> = OnceLock::new();

#[cfg(any(test, feature = "test-hooks"))]
type DirectPutMetadataPublishHook = Arc<dyn Fn() -> Result<(), ObjectPgActionError> + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
static AFTER_DIRECT_PUT_METADATA_PUBLISH_HOOKS: OnceLock<
    Mutex<HashMap<usize, DirectPutMetadataPublishHook>>,
> = OnceLock::new();

#[cfg(any(test, feature = "test-hooks"))]
pub struct BucketScopedTestHookGuard;

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for BucketScopedTestHookGuard {
    fn drop(&mut self) {
        let hooks =
            BUCKET_SCOPED_TEST_HOOKS.get_or_init(|| Mutex::new(BucketScopedTestHooks::default()));
        *hooks.lock().unwrap() = BucketScopedTestHooks::default();
    }
}

#[cfg(any(test, feature = "test-hooks"))]
pub fn install_bucket_scoped_test_hooks(hooks: BucketScopedTestHooks) -> BucketScopedTestHookGuard {
    let slot =
        BUCKET_SCOPED_TEST_HOOKS.get_or_init(|| Mutex::new(BucketScopedTestHooks::default()));
    *slot.lock().unwrap() = hooks;
    BucketScopedTestHookGuard
}

#[cfg(any(test, feature = "test-hooks"))]
fn maybe_run_bucket_scoped_test_hook(
    bucket: &BucketName,
    project: impl FnOnce(BucketScopedTestHooks) -> Option<Arc<dyn Fn() + Send + Sync>>,
) {
    let hooks = BUCKET_SCOPED_TEST_HOOKS
        .get_or_init(|| Mutex::new(BucketScopedTestHooks::default()))
        .lock()
        .unwrap()
        .clone();
    if hooks.target.as_ref().is_some_and(|target| target == bucket) {
        if let Some(hook) = project(hooks) {
            hook();
        }
    }
}

#[cfg(any(test, feature = "test-hooks"))]
pub(super) fn maybe_run_before_bucket_lock_acquire_hook(bucket: &BucketName) {
    maybe_run_bucket_scoped_test_hook(bucket, |hooks| hooks.before_bucket_lock_acquire)
}

#[cfg(any(test, feature = "test-hooks"))]
pub(super) fn maybe_run_bucket_write_drain_wait_hook(bucket: &BucketName) {
    maybe_run_bucket_scoped_test_hook(bucket, |hooks| hooks.before_bucket_write_drain_wait)
}

#[cfg(not(any(test, feature = "test-hooks")))]
pub(super) fn maybe_run_bucket_write_drain_wait_hook(_: &BucketName) {}

#[cfg(any(test, feature = "test-hooks"))]
pub(super) fn maybe_run_before_lifecycle_context_load_hook(bucket: &BucketName) {
    maybe_run_bucket_scoped_test_hook(bucket, |hooks| hooks.before_lifecycle_context_load)
}

#[cfg(not(any(test, feature = "test-hooks")))]
pub(super) fn maybe_run_before_lifecycle_context_load_hook(_: &BucketName) {}

#[cfg(any(test, feature = "test-hooks"))]
pub(super) fn maybe_run_before_lifecycle_bucket_write_proof_acquire_hook(bucket: &BucketName) {
    maybe_run_bucket_scoped_test_hook(bucket, |hooks| {
        hooks.before_lifecycle_bucket_write_proof_acquire
    })
}

#[cfg(not(any(test, feature = "test-hooks")))]
pub(super) fn maybe_run_before_lifecycle_bucket_write_proof_acquire_hook(_: &BucketName) {}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn maybe_run_after_begin_bucket_delete_drain_hook(bucket: &BucketName) {
    maybe_run_bucket_scoped_test_hook(bucket, |hooks| hooks.after_begin_bucket_delete_drain)
}

#[cfg(not(any(test, feature = "test-hooks")))]
pub(crate) fn maybe_run_after_begin_bucket_delete_drain_hook(_: &BucketName) {}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn maybe_run_after_bucket_delete_finalize_hook(bucket: &BucketName) {
    maybe_run_bucket_scoped_test_hook(bucket, |hooks| hooks.after_bucket_delete_finalize)
}

#[cfg(not(any(test, feature = "test-hooks")))]
pub(crate) fn maybe_run_after_bucket_delete_finalize_hook(_: &BucketName) {}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn maybe_run_before_completed_multipart_prune_hook(
    bucket: &BucketName,
) -> Result<(), ObjectPgActionError> {
    let hooks = BUCKET_SCOPED_TEST_HOOKS
        .get_or_init(|| Mutex::new(BucketScopedTestHooks::default()))
        .lock()
        .unwrap()
        .clone();
    if hooks.target.as_ref().is_some_and(|target| target == bucket) {
        if let Some(hook) = hooks.before_completed_multipart_prune {
            hook()?;
        }
    }
    Ok(())
}

#[cfg(not(any(test, feature = "test-hooks")))]
pub(crate) fn maybe_run_before_completed_multipart_prune_hook(
    _: &BucketName,
) -> Result<(), ObjectPgActionError> {
    Ok(())
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn maybe_run_after_direct_put_metadata_publish_hook(
    scope_id: usize,
) -> Result<(), ObjectPgActionError> {
    let hook = AFTER_DIRECT_PUT_METADATA_PUBLISH_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap()
        .get(&scope_id)
        .cloned();
    if let Some(hook) = hook {
        hook()?;
    }
    Ok(())
}

/// A local storage node managing multiple PG stores.
///
/// On construction, takes a data directory and a list of PG IDs.
/// Creates PG directories if they don't exist. Routes operations
/// to the appropriate PgStore.
pub struct LocalStorageNode {
    stores: HashMap<u32, PgStore>,
    pg_id_list: Vec<u32>,
    data_dir: PathBuf,
}

impl LocalStorageNode {
    /// Open a storage node, creating PG directories as needed.
    pub fn open(data_dir: &Path, pg_ids: &[u32]) -> Result<Self, StoreError> {
        std::fs::create_dir_all(data_dir).map_err(|e| StoreError::Io {
            context: "create data dir",
            source: e,
        })?;

        let mut stores = HashMap::with_capacity(pg_ids.len());
        let mut pg_id_list = Vec::with_capacity(pg_ids.len());

        for &pg_id in pg_ids {
            let pg_dir = data_dir.join(format!("pg-{pg_id:04}"));
            let store = PgStore::open(&pg_dir, pg_id)?;
            stores.insert(pg_id, store);
            pg_id_list.push(pg_id);
        }

        pg_id_list.sort_unstable();

        Ok(Self {
            stores,
            pg_id_list,
            data_dir: data_dir.to_path_buf(),
        })
    }

    /// Return the data directory path.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Get a reference to the PgStore for the given PG ID.
    ///
    /// This returns the full PgStore which implements both ShardStore
    /// and PgMetadataStore.
    #[cfg(test)]
    pub(crate) fn get_pg(&self, pg_id: u32) -> Result<&PgStore, StoreError> {
        observability::trace_scope!(TRACE_TARGET, "LocalStorageNode::get_pg", "pg_id={}", pg_id);
        self.stores
            .get(&pg_id)
            .ok_or(StoreError::PgNotFound { pg_id })
    }
}

impl StorageNode for LocalStorageNode {
    fn get_pg_store(&self, pg_id: u32) -> Result<&dyn ShardStore, StoreError> {
        self.stores
            .get(&pg_id)
            .map(|s| s as &dyn ShardStore)
            .ok_or(StoreError::PgNotFound { pg_id })
    }

    fn pg_ids(&self) -> &[u32] {
        &self.pg_id_list
    }
}

/// A shared storage node managing multiple PG stores behind mutexes.
///
/// All frontends share one `Arc<SharedStorageNode>`. Each PG's `PgStore`
/// is wrapped in a `std::sync::Mutex`, serializing all operations within
/// a PG while allowing parallelism across PGs.
pub struct SharedStorageNode {
    stores: HashMap<u32, Mutex<PgStore>>,
    pg_paths: HashMap<u32, PgDataPaths>,
    pg_id_list: Vec<u32>,
    pg_topology: PgTopology,
    default_ec_shape: EcShape,
    data_dir: PathBuf,
    #[cfg(any(test, feature = "test-hooks"))]
    bucket_locks: Vec<Mutex<()>>,
    object_payload_leases: Mutex<ObjectPayloadLeaseState>,
    reclaim_queue: (Mutex<ReclaimQueueState>, Condvar),
    ec_write_states: Mutex<HashMap<EcShape, Arc<StorageEcWriteState>>>,
}

type ReclaimRoot = (BucketName, ObjectKey, GenerationId);

#[derive(Debug, Default)]
struct ObjectPayloadLeaseState {
    leases: HashMap<ReclaimRoot, usize>,
    reclaim_fences: HashSet<ReclaimRoot>,
    active_reclaims: HashSet<ReclaimRoot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReclaimWorkItem {
    ObjectPayload(ReclaimRoot),
    BucketDelete(BucketName),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReclaimQueueInsert {
    Queued,
    Deduplicated,
    PgCapacityDeferred,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketDeleteFinalizeOutcome {
    NotFound,
    NotDeleting,
    Pending,
    Finalized,
}

#[derive(Debug, Clone)]
pub enum BucketCreateAttemptOutcome {
    Created(BucketInfo),
    Exists(BucketInfo),
}

struct ReclaimQueueState {
    work_queue: VecDeque<ReclaimWorkItem>,
    queued_objects: HashSet<ReclaimRoot>,
    queued_bucket_deletes: HashSet<BucketName>,
}

#[cfg(any(test, feature = "test-hooks"))]
const BUCKET_LOCK_STRIPES: usize = 256;

impl SharedStorageNode {
    pub const DEFAULT_EC_SHAPE: EcShape = EcShape { k: 4, m: 2 };

    pub(crate) fn topology_only(
        pg_ids: &[u32],
        default_ec_shape: EcShape,
    ) -> Result<Self, StoreError> {
        EcConfig::new(default_ec_shape.k, default_ec_shape.m).map_err(|error| {
            StoreError::ErasureCoding {
                context: "validate storage default ec shape",
                reason: error.to_string(),
            }
        })?;
        let mut pg_id_list = pg_ids.to_vec();
        pg_id_list.sort_unstable();

        #[cfg(any(test, feature = "test-hooks"))]
        let mut bucket_locks = Vec::with_capacity(BUCKET_LOCK_STRIPES);
        #[cfg(any(test, feature = "test-hooks"))]
        for _ in 0..BUCKET_LOCK_STRIPES {
            bucket_locks.push(Mutex::new(()));
        }

        Ok(Self {
            stores: HashMap::new(),
            pg_paths: HashMap::new(),
            pg_id_list,
            pg_topology: PgTopology::new(pg_ids).expect("topology-only storage node must have PGs"),
            default_ec_shape,
            data_dir: PathBuf::new(),
            #[cfg(any(test, feature = "test-hooks"))]
            bucket_locks,
            object_payload_leases: Mutex::new(ObjectPayloadLeaseState::default()),
            reclaim_queue: (
                Mutex::new(ReclaimQueueState {
                    work_queue: VecDeque::new(),
                    queued_objects: HashSet::new(),
                    queued_bucket_deletes: HashSet::new(),
                }),
                Condvar::new(),
            ),
            ec_write_states: Mutex::new(HashMap::new()),
        })
    }

    /// Open a shared storage node, creating PG directories as needed.
    pub fn open(data_dir: &Path, pg_ids: &[u32]) -> Result<Self, StoreError> {
        Self::open_with_default_ec_shape(data_dir, pg_ids, Self::DEFAULT_EC_SHAPE)
    }

    pub fn open_with_default_ec_shape(
        data_dir: &Path,
        pg_ids: &[u32],
        default_ec_shape: EcShape,
    ) -> Result<Self, StoreError> {
        EcConfig::new(default_ec_shape.k, default_ec_shape.m).map_err(|error| {
            StoreError::ErasureCoding {
                context: "validate storage default ec shape",
                reason: error.to_string(),
            }
        })?;
        std::fs::create_dir_all(data_dir).map_err(|e| StoreError::Io {
            context: "create data dir",
            source: e,
        })?;

        let mut stores = HashMap::with_capacity(pg_ids.len());
        let mut pg_paths = HashMap::with_capacity(pg_ids.len());
        let mut pg_id_list = Vec::with_capacity(pg_ids.len());

        for &pg_id in pg_ids {
            let pg_dir = data_dir.join(format!("pg-{pg_id:04}"));
            let store = PgStore::open(&pg_dir, pg_id)?;
            let shards_dir = pg_dir.join("shards");
            let tmp_dir = pg_dir.join("tmp");
            stores.insert(pg_id, Mutex::new(store));
            pg_paths.insert(
                pg_id,
                PgDataPaths {
                    shards_dir,
                    tmp_dir,
                },
            );
            pg_id_list.push(pg_id);
        }

        pg_id_list.sort_unstable();

        #[cfg(any(test, feature = "test-hooks"))]
        let mut bucket_locks = Vec::with_capacity(BUCKET_LOCK_STRIPES);
        #[cfg(any(test, feature = "test-hooks"))]
        for _ in 0..BUCKET_LOCK_STRIPES {
            bucket_locks.push(Mutex::new(()));
        }
        Ok(Self {
            stores,
            pg_paths,
            pg_id_list,
            pg_topology: PgTopology::new(pg_ids).expect("shared storage node must have PGs"),
            default_ec_shape,
            data_dir: data_dir.to_path_buf(),
            #[cfg(any(test, feature = "test-hooks"))]
            bucket_locks,
            object_payload_leases: Mutex::new(ObjectPayloadLeaseState::default()),
            reclaim_queue: (
                Mutex::new(ReclaimQueueState {
                    work_queue: VecDeque::new(),
                    queued_objects: HashSet::new(),
                    queued_bucket_deletes: HashSet::new(),
                }),
                Condvar::new(),
            ),
            ec_write_states: Mutex::new(HashMap::new()),
        })
    }

    pub fn default_ec_shape(&self) -> EcShape {
        self.default_ec_shape
    }

    /// Return the data directory path.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Return the sorted list of PG IDs.
    pub fn pg_ids(&self) -> &[u32] {
        &self.pg_id_list
    }

    pub fn pg_topology(&self) -> &PgTopology {
        &self.pg_topology
    }

    pub fn bucket_pg_id_for(&self, bucket: &BucketName) -> u32 {
        self.pg_topology.bucket_pg_for(bucket)
    }

    fn ec_write_state(&self, shape: EcShape) -> Result<Arc<StorageEcWriteState>, StoreError> {
        if let Some(existing) = self.ec_write_states.lock().unwrap().get(&shape).cloned() {
            return Ok(existing);
        }

        let config =
            EcConfig::new(shape.k, shape.m).map_err(|error| StoreError::ErasureCoding {
                context: "build erasure coding config",
                reason: error.to_string(),
            })?;
        let state = Arc::new(StorageEcWriteState {
            codec: ErasureCodec::new(config).map_err(|error| StoreError::ErasureCoding {
                context: "build erasure coding codec",
                reason: error.to_string(),
            })?,
            scratch: Arc::new(EncodeScratchPool::new(config)),
        });

        let mut guard = self.ec_write_states.lock().unwrap();
        Ok(guard
            .entry(shape)
            .or_insert_with(|| Arc::clone(&state))
            .clone())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_ec_scratch_allocation_count(&self, shape: EcShape) -> usize {
        self.ec_write_states
            .lock()
            .unwrap()
            .get(&shape)
            .map_or(0, |state| state.scratch.allocation_count())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn bucket_lock_index(&self, bucket: &BucketName) -> usize {
        (rapidhash_v3_micro_inline::<true, false>(bucket.as_str().as_bytes(), &RAPIDHASH_SECRETS)
            as usize)
            % self.bucket_locks.len()
    }

    /// Lock a bucket-scoped stripe mutex.
    ///
    /// Test helper for legacy bucket lock probes.
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn lock_bucket(&self, bucket: &BucketName) -> BucketLockGuard<'_> {
        observability::trace_scope!(
            TRACE_TARGET,
            "SharedStorageNode::lock_bucket",
            "bucket={:?}",
            bucket
        );
        let idx = self.bucket_lock_index(bucket);
        let trace = observability::current_context();
        maybe_run_before_bucket_lock_acquire_hook(bucket);
        let wait_started_at = Instant::now();
        let guard = self.bucket_locks[idx]
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let wait_us = wait_started_at.elapsed().as_micros();
        if wait_us >= LOCK_WAIT_EVENT_THRESHOLD_US {
            if let Some(trace) = &trace {
                let _ = observability::emit_bucket_lock_wait_exceeded(
                    trace,
                    TRACE_TARGET,
                    bucket,
                    idx,
                    wait_us,
                );
            }
        }
        let _ = trace;
        BucketLockGuard { guard }
    }

    #[cfg(test)]
    pub fn try_lock_bucket(&self, bucket: &BucketName) -> Option<BucketLockGuard<'_>> {
        let idx = self.bucket_lock_index(bucket);
        match self.bucket_locks[idx].try_lock() {
            Ok(guard) => Some(BucketLockGuard { guard }),
            Err(std::sync::TryLockError::WouldBlock) => None,
            Err(std::sync::TryLockError::Poisoned(error)) => Some(BucketLockGuard {
                guard: error.into_inner(),
            }),
        }
    }

    #[cfg(feature = "test-hooks")]
    pub fn try_probe_bucket_pg_available(
        &self,
        bucket: &BucketName,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let pg_id = self.pg_topology.bucket_pg_for(bucket);
        let Some(pg) = self.stores.get(&pg_id) else {
            return Err(StoreError::PgNotFound { pg_id }.into());
        };
        match pg.try_lock() {
            Ok(_guard) => Ok(true),
            Err(std::sync::TryLockError::WouldBlock) => Ok(false),
            Err(std::sync::TryLockError::Poisoned(error)) => {
                drop(error.into_inner());
                Ok(true)
            }
        }
    }

    #[cfg(feature = "test-hooks")]
    pub fn try_probe_object_pg_available(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<bool, ObjectPgActionError> {
        let pg_id = self.pg_topology.object_pg_for(bucket, key);
        let Some(pg) = self.stores.get(&pg_id) else {
            return Err(StoreError::PgNotFound { pg_id }.into());
        };
        match pg.try_lock() {
            Ok(_guard) => Ok(true),
            Err(std::sync::TryLockError::WouldBlock) => Ok(false),
            Err(std::sync::TryLockError::Poisoned(error)) => {
                drop(error.into_inner());
                Ok(true)
            }
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_bucket_pg_id_for(&self, bucket: &BucketName) -> u32 {
        self.bucket_pg_id_for(bucket)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_head_bucket_raw(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        let pg_id = self.test_bucket_pg_id_for(bucket);
        let pg = self.get_pg(pg_id)?;
        Ok(pg.head_bucket_raw(bucket)?)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_object_pg_id_for(&self, bucket: &BucketName, key: &ObjectKey) -> u32 {
        self.pg_topology.object_pg_for(bucket, key)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_data_pg_id_for(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> u32 {
        self.pg_topology
            .object_generation_segment_data_pg(bucket, key, generation_id, 0)
            .get()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_object_generation_reservation_for(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
    ) -> Result<GenerationId, ObjectPgActionError> {
        let pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
        Ok(pg.get_object_generation_reservation(bucket, key, reservation_id)?)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_multipart_part_data_pg_id_for(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        object_generation_id: GenerationId,
        part_number: u32,
    ) -> u32 {
        self.pg_topology
            .object_generation_multipart_part_data_pg(
                bucket,
                key,
                object_generation_id,
                part_number,
            )
            .get()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_get_object_meta(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<StoredObject, ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(bucket, key);
        let pg = self.get_pg(pg_id)?;
        Ok(pg.get_object_meta(bucket, key)?)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_get_multipart_upload(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(bucket, key);
        let pg = self.get_pg(pg_id)?;
        Ok(pg.get_multipart_upload(upload_id)?)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_get_multipart_part(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        part_number: u16,
    ) -> Result<MultipartPartRecord, ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(bucket, key);
        let pg = self.get_pg(pg_id)?;
        Ok(pg.get_multipart_part(upload_id, u32::from(part_number))?)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_list_multipart_parts(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        req: &ListPartsReq,
    ) -> Result<ListPartsResp, ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(bucket, key);
        let pg = self.get_pg(pg_id)?;
        Ok(pg.list_multipart_parts(req)?)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_list_multipart_uploads_for_bucket(
        &self,
        bucket: &BucketName,
    ) -> Result<Vec<MultipartUploadRecord>, ObjectPgActionError> {
        let mut uploads = Vec::new();
        for &pg_id in &self.pg_id_list {
            let pg = self.get_pg(pg_id)?;
            let listed = pg.list_multipart_uploads(&ListMultipartUploadsReq {
                bucket: bucket.clone(),
                prefix: None,
                key_marker: None,
                upload_id_marker: None,
                max_uploads: u32::MAX,
            })?;
            uploads.extend(listed.uploads);
        }
        Ok(uploads)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_get_object_segments(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<Vec<ObjectSegmentRecord>, ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(bucket, key);
        let pg = self.get_pg(pg_id)?;
        Ok(pg.get_object_segments(bucket, key, version_id)?)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_replace_live_object_segments(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        segments: &[ObjectSegmentRecord],
    ) -> Result<(), ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(bucket, key);
        let pg = self.get_pg(pg_id)?;
        let live = match pg.get_object_meta(bucket, key)? {
            StoredObject::Live(record) => record,
            StoredObject::DeleteMarker(_) => return Err(StoreError::NotFound.into()),
        };
        assert_eq!(
            live.version_id, version_id,
            "test_replace_live_object_segments called for non-current live version"
        );
        pg.put_object_with_segments(
            &PutLiveObjectReq {
                bucket: live.bucket,
                key: live.key,
                version_id: live.version_id,
                owner: live.owner,
                acl_grants: live.acl_grants,
                public_read: live.public_read,
                generation_id: live.generation_id,
                size: live.size,
                etag: live.etag,
                ec: live.ec,
                layout: live.layout,
                tags: live.tags,
                metadata_blob: live.metadata_blob,
                system_metadata_blob: live.system_metadata_blob,
                object_lock: live.object_lock,
                encryption: live.encryption,
            },
            segments,
        )?;
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_get_object_parts(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<Vec<ObjectPartRecord>, ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(bucket, key);
        let pg = self.get_pg(pg_id)?;
        Ok(pg.get_object_parts(bucket, key, version_id)?)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_replace_object_parts(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        parts: &[ObjectPartRecord],
    ) -> Result<(), ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(bucket, key);
        let pg = self.get_pg(pg_id)?;
        pg.delete_object_parts(bucket, key, version_id)?;
        Ok(pg.commit_object_parts(parts)?)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_get_object_version(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<StoredObject, ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(bucket, key);
        let pg = self.get_pg(pg_id)?;
        Ok(pg.get_object_version(bucket, key, version_id)?)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_get_object_segments_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<Option<ObjectSegmentsReclaimRecord>, ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(bucket, key);
        let pg = self.get_pg(pg_id)?;
        Ok(pg.get_object_segments_reclaim(bucket, key, generation_id)?)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_put_object_segments_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reclaim: &ObjectSegmentsReclaimRecord,
    ) -> Result<(), ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(bucket, key);
        let pg = self.get_pg(pg_id)?;
        Ok(pg.put_object_segments_reclaim(reclaim)?)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_put_multipart_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reclaim: &MultipartReclaimRecord,
    ) -> Result<(), ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(bucket, key);
        let pg = self.get_pg(pg_id)?;
        Ok(pg.put_multipart_reclaim(reclaim)?)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_payload_reclaim_exists(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<bool, ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(bucket, key);
        let pg = self.get_pg(pg_id)?;
        Ok(pg.payload_reclaim_exists(bucket, key, generation_id)?)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_list_bucket_payload_reclaim_roots(
        &self,
        bucket: &BucketName,
    ) -> Result<Vec<PayloadReclaimRoot>, ObjectPgActionError> {
        let mut roots = Vec::new();
        for &pg_id in &self.pg_id_list {
            let pg = self.get_pg(pg_id)?;
            if let Some(root) = PgMetadataStore::get_bucket_payload_reclaim_root(&*pg, bucket)? {
                roots.push(root);
            }
        }
        Ok(roots)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_force_became_noncurrent_at(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        became_noncurrent_at: u64,
    ) -> Result<(), ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(bucket, key);
        let pg = self.get_pg(pg_id)?;
        pg.connection()
            .execute(
                "UPDATE objects SET became_noncurrent_at = ?1 \
                 WHERE bucket = ?2 AND key = ?3 AND version_id = ?4",
                rusqlite::params![
                    became_noncurrent_at,
                    bucket.as_str(),
                    key.as_str(),
                    version_id.to_u64()
                ],
            )
            .map_err(|source| crate::error::StoreError::Db {
                context: "force became_noncurrent_at in test helper",
                source,
            })?;
        pg.refresh_metadata_command_state_digest()?;
        Ok(())
    }

    #[cfg(test)]
    pub fn test_create_deleting_bucket(
        &self,
        bucket: &BucketName,
    ) -> Result<(), BucketWriteDrainError> {
        let owner_canonical_id = CanonicalUserId::from_principal("default-owner");
        let acl_grants = AclGrants::default();
        let create = CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: "default-owner",
            owner_canonical_id: &owner_canonical_id,
            acl_grants: &acl_grants,
            public_read: false,
            public_write: false,
            versioning: BucketVersioningState::Disabled,
            object_lock: BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        };
        let _ = self
            .create_bucket_with_config_and_load_info(&create)
            .map_err(|err| match err {
                BucketSnapshotLoadError::Store(err) => BucketWriteDrainError::Store(err),
                BucketSnapshotLoadError::Metadata(err) => BucketWriteDrainError::Metadata(err),
            })?;
        self.mark_bucket_deleting(bucket)?;
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_get_all_multipart_part_segments_for_upload(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<Vec<MultipartPartSegmentRecord>, ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(bucket, key);
        let pg = self.get_pg(pg_id)?;
        Ok(pg.get_all_multipart_part_segments_for_upload(upload_id)?)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_set_upload_state(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        state: UploadState,
    ) -> Result<(), ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(bucket, key);
        let pg = self.get_pg(pg_id)?;
        pg.set_upload_state(upload_id, state)?;
        pg.refresh_metadata_command_state_digest()?;
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_list_stream_segments(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<Vec<StreamUploadSegmentRecord>, ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(bucket, key);
        let pg = self.get_pg(pg_id)?;
        Ok(pg.list_stream_segments(session_id)?)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_force_stream_upload_created_at(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        created_at: u64,
    ) -> Result<(), ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(bucket, key);
        let pg = self.get_pg(pg_id)?;
        pg.connection()
            .execute(
                "UPDATE stream_uploads SET created_at = ?1 WHERE session_id = ?2",
                rusqlite::params![created_at as i64, session_id.as_str()],
            )
            .map_err(|source| crate::error::StoreError::Db {
                context: "force stream_upload created_at in test helper",
                source,
            })?;
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_list_all_stream_uploads(
        &self,
    ) -> Result<Vec<StreamUploadRecord>, ObjectPgActionError> {
        let mut sessions = Vec::new();
        for &pg_id in &self.pg_id_list {
            let pg = self.get_pg(pg_id)?;
            sessions.extend(pg.list_all_stream_uploads()?);
        }
        Ok(sessions)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_create_stream_upload(
        &self,
        req: &CreateStreamUploadReq,
    ) -> Result<(), ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(&req.bucket, &req.key);
        let pg = self.get_pg(pg_id)?;
        Ok(pg.create_stream_upload(req)?)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_hook_scope_id(&self) -> usize {
        std::ptr::from_ref(self).addr()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_install_after_direct_put_metadata_publish_hook(
        &self,
        hook: Arc<dyn Fn() -> Result<(), ObjectPgActionError> + Send + Sync>,
    ) -> DirectPutMetadataPublishTestHookGuard {
        let scope_id = self.test_hook_scope_id();
        let hooks =
            AFTER_DIRECT_PUT_METADATA_PUBLISH_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        hooks.lock().unwrap().insert(scope_id, hook);
        DirectPutMetadataPublishTestHookGuard { scope_id }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_shard_exists(&self, pg_id: u32, key: &ShardKey) -> Result<bool, StoreError> {
        let pg = self.get_pg(pg_id)?;
        match pg.stat_shard(key) {
            Ok(_) => Ok(true),
            Err(StoreError::NotFound) => Ok(false),
            Err(other) => Err(other),
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_lock_bucket_pg(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketPgTestGuard<'_>, StoreError> {
        let pg_id = self.test_bucket_pg_id_for(bucket);
        let guard = self.get_pg(pg_id)?;
        Ok(BucketPgTestGuard { guard })
    }

    /// Lock and return a guard for the given PG.
    pub(crate) fn get_pg(&self, pg_id: u32) -> Result<MutexGuard<'_, PgStore>, StoreError> {
        observability::trace_scope!(TRACE_TARGET, "SharedStorageNode::get_pg", "pg_id={}", pg_id);
        let mutex = self
            .stores
            .get(&pg_id)
            .ok_or(StoreError::PgNotFound { pg_id })?;
        Ok(mutex.lock().unwrap_or_else(|e| e.into_inner()))
    }

    pub fn pg_heartbeat_observation(
        &self,
        pg_id: PgId,
        state: PgState,
    ) -> Result<NodePgHeartbeatObservation, StoreError> {
        let pg = self.get_pg(pg_id.get())?;
        let metadata_state = pg.metadata_command_replica_state()?;
        Ok(NodePgHeartbeatObservation {
            pg_id,
            state,
            metadata_proof: PgMetadataProof::new(
                metadata_state.applied_log_index,
                metadata_state.applied_log_hash,
                metadata_state.state_digest,
            ),
        })
    }

    /// Write a shard file durably without taking the per-PG mutex.
    ///
    /// This is used on the write hot path so file IO and fsync do not hold the
    /// PG metadata lock. Callers must publish the corresponding shard row under
    /// the PG mutex afterward before the shard becomes visible to reads.
    pub(crate) fn write_shard_file(
        &self,
        pg_id: u32,
        key: &ShardKey,
        data: &[u8],
    ) -> Result<WriteAck, StoreError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "SharedStorageNode::write_shard_file",
            "pg_id={} shard={} bytes={}",
            pg_id,
            key,
            data.len()
        );
        let paths = self
            .pg_paths
            .get(&pg_id)
            .ok_or(StoreError::PgNotFound { pg_id })?;
        PgStore::write_shard_file_durable(&paths.tmp_dir, &paths.shards_dir, key, data)
    }

    pub(crate) fn write_shard_file_if_absent(
        &self,
        pg_id: u32,
        key: &ShardKey,
        data: &[u8],
    ) -> Result<WriteAck, StoreError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "SharedStorageNode::write_shard_file_if_absent",
            "pg_id={} shard={} bytes={}",
            pg_id,
            key,
            data.len()
        );
        let paths = self
            .pg_paths
            .get(&pg_id)
            .ok_or(StoreError::PgNotFound { pg_id })?;
        PgStore::write_shard_file_durable_if_absent(&paths.tmp_dir, &paths.shards_dir, key, data)
    }

    /// Read a shard file directly without taking the per-PG mutex.
    pub(crate) fn read_shard_file(
        &self,
        pg_id: u32,
        key: &ShardKey,
    ) -> Result<Vec<u8>, StoreError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "SharedStorageNode::read_shard_file",
            "pg_id={} shard={}",
            pg_id,
            key
        );
        let paths = self
            .pg_paths
            .get(&pg_id)
            .ok_or(StoreError::PgNotFound { pg_id })?;
        let shard_path = PgStore::shard_path_for_shards_dir(&paths.shards_dir, key);
        fs::read(&shard_path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                return StoreError::NotFound;
            }
            StoreError::Io {
                context: "read shard file",
                source: e,
            }
        })
    }

    /// Read a shard file directly into a caller-provided buffer without taking
    /// the per-PG mutex.
    pub(crate) fn read_shard_file_into(
        &self,
        pg_id: u32,
        key: &ShardKey,
        dst: &mut [u8],
    ) -> Result<(), StoreError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "SharedStorageNode::read_shard_file_into",
            "pg_id={} shard={} bytes={}",
            pg_id,
            key,
            dst.len()
        );
        let paths = self
            .pg_paths
            .get(&pg_id)
            .ok_or(StoreError::PgNotFound { pg_id })?;
        let shard_path = PgStore::shard_path_for_shards_dir(&paths.shards_dir, key);
        let mut file = fs::File::open(&shard_path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                return StoreError::NotFound;
            }
            StoreError::Io {
                context: "open shard file",
                source: e,
            }
        })?;

        file.read_exact(dst).map_err(|e| StoreError::Io {
            context: "read shard file",
            source: e,
        })?;

        let mut extra = [0u8; 1];
        match file.read(&mut extra) {
            Ok(0) => Ok(()),
            Ok(_) => Err(StoreError::Io {
                context: "read shard file length mismatch",
                source: std::io::Error::from(std::io::ErrorKind::InvalidData),
            }),
            Err(e) => Err(StoreError::Io {
                context: "read shard file",
                source: e,
            }),
        }
    }

    /// Delete a shard file directly without mutating the PG shard metadata row.
    pub(crate) fn delete_shard_file(&self, pg_id: u32, key: &ShardKey) -> Result<(), StoreError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "SharedStorageNode::delete_shard_file",
            "pg_id={} shard={}",
            pg_id,
            key
        );
        let paths = self
            .pg_paths
            .get(&pg_id)
            .ok_or(StoreError::PgNotFound { pg_id })?;
        let shard_path = PgStore::shard_path_for_shards_dir(&paths.shards_dir, key);
        match fs::remove_file(&shard_path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(StoreError::Io {
                context: "unlink shard file",
                source: e,
            }),
        }
    }

    /// List local shard files for scavenger audit without taking the per-PG
    /// metadata mutex.
    pub(crate) fn list_scavenger_shard_files(
        &self,
        pg_id: u32,
    ) -> Result<ScavengerShardFileScan, StoreError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "SharedStorageNode::list_scavenger_shard_files",
            "pg_id={}",
            pg_id
        );
        let paths = self
            .pg_paths
            .get(&pg_id)
            .ok_or(StoreError::PgNotFound { pg_id })?;
        PgStore::list_scavenger_shard_files_in_dir(&paths.shards_dir)
    }

    /// Acquire an in-memory lease on an object payload generation.
    ///
    /// Returns false if this storage node has fenced the generation for
    /// physical reclaim.
    pub(crate) fn try_acquire_object_payload_lease(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> bool {
        let root = (bucket.clone(), key.clone(), generation_id);
        let mut state = self
            .object_payload_leases
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if state.reclaim_fences.contains(&root) {
            return false;
        }
        *state.leases.entry(root).or_insert(0) += 1;
        true
    }

    /// Release an in-memory lease on an object payload generation.
    ///
    /// Returns the remaining active lease count after release.
    pub(crate) fn release_object_payload_lease(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> usize {
        let mut state = self
            .object_payload_leases
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let root = (bucket.clone(), key.clone(), generation_id);
        let entry = state
            .leases
            .get_mut(&(bucket.clone(), key.clone(), generation_id))
            .expect("object payload lease release without acquire");
        *entry -= 1;
        let remaining = *entry;
        if remaining == 0 {
            state.leases.remove(&root);
        }
        remaining
    }

    /// Fence an object payload generation for physical reclaim.
    ///
    /// Returns false if any read lease or another reclaim is active.
    pub(crate) fn try_begin_object_payload_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> bool {
        let root = (bucket.clone(), key.clone(), generation_id);
        let mut state = self
            .object_payload_leases
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if state.leases.get(&root).copied().unwrap_or(0) != 0
            || state.active_reclaims.contains(&root)
        {
            return false;
        }
        state.active_reclaims.insert(root.clone());
        state.reclaim_fences.insert(root);
        true
    }

    /// Finish physical reclaim fencing for a generation.
    pub(crate) fn finish_object_payload_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        keep_fence: bool,
    ) {
        let root = (bucket.clone(), key.clone(), generation_id);
        let mut state = self
            .object_payload_leases
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        state.active_reclaims.remove(&root);
        if !keep_fence {
            state.reclaim_fences.remove(&root);
        }
    }

    /// Clear a reclaim fence after a matching terminal reclaim command has converged.
    pub(crate) fn clear_object_payload_reclaim_fence(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) {
        self.object_payload_leases
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .reclaim_fences
            .remove(&(bucket.clone(), key.clone(), generation_id));
    }

    /// Return the number of active object-payload leases for a generation.
    pub(crate) fn object_payload_lease_count(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> usize {
        let state = self
            .object_payload_leases
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        state
            .leases
            .get(&(bucket.clone(), key.clone(), generation_id))
            .copied()
            .unwrap_or(0)
    }

    /// Return the number of active object-payload leases for a bucket.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn bucket_object_payload_lease_count(&self, bucket: &BucketName) -> usize {
        let state = self
            .object_payload_leases
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        state
            .leases
            .iter()
            .filter(|((lease_bucket, _, _), _)| lease_bucket == bucket)
            .map(|(_, count)| *count)
            .sum()
    }

    /// Queue a payload generation for background reclaim.
    pub fn enqueue_object_payload_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> bool {
        matches!(
            self.enqueue_object_payload_reclaim_for_pg(
                bucket,
                key,
                generation_id,
                self.pg_topology.object_pg_for(bucket, key)
            ),
            ReclaimQueueInsert::Queued
        )
    }

    pub(crate) fn enqueue_object_payload_reclaim_for_pg(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        _pg_id: u32,
    ) -> ReclaimQueueInsert {
        let root = (bucket.clone(), key.clone(), generation_id);
        let (state_lock, cv) = &self.reclaim_queue;
        let mut state = state_lock.lock().unwrap_or_else(|e| e.into_inner());
        if state.queued_objects.insert(root.clone()) {
            state
                .work_queue
                .push_back(ReclaimWorkItem::ObjectPayload(root));
            Self::emit_reclaim_queue_action(&state, "object_payload", "enqueue");
            cv.notify_one();
            ReclaimQueueInsert::Queued
        } else {
            Self::emit_reclaim_queue_action(&state, "object_payload", "deduplicate");
            ReclaimQueueInsert::Deduplicated
        }
    }

    /// Queue a bucket for deferred final deletion once reclaim is drained.
    pub fn enqueue_bucket_delete_finalize(&self, bucket: &BucketName) -> bool {
        let (state_lock, cv) = &self.reclaim_queue;
        let mut state = state_lock.lock().unwrap_or_else(|e| e.into_inner());
        let bucket = bucket.clone();
        if state.queued_bucket_deletes.insert(bucket.clone()) {
            state
                .work_queue
                .push_back(ReclaimWorkItem::BucketDelete(bucket));
            Self::emit_reclaim_queue_action(&state, "bucket_delete", "enqueue");
            cv.notify_one();
            true
        } else {
            Self::emit_reclaim_queue_action(&state, "bucket_delete", "deduplicate");
            false
        }
    }

    /// Take one queued reclaim work item if immediately available.
    pub fn try_take_reclaim_work(&self) -> Option<ReclaimWorkItem> {
        let mut state = self
            .reclaim_queue
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        Self::pop_reclaim_work(&mut state)
    }

    /// Block until reclaim work is available, or stop has been requested.
    pub fn wait_for_reclaim_work(&self, stop: &AtomicBool) -> Option<ReclaimWorkItem> {
        let (state_lock, cv) = &self.reclaim_queue;
        let mut state = state_lock.lock().unwrap_or_else(|e| e.into_inner());
        while state.work_queue.is_empty() && !stop.load(Ordering::SeqCst) {
            let (next_state, _) = cv
                .wait_timeout(
                    state,
                    std::time::Duration::from_millis(RECLAIM_WORKER_WAIT_POLL_MILLIS),
                )
                .unwrap_or_else(|e| e.into_inner());
            state = next_state;
        }
        if stop.load(Ordering::SeqCst) {
            return None;
        }
        Self::pop_reclaim_work(&mut state)
    }

    /// Wake reclaim workers so they can observe shutdown or new work.
    pub fn wake_reclaim_workers(&self) {
        self.reclaim_queue.1.notify_all();
    }

    fn pop_reclaim_work(state: &mut ReclaimQueueState) -> Option<ReclaimWorkItem> {
        let work = state.work_queue.pop_front()?;
        match &work {
            ReclaimWorkItem::ObjectPayload(root) => {
                state.queued_objects.remove(root);
                Self::emit_reclaim_queue_action(state, "object_payload", "dequeue");
            }
            ReclaimWorkItem::BucketDelete(bucket) => {
                state.queued_bucket_deletes.remove(bucket);
                Self::emit_reclaim_queue_action(state, "bucket_delete", "dequeue");
            }
        }
        Some(work)
    }

    fn emit_reclaim_queue_action(
        state: &ReclaimQueueState,
        work_kind: &'static str,
        action: &'static str,
    ) {
        let _ = observability::emit_reclaim_queue_action(
            TRACE_TARGET,
            observability::ReclaimQueueSummary {
                work_kind,
                action,
                queue_depth: state.work_queue.len(),
                object_payload_depth: state.queued_objects.len(),
                object_payload_outstanding_depth: 0,
                bucket_delete_depth: state.queued_bucket_deletes.len(),
            },
        );
    }
}

impl EncodeScratchPool {
    fn new(_: EcConfig) -> Self {
        let max_cached = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .max(1);
        Self {
            max_cached,
            cached: Mutex::new(Vec::new()),
            #[cfg(any(test, feature = "test-hooks"))]
            allocations: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn checkout(self: &Arc<Self>, required_len: usize) -> EncodeScratch {
        let mut cached = self.cached.lock().unwrap();
        let maybe_idx = cached.iter().rposition(|buf| buf.len() >= required_len);
        let mut buf = maybe_idx.map_or_else(
            || {
                #[cfg(any(test, feature = "test-hooks"))]
                self.allocations.fetch_add(1, Ordering::Relaxed);
                vec![0u8; required_len]
            },
            |idx| cached.swap_remove(idx),
        );
        drop(cached);
        if buf.len() < required_len {
            buf.resize(required_len, 0);
        }
        EncodeScratch {
            pool: Arc::clone(self),
            buf: Some(buf),
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn allocation_count(&self) -> usize {
        self.allocations.load(Ordering::Relaxed)
    }
}

impl EncodeScratch {
    fn as_mut_slice(&mut self, len: usize) -> &mut [u8] {
        debug_assert!(len <= self.buf.as_ref().unwrap().len());
        &mut self.buf.as_mut().unwrap()[..len]
    }

    fn as_slice(&self, len: usize) -> &[u8] {
        debug_assert!(len <= self.buf.as_ref().unwrap().len());
        &self.buf.as_ref().unwrap()[..len]
    }
}

impl Drop for EncodeScratch {
    fn drop(&mut self) {
        let Some(buf) = self.buf.take() else {
            return;
        };
        let mut cached = self.pool.cached.lock().unwrap();
        if cached.len() < self.pool.max_cached {
            cached.push(buf);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bucket_name(name: &str) -> BucketName {
        BucketName::try_from(name).unwrap()
    }

    fn object_key(key: &str) -> ObjectKey {
        ObjectKey::try_from(key.to_string()).unwrap()
    }

    fn put_standard_object_for_snapshot_test(
        node: &SharedStorageNode,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        size: u64,
    ) -> StoredObject {
        let object_pg = node
            .get_pg(node.pg_topology().object_pg_for(bucket, key))
            .unwrap();
        object_pg
            .put_object_with_segments(
                &PutLiveObjectReq {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    version_id: VersionId::Null,
                    owner: crate::OwnerIdentity::from_principal("owner"),
                    acl_grants: AclGrants::default(),
                    public_read: false,
                    generation_id,
                    size,
                    etag: crate::ObjectEtag::single_part(size),
                    ec: SharedStorageNode::DEFAULT_EC_SHAPE,
                    layout: crate::ObjectLayout::Standard,
                    tags: None,
                    metadata_blob: None,
                    system_metadata_blob: None,
                    object_lock: crate::ObjectLockState::default(),
                    encryption: crate::ObjectEncryption::None,
                },
                &[],
            )
            .unwrap();
        object_pg.get_object_meta(bucket, key).unwrap()
    }

    #[test]
    fn data_dir_accessor() {
        let tmp = test_util::tempdir();
        let node = LocalStorageNode::open(tmp.path(), &[0, 1]).unwrap();
        assert_eq!(node.data_dir(), tmp.path());
    }

    #[test]
    fn get_pg_store_not_found() {
        let tmp = test_util::tempdir();
        let node = LocalStorageNode::open(tmp.path(), &[0, 1]).unwrap();
        let result = node.get_pg_store(999);
        assert!(result.is_err());
    }

    #[test]
    fn get_pg_valid() {
        let tmp = test_util::tempdir();
        let node = LocalStorageNode::open(tmp.path(), &[0, 1, 2]).unwrap();
        assert!(node.get_pg(0).is_ok());
        assert!(node.get_pg(1).is_ok());
        assert!(node.get_pg(2).is_ok());
        assert!(node.get_pg(3).is_err());
    }

    #[test]
    fn pg_ids_sorted() {
        let tmp = test_util::tempdir();
        let node = LocalStorageNode::open(tmp.path(), &[5, 2, 8, 1]).unwrap();
        assert_eq!(node.pg_ids(), &[1, 2, 5, 8]);
    }

    #[test]
    fn shared_storage_node_rejects_invalid_default_ec_shape() {
        let tmp = test_util::tempdir();
        let err = match SharedStorageNode::open_with_default_ec_shape(
            tmp.path(),
            &[0, 1],
            EcShape { k: 0, m: 2 },
        ) {
            Ok(_) => panic!("expected invalid default EC shape to be rejected"),
            Err(err) => err,
        };

        assert!(matches!(err, StoreError::ErasureCoding { .. }));
    }

    // ── SharedStorageNode tests ──────────────────────────────────────

    #[test]
    fn shared_node_open_and_get_pg() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0, 1, 2]).unwrap();
        assert!(node.get_pg(0).is_ok());
        assert!(node.get_pg(1).is_ok());
        assert!(node.get_pg(2).is_ok());
        assert!(node.get_pg(3).is_err());
    }

    #[test]
    fn shared_node_pg_ids_sorted() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[5, 2, 8, 1]).unwrap();
        assert_eq!(node.pg_ids(), &[1, 2, 5, 8]);
    }

    #[test]
    fn shared_node_data_dir() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0]).unwrap();
        assert_eq!(node.data_dir(), tmp.path());
    }

    #[test]
    fn shared_node_pg_heartbeat_observation_uses_metadata_replica_state() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0]).unwrap();
        let metadata_state = {
            let pg = node.get_pg(0).unwrap();
            pg.metadata_command_replica_state().unwrap()
        };

        let observation = node
            .pg_heartbeat_observation(PgId::new(0), PgState::Peering)
            .unwrap();

        assert_eq!(observation.pg_id, PgId::new(0));
        assert_eq!(observation.state, PgState::Peering);
        assert_eq!(
            observation.metadata_proof.applied_log_index,
            metadata_state.applied_log_index
        );
        assert_eq!(
            observation.metadata_proof.applied_log_hash,
            metadata_state.applied_log_hash
        );
        assert_eq!(
            observation.metadata_proof.state_digest,
            metadata_state.state_digest
        );
    }

    fn create_bucket_for_snapshot_test(node: &SharedStorageNode, name: &str) -> BucketName {
        let bucket = bucket_name(name);
        let bucket_pg = node
            .get_pg(node.pg_topology().bucket_pg_for(&bucket))
            .unwrap();
        bucket_pg
            .create_bucket(
                &bucket,
                "owner",
                &s3_types::CanonicalUserId::from_principal("owner"),
                &s3_types::AclGrants::default(),
                false,
                false,
            )
            .unwrap();
        bucket
    }

    #[test]
    fn load_bucket_snapshot_loads_tags_when_abac_enabled() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0, 1]).unwrap();
        let bucket = create_bucket_for_snapshot_test(&node, "bucket");
        let bucket_pg = node
            .get_pg(node.pg_topology().bucket_pg_for(&bucket))
            .unwrap();
        bucket_pg
            .put_bucket_subresource(
                &bucket,
                crate::types::PutBucketSubresource {
                    kind: crate::types::BucketSubresourceKind::Tagging,
                    body: "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>",
                    aux: crate::types::BucketSubresourceAux::None,
                },
            )
            .unwrap();
        bucket_pg.put_bucket_abac_enabled(&bucket, true).unwrap();
        drop(bucket_pg);

        let snapshot = node
            .load_bucket_snapshot(
                &bucket,
                crate::types::BucketSnapshotRequest {
                    tags: crate::types::BucketSnapshotTagsRequest::IfBucketAbacEnabled,
                    ..Default::default()
                },
            )
            .unwrap();

        assert!(matches!(
            snapshot.tags,
            crate::types::LoadedBucketSubresource::Loaded(_)
        ));
    }

    #[test]
    fn load_bucket_snapshot_skips_tags_when_abac_disabled() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0, 1]).unwrap();
        let bucket = create_bucket_for_snapshot_test(&node, "bucket");
        let bucket_pg = node
            .get_pg(node.pg_topology().bucket_pg_for(&bucket))
            .unwrap();
        bucket_pg
            .put_bucket_subresource(
                &bucket,
                crate::types::PutBucketSubresource {
                    kind: crate::types::BucketSubresourceKind::Tagging,
                    body: "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>",
                    aux: crate::types::BucketSubresourceAux::None,
                },
            )
            .unwrap();
        drop(bucket_pg);

        let snapshot = node
            .load_bucket_snapshot(
                &bucket,
                crate::types::BucketSnapshotRequest {
                    tags: crate::types::BucketSnapshotTagsRequest::IfBucketAbacEnabled,
                    ..Default::default()
                },
            )
            .unwrap();

        assert!(matches!(
            snapshot.tags,
            crate::types::LoadedBucketSubresource::NotRequested
        ));
    }

    #[test]
    fn object_read_snapshot_subject_rejects_changed_object_row() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0, 1]).unwrap();
        let bucket = create_bucket_for_snapshot_test(&node, "bucket");
        let key = object_key("key");
        let original =
            put_standard_object_for_snapshot_test(&node, &bucket, &key, GenerationId::MIN, 1);
        let subject = node
            .load_object_read_auth_subject(&bucket, &key, None)
            .unwrap();
        assert_eq!(subject.stored, original);

        put_standard_object_for_snapshot_test(
            &node,
            &bucket,
            &key,
            GenerationId::new(2).unwrap(),
            2,
        );

        let err = node
            .load_object_read_snapshot_for_subject(
                &bucket,
                &key,
                None,
                &subject.identity,
                crate::ObjectReadSnapshotMode::MetadataOnly,
            )
            .unwrap_err();
        assert!(matches!(err, ObjectPgActionError::StaleObjectReadSubject));

        let fresh_subject = node
            .load_object_read_auth_subject(&bucket, &key, None)
            .unwrap();
        let snapshot = node
            .load_object_read_snapshot_for_subject(
                &bucket,
                &key,
                None,
                &fresh_subject.identity,
                crate::ObjectReadSnapshotMode::MetadataOnly,
            )
            .unwrap();
        assert_eq!(snapshot.stored, fresh_subject.stored);
    }

    #[test]
    fn object_read_snapshot_subject_treats_missing_object_as_stale() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0, 1]).unwrap();
        let bucket = create_bucket_for_snapshot_test(&node, "bucket");
        let key = object_key("key");
        put_standard_object_for_snapshot_test(&node, &bucket, &key, GenerationId::MIN, 1);
        let subject = node
            .load_object_read_auth_subject(&bucket, &key, None)
            .unwrap();

        let object_pg = node
            .get_pg(node.pg_topology().object_pg_for(&bucket, &key))
            .unwrap();
        object_pg.delete_object_meta(&bucket, &key).unwrap();
        drop(object_pg);

        let err = node
            .load_object_read_snapshot_for_subject(
                &bucket,
                &key,
                None,
                &subject.identity,
                crate::ObjectReadSnapshotMode::MetadataOnly,
            )
            .unwrap_err();
        assert!(matches!(err, ObjectPgActionError::StaleObjectReadSubject));
    }

    #[test]
    fn load_bucket_snapshot_keeps_loaded_tag_view_after_bucket_mutation() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0, 1]).unwrap();
        let bucket = create_bucket_for_snapshot_test(&node, "bucket");
        let bucket_pg_id = node.pg_topology().bucket_pg_for(&bucket);
        let bucket_pg = node.get_pg(bucket_pg_id).unwrap();
        bucket_pg
            .put_bucket_subresource(
                &bucket,
                crate::types::PutBucketSubresource {
                    kind: crate::types::BucketSubresourceKind::Tagging,
                    body: "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>",
                    aux: crate::types::BucketSubresourceAux::None,
                },
            )
            .unwrap();
        bucket_pg.put_bucket_abac_enabled(&bucket, true).unwrap();
        drop(bucket_pg);

        let snapshot = node
            .load_bucket_snapshot(
                &bucket,
                crate::types::BucketSnapshotRequest {
                    tags: crate::types::BucketSnapshotTagsRequest::IfBucketAbacEnabled,
                    ..Default::default()
                },
            )
            .unwrap();

        let bucket_pg = node.get_pg(bucket_pg_id).unwrap();
        bucket_pg
            .put_bucket_subresource(
                &bucket,
                crate::types::PutBucketSubresource {
                    kind: crate::types::BucketSubresourceKind::Tagging,
                    body: "<Tagging><TagSet><Tag><Key>security</Key><Value>private</Value></Tag></TagSet></Tagging>",
                    aux: crate::types::BucketSubresourceAux::None,
                },
            )
            .unwrap();

        assert_eq!(
            snapshot.tags,
            crate::types::LoadedBucketSubresource::Loaded(
                "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>".to_owned()
            )
        );
    }

    #[test]
    fn finish_bucket_write_snapshot_operation_preserves_action_error_over_release_error() {
        let result = SharedStorageNode::finish_bucket_write_snapshot_operation::<(), &'static str>(
            Ok(Err("action failed")),
            Err(crate::error::MetadataError::BucketWriteDraining.into()),
        )
        .unwrap();
        assert_eq!(result, Err("action failed"));
    }

    #[test]
    fn try_finalize_bucket_delete_finalizes_empty_deleting_bucket() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0, 1]).unwrap();
        let bucket = create_bucket_for_snapshot_test(&node, "bucket");
        node.mark_bucket_deleting(&bucket).unwrap();

        assert_eq!(
            node.try_finalize_bucket_delete(&bucket).unwrap(),
            BucketDeleteFinalizeOutcome::Finalized
        );
        assert_eq!(
            node.try_finalize_bucket_delete(&bucket).unwrap(),
            BucketDeleteFinalizeOutcome::NotFound
        );
    }

    #[test]
    fn shared_node_read_shard_file_roundtrip() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0]).unwrap();
        let key = crate::types::ShardKey::new(&[0xAB; 16], 7, 0);
        {
            let pg = node.get_pg(0).unwrap();
            pg.write_shard(&key, b"hello").unwrap();
        }

        let data = node.read_shard_file(0, &key).unwrap();
        assert_eq!(data, b"hello");
    }

    #[test]
    fn shared_node_read_shard_file_into_roundtrip() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0]).unwrap();
        let key = crate::types::ShardKey::new(&[0xBC; 16], 7, 0);
        {
            let pg = node.get_pg(0).unwrap();
            pg.write_shard(&key, b"hello").unwrap();
        }

        let mut buf = [0u8; 5];
        node.read_shard_file_into(0, &key, &mut buf).unwrap();
        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn shared_node_read_shard_file_into_length_mismatch() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0]).unwrap();
        let key = crate::types::ShardKey::new(&[0xCE; 16], 7, 0);
        {
            let pg = node.get_pg(0).unwrap();
            pg.write_shard(&key, b"hello").unwrap();
        }

        let mut buf = [0u8; 4];
        let err = node.read_shard_file_into(0, &key, &mut buf).unwrap_err();
        match err {
            StoreError::Io { context, source } => {
                assert_eq!(context, "read shard file length mismatch");
                assert_eq!(source.kind(), std::io::ErrorKind::InvalidData);
            }
            other => panic!("expected length mismatch io error, got {other:?}"),
        }
    }

    #[test]
    fn shared_node_read_shard_file_missing() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0]).unwrap();
        let key = crate::types::ShardKey::new(&[0xCD; 16], 7, 0);
        let err = node.read_shard_file(0, &key).unwrap_err();
        assert!(matches!(err, StoreError::NotFound));
    }

    #[test]
    fn shared_node_write_shard_file_requires_metadata_registration() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0]).unwrap();
        let key = crate::types::ShardKey::new(&[0xDD; 16], 7, 0);

        let ack = node.write_shard_file(0, &key, b"hello").unwrap();
        assert_eq!(node.read_shard_file(0, &key).unwrap(), b"hello");

        let pg = node.get_pg(0).unwrap();
        let err = pg.read_shard(&key).unwrap_err();
        assert!(matches!(err, StoreError::NotFound));
        pg.register_written_shard(&key, ack).unwrap();
        assert_eq!(pg.read_shard(&key).unwrap().data, b"hello");
    }

    #[test]
    fn shared_node_scavenger_file_scan_does_not_wait_for_pg_mutex() {
        let tmp = test_util::tempdir();
        let node = Arc::new(SharedStorageNode::open(tmp.path(), &[0]).unwrap());
        let key = crate::types::ShardKey::new(&[0xDE; 16], 7, 0);
        node.write_shard_file(0, &key, b"hello").unwrap();

        let pg_guard = node.get_pg(0).unwrap();
        let scan_node = Arc::clone(&node);
        let (tx, rx) = std::sync::mpsc::channel();
        let handle = std::thread::spawn(move || {
            let result = scan_node
                .list_scavenger_shard_files(0)
                .map(|scan| scan.files.len());
            tx.send(result).unwrap();
        });

        let scanned = rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("scavenger file scan should not wait for PgStore mutex")
            .unwrap();
        assert_eq!(scanned, 1);
        drop(pg_guard);
        handle.join().unwrap();
    }

    #[test]
    fn shared_node_bucket_lock_same_bucket_blocks() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0]).unwrap();

        let guard = node.lock_bucket(&bucket_name("bucket-a"));
        assert!(
            node.try_lock_bucket(&bucket_name("bucket-a")).is_none(),
            "second lock on same bucket should be blocked while first guard is held"
        );
        drop(guard);
        assert!(
            node.try_lock_bucket(&bucket_name("bucket-a")).is_some(),
            "bucket lock should become acquirable again after the first guard is dropped"
        );
    }

    #[test]
    fn shared_node_bucket_lock_different_buckets_do_not_deadlock() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0]).unwrap();
        let a_guard = node.lock_bucket(&bucket_name("bucket-a"));
        // Different bucket may map to the same stripe, but this must never deadlock.
        // We only assert that taking locks in sequence is safe.
        drop(a_guard);
        let _b_guard = node.lock_bucket(&bucket_name("bucket-b"));
    }
}
