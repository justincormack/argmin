/// LocalStorageNode and SharedStorageNode — manage multiple PgStores on a single node.
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(any(test, feature = "test-hooks"))]
use std::sync::{Arc, OnceLock};
use std::sync::{Condvar, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Instant;

use rapidhash::v3::{rapidhash_v3_micro_inline, RapidSecrets};

use crate::error::{
    BucketSnapshotLoadError, BucketWriteDrainError, ObjectPgActionError, StoreError,
};
use crate::pg_store::PgStore;
use crate::pg_topology::PgTopology;
use crate::traits::{PgMetadataStore, ShardStore, StorageNode};
use crate::types::{
    AbortMultipartUploadLookup, BucketFastPathInfo, BucketInfo, BucketName, BucketSnapshot,
    BucketSnapshotPair, BucketSnapshotRequest, BucketSnapshotTagsRequest, BucketState,
    BucketSubresourceKind, CreateStreamUploadReq, FinalizeStreamPartOutcome, GenerationId,
    ListMultipartUploadsReq, ListObjectVersionsReq, ListPartsReq, ListedBucketMultipartUploads,
    ListedBucketObjectVersions, ListedBucketObjects, ListedMultipartParts, LoadedBucketSubresource,
    MultipartCompletionPreflight, MultipartCompletionSnapshot, MultipartPartRecord,
    MultipartPartSegmentRecord, MultipartUploadRecord, ObjectKey, ObjectReadSnapshot,
    ObjectReadSnapshotOutcome, PreparedStreamPartCommit, SessionId, ShardKey, StoredObject,
    StreamUploadPartSnapshot, StreamUploadState, StreamUploadTarget, UploadId, UploadState,
    WriteAck,
};

const TRACE_TARGET: &str = "storage";
const RAPIDHASH_SECRETS: RapidSecrets = RapidSecrets::seed(0);
const LOCK_WAIT_EVENT_THRESHOLD_US: u128 = 1_000;
const RECLAIM_WORKER_WAIT_POLL_MILLIS: u64 = 100;

mod bucket_ops;
mod listing_ops;
mod multipart_ops;
mod object_delete_ops;
mod object_metadata_ops;
mod object_read_ops;
mod stream_ops;

fn read_rwlock_unpoisoned<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|err| err.into_inner())
}

fn write_rwlock_unpoisoned<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(|err| err.into_inner())
}

struct PgDataPaths {
    shards_dir: PathBuf,
    tmp_dir: PathBuf,
}

pub struct BucketLockGuard<'a> {
    guard: MutexGuard<'a, ()>,
}

impl Drop for BucketLockGuard<'_> {
    fn drop(&mut self) {
        let _ = &self.guard;
    }
}

pub enum BucketPairPgGuards<'a> {
    Same {
        bucket: MutexGuard<'a, PgStore>,
    },
    Distinct {
        source: MutexGuard<'a, PgStore>,
        destination: MutexGuard<'a, PgStore>,
    },
}

pub struct BucketWriteDrainGuard<'a> {
    node: &'a SharedStorageNode,
    bucket: BucketName,
    persisted: bool,
}

#[cfg(any(test, feature = "test-hooks"))]
#[derive(Default, Clone)]
pub struct BucketScopedTestHooks {
    pub target: Option<BucketName>,
    pub before_bucket_lock_acquire: Option<Arc<dyn Fn() + Send + Sync>>,
    pub before_bucket_write_drain_wait: Option<Arc<dyn Fn() + Send + Sync>>,
    pub before_bucket_write_reservation_retry: Option<Arc<dyn Fn() + Send + Sync>>,
    pub after_bucket_write_reservation_retry: Option<Arc<dyn Fn() + Send + Sync>>,
    pub after_begin_bucket_delete_drain: Option<Arc<dyn Fn() + Send + Sync>>,
    pub before_multipart_completion_lock: Option<Arc<dyn Fn() + Send + Sync>>,
    pub after_multipart_completion_lock: Option<Arc<dyn Fn() + Send + Sync>>,
}

#[cfg(any(test, feature = "test-hooks"))]
static BUCKET_SCOPED_TEST_HOOKS: OnceLock<Mutex<BucketScopedTestHooks>> = OnceLock::new();

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

#[cfg(not(any(test, feature = "test-hooks")))]
pub(super) fn maybe_run_before_bucket_lock_acquire_hook(_: &BucketName) {}

#[cfg(any(test, feature = "test-hooks"))]
pub(super) fn maybe_run_bucket_write_drain_wait_hook(bucket: &BucketName) {
    maybe_run_bucket_scoped_test_hook(bucket, |hooks| hooks.before_bucket_write_drain_wait)
}

#[cfg(not(any(test, feature = "test-hooks")))]
pub(super) fn maybe_run_bucket_write_drain_wait_hook(_: &BucketName) {}

#[cfg(any(test, feature = "test-hooks"))]
pub(super) fn maybe_run_bucket_write_reservation_retry_hook(bucket: &BucketName) {
    maybe_run_bucket_scoped_test_hook(bucket, |hooks| hooks.before_bucket_write_reservation_retry)
}

#[cfg(not(any(test, feature = "test-hooks")))]
pub(super) fn maybe_run_bucket_write_reservation_retry_hook(_: &BucketName) {}

#[cfg(any(test, feature = "test-hooks"))]
pub(super) fn maybe_run_after_bucket_write_reservation_retry_hook(bucket: &BucketName) {
    maybe_run_bucket_scoped_test_hook(bucket, |hooks| hooks.after_bucket_write_reservation_retry)
}

#[cfg(not(any(test, feature = "test-hooks")))]
pub(super) fn maybe_run_after_bucket_write_reservation_retry_hook(_: &BucketName) {}

#[cfg(any(test, feature = "test-hooks"))]
pub(super) fn maybe_run_after_begin_bucket_delete_drain_hook(bucket: &BucketName) {
    maybe_run_bucket_scoped_test_hook(bucket, |hooks| hooks.after_begin_bucket_delete_drain)
}

#[cfg(not(any(test, feature = "test-hooks")))]
pub(super) fn maybe_run_after_begin_bucket_delete_drain_hook(_: &BucketName) {}

#[cfg(any(test, feature = "test-hooks"))]
pub(super) fn maybe_run_before_multipart_completion_lock_hook(bucket: &BucketName) {
    maybe_run_bucket_scoped_test_hook(bucket, |hooks| hooks.before_multipart_completion_lock)
}

#[cfg(not(any(test, feature = "test-hooks")))]
pub(super) fn maybe_run_before_multipart_completion_lock_hook(_: &BucketName) {}

#[cfg(any(test, feature = "test-hooks"))]
pub(super) fn maybe_run_after_multipart_completion_lock_hook(bucket: &BucketName) {
    maybe_run_bucket_scoped_test_hook(bucket, |hooks| hooks.after_multipart_completion_lock)
}

#[cfg(not(any(test, feature = "test-hooks")))]
pub(super) fn maybe_run_after_multipart_completion_lock_hook(_: &BucketName) {}

impl BucketWriteDrainGuard<'_> {
    pub fn persist(mut self) {
        self.persisted = true;
    }
}

impl Drop for BucketWriteDrainGuard<'_> {
    fn drop(&mut self) {
        if !self.persisted {
            let _ = self.node.end_bucket_write_drain(&self.bucket);
        }
    }
}

impl<'a> BucketPairPgGuards<'a> {
    pub fn source(&self) -> &MutexGuard<'a, PgStore> {
        match self {
            Self::Same { bucket } => bucket,
            Self::Distinct { source, .. } => source,
        }
    }

    pub fn destination(&self) -> &MutexGuard<'a, PgStore> {
        match self {
            Self::Same { bucket } => bucket,
            Self::Distinct { destination, .. } => destination,
        }
    }
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
    pub fn get_pg(&self, pg_id: u32) -> Result<&PgStore, StoreError> {
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
    data_dir: PathBuf,
    bucket_locks: Vec<Mutex<()>>,
    multipart_completion_locks: Vec<Mutex<()>>,
    bucket_fast_path: RwLock<HashMap<BucketName, BucketFastPathInfo>>,
    object_payload_leases: Mutex<HashMap<(BucketName, ObjectKey, GenerationId), usize>>,
    reclaim_queue: (Mutex<ReclaimQueueState>, Condvar),
}

type ReclaimRoot = (BucketName, ObjectKey, GenerationId);

pub enum ReclaimWorkItem {
    ObjectPayload(ReclaimRoot),
    BucketDelete(BucketName),
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
    object_queue: VecDeque<ReclaimRoot>,
    queued_objects: HashSet<ReclaimRoot>,
    bucket_delete_queue: VecDeque<BucketName>,
    queued_bucket_deletes: HashSet<BucketName>,
}

const BUCKET_LOCK_STRIPES: usize = 256;

impl SharedStorageNode {
    /// Open a shared storage node, creating PG directories as needed.
    pub fn open(data_dir: &Path, pg_ids: &[u32]) -> Result<Self, StoreError> {
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

        let mut bucket_locks = Vec::with_capacity(BUCKET_LOCK_STRIPES);
        for _ in 0..BUCKET_LOCK_STRIPES {
            bucket_locks.push(Mutex::new(()));
        }
        let mut multipart_completion_locks = Vec::with_capacity(BUCKET_LOCK_STRIPES);
        for _ in 0..BUCKET_LOCK_STRIPES {
            multipart_completion_locks.push(Mutex::new(()));
        }

        Ok(Self {
            stores,
            pg_paths,
            pg_id_list,
            pg_topology: PgTopology::new(pg_ids).expect("shared storage node must have PGs"),
            data_dir: data_dir.to_path_buf(),
            bucket_locks,
            multipart_completion_locks,
            bucket_fast_path: RwLock::new(HashMap::new()),
            object_payload_leases: Mutex::new(HashMap::new()),
            reclaim_queue: (
                Mutex::new(ReclaimQueueState {
                    object_queue: VecDeque::new(),
                    queued_objects: HashSet::new(),
                    bucket_delete_queue: VecDeque::new(),
                    queued_bucket_deletes: HashSet::new(),
                }),
                Condvar::new(),
            ),
        })
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

    fn bucket_lock_index(&self, bucket: &BucketName) -> usize {
        (rapidhash_v3_micro_inline::<true, false>(bucket.as_str().as_bytes(), &RAPIDHASH_SECRETS)
            as usize)
            % self.bucket_locks.len()
    }

    /// Return the cached active-bucket fast-path metadata for `bucket`.
    pub fn get_bucket_fast_path(&self, bucket: &BucketName) -> Option<BucketFastPathInfo> {
        read_rwlock_unpoisoned(&self.bucket_fast_path)
            .get(bucket)
            .cloned()
    }

    /// Insert or replace the cached active-bucket fast-path metadata.
    pub fn upsert_bucket_fast_path(&self, info: BucketFastPathInfo) {
        write_rwlock_unpoisoned(&self.bucket_fast_path).insert(info.name.clone(), info);
    }

    /// Mutate the cached fast-path metadata if present.
    pub fn update_bucket_fast_path_if_present(
        &self,
        bucket: &BucketName,
        update: impl FnOnce(&mut BucketFastPathInfo),
    ) {
        if let Some(info) = write_rwlock_unpoisoned(&self.bucket_fast_path).get_mut(bucket) {
            update(info);
        }
    }

    /// Remove cached fast-path metadata for `bucket`.
    pub fn remove_bucket_fast_path(&self, bucket: &BucketName) {
        write_rwlock_unpoisoned(&self.bucket_fast_path).remove(bucket);
    }

    /// Lock a bucket-scoped stripe mutex.
    ///
    /// Coordinator bucket-mutating operations use this as a coarse per-bucket
    /// gate so multi-step flows (for example, delete-bucket emptiness check
    /// followed by delete) cannot interleave with concurrent writes that would
    /// make the bucket non-empty.
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

    /// Lock a bucket-scoped stripe mutex used to serialize multipart
    /// completion publication order across coordinators.
    pub fn lock_multipart_completion_bucket(&self, bucket: &BucketName) -> BucketLockGuard<'_> {
        observability::trace_scope!(
            TRACE_TARGET,
            "SharedStorageNode::lock_multipart_completion_bucket",
            "bucket={:?}",
            bucket
        );
        let idx = self.bucket_lock_index(bucket);
        let trace = observability::current_context();
        maybe_run_before_multipart_completion_lock_hook(bucket);
        let wait_started_at = Instant::now();
        let guard = self.multipart_completion_locks[idx]
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        maybe_run_after_multipart_completion_lock_hook(bucket);
        let wait_us = wait_started_at.elapsed().as_micros();
        if wait_us >= LOCK_WAIT_EVENT_THRESHOLD_US {
            if let Some(trace) = &trace {
                let _ = observability::emit_multipart_completion_bucket_lock_wait_exceeded(
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

    /// Lock and return a guard for the given PG.
    pub fn get_pg(&self, pg_id: u32) -> Result<MutexGuard<'_, PgStore>, StoreError> {
        observability::trace_scope!(TRACE_TARGET, "SharedStorageNode::get_pg", "pg_id={}", pg_id);
        let mutex = self
            .stores
            .get(&pg_id)
            .ok_or(StoreError::PgNotFound { pg_id })?;
        Ok(mutex.lock().unwrap_or_else(|e| e.into_inner()))
    }

    /// Write a shard file durably without taking the per-PG mutex.
    ///
    /// This is used on the write hot path so file IO and fsync do not hold the
    /// PG metadata lock. Callers must publish the corresponding shard row under
    /// the PG mutex afterward before the shard becomes visible to reads.
    pub fn write_shard_file(
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

    /// Write multiple shard files durably without taking the per-PG mutex.
    ///
    /// This batches the rename durability step across the affected shard
    /// directories for one segment write, while preserving the existing rule
    /// that shards do not become visible until metadata rows are published.
    pub fn write_shard_files(
        &self,
        pg_id: u32,
        shards: &[(ShardKey, &[u8])],
    ) -> Result<Vec<(ShardKey, WriteAck)>, StoreError> {
        let total_bytes: usize = shards.iter().map(|(_, data)| data.len()).sum();
        observability::trace_scope!(
            TRACE_TARGET,
            "SharedStorageNode::write_shard_files",
            "pg_id={} shards={} bytes={}",
            pg_id,
            shards.len(),
            total_bytes
        );
        let paths = self
            .pg_paths
            .get(&pg_id)
            .ok_or(StoreError::PgNotFound { pg_id })?;
        PgStore::write_shard_files_durable(&paths.tmp_dir, &paths.shards_dir, shards)
    }

    /// Read a shard file directly without taking the per-PG mutex.
    ///
    /// The coordinator uses this on the healthy read path and validates the
    /// assembled segment against `segment_crc64` before serving it. If the
    /// shard is missing or the segment checksum does not match, callers fall
    /// back to the fully locked `PgStore::read_shard` path for recovery and
    /// quarantine behavior.
    pub fn read_shard_file(&self, pg_id: u32, key: &ShardKey) -> Result<Vec<u8>, StoreError> {
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
    pub fn read_shard_file_into(
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

    /// Load enough live shards for a segment to support recovery while keeping
    /// PG access, shard status checks, and quarantine handling internal to
    /// storage.
    pub fn load_segment_shards_for_recovery(
        &self,
        shard_pg_id: u32,
        segment_okh: &[u8; 16],
        segment_vid: GenerationId,
        ec_k: u8,
        ec_m: u8,
    ) -> Result<Vec<Option<Vec<u8>>>, StoreError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "SharedStorageNode::load_segment_shards_for_recovery",
            "pg_id={} segment_vid={} k={} m={}",
            shard_pg_id,
            segment_vid.get(),
            ec_k,
            ec_m
        );
        let pg = self.get_pg(shard_pg_id)?;
        let k = ec_k as usize;
        let m = ec_m as usize;
        let mut all_shards = vec![None; k + m];
        let mut present_count = 0usize;

        let read_shard =
            |i: usize, all_shards: &mut [Option<Vec<u8>>], present_count: &mut usize| {
                let shard_key = ShardKey::new(segment_okh, segment_vid.get(), i as u8);
                if let Ok(sd) = pg.read_shard(&shard_key) {
                    all_shards[i] = Some(sd.data);
                    *present_count += 1;
                }
            };

        for i in 0..k {
            read_shard(i, &mut all_shards, &mut present_count);
        }

        if present_count < k {
            for i in k..(k + m) {
                if present_count >= k {
                    break;
                }
                read_shard(i, &mut all_shards, &mut present_count);
            }
        }

        if present_count < k {
            return Err(StoreError::NotFound);
        }

        Ok(all_shards)
    }

    /// Acquire an in-memory lease on an object payload generation.
    pub fn acquire_object_payload_lease(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) {
        let mut leases = self
            .object_payload_leases
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        *leases
            .entry((bucket.clone(), key.clone(), generation_id))
            .or_insert(0) += 1;
    }

    /// Release an in-memory lease on an object payload generation.
    ///
    /// Returns the remaining active lease count after release.
    pub fn release_object_payload_lease(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> usize {
        let mut leases = self
            .object_payload_leases
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let entry = leases
            .get_mut(&(bucket.clone(), key.clone(), generation_id))
            .expect("object payload lease release without acquire");
        *entry -= 1;
        let remaining = *entry;
        if remaining == 0 {
            leases.remove(&(bucket.clone(), key.clone(), generation_id));
        }
        remaining
    }

    /// Return the number of active object-payload leases for a generation.
    pub fn object_payload_lease_count(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> usize {
        let leases = self
            .object_payload_leases
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        leases
            .get(&(bucket.clone(), key.clone(), generation_id))
            .copied()
            .unwrap_or(0)
    }

    /// Return the number of active object-payload leases for a bucket.
    pub fn bucket_object_payload_lease_count(&self, bucket: &BucketName) -> usize {
        let leases = self
            .object_payload_leases
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        leases
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
    ) {
        let root = (bucket.clone(), key.clone(), generation_id);
        let (state_lock, cv) = &self.reclaim_queue;
        let mut state = state_lock.lock().unwrap_or_else(|e| e.into_inner());
        if state.queued_objects.insert(root.clone()) {
            state.object_queue.push_back(root);
            cv.notify_one();
        }
    }

    /// Queue a bucket for deferred final deletion once reclaim is drained.
    pub fn enqueue_bucket_delete_finalize(&self, bucket: &BucketName) {
        let (state_lock, cv) = &self.reclaim_queue;
        let mut state = state_lock.lock().unwrap_or_else(|e| e.into_inner());
        let bucket = bucket.clone();
        if state.queued_bucket_deletes.insert(bucket.clone()) {
            state.bucket_delete_queue.push_back(bucket);
            cv.notify_one();
        }
    }

    /// Take one queued reclaim work item if immediately available.
    pub fn try_take_reclaim_work(&self) -> Option<ReclaimWorkItem> {
        let mut state = self
            .reclaim_queue
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(root) = state.object_queue.pop_front() {
            state.queued_objects.remove(&root);
            return Some(ReclaimWorkItem::ObjectPayload(root));
        }
        let bucket = state.bucket_delete_queue.pop_front()?;
        state.queued_bucket_deletes.remove(&bucket);
        Some(ReclaimWorkItem::BucketDelete(bucket))
    }

    /// Block until reclaim work is available, or stop has been requested.
    pub fn wait_for_reclaim_work(&self, stop: &AtomicBool) -> Option<ReclaimWorkItem> {
        let (state_lock, cv) = &self.reclaim_queue;
        let mut state = state_lock.lock().unwrap_or_else(|e| e.into_inner());
        while state.object_queue.is_empty()
            && state.bucket_delete_queue.is_empty()
            && !stop.load(Ordering::SeqCst)
        {
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
        if let Some(root) = state.object_queue.pop_front() {
            state.queued_objects.remove(&root);
            return Some(ReclaimWorkItem::ObjectPayload(root));
        }
        let bucket = state.bucket_delete_queue.pop_front()?;
        state.queued_bucket_deletes.remove(&bucket);
        Some(ReclaimWorkItem::BucketDelete(bucket))
    }

    /// Wake reclaim workers so they can observe shutdown or new work.
    pub fn wake_reclaim_workers(&self) {
        self.reclaim_queue.1.notify_all();
    }

    /// Lock two PGs for operations that span a metadata PG and a shard PG.
    ///
    /// When both IDs are the same, returns `(guard, None)` — the caller uses
    /// the single guard for both roles. When different, locks in ascending
    /// ID order to prevent deadlocks and returns `(lower_guard, Some(higher_guard))`.
    ///
    /// The first returned guard corresponds to `pg_a`, the second to `pg_b`.
    pub fn lock_two_pgs(
        &self,
        pg_a: u32,
        pg_b: u32,
    ) -> Result<(MutexGuard<'_, PgStore>, Option<MutexGuard<'_, PgStore>>), StoreError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "SharedStorageNode::lock_two_pgs",
            "pg_a={} pg_b={}",
            pg_a,
            pg_b
        );
        if pg_a == pg_b {
            let guard = self.get_pg(pg_a)?;
            return Ok((guard, None));
        }

        // Lock in ascending order to prevent deadlocks.
        if pg_a < pg_b {
            let guard_a = self.get_pg(pg_a)?;
            let guard_b = self.get_pg(pg_b)?;
            Ok((guard_a, Some(guard_b)))
        } else {
            let guard_b = self.get_pg(pg_b)?;
            let guard_a = self.get_pg(pg_a)?;
            Ok((guard_a, Some(guard_b)))
        }
    }

    /// Lock a source/destination bucket PG pair while preserving request roles.
    ///
    /// Ordering is internal to storage. Callers provide the source and
    /// destination PG IDs in request-role order and receive role-preserving
    /// guards back.
    pub fn lock_bucket_pair_pgs(
        &self,
        source_pg_id: u32,
        destination_pg_id: u32,
    ) -> Result<BucketPairPgGuards<'_>, StoreError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "SharedStorageNode::lock_bucket_pair_pgs",
            "source_pg_id={} destination_pg_id={}",
            source_pg_id,
            destination_pg_id
        );
        let (source, destination) = self.lock_two_pgs(source_pg_id, destination_pg_id)?;
        Ok(match destination {
            Some(destination) => BucketPairPgGuards::Distinct {
                source,
                destination,
            },
            None => BucketPairPgGuards::Same { bucket: source },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::AssertUnwindSafe;
    use std::sync::{mpsc, Arc, OnceLock};

    static STORAGE_TEST_HOOK_SERIAL: OnceLock<Mutex<()>> = OnceLock::new();

    fn bucket_name(name: &str) -> BucketName {
        BucketName::try_from(name).unwrap()
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
    fn shared_node_bucket_fast_path_recovers_from_poisoned_lock() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0]).unwrap();
        let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let _guard = node.bucket_fast_path.write().unwrap();
            panic!("poison bucket fast path lock");
        }));

        let info = BucketFastPathInfo {
            name: crate::types::BucketName::try_from("bucket").unwrap(),
            owner_principal: "owner".to_string(),
            owner_canonical_id: s3_types::CanonicalUserId::from_principal("owner"),
            created_at: 0,
            state: crate::types::BucketState::Active,
            versioning: s3_types::BucketVersioningState::Disabled,
            object_lock: s3_types::BucketObjectLockConfig {
                enabled: false,
                default_retention: None,
            },
            acl_grants: s3_types::AclGrants::new(vec![]),
            public_read: false,
            public_write: false,
            public_access_block: None,
            ownership_controls: None,
            bucket_policy_present: false,
            bucket_policy_public: false,
            bucket_policy_generation: 0,
            bucket_lifecycle_present: false,
            bucket_lifecycle_generation: 0,
            bucket_abac_enabled: false,
            encryption: crate::types::EffectiveBucketEncryptionConfig::default(),
        };
        node.upsert_bucket_fast_path(info);
        assert_eq!(
            node.get_bucket_fast_path(&bucket_name("bucket"))
                .as_ref()
                .map(|entry| entry.name.as_str()),
            Some("bucket")
        );
        node.remove_bucket_fast_path(&bucket_name("bucket"));
        assert!(node.get_bucket_fast_path(&bucket_name("bucket")).is_none());
    }

    #[test]
    fn shared_node_lock_two_pgs_same() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0, 1]).unwrap();
        let (guard, opt) = node.lock_two_pgs(0, 0).unwrap();
        assert!(opt.is_none());
        assert_eq!(guard.pg_id(), 0);
    }

    #[test]
    fn shared_node_lock_two_pgs_different() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0, 1]).unwrap();
        let (guard_a, opt_b) = node.lock_two_pgs(0, 1).unwrap();
        assert_eq!(guard_a.pg_id(), 0);
        let guard_b = opt_b.unwrap();
        assert_eq!(guard_b.pg_id(), 1);
    }

    #[test]
    fn shared_node_lock_two_pgs_reversed_order() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0, 1]).unwrap();
        // Request pg_b=0, pg_a=1 — should still lock 0 first internally,
        // but return guards in the requested order.
        let (guard_a, opt_b) = node.lock_two_pgs(1, 0).unwrap();
        assert_eq!(guard_a.pg_id(), 1);
        let guard_b = opt_b.unwrap();
        assert_eq!(guard_b.pg_id(), 0);
    }

    #[test]
    fn shared_node_lock_bucket_pair_pgs_same() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0, 1]).unwrap();
        let guards = node.lock_bucket_pair_pgs(0, 0).unwrap();
        match guards {
            BucketPairPgGuards::Same { bucket } => assert_eq!(bucket.pg_id(), 0),
            BucketPairPgGuards::Distinct { .. } => panic!("expected same-bucket guards"),
        }
    }

    #[test]
    fn shared_node_lock_bucket_pair_pgs_preserves_roles() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0, 1]).unwrap();
        let guards = node.lock_bucket_pair_pgs(1, 0).unwrap();
        match guards {
            BucketPairPgGuards::Same { .. } => panic!("expected distinct-bucket guards"),
            BucketPairPgGuards::Distinct {
                source,
                destination,
            } => {
                assert_eq!(source.pg_id(), 1);
                assert_eq!(destination.pg_id(), 0);
            }
        }
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
    fn with_bucket_write_snapshot_loads_requested_subresources_and_releases() {
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
                    kind: crate::types::BucketSubresourceKind::Lifecycle,
                    body: "<LifecycleConfiguration><Rule><ID>r</ID><Status>Enabled</Status><Filter><Prefix></Prefix></Filter><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>",
                    aux: crate::types::BucketSubresourceAux::None,
                },
            )
            .unwrap();
        drop(bucket_pg);

        let first = node
            .with_bucket_write_snapshot(
                &bucket,
                crate::types::BucketSnapshotRequest {
                    lifecycle: true,
                    ..Default::default()
                },
                |snapshot| Ok::<_, ()>(snapshot.lifecycle),
            )
            .unwrap();
        assert!(matches!(
            first,
            Ok(crate::types::LoadedBucketSubresource::Loaded(_))
        ));

        let second = node
            .with_bucket_write_snapshot(&bucket, Default::default(), |snapshot| {
                Ok::<_, ()>(snapshot.bucket)
            })
            .unwrap();
        assert_eq!(second.unwrap().name, bucket);
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
    fn create_multipart_upload_releases_bucket_write_reservation_on_error() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0, 1]).unwrap();
        let bucket = create_bucket_for_snapshot_test(&node, "bucket");
        let key = ObjectKey::try_from("key").unwrap();

        let result = node.create_multipart_upload(&bucket, &key, Default::default(), |_, _| {
            Ok::<_, ()>((
                (),
                crate::types::CreateMultipartUploadReq {
                    upload_id: crate::tests::multipart_upload_id("upload"),
                    bucket: bucket.clone(),
                    key: key.clone(),
                    tags: None,
                    metadata_blob: vec![].into(),
                    system_metadata_blob: crate::types::SerializedSystemMetadataBlob::default(),
                    initiator: None,
                    owner: crate::types::OwnerIdentity::new(
                        "o".repeat(300),
                        s3_types::CanonicalUserId::from_principal("owner"),
                    ),
                    acl_grants: s3_types::AclGrants::default(),
                    public_read: false,
                    object_lock: crate::types::ObjectLockState::default(),
                    checksum: None,
                    encryption: crate::types::ObjectEncryption::default(),
                },
            ))
        });
        assert!(result.is_err());

        let bucket_pg = node
            .get_pg(node.pg_topology().bucket_pg_for(&bucket))
            .unwrap();
        let raw = bucket_pg.head_bucket_raw(&bucket).unwrap();
        assert_eq!(raw.active_write_reservations, 0);
    }

    #[test]
    fn create_put_object_stream_session_releases_bucket_write_reservation_on_error() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0, 1]).unwrap();
        let bucket = create_bucket_for_snapshot_test(&node, "bucket");
        let key = ObjectKey::try_from("key").unwrap();
        let session_id = crate::tests::stream_session_id("session");

        node.create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::types::ObjectEncryption::default(),
        )
        .unwrap();

        let result =
            node.create_put_object_stream_session(&bucket, &key, Default::default(), |_, _| {
                Ok::<_, ()>((
                    (),
                    crate::types::CreateStreamUploadReq {
                        session_id: session_id.clone(),
                        bucket: bucket.clone(),
                        key: key.clone(),
                        target: crate::types::StreamUploadTarget::PutObject,
                        encryption: crate::types::ObjectEncryption::default(),
                    },
                ))
            });
        assert!(result.is_err());

        let bucket_pg = node
            .get_pg(node.pg_topology().bucket_pg_for(&bucket))
            .unwrap();
        let raw = bucket_pg.head_bucket_raw(&bucket).unwrap();
        assert_eq!(raw.active_write_reservations, 0);
    }

    #[test]
    fn commit_stream_segment_append_cleans_up_cross_pg_loser_shards_on_late_duplicate() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0, 1, 2, 3]).unwrap();
        let bucket = create_bucket_for_snapshot_test(&node, "bucket");
        let key = ObjectKey::try_from("key").unwrap();
        let meta_pg_id = node.pg_topology().object_pg_for(&bucket, &key);

        let (session_id, request, loser_record) = (0..128)
            .find_map(|i| {
                let session_id = crate::tests::stream_session_id(format!("session{i}"));
                node.create_put_object_stream_session_record(
                    &bucket,
                    &key,
                    &session_id,
                    crate::types::ObjectEncryption::default(),
                )
                .unwrap();
                let request = crate::types::PrepareStreamUploadSegmentAppendReq {
                    session_id: session_id.clone(),
                    segment_index: 0,
                    size: 5,
                    segment_crc64: Some(checksum::crc64::checksum(b"hello")),
                    segment_okh: [i as u8; 16],
                    ec: crate::types::EcShape { k: 1, m: 0 },
                };
                let (_, record) = node
                    .prepare_stream_segment_append(&bucket, &key, &request)
                    .unwrap();
                (record.shard_pg_id != meta_pg_id).then_some((session_id, request, record))
            })
            .expect("expected a cross-PG streaming segment routing case");

        let shard_key = ShardKey::new(&loser_record.segment_okh, loser_record.segment_vid.get(), 0);
        let ack = {
            let shard_pg = node.get_pg(loser_record.shard_pg_id).unwrap();
            shard_pg.write_shard(&shard_key, b"hello").unwrap()
        };
        let shard_batch = vec![(&shard_key, ack)];

        let winner_vid = {
            let meta_pg = node.get_pg(meta_pg_id).unwrap();
            let winner_vid = meta_pg.allocate_stream_segment_vid(&session_id).unwrap();
            meta_pg
                .append_stream_segment(&crate::types::StreamUploadSegmentRecord {
                    session_id: session_id.clone(),
                    segment_index: request.segment_index,
                    size: request.size,
                    segment_crc64: request.segment_crc64,
                    segment_okh: [0x55; 16],
                    segment_vid: winner_vid,
                    shard_pg_id: meta_pg_id,
                    ec_k: request.ec.k,
                    ec_m: request.ec.m,
                })
                .unwrap();
            winner_vid
        };

        let err = node
            .commit_stream_segment_append(
                &bucket,
                &key,
                &session_id,
                request.segment_index,
                &loser_record,
                &shard_batch,
            )
            .unwrap_err();
        assert!(matches!(
            err,
            crate::error::ObjectPgActionError::InvalidRequest { reason }
                if reason == "duplicate segment_index 0"
        ));

        let shard_pg = node.get_pg(loser_record.shard_pg_id).unwrap();
        assert!(matches!(
            shard_pg.read_shard(&shard_key),
            Err(crate::error::StoreError::NotFound)
        ));
        let meta_pg = node.get_pg(meta_pg_id).unwrap();
        let segments = meta_pg.list_stream_segments(&session_id).unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].segment_vid, winner_vid);
    }

    #[test]
    fn bucket_write_drain_guard_releases_on_drop() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0, 1]).unwrap();
        let bucket = create_bucket_for_snapshot_test(&node, "bucket");

        {
            let _drain = node.begin_bucket_write_drain(&bucket).unwrap();
        }

        let result = node
            .with_bucket_write_snapshot(&bucket, Default::default(), |snapshot| {
                Ok::<_, ()>(snapshot.bucket)
            })
            .unwrap();
        assert_eq!(result.unwrap().name, bucket);
    }

    #[test]
    fn with_bucket_write_snapshot_waits_for_temporary_drain_then_succeeds() {
        let tmp = test_util::tempdir();
        let node = Arc::new(SharedStorageNode::open(tmp.path(), &[0, 1]).unwrap());
        let bucket = create_bucket_for_snapshot_test(&node, "bucket");

        let _serial = STORAGE_TEST_HOOK_SERIAL
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap();
        let (retry_tx, retry_rx) = mpsc::channel();
        let _hook_guard = install_bucket_scoped_test_hooks(BucketScopedTestHooks {
            target: Some(bucket.clone()),
            after_bucket_write_reservation_retry: Some(Arc::new(move || {
                let _ = retry_tx.send(());
            })),
            ..BucketScopedTestHooks::default()
        });

        let drain = node.begin_bucket_write_drain(&bucket).unwrap();
        let node_for_thread = Arc::clone(&node);
        let bucket_for_thread = bucket.clone();
        let (result_tx, result_rx) = mpsc::channel();
        let handle = std::thread::spawn(move || {
            let result = node_for_thread
                .with_bucket_write_snapshot(&bucket_for_thread, Default::default(), |snapshot| {
                    Ok::<_, ()>(snapshot.bucket)
                })
                .unwrap();
            result_tx.send(result).unwrap();
        });

        retry_rx.recv().unwrap();
        assert!(matches!(
            result_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));

        drop(drain);

        let result = result_rx.recv().unwrap();
        assert_eq!(result.unwrap().name, bucket);
        handle.join().unwrap();
    }

    #[test]
    fn with_bucket_write_snapshot_stops_waiting_once_bucket_delete_becomes_terminal() {
        let tmp = test_util::tempdir();
        let node = Arc::new(SharedStorageNode::open(tmp.path(), &[0, 1]).unwrap());
        let bucket = create_bucket_for_snapshot_test(&node, "bucket");

        #[derive(Debug)]
        enum RequestEvent {
            FirstRetryObserved,
            UnexpectedExtraRetry,
            Completed(Result<Result<BucketInfo, ()>, crate::error::BucketSnapshotLoadError>),
        }

        let _serial = STORAGE_TEST_HOOK_SERIAL
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap();
        let (delete_paused_tx, delete_paused_rx) = mpsc::channel();
        let delete_release = Arc::new((Mutex::new(false), Condvar::new()));
        let retry_release = Arc::new((Mutex::new(false), Condvar::new()));
        let retry_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (event_tx, event_rx) = mpsc::channel();
        let event_tx_for_hook = event_tx.clone();
        let retry_count_hook = Arc::clone(&retry_count);
        let delete_release_hook = Arc::clone(&delete_release);
        let retry_release_hook = Arc::clone(&retry_release);
        let _hook_guard = install_bucket_scoped_test_hooks(BucketScopedTestHooks {
            target: Some(bucket.clone()),
            after_begin_bucket_delete_drain: Some(Arc::new(move || {
                let _ = delete_paused_tx.send(());
                let (lock, cvar) = &*delete_release_hook;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = cvar.wait(released).unwrap();
                }
            })),
            after_bucket_write_reservation_retry: Some(Arc::new(move || {
                match retry_count_hook.fetch_add(1, Ordering::SeqCst) {
                    0 => {
                        let _ = event_tx_for_hook.send(RequestEvent::FirstRetryObserved);
                        let (lock, cvar) = &*retry_release_hook;
                        let mut released = lock.lock().unwrap();
                        while !*released {
                            released = cvar.wait(released).unwrap();
                        }
                    }
                    _ => {
                        let _ = event_tx_for_hook.send(RequestEvent::UnexpectedExtraRetry);
                    }
                }
            })),
            ..BucketScopedTestHooks::default()
        });

        let node_for_delete = Arc::clone(&node);
        let bucket_for_delete = bucket.clone();
        let (delete_tx, delete_rx) = mpsc::channel();
        let delete_handle = std::thread::spawn(move || {
            let result = node_for_delete.begin_bucket_delete(&bucket_for_delete);
            delete_tx.send(result).unwrap();
        });

        delete_paused_rx.recv().unwrap();

        let node_for_request = Arc::clone(&node);
        let bucket_for_request = bucket.clone();
        let request_handle = std::thread::spawn(move || {
            let result = node_for_request.with_bucket_write_snapshot(
                &bucket_for_request,
                Default::default(),
                |snapshot| Ok::<_, ()>(snapshot.bucket),
            );
            event_tx.send(RequestEvent::Completed(result)).unwrap();
        });

        match event_rx.recv().unwrap() {
            RequestEvent::FirstRetryObserved => {}
            other => panic!("expected first retry event, got {other:?}"),
        }

        {
            let (lock, cvar) = &*delete_release;
            let mut released = lock.lock().unwrap();
            *released = true;
            cvar.notify_all();
        }
        delete_handle.join().unwrap();
        delete_rx.recv().unwrap().unwrap();
        {
            let (lock, cvar) = &*retry_release;
            let mut released = lock.lock().unwrap();
            *released = true;
            cvar.notify_all();
        }

        match event_rx.recv().unwrap() {
            RequestEvent::Completed(Err(crate::error::BucketSnapshotLoadError::Metadata(
                crate::error::MetadataError::BucketNotFound { .. },
            ))) => {}
            RequestEvent::UnexpectedExtraRetry => {
                panic!("request retried again after bucket delete became terminal")
            }
            other => panic!("expected BucketNotFound completion, got {other:?}"),
        }
        request_handle.join().unwrap();
    }

    #[test]
    fn begin_bucket_delete_rejects_nonempty_bucket() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0, 1]).unwrap();
        let bucket = create_bucket_for_snapshot_test(&node, "bucket");
        let object_pg = node
            .get_pg(node.pg_topology().object_pg(bucket.as_str(), "key"))
            .unwrap();
        object_pg
            .create_multipart_upload(&crate::types::CreateMultipartUploadReq {
                upload_id: crate::tests::multipart_upload_id("upload"),
                bucket: bucket.clone(),
                key: ObjectKey::try_from("key").unwrap(),
                tags: None,
                metadata_blob: vec![].into(),
                system_metadata_blob: crate::types::SerializedSystemMetadataBlob::default(),
                initiator: None,
                owner: crate::types::OwnerIdentity::from_principal("owner"),
                acl_grants: s3_types::AclGrants::default(),
                public_read: false,
                object_lock: crate::types::ObjectLockState::default(),
                checksum: None,
                encryption: crate::types::ObjectEncryption::None,
            })
            .unwrap();
        drop(object_pg);

        let err = node.begin_bucket_delete(&bucket).unwrap_err();
        assert!(matches!(
            err,
            BucketWriteDrainError::Metadata(crate::error::MetadataError::BucketNotEmpty)
        ));
    }

    #[test]
    fn try_finalize_bucket_delete_finalizes_empty_deleting_bucket() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0, 1]).unwrap();
        let bucket = create_bucket_for_snapshot_test(&node, "bucket");
        node.begin_bucket_delete(&bucket).unwrap();

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
    fn shared_node_write_shard_files_requires_metadata_registration() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0]).unwrap();
        let key_a = crate::types::ShardKey::new(&[0xEE; 16], 7, 0);
        let key_b = crate::types::ShardKey::new(&[0xEE; 16], 7, 1);

        let written = node
            .write_shard_files(
                0,
                &[
                    (key_a.clone(), b"hello".as_slice()),
                    (key_b.clone(), b"world".as_slice()),
                ],
            )
            .unwrap();
        assert_eq!(node.read_shard_file(0, &key_a).unwrap(), b"hello");
        assert_eq!(node.read_shard_file(0, &key_b).unwrap(), b"world");

        let pg = node.get_pg(0).unwrap();
        assert!(matches!(pg.read_shard(&key_a), Err(StoreError::NotFound)));
        assert!(matches!(pg.read_shard(&key_b), Err(StoreError::NotFound)));
        for (key, ack) in written {
            pg.register_written_shard(&key, ack).unwrap();
        }
        assert_eq!(pg.read_shard(&key_a).unwrap().data, b"hello");
        assert_eq!(pg.read_shard(&key_b).unwrap().data, b"world");
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
