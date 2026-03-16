/// LocalStorageNode and SharedStorageNode — manage multiple PgStores on a single node.
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex, MutexGuard};
use std::time::Instant;

use crate::error::StoreError;
use crate::pg_store::PgStore;
use crate::traits::{ShardStore, StorageNode};
use crate::types::GenerationId;

const TRACE_TARGET: &str = "storage";

pub struct BucketLockGuard<'a> {
    guard: MutexGuard<'a, ()>,
    bucket: String,
    stripe: usize,
    acquired_at: Instant,
    trace: Option<observability::TraceContext>,
}

impl Drop for BucketLockGuard<'_> {
    fn drop(&mut self) {
        let _ = &self.guard;
        if let Some(trace) = &self.trace {
            let _ = observability::event_in_context(
                trace,
                TRACE_TARGET,
                "bucket_lock_released",
                Some(format_args!(
                    "bucket={} stripe={} hold_us={}",
                    self.bucket,
                    self.stripe,
                    self.acquired_at.elapsed().as_micros()
                )),
            );
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
    pg_id_list: Vec<u32>,
    data_dir: PathBuf,
    bucket_locks: Vec<Mutex<()>>,
    object_payload_leases: Mutex<HashMap<(String, String, GenerationId), usize>>,
    reclaim_queue: (Mutex<ReclaimQueueState>, Condvar),
}

type ReclaimRoot = (String, String, GenerationId);

pub enum ReclaimWorkItem {
    ObjectPayload(ReclaimRoot),
    BucketDelete(String),
}

struct ReclaimQueueState {
    object_queue: VecDeque<ReclaimRoot>,
    queued_objects: HashSet<ReclaimRoot>,
    bucket_delete_queue: VecDeque<String>,
    queued_bucket_deletes: HashSet<String>,
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
        let mut pg_id_list = Vec::with_capacity(pg_ids.len());

        for &pg_id in pg_ids {
            let pg_dir = data_dir.join(format!("pg-{pg_id:04}"));
            let store = PgStore::open(&pg_dir, pg_id)?;
            stores.insert(pg_id, Mutex::new(store));
            pg_id_list.push(pg_id);
        }

        pg_id_list.sort_unstable();

        let mut bucket_locks = Vec::with_capacity(BUCKET_LOCK_STRIPES);
        for _ in 0..BUCKET_LOCK_STRIPES {
            bucket_locks.push(Mutex::new(()));
        }

        Ok(Self {
            stores,
            pg_id_list,
            data_dir: data_dir.to_path_buf(),
            bucket_locks,
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

    fn bucket_lock_index(&self, bucket: &str) -> usize {
        (rapidhash::rapidhash(bucket.as_bytes()) as usize) % self.bucket_locks.len()
    }

    /// Lock a bucket-scoped stripe mutex.
    ///
    /// Coordinator bucket-mutating operations use this as a coarse per-bucket
    /// gate so multi-step flows (for example, delete-bucket emptiness check
    /// followed by delete) cannot interleave with concurrent writes that would
    /// make the bucket non-empty.
    pub fn lock_bucket(&self, bucket: &str) -> BucketLockGuard<'_> {
        observability::trace_scope!(
            TRACE_TARGET,
            "SharedStorageNode::lock_bucket",
            "bucket={}",
            bucket
        );
        let idx = self.bucket_lock_index(bucket);
        let trace = observability::current_context();
        let wait_started_at = Instant::now();
        let guard = self.bucket_locks[idx]
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let wait_us = wait_started_at.elapsed().as_micros();
        let acquired_at = Instant::now();
        if let Some(trace) = &trace {
            let _ = observability::event_in_context(
                trace,
                TRACE_TARGET,
                "bucket_lock_acquired",
                Some(format_args!(
                    "bucket={} stripe={} wait_us={}",
                    bucket, idx, wait_us
                )),
            );
        }
        BucketLockGuard {
            guard,
            bucket: bucket.to_string(),
            stripe: idx,
            acquired_at,
            trace,
        }
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

    /// Acquire an in-memory lease on an object payload generation.
    pub fn acquire_object_payload_lease(
        &self,
        bucket: &str,
        key: &str,
        generation_id: GenerationId,
    ) {
        let mut leases = self
            .object_payload_leases
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        *leases
            .entry((bucket.to_string(), key.to_string(), generation_id))
            .or_insert(0) += 1;
    }

    /// Release an in-memory lease on an object payload generation.
    ///
    /// Returns the remaining active lease count after release.
    pub fn release_object_payload_lease(
        &self,
        bucket: &str,
        key: &str,
        generation_id: GenerationId,
    ) -> usize {
        let mut leases = self
            .object_payload_leases
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let entry = leases
            .get_mut(&(bucket.to_string(), key.to_string(), generation_id))
            .expect("object payload lease release without acquire");
        *entry -= 1;
        let remaining = *entry;
        if remaining == 0 {
            leases.remove(&(bucket.to_string(), key.to_string(), generation_id));
        }
        remaining
    }

    /// Return the number of active object-payload leases for a generation.
    pub fn object_payload_lease_count(
        &self,
        bucket: &str,
        key: &str,
        generation_id: GenerationId,
    ) -> usize {
        let leases = self
            .object_payload_leases
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        leases
            .get(&(bucket.to_string(), key.to_string(), generation_id))
            .copied()
            .unwrap_or(0)
    }

    /// Return the number of active object-payload leases for a bucket.
    pub fn bucket_object_payload_lease_count(&self, bucket: &str) -> usize {
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
        bucket: &str,
        key: &str,
        generation_id: GenerationId,
    ) {
        let root = (bucket.to_string(), key.to_string(), generation_id);
        let (state_lock, cv) = &self.reclaim_queue;
        let mut state = state_lock.lock().unwrap_or_else(|e| e.into_inner());
        if state.queued_objects.insert(root.clone()) {
            state.object_queue.push_back(root);
            cv.notify_one();
        }
    }

    /// Queue a bucket for deferred final deletion once reclaim is drained.
    pub fn enqueue_bucket_delete_finalize(&self, bucket: &str) {
        let (state_lock, cv) = &self.reclaim_queue;
        let mut state = state_lock.lock().unwrap_or_else(|e| e.into_inner());
        let bucket = bucket.to_string();
        if state.queued_bucket_deletes.insert(bucket.clone()) {
            state.bucket_delete_queue.push_back(bucket);
            cv.notify_one();
        }
    }

    /// Block until reclaim work is available, or stop has been requested.
    pub fn wait_for_reclaim_work(&self, stop: &AtomicBool) -> Option<ReclaimWorkItem> {
        let (state_lock, cv) = &self.reclaim_queue;
        let mut state = state_lock.lock().unwrap_or_else(|e| e.into_inner());
        while state.object_queue.is_empty()
            && state.bucket_delete_queue.is_empty()
            && !stop.load(Ordering::SeqCst)
        {
            state = cv.wait(state).unwrap_or_else(|e| e.into_inner());
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn shared_node_bucket_lock_same_bucket_blocks() {
        use std::sync::mpsc::channel;
        use std::time::Duration;

        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0]).unwrap();

        let (tx, rx) = channel();
        std::thread::scope(|s| {
            let guard = node.lock_bucket("bucket-a");
            s.spawn(|| {
                let _g2 = node.lock_bucket("bucket-a");
                tx.send(()).unwrap();
            });

            // Second lock on same bucket should block while first guard is held.
            assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());
            drop(guard);
            rx.recv_timeout(Duration::from_secs(1)).unwrap();
        });
    }

    #[test]
    fn shared_node_bucket_lock_different_buckets_do_not_deadlock() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0]).unwrap();
        let a_guard = node.lock_bucket("bucket-a");
        // Different bucket may map to the same stripe, but this must never deadlock.
        // We only assert that taking locks in sequence is safe.
        drop(a_guard);
        let _b_guard = node.lock_bucket("bucket-b");
    }
}
