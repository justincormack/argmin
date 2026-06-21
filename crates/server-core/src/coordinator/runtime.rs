use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use s3_types::BucketLifecycleConfiguration;
use storage::{
    AuthorizedMultipartUploadRecord, BucketInfo, BucketName, EcShape, GenerationId,
    ObjectEncryption, ObjectKey, ReclaimWorkItem, SegmentStoredBytesRequest, StorageCluster,
    StorageClusterRuntimeMapHandle, StoreError, UploadId, UploadState, VersionId,
};

use super::payload::SharedPayloadBuffer;
use super::read_core::{PayloadLease, ReadRuntime, SegmentPayloadRecord};
#[cfg(test)]
use super::test_hooks::maybe_run_shard_repair_worker_idle_timeout_hook;
use super::TRACE_TARGET;
use super::{lock_mutex_unpoisoned, Coordinator, LIFECYCLE_SWEEP_INTERVAL_MILLIS};
#[cfg(test)]
use super::{trusted_bucket_name, trusted_object_key};
use crate::error::ServerError;
use crate::sse::{
    decrypt_managed_encryption_segment, decrypt_sse_customer_segment, SseCustomerRequest,
    SseCustomerSegmentScope,
};

static LIFECYCLE_SWEEPER_REGISTRY: OnceLock<Mutex<HashMap<usize, Weak<LifecycleSweeper>>>> =
    OnceLock::new();
static SHARD_SCAVENGER_SWEEPER_REGISTRY: OnceLock<
    Mutex<HashMap<usize, Weak<ShardScavengerSweeper>>>,
> = OnceLock::new();
static SHARD_REPAIR_SWEEPER_REGISTRY: OnceLock<Mutex<HashMap<usize, Weak<ShardRepairSweeper>>>> =
    OnceLock::new();
static SHARD_BACKFILL_SWEEPER_REGISTRY: OnceLock<
    Mutex<HashMap<usize, Weak<ShardBackfillSweeper>>>,
> = OnceLock::new();
static STREAM_SESSION_SWEEPER_REGISTRY: OnceLock<
    Mutex<HashMap<usize, Weak<StreamSessionSweeper>>>,
> = OnceLock::new();
static BACKGROUND_WORK_ADMISSION_REGISTRY: OnceLock<
    Mutex<HashMap<usize, Weak<BackgroundWorkAdmission>>>,
> = OnceLock::new();
static SHARD_REPAIR_CLAIM_COUNTER: AtomicU64 = AtomicU64::new(1);
static SHARD_BACKFILL_CLAIM_COUNTER: AtomicU64 = AtomicU64::new(1);

const OBJECT_PAYLOAD_RECLAIM_PG_RETRY_COOLDOWN: Duration = Duration::from_millis(100);
const RECLAIM_DURABLE_SCAN_INTERVAL: Duration = Duration::from_millis(250);
const SHARD_REPAIR_DURABLE_SCAN_INTERVAL: Duration = Duration::from_millis(250);
const SHARD_REPAIR_CLAIM_LEASE_MILLIS: u64 = 30_000;
const SHARD_REPAIR_ERROR_BACKOFF_MILLIS: u64 = 1_000;
const SHARD_BACKFILL_DURABLE_SCAN_INTERVAL: Duration = Duration::from_millis(250);
const SHARD_BACKFILL_CLAIM_LEASE_MILLIS: u64 = 30_000;
const SHARD_BACKFILL_ERROR_BACKOFF_MILLIS: u64 = 1_000;
const LIFECYCLE_SWEEP_HEARTBEAT_INTERVAL_ITEMS: usize = 256;
const LIFECYCLE_SWEEP_ERROR_CONTEXT_MAX_CHARS: usize = 1024;
const BACKGROUND_KNOWN_DAMAGE_REPAIR_LIMIT: usize = 1;
const BACKGROUND_RECLAIM_CLEANUP_LIMIT: usize = 1;
const BACKGROUND_LIFECYCLE_CLEANUP_LIMIT: usize = 1;
const BACKGROUND_STREAM_SESSION_CLEANUP_LIMIT: usize = 1;
const BACKGROUND_OPPORTUNISTIC_SCAN_LIMIT: usize = 1;
const BACKGROUND_FOREGROUND_PRESSURE_HOLD: Duration = Duration::from_millis(1_000);
const BACKGROUND_FOREGROUND_PRESSURE_SAMPLE_INTERVAL: Duration = Duration::from_millis(250);
const BACKGROUND_FOREGROUND_PRESSURE_MAX_SAMPLE_GAP: Duration = Duration::from_millis(1_250);

type ObjectPayloadReclaimRoot = (BucketName, ObjectKey, GenerationId);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BackgroundWorkClass {
    KnownDamageRepair,
    ReclaimCleanup,
    LifecycleCleanup,
    StreamSessionCleanup,
    OpportunisticScan,
}

impl BackgroundWorkClass {
    fn name(self) -> &'static str {
        match self {
            Self::KnownDamageRepair => "known_damage_repair",
            Self::ReclaimCleanup => "reclaim_cleanup",
            Self::LifecycleCleanup => "lifecycle_cleanup",
            Self::StreamSessionCleanup => "stream_session_cleanup",
            Self::OpportunisticScan => "opportunistic_scan",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BackgroundWorkAdmissionLimits {
    known_damage_repair: usize,
    reclaim_cleanup: usize,
    lifecycle_cleanup: usize,
    stream_session_cleanup: usize,
    opportunistic_scan: usize,
}

impl Default for BackgroundWorkAdmissionLimits {
    fn default() -> Self {
        Self {
            known_damage_repair: BACKGROUND_KNOWN_DAMAGE_REPAIR_LIMIT,
            reclaim_cleanup: BACKGROUND_RECLAIM_CLEANUP_LIMIT,
            lifecycle_cleanup: BACKGROUND_LIFECYCLE_CLEANUP_LIMIT,
            stream_session_cleanup: BACKGROUND_STREAM_SESSION_CLEANUP_LIMIT,
            opportunistic_scan: BACKGROUND_OPPORTUNISTIC_SCAN_LIMIT,
        }
    }
}

#[derive(Debug)]
struct BackgroundWorkAdmission {
    limits: BackgroundWorkAdmissionLimits,
    pressure: Mutex<BackgroundWorkPressureState>,
    known_damage_repair_active: AtomicUsize,
    reclaim_cleanup_active: AtomicUsize,
    lifecycle_cleanup_active: AtomicUsize,
    stream_session_cleanup_active: AtomicUsize,
    opportunistic_scan_active: AtomicUsize,
}

#[derive(Debug, Default)]
struct BackgroundWorkPressureState {
    last_snapshot: Option<observability::MetricsSnapshot>,
    last_snapshot_at: Option<Instant>,
    foreground_pressure_until: Option<Instant>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BackgroundWorkPressure {
    foreground: bool,
    durable_backlog: bool,
}

struct BackgroundWorkPermit {
    admission: Arc<BackgroundWorkAdmission>,
    class: BackgroundWorkClass,
    started_at: Instant,
    active: bool,
}

impl BackgroundWorkAdmission {
    fn new() -> Self {
        Self::with_limits(BackgroundWorkAdmissionLimits::default())
    }

    fn with_limits(limits: BackgroundWorkAdmissionLimits) -> Self {
        Self {
            limits,
            pressure: Mutex::new(BackgroundWorkPressureState::default()),
            known_damage_repair_active: AtomicUsize::new(0),
            reclaim_cleanup_active: AtomicUsize::new(0),
            lifecycle_cleanup_active: AtomicUsize::new(0),
            stream_session_cleanup_active: AtomicUsize::new(0),
            opportunistic_scan_active: AtomicUsize::new(0),
        }
    }

    fn try_acquire(self: &Arc<Self>, class: BackgroundWorkClass) -> Option<BackgroundWorkPermit> {
        if let Some(event) = self.policy_denial_event(class) {
            self.emit(class, event, None);
            return None;
        }

        let counter = self.counter_for(class);
        let limit = self.limit_for(class);
        let mut active = counter.load(Ordering::Acquire);
        while active < limit {
            match counter.compare_exchange_weak(
                active,
                active + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.emit(class, "admitted", None);
                    return Some(BackgroundWorkPermit {
                        admission: Arc::clone(self),
                        class,
                        started_at: Instant::now(),
                        active: true,
                    });
                }
                Err(observed) => active = observed,
            }
        }
        self.emit(class, "denied_limit", None);
        None
    }

    fn policy_denial_event(&self, class: BackgroundWorkClass) -> Option<&'static str> {
        if class != BackgroundWorkClass::OpportunisticScan {
            return None;
        }

        let pressure = self.observe_pressure();
        if pressure.foreground {
            Some("denied_foreground_pressure")
        } else if pressure.durable_backlog {
            Some("denied_backlog_pressure")
        } else {
            None
        }
    }

    fn observe_pressure(&self) -> BackgroundWorkPressure {
        let snapshot = observability::metrics_snapshot();
        lock_mutex_unpoisoned(&self.pressure).observe(Instant::now(), snapshot)
    }

    fn counter_for(&self, class: BackgroundWorkClass) -> &AtomicUsize {
        match class {
            BackgroundWorkClass::KnownDamageRepair => &self.known_damage_repair_active,
            BackgroundWorkClass::ReclaimCleanup => &self.reclaim_cleanup_active,
            BackgroundWorkClass::LifecycleCleanup => &self.lifecycle_cleanup_active,
            BackgroundWorkClass::StreamSessionCleanup => &self.stream_session_cleanup_active,
            BackgroundWorkClass::OpportunisticScan => &self.opportunistic_scan_active,
        }
    }

    fn limit_for(&self, class: BackgroundWorkClass) -> usize {
        match class {
            BackgroundWorkClass::KnownDamageRepair => self.limits.known_damage_repair,
            BackgroundWorkClass::ReclaimCleanup => self.limits.reclaim_cleanup,
            BackgroundWorkClass::LifecycleCleanup => self.limits.lifecycle_cleanup,
            BackgroundWorkClass::StreamSessionCleanup => self.limits.stream_session_cleanup,
            BackgroundWorkClass::OpportunisticScan => self.limits.opportunistic_scan,
        }
    }

    fn active_total(&self) -> usize {
        self.known_damage_repair_active.load(Ordering::Acquire)
            + self.reclaim_cleanup_active.load(Ordering::Acquire)
            + self.lifecycle_cleanup_active.load(Ordering::Acquire)
            + self.stream_session_cleanup_active.load(Ordering::Acquire)
            + self.opportunistic_scan_active.load(Ordering::Acquire)
    }

    fn emit(&self, class: BackgroundWorkClass, event: &'static str, elapsed_us: Option<u64>) {
        let _ = observability::emit_background_work_admission_event(
            TRACE_TARGET,
            observability::BackgroundWorkAdmissionSummary {
                class: class.name(),
                event,
                active_total: self.active_total(),
                elapsed_us,
            },
        );
    }
}

impl BackgroundWorkPressureState {
    fn observe(
        &mut self,
        now: Instant,
        snapshot: observability::MetricsSnapshot,
    ) -> BackgroundWorkPressure {
        if self.last_snapshot.zip(self.last_snapshot_at).is_some_and(
            |(last_snapshot, last_snapshot_at)| {
                now.checked_duration_since(last_snapshot_at)
                    .is_some_and(|elapsed| elapsed <= BACKGROUND_FOREGROUND_PRESSURE_MAX_SAMPLE_GAP)
                    && background_work_foreground_pressure_delta(last_snapshot, snapshot)
            },
        ) {
            self.foreground_pressure_until = Some(now + BACKGROUND_FOREGROUND_PRESSURE_HOLD);
        }
        self.last_snapshot = Some(snapshot);
        self.last_snapshot_at = Some(now);

        BackgroundWorkPressure {
            foreground: self
                .foreground_pressure_until
                .is_some_and(|pressure_until| now < pressure_until)
                || background_work_foreground_pressure_active(snapshot),
            durable_backlog: background_work_durable_backlog_active(snapshot),
        }
    }
}

fn background_work_foreground_pressure_delta(
    last: observability::MetricsSnapshot,
    current: observability::MetricsSnapshot,
) -> bool {
    current.request_admission_wait_total > last.request_admission_wait_total
        || current.request_admission_timeout_total > last.request_admission_timeout_total
        || current.storage_rpc_admission_wait_total > last.storage_rpc_admission_wait_total
        || current.storage_rpc_admission_timeout_total > last.storage_rpc_admission_timeout_total
}

fn background_work_foreground_pressure_active(snapshot: observability::MetricsSnapshot) -> bool {
    snapshot.inflight_requests > 0
        || snapshot.storage_rpc_active_read > 0
        || snapshot.storage_rpc_active_start_write > 0
        || snapshot.storage_rpc_active_list > 0
}

fn background_work_durable_backlog_active(snapshot: observability::MetricsSnapshot) -> bool {
    snapshot.reclaim_work_queue_depth > 0
        || snapshot.object_payload_reclaim_queue_depth > 0
        || snapshot.object_payload_reclaim_outstanding_depth > 0
        || snapshot.bucket_delete_finalize_queue_depth > 0
        || snapshot.shard_repair_queue_depth > 0
        || snapshot.shard_backfill_queue_depth > 0
}

impl Drop for BackgroundWorkPermit {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let previous = self
            .admission
            .counter_for(self.class)
            .fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0);
        let elapsed_us = u64::try_from(self.started_at.elapsed().as_micros()).unwrap_or(u64::MAX);
        self.admission
            .emit(self.class, "finished", Some(elapsed_us));
        self.active = false;
    }
}

fn background_work_admission_for(
    storage_cluster: &Arc<StorageCluster>,
) -> Arc<BackgroundWorkAdmission> {
    let registry = BACKGROUND_WORK_ADMISSION_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()));
    let mut registry: std::sync::MutexGuard<'_, HashMap<usize, Weak<BackgroundWorkAdmission>>> =
        lock_mutex_unpoisoned(registry);
    registry.retain(|_, admission| admission.upgrade().is_some());

    let key = storage_cluster.process_local_registry_key();
    if let Some(existing) = registry.get(&key).and_then(Weak::upgrade) {
        return existing;
    }

    let admission = Arc::new(BackgroundWorkAdmission::new());
    registry.insert(key, Arc::downgrade(&admission));
    admission
}

fn earliest_object_payload_reclaim_retry_sleep(
    storage_node: &StorageCluster,
    deferred_work: &VecDeque<ObjectPayloadReclaimRoot>,
    retry_after_by_pg: &HashMap<u32, Instant>,
) -> Option<Duration> {
    let now = Instant::now();
    let mut earliest_retry: Option<Instant> = None;
    for (bucket, key, _) in deferred_work {
        let pg_id = storage_node.object_payload_reclaim_pg_id(bucket, key);
        let retry_after = retry_after_by_pg.get(&pg_id)?;
        if *retry_after <= now {
            return None;
        }
        earliest_retry = Some(match earliest_retry {
            Some(earliest_retry) => earliest_retry.min(*retry_after),
            None => *retry_after,
        });
    }
    earliest_retry.map(|retry_after| {
        retry_after
            .duration_since(now)
            .min(OBJECT_PAYLOAD_RECLAIM_PG_RETRY_COOLDOWN)
    })
}

fn defer_object_payload_reclaim(
    deferred_work: &mut VecDeque<ObjectPayloadReclaimRoot>,
    deferred_roots: &mut HashSet<ObjectPayloadReclaimRoot>,
    root: ObjectPayloadReclaimRoot,
) {
    if deferred_roots.insert(root.clone()) {
        deferred_work.push_back(root);
    }
}

fn enqueue_durable_reclaim_work_if_due(
    storage_node: &StorageCluster,
    excluded_object_payload_roots: &HashSet<ObjectPayloadReclaimRoot>,
    next_scan_at: &mut Instant,
) {
    let now = Instant::now();
    if now < *next_scan_at {
        return;
    }
    storage_node
        .enqueue_durable_reclaim_work_excluding_object_payload(excluded_object_payload_roots);
    *next_scan_at = now + RECLAIM_DURABLE_SCAN_INTERVAL;
}

/// The coordinator ties together EC, storage, and metadata.
pub(super) struct ReclaimSweeper {
    pub(super) storage_node: Arc<StorageCluster>,
    pub(super) stop: Arc<AtomicBool>,
    pub(super) handle: Option<JoinHandle<()>>,
}

pub(super) struct LifecycleSweeper {
    pub(super) stop: Arc<AtomicBool>,
    pub(super) wake: Arc<(Mutex<bool>, Condvar)>,
    pub(super) handle: Mutex<Option<JoinHandle<()>>>,
}

pub(super) struct ShardScavengerSweeper {
    pub(super) stop: Arc<AtomicBool>,
    pub(super) wake: Arc<(Mutex<bool>, Condvar)>,
    pub(super) handle: Mutex<Option<JoinHandle<()>>>,
}

pub(super) struct ShardRepairSweeper {
    pub(super) storage_node: Arc<StorageCluster>,
    pub(super) stop: Arc<AtomicBool>,
    pub(super) handle: Mutex<Option<JoinHandle<()>>>,
}

pub(super) struct ShardBackfillSweeper {
    pub(super) stop: Arc<AtomicBool>,
    pub(super) wake: Arc<(Mutex<bool>, Condvar)>,
    pub(super) handle: Mutex<Option<JoinHandle<()>>>,
}

pub(super) struct StreamSessionSweeper {
    pub(super) stop: Arc<AtomicBool>,
    pub(super) wake: Arc<(Mutex<bool>, Condvar)>,
    pub(super) handle: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct LifecycleSweepStats {
    pub(super) discovered_roots: u64,
    pub(super) acquired_claims: u64,
    pub(super) busy_claims: u64,
    pub(super) recovered_expired_claims: u64,
    pub(super) released_claims: u64,
    pub(super) failed_claims: u64,
    pub(super) scanned_buckets: u64,
    pub(super) expired_current_objects: u64,
    pub(super) expired_noncurrent_versions: u64,
    pub(super) expired_delete_markers: u64,
    pub(super) skipped_expired_delete_markers: u64,
    pub(super) aborted_multipart_uploads: u64,
}

impl Drop for ReclaimSweeper {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        self.storage_node.wake_reclaim_workers();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl ReclaimSweeper {
    pub(super) fn spawn(
        storage_cluster: Arc<StorageCluster>,
        runtime: ReadRuntime,
    ) -> Result<Self, ServerError> {
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let worker_node = Arc::clone(&storage_cluster);
        let admission = background_work_admission_for(&storage_cluster);
        let handle = std::thread::Builder::new()
            .name("argmin-reclaim".to_string())
            .spawn(move || {
                let mut object_payload_reclaim_pg_retry_after: HashMap<u32, Instant> =
                    HashMap::new();
                let mut deferred_object_payload_reclaim = VecDeque::new();
                let mut deferred_object_payload_reclaim_roots = HashSet::new();
                let mut next_durable_scan_at = Instant::now();
                let mut pending_work = None;
                while !worker_stop.load(Ordering::SeqCst) {
                    enqueue_durable_reclaim_work_if_due(
                        &worker_node,
                        &deferred_object_payload_reclaim_roots,
                        &mut next_durable_scan_at,
                    );
                    let Some(work) = pending_work
                        .take()
                        .or_else(|| worker_node.try_take_reclaim_work())
                        .or_else(|| {
                            if deferred_object_payload_reclaim.is_empty() {
                                return None;
                            }
                            enqueue_durable_reclaim_work_if_due(
                                &worker_node,
                                &deferred_object_payload_reclaim_roots,
                                &mut next_durable_scan_at,
                            );
                            worker_node.try_take_reclaim_work().or_else(|| {
                                deferred_object_payload_reclaim.pop_front().map(|root| {
                                    deferred_object_payload_reclaim_roots.remove(&root);
                                    ReclaimWorkItem::ObjectPayload(root)
                                })
                            })
                        })
                        .or_else(|| worker_node.wait_for_reclaim_work(&worker_stop))
                    else {
                        break;
                    };
                    let Some(_cleanup_permit) =
                        admission.try_acquire(BackgroundWorkClass::ReclaimCleanup)
                    else {
                        pending_work = Some(work);
                        std::thread::sleep(OBJECT_PAYLOAD_RECLAIM_PG_RETRY_COOLDOWN);
                        continue;
                    };
                    match work {
                        ReclaimWorkItem::ObjectPayload((bucket, key, generation_id)) => {
                            let root = (bucket, key, generation_id);
                            if deferred_object_payload_reclaim_roots.contains(&root) {
                                continue;
                            }
                            let (bucket, key, generation_id) = root;
                            let pg_id = worker_node.object_payload_reclaim_pg_id(&bucket, &key);
                            let is_pg_cooled = if let Some(retry_after) =
                                object_payload_reclaim_pg_retry_after.get(&pg_id)
                            {
                                let now = Instant::now();
                                *retry_after > now
                            } else {
                                false
                            };
                            if is_pg_cooled {
                                defer_object_payload_reclaim(
                                    &mut deferred_object_payload_reclaim,
                                    &mut deferred_object_payload_reclaim_roots,
                                    (bucket, key, generation_id),
                                );
                            } else {
                                let result = runtime.try_reclaim_object_payload_for_with_outcome(
                                    &bucket,
                                    &key,
                                    generation_id,
                                );
                                if matches!(
                                    result,
                                    Ok(storage::cluster::ObjectPayloadReclaimAttempt::Deferred)
                                        | Err(ServerError::OperationAborted | ServerError::SlowDown)
                                ) {
                                    object_payload_reclaim_pg_retry_after.insert(
                                        pg_id,
                                        Instant::now() + OBJECT_PAYLOAD_RECLAIM_PG_RETRY_COOLDOWN,
                                    );
                                    defer_object_payload_reclaim(
                                        &mut deferred_object_payload_reclaim,
                                        &mut deferred_object_payload_reclaim_roots,
                                        (bucket, key, generation_id),
                                    );
                                } else {
                                    worker_node.finish_object_payload_reclaim_work(
                                        &bucket,
                                        &key,
                                        generation_id,
                                    );
                                    object_payload_reclaim_pg_retry_after.remove(&pg_id);
                                }
                            }
                        }
                        ReclaimWorkItem::BucketDelete(bucket) => {
                            let _ = runtime.try_finalize_bucket_delete_for(&bucket);
                        }
                    }
                    if pending_work.is_none() && !deferred_object_payload_reclaim.is_empty() {
                        enqueue_durable_reclaim_work_if_due(
                            &worker_node,
                            &deferred_object_payload_reclaim_roots,
                            &mut next_durable_scan_at,
                        );
                        if let Some(work) = worker_node.try_take_reclaim_work() {
                            pending_work = Some(work);
                        } else if let Some(sleep_for) = earliest_object_payload_reclaim_retry_sleep(
                            &worker_node,
                            &deferred_object_payload_reclaim,
                            &object_payload_reclaim_pg_retry_after,
                        ) {
                            std::thread::sleep(sleep_for);
                        }
                    }
                }
            })
            .map_err(|e| ServerError::InternalError {
                reason: format!("failed to start reclaim worker: {e}"),
            })?;
        Ok(Self {
            storage_node: storage_cluster,
            stop,
            handle: Some(handle),
        })
    }

    pub(super) fn disabled(storage_cluster: Arc<StorageCluster>) -> Self {
        Self {
            storage_node: storage_cluster,
            stop: Arc::new(AtomicBool::new(true)),
            handle: None,
        }
    }
}

impl Drop for LifecycleSweeper {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        *lock_mutex_unpoisoned(&self.wake.0) = true;
        self.wake.1.notify_all();
        if let Some(handle) = lock_mutex_unpoisoned(&self.handle).take() {
            let _ = handle.join();
        }
    }
}

impl Drop for ShardScavengerSweeper {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        *lock_mutex_unpoisoned(&self.wake.0) = true;
        self.wake.1.notify_all();
        if let Some(handle) = lock_mutex_unpoisoned(&self.handle).take() {
            let _ = handle.join();
        }
    }
}

impl Drop for ShardRepairSweeper {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        self.storage_node.wake_placed_segment_shard_repair_workers();
        if let Some(handle) = lock_mutex_unpoisoned(&self.handle).take() {
            let _ = handle.join();
        }
    }
}

impl Drop for ShardBackfillSweeper {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        *lock_mutex_unpoisoned(&self.wake.0) = true;
        self.wake.1.notify_all();
        if let Some(handle) = lock_mutex_unpoisoned(&self.handle).take() {
            let _ = handle.join();
        }
    }
}

impl Drop for StreamSessionSweeper {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        *lock_mutex_unpoisoned(&self.wake.0) = true;
        self.wake.1.notify_all();
        if let Some(handle) = lock_mutex_unpoisoned(&self.handle).take() {
            let _ = handle.join();
        }
    }
}

impl LifecycleSweeper {
    pub(super) fn acquire_shared(
        storage_cluster: &Arc<StorageCluster>,
        runtime: ReadRuntime,
    ) -> Result<Arc<Self>, ServerError> {
        let registry = LIFECYCLE_SWEEPER_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()));
        let mut registry: std::sync::MutexGuard<'_, HashMap<usize, Weak<LifecycleSweeper>>> =
            lock_mutex_unpoisoned(registry);
        registry.retain(|_, sweeper| sweeper.upgrade().is_some());

        let key = storage_cluster.process_local_registry_key();
        if let Some(existing) = registry.get(&key).and_then(Weak::upgrade) {
            return Ok(existing);
        }

        let admission = background_work_admission_for(storage_cluster);
        let sweeper = Self::spawn(runtime, admission)?;
        registry.insert(key, Arc::downgrade(&sweeper));
        Ok(sweeper)
    }

    fn spawn(
        runtime: ReadRuntime,
        admission: Arc<BackgroundWorkAdmission>,
    ) -> Result<Arc<Self>, ServerError> {
        let stop = Arc::new(AtomicBool::new(false));
        let wake = Arc::new((Mutex::new(false), Condvar::new()));
        let sweeper = Arc::new(Self {
            stop: Arc::clone(&stop),
            wake: Arc::clone(&wake),
            handle: Mutex::new(None),
        });
        let handle = std::thread::Builder::new()
            .name("argmin-lifecycle".to_string())
            .spawn(move || {
                while !stop.load(Ordering::SeqCst) {
                    if let Some(_permit) =
                        admission.try_acquire(BackgroundWorkClass::LifecycleCleanup)
                    {
                        let _ = runtime.run_lifecycle_sweep_at(Coordinator::now_millis());
                    }
                    if stop.load(Ordering::SeqCst) {
                        break;
                    }
                    let stop_guard = lock_mutex_unpoisoned(&wake.0);
                    if *stop_guard {
                        break;
                    }
                    let _ = wake
                        .1
                        .wait_timeout_while(
                            stop_guard,
                            Duration::from_millis(LIFECYCLE_SWEEP_INTERVAL_MILLIS),
                            |stop_requested| !*stop_requested,
                        )
                        .unwrap_or_else(|e| e.into_inner());
                }
            })
            .map_err(|e| ServerError::InternalError {
                reason: format!("failed to start lifecycle worker: {e}"),
            })?;
        *lock_mutex_unpoisoned(&sweeper.handle) = Some(handle);
        Ok(sweeper)
    }

    pub(super) fn disabled() -> Arc<Self> {
        Arc::new(Self {
            stop: Arc::new(AtomicBool::new(true)),
            wake: Arc::new((Mutex::new(true), Condvar::new())),
            handle: Mutex::new(None),
        })
    }
}

impl ShardScavengerSweeper {
    pub(super) fn acquire_shared(
        storage_handle: &StorageClusterRuntimeMapHandle,
    ) -> Result<Arc<Self>, ServerError> {
        let registry = SHARD_SCAVENGER_SWEEPER_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()));
        let mut registry: std::sync::MutexGuard<'_, HashMap<usize, Weak<ShardScavengerSweeper>>> =
            lock_mutex_unpoisoned(registry);
        registry.retain(|_, sweeper| sweeper.upgrade().is_some());

        let storage_cluster = storage_handle.current();
        let key = storage_cluster.process_local_registry_key();
        if let Some(existing) = registry.get(&key).and_then(Weak::upgrade) {
            return Ok(existing);
        }

        let sweeper = Self::spawn(storage_handle.clone())?;
        registry.insert(key, Arc::downgrade(&sweeper));
        Ok(sweeper)
    }

    fn spawn(storage_handle: StorageClusterRuntimeMapHandle) -> Result<Arc<Self>, ServerError> {
        let stop = Arc::new(AtomicBool::new(false));
        let wake = Arc::new((Mutex::new(false), Condvar::new()));
        let sweeper = Arc::new(Self {
            stop: Arc::clone(&stop),
            wake: Arc::clone(&wake),
            handle: Mutex::new(None),
        });
        let handle = std::thread::Builder::new()
            .name("argmin-shard-scavenger".to_string())
            .spawn(move || {
                let sweep_interval =
                    Duration::from_millis(super::SHARD_SCAVENGER_SWEEP_INTERVAL_MILLIS);
                let pressure_sample_interval = BACKGROUND_FOREGROUND_PRESSURE_SAMPLE_INTERVAL;
                let mut next_sweep = Instant::now();
                while !stop.load(Ordering::SeqCst) {
                    let storage_cluster = storage_handle.current();
                    let admission = background_work_admission_for(&storage_cluster);
                    admission.observe_pressure();
                    let now = Instant::now();
                    if now >= next_sweep {
                        if let Some(_permit) =
                            admission.try_acquire(BackgroundWorkClass::OpportunisticScan)
                        {
                            if let Err(error) = storage_cluster.audit_shard_storage_for_scavenger()
                            {
                                let _ = observability::event(
                                    TRACE_TARGET,
                                    "shard_scavenger_audit_error",
                                    Some(format_args!("error={error}")),
                                );
                            }
                            match storage_cluster
                                .enqueue_placed_segment_shard_backfills_from_scavenger_references()
                            {
                                Ok(summary) => {
                                    let _ = observability::event(
                                        TRACE_TARGET,
                                        "shard_backfill_candidate_scan",
                                        Some(format_args!(
                                            "scanned={} current_epoch={} already_complete={} enqueued={} unrecoverable={} failed={}",
                                            summary.scanned,
                                            summary.current_epoch,
                                            summary.already_complete,
                                            summary.enqueued,
                                            summary.unrecoverable,
                                            summary.failed,
                                        )),
                                    );
                                }
                                Err(error) => {
                                    let _ = observability::event(
                                        TRACE_TARGET,
                                        "shard_backfill_candidate_scan_error",
                                        Some(format_args!("error={error}")),
                                    );
                                }
                            }
                        }
                        next_sweep = Instant::now() + sweep_interval;
                    }
                    if stop.load(Ordering::SeqCst) {
                        break;
                    }
                    let stop_guard = lock_mutex_unpoisoned(&wake.0);
                    if *stop_guard {
                        break;
                    }
                    let now = Instant::now();
                    let wait_for = next_sweep
                        .checked_duration_since(now)
                        .unwrap_or_default()
                        .min(pressure_sample_interval);
                    let _ = wake
                        .1
                        .wait_timeout_while(stop_guard, wait_for, |stop_requested| !*stop_requested)
                        .unwrap_or_else(|e| e.into_inner());
                }
            })
            .map_err(|e| ServerError::InternalError {
                reason: format!("failed to start shard scavenger worker: {e}"),
            })?;
        *lock_mutex_unpoisoned(&sweeper.handle) = Some(handle);
        Ok(sweeper)
    }

    pub(super) fn disabled() -> Arc<Self> {
        Arc::new(Self {
            stop: Arc::new(AtomicBool::new(true)),
            wake: Arc::new((Mutex::new(true), Condvar::new())),
            handle: Mutex::new(None),
        })
    }
}

impl ShardRepairSweeper {
    pub(super) fn acquire_shared(
        storage_cluster: &Arc<StorageCluster>,
    ) -> Result<Arc<Self>, ServerError> {
        let registry = SHARD_REPAIR_SWEEPER_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()));
        let mut registry: std::sync::MutexGuard<'_, HashMap<usize, Weak<ShardRepairSweeper>>> =
            lock_mutex_unpoisoned(registry);
        registry.retain(|_, sweeper| sweeper.upgrade().is_some());

        let key = storage_cluster.process_local_registry_key();
        if let Some(existing) = registry.get(&key).and_then(Weak::upgrade) {
            return Ok(existing);
        }

        let sweeper = Self::spawn(Arc::clone(storage_cluster))?;
        registry.insert(key, Arc::downgrade(&sweeper));
        Ok(sweeper)
    }

    fn spawn(storage_cluster: Arc<StorageCluster>) -> Result<Arc<Self>, ServerError> {
        let stop = Arc::new(AtomicBool::new(false));
        let admission = background_work_admission_for(&storage_cluster);
        let sweeper = Arc::new(Self {
            storage_node: Arc::clone(&storage_cluster),
            stop: Arc::clone(&stop),
            handle: Mutex::new(None),
        });
        let handle = std::thread::Builder::new()
            .name("argmin-shard-repair".to_string())
            .spawn(move || {
                let owner_token = format!(
                    "shard-repair-worker-{}",
                    storage_cluster.process_local_registry_key()
                );
                let mut next_durable_scan_at = Instant::now();
                while !stop.load(Ordering::SeqCst) {
                    let now = Instant::now();
                    if now >= next_durable_scan_at {
                        match storage_cluster.enqueue_durable_placed_segment_shard_repair_work() {
                            Ok(summary) if summary.scanned == 0 => {
                                let _ = observability::emit_shard_repair_event(
                                    TRACE_TARGET,
                                    observability::ShardRepairEventSummary {
                                        pg_id: None,
                                        event: "durable_scan_empty",
                                        queue_depth: None,
                                        shards_rewritten: None,
                                    },
                                );
                            }
                            Ok(summary) if summary.enqueued > 0 => {
                                let _ = observability::emit_shard_repair_event(
                                    TRACE_TARGET,
                                    observability::ShardRepairEventSummary {
                                        pg_id: None,
                                        event: "durable_scan_queued",
                                        queue_depth: None,
                                        shards_rewritten: None,
                                    },
                                );
                            }
                            Ok(_) => {
                                let _ = observability::emit_shard_repair_event(
                                    TRACE_TARGET,
                                    observability::ShardRepairEventSummary {
                                        pg_id: None,
                                        event: "durable_scan_no_new_enqueue",
                                        queue_depth: None,
                                        shards_rewritten: None,
                                    },
                                );
                            }
                            Err(error) => {
                                let _ = observability::emit_shard_repair_event(
                                    TRACE_TARGET,
                                    observability::ShardRepairEventSummary {
                                        pg_id: None,
                                        event: "durable_scan_failed",
                                        queue_depth: None,
                                        shards_rewritten: None,
                                    },
                                );
                                let _ = observability::event(
                                    TRACE_TARGET,
                                    "shard_repair_durable_scan_error",
                                    Some(format_args!("error={error}")),
                                );
                            }
                        }
                        next_durable_scan_at = now + SHARD_REPAIR_DURABLE_SCAN_INTERVAL;
                    }

                    let Some(work_item) = storage_cluster
                        .try_take_placed_segment_shard_repair_work()
                        .or_else(|| {
                            storage_cluster.wait_for_placed_segment_shard_repair_work(&stop)
                        })
                    else {
                        if stop.load(Ordering::SeqCst) {
                            break;
                        }
                        #[cfg(test)]
                        maybe_run_shard_repair_worker_idle_timeout_hook(
                            storage_cluster.process_local_registry_key(),
                        );
                        continue;
                    };
                    if stop.load(Ordering::SeqCst) {
                        break;
                    }

                    let now_ms = Coordinator::now_millis();
                    let claim_id = format!(
                        "shard-repair-{}-{}-{}-{}",
                        storage_cluster.process_local_registry_key(),
                        work_item.request.data_pg_id,
                        work_item.shard_index.get(),
                        SHARD_REPAIR_CLAIM_COUNTER.fetch_add(1, Ordering::Relaxed)
                    );
                    let claim = match storage_cluster.acquire_placed_segment_shard_repair_claim(
                        work_item.request.data_pg_id,
                        &claim_id,
                        &owner_token,
                        now_ms,
                        now_ms.saturating_add(SHARD_REPAIR_CLAIM_LEASE_MILLIS),
                        now_ms,
                    ) {
                        Ok(Some(claim)) => {
                            let _ = observability::emit_shard_repair_event(
                                TRACE_TARGET,
                                observability::ShardRepairEventSummary {
                                    pg_id: Some(work_item.request.data_pg_id),
                                    event: "claim_started",
                                    queue_depth: None,
                                    shards_rewritten: None,
                                },
                            );
                            claim
                        }
                        Ok(None) => {
                            let _ = observability::emit_shard_repair_event(
                                TRACE_TARGET,
                                observability::ShardRepairEventSummary {
                                    pg_id: Some(work_item.request.data_pg_id),
                                    event: "claim_empty",
                                    queue_depth: None,
                                    shards_rewritten: None,
                                },
                            );
                            continue;
                        }
                        Err(error) => {
                            let _ = observability::emit_shard_repair_event(
                                TRACE_TARGET,
                                observability::ShardRepairEventSummary {
                                    pg_id: Some(work_item.request.data_pg_id),
                                    event: "claim_failed",
                                    queue_depth: None,
                                    shards_rewritten: None,
                                },
                            );
                            let _ = observability::event(
                                TRACE_TARGET,
                                "shard_repair_claim_error",
                                Some(format_args!("error={error}")),
                            );
                            continue;
                        }
                    };

                    let Some(_repair_permit) =
                        admission.try_acquire(BackgroundWorkClass::KnownDamageRepair)
                    else {
                        let _ = observability::emit_shard_repair_event(
                            TRACE_TARGET,
                            observability::ShardRepairEventSummary {
                                pg_id: Some(claim.work_item.request.data_pg_id),
                                event: "admission_denied",
                                queue_depth: None,
                                shards_rewritten: None,
                            },
                        );
                        let next_attempt_after = Coordinator::now_millis()
                            .saturating_add(SHARD_REPAIR_ERROR_BACKOFF_MILLIS);
                        if let Err(record_error) = storage_cluster
                            .record_placed_segment_shard_repair_claim_error(
                                &claim,
                                "background known-damage repair admission denied",
                                next_attempt_after,
                            )
                        {
                            let _ = observability::emit_shard_repair_event(
                                TRACE_TARGET,
                                observability::ShardRepairEventSummary {
                                    pg_id: Some(claim.work_item.request.data_pg_id),
                                    event: "record_error_failed",
                                    queue_depth: None,
                                    shards_rewritten: None,
                                },
                            );
                            let _ = observability::event(
                                TRACE_TARGET,
                                "shard_repair_record_error_failed",
                                Some(format_args!("record_error={record_error}")),
                            );
                        }
                        continue;
                    };

                    let _ = observability::emit_shard_repair_event(
                        TRACE_TARGET,
                        observability::ShardRepairEventSummary {
                            pg_id: Some(claim.work_item.request.data_pg_id),
                            event: "started",
                            queue_depth: None,
                            shards_rewritten: None,
                        },
                    );
                    match storage_cluster
                        .repair_placed_segment_payload_shards_if_needed_preserving_repair_rows(
                            claim.work_item.request,
                        ) {
                        Ok(repaired_acks) => {
                            let event = if repaired_acks.is_empty() {
                                "resolved_clean"
                            } else {
                                "repaired"
                            };
                            let _ = observability::emit_shard_repair_event(
                                TRACE_TARGET,
                                observability::ShardRepairEventSummary {
                                    pg_id: Some(claim.work_item.request.data_pg_id),
                                    event,
                                    queue_depth: None,
                                    shards_rewritten: Some(repaired_acks.len()),
                                },
                            );
                            match storage_cluster.complete_placed_segment_shard_repair_claim(&claim)
                            {
                                Ok(true) => {
                                    let _ = observability::emit_shard_repair_event(
                                        TRACE_TARGET,
                                        observability::ShardRepairEventSummary {
                                            pg_id: Some(claim.work_item.request.data_pg_id),
                                            event: "complete_succeeded",
                                            queue_depth: None,
                                            shards_rewritten: None,
                                        },
                                    );
                                }
                                Ok(false) => {
                                    let _ = observability::emit_shard_repair_event(
                                        TRACE_TARGET,
                                        observability::ShardRepairEventSummary {
                                            pg_id: Some(claim.work_item.request.data_pg_id),
                                            event: "complete_stale",
                                            queue_depth: None,
                                            shards_rewritten: None,
                                        },
                                    );
                                }
                                Err(error) => {
                                    let _ = observability::emit_shard_repair_event(
                                        TRACE_TARGET,
                                        observability::ShardRepairEventSummary {
                                            pg_id: Some(claim.work_item.request.data_pg_id),
                                            event: "complete_failed",
                                            queue_depth: None,
                                            shards_rewritten: None,
                                        },
                                    );
                                    let _ = observability::event(
                                        TRACE_TARGET,
                                        "shard_repair_complete_error",
                                        Some(format_args!("error={error}")),
                                    );
                                }
                            }
                        }
                        Err(error) => {
                            let event = if matches!(error, StoreError::NotFound) {
                                "unrecoverable"
                            } else {
                                "failed"
                            };
                            let _ = observability::emit_shard_repair_event(
                                TRACE_TARGET,
                                observability::ShardRepairEventSummary {
                                    pg_id: Some(claim.work_item.request.data_pg_id),
                                    event,
                                    queue_depth: None,
                                    shards_rewritten: None,
                                },
                            );
                            let next_attempt_after = Coordinator::now_millis()
                                .saturating_add(SHARD_REPAIR_ERROR_BACKOFF_MILLIS);
                            if let Err(record_error) = storage_cluster
                                .record_placed_segment_shard_repair_claim_error(
                                    &claim,
                                    &error.to_string(),
                                    next_attempt_after,
                                )
                            {
                                let _ = observability::emit_shard_repair_event(
                                    TRACE_TARGET,
                                    observability::ShardRepairEventSummary {
                                        pg_id: Some(claim.work_item.request.data_pg_id),
                                        event: "record_error_failed",
                                        queue_depth: None,
                                        shards_rewritten: None,
                                    },
                                );
                                let _ = observability::event(
                                    TRACE_TARGET,
                                    "shard_repair_record_error_failed",
                                    Some(format_args!(
                                        "repair_error={error} record_error={record_error}"
                                    )),
                                );
                            }
                        }
                    }
                }
            })
            .map_err(|e| ServerError::InternalError {
                reason: format!("failed to start shard repair worker: {e}"),
            })?;
        *lock_mutex_unpoisoned(&sweeper.handle) = Some(handle);
        Ok(sweeper)
    }

    pub(super) fn disabled(storage_cluster: Arc<StorageCluster>) -> Arc<Self> {
        Arc::new(Self {
            storage_node: storage_cluster,
            stop: Arc::new(AtomicBool::new(true)),
            handle: Mutex::new(None),
        })
    }
}

impl ShardBackfillSweeper {
    pub(super) fn acquire_shared(
        storage_handle: &StorageClusterRuntimeMapHandle,
    ) -> Result<Arc<Self>, ServerError> {
        let registry = SHARD_BACKFILL_SWEEPER_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()));
        let mut registry: std::sync::MutexGuard<'_, HashMap<usize, Weak<ShardBackfillSweeper>>> =
            lock_mutex_unpoisoned(registry);
        registry.retain(|_, sweeper| sweeper.upgrade().is_some());

        let storage_cluster = storage_handle.current();
        let key = storage_cluster.process_local_registry_key();
        if let Some(existing) = registry.get(&key).and_then(Weak::upgrade) {
            return Ok(existing);
        }

        let sweeper = Self::spawn(storage_handle.clone())?;
        registry.insert(key, Arc::downgrade(&sweeper));
        Ok(sweeper)
    }

    fn spawn(storage_handle: StorageClusterRuntimeMapHandle) -> Result<Arc<Self>, ServerError> {
        let stop = Arc::new(AtomicBool::new(false));
        let wake = Arc::new((Mutex::new(false), Condvar::new()));
        let sweeper = Arc::new(Self {
            stop: Arc::clone(&stop),
            wake: Arc::clone(&wake),
            handle: Mutex::new(None),
        });
        let handle = std::thread::Builder::new()
            .name("argmin-shard-backfill".to_string())
            .spawn(move || {
                while !stop.load(Ordering::SeqCst) {
                    let storage_cluster = storage_handle.current();
                    let admission = background_work_admission_for(&storage_cluster);
                    let owner_token = format!(
                        "shard-backfill-worker-{}",
                        storage_cluster.process_local_registry_key()
                    );
                    admission.observe_pressure();
                    if let Some(_permit) =
                        admission.try_acquire(BackgroundWorkClass::KnownDamageRepair)
                    {
                        run_one_placed_segment_shard_backfill(&storage_cluster, &owner_token);
                    } else {
                        emit_shard_backfill_event(
                            None,
                            "admission_denied",
                            shard_backfill_queue_depth(&storage_cluster),
                            None,
                        );
                    }

                    let stop_guard = lock_mutex_unpoisoned(&wake.0);
                    if stop.load(Ordering::SeqCst) {
                        break;
                    }
                    let _ = wake
                        .1
                        .wait_timeout_while(
                            stop_guard,
                            SHARD_BACKFILL_DURABLE_SCAN_INTERVAL,
                            |stop_requested| !*stop_requested,
                        )
                        .unwrap_or_else(|e| e.into_inner());
                }
            })
            .map_err(|e| ServerError::InternalError {
                reason: format!("failed to start shard backfill worker: {e}"),
            })?;
        *lock_mutex_unpoisoned(&sweeper.handle) = Some(handle);
        Ok(sweeper)
    }

    pub(super) fn disabled() -> Arc<Self> {
        Arc::new(Self {
            stop: Arc::new(AtomicBool::new(true)),
            wake: Arc::new((Mutex::new(true), Condvar::new())),
            handle: Mutex::new(None),
        })
    }
}

fn run_one_placed_segment_shard_backfill(storage_cluster: &StorageCluster, owner_token: &str) {
    let now_ms = Coordinator::now_millis();
    let claim_id = format!(
        "shard-backfill-{}-{}",
        storage_cluster.process_local_registry_key(),
        SHARD_BACKFILL_CLAIM_COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    let queue_depth = shard_backfill_queue_depth(storage_cluster);
    let claim = match storage_cluster.acquire_next_placed_segment_shard_backfill_claim(
        &claim_id,
        owner_token,
        now_ms,
        now_ms.saturating_add(SHARD_BACKFILL_CLAIM_LEASE_MILLIS),
        now_ms,
    ) {
        Ok(Some(claim)) => {
            emit_shard_backfill_event(
                Some(claim.work_item.request.data_pg_id),
                "claim_started",
                queue_depth,
                None,
            );
            claim
        }
        Ok(None) => {
            emit_shard_backfill_event(None, "durable_scan_no_claim", queue_depth, None);
            return;
        }
        Err(error) => {
            emit_shard_backfill_event(None, "claim_failed", queue_depth, None);
            let _ = observability::event(
                TRACE_TARGET,
                "shard_backfill_claim_error",
                Some(format_args!("error={error}")),
            );
            return;
        }
    };

    emit_shard_backfill_event(
        Some(claim.work_item.request.data_pg_id),
        "started",
        None,
        None,
    );
    match storage_cluster.backfill_placed_segment_payload_shards_for_work_item(&claim.work_item) {
        Ok(backfilled_acks) => {
            let event = if backfilled_acks.is_empty() {
                "resolved_clean"
            } else {
                "backfilled"
            };
            emit_shard_backfill_event(
                Some(claim.work_item.request.data_pg_id),
                event,
                None,
                Some(backfilled_acks.len()),
            );
            match storage_cluster.complete_placed_segment_shard_backfill_claim(&claim) {
                Ok(true) => {
                    emit_shard_backfill_event(
                        Some(claim.work_item.request.data_pg_id),
                        "complete_succeeded",
                        shard_backfill_queue_depth(storage_cluster),
                        None,
                    );
                }
                Ok(false) => {
                    emit_shard_backfill_event(
                        Some(claim.work_item.request.data_pg_id),
                        "complete_stale",
                        shard_backfill_queue_depth(storage_cluster),
                        None,
                    );
                }
                Err(error) => {
                    emit_shard_backfill_event(
                        Some(claim.work_item.request.data_pg_id),
                        "complete_failed",
                        shard_backfill_queue_depth(storage_cluster),
                        None,
                    );
                    let _ = observability::event(
                        TRACE_TARGET,
                        "shard_backfill_complete_error",
                        Some(format_args!("error={error}")),
                    );
                }
            }
        }
        Err(error) => {
            emit_shard_backfill_event(
                Some(claim.work_item.request.data_pg_id),
                "failed",
                None,
                None,
            );
            let next_attempt_after =
                Coordinator::now_millis().saturating_add(SHARD_BACKFILL_ERROR_BACKOFF_MILLIS);
            if let Err(record_error) = storage_cluster
                .record_placed_segment_shard_backfill_claim_error(
                    &claim,
                    &error.to_string(),
                    next_attempt_after,
                )
            {
                emit_shard_backfill_event(
                    Some(claim.work_item.request.data_pg_id),
                    "record_error_failed",
                    shard_backfill_queue_depth(storage_cluster),
                    None,
                );
                let _ = observability::event(
                    TRACE_TARGET,
                    "shard_backfill_record_error_failed",
                    Some(format_args!(
                        "backfill_error={error} record_error={record_error}"
                    )),
                );
            }
        }
    }
}

fn shard_backfill_queue_depth(storage_cluster: &StorageCluster) -> Option<usize> {
    match storage_cluster.placed_segment_shard_backfill_backlog_depth() {
        Ok(depth) => Some(depth),
        Err(error) => {
            let _ = observability::event(
                TRACE_TARGET,
                "shard_backfill_backlog_depth_error",
                Some(format_args!("error={error}")),
            );
            None
        }
    }
}

fn emit_shard_backfill_event(
    pg_id: Option<u32>,
    event: &'static str,
    queue_depth: Option<usize>,
    shards_written: Option<usize>,
) {
    let _ = observability::emit_shard_backfill_event(
        TRACE_TARGET,
        observability::ShardBackfillEventSummary {
            pg_id,
            event,
            queue_depth,
            shards_written,
        },
    );
}

impl StreamSessionSweeper {
    pub(super) fn acquire_shared(
        storage_cluster: &Arc<StorageCluster>,
    ) -> Result<Arc<Self>, ServerError> {
        let registry = STREAM_SESSION_SWEEPER_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()));
        let mut registry: std::sync::MutexGuard<'_, HashMap<usize, Weak<StreamSessionSweeper>>> =
            lock_mutex_unpoisoned(registry);
        registry.retain(|_, sweeper| sweeper.upgrade().is_some());

        let key = storage_cluster.process_local_registry_key();
        if let Some(existing) = registry.get(&key).and_then(Weak::upgrade) {
            return Ok(existing);
        }

        let sweeper = Self::spawn(Arc::clone(storage_cluster))?;
        registry.insert(key, Arc::downgrade(&sweeper));
        Ok(sweeper)
    }

    fn spawn(storage_cluster: Arc<StorageCluster>) -> Result<Arc<Self>, ServerError> {
        let stop = Arc::new(AtomicBool::new(false));
        let wake = Arc::new((Mutex::new(false), Condvar::new()));
        let admission = background_work_admission_for(&storage_cluster);
        let sweeper = Arc::new(Self {
            stop: Arc::clone(&stop),
            wake: Arc::clone(&wake),
            handle: Mutex::new(None),
        });
        let handle = std::thread::Builder::new()
            .name("argmin-stream-session-sweeper".to_string())
            .spawn(move || {
                while !stop.load(Ordering::SeqCst) {
                    if let Some(_permit) =
                        admission.try_acquire(BackgroundWorkClass::StreamSessionCleanup)
                    {
                        let count = storage_cluster.scavenge_abandoned_stream_sessions(
                            super::STREAM_SESSION_SCAVENGE_MAX_AGE_MILLIS,
                        );
                        if count > 0 {
                            let _ = observability::event(
                                TRACE_TARGET,
                                "stream_session_sweep_abandoned",
                                Some(format_args!("aborted_sessions={count}")),
                            );
                        }
                    }
                    if stop.load(Ordering::SeqCst) {
                        break;
                    }
                    let stop_guard = lock_mutex_unpoisoned(&wake.0);
                    if *stop_guard {
                        break;
                    }
                    let _ = wake
                        .1
                        .wait_timeout_while(
                            stop_guard,
                            Duration::from_millis(super::STREAM_SESSION_SWEEP_INTERVAL_MILLIS),
                            |stop_requested| !*stop_requested,
                        )
                        .unwrap_or_else(|e| e.into_inner());
                }
            })
            .map_err(|e| ServerError::InternalError {
                reason: format!("failed to start stream session sweeper: {e}"),
            })?;
        *lock_mutex_unpoisoned(&sweeper.handle) = Some(handle);
        Ok(sweeper)
    }

    pub(super) fn disabled() -> Arc<Self> {
        Arc::new(Self {
            stop: Arc::new(AtomicBool::new(true)),
            wake: Arc::new((Mutex::new(true), Condvar::new())),
            handle: Mutex::new(None),
        })
    }
}

impl ReadRuntime {
    fn map_bucket_snapshot_error(error: storage::BucketSnapshotLoadError) -> ServerError {
        match error {
            storage::BucketSnapshotLoadError::Store(
                storage::StoreError::MetadataCommandLogConflict { .. }
                | storage::StoreError::MetadataCommandPendingConflict { .. },
            ) => ServerError::OperationAborted,
            storage::BucketSnapshotLoadError::Store(error) => super::map_store_error(error),
            storage::BucketSnapshotLoadError::Metadata(ref error)
                if super::metadata_error_is_command_contention(error) =>
            {
                ServerError::OperationAborted
            }
            storage::BucketSnapshotLoadError::Metadata(
                storage::MetadataError::BucketNotFound { name },
            ) => ServerError::BucketNotFound {
                name: name.to_string(),
            },
            storage::BucketSnapshotLoadError::Metadata(error) => ServerError::Metadata(error),
        }
    }

    pub(super) fn enqueue_object_payload_reclaim_for(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) {
        self.storage_node
            .enqueue_object_payload_reclaim(bucket, key, generation_id);
    }

    #[cfg(test)]
    pub(super) fn enqueue_object_payload_reclaim(
        &self,
        bucket: &str,
        key: &str,
        generation_id: GenerationId,
    ) {
        self.enqueue_object_payload_reclaim_for(
            &trusted_bucket_name(bucket),
            &trusted_object_key(key),
            generation_id,
        );
    }

    pub(super) fn enqueue_bucket_delete_finalize_for(&self, bucket: &BucketName) {
        self.storage_node.enqueue_bucket_delete_finalize(bucket);
    }

    fn lifecycle_config_for_bucket_info(
        &self,
        bucket_info: &BucketInfo,
    ) -> Result<Option<Arc<BucketLifecycleConfiguration>>, ServerError> {
        if !bucket_info.bucket_lifecycle_present {
            return Ok(None);
        }

        let raw_config = self
            .storage_node
            .get_bucket_subresource(&bucket_info.name, storage::BucketSubresourceKind::Lifecycle)
            .map_err(Self::map_bucket_snapshot_error)?;

        Self::parse_lifecycle_config(bucket_info.name.as_str(), raw_config.as_deref())
    }

    fn parse_lifecycle_config(
        bucket: &str,
        raw_config: Option<&str>,
    ) -> Result<Option<Arc<BucketLifecycleConfiguration>>, ServerError> {
        raw_config
            .map(|config_xml| {
                s3_types::parse_lifecycle_configuration_xml(config_xml.as_bytes()).map_err(
                    |error| ServerError::InternalError {
                        reason: format!(
                            "stored lifecycle configuration for {bucket} failed to parse at sweep time: {error}",
                        ),
                    },
                )
            })
            .transpose()
            .map(|config| config.map(Arc::new))
    }

    pub(super) fn run_lifecycle_sweep_at(
        &self,
        now_millis: u64,
    ) -> Result<LifecycleSweepStats, ServerError> {
        let mut stats = LifecycleSweepStats {
            discovered_roots: 0,
            acquired_claims: 0,
            busy_claims: 0,
            recovered_expired_claims: 0,
            released_claims: 0,
            failed_claims: 0,
            scanned_buckets: 0,
            expired_current_objects: 0,
            expired_noncurrent_versions: 0,
            expired_delete_markers: 0,
            skipped_expired_delete_markers: 0,
            aborted_multipart_uploads: 0,
        };
        let mut processed_buckets: HashSet<(BucketName, u64)> = HashSet::new();
        let claim_now_millis = storage::clock::wall_time_millis();
        let sweep_roots = self
            .storage_node
            .list_lifecycle_sweep_roots(claim_now_millis)
            .map_err(Coordinator::map_object_pg_action_error)?;
        stats.discovered_roots = sweep_roots.len() as u64;
        let _ = observability::event(
            TRACE_TARGET,
            "lifecycle_sweep_pass_start",
            Some(format_args!(
                "roots={} lifecycle_now_millis={} claim_now_millis={}",
                stats.discovered_roots, now_millis, claim_now_millis
            )),
        );

        for root in sweep_roots {
            let claim_now_millis = storage::clock::wall_time_millis();
            let Some(claim) = self
                .storage_node
                .acquire_lifecycle_sweep_claim(
                    &root.bucket,
                    root.bucket_incarnation_generation,
                    claim_now_millis,
                )
                .map_err(Coordinator::map_object_pg_action_error)?
            else {
                stats.busy_claims += 1;
                let _ = observability::event(
                    TRACE_TARGET,
                    "lifecycle_sweep_claim_busy",
                    Some(format_args!(
                        "bucket={:?} incarnation={} source={:?}",
                        root.bucket, root.bucket_incarnation_generation, root.source
                    )),
                );
                continue;
            };
            stats.acquired_claims += 1;
            if claim.attempt_count > 1 {
                stats.recovered_expired_claims += 1;
            }
            let _ = observability::event(
                TRACE_TARGET,
                "lifecycle_sweep_claim_acquired",
                Some(format_args!(
                    "bucket={:?} incarnation={} claim_id={} source={:?} attempt_count={}",
                    claim.bucket,
                    claim.bucket_incarnation_generation,
                    claim.claim_id,
                    root.source,
                    claim.attempt_count
                )),
            );
            if !processed_buckets.insert((root.bucket.clone(), root.bucket_incarnation_generation))
            {
                self.storage_node
                    .release_lifecycle_sweep_claim(&claim)
                    .map_err(Coordinator::map_object_pg_action_error)?;
                stats.released_claims += 1;
                continue;
            }
            let claim = self
                .storage_node
                .heartbeat_lifecycle_sweep_claim(&claim, storage::clock::wall_time_millis())
                .map_err(Coordinator::map_object_pg_action_error)?;

            stats.scanned_buckets += 1;
            let result =
                self.run_claimed_lifecycle_sweep_for_bucket(&claim, now_millis, &mut stats);
            match result {
                Ok(()) => {
                    self.storage_node
                        .release_lifecycle_sweep_claim(&claim)
                        .map_err(Coordinator::map_object_pg_action_error)?;
                    stats.released_claims += 1;
                    let _ = observability::event(
                        TRACE_TARGET,
                        "lifecycle_sweep_claim_released",
                        Some(format_args!(
                            "bucket={:?} incarnation={} claim_id={}",
                            claim.bucket, claim.bucket_incarnation_generation, claim.claim_id
                        )),
                    );
                }
                Err(error) => {
                    stats.failed_claims += 1;
                    let error_context = lifecycle_sweep_error_context(&error);
                    let record_result = self
                        .storage_node
                        .record_lifecycle_sweep_claim_error(&claim, &error_context);
                    match record_result {
                        Ok(_) => {
                            let _ = observability::event(
                                TRACE_TARGET,
                                "lifecycle_sweep_claim_error_recorded",
                                Some(format_args!(
                                    "bucket={:?} incarnation={} claim_id={} failed_claims={} error={}",
                                    claim.bucket,
                                    claim.bucket_incarnation_generation,
                                    claim.claim_id,
                                    stats.failed_claims,
                                    error_context
                                )),
                            );
                        }
                        Err(record_error) => {
                            let _ = observability::event(
                                TRACE_TARGET,
                                "lifecycle_sweep_claim_error_record_failed",
                                Some(format_args!(
                                    "bucket={:?} claim_id={} failed_claims={} error={} record_error={record_error}",
                                    claim.bucket, claim.claim_id, stats.failed_claims, error_context
                                )),
                            );
                        }
                    }
                    return Err(error);
                }
            }
        }

        let _ = observability::event(
            TRACE_TARGET,
            "lifecycle_sweep_pass_complete",
            Some(format_args!(
                "roots={} acquired_claims={} busy_claims={} recovered_expired_claims={} released_claims={} failed_claims={} scanned_buckets={} expired_current_objects={} expired_noncurrent_versions={} expired_delete_markers={} skipped_expired_delete_markers={} aborted_multipart_uploads={}",
                stats.discovered_roots,
                stats.acquired_claims,
                stats.busy_claims,
                stats.recovered_expired_claims,
                stats.released_claims,
                stats.failed_claims,
                stats.scanned_buckets,
                stats.expired_current_objects,
                stats.expired_noncurrent_versions,
                stats.expired_delete_markers,
                stats.skipped_expired_delete_markers,
                stats.aborted_multipart_uploads
            )),
        );

        Ok(stats)
    }

    fn run_claimed_lifecycle_sweep_for_bucket(
        &self,
        claim: &storage::LifecycleSweepClaimRecord,
        now_millis: u64,
        stats: &mut LifecycleSweepStats,
    ) -> Result<(), ServerError> {
        let bucket = &claim.bucket;
        let bucket_info = match self.storage_node.head_bucket_info(bucket) {
            Ok(bucket_info) => bucket_info,
            Err(storage::BucketSnapshotLoadError::Metadata(
                storage::MetadataError::BucketNotFound { .. },
            )) => return Ok(()),
            Err(error) => return Err(Self::map_bucket_snapshot_error(error)),
        };
        if bucket_info.bucket_incarnation_generation != claim.bucket_incarnation_generation {
            return Ok(());
        }

        self.expire_due_current_objects_for_bucket(claim, &bucket_info, now_millis, stats)?;
        self.expire_due_noncurrent_versions_for_bucket(claim, &bucket_info, now_millis, stats)?;
        self.expire_due_delete_markers_for_bucket(claim, &bucket_info, now_millis, stats)?;
        self.abort_due_multipart_uploads_for_bucket(claim, &bucket_info, now_millis, stats)?;
        self.heartbeat_lifecycle_sweep_claim(claim)?;
        stats.aborted_multipart_uploads +=
            self.finish_aborting_multipart_uploads_for_bucket(claim, &bucket_info.name)?;
        Ok(())
    }

    fn heartbeat_lifecycle_sweep_claim(
        &self,
        claim: &storage::LifecycleSweepClaimRecord,
    ) -> Result<(), ServerError> {
        self.storage_node
            .heartbeat_lifecycle_sweep_claim(claim, storage::clock::wall_time_millis())
            .map(drop)
            .map_err(Coordinator::map_object_pg_action_error)
    }

    fn expire_due_current_objects_for_bucket(
        &self,
        claim: &storage::LifecycleSweepClaimRecord,
        bucket_info: &BucketInfo,
        now_millis: u64,
        stats: &mut LifecycleSweepStats,
    ) -> Result<(), ServerError> {
        let Some(config) = self.lifecycle_config_for_bucket_info(bucket_info)? else {
            return Ok(());
        };

        let mut candidates = Vec::new();
        let objects = self
            .storage_node
            .list_all_objects_for_bucket(&bucket_info.name)
            .map_err(Coordinator::map_object_pg_action_error)?;
        for (index, object) in objects.into_iter().enumerate() {
            if index > 0 && index % LIFECYCLE_SWEEP_HEARTBEAT_INTERVAL_ITEMS == 0 {
                self.heartbeat_lifecycle_sweep_claim(claim)?;
            }
            let Some(record) = object.into_live() else {
                continue;
            };
            let tags = match record.tags.as_deref() {
                Some(tags_xml) => Coordinator::parse_serialized_tag_set(tags_xml)?,
                None => Vec::new(),
            };
            let Some(expiration) = Coordinator::evaluate_current_object_lifecycle_expiration(
                &config,
                record.key.as_str(),
                &tags,
                record.size,
                record.last_modified,
            ) else {
                continue;
            };
            if expiration.expiry_time_millis <= now_millis {
                candidates.push((record.key, record.version_id));
            }
        }

        for (key, version_id) in candidates {
            self.heartbeat_lifecycle_sweep_claim(claim)?;
            if self.expire_current_object_if_due(
                &bucket_info.name,
                &key,
                version_id,
                claim.bucket_incarnation_generation,
                now_millis,
            )? {
                stats.expired_current_objects += 1;
            }
        }

        Ok(())
    }

    fn finish_aborting_multipart_uploads_for_bucket(
        &self,
        claim: &storage::LifecycleSweepClaimRecord,
        bucket: &BucketName,
    ) -> Result<u64, ServerError> {
        let mut candidates = Vec::new();
        let uploads = self
            .storage_node
            .list_all_multipart_uploads_for_bucket(bucket)
            .map_err(Coordinator::map_object_pg_action_error)?;
        for (index, upload) in uploads.into_iter().enumerate() {
            if index > 0 && index % LIFECYCLE_SWEEP_HEARTBEAT_INTERVAL_ITEMS == 0 {
                self.heartbeat_lifecycle_sweep_claim(claim)?;
            }
            if upload.state == UploadState::Aborting {
                candidates.push((upload.key, upload.upload_id));
            }
        }

        let mut finished = 0u64;
        for (key, upload_id) in candidates {
            if self.abort_multipart_upload_for_lifecycle_sweep(
                bucket,
                &key,
                &upload_id,
                claim.bucket_incarnation_generation,
            )? {
                finished += 1;
            }
        }
        Ok(finished)
    }

    fn expire_due_noncurrent_versions_for_bucket(
        &self,
        claim: &storage::LifecycleSweepClaimRecord,
        bucket_info: &BucketInfo,
        now_millis: u64,
        stats: &mut LifecycleSweepStats,
    ) -> Result<(), ServerError> {
        let Some(config) = self.lifecycle_config_for_bucket_info(bucket_info)? else {
            return Ok(());
        };

        let mut candidate_keys = Vec::new();
        let versions = self
            .storage_node
            .list_all_object_versions_for_bucket(&bucket_info.name)
            .map_err(Coordinator::map_object_pg_action_error)?;
        let mut group_start = 0usize;
        let mut groups_seen = 0usize;
        while group_start < versions.len() {
            if groups_seen > 0
                && groups_seen.is_multiple_of(LIFECYCLE_SWEEP_HEARTBEAT_INTERVAL_ITEMS)
            {
                self.heartbeat_lifecycle_sweep_claim(claim)?;
            }
            groups_seen += 1;
            let key = versions[group_start].key().clone();
            let mut group_end = group_start + 1;
            while group_end < versions.len() && versions[group_end].key() == &key {
                group_end += 1;
            }

            if !Coordinator::evaluate_due_noncurrent_version_expirations(
                &config,
                &versions[group_start..group_end],
                now_millis,
            )?
            .is_empty()
            {
                candidate_keys.push(key);
            }
            group_start = group_end;
        }

        for key in candidate_keys {
            self.heartbeat_lifecycle_sweep_claim(claim)?;
            stats.expired_noncurrent_versions += self.expire_noncurrent_versions_if_due(
                &bucket_info.name,
                &key,
                claim.bucket_incarnation_generation,
                now_millis,
            )?;
        }

        Ok(())
    }

    pub(super) fn expire_current_object_if_due(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        expected_version_id: VersionId,
        expected_bucket_incarnation_generation: u64,
        now_millis: u64,
    ) -> Result<bool, ServerError> {
        let outcome = self
            .storage_node
            .expire_current_object_if_due(
                bucket,
                key,
                expected_version_id,
                expected_bucket_incarnation_generation,
                |raw_lifecycle, record| {
                    let Some(config) =
                        Self::parse_lifecycle_config(bucket.as_str(), raw_lifecycle)?
                    else {
                        return Ok::<bool, ServerError>(false);
                    };
                    let tags = match record.tags.as_deref() {
                        Some(tags_xml) => Coordinator::parse_serialized_tag_set(tags_xml)?,
                        None => Vec::new(),
                    };
                    let Some(expiration) =
                        Coordinator::evaluate_current_object_lifecycle_expiration(
                            &config,
                            key.as_str(),
                            &tags,
                            record.size,
                            record.last_modified,
                        )
                    else {
                        return Ok::<bool, ServerError>(false);
                    };
                    Ok::<bool, ServerError>(expiration.expiry_time_millis <= now_millis)
                },
            )
            .map_err(Coordinator::map_object_pg_action_error)??;

        let Some(outcome) = outcome else {
            return Ok(false);
        };
        if let Some(generation_id) = outcome.reclaim_generation_id {
            self.enqueue_object_payload_reclaim_for(bucket, key, generation_id);
        }
        Ok(true)
    }

    fn expire_noncurrent_versions_if_due(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        expected_bucket_incarnation_generation: u64,
        now_millis: u64,
    ) -> Result<u64, ServerError> {
        let current_unix_seconds = Coordinator::current_unix_seconds()?;
        let reclaimed_generation_ids = self
            .storage_node
            .delete_noncurrent_live_versions_if_due(
                bucket,
                key,
                expected_bucket_incarnation_generation,
                |raw_lifecycle, versions| {
                    let Some(config) =
                        Self::parse_lifecycle_config(bucket.as_str(), raw_lifecycle)?
                    else {
                        return Ok::<HashSet<VersionId>, ServerError>(HashSet::new());
                    };
                    let due_versions = Coordinator::evaluate_due_noncurrent_version_expirations(
                        &config, versions, now_millis,
                    )?;
                    let due_version_ids = due_versions
                        .iter()
                        .map(|candidate| candidate.version_id)
                        .collect::<HashSet<_>>();

                    let eligible_version_ids = versions
                        .iter()
                        .filter_map(|stored| stored.as_live())
                        .filter(|record| due_version_ids.contains(&record.version_id))
                        .filter(|record| {
                            Coordinator::validate_delete_against_object_lock(
                                record.object_lock,
                                false,
                                false,
                                current_unix_seconds,
                            )
                            .is_ok()
                        })
                        .map(|record| record.version_id)
                        .collect();
                    Ok::<HashSet<VersionId>, ServerError>(eligible_version_ids)
                },
            )
            .map_err(Coordinator::map_object_pg_action_error)??;

        for generation_id in &reclaimed_generation_ids {
            self.enqueue_object_payload_reclaim_for(bucket, key, *generation_id);
        }

        Ok(reclaimed_generation_ids.len() as u64)
    }

    fn expire_due_delete_markers_for_bucket(
        &self,
        claim: &storage::LifecycleSweepClaimRecord,
        bucket_info: &BucketInfo,
        now_millis: u64,
        stats: &mut LifecycleSweepStats,
    ) -> Result<(), ServerError> {
        let Some(config) = self.lifecycle_config_for_bucket_info(bucket_info)? else {
            return Ok(());
        };

        let mut candidates = Vec::new();
        let versions = self
            .storage_node
            .list_all_object_versions_for_bucket(&bucket_info.name)
            .map_err(Coordinator::map_object_pg_action_error)?;
        let mut group_start = 0usize;
        let mut groups_seen = 0usize;
        while group_start < versions.len() {
            if groups_seen > 0
                && groups_seen.is_multiple_of(LIFECYCLE_SWEEP_HEARTBEAT_INTERVAL_ITEMS)
            {
                self.heartbeat_lifecycle_sweep_claim(claim)?;
            }
            groups_seen += 1;
            let key = versions[group_start].key().clone();
            let mut group_end = group_start + 1;
            while group_end < versions.len() && versions[group_end].key() == &key {
                group_end += 1;
            }

            if let Some(expiration) = Coordinator::evaluate_due_expired_delete_marker(
                &config,
                &versions[group_start..group_end],
                now_millis,
            ) {
                candidates.push((key, expiration.version_id));
            }
            group_start = group_end;
        }

        for (key, version_id) in candidates {
            self.heartbeat_lifecycle_sweep_claim(claim)?;
            if self.expire_delete_marker_if_due(
                &bucket_info.name,
                &key,
                version_id,
                claim.bucket_incarnation_generation,
                now_millis,
            )? {
                stats.expired_delete_markers += 1;
            } else {
                stats.skipped_expired_delete_markers += 1;
            }
        }

        Ok(())
    }

    fn expire_delete_marker_if_due(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        expected_version_id: VersionId,
        expected_bucket_incarnation_generation: u64,
        now_millis: u64,
    ) -> Result<bool, ServerError> {
        self.storage_node
            .delete_expired_delete_marker_if_due(
                bucket,
                key,
                expected_version_id,
                expected_bucket_incarnation_generation,
                |raw_lifecycle, versions| {
                    let Some(config) =
                        Self::parse_lifecycle_config(bucket.as_str(), raw_lifecycle)?
                    else {
                        return Ok::<bool, ServerError>(false);
                    };
                    let Some(expiration) = Coordinator::evaluate_due_expired_delete_marker(
                        &config, versions, now_millis,
                    ) else {
                        return Ok::<bool, ServerError>(false);
                    };
                    Ok::<bool, ServerError>(expiration.version_id == expected_version_id)
                },
            )
            .map_err(Coordinator::map_object_pg_action_error)?
    }

    fn abort_due_multipart_uploads_for_bucket(
        &self,
        claim: &storage::LifecycleSweepClaimRecord,
        bucket_info: &BucketInfo,
        now_millis: u64,
        stats: &mut LifecycleSweepStats,
    ) -> Result<(), ServerError> {
        let Some(config) = self.lifecycle_config_for_bucket_info(bucket_info)? else {
            return Ok(());
        };

        let mut candidates = Vec::new();
        let uploads = self
            .storage_node
            .list_all_multipart_uploads_for_bucket(&bucket_info.name)
            .map_err(Coordinator::map_object_pg_action_error)?;
        for (index, upload) in uploads.into_iter().enumerate() {
            if index > 0 && index % LIFECYCLE_SWEEP_HEARTBEAT_INTERVAL_ITEMS == 0 {
                self.heartbeat_lifecycle_sweep_claim(claim)?;
            }
            if upload.state != UploadState::InProgress && upload.state != UploadState::Aborting {
                continue;
            }
            let Some(headers) = Coordinator::evaluate_multipart_lifecycle_abort_headers(
                &config,
                upload.key.as_str(),
                upload.initiated_at,
            ) else {
                continue;
            };
            if headers.abort_time_millis <= now_millis {
                candidates.push((upload.key, upload.upload_id));
            }
        }

        for (key, upload_id) in candidates {
            self.heartbeat_lifecycle_sweep_claim(claim)?;
            if self.abort_multipart_upload_if_due(
                &bucket_info.name,
                &key,
                &upload_id,
                claim.bucket_incarnation_generation,
                now_millis,
            )? {
                stats.aborted_multipart_uploads += 1;
            }
        }

        Ok(())
    }

    pub(super) fn abort_multipart_upload_if_due(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        expected_bucket_incarnation_generation: u64,
        now_millis: u64,
    ) -> Result<bool, ServerError> {
        #[cfg(feature = "deep-tracing")]
        if let Some(trace) = observability::current_context() {
            let _ = observability::event_in_context(
                &trace,
                TRACE_TARGET,
                "multipart_abort_request",
                Some(format_args!(
                    "source=lifecycle-check bucket={:?} key={:?} upload_id={:?} now_millis={}",
                    bucket, key, upload_id, now_millis
                )),
            );
        }
        self.storage_node
            .abort_multipart_upload_if_due(
                bucket,
                key,
                upload_id,
                expected_bucket_incarnation_generation,
                |raw_lifecycle, upload| {
                    let Some(config) =
                        Self::parse_lifecycle_config(bucket.as_str(), raw_lifecycle)?
                    else {
                        return Ok::<bool, ServerError>(false);
                    };

                    let Some(headers) = Coordinator::evaluate_multipart_lifecycle_abort_headers(
                        &config,
                        key.as_str(),
                        upload.initiated_at,
                    ) else {
                        return Ok::<bool, ServerError>(false);
                    };
                    Ok::<bool, ServerError>(headers.abort_time_millis <= now_millis)
                },
            )
            .map_err(Coordinator::map_object_pg_action_error)?
    }

    pub(super) fn abort_multipart_upload_for_lifecycle_sweep(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        expected_bucket_incarnation_generation: u64,
    ) -> Result<bool, ServerError> {
        self.storage_node
            .abort_multipart_upload_for_lifecycle_sweep(
                bucket,
                key,
                upload_id,
                expected_bucket_incarnation_generation,
            )
            .map_err(Coordinator::map_object_pg_action_error)
    }

    pub(super) fn abort_authorized_multipart_upload_internal(
        &self,
        upload: &AuthorizedMultipartUploadRecord,
    ) -> Result<bool, ServerError> {
        self.storage_node
            .abort_authorized_multipart_upload(upload)
            .map_err(Coordinator::map_object_pg_action_error)
    }

    #[cfg(test)]
    pub(super) fn acquire_object_payload_lease_for(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<PayloadLease, ServerError> {
        let lease = self
            .storage_node
            .acquire_object_payload_lease(bucket, key, generation_id)?;
        Ok(PayloadLease { lease: Some(lease) })
    }

    pub(super) fn acquire_object_payload_lease_for_shard_locations(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        locations: &[storage::ShardLocation],
    ) -> Result<PayloadLease, ServerError> {
        let lease = self
            .storage_node
            .acquire_object_payload_lease_for_shard_locations(
                bucket,
                key,
                generation_id,
                locations,
            )?;
        Ok(PayloadLease { lease: Some(lease) })
    }

    #[cfg(test)]
    pub(super) fn acquire_object_payload_lease(
        &self,
        bucket: &str,
        key: &str,
        generation_id: GenerationId,
    ) -> PayloadLease {
        self.acquire_object_payload_lease_for(
            &trusted_bucket_name(bucket),
            &trusted_object_key(key),
            generation_id,
        )
        .expect("current storage cluster should acquire payload lease")
    }

    #[cfg(test)]
    pub(super) fn try_reclaim_object_payload_for(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<bool, ServerError> {
        self.storage_node
            .reclaim_object_payload_if_unleased(bucket, key, generation_id)
            .map_err(Coordinator::map_object_pg_action_error)
    }

    fn try_reclaim_object_payload_for_with_outcome(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<storage::cluster::ObjectPayloadReclaimAttempt, ServerError> {
        self.storage_node
            .reclaim_object_payload_if_unleased_with_outcome(bucket, key, generation_id)
            .map_err(Coordinator::map_object_pg_action_error)
    }

    #[cfg(test)]
    pub(super) fn try_reclaim_object_payload(
        &self,
        bucket: &str,
        key: &str,
        generation_id: GenerationId,
    ) -> Result<bool, ServerError> {
        self.try_reclaim_object_payload_for(
            &trusted_bucket_name(bucket),
            &trusted_object_key(key),
            generation_id,
        )
    }

    pub(super) fn try_finalize_bucket_delete_for(
        &self,
        bucket: &BucketName,
    ) -> Result<(), ServerError> {
        match self.storage_node.try_finalize_bucket_delete(bucket) {
            Ok(
                storage::BucketDeleteFinalizeOutcome::NotFound
                | storage::BucketDeleteFinalizeOutcome::NotDeleting
                | storage::BucketDeleteFinalizeOutcome::Pending
                | storage::BucketDeleteFinalizeOutcome::Finalized,
            ) => Ok(()),
            Err(error) => Err(super::bucket::map_bucket_write_drain_error(error)),
        }
    }

    pub(super) fn read_checked_segment_payload(
        &self,
        segment: &SegmentPayloadRecord,
        part_number: Option<u32>,
        sse_customer_request: Option<&SseCustomerRequest>,
    ) -> Result<Arc<SharedPayloadBuffer>, ServerError> {
        let k = segment.ec_k as usize;
        let padded = segment.stored_size().div_ceil(k) * k;

        let mut buf = self.payload_buffer_pool.checkout(padded);
        self.storage_node
            .read_segment_payload_stored_bytes_into(
                SegmentStoredBytesRequest {
                    data_pg_id: segment.data_pg_id,
                    segment_okh: segment.segment_okh,
                    segment_vid: segment.segment_vid,
                    stored_size: segment.stored_size(),
                    segment_crc64: segment.segment_crc64,
                    ec: EcShape {
                        k: segment.ec_k,
                        m: segment.ec_m,
                    },
                },
                &mut buf,
            )
            .map_err(super::map_store_error)?;
        if matches!(segment.encryption, ObjectEncryption::None) {
            Ok(buf.into_shared())
        } else {
            let plaintext =
                self.decrypt_segment_if_needed(segment, part_number, sse_customer_request, &buf)?;
            buf.resize_zeroed(0);
            buf.extend_from_slice(&plaintext);
            Ok(buf.into_shared())
        }
    }

    fn decrypt_segment_if_needed(
        &self,
        segment: &SegmentPayloadRecord,
        part_number: Option<u32>,
        sse_customer_request: Option<&SseCustomerRequest>,
        stored_bytes: &[u8],
    ) -> Result<Vec<u8>, ServerError> {
        match &segment.encryption {
            ObjectEncryption::None => Ok(stored_bytes.to_vec()),
            ObjectEncryption::SseCustomer(state) => {
                let request = sse_customer_request.ok_or(ServerError::InvalidRequest {
                    reason: "SSE-C headers are required for this object".to_string(),
                })?;
                let validator =
                    self.sse_c_validator
                        .as_ref()
                        .ok_or(ServerError::InternalError {
                            reason: "SSE-C validator key is not configured".to_string(),
                        })?;
                let segment_scope = part_number
                    .map_or(Ok(SseCustomerSegmentScope::object()), |p| {
                        SseCustomerSegmentScope::multipart_part(p)
                    })?;
                decrypt_sse_customer_segment(
                    validator,
                    state,
                    request,
                    segment_scope,
                    segment.segment_index,
                    stored_bytes,
                    segment.size as usize,
                )
            }
            ObjectEncryption::SseS3(state) => {
                let provider =
                    self.managed_key_provider
                        .as_ref()
                        .ok_or(ServerError::InternalError {
                            reason: "SSE-S3 key provider is not configured".to_string(),
                        })?;
                let segment_scope = part_number
                    .map_or(Ok(SseCustomerSegmentScope::object()), |p| {
                        SseCustomerSegmentScope::multipart_part(p)
                    })?;
                decrypt_managed_encryption_segment(
                    provider,
                    state,
                    segment_scope,
                    segment.segment_index,
                    stored_bytes,
                    segment.size as usize,
                )
            }
        }
    }
}

fn lifecycle_sweep_error_context(error: &ServerError) -> String {
    let raw = format!("{error:?}");
    if raw.chars().count() <= LIFECYCLE_SWEEP_ERROR_CONTEXT_MAX_CHARS {
        return raw;
    }

    raw.chars()
        .take(LIFECYCLE_SWEEP_ERROR_CONTEXT_MAX_CHARS)
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use storage::{
        ClusterEpoch, EcShape, GenerationId, LocalClusterMap, NodeId, ObjectEncryption, PgTopology,
        StorageCluster, StoreError,
    };

    use super::super::payload::PayloadBufferPool;
    use super::*;

    #[test]
    fn background_work_admission_limits_and_releases_per_class() {
        let admission = Arc::new(BackgroundWorkAdmission::with_limits(
            BackgroundWorkAdmissionLimits {
                known_damage_repair: 1,
                reclaim_cleanup: 1,
                lifecycle_cleanup: 1,
                stream_session_cleanup: 1,
                opportunistic_scan: 0,
            },
        ));

        let repair_permit = admission
            .try_acquire(BackgroundWorkClass::KnownDamageRepair)
            .expect("first repair permit should fit limit");
        assert!(
            admission
                .try_acquire(BackgroundWorkClass::KnownDamageRepair)
                .is_none(),
            "second repair permit should be denied at limit"
        );

        let reclaim_permit = admission
            .try_acquire(BackgroundWorkClass::ReclaimCleanup)
            .expect("reclaim cleanup should have its own class limit");
        assert!(
            admission
                .try_acquire(BackgroundWorkClass::ReclaimCleanup)
                .is_none(),
            "second reclaim cleanup permit should be denied at limit"
        );
        let lifecycle_permit = admission
            .try_acquire(BackgroundWorkClass::LifecycleCleanup)
            .expect("lifecycle cleanup should have its own class limit");
        assert!(
            admission
                .try_acquire(BackgroundWorkClass::LifecycleCleanup)
                .is_none(),
            "second lifecycle cleanup permit should be denied at limit"
        );
        let stream_session_permit = admission
            .try_acquire(BackgroundWorkClass::StreamSessionCleanup)
            .expect("stream session cleanup should have its own class limit");
        assert!(
            admission
                .try_acquire(BackgroundWorkClass::StreamSessionCleanup)
                .is_none(),
            "second stream session cleanup permit should be denied at limit"
        );
        assert_eq!(admission.active_total(), 4);
        assert!(
            admission
                .try_acquire(BackgroundWorkClass::OpportunisticScan)
                .is_none(),
            "zero-limit scan class should deny"
        );

        drop(repair_permit);
        assert_eq!(admission.active_total(), 3);
        let replacement = admission
            .try_acquire(BackgroundWorkClass::KnownDamageRepair)
            .expect("dropping a permit should release class capacity");
        drop(replacement);
        drop(reclaim_permit);
        drop(lifecycle_permit);
        drop(stream_session_permit);
        assert_eq!(admission.active_total(), 0);
    }

    #[test]
    fn background_work_pressure_uses_recent_counter_deltas() {
        let mut state = BackgroundWorkPressureState::default();
        let now = Instant::now();
        let mut snapshot = observability::MetricsSnapshot::default();

        assert_eq!(
            state.observe(now, snapshot),
            BackgroundWorkPressure {
                foreground: false,
                durable_backlog: false,
            }
        );

        snapshot.request_admission_wait_total += 1;
        let production_sweep_gap = Duration::from_millis(60_000);
        assert_eq!(
            state.observe(now + production_sweep_gap, snapshot),
            BackgroundWorkPressure {
                foreground: false,
                durable_backlog: false,
            },
            "counter deltas observed after a scavenger sweep gap are stale"
        );

        snapshot.storage_rpc_admission_wait_total += 1;
        let sampler_tick_before_next_scan = now + (production_sweep_gap * 2)
            - BACKGROUND_FOREGROUND_PRESSURE_SAMPLE_INTERVAL
            - Duration::from_millis(20);
        assert_eq!(
            state.observe(sampler_tick_before_next_scan, snapshot),
            BackgroundWorkPressure {
                foreground: false,
                durable_backlog: false,
            }
        );

        snapshot.storage_rpc_admission_timeout_total += 1;
        let recent = now + (production_sweep_gap * 2);
        assert_eq!(
            state.observe(recent, snapshot),
            BackgroundWorkPressure {
                foreground: true,
                durable_backlog: false,
            },
            "a faster sampler catches pressure just before a production scan"
        );

        assert_eq!(
            state.observe(
                recent + BACKGROUND_FOREGROUND_PRESSURE_HOLD + Duration::from_millis(20),
                snapshot,
            ),
            BackgroundWorkPressure {
                foreground: false,
                durable_backlog: false,
            },
            "old cumulative counters must not keep scans denied forever"
        );
    }

    #[test]
    fn background_work_pressure_ignores_metadata_recovery_counters() {
        let mut state = BackgroundWorkPressureState::default();
        let now = Instant::now();
        let mut snapshot = observability::MetricsSnapshot::default();

        assert_eq!(
            state.observe(now, snapshot),
            BackgroundWorkPressure {
                foreground: false,
                durable_backlog: false,
            }
        );

        snapshot.metadata_command_recovery_wait_total += 1;
        snapshot.metadata_command_recovery_timeout_total += 1;
        snapshot.metadata_command_budget_exhausted_total += 1;
        assert_eq!(
            state.observe(now + Duration::from_millis(10), snapshot),
            BackgroundWorkPressure {
                foreground: false,
                durable_backlog: false,
            },
            "process-wide metadata recovery contention is not necessarily foreground S3 pressure"
        );
    }

    #[test]
    fn background_work_pressure_detects_active_foreground_and_durable_backlog() {
        let mut state = BackgroundWorkPressureState::default();
        let now = Instant::now();
        let mut snapshot = observability::MetricsSnapshot {
            inflight_requests: 1,
            reclaim_work_queue_depth: 7,
            ..observability::MetricsSnapshot::default()
        };

        assert_eq!(
            state.observe(now, snapshot),
            BackgroundWorkPressure {
                foreground: true,
                durable_backlog: true,
            }
        );

        snapshot.inflight_requests = 0;
        assert_eq!(
            state.observe(now + Duration::from_millis(10), snapshot),
            BackgroundWorkPressure {
                foreground: false,
                durable_backlog: true,
            }
        );
    }

    #[test]
    fn stale_read_runtime_rejects_zero_size_segment_payload() {
        let tmp = test_util::tempdir();
        let node_ids = [
            NodeId::new(0),
            NodeId::new(1),
            NodeId::new(2),
            NodeId::new(3),
            NodeId::new(4),
            NodeId::new(5),
        ];
        let ec_shape = EcShape { k: 4, m: 2 };
        let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap());
        let stale_cluster = StorageCluster::test_from_local_map_with_epoch(
            Arc::clone(&map),
            ClusterEpoch::new(2).unwrap(),
        )
        .unwrap();
        let runtime = ReadRuntime {
            storage_node: stale_cluster,
            pg_topology: PgTopology::new(&[0]).unwrap(),
            payload_buffer_pool: PayloadBufferPool::new(ec_shape),
            sse_c_validator: None,
            managed_key_provider: None,
        };
        let segment = SegmentPayloadRecord {
            segment_index: 0,
            size: 0,
            segment_crc64: 0,
            segment_okh: [61; 16],
            segment_vid: GenerationId::MIN,
            data_pg_id: 0,
            ec_k: ec_shape.k,
            ec_m: ec_shape.m,
            encryption: ObjectEncryption::None,
        };

        let err = runtime
            .read_checked_segment_payload(&segment, None, None)
            .unwrap_err();

        assert!(matches!(
            err,
            ServerError::Store(StoreError::StalePayloadOperation {
                pg_id: 0,
                operation_epoch,
                current_epoch: ClusterEpoch::INITIAL,
            }) if operation_epoch == ClusterEpoch::new(2).unwrap()
        ));
    }
}
