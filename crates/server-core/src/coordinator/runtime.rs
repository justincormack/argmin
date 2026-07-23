use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use s3_types::BucketLifecycleConfiguration;
#[cfg(test)]
use storage::PgTopology;
use storage::{
    AuthorizedMultipartUploadRecord, BucketDeleteBeginRoot, BucketDeleteFinalizeRoot, BucketInfo,
    BucketName, EcShape, GenerationId, ObjectEncryption, ObjectKey,
    PlacedSegmentShardBackfillClaimAcquireParams, PlacedSegmentShardRepairClaimAcquireParams,
    ReclaimWorkItem, SegmentStoredBytesRequest, StorageCluster, StorageClusterRuntimeMapHandle,
    StoreError, UploadId, UploadState, VersionId,
};

use super::payload::SharedPayloadBuffer;
use super::read_core::{PayloadLease, ReadRuntime, SegmentPayloadRecord};
#[cfg(test)]
use super::test_hooks::{
    maybe_run_after_reclaim_work_dequeued_hook, maybe_run_before_reclaim_work_execute_hook,
    maybe_run_reclaim_worker_idle_return_hook, maybe_run_shard_repair_worker_idle_timeout_hook,
    reclaim_worker_durable_scan_delay_override,
};
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
static RECLAIM_SWEEPER_REGISTRY: OnceLock<Mutex<HashMap<usize, Weak<ReclaimSweeper>>>> =
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
const BUCKET_DELETE_BEGIN_RETRY_COOLDOWN: Duration = Duration::from_millis(100);
const BUCKET_DELETE_FINALIZE_RETRY_COOLDOWN: Duration = Duration::from_millis(100);
const BUCKET_DELETE_FINALIZE_ERROR_RETRY_COOLDOWN: Duration = Duration::from_secs(1);
const RECLAIM_DURABLE_SCAN_BATCH_PGS: usize = 8;
const RECLAIM_DURABLE_SCAN_SAFETY_INTERVAL: Duration = Duration::from_secs(60);
const RECLAIM_DURABLE_SCAN_ROUTE_REFRESH_BACKOFF: Duration = Duration::from_secs(1);
const RECLAIM_DURABLE_SCAN_INCOMPLETE_RETRY: Duration = Duration::from_secs(1);
const SHARD_REPAIR_DURABLE_SCAN_INTERVAL: Duration = Duration::from_millis(250);
const SHARD_REPAIR_CLAIM_LEASE_MILLIS: u64 = 30_000;
const SHARD_REPAIR_ERROR_BACKOFF_MILLIS: u64 = 1_000;
const SHARD_BACKFILL_DURABLE_SCAN_INTERVAL: Duration = Duration::from_millis(250);
const SHARD_BACKFILL_CLAIM_LEASE_MILLIS: u64 = 30_000;
const SHARD_BACKFILL_ERROR_BACKOFF_MILLIS: u64 = 1_000;
const LIFECYCLE_SWEEP_HEARTBEAT_INTERVAL_ITEMS: usize = 256;
const LIFECYCLE_SWEEP_ERROR_CONTEXT_MAX_CHARS: usize = 1024;
const BACKGROUND_KNOWN_DAMAGE_REPAIR_LIMIT: usize = 1;
const BACKGROUND_BACKFILL_CANDIDATE_SCAN_LIMIT: usize = 1;
const BACKGROUND_ROUTINE_BACKFILL_LIMIT: usize = 1;
const BACKGROUND_RECLAIM_CLEANUP_LIMIT: usize = 1;
const BACKGROUND_LIFECYCLE_CLEANUP_LIMIT: usize = 1;
const BACKGROUND_STREAM_SESSION_CLEANUP_LIMIT: usize = 1;
const BACKGROUND_OPPORTUNISTIC_SCAN_LIMIT: usize = 1;
const BACKGROUND_ROUTINE_METADATA_CHECKPOINT_LIMIT: usize = 1;
const BACKGROUND_FOREGROUND_PRESSURE_HOLD: Duration = Duration::from_millis(1_000);
const BACKGROUND_FOREGROUND_PRESSURE_SAMPLE_INTERVAL: Duration = Duration::from_millis(250);
const BACKGROUND_FOREGROUND_PRESSURE_MAX_SAMPLE_GAP: Duration = Duration::from_millis(1_250);

type ObjectPayloadReclaimRoot = (BucketName, ObjectKey, GenerationId);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BackgroundWorkClass {
    KnownDamageRepair,
    BackfillCandidateScan,
    RoutineBackfill,
    ReclaimCleanup,
    LifecycleCleanup,
    StreamSessionCleanup,
    OpportunisticScan,
    RoutineMetadataCheckpoint,
}

impl BackgroundWorkClass {
    fn observability_class(self) -> observability::BackgroundWorkClass {
        match self {
            Self::KnownDamageRepair => observability::BackgroundWorkClass::KnownDamageRepair,
            Self::BackfillCandidateScan => {
                observability::BackgroundWorkClass::BackfillCandidateScan
            }
            Self::RoutineBackfill => observability::BackgroundWorkClass::RoutineBackfill,
            Self::ReclaimCleanup => observability::BackgroundWorkClass::ReclaimCleanup,
            Self::LifecycleCleanup => observability::BackgroundWorkClass::LifecycleCleanup,
            Self::StreamSessionCleanup => observability::BackgroundWorkClass::StreamSessionCleanup,
            Self::OpportunisticScan => observability::BackgroundWorkClass::OpportunisticScan,
            Self::RoutineMetadataCheckpoint => {
                observability::BackgroundWorkClass::RoutineMetadataCheckpoint
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BackgroundWorkAdmissionLimits {
    known_damage_repair: usize,
    backfill_candidate_scan: usize,
    routine_backfill: usize,
    reclaim_cleanup: usize,
    lifecycle_cleanup: usize,
    stream_session_cleanup: usize,
    opportunistic_scan: usize,
    routine_metadata_checkpoint: usize,
}

impl Default for BackgroundWorkAdmissionLimits {
    fn default() -> Self {
        Self {
            known_damage_repair: BACKGROUND_KNOWN_DAMAGE_REPAIR_LIMIT,
            backfill_candidate_scan: BACKGROUND_BACKFILL_CANDIDATE_SCAN_LIMIT,
            routine_backfill: BACKGROUND_ROUTINE_BACKFILL_LIMIT,
            reclaim_cleanup: BACKGROUND_RECLAIM_CLEANUP_LIMIT,
            lifecycle_cleanup: BACKGROUND_LIFECYCLE_CLEANUP_LIMIT,
            stream_session_cleanup: BACKGROUND_STREAM_SESSION_CLEANUP_LIMIT,
            opportunistic_scan: BACKGROUND_OPPORTUNISTIC_SCAN_LIMIT,
            routine_metadata_checkpoint: BACKGROUND_ROUTINE_METADATA_CHECKPOINT_LIMIT,
        }
    }
}

#[derive(Debug)]
struct BackgroundWorkAdmission {
    limits: BackgroundWorkAdmissionLimits,
    pressure: Mutex<BackgroundWorkPressureState>,
    known_damage_repair_active: AtomicUsize,
    backfill_candidate_scan_active: AtomicUsize,
    routine_backfill_active: AtomicUsize,
    reclaim_cleanup_active: AtomicUsize,
    lifecycle_cleanup_active: AtomicUsize,
    stream_session_cleanup_active: AtomicUsize,
    opportunistic_scan_active: AtomicUsize,
    routine_metadata_checkpoint_active: AtomicUsize,
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
            backfill_candidate_scan_active: AtomicUsize::new(0),
            routine_backfill_active: AtomicUsize::new(0),
            reclaim_cleanup_active: AtomicUsize::new(0),
            lifecycle_cleanup_active: AtomicUsize::new(0),
            stream_session_cleanup_active: AtomicUsize::new(0),
            opportunistic_scan_active: AtomicUsize::new(0),
            routine_metadata_checkpoint_active: AtomicUsize::new(0),
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
                    self.emit(
                        class,
                        observability::BackgroundWorkAdmissionEvent::Admitted,
                        None,
                    );
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
        self.emit(
            class,
            observability::BackgroundWorkAdmissionEvent::DeniedLimit,
            None,
        );
        None
    }

    fn policy_denial_event(
        &self,
        class: BackgroundWorkClass,
    ) -> Option<observability::BackgroundWorkAdmissionEvent> {
        let pressure = self.observe_pressure();
        self.policy_denial_event_for_pressure(class, pressure)
    }

    fn policy_denial_event_for_pressure(
        &self,
        class: BackgroundWorkClass,
        pressure: BackgroundWorkPressure,
    ) -> Option<observability::BackgroundWorkAdmissionEvent> {
        match class {
            BackgroundWorkClass::KnownDamageRepair
            | BackgroundWorkClass::ReclaimCleanup
            | BackgroundWorkClass::LifecycleCleanup
            | BackgroundWorkClass::StreamSessionCleanup => None,
            BackgroundWorkClass::BackfillCandidateScan | BackgroundWorkClass::RoutineBackfill => {
                if pressure.foreground {
                    Some(observability::BackgroundWorkAdmissionEvent::DeniedForegroundPressure)
                } else if self.known_damage_repair_active.load(Ordering::Acquire) > 0 {
                    Some(observability::BackgroundWorkAdmissionEvent::DeniedKnownDamageActive)
                } else {
                    None
                }
            }
            BackgroundWorkClass::OpportunisticScan => {
                if pressure.foreground {
                    Some(observability::BackgroundWorkAdmissionEvent::DeniedForegroundPressure)
                } else if pressure.durable_backlog {
                    Some(observability::BackgroundWorkAdmissionEvent::DeniedBacklogPressure)
                } else {
                    None
                }
            }
            BackgroundWorkClass::RoutineMetadataCheckpoint => {
                if pressure.foreground {
                    Some(observability::BackgroundWorkAdmissionEvent::DeniedForegroundPressure)
                } else {
                    None
                }
            }
        }
    }

    fn observe_pressure(&self) -> BackgroundWorkPressure {
        let snapshot = observability::metrics_snapshot();
        lock_mutex_unpoisoned(&self.pressure).observe(Instant::now(), snapshot)
    }

    fn counter_for(&self, class: BackgroundWorkClass) -> &AtomicUsize {
        match class {
            BackgroundWorkClass::KnownDamageRepair => &self.known_damage_repair_active,
            BackgroundWorkClass::BackfillCandidateScan => &self.backfill_candidate_scan_active,
            BackgroundWorkClass::RoutineBackfill => &self.routine_backfill_active,
            BackgroundWorkClass::ReclaimCleanup => &self.reclaim_cleanup_active,
            BackgroundWorkClass::LifecycleCleanup => &self.lifecycle_cleanup_active,
            BackgroundWorkClass::StreamSessionCleanup => &self.stream_session_cleanup_active,
            BackgroundWorkClass::OpportunisticScan => &self.opportunistic_scan_active,
            BackgroundWorkClass::RoutineMetadataCheckpoint => {
                &self.routine_metadata_checkpoint_active
            }
        }
    }

    fn limit_for(&self, class: BackgroundWorkClass) -> usize {
        match class {
            BackgroundWorkClass::KnownDamageRepair => self.limits.known_damage_repair,
            BackgroundWorkClass::BackfillCandidateScan => self.limits.backfill_candidate_scan,
            BackgroundWorkClass::RoutineBackfill => self.limits.routine_backfill,
            BackgroundWorkClass::ReclaimCleanup => self.limits.reclaim_cleanup,
            BackgroundWorkClass::LifecycleCleanup => self.limits.lifecycle_cleanup,
            BackgroundWorkClass::StreamSessionCleanup => self.limits.stream_session_cleanup,
            BackgroundWorkClass::OpportunisticScan => self.limits.opportunistic_scan,
            BackgroundWorkClass::RoutineMetadataCheckpoint => {
                self.limits.routine_metadata_checkpoint
            }
        }
    }

    fn active_total(&self) -> usize {
        self.known_damage_repair_active.load(Ordering::Acquire)
            + self.backfill_candidate_scan_active.load(Ordering::Acquire)
            + self.routine_backfill_active.load(Ordering::Acquire)
            + self.reclaim_cleanup_active.load(Ordering::Acquire)
            + self.lifecycle_cleanup_active.load(Ordering::Acquire)
            + self.stream_session_cleanup_active.load(Ordering::Acquire)
            + self.opportunistic_scan_active.load(Ordering::Acquire)
            + self
                .routine_metadata_checkpoint_active
                .load(Ordering::Acquire)
    }

    fn emit(
        &self,
        class: BackgroundWorkClass,
        event: observability::BackgroundWorkAdmissionEvent,
        elapsed_us: Option<u64>,
    ) {
        let _ = observability::emit_background_work_admission_event(
            TRACE_TARGET,
            observability::BackgroundWorkAdmissionSummary {
                class: class.observability_class(),
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
        || snapshot.bucket_delete_begin_queue_depth > 0
        || snapshot.bucket_delete_finalize_queue_depth > 0
        || snapshot.bucket_delete_finalize_outstanding_depth > 0
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
        self.admission.emit(
            self.class,
            observability::BackgroundWorkAdmissionEvent::Finished,
            Some(elapsed_us),
        );
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

fn earliest_bucket_delete_begin_retry_sleep(
    deferred_work: &VecDeque<BucketDeleteBeginRoot>,
    retry_after_by_root: &HashMap<BucketDeleteBeginRoot, Instant>,
) -> Option<Duration> {
    let now = Instant::now();
    let mut earliest_retry: Option<Instant> = None;
    for root in deferred_work {
        let retry_after = retry_after_by_root.get(root)?;
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
            .min(BUCKET_DELETE_BEGIN_RETRY_COOLDOWN)
    })
}

fn defer_bucket_delete_begin(
    deferred_work: &mut VecDeque<BucketDeleteBeginRoot>,
    deferred_roots: &mut HashSet<BucketDeleteBeginRoot>,
    root: BucketDeleteBeginRoot,
) {
    if deferred_roots.insert(root.clone()) {
        deferred_work.push_back(root);
    }
}

fn earliest_bucket_delete_finalize_retry_sleep(
    deferred_work: &VecDeque<BucketDeleteFinalizeRoot>,
    retry_after_by_root: &HashMap<BucketDeleteFinalizeRoot, Instant>,
) -> Option<Duration> {
    let now = Instant::now();
    let mut earliest_retry: Option<Instant> = None;
    for root in deferred_work {
        let retry_after = retry_after_by_root.get(root)?;
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
            .min(BUCKET_DELETE_FINALIZE_RETRY_COOLDOWN)
    })
}

fn defer_bucket_delete_finalize(
    deferred_work: &mut VecDeque<BucketDeleteFinalizeRoot>,
    deferred_roots: &mut HashSet<BucketDeleteFinalizeRoot>,
    root: BucketDeleteFinalizeRoot,
) {
    if deferred_roots.insert(root.clone()) {
        deferred_work.push_back(root);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BucketDeleteFinalizeWorkerDisposition {
    Finish,
    RetryAfter(Duration),
}

fn bucket_delete_finalize_worker_disposition(
    result: &Result<storage::BucketDeleteFinalizeOutcome, ServerError>,
) -> BucketDeleteFinalizeWorkerDisposition {
    match result {
        Ok(outcome) if outcome.is_terminal() => BucketDeleteFinalizeWorkerDisposition::Finish,
        Ok(storage::BucketDeleteFinalizeOutcome::Pending)
        | Err(ServerError::OperationAborted | ServerError::SlowDown) => {
            BucketDeleteFinalizeWorkerDisposition::RetryAfter(BUCKET_DELETE_FINALIZE_RETRY_COOLDOWN)
        }
        Err(_) => BucketDeleteFinalizeWorkerDisposition::RetryAfter(
            BUCKET_DELETE_FINALIZE_ERROR_RETRY_COOLDOWN,
        ),
        Ok(_) => unreachable!("all nonterminal bucket finalizer outcomes must retry"),
    }
}

fn bucket_delete_begin_root_is_stale(
    storage_node: &StorageCluster,
    root: &BucketDeleteBeginRoot,
) -> bool {
    match storage_node.head_bucket_info(&root.bucket) {
        Ok(info) => {
            info.bucket_execution_generation != root.bucket_execution_generation
                || info.bucket_incarnation_generation != root.bucket_incarnation_generation
        }
        Err(storage::BucketSnapshotLoadError::Metadata(
            storage::MetadataError::BucketNotFound { .. },
        )) => true,
        Err(_) => false,
    }
}

fn shortest_retry_sleep(left: Option<Duration>, right: Option<Duration>) -> Option<Duration> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

struct DurableReclaimScanSchedule {
    next_scan_at: Instant,
    next_pg_id: Option<u32>,
    pass_in_progress: bool,
    retry_pass_required: bool,
}

impl DurableReclaimScanSchedule {
    fn immediate() -> Self {
        Self {
            next_scan_at: Instant::now(),
            next_pg_id: None,
            pass_in_progress: true,
            retry_pass_required: false,
        }
    }

    fn record_batch(
        &mut self,
        batch: storage::DurableReclaimScanBatch,
        scan_completed_at: Instant,
        clean_pass_delay: Duration,
    ) {
        self.next_pg_id = batch.next_pg_id;
        self.retry_pass_required |= batch.retry_pass_required;
        match batch.outcome {
            storage::DurableReclaimScanOutcome::RouteRefreshRequired => {
                self.next_scan_at = scan_completed_at + RECLAIM_DURABLE_SCAN_ROUTE_REFRESH_BACKOFF;
            }
            storage::DurableReclaimScanOutcome::Complete if batch.next_pg_id.is_some() => {
                self.next_scan_at = scan_completed_at;
            }
            storage::DurableReclaimScanOutcome::Complete => {
                self.pass_in_progress = false;
                self.next_scan_at = scan_completed_at
                    + if self.retry_pass_required {
                        RECLAIM_DURABLE_SCAN_INCOMPLETE_RETRY
                    } else {
                        clean_pass_delay
                    };
                self.retry_pass_required = false;
            }
        }
    }
}

fn enqueue_durable_reclaim_work_if_due(
    storage_node: &Arc<StorageCluster>,
    excluded_object_payload_roots: &HashSet<ObjectPayloadReclaimRoot>,
    excluded_bucket_delete_begin_roots: &HashSet<BucketDeleteBeginRoot>,
    excluded_bucket_delete_finalize_roots: &HashSet<BucketDeleteFinalizeRoot>,
    schedule: &mut DurableReclaimScanSchedule,
) {
    if Instant::now() < schedule.next_scan_at {
        return;
    }
    if !schedule.pass_in_progress {
        schedule.pass_in_progress = true;
        schedule.next_pg_id = None;
        schedule.retry_pass_required = false;
    }
    let excluded_bucket_names = excluded_bucket_delete_finalize_roots
        .iter()
        .map(|root| root.bucket.clone())
        .collect();
    let admission = background_work_admission_for(storage_node);
    let Some(_scan_permit) = admission.try_acquire(BackgroundWorkClass::ReclaimCleanup) else {
        schedule.next_scan_at = Instant::now() + OBJECT_PAYLOAD_RECLAIM_PG_RETRY_COOLDOWN;
        return;
    };
    let batch = storage_node.enqueue_durable_reclaim_work_batch_excluding(
        schedule.next_pg_id,
        RECLAIM_DURABLE_SCAN_BATCH_PGS,
        excluded_object_payload_roots,
        excluded_bucket_delete_begin_roots,
        &excluded_bucket_names,
    );
    let scan_completed_at = Instant::now();
    let clean_pass_delay = {
        #[cfg(test)]
        {
            reclaim_worker_durable_scan_delay_override(storage_node.process_local_registry_key())
                .unwrap_or(RECLAIM_DURABLE_SCAN_SAFETY_INTERVAL)
        }
        #[cfg(not(test))]
        {
            RECLAIM_DURABLE_SCAN_SAFETY_INTERVAL
        }
    };
    schedule.record_batch(batch, scan_completed_at, clean_pass_delay);
}

/// The coordinator ties together EC, storage, and metadata.
pub(super) struct ReclaimSweeper {
    pub(super) storage_handle: StorageClusterRuntimeMapHandle,
    pub(super) stop: Arc<AtomicBool>,
    pub(super) handle: Mutex<Option<JoinHandle<()>>>,
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
    pub(super) storage_handle: StorageClusterRuntimeMapHandle,
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
        self.storage_handle.current().wake_reclaim_workers();
        if let Some(handle) = lock_mutex_unpoisoned(&self.handle).take() {
            let _ = handle.join();
        }
    }
}

impl ReclaimSweeper {
    pub(super) fn acquire_shared(
        storage_handle: &StorageClusterRuntimeMapHandle,
        runtime: ReadRuntime,
    ) -> Result<Arc<Self>, ServerError> {
        let registry = RECLAIM_SWEEPER_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()));
        let mut registry: std::sync::MutexGuard<'_, HashMap<usize, Weak<ReclaimSweeper>>> =
            lock_mutex_unpoisoned(registry);
        registry.retain(|_, sweeper| sweeper.upgrade().is_some());

        let storage_cluster = storage_handle.current();
        let key = storage_cluster.process_local_registry_key();
        if let Some(existing) = registry.get(&key).and_then(Weak::upgrade) {
            return Ok(existing);
        }

        let sweeper = Self::spawn(storage_handle.clone(), runtime)?;
        registry.insert(key, Arc::downgrade(&sweeper));
        Ok(sweeper)
    }

    fn spawn(
        storage_handle: StorageClusterRuntimeMapHandle,
        runtime: ReadRuntime,
    ) -> Result<Arc<Self>, ServerError> {
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let storage_handle_for_drop = storage_handle.clone();
        let sweeper = Arc::new(Self {
            storage_handle: storage_handle_for_drop,
            stop,
            handle: Mutex::new(None),
        });
        let handle = std::thread::Builder::new()
            .name("argmin-reclaim".to_string())
            .spawn(move || {
                let mut object_payload_reclaim_pg_retry_after: HashMap<u32, Instant> =
                    HashMap::new();
                let mut deferred_object_payload_reclaim = VecDeque::new();
                let mut deferred_object_payload_reclaim_roots = HashSet::new();
                let mut bucket_delete_begin_retry_after: HashMap<BucketDeleteBeginRoot, Instant> =
                    HashMap::new();
                let mut deferred_bucket_delete_begin: VecDeque<BucketDeleteBeginRoot> =
                    VecDeque::new();
                let mut deferred_bucket_delete_begin_roots: HashSet<BucketDeleteBeginRoot> =
                    HashSet::new();
                let mut bucket_delete_finalize_retry_after: HashMap<
                    BucketDeleteFinalizeRoot,
                    Instant,
                > = HashMap::new();
                let mut deferred_bucket_delete_finalize: VecDeque<BucketDeleteFinalizeRoot> =
                    VecDeque::new();
                let mut deferred_bucket_delete_finalize_roots: HashSet<BucketDeleteFinalizeRoot> =
                    HashSet::new();
                let mut durable_scan_schedule = DurableReclaimScanSchedule::immediate();
                let mut pending_work: Option<(Arc<StorageCluster>, ReclaimWorkItem)> = None;
                while !worker_stop.load(Ordering::SeqCst) {
                    let current_worker_node = storage_handle.current();
                    enqueue_durable_reclaim_work_if_due(
                        &current_worker_node,
                        &deferred_object_payload_reclaim_roots,
                        &deferred_bucket_delete_begin_roots,
                        &deferred_bucket_delete_finalize_roots,
                        &mut durable_scan_schedule,
                    );
                    let Some((queue_owner, work)) = pending_work
                        .take()
                        .or_else(|| {
                            current_worker_node
                                .try_take_reclaim_work()
                                .map(|work| (Arc::clone(&current_worker_node), work))
                        })
                        .or_else(|| {
                            if deferred_object_payload_reclaim.is_empty()
                                && deferred_bucket_delete_begin.is_empty()
                                && deferred_bucket_delete_finalize.is_empty()
                            {
                                return None;
                            }
                            enqueue_durable_reclaim_work_if_due(
                                &current_worker_node,
                                &deferred_object_payload_reclaim_roots,
                                &deferred_bucket_delete_begin_roots,
                                &deferred_bucket_delete_finalize_roots,
                                &mut durable_scan_schedule,
                            );
                            current_worker_node
                                .try_take_reclaim_work()
                                .map(|work| (Arc::clone(&current_worker_node), work))
                                .or_else(|| {
                                    if let Some(root) = deferred_object_payload_reclaim.pop_front()
                                    {
                                        deferred_object_payload_reclaim_roots.remove(&root);
                                        return Some((
                                            Arc::clone(&current_worker_node),
                                            ReclaimWorkItem::ObjectPayload(root),
                                        ));
                                    }
                                    deferred_bucket_delete_begin
                                        .pop_front()
                                        .map(|root| {
                                            deferred_bucket_delete_begin_roots.remove(&root);
                                            (
                                                Arc::clone(&current_worker_node),
                                                ReclaimWorkItem::BucketDeleteBegin(root),
                                            )
                                        })
                                        .or_else(|| {
                                            deferred_bucket_delete_finalize.pop_front().map(
                                                |root| {
                                                    deferred_bucket_delete_finalize_roots
                                                        .remove(&root);
                                                    (
                                                        Arc::clone(&current_worker_node),
                                                        ReclaimWorkItem::BucketDelete(root),
                                                    )
                                                },
                                            )
                                        })
                                })
                        })
                        .or_else(|| {
                            let work =
                                wait_for_runtime_map_reclaim_work(&storage_handle, &worker_stop);
                            #[cfg(test)]
                            if work.is_none() {
                                maybe_run_reclaim_worker_idle_return_hook(
                                    current_worker_node.process_local_registry_key(),
                                );
                            }
                            work
                        })
                    else {
                        if worker_stop.load(Ordering::SeqCst) {
                            break;
                        }
                        continue;
                    };
                    #[cfg(test)]
                    maybe_run_after_reclaim_work_dequeued_hook(
                        queue_owner.process_local_registry_key(),
                    );
                    let execution_node = storage_handle.current();
                    #[cfg(test)]
                    maybe_run_before_reclaim_work_execute_hook(
                        queue_owner.process_local_registry_key(),
                        Arc::clone(&execution_node),
                    );
                    let current_runtime = runtime.with_storage_node(Arc::clone(&execution_node));
                    let admission = background_work_admission_for(&execution_node);
                    let Some(_cleanup_permit) =
                        admission.try_acquire(BackgroundWorkClass::ReclaimCleanup)
                    else {
                        pending_work = Some((queue_owner, work));
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
                            let pg_id = execution_node.object_payload_reclaim_pg_id(&bucket, &key);
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
                                let result = current_runtime
                                    .try_reclaim_object_payload_for_with_outcome(
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
                                    queue_owner.finish_object_payload_reclaim_work(
                                        &bucket,
                                        &key,
                                        generation_id,
                                    );
                                    object_payload_reclaim_pg_retry_after.remove(&pg_id);
                                }
                            }
                        }
                        ReclaimWorkItem::BucketDelete(root) => {
                            if deferred_bucket_delete_finalize_roots.contains(&root) {
                                continue;
                            }
                            let is_cooled = bucket_delete_finalize_retry_after
                                .get(&root)
                                .is_some_and(|retry_after| *retry_after > Instant::now());
                            if is_cooled {
                                defer_bucket_delete_finalize(
                                    &mut deferred_bucket_delete_finalize,
                                    &mut deferred_bucket_delete_finalize_roots,
                                    root,
                                );
                            } else {
                                let result = current_runtime
                                    .try_finalize_bucket_delete_for_with_outcome(&root);
                                let _ = observability::event(
                                    TRACE_TARGET,
                                    "bucket_delete_finalize_worker_result",
                                    Some(format_args!("root={root:?} result={result:?}")),
                                );
                                match bucket_delete_finalize_worker_disposition(&result) {
                                    BucketDeleteFinalizeWorkerDisposition::Finish => {
                                        queue_owner.finish_bucket_delete_finalize_work(&root);
                                        bucket_delete_finalize_retry_after.remove(&root);
                                        bucket_delete_begin_retry_after.retain(|begin, _| {
                                            begin.bucket != root.bucket
                                                || begin.bucket_incarnation_generation
                                                    != root.bucket_incarnation_generation
                                        });
                                        deferred_bucket_delete_begin_roots.retain(|begin| {
                                            begin.bucket != root.bucket
                                                || begin.bucket_incarnation_generation
                                                    != root.bucket_incarnation_generation
                                        });
                                        deferred_bucket_delete_begin.retain(|begin| {
                                            begin.bucket != root.bucket
                                                || begin.bucket_incarnation_generation
                                                    != root.bucket_incarnation_generation
                                        });
                                    }
                                    BucketDeleteFinalizeWorkerDisposition::RetryAfter(delay) => {
                                        bucket_delete_finalize_retry_after
                                            .insert(root.clone(), Instant::now() + delay);
                                        defer_bucket_delete_finalize(
                                            &mut deferred_bucket_delete_finalize,
                                            &mut deferred_bucket_delete_finalize_roots,
                                            root,
                                        );
                                    }
                                }
                            }
                        }
                        ReclaimWorkItem::BucketDeleteBegin(root) => {
                            if deferred_bucket_delete_begin_roots.contains(&root) {
                                continue;
                            }
                            let is_cooled = if let Some(retry_after) =
                                bucket_delete_begin_retry_after.get(&root)
                            {
                                *retry_after > Instant::now()
                            } else {
                                false
                            };
                            if is_cooled {
                                defer_bucket_delete_begin(
                                    &mut deferred_bucket_delete_begin,
                                    &mut deferred_bucket_delete_begin_roots,
                                    root,
                                );
                            } else {
                                match current_runtime.storage_node.begin_bucket_delete_if_current(
                                    &root.bucket,
                                    storage::cluster::BucketIdentityGenerations {
                                        bucket_execution_generation: root
                                            .bucket_execution_generation,
                                        bucket_incarnation_generation: root
                                            .bucket_incarnation_generation,
                                    },
                                ) {
                                    Ok(()) => {
                                        bucket_delete_begin_retry_after.remove(&root);
                                        queue_owner.enqueue_bucket_delete_finalize(
                                            BucketDeleteFinalizeRoot {
                                                bucket: root.bucket.clone(),
                                                bucket_incarnation_generation: root
                                                    .bucket_incarnation_generation,
                                            },
                                        );
                                    }
                                    Err(error) => {
                                        if bucket_delete_begin_root_is_stale(
                                            &current_runtime.storage_node,
                                            &root,
                                        ) {
                                            bucket_delete_begin_retry_after.remove(&root);
                                            continue;
                                        }
                                        let server_error =
                                            super::bucket::map_bucket_write_drain_error(error);
                                        if matches!(
                                            server_error,
                                            ServerError::OperationAborted | ServerError::SlowDown
                                        ) {
                                            bucket_delete_begin_retry_after.insert(
                                                root.clone(),
                                                Instant::now() + BUCKET_DELETE_BEGIN_RETRY_COOLDOWN,
                                            );
                                            defer_bucket_delete_begin(
                                                &mut deferred_bucket_delete_begin,
                                                &mut deferred_bucket_delete_begin_roots,
                                                root,
                                            );
                                        } else {
                                            bucket_delete_begin_retry_after.remove(&root);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if pending_work.is_none()
                        && (!deferred_object_payload_reclaim.is_empty()
                            || !deferred_bucket_delete_begin.is_empty()
                            || !deferred_bucket_delete_finalize.is_empty())
                    {
                        enqueue_durable_reclaim_work_if_due(
                            &execution_node,
                            &deferred_object_payload_reclaim_roots,
                            &deferred_bucket_delete_begin_roots,
                            &deferred_bucket_delete_finalize_roots,
                            &mut durable_scan_schedule,
                        );
                        if let Some(work) = execution_node.try_take_reclaim_work() {
                            pending_work = Some((Arc::clone(&execution_node), work));
                        } else if let Some(sleep_for) = shortest_retry_sleep(
                            shortest_retry_sleep(
                                earliest_object_payload_reclaim_retry_sleep(
                                    &execution_node,
                                    &deferred_object_payload_reclaim,
                                    &object_payload_reclaim_pg_retry_after,
                                ),
                                earliest_bucket_delete_begin_retry_sleep(
                                    &deferred_bucket_delete_begin,
                                    &bucket_delete_begin_retry_after,
                                ),
                            ),
                            earliest_bucket_delete_finalize_retry_sleep(
                                &deferred_bucket_delete_finalize,
                                &bucket_delete_finalize_retry_after,
                            ),
                        ) {
                            std::thread::sleep(sleep_for);
                        }
                    }
                }
            })
            .map_err(|e| ServerError::InternalError {
                reason: format!("failed to start reclaim worker: {e}"),
            })?;
        *lock_mutex_unpoisoned(&sweeper.handle) = Some(handle);
        Ok(sweeper)
    }

    pub(super) fn disabled(storage_cluster: Arc<StorageCluster>) -> Arc<Self> {
        Arc::new(Self {
            storage_handle: StorageClusterRuntimeMapHandle::new(storage_cluster),
            stop: Arc::new(AtomicBool::new(true)),
            handle: Mutex::new(None),
        })
    }
}

fn wait_for_runtime_map_reclaim_work(
    storage_handle: &StorageClusterRuntimeMapHandle,
    stop: &AtomicBool,
) -> Option<(Arc<StorageCluster>, ReclaimWorkItem)> {
    if stop.load(Ordering::SeqCst) {
        return None;
    }
    let worker_node = storage_handle.current();
    let work = worker_node.wait_for_queued_reclaim_work_poll(stop)?;
    Some((worker_node, work))
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
        self.storage_handle
            .current()
            .wake_placed_segment_shard_repair_workers();
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
        storage_handle: &StorageClusterRuntimeMapHandle,
        runtime: ReadRuntime,
    ) -> Result<Arc<Self>, ServerError> {
        let registry = LIFECYCLE_SWEEPER_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()));
        let mut registry: std::sync::MutexGuard<'_, HashMap<usize, Weak<LifecycleSweeper>>> =
            lock_mutex_unpoisoned(registry);
        registry.retain(|_, sweeper| sweeper.upgrade().is_some());

        let storage_cluster = storage_handle.current();
        let key = storage_cluster.process_local_registry_key();
        if let Some(existing) = registry.get(&key).and_then(Weak::upgrade) {
            return Ok(existing);
        }

        let admission = background_work_admission_for(&storage_cluster);
        let sweeper = Self::spawn(storage_handle.clone(), runtime, admission)?;
        registry.insert(key, Arc::downgrade(&sweeper));
        Ok(sweeper)
    }

    fn spawn(
        storage_handle: StorageClusterRuntimeMapHandle,
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
                        let current_runtime =
                            lifecycle_runtime_for_sweep(&storage_handle, &runtime);
                        let _ = current_runtime.run_lifecycle_sweep_at(Coordinator::now_millis());
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

pub(super) fn lifecycle_runtime_for_sweep(
    storage_handle: &StorageClusterRuntimeMapHandle,
    runtime: &ReadRuntime,
) -> ReadRuntime {
    runtime.with_storage_node(storage_handle.current())
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
                let sweep_interval = shard_scavenger_sweep_interval();
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
                        }
                        if let Some(_permit) =
                            admission.try_acquire(BackgroundWorkClass::BackfillCandidateScan)
                        {
                            match storage_cluster
                                .enqueue_placed_segment_shard_backfills_from_scavenger_references()
                            {
                                Ok(summary) => {
                                    let _ = observability::emit_shard_backfill_candidate_scan(
                                        TRACE_TARGET,
                                        observability::ShardBackfillCandidateScanSummary {
                                            scanned: summary.scanned,
                                            current_epoch: summary.current_epoch,
                                            already_queued: summary.already_queued,
                                            already_complete: summary.already_complete,
                                            enqueued: summary.enqueued,
                                            unrecoverable: summary.unrecoverable,
                                            deferred: summary.deferred,
                                            failed: summary.failed,
                                            limit_reached: summary.limit_reached,
                                        },
                                    );
                                }
                                Err(error) => {
                                    let _ = observability::emit_shard_backfill_candidate_scan_error(
                                        TRACE_TARGET,
                                        &error,
                                    );
                                }
                            }
                        }
                        if let Some(_permit) =
                            admission.try_acquire(BackgroundWorkClass::RoutineMetadataCheckpoint)
                        {
                            match storage_cluster.record_routine_metadata_command_checkpoints() {
                                Ok(summary) => {
                                    let _ = observability::emit_metadata_command_checkpoint_record_scan(
                                        TRACE_TARGET,
                                        observability::MetadataCommandCheckpointRecordSummary {
                                            scanned: summary.scanned,
                                            recorded: summary.recorded,
                                            already_current: summary.already_current,
                                            skipped_cadence: summary.skipped_cadence,
                                            skipped_inactive: summary.skipped_inactive,
                                            skipped_empty: summary.skipped_empty,
                                            skipped_stale_epoch: summary.skipped_stale_epoch,
                                            compacted: summary.compacted,
                                            compaction_deleted_entries: summary
                                                .compaction_deleted_entries,
                                            compaction_noop: summary.compaction_noop,
                                            compaction_no_checkpoint: summary
                                                .compaction_no_checkpoint,
                                            compaction_pending: summary.compaction_pending,
                                            compaction_failed: summary.compaction_failed,
                                            failed: summary.failed,
                                            limit_reached: summary.limit_reached,
                                        },
                                    );
                                }
                                Err(error) => {
                                    let _ =
                                        observability::emit_metadata_command_checkpoint_record_scan_error(
                                            TRACE_TARGET,
                                            &error,
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

fn shard_scavenger_sweep_interval() -> Duration {
    match std::env::var("ARGMIN_SHARD_SCAVENGER_SWEEP_INTERVAL_MS") {
        Ok(value) => match value.parse::<u64>() {
            Ok(0) | Err(_) => Duration::from_millis(super::SHARD_SCAVENGER_SWEEP_INTERVAL_MILLIS),
            Ok(ms) => Duration::from_millis(ms),
        },
        Err(_) => Duration::from_millis(super::SHARD_SCAVENGER_SWEEP_INTERVAL_MILLIS),
    }
}

impl ShardRepairSweeper {
    pub(super) fn acquire_shared(
        storage_handle: &StorageClusterRuntimeMapHandle,
    ) -> Result<Arc<Self>, ServerError> {
        let registry = SHARD_REPAIR_SWEEPER_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()));
        let mut registry: std::sync::MutexGuard<'_, HashMap<usize, Weak<ShardRepairSweeper>>> =
            lock_mutex_unpoisoned(registry);
        registry.retain(|_, sweeper| sweeper.upgrade().is_some());

        let storage_cluster = storage_handle.current();
        let key = storage_cluster.process_local_registry_key();
        if let Some(existing) = registry.get(&key).and_then(Weak::upgrade) {
            return Ok(existing);
        }

        let admission = background_work_admission_for(&storage_cluster);
        let sweeper = Self::spawn(storage_handle.clone(), key, admission)?;
        registry.insert(key, Arc::downgrade(&sweeper));
        Ok(sweeper)
    }

    fn spawn(
        storage_handle: StorageClusterRuntimeMapHandle,
        registry_key: usize,
        admission: Arc<BackgroundWorkAdmission>,
    ) -> Result<Arc<Self>, ServerError> {
        let stop = Arc::new(AtomicBool::new(false));
        let sweeper = Arc::new(Self {
            storage_handle: storage_handle.clone(),
            stop: Arc::clone(&stop),
            handle: Mutex::new(None),
        });
        let handle = std::thread::Builder::new()
            .name("argmin-shard-repair".to_string())
            .spawn(move || {
                let owner_token = format!("shard-repair-worker-{}", registry_key);
                let mut next_durable_scan_at = Instant::now();
                while !stop.load(Ordering::SeqCst) {
                    let storage_cluster = shard_repair_cluster_for_work(&storage_handle);
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
                                observability::record_shard_repair_error(
                                    None,
                                    "durable_scan_failed",
                                    error.diagnostic_kind(),
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
                        maybe_run_shard_repair_worker_idle_timeout_hook(registry_key);
                        continue;
                    };
                    if stop.load(Ordering::SeqCst) {
                        break;
                    }

                    let now_ms = Coordinator::now_millis();
                    let claim_id = format!(
                        "shard-repair-{}-{}-{}-{}",
                        registry_key,
                        work_item.request.data_pg_id,
                        work_item.shard_index.get(),
                        SHARD_REPAIR_CLAIM_COUNTER.fetch_add(1, Ordering::Relaxed)
                    );
                    let claim_acquire = PlacedSegmentShardRepairClaimAcquireParams {
                        claim_id,
                        owner_token: owner_token.clone(),
                        claimed_at: now_ms,
                        lease_deadline: now_ms.saturating_add(SHARD_REPAIR_CLAIM_LEASE_MILLIS),
                        now: now_ms,
                    };
                    let claim = match storage_cluster.acquire_placed_segment_shard_repair_claim(
                        work_item.request.data_pg_id,
                        &claim_acquire,
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
                            observability::record_shard_repair_error(
                                Some(work_item.request.data_pg_id),
                                "claim_failed",
                                error.diagnostic_kind(),
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
                            observability::record_shard_repair_error(
                                Some(claim.work_item.request.data_pg_id),
                                "record_error_failed",
                                record_error.diagnostic_kind(),
                            );
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
                                    observability::record_shard_repair_error(
                                        Some(claim.work_item.request.data_pg_id),
                                        "complete_failed",
                                        error.diagnostic_kind(),
                                    );
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
                            observability::record_shard_repair_error(
                                Some(claim.work_item.request.data_pg_id),
                                event,
                                error.diagnostic_kind(),
                            );
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
                                observability::record_shard_repair_error(
                                    Some(claim.work_item.request.data_pg_id),
                                    "record_error_failed",
                                    record_error.diagnostic_kind(),
                                );
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

    pub(super) fn disabled(storage_handle: StorageClusterRuntimeMapHandle) -> Arc<Self> {
        Arc::new(Self {
            storage_handle,
            stop: Arc::new(AtomicBool::new(true)),
            handle: Mutex::new(None),
        })
    }
}

pub(super) fn shard_repair_cluster_for_work(
    storage_handle: &StorageClusterRuntimeMapHandle,
) -> Arc<StorageCluster> {
    storage_handle.current()
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
                    run_one_placed_segment_shard_backfill(
                        &storage_cluster,
                        &owner_token,
                        &admission,
                    );

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

fn run_one_placed_segment_shard_backfill(
    storage_cluster: &StorageCluster,
    owner_token: &str,
    admission: &Arc<BackgroundWorkAdmission>,
) {
    let now_ms = Coordinator::now_millis();
    let claim_id = format!(
        "shard-backfill-{}-{}",
        storage_cluster.process_local_registry_key(),
        SHARD_BACKFILL_CLAIM_COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    let queue_depth = shard_backfill_queue_depth(storage_cluster);
    let claim_acquire = PlacedSegmentShardBackfillClaimAcquireParams {
        claim_id,
        owner_token: owner_token.to_string(),
        claimed_at: now_ms,
        lease_deadline: now_ms.saturating_add(SHARD_BACKFILL_CLAIM_LEASE_MILLIS),
        now: now_ms,
    };
    let claim =
        match storage_cluster.acquire_next_placed_segment_shard_backfill_claim(&claim_acquire) {
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
                observability::record_shard_backfill_error(
                    None,
                    "claim_failed",
                    error.diagnostic_kind(),
                );
                let _ = observability::event(
                    TRACE_TARGET,
                    "shard_backfill_claim_error",
                    Some(format_args!("error={error}")),
                );
                return;
            }
        };

    let admission_class =
        shard_backfill_admission_class(claim.remaining_tolerance, claim.work_item.request.ec.m);
    let Some(_permit) = admission.try_acquire(admission_class) else {
        emit_shard_backfill_event(
            Some(claim.work_item.request.data_pg_id),
            "admission_denied",
            shard_backfill_queue_depth(storage_cluster),
            None,
        );
        let next_attempt_after =
            Coordinator::now_millis().saturating_add(SHARD_BACKFILL_ERROR_BACKOFF_MILLIS);
        if let Err(record_error) = storage_cluster.record_placed_segment_shard_backfill_claim_error(
            &claim,
            "background shard backfill admission denied",
            next_attempt_after,
        ) {
            observability::record_shard_backfill_error(
                Some(claim.work_item.request.data_pg_id),
                "record_error_failed",
                record_error.diagnostic_kind(),
            );
            emit_shard_backfill_event(
                Some(claim.work_item.request.data_pg_id),
                "record_error_failed",
                shard_backfill_queue_depth(storage_cluster),
                None,
            );
            let _ = observability::event(
                TRACE_TARGET,
                "shard_backfill_record_error_failed",
                Some(format_args!("record_error={record_error}")),
            );
        }
        return;
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
                    observability::record_shard_backfill_error(
                        Some(claim.work_item.request.data_pg_id),
                        shard_backfill_completion_error_event(&error),
                        error.diagnostic_kind(),
                    );
                    emit_shard_backfill_event(
                        Some(claim.work_item.request.data_pg_id),
                        shard_backfill_completion_error_event(&error),
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
            let event = if shard_backfill_error_is_stale_retry(&error) {
                "stale_retry"
            } else {
                "failed"
            };
            observability::record_shard_backfill_error(
                Some(claim.work_item.request.data_pg_id),
                event,
                error.diagnostic_kind(),
            );
            emit_shard_backfill_event(Some(claim.work_item.request.data_pg_id), event, None, None);
            let _ = observability::event(
                TRACE_TARGET,
                "shard_backfill_error",
                Some(format_args!(
                    "pg_id={} source_epoch={} desired_epoch={} error={error}",
                    claim.work_item.request.data_pg_id,
                    claim.work_item.source_cluster_epoch,
                    claim.work_item.desired_cluster_epoch,
                )),
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
                observability::record_shard_backfill_error(
                    Some(claim.work_item.request.data_pg_id),
                    "record_error_failed",
                    record_error.diagnostic_kind(),
                );
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

#[cfg(test)]
pub(super) fn run_one_placed_segment_shard_backfill_for_test(
    storage_cluster: &StorageCluster,
    owner_token: &str,
) {
    let admission = Arc::new(BackgroundWorkAdmission::new());
    run_one_placed_segment_shard_backfill(storage_cluster, owner_token, &admission);
}

fn shard_backfill_admission_class(remaining_tolerance: u8, ec_m: u8) -> BackgroundWorkClass {
    if remaining_tolerance < ec_m {
        BackgroundWorkClass::KnownDamageRepair
    } else {
        BackgroundWorkClass::RoutineBackfill
    }
}

fn shard_backfill_error_is_stale_retry(error: &StoreError) -> bool {
    match error {
        StoreError::ShardStore { source, .. } => shard_backfill_error_is_stale_retry(source),
        StoreError::StalePayloadOperation { .. }
        | StoreError::StaleMetadataPrimaryBridge { .. }
        | StoreError::StaleMetadataOperation { .. }
        | StoreError::StaleMetadataRoute { .. }
        | StoreError::RouteMapExpired { .. }
        | StoreError::StaleShardOperation { .. }
        | StoreError::StaleShardLocation { .. }
        | StoreError::StorageRpcResourceExhausted { .. } => true,
        StoreError::StorageRpc { code, .. } => shard_backfill_remote_error_is_stale_retry(*code),
        _ => false,
    }
}

fn shard_backfill_completion_error_event(error: &StoreError) -> &'static str {
    if shard_backfill_error_is_stale_retry(error) {
        "complete_stale"
    } else {
        "complete_failed"
    }
}

fn shard_backfill_remote_error_is_stale_retry(code: storage::StorageRpcErrorCode) -> bool {
    matches!(
        code,
        storage::StorageRpcErrorCode::StaleShardLocation
            | storage::StorageRpcErrorCode::InactivePgRoute
            | storage::StorageRpcErrorCode::NonActingSetAccess
            | storage::StorageRpcErrorCode::TransportTimeout
            | storage::StorageRpcErrorCode::TransportClosed
            | storage::StorageRpcErrorCode::WrongClusterEpoch
    )
}

fn shard_backfill_queue_depth(storage_cluster: &StorageCluster) -> Option<usize> {
    match storage_cluster.placed_segment_shard_backfill_backlog_depth() {
        Ok(depth) => Some(depth),
        Err(error) => {
            observability::record_shard_backfill_error(
                None,
                "backlog_depth_failed",
                error.diagnostic_kind(),
            );
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
        storage_handle: &StorageClusterRuntimeMapHandle,
    ) -> Result<Arc<Self>, ServerError> {
        let registry = STREAM_SESSION_SWEEPER_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()));
        let mut registry: std::sync::MutexGuard<'_, HashMap<usize, Weak<StreamSessionSweeper>>> =
            lock_mutex_unpoisoned(registry);
        registry.retain(|_, sweeper| sweeper.upgrade().is_some());

        let key = storage_handle.current().process_local_registry_key();
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
            .name("argmin-stream-session-sweeper".to_string())
            .spawn(move || {
                while !stop.load(Ordering::SeqCst) {
                    let storage_cluster = storage_handle.current();
                    let admission = background_work_admission_for(&storage_cluster);
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
    fn with_storage_node(&self, storage_node: Arc<StorageCluster>) -> Self {
        Self {
            storage_node: Arc::clone(&storage_node),
            #[cfg(test)]
            pg_topology: PgTopology::new(storage_node.test_pg_ids())
                .expect("coordinator storage node should expose a valid PG topology"),
            payload_buffer_pool: Arc::clone(&self.payload_buffer_pool),
            sse_c_validator: self.sse_c_validator.clone(),
            managed_key_provider: self.managed_key_provider.clone(),
        }
    }

    fn map_bucket_snapshot_error(error: storage::BucketSnapshotLoadError) -> ServerError {
        match error {
            storage::BucketSnapshotLoadError::Store(
                storage::StoreError::MetadataCommandLogConflict { .. }
                | storage::StoreError::MetadataCommandLogGap { .. }
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

    pub(super) fn enqueue_bucket_delete_finalize_for(&self, root: BucketDeleteFinalizeRoot) {
        self.storage_node.enqueue_bucket_delete_finalize(root);
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

    #[cfg(test)]
    pub(super) fn try_finalize_bucket_delete_for(
        &self,
        bucket: &BucketName,
    ) -> Result<(), ServerError> {
        match self
            .storage_node
            .try_finalize_bucket_delete(bucket)
            .map_err(super::bucket::map_bucket_write_drain_error)?
        {
            storage::BucketDeleteFinalizeOutcome::NotFound
            | storage::BucketDeleteFinalizeOutcome::NotDeleting
            | storage::BucketDeleteFinalizeOutcome::StaleIncarnation
            | storage::BucketDeleteFinalizeOutcome::Pending
            | storage::BucketDeleteFinalizeOutcome::Finalized => Ok(()),
        }
    }

    pub(super) fn try_finalize_bucket_delete_for_with_outcome(
        &self,
        root: &BucketDeleteFinalizeRoot,
    ) -> Result<storage::BucketDeleteFinalizeOutcome, ServerError> {
        let result = self.storage_node.try_finalize_bucket_delete_root(root);
        if let Err(error) = &result {
            let _ = observability::event(
                TRACE_TARGET,
                "bucket_delete_finalize_storage_error",
                Some(format_args!("root={root:?} error={error:?}")),
            );
        }
        result.map_err(super::bucket::map_bucket_write_drain_error)
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
            .read_segment_payload_stored_bytes_at_placement_epoch_into(
                segment.placement_cluster_epoch,
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
    fn reclaim_durable_scan_schedule_preserves_cursor_and_uses_bounded_delays() {
        let completed_at = Instant::now();
        let mut schedule = DurableReclaimScanSchedule::immediate();
        schedule.record_batch(
            storage::DurableReclaimScanBatch {
                outcome: storage::DurableReclaimScanOutcome::Complete,
                next_pg_id: Some(8),
                scanned_pgs: 8,
                retry_pass_required: true,
            },
            completed_at,
            RECLAIM_DURABLE_SCAN_SAFETY_INTERVAL,
        );
        assert_eq!(schedule.next_pg_id, Some(8));
        assert_eq!(schedule.next_scan_at, completed_at);
        assert!(schedule.pass_in_progress);
        assert!(schedule.retry_pass_required);

        schedule.record_batch(
            storage::DurableReclaimScanBatch {
                outcome: storage::DurableReclaimScanOutcome::RouteRefreshRequired,
                next_pg_id: Some(8),
                scanned_pgs: 0,
                retry_pass_required: false,
            },
            completed_at,
            RECLAIM_DURABLE_SCAN_SAFETY_INTERVAL,
        );
        assert_eq!(schedule.next_pg_id, Some(8));
        assert_eq!(
            schedule.next_scan_at,
            completed_at + RECLAIM_DURABLE_SCAN_ROUTE_REFRESH_BACKOFF
        );
        assert!(schedule.pass_in_progress);
        assert!(schedule.retry_pass_required);

        schedule.record_batch(
            storage::DurableReclaimScanBatch {
                outcome: storage::DurableReclaimScanOutcome::Complete,
                next_pg_id: None,
                scanned_pgs: 4,
                retry_pass_required: false,
            },
            completed_at,
            RECLAIM_DURABLE_SCAN_SAFETY_INTERVAL,
        );
        assert_eq!(schedule.next_pg_id, None);
        assert_eq!(
            schedule.next_scan_at,
            completed_at + RECLAIM_DURABLE_SCAN_INCOMPLETE_RETRY
        );
        assert!(!schedule.pass_in_progress);
        assert!(!schedule.retry_pass_required);

        schedule.pass_in_progress = true;
        schedule.record_batch(
            storage::DurableReclaimScanBatch {
                outcome: storage::DurableReclaimScanOutcome::Complete,
                next_pg_id: None,
                scanned_pgs: 20,
                retry_pass_required: false,
            },
            completed_at,
            RECLAIM_DURABLE_SCAN_SAFETY_INTERVAL,
        );
        assert_eq!(
            schedule.next_scan_at,
            completed_at + RECLAIM_DURABLE_SCAN_SAFETY_INTERVAL
        );
    }

    #[test]
    fn bucket_delete_finalize_retry_sleep_waits_for_cooled_deferred_root() {
        let root = BucketDeleteFinalizeRoot {
            bucket: trusted_bucket_name("cooled-finalizer"),
            bucket_incarnation_generation: 1,
        };
        let mut deferred_work = VecDeque::new();
        deferred_work.push_back(root.clone());
        let mut retry_after_by_root = HashMap::new();
        retry_after_by_root.insert(
            root.clone(),
            Instant::now() + BUCKET_DELETE_FINALIZE_RETRY_COOLDOWN,
        );

        let sleep_for =
            earliest_bucket_delete_finalize_retry_sleep(&deferred_work, &retry_after_by_root)
                .expect("cooled finalizer root should produce a retry sleep");
        assert!(
            sleep_for <= BUCKET_DELETE_FINALIZE_RETRY_COOLDOWN,
            "retry sleep should be capped by the finalizer cooldown, got {sleep_for:?}"
        );

        retry_after_by_root.insert(root, Instant::now() - Duration::from_millis(1));
        assert_eq!(
            earliest_bucket_delete_finalize_retry_sleep(&deferred_work, &retry_after_by_root),
            None,
            "ready finalizer root should not sleep"
        );
    }

    #[test]
    fn bucket_delete_finalize_worker_retries_unexpected_errors() {
        let result = Err(ServerError::InternalError {
            reason: "post-delete claim release failed".to_string(),
        });

        assert_eq!(
            bucket_delete_finalize_worker_disposition(&result),
            BucketDeleteFinalizeWorkerDisposition::RetryAfter(
                BUCKET_DELETE_FINALIZE_ERROR_RETRY_COOLDOWN
            ),
            "an error after durable bucket deletion must remain retryable so NotFound can clear the exact outstanding root"
        );
    }

    #[test]
    fn background_work_admission_limits_and_releases_per_class() {
        let admission = Arc::new(BackgroundWorkAdmission::with_limits(
            BackgroundWorkAdmissionLimits {
                known_damage_repair: 1,
                backfill_candidate_scan: 1,
                routine_backfill: 1,
                reclaim_cleanup: 1,
                lifecycle_cleanup: 1,
                stream_session_cleanup: 1,
                opportunistic_scan: 0,
                routine_metadata_checkpoint: 1,
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
        let checkpoint_permit = admission
            .try_acquire(BackgroundWorkClass::RoutineMetadataCheckpoint)
            .expect("routine metadata checkpoint should have its own class limit");
        assert!(
            admission
                .try_acquire(BackgroundWorkClass::RoutineMetadataCheckpoint)
                .is_none(),
            "second routine metadata checkpoint should be denied at limit"
        );
        assert_eq!(admission.active_total(), 5);
        drop(checkpoint_permit);
        assert_eq!(admission.active_total(), 4);
        assert!(
            admission
                .try_acquire(BackgroundWorkClass::OpportunisticScan)
                .is_none(),
            "zero-limit scan class should deny"
        );

        drop(repair_permit);
        assert_eq!(admission.active_total(), 3);
        let backfill_candidate_scan_permit = admission
            .try_acquire(BackgroundWorkClass::BackfillCandidateScan)
            .expect("backfill candidate scan should have its own class limit");
        assert!(
            admission
                .try_acquire(BackgroundWorkClass::BackfillCandidateScan)
                .is_none(),
            "second backfill candidate scan permit should be denied at limit"
        );
        assert_eq!(admission.active_total(), 4);
        drop(backfill_candidate_scan_permit);
        let routine_backfill_permit = admission
            .try_acquire(BackgroundWorkClass::RoutineBackfill)
            .expect("routine backfill should have its own class limit");
        assert!(
            admission
                .try_acquire(BackgroundWorkClass::RoutineBackfill)
                .is_none(),
            "second routine backfill permit should be denied at limit"
        );
        assert_eq!(admission.active_total(), 4);
        let replacement = admission
            .try_acquire(BackgroundWorkClass::KnownDamageRepair)
            .expect("dropping a permit should release class capacity");
        drop(replacement);
        drop(reclaim_permit);
        drop(lifecycle_permit);
        drop(stream_session_permit);
        drop(routine_backfill_permit);
        assert_eq!(admission.active_total(), 0);
    }

    #[test]
    fn background_work_admission_denies_routine_backfill_behind_known_damage() {
        let admission = Arc::new(BackgroundWorkAdmission::with_limits(
            BackgroundWorkAdmissionLimits {
                known_damage_repair: 1,
                backfill_candidate_scan: 1,
                routine_backfill: 1,
                reclaim_cleanup: 0,
                lifecycle_cleanup: 0,
                stream_session_cleanup: 0,
                opportunistic_scan: 0,
                routine_metadata_checkpoint: 1,
            },
        ));

        let repair_permit = admission
            .try_acquire(BackgroundWorkClass::KnownDamageRepair)
            .expect("known damage should fit its class limit");
        assert!(
            admission
                .try_acquire(BackgroundWorkClass::RoutineBackfill)
                .is_none(),
            "routine backfill should wait while known-damage work is active"
        );
        assert!(
            admission
                .try_acquire(BackgroundWorkClass::BackfillCandidateScan)
                .is_none(),
            "backfill candidate scan should wait while known-damage work is active"
        );
        drop(repair_permit);
        let scan_permit = admission
            .try_acquire(BackgroundWorkClass::BackfillCandidateScan)
            .expect("backfill candidate scan should run once known-damage work is idle");
        drop(scan_permit);
        let routine_permit = admission
            .try_acquire(BackgroundWorkClass::RoutineBackfill)
            .expect("routine backfill should run once known-damage work is idle");
        drop(routine_permit);
    }

    #[test]
    fn background_work_admission_keeps_checkpoints_available_under_backlog() {
        let admission = Arc::new(BackgroundWorkAdmission::with_limits(
            BackgroundWorkAdmissionLimits {
                known_damage_repair: 1,
                backfill_candidate_scan: 1,
                routine_backfill: 1,
                reclaim_cleanup: 1,
                lifecycle_cleanup: 1,
                stream_session_cleanup: 1,
                opportunistic_scan: 1,
                routine_metadata_checkpoint: 1,
            },
        ));
        let backlog = BackgroundWorkPressure {
            foreground: false,
            durable_backlog: true,
        };

        assert_eq!(
            admission
                .policy_denial_event_for_pressure(BackgroundWorkClass::OpportunisticScan, backlog),
            Some(observability::BackgroundWorkAdmissionEvent::DeniedBacklogPressure),
            "opportunistic scans should still wait behind durable cleanup/backfill backlog"
        );
        assert_eq!(
            admission.policy_denial_event_for_pressure(
                BackgroundWorkClass::RoutineMetadataCheckpoint,
                backlog,
            ),
            None,
            "checkpoint cadence must continue under durable backlog to bound command-log growth"
        );
    }

    #[test]
    fn background_work_admission_keeps_checkpoints_foreground_sensitive() {
        let admission = Arc::new(BackgroundWorkAdmission::new());
        let foreground = BackgroundWorkPressure {
            foreground: true,
            durable_backlog: false,
        };

        assert_eq!(
            admission.policy_denial_event_for_pressure(
                BackgroundWorkClass::RoutineMetadataCheckpoint,
                foreground,
            ),
            Some(observability::BackgroundWorkAdmissionEvent::DeniedForegroundPressure)
        );
    }

    #[test]
    fn shard_backfill_admission_class_uses_ec_risk_tolerance() {
        assert_eq!(
            shard_backfill_admission_class(2, 2),
            BackgroundWorkClass::RoutineBackfill
        );
        assert_eq!(
            shard_backfill_admission_class(1, 2),
            BackgroundWorkClass::KnownDamageRepair
        );
        assert_eq!(
            shard_backfill_admission_class(0, 2),
            BackgroundWorkClass::KnownDamageRepair
        );
        assert_eq!(
            shard_backfill_admission_class(0, 0),
            BackgroundWorkClass::RoutineBackfill
        );
    }

    #[test]
    fn shard_backfill_error_classifies_stale_retries() {
        assert!(shard_backfill_error_is_stale_retry(
            &StoreError::StalePayloadOperation {
                pg_id: 7,
                operation_epoch: storage::ClusterEpoch::INITIAL,
                current_epoch: storage::ClusterEpoch::new(2).unwrap(),
            }
        ));
        assert!(shard_backfill_error_is_stale_retry(
            &StoreError::ShardStore {
                node_id: 2,
                pg_id: 7,
                cluster_epoch: storage::ClusterEpoch::INITIAL,
                source: Box::new(StoreError::StaleShardLocation {
                    node_id: 2,
                    pg_id: 7,
                    location_epoch: storage::ClusterEpoch::INITIAL,
                    current_epoch: storage::ClusterEpoch::new(2).unwrap(),
                }),
            }
        ));
        assert!(shard_backfill_error_is_stale_retry(
            &StoreError::StorageRpc {
                node_id: 2,
                operation: "shard repair write",
                code: storage::StorageRpcErrorCode::StaleShardLocation,
                message: "request route epoch 10 does not match storage-node epoch 11".to_string(),
            }
        ));
        assert!(!shard_backfill_error_is_stale_retry(&StoreError::NotFound));
    }

    #[test]
    fn shard_backfill_completion_error_classifies_stale_as_non_hard() {
        assert_eq!(
            shard_backfill_completion_error_event(&StoreError::StalePayloadOperation {
                pg_id: 7,
                operation_epoch: storage::ClusterEpoch::INITIAL,
                current_epoch: storage::ClusterEpoch::new(2).unwrap(),
            }),
            "complete_stale"
        );
        assert_eq!(
            shard_backfill_completion_error_event(&StoreError::NotFound),
            "complete_failed"
        );
        assert_eq!(
            shard_backfill_completion_error_event(&StoreError::StorageRpc {
                node_id: 2,
                operation: "complete placed segment shard backfill claim",
                code: storage::StorageRpcErrorCode::TransportTimeout,
                message: "storage RPC stream I/O error: timed out".to_string(),
            }),
            "complete_stale"
        );
        assert_eq!(
            shard_backfill_completion_error_event(&StoreError::StorageRpc {
                node_id: 2,
                operation: "complete placed segment shard backfill claim",
                code: storage::StorageRpcErrorCode::TransportClosed,
                message: "storage RPC stream I/O error: early eof".to_string(),
            }),
            "complete_stale"
        );
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
            placement_cluster_epoch: ClusterEpoch::INITIAL,
            ec_k: ec_shape.k,
            ec_m: ec_shape.m,
            encryption: ObjectEncryption::None,
        };

        let err = runtime
            .read_checked_segment_payload(&segment, None, None)
            .unwrap_err();

        assert!(matches!(err, ServerError::OperationAborted), "{err:?}");
    }
}
