// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::cluster::{
    DurablePlacedSegmentShardRepairEnqueueSummary, MetadataCommandCheckpointScanCursor,
    PlacedSegmentShardBackfillCandidateEnqueueSummary,
    PlacedSegmentShardBackfillCandidateScanCursor, StorageClusterRouteAdmissionDomain,
    StorageClusterRouteHandle, StreamSessionSweepSummary,
};
use crate::types::{
    PlacedSegmentShardBackfillClaimAcquireParams, PlacedSegmentShardBackfillClaimRecord,
    PlacedSegmentShardRepairClaimAcquireParams, PlacedSegmentShardRepairClaimRecord,
};
use crate::{PgId, StorageNodeFailureClass, StoreError};

mod reclaim;
pub use reclaim::StorageReclaimSweeper;
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub use reclaim::{
    install_reclaim_worker_test_hooks, StorageReclaimWorkerTestHookGuard,
    StorageReclaimWorkerTestHooks,
};

const TRACE_TARGET: &str = "storage";
#[cfg(test)]
const SHARD_SCAVENGER_SWEEP_INTERVAL: Duration = Duration::from_millis(50);
#[cfg(not(test))]
const SHARD_SCAVENGER_SWEEP_INTERVAL: Duration = Duration::from_secs(60);
const STREAM_SESSION_SWEEP_INTERVAL: Duration = Duration::from_secs(10);
const STREAM_SESSION_SCAVENGE_MAX_AGE_MILLIS: u64 = 60_000;
const SHARD_REPAIR_DURABLE_SCAN_INTERVAL: Duration = Duration::from_millis(250);
const SHARD_REPAIR_CLAIM_LEASE_MILLIS: u64 = 30_000;
const SHARD_REPAIR_ERROR_BACKOFF_MILLIS: u64 = 1_000;
const SHARD_BACKFILL_DURABLE_SCAN_INTERVAL: Duration = Duration::from_millis(250);
const SHARD_BACKFILL_CANDIDATE_SCAN_INTERVAL: Duration = Duration::from_millis(250);
const METADATA_COMMAND_CHECKPOINT_SCAN_INTERVAL: Duration = Duration::from_secs(1);
const METADATA_COMMAND_CHECKPOINT_PG_SCAN_LIMIT: usize = 8;
const SHARD_BACKFILL_CLAIM_LEASE_MILLIS: u64 = 30_000;
const SHARD_BACKFILL_ERROR_BACKOFF_MILLIS: u64 = 1_000;
const BACKGROUND_KNOWN_DAMAGE_REPAIR_LIMIT: usize = 1;
const BACKGROUND_BACKFILL_CANDIDATE_SCAN_LIMIT: usize = 1;
const BACKGROUND_ROUTINE_BACKFILL_LIMIT: usize = 1;
const BACKGROUND_RECLAIM_CLEANUP_LIMIT: usize = 1;
const BACKGROUND_LIFECYCLE_CLEANUP_LIMIT: usize = 1;
const BACKGROUND_OPPORTUNISTIC_SCAN_LIMIT: usize = 1;
const BACKGROUND_ROUTINE_METADATA_CHECKPOINT_LIMIT: usize = 1;
const BACKGROUND_FOREGROUND_PRESSURE_HOLD: Duration = Duration::from_millis(1_000);
const BACKGROUND_FOREGROUND_PRESSURE_SAMPLE_INTERVAL: Duration = Duration::from_millis(250);
const BACKGROUND_FOREGROUND_PRESSURE_MAX_SAMPLE_GAP: Duration = Duration::from_millis(1_250);

static MAINTENANCE_ADMISSION_REGISTRY: OnceLock<
    Mutex<Vec<StorageMaintenanceAdmissionRegistration>>,
> = OnceLock::new();
static SHARD_SCAVENGER_SWEEPER_REGISTRY: OnceLock<Mutex<Vec<Weak<StorageShardScavengerSweeper>>>> =
    OnceLock::new();
static SHARD_REPAIR_SWEEPER_REGISTRY: OnceLock<Mutex<Vec<Weak<StorageShardRepairSweeper>>>> =
    OnceLock::new();
static SHARD_REPAIR_CLAIM_COUNTER: AtomicU64 = AtomicU64::new(1);
static SHARD_BACKFILL_SWEEPER_REGISTRY: OnceLock<Mutex<Vec<Weak<StorageShardBackfillSweeper>>>> =
    OnceLock::new();
static SHARD_BACKFILL_CLAIM_COUNTER: AtomicU64 = AtomicU64::new(1);

static STREAM_SESSION_SWEEPER_REGISTRY: OnceLock<Mutex<Vec<Weak<StorageStreamSessionSweeper>>>> =
    OnceLock::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StorageMaintenanceClass {
    KnownDamageRepair,
    BackfillCandidateScan,
    RoutineBackfill,
    ReclaimCleanup,
    LifecycleCleanup,
    OpportunisticScan,
    RoutineMetadataCheckpoint,
}

impl StorageMaintenanceClass {
    fn observability_class(self) -> observability::BackgroundWorkClass {
        match self {
            Self::KnownDamageRepair => observability::BackgroundWorkClass::KnownDamageRepair,
            Self::BackfillCandidateScan => {
                observability::BackgroundWorkClass::BackfillCandidateScan
            }
            Self::RoutineBackfill => observability::BackgroundWorkClass::RoutineBackfill,
            Self::ReclaimCleanup => observability::BackgroundWorkClass::ReclaimCleanup,
            Self::LifecycleCleanup => observability::BackgroundWorkClass::LifecycleCleanup,
            Self::OpportunisticScan => observability::BackgroundWorkClass::OpportunisticScan,
            Self::RoutineMetadataCheckpoint => {
                observability::BackgroundWorkClass::RoutineMetadataCheckpoint
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StorageMaintenanceAdmissionLimits {
    known_damage_repair: usize,
    backfill_candidate_scan: usize,
    routine_backfill: usize,
    reclaim_cleanup: usize,
    lifecycle_cleanup: usize,
    opportunistic_scan: usize,
    routine_metadata_checkpoint: usize,
}

impl Default for StorageMaintenanceAdmissionLimits {
    fn default() -> Self {
        Self {
            known_damage_repair: BACKGROUND_KNOWN_DAMAGE_REPAIR_LIMIT,
            backfill_candidate_scan: BACKGROUND_BACKFILL_CANDIDATE_SCAN_LIMIT,
            routine_backfill: BACKGROUND_ROUTINE_BACKFILL_LIMIT,
            reclaim_cleanup: BACKGROUND_RECLAIM_CLEANUP_LIMIT,
            lifecycle_cleanup: BACKGROUND_LIFECYCLE_CLEANUP_LIMIT,
            opportunistic_scan: BACKGROUND_OPPORTUNISTIC_SCAN_LIMIT,
            routine_metadata_checkpoint: BACKGROUND_ROUTINE_METADATA_CHECKPOINT_LIMIT,
        }
    }
}

/// Opaque, storage-owned admission domain shared by physical maintenance workers.
///
/// Callers may request permits for the remaining higher-layer workers while
/// those workers are migrated, but neither the admission classes nor their
/// counters and pressure policy cross the storage boundary.
#[derive(Debug)]
pub struct StorageMaintenanceAdmission {
    limits: StorageMaintenanceAdmissionLimits,
    pressure: Mutex<StorageMaintenancePressureState>,
    known_damage_repair_active: AtomicUsize,
    backfill_candidate_scan_active: AtomicUsize,
    routine_backfill_active: AtomicUsize,
    reclaim_cleanup_active: AtomicUsize,
    lifecycle_cleanup_active: AtomicUsize,
    opportunistic_scan_active: AtomicUsize,
    routine_metadata_checkpoint_active: AtomicUsize,
}

struct StorageMaintenanceAdmissionRegistration {
    route_domain: StorageClusterRouteAdmissionDomain,
    admission: Weak<StorageMaintenanceAdmission>,
}

#[derive(Debug, Default)]
struct StorageMaintenancePressureState {
    last_snapshot: Option<observability::MetricsSnapshot>,
    last_snapshot_at: Option<Instant>,
    foreground_pressure_until: Option<Instant>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StorageMaintenancePressure {
    foreground: bool,
    durable_backlog: bool,
}

/// Opaque proof that one storage-maintenance operation was admitted.
pub struct StorageMaintenancePermit {
    admission: Arc<StorageMaintenanceAdmission>,
    class: StorageMaintenanceClass,
    started_at: Instant,
    active: bool,
}

/// Opaque failure to start a storage-owned maintenance worker.
pub struct StorageMaintenanceStartError {
    _diagnostic: Box<str>,
}

impl StorageMaintenanceStartError {
    fn worker_spawn(worker: &'static str, error: std::io::Error) -> Self {
        let diagnostic = format!("failed to start {worker}: {error}").into_boxed_str();
        let _ = observability::event(
            TRACE_TARGET,
            "storage_maintenance_worker_start_error",
            Some(format_args!("worker={worker} error={error}")),
        );
        Self {
            _diagnostic: diagnostic,
        }
    }
}

impl fmt::Debug for StorageMaintenanceStartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageMaintenanceStartError")
            .field("diagnostic", &"<redacted>")
            .finish()
    }
}

impl fmt::Display for StorageMaintenanceStartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("failed to start storage maintenance worker")
    }
}

impl std::error::Error for StorageMaintenanceStartError {}

impl StorageMaintenanceAdmission {
    /// Return the maintenance admission domain associated with this route
    /// publication domain, including across runtime-map generations.
    #[must_use]
    pub fn acquire_shared(storage_handle: &StorageClusterRouteHandle) -> Arc<Self> {
        let registry = MAINTENANCE_ADMISSION_REGISTRY.get_or_init(|| Mutex::new(Vec::new()));
        let mut registry = registry.lock().unwrap_or_else(|error| error.into_inner());
        registry.retain(|registration| registration.admission.upgrade().is_some());

        if let Some(existing) = registry.iter().find_map(|registration| {
            registration
                .route_domain
                .matches(storage_handle)
                .then(|| registration.admission.upgrade())
                .flatten()
        }) {
            return existing;
        }

        let admission = Arc::new(Self::new());
        registry.push(StorageMaintenanceAdmissionRegistration {
            route_domain: storage_handle.route_admission_domain(),
            admission: Arc::downgrade(&admission),
        });
        admission
    }

    fn new() -> Self {
        Self::with_limits(StorageMaintenanceAdmissionLimits::default())
    }

    fn with_limits(limits: StorageMaintenanceAdmissionLimits) -> Self {
        Self {
            limits,
            pressure: Mutex::new(StorageMaintenancePressureState::default()),
            known_damage_repair_active: AtomicUsize::new(0),
            backfill_candidate_scan_active: AtomicUsize::new(0),
            routine_backfill_active: AtomicUsize::new(0),
            reclaim_cleanup_active: AtomicUsize::new(0),
            lifecycle_cleanup_active: AtomicUsize::new(0),
            opportunistic_scan_active: AtomicUsize::new(0),
            routine_metadata_checkpoint_active: AtomicUsize::new(0),
        }
    }

    #[must_use]
    pub fn try_known_damage_repair(self: &Arc<Self>) -> Option<StorageMaintenancePermit> {
        self.try_acquire(StorageMaintenanceClass::KnownDamageRepair)
    }

    #[must_use]
    pub fn try_routine_backfill(self: &Arc<Self>) -> Option<StorageMaintenancePermit> {
        self.try_acquire(StorageMaintenanceClass::RoutineBackfill)
    }

    #[must_use]
    pub fn try_reclaim_cleanup(self: &Arc<Self>) -> Option<StorageMaintenancePermit> {
        self.try_acquire(StorageMaintenanceClass::ReclaimCleanup)
    }

    #[must_use]
    pub fn try_lifecycle_cleanup(self: &Arc<Self>) -> Option<StorageMaintenancePermit> {
        self.try_acquire(StorageMaintenanceClass::LifecycleCleanup)
    }

    fn try_backfill_candidate_scan(self: &Arc<Self>) -> Option<StorageMaintenancePermit> {
        self.try_acquire(StorageMaintenanceClass::BackfillCandidateScan)
    }

    fn try_opportunistic_scan(self: &Arc<Self>) -> Option<StorageMaintenancePermit> {
        self.try_acquire(StorageMaintenanceClass::OpportunisticScan)
    }

    fn try_routine_metadata_checkpoint(self: &Arc<Self>) -> Option<StorageMaintenancePermit> {
        self.try_acquire(StorageMaintenanceClass::RoutineMetadataCheckpoint)
    }

    /// Refresh process-wide pressure observations for workers whose main loop
    /// samples more frequently than it requests a permit.
    pub fn observe_pressure(&self) {
        let _ = self.current_pressure();
    }

    fn try_acquire(
        self: &Arc<Self>,
        class: StorageMaintenanceClass,
    ) -> Option<StorageMaintenancePermit> {
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
                    return Some(StorageMaintenancePermit {
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
        class: StorageMaintenanceClass,
    ) -> Option<observability::BackgroundWorkAdmissionEvent> {
        self.policy_denial_event_for_pressure(class, self.current_pressure())
    }

    fn policy_denial_event_for_pressure(
        &self,
        class: StorageMaintenanceClass,
        pressure: StorageMaintenancePressure,
    ) -> Option<observability::BackgroundWorkAdmissionEvent> {
        match class {
            StorageMaintenanceClass::KnownDamageRepair
            | StorageMaintenanceClass::ReclaimCleanup
            | StorageMaintenanceClass::LifecycleCleanup => None,
            StorageMaintenanceClass::BackfillCandidateScan
            | StorageMaintenanceClass::RoutineBackfill => {
                if pressure.foreground {
                    Some(observability::BackgroundWorkAdmissionEvent::DeniedForegroundPressure)
                } else if self.known_damage_repair_active.load(Ordering::Acquire) > 0 {
                    Some(observability::BackgroundWorkAdmissionEvent::DeniedKnownDamageActive)
                } else {
                    None
                }
            }
            StorageMaintenanceClass::OpportunisticScan => {
                if pressure.foreground {
                    Some(observability::BackgroundWorkAdmissionEvent::DeniedForegroundPressure)
                } else if pressure.durable_backlog {
                    Some(observability::BackgroundWorkAdmissionEvent::DeniedBacklogPressure)
                } else {
                    None
                }
            }
            StorageMaintenanceClass::RoutineMetadataCheckpoint => {
                if pressure.foreground {
                    Some(observability::BackgroundWorkAdmissionEvent::DeniedForegroundPressure)
                } else {
                    None
                }
            }
        }
    }

    fn current_pressure(&self) -> StorageMaintenancePressure {
        let snapshot = observability::metrics_snapshot();
        self.pressure
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .observe(Instant::now(), snapshot)
    }

    fn counter_for(&self, class: StorageMaintenanceClass) -> &AtomicUsize {
        match class {
            StorageMaintenanceClass::KnownDamageRepair => &self.known_damage_repair_active,
            StorageMaintenanceClass::BackfillCandidateScan => &self.backfill_candidate_scan_active,
            StorageMaintenanceClass::RoutineBackfill => &self.routine_backfill_active,
            StorageMaintenanceClass::ReclaimCleanup => &self.reclaim_cleanup_active,
            StorageMaintenanceClass::LifecycleCleanup => &self.lifecycle_cleanup_active,
            StorageMaintenanceClass::OpportunisticScan => &self.opportunistic_scan_active,
            StorageMaintenanceClass::RoutineMetadataCheckpoint => {
                &self.routine_metadata_checkpoint_active
            }
        }
    }

    fn limit_for(&self, class: StorageMaintenanceClass) -> usize {
        match class {
            StorageMaintenanceClass::KnownDamageRepair => self.limits.known_damage_repair,
            StorageMaintenanceClass::BackfillCandidateScan => self.limits.backfill_candidate_scan,
            StorageMaintenanceClass::RoutineBackfill => self.limits.routine_backfill,
            StorageMaintenanceClass::ReclaimCleanup => self.limits.reclaim_cleanup,
            StorageMaintenanceClass::LifecycleCleanup => self.limits.lifecycle_cleanup,
            StorageMaintenanceClass::OpportunisticScan => self.limits.opportunistic_scan,
            StorageMaintenanceClass::RoutineMetadataCheckpoint => {
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
            + self.opportunistic_scan_active.load(Ordering::Acquire)
            + self
                .routine_metadata_checkpoint_active
                .load(Ordering::Acquire)
    }

    fn emit(
        &self,
        class: StorageMaintenanceClass,
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

impl StorageMaintenancePressureState {
    fn observe(
        &mut self,
        now: Instant,
        snapshot: observability::MetricsSnapshot,
    ) -> StorageMaintenancePressure {
        if self.last_snapshot.zip(self.last_snapshot_at).is_some_and(
            |(last_snapshot, last_snapshot_at)| {
                now.checked_duration_since(last_snapshot_at)
                    .is_some_and(|elapsed| elapsed <= BACKGROUND_FOREGROUND_PRESSURE_MAX_SAMPLE_GAP)
                    && maintenance_foreground_pressure_delta(last_snapshot, snapshot)
            },
        ) {
            self.foreground_pressure_until = Some(now + BACKGROUND_FOREGROUND_PRESSURE_HOLD);
        }
        self.last_snapshot = Some(snapshot);
        self.last_snapshot_at = Some(now);

        StorageMaintenancePressure {
            foreground: self
                .foreground_pressure_until
                .is_some_and(|pressure_until| now < pressure_until)
                || maintenance_foreground_pressure_active(snapshot),
            durable_backlog: maintenance_durable_backlog_active(snapshot),
        }
    }
}

fn maintenance_foreground_pressure_delta(
    last: observability::MetricsSnapshot,
    current: observability::MetricsSnapshot,
) -> bool {
    current.request_admission_wait_total > last.request_admission_wait_total
        || current.request_admission_timeout_total > last.request_admission_timeout_total
}

fn maintenance_foreground_pressure_active(snapshot: observability::MetricsSnapshot) -> bool {
    let capacity = snapshot.request_admission_capacity;
    if capacity == 0 {
        return false;
    }
    let high_water = capacity.saturating_sub(capacity / 4).max(1);
    snapshot.inflight_requests >= high_water
}

fn maintenance_durable_backlog_active(snapshot: observability::MetricsSnapshot) -> bool {
    snapshot.reclaim_work_queue_depth > 0
        || snapshot.object_payload_reclaim_queue_depth > 0
        || snapshot.object_payload_reclaim_outstanding_depth > 0
        || snapshot.bucket_delete_begin_queue_depth > 0
        || snapshot.bucket_delete_finalize_queue_depth > 0
        || snapshot.bucket_delete_finalize_outstanding_depth > 0
        || snapshot.shard_repair_queue_depth > 0
        || snapshot.shard_backfill_queue_depth > 0
}

impl Drop for StorageMaintenancePermit {
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

/// Opaque storage-owned state for bounded backfill-candidate discovery.
///
/// The cursor is deliberately retained with the route handle so callers can
/// request a scan without learning or persisting placement-group scan state.
pub(crate) struct StorageBackfillCandidateScanner {
    storage_handle: StorageClusterRouteHandle,
    cursor: Mutex<PlacedSegmentShardBackfillCandidateScanCursor>,
}

impl StorageBackfillCandidateScanner {
    #[must_use]
    pub(crate) fn new(storage_handle: StorageClusterRouteHandle) -> Self {
        Self {
            storage_handle,
            cursor: Mutex::new(PlacedSegmentShardBackfillCandidateScanCursor::default()),
        }
    }

    /// Run one bounded candidate scan and emit storage-owned diagnostics.
    ///
    /// Candidate counts, cursor state, and implementation failures remain
    /// inside storage; the caller merely schedules the maintenance operation.
    pub(crate) fn scan(&self) {
        match self.scan_inner() {
            Ok(summary) => emit_scan_summary(summary),
            Err(error) => {
                let _ =
                    observability::emit_shard_backfill_candidate_scan_error(TRACE_TARGET, &error);
            }
        }
    }

    fn scan_inner(&self) -> Result<PlacedSegmentShardBackfillCandidateEnqueueSummary, StoreError> {
        let mut cursor = self
            .cursor
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.storage_handle
            .current()
            .enqueue_placed_segment_shard_backfills_from_scavenger_references(&mut cursor)
    }

    #[cfg(test)]
    pub(crate) fn scan_with_limit(
        &self,
        scan_limit: usize,
    ) -> Result<PlacedSegmentShardBackfillCandidateEnqueueSummary, StoreError> {
        let mut cursor = self
            .cursor
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.storage_handle
            .current()
            .enqueue_placed_segment_shard_backfills_from_scavenger_references_with_cursor_and_limit(
                &mut cursor,
                scan_limit,
                usize::MAX,
                usize::MAX,
                None,
            )
    }
}

fn emit_scan_summary(summary: PlacedSegmentShardBackfillCandidateEnqueueSummary) {
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

struct StorageMetadataCheckpointScanner {
    storage_handle: StorageClusterRouteHandle,
    cursor: Mutex<MetadataCommandCheckpointScanCursor>,
}

impl StorageMetadataCheckpointScanner {
    fn new(storage_handle: StorageClusterRouteHandle) -> Self {
        Self {
            storage_handle,
            cursor: Mutex::new(MetadataCommandCheckpointScanCursor::default()),
        }
    }

    fn scan(&self) -> Result<crate::cluster::MetadataCommandCheckpointRecordSummary, StoreError> {
        let mut cursor = self
            .cursor
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.storage_handle
            .current()
            .record_routine_metadata_command_checkpoints_with_limit(
                &mut cursor,
                crate::cluster::METADATA_COMMAND_CHECKPOINT_RECORD_LIMIT,
                METADATA_COMMAND_CHECKPOINT_PG_SCAN_LIMIT,
            )
    }
}

/// Opaque storage-owned durable shard-backfill execution worker.
pub struct StorageShardBackfillSweeper {
    storage_handle: StorageClusterRouteHandle,
    stop: Arc<AtomicBool>,
    wake: Arc<(Mutex<bool>, Condvar)>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl StorageShardBackfillSweeper {
    pub fn acquire_shared(
        storage_handle: &StorageClusterRouteHandle,
    ) -> Result<Arc<Self>, StorageMaintenanceStartError> {
        let registry = SHARD_BACKFILL_SWEEPER_REGISTRY.get_or_init(|| Mutex::new(Vec::new()));
        let mut registry = registry.lock().unwrap_or_else(|error| error.into_inner());
        registry.retain(|sweeper| sweeper.upgrade().is_some());

        if let Some(existing) = registry.iter().filter_map(Weak::upgrade).find(|sweeper| {
            sweeper
                .storage_handle
                .shares_route_admission_with(storage_handle)
        }) {
            return Ok(existing);
        }

        let sweeper = Self::spawn(storage_handle.clone())?;
        registry.push(Arc::downgrade(&sweeper));
        Ok(sweeper)
    }

    fn spawn(
        storage_handle: StorageClusterRouteHandle,
    ) -> Result<Arc<Self>, StorageMaintenanceStartError> {
        let worker_identity = random_storage_worker_identity().map_err(|error| {
            StorageMaintenanceStartError::worker_spawn("shard backfill identity", error)
        })?;
        let owner_token = format!("shard-backfill-worker-{worker_identity}");
        let admission = StorageMaintenanceAdmission::acquire_shared(&storage_handle);
        let stop = Arc::new(AtomicBool::new(false));
        let wake = Arc::new((Mutex::new(false), Condvar::new()));
        let sweeper = Arc::new(Self {
            storage_handle: storage_handle.clone(),
            stop: Arc::clone(&stop),
            wake: Arc::clone(&wake),
            handle: Mutex::new(None),
        });
        let handle = std::thread::Builder::new()
            .name("argmin-shard-backfill".to_string())
            .spawn(move || {
                let mut last_claimed_pg_id = None;
                while !stop.load(Ordering::SeqCst) {
                    admission.observe_pressure();
                    run_one_placed_segment_shard_backfill(
                        &storage_handle.current(),
                        &worker_identity,
                        &owner_token,
                        &admission,
                        &mut last_claimed_pg_id,
                    );

                    let stop_guard = wake.0.lock().unwrap_or_else(|error| error.into_inner());
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
                        .unwrap_or_else(|error| error.into_inner());
                }
            })
            .map_err(|error| StorageMaintenanceStartError::worker_spawn("shard backfill", error))?;
        *sweeper
            .handle
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(handle);
        Ok(sweeper)
    }

    #[must_use]
    pub fn disabled(storage_handle: StorageClusterRouteHandle) -> Arc<Self> {
        Arc::new(Self {
            storage_handle,
            stop: Arc::new(AtomicBool::new(true)),
            wake: Arc::new((Mutex::new(true), Condvar::new())),
            handle: Mutex::new(None),
        })
    }

    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    #[must_use]
    pub(crate) fn test_is_enabled(&self) -> bool {
        !self.stop.load(Ordering::SeqCst)
    }

    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub(crate) fn test_routes_to(&self, expected: &Arc<crate::StorageCluster>) -> bool {
        Arc::ptr_eq(&self.storage_handle.current(), expected)
    }

    #[cfg(test)]
    #[doc(hidden)]
    pub(crate) fn test_backfill_one_pending(&self, owner_token: &str) {
        let admission = StorageMaintenanceAdmission::acquire_shared(&self.storage_handle);
        run_one_placed_segment_shard_backfill(
            &self.storage_handle.current(),
            "deterministic-test-worker",
            owner_token,
            &admission,
            &mut None,
        );
    }
}

impl Drop for StorageShardBackfillSweeper {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        *self
            .wake
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = true;
        self.wake.1.notify_all();
        if let Some(handle) = self
            .handle
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            let _ = handle.join();
        }
    }
}

fn run_one_placed_segment_shard_backfill(
    storage_cluster: &crate::StorageCluster,
    worker_identity: &str,
    owner_token: &str,
    admission: &Arc<StorageMaintenanceAdmission>,
    last_claimed_pg_id: &mut Option<PgId>,
) {
    let now_ms = crate::clock::current_time_millis();
    let claim_id = format!(
        "shard-backfill-{}-{}",
        worker_identity,
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
    let claim = match storage_cluster.acquire_next_placed_segment_shard_backfill_claim_with_cursor(
        &claim_acquire,
        last_claimed_pg_id,
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
            record_shard_backfill_failure(
                None,
                "claim_failed",
                "shard_backfill_claim_error",
                &error,
                queue_depth,
            );
            return;
        }
    };

    let permit = match shard_backfill_admission_class(
        claim.remaining_tolerance,
        claim.work_item.request.ec.m,
    ) {
        ShardBackfillAdmission::KnownDamageRepair => admission.try_known_damage_repair(),
        ShardBackfillAdmission::RoutineBackfill => admission.try_routine_backfill(),
    };
    let Some(_permit) = permit else {
        emit_shard_backfill_event(
            Some(claim.work_item.request.data_pg_id),
            "admission_denied",
            shard_backfill_queue_depth(storage_cluster),
            None,
        );
        record_shard_backfill_claim_error(
            storage_cluster,
            &claim,
            "background shard backfill admission denied",
            crate::clock::current_time_millis().saturating_add(SHARD_BACKFILL_ERROR_BACKOFF_MILLIS),
            None,
        );
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
            emit_shard_backfill_event(
                Some(claim.work_item.request.data_pg_id),
                if backfilled_acks.is_empty() {
                    "resolved_clean"
                } else {
                    "backfilled"
                },
                None,
                Some(backfilled_acks.len()),
            );
            match storage_cluster.complete_placed_segment_shard_backfill_claim(&claim) {
                Ok(true) => emit_shard_backfill_event(
                    Some(claim.work_item.request.data_pg_id),
                    "complete_succeeded",
                    shard_backfill_queue_depth(storage_cluster),
                    None,
                ),
                Ok(false) => emit_shard_backfill_event(
                    Some(claim.work_item.request.data_pg_id),
                    "complete_stale",
                    shard_backfill_queue_depth(storage_cluster),
                    None,
                ),
                Err(error) => record_shard_backfill_failure(
                    Some(claim.work_item.request.data_pg_id),
                    shard_backfill_completion_error_event(&error),
                    "shard_backfill_complete_error",
                    &error,
                    shard_backfill_queue_depth(storage_cluster),
                ),
            }
        }
        Err(error) => {
            if shard_backfill_error_may_mean_source_is_obsolete(&error) {
                match storage_cluster
                    .placed_segment_shard_backfill_source_is_referenced(&claim.work_item)
                {
                    Ok(false) => {
                        complete_obsolete_placed_segment_shard_backfill(storage_cluster, &claim);
                        return;
                    }
                    Ok(true) => {}
                    Err(reference_error) => {
                        let _ = observability::event(
                            TRACE_TARGET,
                            "shard_backfill_source_reference_check_error",
                            Some(format_args!("error={reference_error}")),
                        );
                    }
                }
            }
            let event = if shard_backfill_error_is_stale_retry(&error) {
                "stale_retry"
            } else {
                "failed"
            };
            let pg_id = claim.work_item.request.data_pg_id;
            observability::record_shard_backfill_error(Some(pg_id), event, error.diagnostic_kind());
            emit_shard_backfill_event(Some(pg_id), event, None, None);
            let _ = observability::event(
                TRACE_TARGET,
                "shard_backfill_error",
                Some(format_args!(
                    "pg_id={pg_id} source_epoch={} desired_epoch={} error={error}",
                    claim.work_item.source_cluster_epoch, claim.work_item.desired_cluster_epoch,
                )),
            );
            record_shard_backfill_claim_error(
                storage_cluster,
                &claim,
                &error.to_string(),
                crate::clock::current_time_millis()
                    .saturating_add(SHARD_BACKFILL_ERROR_BACKOFF_MILLIS),
                Some(&error),
            );
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ShardBackfillAdmission {
    KnownDamageRepair,
    RoutineBackfill,
}

fn shard_backfill_admission_class(remaining_tolerance: u8, ec_m: u8) -> ShardBackfillAdmission {
    if remaining_tolerance < ec_m {
        ShardBackfillAdmission::KnownDamageRepair
    } else {
        ShardBackfillAdmission::RoutineBackfill
    }
}

fn shard_backfill_error_may_mean_source_is_obsolete(error: &StoreError) -> bool {
    if error.is_payload_not_found() {
        return true;
    }
    match error {
        StoreError::ShardStore { source, .. } => {
            shard_backfill_error_may_mean_source_is_obsolete(source)
        }
        StoreError::HistoricalPgRouteNotRetained { .. }
        | StoreError::PlacedSegmentBackfillSourceUnavailable => true,
        _ => false,
    }
}

fn shard_backfill_error_is_stale_retry(error: &StoreError) -> bool {
    if error
        .storage_node_failure_class()
        .is_some_and(storage_node_failure_is_shard_backfill_stale_retry)
    {
        return true;
    }
    match error {
        StoreError::ShardStore { source, .. } => shard_backfill_error_is_stale_retry(source),
        StoreError::StalePayloadOperation { .. }
        | StoreError::StaleMetadataPrimaryBridge { .. }
        | StoreError::StaleMetadataOperation { .. }
        | StoreError::StaleMetadataRoute { .. }
        | StoreError::StaleMetadataReadProof { .. }
        | StoreError::RouteMapExpired { .. }
        | StoreError::StaleShardOperation { .. }
        | StoreError::StaleShardLocation { .. }
        | StoreError::StorageRpcResourceExhausted { .. } => true,
        _ => false,
    }
}

fn storage_node_failure_is_shard_backfill_stale_retry(failure: StorageNodeFailureClass) -> bool {
    match failure {
        StorageNodeFailureClass::ShardLocationStale
        | StorageNodeFailureClass::PgRouteUnavailable
        | StorageNodeFailureClass::TransportInterrupted => true,
        StorageNodeFailureClass::MetadataCommandContention
        | StorageNodeFailureClass::MetadataTransferHistoricalRouteActive => false,
    }
}

fn shard_backfill_completion_error_event(error: &StoreError) -> &'static str {
    if shard_backfill_error_is_stale_retry(error) {
        "complete_stale"
    } else {
        "complete_failed"
    }
}

fn shard_backfill_record_error_event(error: &StoreError) -> &'static str {
    if shard_backfill_error_is_stale_retry(error) {
        "record_retry"
    } else {
        "record_error_failed"
    }
}

fn complete_obsolete_placed_segment_shard_backfill(
    storage_cluster: &crate::StorageCluster,
    claim: &PlacedSegmentShardBackfillClaimRecord,
) {
    match storage_cluster.complete_placed_segment_shard_backfill_claim(claim) {
        Ok(true) => emit_shard_backfill_event(
            Some(claim.work_item.request.data_pg_id),
            "obsolete_source_complete_succeeded",
            shard_backfill_queue_depth(storage_cluster),
            None,
        ),
        Ok(false) => emit_shard_backfill_event(
            Some(claim.work_item.request.data_pg_id),
            "obsolete_source_complete_stale",
            shard_backfill_queue_depth(storage_cluster),
            None,
        ),
        Err(error) => record_shard_backfill_failure(
            Some(claim.work_item.request.data_pg_id),
            "obsolete_source_complete_failed",
            "shard_backfill_obsolete_source_complete_error",
            &error,
            shard_backfill_queue_depth(storage_cluster),
        ),
    }
}

fn record_shard_backfill_claim_error(
    storage_cluster: &crate::StorageCluster,
    claim: &PlacedSegmentShardBackfillClaimRecord,
    last_error: &str,
    next_attempt_after: u64,
    backfill_error: Option<&StoreError>,
) {
    if let Err(record_error) = storage_cluster.record_placed_segment_shard_backfill_claim_error(
        claim,
        last_error,
        next_attempt_after,
    ) {
        let event = shard_backfill_record_error_event(&record_error);
        record_shard_backfill_failure(
            Some(claim.work_item.request.data_pg_id),
            event,
            "shard_backfill_record_error",
            &record_error,
            shard_backfill_queue_depth(storage_cluster),
        );
        let _ = observability::event(
            TRACE_TARGET,
            "shard_backfill_record_error_context",
            Some(format_args!(
                "backfill_error={}",
                backfill_error.map_or("<admission denied>".to_string(), ToString::to_string)
            )),
        );
    }
}

fn record_shard_backfill_failure(
    pg_id: Option<u32>,
    event: &'static str,
    trace_event: &'static str,
    error: &StoreError,
    queue_depth: Option<usize>,
) {
    observability::record_shard_backfill_error(pg_id, event, error.diagnostic_kind());
    emit_shard_backfill_event(pg_id, event, queue_depth, None);
    let _ = observability::event(
        TRACE_TARGET,
        trace_event,
        Some(format_args!("error={error}")),
    );
}

fn shard_backfill_queue_depth(storage_cluster: &crate::StorageCluster) -> Option<usize> {
    match storage_cluster.placed_segment_shard_backfill_backlog_depth() {
        Ok(depth) => Some(depth),
        Err(error) => {
            record_shard_backfill_failure(
                None,
                "backlog_depth_failed",
                "shard_backfill_backlog_depth_error",
                &error,
                None,
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

/// Opaque storage-owned shard-audit, candidate-discovery, and checkpoint worker.
pub struct StorageShardScavengerSweeper {
    storage_handle: StorageClusterRouteHandle,
    stop: Arc<AtomicBool>,
    wake: Arc<(Mutex<bool>, Condvar)>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl StorageShardScavengerSweeper {
    pub fn acquire_shared(
        storage_handle: &StorageClusterRouteHandle,
    ) -> Result<Arc<Self>, StorageMaintenanceStartError> {
        let registry = SHARD_SCAVENGER_SWEEPER_REGISTRY.get_or_init(|| Mutex::new(Vec::new()));
        let mut registry = registry.lock().unwrap_or_else(|error| error.into_inner());
        registry.retain(|sweeper| sweeper.upgrade().is_some());

        if let Some(existing) = registry.iter().filter_map(Weak::upgrade).find(|sweeper| {
            sweeper
                .storage_handle
                .shares_route_admission_with(storage_handle)
        }) {
            return Ok(existing);
        }

        let sweeper = Self::spawn(storage_handle.clone())?;
        registry.push(Arc::downgrade(&sweeper));
        Ok(sweeper)
    }

    fn spawn(
        storage_handle: StorageClusterRouteHandle,
    ) -> Result<Arc<Self>, StorageMaintenanceStartError> {
        let admission = StorageMaintenanceAdmission::acquire_shared(&storage_handle);
        let stop = Arc::new(AtomicBool::new(false));
        let wake = Arc::new((Mutex::new(false), Condvar::new()));
        let sweeper = Arc::new(Self {
            storage_handle: storage_handle.clone(),
            stop: Arc::clone(&stop),
            wake: Arc::clone(&wake),
            handle: Mutex::new(None),
        });
        let handle = std::thread::Builder::new()
            .name("argmin-shard-scavenger".to_string())
            .spawn(move || {
                let audit_interval = shard_scavenger_sweep_interval();
                let candidate_interval = shard_backfill_candidate_scan_interval();
                let checkpoint_interval = metadata_command_checkpoint_scan_interval();
                let mut next_audit = Instant::now() + audit_interval;
                let mut next_candidate_scan = Instant::now();
                let mut next_checkpoint_scan = Instant::now();
                let candidate_scanner =
                    StorageBackfillCandidateScanner::new(storage_handle.clone());
                let checkpoint_scanner =
                    StorageMetadataCheckpointScanner::new(storage_handle.clone());
                while !stop.load(Ordering::SeqCst) {
                    admission.observe_pressure();
                    let now = Instant::now();
                    if now >= next_audit {
                        run_shard_scavenger_audit(&storage_handle, &admission);
                        next_audit = Instant::now() + audit_interval;
                    }
                    if now >= next_candidate_scan {
                        run_backfill_candidate_scan(&admission, &candidate_scanner);
                        next_candidate_scan = Instant::now() + candidate_interval;
                    }
                    if now >= next_checkpoint_scan {
                        run_metadata_checkpoint_scan(&admission, &checkpoint_scanner);
                        next_checkpoint_scan = Instant::now() + checkpoint_interval;
                    }
                    if stop.load(Ordering::SeqCst) {
                        break;
                    }
                    let stop_guard = wake.0.lock().unwrap_or_else(|error| error.into_inner());
                    if *stop_guard {
                        break;
                    }
                    let next_work = next_audit
                        .min(next_candidate_scan)
                        .min(next_checkpoint_scan);
                    let wait_for = next_work
                        .checked_duration_since(Instant::now())
                        .unwrap_or_default()
                        .min(BACKGROUND_FOREGROUND_PRESSURE_SAMPLE_INTERVAL);
                    let _ = wake
                        .1
                        .wait_timeout_while(stop_guard, wait_for, |stop_requested| !*stop_requested)
                        .unwrap_or_else(|error| error.into_inner());
                }
            })
            .map_err(|error| {
                StorageMaintenanceStartError::worker_spawn("shard scavenger", error)
            })?;
        *sweeper
            .handle
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(handle);
        Ok(sweeper)
    }

    #[must_use]
    pub fn disabled(storage_handle: StorageClusterRouteHandle) -> Arc<Self> {
        Arc::new(Self {
            storage_handle,
            stop: Arc::new(AtomicBool::new(true)),
            wake: Arc::new((Mutex::new(true), Condvar::new())),
            handle: Mutex::new(None),
        })
    }

    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    #[must_use]
    pub(crate) fn test_is_enabled(&self) -> bool {
        !self.stop.load(Ordering::SeqCst)
    }

    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub(crate) fn test_routes_to(&self, expected: &Arc<crate::StorageCluster>) -> bool {
        Arc::ptr_eq(&self.storage_handle.current(), expected)
    }
}

impl Drop for StorageShardScavengerSweeper {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        *self
            .wake
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = true;
        self.wake.1.notify_all();
        if let Some(handle) = self
            .handle
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            let _ = handle.join();
        }
    }
}

fn shard_scavenger_sweep_interval() -> Duration {
    maintenance_interval_from_env(
        "ARGMIN_SHARD_SCAVENGER_SWEEP_INTERVAL_MS",
        SHARD_SCAVENGER_SWEEP_INTERVAL,
    )
}

fn shard_backfill_candidate_scan_interval() -> Duration {
    maintenance_interval_from_env(
        "ARGMIN_SHARD_BACKFILL_CANDIDATE_SCAN_INTERVAL_MS",
        SHARD_BACKFILL_CANDIDATE_SCAN_INTERVAL,
    )
}

fn metadata_command_checkpoint_scan_interval() -> Duration {
    maintenance_interval_from_env(
        "ARGMIN_METADATA_COMMAND_CHECKPOINT_SCAN_INTERVAL_MS",
        METADATA_COMMAND_CHECKPOINT_SCAN_INTERVAL,
    )
}

fn maintenance_interval_from_env(name: &str, default: Duration) -> Duration {
    match std::env::var(name) {
        Ok(value) => match value.parse::<u64>() {
            Ok(0) | Err(_) => default,
            Ok(ms) => Duration::from_millis(ms),
        },
        Err(_) => default,
    }
}

fn run_shard_scavenger_audit(
    storage_handle: &StorageClusterRouteHandle,
    admission: &Arc<StorageMaintenanceAdmission>,
) {
    let storage_cluster = storage_handle.current();
    if let Some(_permit) = admission.try_opportunistic_scan() {
        if let Err(error) = storage_cluster.audit_shard_storage_for_scavenger() {
            let _ = observability::event(
                TRACE_TARGET,
                "shard_scavenger_audit_error",
                Some(format_args!("error={error}")),
            );
        }
    }
}

fn run_backfill_candidate_scan(
    admission: &Arc<StorageMaintenanceAdmission>,
    candidate_scanner: &StorageBackfillCandidateScanner,
) {
    if let Some(_permit) = admission.try_backfill_candidate_scan() {
        candidate_scanner.scan();
    }
}

fn run_metadata_checkpoint_scan(
    admission: &Arc<StorageMaintenanceAdmission>,
    checkpoint_scanner: &StorageMetadataCheckpointScanner,
) {
    if let Some(_permit) = admission.try_routine_metadata_checkpoint() {
        match checkpoint_scanner.scan() {
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
                        compaction_deleted_entries: summary.compaction_deleted_entries,
                        compaction_noop: summary.compaction_noop,
                        compaction_no_checkpoint: summary.compaction_no_checkpoint,
                        compaction_pending: summary.compaction_pending,
                        compaction_failed: summary.compaction_failed,
                        failed: summary.failed,
                        limit_reached: summary.limit_reached,
                    },
                );
            }
            Err(error) => {
                let _ = observability::emit_metadata_command_checkpoint_record_scan_error(
                    TRACE_TARGET,
                    &error,
                );
            }
        }
    }
}

/// Opaque storage-owned durable shard-repair execution worker.
pub struct StorageShardRepairSweeper {
    storage_handle: StorageClusterRouteHandle,
    stop: Arc<AtomicBool>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl StorageShardRepairSweeper {
    pub fn acquire_shared(
        storage_handle: &StorageClusterRouteHandle,
    ) -> Result<Arc<Self>, StorageMaintenanceStartError> {
        let registry = SHARD_REPAIR_SWEEPER_REGISTRY.get_or_init(|| Mutex::new(Vec::new()));
        let mut registry = registry.lock().unwrap_or_else(|error| error.into_inner());
        registry.retain(|sweeper| sweeper.upgrade().is_some());

        if let Some(existing) = registry.iter().filter_map(Weak::upgrade).find(|sweeper| {
            sweeper
                .storage_handle
                .shares_route_admission_with(storage_handle)
        }) {
            return Ok(existing);
        }

        let sweeper = Self::spawn(storage_handle.clone())?;
        registry.push(Arc::downgrade(&sweeper));
        Ok(sweeper)
    }

    fn spawn(
        storage_handle: StorageClusterRouteHandle,
    ) -> Result<Arc<Self>, StorageMaintenanceStartError> {
        let worker_identity = random_storage_worker_identity().map_err(|error| {
            StorageMaintenanceStartError::worker_spawn("shard repair identity", error)
        })?;
        let owner_token = format!("shard-repair-worker-{worker_identity}");
        let admission = StorageMaintenanceAdmission::acquire_shared(&storage_handle);
        let stop = Arc::new(AtomicBool::new(false));
        let sweeper = Arc::new(Self {
            storage_handle: storage_handle.clone(),
            stop: Arc::clone(&stop),
            handle: Mutex::new(None),
        });
        let handle = std::thread::Builder::new()
            .name("argmin-shard-repair".to_string())
            .spawn(move || {
                run_shard_repair_worker(
                    &storage_handle,
                    &stop,
                    &admission,
                    &worker_identity,
                    &owner_token,
                );
            })
            .map_err(|error| StorageMaintenanceStartError::worker_spawn("shard repair", error))?;
        *sweeper
            .handle
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(handle);
        Ok(sweeper)
    }

    #[must_use]
    pub fn disabled(storage_handle: StorageClusterRouteHandle) -> Arc<Self> {
        Arc::new(Self {
            storage_handle,
            stop: Arc::new(AtomicBool::new(true)),
            handle: Mutex::new(None),
        })
    }

    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    #[must_use]
    pub(crate) fn test_is_enabled(&self) -> bool {
        !self.stop.load(Ordering::SeqCst)
    }

    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub(crate) fn test_routes_to(&self, expected: &Arc<crate::StorageCluster>) -> bool {
        Arc::ptr_eq(&self.storage_handle.current(), expected)
    }

    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub(crate) fn test_repair_one_pending(&self) -> bool {
        let storage_cluster = self.storage_handle.current();
        emit_shard_repair_durable_scan(
            storage_cluster.enqueue_durable_placed_segment_shard_repair_work(),
        );
        let Some(work_item) = storage_cluster.try_take_placed_segment_shard_repair_work() else {
            return false;
        };
        let admission = StorageMaintenanceAdmission::acquire_shared(&self.storage_handle);
        process_shard_repair_work_item(
            &storage_cluster,
            &admission,
            "deterministic-test-worker",
            "shard-repair-deterministic-test-worker",
            work_item,
        );
        true
    }
}

impl Drop for StorageShardRepairSweeper {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        self.storage_handle
            .current()
            .wake_placed_segment_shard_repair_workers();
        if let Some(handle) = self
            .handle
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            let _ = handle.join();
        }
    }
}

fn random_storage_worker_identity() -> Result<String, std::io::Error> {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut bytes = [0u8; 16];
    argmin_crypto::random::fill(&mut bytes)
        .map_err(|_| std::io::Error::other("secure random generation failed"))?;
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    Ok(encoded)
}

fn run_shard_repair_worker(
    storage_handle: &StorageClusterRouteHandle,
    stop: &AtomicBool,
    admission: &Arc<StorageMaintenanceAdmission>,
    worker_identity: &str,
    owner_token: &str,
) {
    let mut next_durable_scan_at = Instant::now();
    while !stop.load(Ordering::SeqCst) {
        let storage_cluster = storage_handle.current();
        let now = Instant::now();
        if now >= next_durable_scan_at {
            emit_shard_repair_durable_scan(
                storage_cluster.enqueue_durable_placed_segment_shard_repair_work(),
            );
            next_durable_scan_at = now + SHARD_REPAIR_DURABLE_SCAN_INTERVAL;
        }

        let Some(work_item) = storage_cluster
            .try_take_placed_segment_shard_repair_work()
            .or_else(|| storage_cluster.wait_for_placed_segment_shard_repair_work(stop))
        else {
            continue;
        };
        if stop.load(Ordering::SeqCst) {
            break;
        }

        process_shard_repair_work_item(
            &storage_cluster,
            admission,
            worker_identity,
            owner_token,
            work_item,
        );
    }
}

fn process_shard_repair_work_item(
    storage_cluster: &crate::StorageCluster,
    admission: &Arc<StorageMaintenanceAdmission>,
    worker_identity: &str,
    owner_token: &str,
    work_item: crate::types::PlacedSegmentShardRepairWorkItem,
) {
    let now_ms = crate::clock::current_time_millis();
    let claim_id = format!(
        "shard-repair-{}-{}-{}-{}",
        worker_identity,
        work_item.request.data_pg_id,
        work_item.shard_index.get(),
        SHARD_REPAIR_CLAIM_COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    let claim_acquire = PlacedSegmentShardRepairClaimAcquireParams {
        claim_id,
        owner_token: owner_token.to_string(),
        claimed_at: now_ms,
        lease_deadline: now_ms.saturating_add(SHARD_REPAIR_CLAIM_LEASE_MILLIS),
        now: now_ms,
    };
    let claim = match storage_cluster
        .acquire_placed_segment_shard_repair_claim(work_item.request.data_pg_id, &claim_acquire)
    {
        Ok(Some(claim)) => {
            emit_shard_repair_event(Some(work_item.request.data_pg_id), "claim_started", None);
            claim
        }
        Ok(None) => {
            emit_shard_repair_event(Some(work_item.request.data_pg_id), "claim_empty", None);
            return;
        }
        Err(error) => {
            record_shard_repair_failure(
                Some(work_item.request.data_pg_id),
                "claim_failed",
                "shard_repair_claim_error",
                &error,
            );
            return;
        }
    };

    let Some(_repair_permit) = admission.try_known_damage_repair() else {
        emit_shard_repair_event(
            Some(claim.work_item.request.data_pg_id),
            "admission_denied",
            None,
        );
        let next_attempt_after =
            crate::clock::current_time_millis().saturating_add(SHARD_REPAIR_ERROR_BACKOFF_MILLIS);
        record_shard_repair_claim_error(
            storage_cluster,
            &claim,
            "background known-damage repair admission denied",
            next_attempt_after,
            None,
        );
        return;
    };

    emit_shard_repair_event(Some(claim.work_item.request.data_pg_id), "started", None);
    match storage_cluster.repair_placed_segment_payload_shards_if_needed_preserving_repair_rows(
        claim.work_item.request,
    ) {
        Ok(repaired_acks) => {
            let event = if repaired_acks.is_empty() {
                "resolved_clean"
            } else {
                "repaired"
            };
            emit_shard_repair_event(
                Some(claim.work_item.request.data_pg_id),
                event,
                Some(repaired_acks.len()),
            );
            match storage_cluster.complete_placed_segment_shard_repair_claim(&claim) {
                Ok(true) => emit_shard_repair_event(
                    Some(claim.work_item.request.data_pg_id),
                    "complete_succeeded",
                    None,
                ),
                Ok(false) => emit_shard_repair_event(
                    Some(claim.work_item.request.data_pg_id),
                    "complete_stale",
                    None,
                ),
                Err(error) => record_shard_repair_failure(
                    Some(claim.work_item.request.data_pg_id),
                    "complete_failed",
                    "shard_repair_complete_error",
                    &error,
                ),
            }
        }
        Err(error) => {
            let event = if matches!(error, StoreError::NotFound) {
                "unrecoverable"
            } else {
                "failed"
            };
            record_shard_repair_failure(
                Some(claim.work_item.request.data_pg_id),
                event,
                "shard_repair_error",
                &error,
            );
            let next_attempt_after = crate::clock::current_time_millis()
                .saturating_add(SHARD_REPAIR_ERROR_BACKOFF_MILLIS);
            record_shard_repair_claim_error(
                storage_cluster,
                &claim,
                &error.to_string(),
                next_attempt_after,
                Some(&error),
            );
        }
    }
}

fn emit_shard_repair_durable_scan(
    result: Result<DurablePlacedSegmentShardRepairEnqueueSummary, StoreError>,
) {
    match result {
        Ok(summary) if summary.scanned == 0 => {
            emit_shard_repair_event(None, "durable_scan_empty", None);
        }
        Ok(summary) if summary.enqueued > 0 => {
            emit_shard_repair_event(None, "durable_scan_queued", None);
        }
        Ok(_) => emit_shard_repair_event(None, "durable_scan_no_new_enqueue", None),
        Err(error) => {
            record_shard_repair_failure(
                None,
                "durable_scan_failed",
                "shard_repair_durable_scan_error",
                &error,
            );
        }
    }
}

fn emit_shard_repair_event(
    pg_id: Option<u32>,
    event: &'static str,
    shards_rewritten: Option<usize>,
) {
    let _ = observability::emit_shard_repair_event(
        TRACE_TARGET,
        observability::ShardRepairEventSummary {
            pg_id,
            event,
            queue_depth: None,
            shards_rewritten,
        },
    );
}

fn record_shard_repair_failure(
    pg_id: Option<u32>,
    event: &'static str,
    trace_event: &'static str,
    error: &StoreError,
) {
    observability::record_shard_repair_error(pg_id, event, error.diagnostic_kind());
    emit_shard_repair_event(pg_id, event, None);
    let _ = observability::event(
        TRACE_TARGET,
        trace_event,
        Some(format_args!("error={error}")),
    );
}

fn record_shard_repair_claim_error(
    storage_cluster: &crate::StorageCluster,
    claim: &PlacedSegmentShardRepairClaimRecord,
    last_error: &str,
    next_attempt_after: u64,
    repair_error: Option<&StoreError>,
) {
    if let Err(record_error) = storage_cluster.record_placed_segment_shard_repair_claim_error(
        claim,
        last_error,
        next_attempt_after,
    ) {
        observability::record_shard_repair_error(
            Some(claim.work_item.request.data_pg_id),
            "record_error_failed",
            record_error.diagnostic_kind(),
        );
        emit_shard_repair_event(
            Some(claim.work_item.request.data_pg_id),
            "record_error_failed",
            None,
        );
        let _ = observability::event(
            TRACE_TARGET,
            "shard_repair_record_error_failed",
            Some(format_args!(
                "repair_error={} record_error={record_error}",
                repair_error.map_or("<admission denied>".to_string(), ToString::to_string)
            )),
        );
    }
}

/// Opaque storage-owned abandoned stream-session cleanup worker.
pub struct StorageStreamSessionSweeper {
    storage_handle: StorageClusterRouteHandle,
    #[cfg(feature = "test-hooks")]
    max_age_ms: u64,
    stop: Arc<AtomicBool>,
    wake: Arc<(Mutex<bool>, Condvar)>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

#[cfg(feature = "test-hooks")]
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageStreamSessionSweepTestSummary {
    pub discovered: usize,
    pub due: usize,
    pub cleaned: usize,
    pub reservation_check_failed: usize,
    pub abort_failed: usize,
}

impl StorageStreamSessionSweeper {
    pub fn acquire_shared(
        storage_handle: &StorageClusterRouteHandle,
    ) -> Result<Arc<Self>, StorageMaintenanceStartError> {
        Self::acquire_shared_with_settings(
            storage_handle,
            STREAM_SESSION_SWEEP_INTERVAL,
            STREAM_SESSION_SCAVENGE_MAX_AGE_MILLIS,
        )
    }

    fn acquire_shared_with_settings(
        storage_handle: &StorageClusterRouteHandle,
        sweep_interval: Duration,
        max_age_ms: u64,
    ) -> Result<Arc<Self>, StorageMaintenanceStartError> {
        let registry = STREAM_SESSION_SWEEPER_REGISTRY.get_or_init(|| Mutex::new(Vec::new()));
        let mut registry = registry.lock().unwrap_or_else(|error| error.into_inner());
        registry.retain(|sweeper| sweeper.upgrade().is_some());

        if let Some(existing) = registry.iter().filter_map(Weak::upgrade).find(|sweeper| {
            sweeper
                .storage_handle
                .shares_route_admission_with(storage_handle)
        }) {
            return Ok(existing);
        }

        let sweeper = Self::spawn(storage_handle.clone(), sweep_interval, max_age_ms)?;
        registry.push(Arc::downgrade(&sweeper));
        Ok(sweeper)
    }

    fn spawn(
        storage_handle: StorageClusterRouteHandle,
        sweep_interval: Duration,
        max_age_ms: u64,
    ) -> Result<Arc<Self>, StorageMaintenanceStartError> {
        let stop = Arc::new(AtomicBool::new(false));
        let wake = Arc::new((Mutex::new(false), Condvar::new()));
        let sweeper = Arc::new(Self {
            storage_handle: storage_handle.clone(),
            #[cfg(feature = "test-hooks")]
            max_age_ms,
            stop: Arc::clone(&stop),
            wake: Arc::clone(&wake),
            handle: Mutex::new(None),
        });
        let handle = std::thread::Builder::new()
            .name("argmin-stream-session-sweeper".to_string())
            .spawn(move || {
                while !stop.load(Ordering::SeqCst) {
                    sweep_abandoned_stream_sessions(&storage_handle, max_age_ms);
                    if stop.load(Ordering::SeqCst) {
                        break;
                    }
                    let stop_guard = wake.0.lock().unwrap_or_else(|error| error.into_inner());
                    if *stop_guard {
                        break;
                    }
                    let _ = wake
                        .1
                        .wait_timeout_while(stop_guard, sweep_interval, |stop_requested| {
                            !*stop_requested
                        })
                        .unwrap_or_else(|error| error.into_inner());
                }
            })
            .map_err(|error| {
                StorageMaintenanceStartError::worker_spawn("stream-session sweeper", error)
            })?;
        *sweeper
            .handle
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(handle);
        Ok(sweeper)
    }

    #[must_use]
    pub fn disabled(storage_handle: StorageClusterRouteHandle) -> Arc<Self> {
        Arc::new(Self {
            storage_handle,
            #[cfg(feature = "test-hooks")]
            max_age_ms: STREAM_SESSION_SCAVENGE_MAX_AGE_MILLIS,
            stop: Arc::new(AtomicBool::new(true)),
            wake: Arc::new((Mutex::new(true), Condvar::new())),
            handle: Mutex::new(None),
        })
    }

    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub(crate) fn test_sweep_once(&self) -> StorageStreamSessionSweepTestSummary {
        let summary = sweep_abandoned_stream_sessions(&self.storage_handle, self.max_age_ms);
        StorageStreamSessionSweepTestSummary {
            discovered: summary.discovered,
            due: summary.due,
            cleaned: summary.cleaned,
            reservation_check_failed: summary.reservation_check_failed,
            abort_failed: summary.abort_failed,
        }
    }

    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    #[must_use]
    pub(crate) fn test_is_enabled(&self) -> bool {
        !self.stop.load(Ordering::SeqCst)
    }

    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub(crate) fn test_routes_to(&self, expected: &Arc<crate::StorageCluster>) -> bool {
        Arc::ptr_eq(&self.storage_handle.current(), expected)
    }
}

impl Drop for StorageStreamSessionSweeper {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        *self
            .wake
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = true;
        self.wake.1.notify_all();
        if let Some(handle) = self
            .handle
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            let _ = handle.join();
        }
    }
}

fn sweep_abandoned_stream_sessions(
    storage_handle: &StorageClusterRouteHandle,
    max_age_ms: u64,
) -> StreamSessionSweepSummary {
    let summary = storage_handle
        .current()
        .scavenge_abandoned_stream_sessions(max_age_ms);
    if summary.cleaned > 0 {
        let _ = observability::event(
            TRACE_TARGET,
            "stream_session_sweep_abandoned",
            Some(format_args!("aborted_sessions={}", summary.cleaned)),
        );
    }
    if summary.reservation_check_failed > 0 || summary.abort_failed > 0 {
        let _ = observability::event(
            TRACE_TARGET,
            "stream_session_sweep_error",
            Some(format_args!(
                "reservation_check_failed={} abort_failed={}",
                summary.reservation_check_failed, summary.abort_failed
            )),
        );
    }
    summary
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maintenance_admission_limits_and_releases_each_class() {
        let admission = Arc::new(StorageMaintenanceAdmission::with_limits(
            StorageMaintenanceAdmissionLimits {
                known_damage_repair: 1,
                backfill_candidate_scan: 1,
                routine_backfill: 1,
                reclaim_cleanup: 1,
                lifecycle_cleanup: 1,
                opportunistic_scan: 0,
                routine_metadata_checkpoint: 1,
            },
        ));

        let repair = admission
            .try_known_damage_repair()
            .expect("first repair permit should fit");
        assert!(admission.try_known_damage_repair().is_none());
        let reclaim = admission
            .try_reclaim_cleanup()
            .expect("reclaim has an independent permit");
        assert!(admission.try_reclaim_cleanup().is_none());
        let lifecycle = admission
            .try_lifecycle_cleanup()
            .expect("lifecycle has an independent permit");
        assert!(admission.try_lifecycle_cleanup().is_none());
        let checkpoint = admission
            .try_routine_metadata_checkpoint()
            .expect("checkpoint has an independent permit");
        assert!(admission.try_routine_metadata_checkpoint().is_none());
        assert_eq!(admission.active_total(), 4);
        assert!(admission.try_opportunistic_scan().is_none());

        drop(repair);
        let candidate = admission
            .try_backfill_candidate_scan()
            .expect("candidate scan should run after repair releases");
        assert!(admission.try_backfill_candidate_scan().is_none());
        drop(candidate);
        let routine = admission
            .try_routine_backfill()
            .expect("routine backfill should fit");
        assert!(admission.try_routine_backfill().is_none());
        drop(checkpoint);
        drop(reclaim);
        drop(lifecycle);
        drop(routine);
        assert_eq!(admission.active_total(), 0);
    }

    #[test]
    fn maintenance_admission_prioritizes_known_damage_over_backfill() {
        let admission = Arc::new(StorageMaintenanceAdmission::new());
        let repair = admission
            .try_known_damage_repair()
            .expect("known damage should be admitted");
        assert!(admission.try_routine_backfill().is_none());
        assert!(admission.try_backfill_candidate_scan().is_none());
        drop(repair);
        assert!(admission.try_backfill_candidate_scan().is_some());
        assert!(admission.try_routine_backfill().is_some());
    }

    #[test]
    fn maintenance_admission_keeps_checkpoints_available_under_backlog() {
        let admission = StorageMaintenanceAdmission::new();
        let backlog = StorageMaintenancePressure {
            foreground: false,
            durable_backlog: true,
        };

        assert_eq!(
            admission.policy_denial_event_for_pressure(
                StorageMaintenanceClass::OpportunisticScan,
                backlog,
            ),
            Some(observability::BackgroundWorkAdmissionEvent::DeniedBacklogPressure)
        );
        assert_eq!(
            admission.policy_denial_event_for_pressure(
                StorageMaintenanceClass::RoutineMetadataCheckpoint,
                backlog,
            ),
            None
        );
        assert_eq!(
            admission.policy_denial_event_for_pressure(
                StorageMaintenanceClass::RoutineMetadataCheckpoint,
                StorageMaintenancePressure {
                    foreground: true,
                    durable_backlog: false,
                },
            ),
            Some(observability::BackgroundWorkAdmissionEvent::DeniedForegroundPressure)
        );
    }

    #[test]
    fn maintenance_pressure_uses_only_recent_foreground_deltas() {
        let mut state = StorageMaintenancePressureState::default();
        let now = Instant::now();
        let mut snapshot = observability::MetricsSnapshot::default();
        assert_eq!(
            state.observe(now, snapshot),
            StorageMaintenancePressure {
                foreground: false,
                durable_backlog: false,
            }
        );

        snapshot.request_admission_wait_total += 1;
        let production_sweep_gap = Duration::from_secs(60);
        assert!(
            !state
                .observe(now + production_sweep_gap, snapshot)
                .foreground
        );

        snapshot.request_admission_timeout_total += 1;
        let before_next_scan = now + (production_sweep_gap * 2)
            - BACKGROUND_FOREGROUND_PRESSURE_SAMPLE_INTERVAL
            - Duration::from_millis(20);
        assert!(!state.observe(before_next_scan, snapshot).foreground);
        snapshot.request_admission_wait_total += 1;
        let recent = now + (production_sweep_gap * 2);
        assert!(state.observe(recent, snapshot).foreground);
        assert!(
            !state
                .observe(
                    recent + BACKGROUND_FOREGROUND_PRESSURE_HOLD + Duration::from_millis(20),
                    snapshot,
                )
                .foreground
        );
    }

    #[test]
    fn maintenance_pressure_ignores_recovery_counters_and_detects_live_work() {
        let mut state = StorageMaintenancePressureState::default();
        let now = Instant::now();
        let mut snapshot = observability::MetricsSnapshot::default();
        let _ = state.observe(now, snapshot);
        snapshot.metadata_command_recovery_wait_total += 1;
        snapshot.metadata_command_recovery_timeout_total += 1;
        snapshot.metadata_command_budget_exhausted_total += 1;
        assert!(
            !state
                .observe(now + Duration::from_millis(10), snapshot)
                .foreground
        );

        snapshot.request_admission_capacity = 8;
        snapshot.inflight_requests = 6;
        snapshot.reclaim_work_queue_depth = 7;
        assert_eq!(
            state.observe(now + Duration::from_millis(20), snapshot),
            StorageMaintenancePressure {
                foreground: true,
                durable_backlog: true,
            }
        );
    }

    #[test]
    fn maintenance_pressure_does_not_treat_background_storage_rpcs_as_foreground() {
        let mut state = StorageMaintenancePressureState::default();
        let now = Instant::now();
        let mut snapshot = observability::MetricsSnapshot::default();
        assert_eq!(
            state.observe(now, snapshot),
            StorageMaintenancePressure {
                foreground: false,
                durable_backlog: false,
            }
        );

        snapshot.storage_rpc_admission_wait_total = 1;
        snapshot.storage_rpc_admission_timeout_total = 1;
        snapshot.storage_rpc_active_total = 4;
        snapshot.storage_rpc_active_read = 1;
        snapshot.storage_rpc_active_start_write = 1;
        snapshot.storage_rpc_active_list = 2;
        assert_eq!(
            state.observe(now + Duration::from_millis(10), snapshot),
            StorageMaintenancePressure {
                foreground: false,
                durable_backlog: false,
            },
            "unattributed storage RPC activity includes background workflows and must not make background classes deny one another"
        );

        snapshot.request_admission_capacity = 32;
        snapshot.inflight_requests = 1;
        assert_eq!(
            state.observe(now + Duration::from_millis(20), snapshot),
            StorageMaintenancePressure {
                foreground: false,
                durable_backlog: false,
            },
            "one admitted request must not suppress background work when capacity remains"
        );

        snapshot.inflight_requests = 24;
        assert_eq!(
            state.observe(now + Duration::from_millis(30), snapshot),
            StorageMaintenancePressure {
                foreground: true,
                durable_backlog: false,
            },
            "the admission high-water mark identifies foreground pressure"
        );
    }

    #[test]
    fn maintenance_foreground_pressure_reserves_one_quarter_of_request_capacity() {
        for (capacity, below_high_water, high_water) in
            [(1, 0, 1), (2, 1, 2), (4, 2, 3), (8, 5, 6), (32, 23, 24)]
        {
            let below = observability::MetricsSnapshot {
                inflight_requests: below_high_water,
                request_admission_capacity: capacity,
                ..observability::MetricsSnapshot::default()
            };
            assert!(!maintenance_foreground_pressure_active(below));

            let at = observability::MetricsSnapshot {
                inflight_requests: high_water,
                request_admission_capacity: capacity,
                ..observability::MetricsSnapshot::default()
            };
            assert!(maintenance_foreground_pressure_active(at));
        }
    }

    #[test]
    fn maintenance_start_error_keeps_worker_diagnostic_opaque() {
        let error = StorageMaintenanceStartError {
            _diagnostic: "sensitive worker spawn detail".into(),
        };

        assert_eq!(
            error.to_string(),
            "failed to start storage maintenance worker"
        );
        assert_eq!(
            format!("{error:?}"),
            "StorageMaintenanceStartError { diagnostic: \"<redacted>\" }"
        );
        assert!(std::error::Error::source(&error).is_none());
    }

    #[test]
    fn storage_worker_identity_is_opaque_fixed_width_hex() {
        let identity = random_storage_worker_identity().unwrap();

        assert_eq!(identity.len(), 32);
        assert!(identity
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()));
    }

    #[test]
    fn shard_backfill_admission_class_uses_ec_risk_tolerance() {
        assert_eq!(
            shard_backfill_admission_class(2, 2),
            ShardBackfillAdmission::RoutineBackfill
        );
        assert_eq!(
            shard_backfill_admission_class(1, 2),
            ShardBackfillAdmission::KnownDamageRepair
        );
        assert_eq!(
            shard_backfill_admission_class(0, 2),
            ShardBackfillAdmission::KnownDamageRepair
        );
        assert_eq!(
            shard_backfill_admission_class(0, 0),
            ShardBackfillAdmission::RoutineBackfill
        );
    }

    #[test]
    fn shard_backfill_error_classifies_stale_retries() {
        assert!(shard_backfill_error_is_stale_retry(
            &StoreError::StalePayloadOperation {
                pg_id: 7,
                operation_epoch: crate::ClusterEpoch::INITIAL,
                current_epoch: crate::ClusterEpoch::new(2).unwrap(),
            }
        ));
        assert!(shard_backfill_error_is_stale_retry(
            &StoreError::ShardStore {
                node_id: 2,
                pg_id: 7,
                cluster_epoch: crate::ClusterEpoch::INITIAL,
                source: Box::new(StoreError::StaleShardLocation {
                    node_id: 2,
                    pg_id: 7,
                    location_epoch: crate::ClusterEpoch::INITIAL,
                    current_epoch: crate::ClusterEpoch::new(2).unwrap(),
                }),
            }
        ));
        assert!(!shard_backfill_error_is_stale_retry(&StoreError::NotFound));
    }

    #[test]
    fn shard_backfill_source_reference_check_is_limited_to_source_loss() {
        assert!(shard_backfill_error_may_mean_source_is_obsolete(
            &StoreError::NotFound
        ));
        assert!(shard_backfill_error_may_mean_source_is_obsolete(
            &StoreError::HistoricalPgRouteNotRetained {
                pg_id: 7,
                cluster_epoch: crate::ClusterEpoch::INITIAL,
            }
        ));
        assert!(shard_backfill_error_may_mean_source_is_obsolete(
            &StoreError::PlacedSegmentBackfillSourceUnavailable
        ));

        for error in [
            StoreError::PayloadShardSetMismatch {
                reason: "target verification failed".to_string(),
            },
            StoreError::Io {
                context: "contact source node",
                source: std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "source node unavailable",
                ),
            },
            StoreError::StalePayloadOperation {
                pg_id: 7,
                operation_epoch: crate::ClusterEpoch::INITIAL,
                current_epoch: crate::ClusterEpoch::new(2).unwrap(),
            },
            StoreError::RouteMapExpired {
                cluster_epoch: crate::ClusterEpoch::INITIAL,
                valid_until_ms: 1,
                now_ms: 2,
            },
        ] {
            assert!(!shard_backfill_error_may_mean_source_is_obsolete(&error));
        }
    }

    #[test]
    fn shard_backfill_retry_selects_storage_node_failure_classes() {
        for failure in [
            StorageNodeFailureClass::ShardLocationStale,
            StorageNodeFailureClass::PgRouteUnavailable,
            StorageNodeFailureClass::TransportInterrupted,
        ] {
            assert!(storage_node_failure_is_shard_backfill_stale_retry(failure));
        }
        for failure in [
            StorageNodeFailureClass::MetadataCommandContention,
            StorageNodeFailureClass::MetadataTransferHistoricalRouteActive,
        ] {
            assert!(!storage_node_failure_is_shard_backfill_stale_retry(failure));
        }
    }

    #[test]
    fn shard_backfill_outcome_events_classify_stale_errors() {
        assert_eq!(
            shard_backfill_completion_error_event(&StoreError::StalePayloadOperation {
                pg_id: 7,
                operation_epoch: crate::ClusterEpoch::INITIAL,
                current_epoch: crate::ClusterEpoch::new(2).unwrap(),
            }),
            "complete_stale"
        );
        assert_eq!(
            shard_backfill_completion_error_event(&StoreError::NotFound),
            "complete_failed"
        );
        assert_eq!(
            shard_backfill_record_error_event(&StoreError::StaleShardLocation {
                node_id: 2,
                pg_id: 7,
                location_epoch: crate::ClusterEpoch::INITIAL,
                current_epoch: crate::ClusterEpoch::new(2).unwrap(),
            }),
            "record_retry"
        );
        assert_eq!(
            shard_backfill_record_error_event(&StoreError::NotFound),
            "record_error_failed"
        );
    }
}
