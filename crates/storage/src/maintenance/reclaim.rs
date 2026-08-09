// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::cluster::{
    DurableReclaimScanBatch, DurableReclaimScanOutcome, ObjectPayloadReclaimAttempt,
    StorageClusterRouteHandle,
};
use crate::{
    BucketDeleteBeginRoot, BucketDeleteFinalizeOutcome, BucketDeleteFinalizeRoot, BucketName,
    BucketWriteDrainError, GenerationId, MetadataError, ObjectKey, ObjectPgActionError,
    ReclaimWorkItem, StorageCluster, StoreError,
};

use super::{StorageMaintenanceAdmission, StorageMaintenanceStartError, TRACE_TARGET};

const OBJECT_PAYLOAD_RECLAIM_PG_RETRY_COOLDOWN: Duration = Duration::from_millis(100);
const BUCKET_DELETE_BEGIN_RETRY_COOLDOWN: Duration = Duration::from_millis(100);
const BUCKET_DELETE_FINALIZE_RETRY_COOLDOWN: Duration = Duration::from_millis(100);
const BUCKET_DELETE_FINALIZE_ERROR_RETRY_COOLDOWN: Duration = Duration::from_secs(1);
const RECLAIM_DURABLE_SCAN_BATCH_PGS: usize = 8;
const RECLAIM_DURABLE_SCAN_SAFETY_INTERVAL: Duration = Duration::from_secs(60);
const RECLAIM_DURABLE_SCAN_ROUTE_REFRESH_BACKOFF: Duration = Duration::from_secs(1);
const RECLAIM_DURABLE_SCAN_INCOMPLETE_RETRY: Duration = Duration::from_secs(1);

type ObjectPayloadReclaimRoot = (BucketName, ObjectKey, GenerationId);

static RECLAIM_SWEEPER_REGISTRY: OnceLock<Mutex<Vec<Weak<StorageReclaimSweeper>>>> =
    OnceLock::new();

#[cfg(feature = "test-hooks")]
#[derive(Default, Clone)]
pub struct StorageReclaimWorkerTestHooks {
    pub target_registry_key: Option<crate::ProcessLocalRegistryKey>,
    pub durable_scan_delay_override: Option<Duration>,
    pub after_idle_return: Option<Arc<dyn Fn() + Send + Sync>>,
    pub after_work_dequeued: Option<Arc<dyn Fn() + Send + Sync>>,
    pub before_work_execute: Option<Arc<dyn Fn(Arc<StorageCluster>) + Send + Sync>>,
}

#[cfg(feature = "test-hooks")]
static RECLAIM_WORKER_TEST_HOOKS: OnceLock<Mutex<StorageReclaimWorkerTestHooks>> = OnceLock::new();

#[cfg(feature = "test-hooks")]
pub struct StorageReclaimWorkerTestHookGuard;

#[cfg(feature = "test-hooks")]
impl Drop for StorageReclaimWorkerTestHookGuard {
    fn drop(&mut self) {
        let hooks = RECLAIM_WORKER_TEST_HOOKS
            .get_or_init(|| Mutex::new(StorageReclaimWorkerTestHooks::default()));
        *hooks.lock().unwrap_or_else(|error| error.into_inner()) =
            StorageReclaimWorkerTestHooks::default();
    }
}

#[cfg(feature = "test-hooks")]
pub fn install_reclaim_worker_test_hooks(
    hooks: StorageReclaimWorkerTestHooks,
) -> StorageReclaimWorkerTestHookGuard {
    let slot = RECLAIM_WORKER_TEST_HOOKS
        .get_or_init(|| Mutex::new(StorageReclaimWorkerTestHooks::default()));
    *slot.lock().unwrap_or_else(|error| error.into_inner()) = hooks;
    StorageReclaimWorkerTestHookGuard
}

#[cfg(feature = "test-hooks")]
fn reclaim_worker_test_hooks(
    registry_key: crate::ProcessLocalRegistryKey,
) -> StorageReclaimWorkerTestHooks {
    let hooks = RECLAIM_WORKER_TEST_HOOKS
        .get_or_init(|| Mutex::new(StorageReclaimWorkerTestHooks::default()))
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .clone();
    if hooks
        .target_registry_key
        .is_some_and(|target| target != registry_key)
    {
        StorageReclaimWorkerTestHooks::default()
    } else {
        hooks
    }
}

struct DeferredReclaimWork<T> {
    queue_owner: Arc<StorageCluster>,
    root: T,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeferredReclaimClass {
    ObjectPayload,
    BucketDeleteBegin,
    BucketDeleteFinalize,
}

impl DeferredReclaimClass {
    fn next(self) -> Self {
        match self {
            Self::ObjectPayload => Self::BucketDeleteBegin,
            Self::BucketDeleteBegin => Self::BucketDeleteFinalize,
            Self::BucketDeleteFinalize => Self::ObjectPayload,
        }
    }
}

fn select_deferred_reclaim_class(
    next_class: &mut DeferredReclaimClass,
    object_payload_available: bool,
    bucket_delete_begin_available: bool,
    bucket_delete_finalize_available: bool,
) -> Option<DeferredReclaimClass> {
    for _ in 0..3 {
        let candidate = *next_class;
        *next_class = candidate.next();
        let available = match candidate {
            DeferredReclaimClass::ObjectPayload => object_payload_available,
            DeferredReclaimClass::BucketDeleteBegin => bucket_delete_begin_available,
            DeferredReclaimClass::BucketDeleteFinalize => bucket_delete_finalize_available,
        };
        if available {
            return Some(candidate);
        }
    }
    None
}

fn defer_object_payload_reclaim(
    deferred_work: &mut VecDeque<DeferredReclaimWork<ObjectPayloadReclaimRoot>>,
    deferred_roots: &mut HashSet<ObjectPayloadReclaimRoot>,
    queue_owner: Arc<StorageCluster>,
    root: ObjectPayloadReclaimRoot,
) {
    if deferred_roots.insert(root.clone()) {
        deferred_work.push_back(DeferredReclaimWork { queue_owner, root });
    }
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

fn defer_bucket_delete_finalize(
    deferred_work: &mut VecDeque<DeferredReclaimWork<BucketDeleteFinalizeRoot>>,
    deferred_roots: &mut HashSet<BucketDeleteFinalizeRoot>,
    queue_owner: Arc<StorageCluster>,
    root: BucketDeleteFinalizeRoot,
) {
    if deferred_roots.insert(root.clone()) {
        deferred_work.push_back(DeferredReclaimWork { queue_owner, root });
    }
}

fn earliest_object_payload_reclaim_retry_sleep(
    storage_node: &StorageCluster,
    deferred_work: &VecDeque<DeferredReclaimWork<ObjectPayloadReclaimRoot>>,
    retry_after_by_pg: &HashMap<u32, Instant>,
) -> Option<Duration> {
    let now = Instant::now();
    let mut earliest_retry: Option<Instant> = None;
    for deferred in deferred_work {
        let (bucket, key, _) = &deferred.root;
        let pg_id = storage_node.object_payload_reclaim_pg_id(bucket, key);
        let retry_after = retry_after_by_pg.get(&pg_id)?;
        if *retry_after <= now {
            return None;
        }
        earliest_retry = Some(earliest_retry.map_or(*retry_after, |old| old.min(*retry_after)));
    }
    earliest_retry.map(|retry_after| {
        retry_after
            .duration_since(now)
            .min(OBJECT_PAYLOAD_RECLAIM_PG_RETRY_COOLDOWN)
    })
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
        earliest_retry = Some(earliest_retry.map_or(*retry_after, |old| old.min(*retry_after)));
    }
    earliest_retry.map(|retry_after| {
        retry_after
            .duration_since(now)
            .min(BUCKET_DELETE_BEGIN_RETRY_COOLDOWN)
    })
}

fn earliest_bucket_delete_finalize_retry_sleep<'a>(
    deferred_roots: impl IntoIterator<Item = &'a BucketDeleteFinalizeRoot>,
    retry_after_by_root: &HashMap<BucketDeleteFinalizeRoot, Instant>,
) -> Option<Duration> {
    let now = Instant::now();
    let mut earliest_retry: Option<Instant> = None;
    for root in deferred_roots {
        let retry_after = retry_after_by_root.get(root)?;
        if *retry_after <= now {
            return None;
        }
        earliest_retry = Some(earliest_retry.map_or(*retry_after, |old| old.min(*retry_after)));
    }
    earliest_retry.map(|retry_after| {
        retry_after
            .duration_since(now)
            .min(BUCKET_DELETE_FINALIZE_RETRY_COOLDOWN)
    })
}

fn shortest_retry_sleep(left: Option<Duration>, right: Option<Duration>) -> Option<Duration> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

fn metadata_error_is_command_contention(error: &MetadataError) -> bool {
    matches!(
        error,
        MetadataError::ObjectGenerationReservationConflict { .. }
            | MetadataError::ObjectVersionReservationConflict { .. }
            | MetadataError::BucketWriteReservationConflict { .. }
            | MetadataError::BucketWriteReservationNotFound { .. }
            | MetadataError::StaleBucketMetadataCommand { .. }
            | MetadataError::StaleObjectWriteCommand { .. }
    )
}

fn store_error_is_retryable(error: &StoreError) -> bool {
    if error.storage_node_failure_class().is_some_and(|failure| {
        matches!(
            failure,
            crate::StorageNodeFailureClass::ShardLocationStale
                | crate::StorageNodeFailureClass::PgRouteUnavailable
                | crate::StorageNodeFailureClass::MetadataCommandContention
                | crate::StorageNodeFailureClass::TransportInterrupted
        )
    }) {
        return true;
    }
    match error {
        StoreError::MetadataCommandContention { .. }
        | StoreError::MetadataCommandLogConflict { .. }
        | StoreError::MetadataCommandLogGap { .. }
        | StoreError::MetadataCommandPendingConflict { .. }
        | StoreError::StalePayloadOperation { .. }
        | StoreError::StaleMetadataCommand { .. }
        | StoreError::StaleMetadataPrimaryBridge { .. }
        | StoreError::StaleMetadataOperation { .. }
        | StoreError::StaleMetadataRoute { .. }
        | StoreError::StaleMetadataReadProof { .. }
        | StoreError::RouteMapExpired { .. }
        | StoreError::RouteAdmissionClusterMismatch { .. }
        | StoreError::StaleShardOperation { .. }
        | StoreError::StaleShardLocation { .. }
        | StoreError::PgNotActive { .. }
        | StoreError::ShardPgNotActive { .. }
        | StoreError::StorageRpcResourceExhausted { .. } => true,
        StoreError::ShardStore { source, .. } => store_error_is_retryable(source),
        _ => false,
    }
}

fn object_payload_reclaim_worker_should_defer(
    result: &Result<ObjectPayloadReclaimAttempt, ObjectPgActionError>,
) -> bool {
    match result {
        Ok(ObjectPayloadReclaimAttempt::Deferred) => true,
        Ok(ObjectPayloadReclaimAttempt::Completed | ObjectPayloadReclaimAttempt::MissingRoot) => {
            false
        }
        Err(ObjectPgActionError::Store(error)) => store_error_is_retryable(error),
        Err(ObjectPgActionError::Metadata(error)) => metadata_error_is_command_contention(error),
        Err(
            ObjectPgActionError::InvalidRequest { .. }
            | ObjectPgActionError::StaleObjectReadSubject
            | ObjectPgActionError::StaleDirectPutCommitSnapshot
            | ObjectPgActionError::StaleStreamFinalizeSnapshot
            | ObjectPgActionError::StaleMultipartCompletionSnapshot
            | ObjectPgActionError::MultipartConditionalRequestConflict,
        ) => false,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BucketDeleteFinalizeWorkerDisposition {
    Finish,
    RetryAfter(Duration),
}

fn bucket_write_error_is_retryable(error: &BucketWriteDrainError) -> bool {
    match error {
        BucketWriteDrainError::Store(error) => store_error_is_retryable(error),
        BucketWriteDrainError::Metadata(error) => metadata_error_is_command_contention(error),
    }
}

fn bucket_delete_finalize_worker_disposition(
    result: &Result<BucketDeleteFinalizeOutcome, BucketWriteDrainError>,
) -> BucketDeleteFinalizeWorkerDisposition {
    match result {
        Ok(outcome) if outcome.is_terminal() => BucketDeleteFinalizeWorkerDisposition::Finish,
        Ok(BucketDeleteFinalizeOutcome::Pending) => {
            BucketDeleteFinalizeWorkerDisposition::RetryAfter(BUCKET_DELETE_FINALIZE_RETRY_COOLDOWN)
        }
        Err(error) if bucket_write_error_is_retryable(error) => {
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
    match storage_node.head_bucket_info(root.bucket()) {
        Ok(info) => {
            info.bucket_execution_generation != root.bucket_execution_generation()
                || info.bucket_incarnation_generation != root.bucket_incarnation_generation()
        }
        Err(error)
            if matches!(
                error.kind(),
                crate::BucketSnapshotLoadFailureKind::BucketNotFound { .. }
            ) =>
        {
            true
        }
        Err(_) => false,
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
        batch: DurableReclaimScanBatch,
        scan_completed_at: Instant,
        clean_pass_delay: Duration,
    ) {
        self.next_pg_id = batch.next_pg_id;
        self.retry_pass_required |= batch.retry_pass_required;
        match batch.outcome {
            DurableReclaimScanOutcome::RouteRefreshRequired => {
                self.next_scan_at = scan_completed_at + RECLAIM_DURABLE_SCAN_ROUTE_REFRESH_BACKOFF;
            }
            DurableReclaimScanOutcome::Complete if batch.next_pg_id.is_some() => {
                self.next_scan_at = scan_completed_at;
            }
            DurableReclaimScanOutcome::Complete => {
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
    admission: &Arc<StorageMaintenanceAdmission>,
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
    let Some(_scan_permit) = admission.try_reclaim_cleanup() else {
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
    #[cfg(feature = "test-hooks")]
    let clean_pass_delay = reclaim_worker_test_hooks(storage_node.process_local_registry_key())
        .durable_scan_delay_override
        .unwrap_or(RECLAIM_DURABLE_SCAN_SAFETY_INTERVAL);
    #[cfg(not(feature = "test-hooks"))]
    let clean_pass_delay = RECLAIM_DURABLE_SCAN_SAFETY_INTERVAL;
    schedule.record_batch(batch, scan_completed_at, clean_pass_delay);
}

/// Opaque storage-owned worker for durable payload reclaim and accepted bucket deletion.
pub struct StorageReclaimSweeper {
    storage_handle: StorageClusterRouteHandle,
    stop: Arc<AtomicBool>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl StorageReclaimSweeper {
    pub fn acquire_shared(
        storage_handle: &StorageClusterRouteHandle,
    ) -> Result<Arc<Self>, StorageMaintenanceStartError> {
        let registry = RECLAIM_SWEEPER_REGISTRY.get_or_init(|| Mutex::new(Vec::new()));
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
        let worker_stop = Arc::clone(&stop);
        let sweeper = Arc::new(Self {
            storage_handle: storage_handle.clone(),
            stop,
            handle: Mutex::new(None),
        });
        let handle = std::thread::Builder::new()
            .name("argmin-reclaim".to_string())
            .spawn(move || run_reclaim_worker(storage_handle, admission, worker_stop))
            .map_err(|error| StorageMaintenanceStartError::worker_spawn("reclaim", error))?;
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
    pub(crate) fn test_stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
        self.storage_handle.current().wake_reclaim_workers();
        if let Some(handle) = self
            .handle
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            let _ = handle.join();
        }
    }

    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub(crate) fn test_routes_to(&self, expected: &Arc<StorageCluster>) -> bool {
        Arc::ptr_eq(&self.storage_handle.current(), expected)
    }
}

impl Drop for StorageReclaimSweeper {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        self.storage_handle.current().wake_reclaim_workers();
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

fn run_reclaim_worker(
    storage_handle: StorageClusterRouteHandle,
    admission: Arc<StorageMaintenanceAdmission>,
    stop: Arc<AtomicBool>,
) {
    let mut object_retry_after: HashMap<u32, Instant> = HashMap::new();
    let mut deferred_objects: VecDeque<DeferredReclaimWork<ObjectPayloadReclaimRoot>> =
        VecDeque::new();
    let mut deferred_object_roots: HashSet<ObjectPayloadReclaimRoot> = HashSet::new();
    let mut begin_retry_after: HashMap<BucketDeleteBeginRoot, Instant> = HashMap::new();
    let mut deferred_begins: VecDeque<BucketDeleteBeginRoot> = VecDeque::new();
    let mut deferred_begin_roots: HashSet<BucketDeleteBeginRoot> = HashSet::new();
    let mut finalize_retry_after: HashMap<BucketDeleteFinalizeRoot, Instant> = HashMap::new();
    let mut deferred_finalizers: VecDeque<DeferredReclaimWork<BucketDeleteFinalizeRoot>> =
        VecDeque::new();
    let mut deferred_finalizer_roots: HashSet<BucketDeleteFinalizeRoot> = HashSet::new();
    let mut next_deferred_class = DeferredReclaimClass::ObjectPayload;
    let mut scan_schedule = DurableReclaimScanSchedule::immediate();
    let mut pending_work: Option<(Arc<StorageCluster>, ReclaimWorkItem)> = None;

    while !stop.load(Ordering::SeqCst) {
        let current = storage_handle.current();
        enqueue_durable_reclaim_work_if_due(
            &current,
            &admission,
            &deferred_object_roots,
            &deferred_begin_roots,
            &deferred_finalizer_roots,
            &mut scan_schedule,
        );
        let Some((queue_owner, work)) = pending_work
            .take()
            .or_else(|| {
                current
                    .try_take_reclaim_work()
                    .map(|work| (Arc::clone(&current), work))
            })
            .or_else(|| {
                if deferred_objects.is_empty()
                    && deferred_begins.is_empty()
                    && deferred_finalizers.is_empty()
                {
                    return None;
                }
                enqueue_durable_reclaim_work_if_due(
                    &current,
                    &admission,
                    &deferred_object_roots,
                    &deferred_begin_roots,
                    &deferred_finalizer_roots,
                    &mut scan_schedule,
                );
                current
                    .try_take_reclaim_work()
                    .map(|work| (Arc::clone(&current), work))
                    .or_else(|| {
                        let class = select_deferred_reclaim_class(
                            &mut next_deferred_class,
                            !deferred_objects.is_empty(),
                            !deferred_begins.is_empty(),
                            !deferred_finalizers.is_empty(),
                        )?;
                        Some(match class {
                            DeferredReclaimClass::ObjectPayload => {
                                let deferred = deferred_objects
                                    .pop_front()
                                    .expect("selected deferred object reclaim");
                                deferred_object_roots.remove(&deferred.root);
                                (
                                    deferred.queue_owner,
                                    ReclaimWorkItem::ObjectPayload(deferred.root),
                                )
                            }
                            DeferredReclaimClass::BucketDeleteBegin => {
                                let root = deferred_begins
                                    .pop_front()
                                    .expect("selected deferred bucket-delete begin");
                                deferred_begin_roots.remove(&root);
                                (
                                    Arc::clone(&current),
                                    ReclaimWorkItem::BucketDeleteBegin(root),
                                )
                            }
                            DeferredReclaimClass::BucketDeleteFinalize => {
                                let deferred = deferred_finalizers
                                    .pop_front()
                                    .expect("selected deferred bucket finalizer");
                                deferred_finalizer_roots.remove(&deferred.root);
                                (
                                    deferred.queue_owner,
                                    ReclaimWorkItem::BucketDelete(deferred.root),
                                )
                            }
                        })
                    })
            })
            .or_else(|| wait_for_runtime_map_reclaim_work(&storage_handle, &stop))
        else {
            #[cfg(feature = "test-hooks")]
            if !stop.load(Ordering::SeqCst) {
                if let Some(hook) = reclaim_worker_test_hooks(current.process_local_registry_key())
                    .after_idle_return
                {
                    hook();
                }
            }
            continue;
        };

        #[cfg(feature = "test-hooks")]
        if let Some(hook) =
            reclaim_worker_test_hooks(queue_owner.process_local_registry_key()).after_work_dequeued
        {
            hook();
        }
        let execution_node = storage_handle.current();
        #[cfg(feature = "test-hooks")]
        if let Some(hook) =
            reclaim_worker_test_hooks(queue_owner.process_local_registry_key()).before_work_execute
        {
            hook(Arc::clone(&execution_node));
        }
        let Some(_cleanup_permit) = admission.try_reclaim_cleanup() else {
            pending_work = Some((queue_owner, work));
            std::thread::sleep(OBJECT_PAYLOAD_RECLAIM_PG_RETRY_COOLDOWN);
            continue;
        };

        match work {
            ReclaimWorkItem::ObjectPayload((bucket, key, generation_id)) => {
                let root = (bucket, key, generation_id);
                if deferred_object_roots.contains(&root) {
                    if deferred_objects
                        .iter()
                        .find(|deferred| deferred.root == root)
                        .is_some_and(|deferred| !Arc::ptr_eq(&deferred.queue_owner, &queue_owner))
                    {
                        queue_owner.finish_object_payload_reclaim_work(&root.0, &root.1, root.2);
                    }
                    continue;
                }
                let pg_id = execution_node.object_payload_reclaim_pg_id(&root.0, &root.1);
                if object_retry_after
                    .get(&pg_id)
                    .is_some_and(|retry_after| *retry_after > Instant::now())
                {
                    defer_object_payload_reclaim(
                        &mut deferred_objects,
                        &mut deferred_object_roots,
                        Arc::clone(&queue_owner),
                        root,
                    );
                } else {
                    let result = execution_node
                        .reclaim_object_payload_if_unleased_with_outcome(&root.0, &root.1, root.2);
                    if object_payload_reclaim_worker_should_defer(&result) {
                        object_retry_after.insert(
                            pg_id,
                            Instant::now() + OBJECT_PAYLOAD_RECLAIM_PG_RETRY_COOLDOWN,
                        );
                        defer_object_payload_reclaim(
                            &mut deferred_objects,
                            &mut deferred_object_roots,
                            Arc::clone(&queue_owner),
                            root,
                        );
                    } else {
                        queue_owner.finish_object_payload_reclaim_work(&root.0, &root.1, root.2);
                        object_retry_after.remove(&pg_id);
                    }
                }
            }
            ReclaimWorkItem::BucketDelete(root) => {
                if deferred_finalizer_roots.contains(&root) {
                    if deferred_finalizers
                        .iter()
                        .find(|deferred| deferred.root == root)
                        .is_some_and(|deferred| !Arc::ptr_eq(&deferred.queue_owner, &queue_owner))
                    {
                        queue_owner.finish_bucket_delete_finalize_work(&root);
                    }
                    continue;
                }
                if finalize_retry_after
                    .get(&root)
                    .is_some_and(|retry_after| *retry_after > Instant::now())
                {
                    defer_bucket_delete_finalize(
                        &mut deferred_finalizers,
                        &mut deferred_finalizer_roots,
                        Arc::clone(&queue_owner),
                        root,
                    );
                } else {
                    let result = execution_node.try_finalize_bucket_delete_root(&root);
                    let _ = observability::event(
                        TRACE_TARGET,
                        "bucket_delete_finalize_worker_result",
                        Some(format_args!("root={root:?} result={result:?}")),
                    );
                    match bucket_delete_finalize_worker_disposition(&result) {
                        BucketDeleteFinalizeWorkerDisposition::Finish => {
                            queue_owner.finish_bucket_delete_finalize_work(&root);
                            finalize_retry_after.remove(&root);
                            begin_retry_after.retain(|begin, _| {
                                begin.bucket() != &root.bucket
                                    || begin.bucket_incarnation_generation()
                                        != root.bucket_incarnation_generation
                            });
                            deferred_begin_roots.retain(|begin| {
                                begin.bucket() != &root.bucket
                                    || begin.bucket_incarnation_generation()
                                        != root.bucket_incarnation_generation
                            });
                            deferred_begins.retain(|begin| {
                                begin.bucket() != &root.bucket
                                    || begin.bucket_incarnation_generation()
                                        != root.bucket_incarnation_generation
                            });
                        }
                        BucketDeleteFinalizeWorkerDisposition::RetryAfter(delay) => {
                            finalize_retry_after.insert(root.clone(), Instant::now() + delay);
                            defer_bucket_delete_finalize(
                                &mut deferred_finalizers,
                                &mut deferred_finalizer_roots,
                                Arc::clone(&queue_owner),
                                root,
                            );
                        }
                    }
                }
            }
            ReclaimWorkItem::BucketDeleteBegin(root) => {
                if deferred_begin_roots.contains(&root) {
                    continue;
                }
                if begin_retry_after
                    .get(&root)
                    .is_some_and(|retry_after| *retry_after > Instant::now())
                {
                    defer_bucket_delete_begin(
                        &mut deferred_begins,
                        &mut deferred_begin_roots,
                        root,
                    );
                } else {
                    let result = execution_node.continue_adopted_bucket_delete(&root);
                    let _ = observability::event(
                        TRACE_TARGET,
                        "bucket_delete_begin_worker_result",
                        Some(format_args!("root={root:?} result={result:?}")),
                    );
                    match result {
                        Ok(()) => {
                            begin_retry_after.remove(&root);
                            queue_owner.enqueue_bucket_delete_finalize(BucketDeleteFinalizeRoot {
                                bucket: root.bucket().clone(),
                                bucket_incarnation_generation: root.bucket_incarnation_generation(),
                            });
                        }
                        Err(error) => {
                            if bucket_delete_begin_root_is_stale(&execution_node, &root) {
                                begin_retry_after.remove(&root);
                            } else if bucket_write_error_is_retryable(&error) {
                                begin_retry_after.insert(
                                    root.clone(),
                                    Instant::now() + BUCKET_DELETE_BEGIN_RETRY_COOLDOWN,
                                );
                                defer_bucket_delete_begin(
                                    &mut deferred_begins,
                                    &mut deferred_begin_roots,
                                    root,
                                );
                            } else {
                                begin_retry_after.remove(&root);
                            }
                        }
                    }
                }
            }
        }

        if pending_work.is_none()
            && (!deferred_objects.is_empty()
                || !deferred_begins.is_empty()
                || !deferred_finalizers.is_empty())
        {
            enqueue_durable_reclaim_work_if_due(
                &execution_node,
                &admission,
                &deferred_object_roots,
                &deferred_begin_roots,
                &deferred_finalizer_roots,
                &mut scan_schedule,
            );
            if let Some(work) = execution_node.try_take_reclaim_work() {
                pending_work = Some((Arc::clone(&execution_node), work));
            } else if let Some(sleep_for) = shortest_retry_sleep(
                shortest_retry_sleep(
                    earliest_object_payload_reclaim_retry_sleep(
                        &execution_node,
                        &deferred_objects,
                        &object_retry_after,
                    ),
                    earliest_bucket_delete_begin_retry_sleep(&deferred_begins, &begin_retry_after),
                ),
                earliest_bucket_delete_finalize_retry_sleep(
                    deferred_finalizers.iter().map(|deferred| &deferred.root),
                    &finalize_retry_after,
                ),
            ) {
                std::thread::sleep(sleep_for);
            }
        }
    }
}

fn wait_for_runtime_map_reclaim_work(
    storage_handle: &StorageClusterRouteHandle,
    stop: &AtomicBool,
) -> Option<(Arc<StorageCluster>, ReclaimWorkItem)> {
    if stop.load(Ordering::SeqCst) {
        return None;
    }
    let worker_node = storage_handle.current();
    let work = worker_node.wait_for_queued_reclaim_work_poll(stop)?;
    Some((worker_node, work))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_bucket_name(name: &str) -> BucketName {
        BucketName::try_from(name.to_string()).expect("test bucket name must be valid")
    }

    #[test]
    fn durable_scan_schedule_preserves_cursor_and_uses_bounded_delays() {
        let completed_at = Instant::now();
        let mut schedule = DurableReclaimScanSchedule::immediate();
        schedule.record_batch(
            DurableReclaimScanBatch {
                outcome: DurableReclaimScanOutcome::Complete,
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
            DurableReclaimScanBatch {
                outcome: DurableReclaimScanOutcome::RouteRefreshRequired,
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
            DurableReclaimScanBatch {
                outcome: DurableReclaimScanOutcome::Complete,
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
            DurableReclaimScanBatch {
                outcome: DurableReclaimScanOutcome::Complete,
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
            bucket: test_bucket_name("cooled-finalizer"),
            bucket_incarnation_generation: 1,
        };
        let deferred_work = VecDeque::from([root.clone()]);
        let mut retry_after_by_root = HashMap::from([(
            root.clone(),
            Instant::now() + BUCKET_DELETE_FINALIZE_RETRY_COOLDOWN,
        )]);

        let sleep_for =
            earliest_bucket_delete_finalize_retry_sleep(deferred_work.iter(), &retry_after_by_root)
                .expect("cooled finalizer root should produce a retry sleep");
        assert!(
            sleep_for <= BUCKET_DELETE_FINALIZE_RETRY_COOLDOWN,
            "retry sleep should be capped by the finalizer cooldown, got {sleep_for:?}"
        );

        retry_after_by_root.insert(root, Instant::now() - Duration::from_millis(1));
        assert_eq!(
            earliest_bucket_delete_finalize_retry_sleep(deferred_work.iter(), &retry_after_by_root),
            None,
            "ready finalizer root should not sleep"
        );
    }

    #[test]
    fn bucket_delete_finalize_worker_retries_unexpected_errors() {
        let result = Err(BucketWriteDrainError::Store(StoreError::NotFound));

        assert_eq!(
            bucket_delete_finalize_worker_disposition(&result),
            BucketDeleteFinalizeWorkerDisposition::RetryAfter(
                BUCKET_DELETE_FINALIZE_ERROR_RETRY_COOLDOWN
            ),
            "an error after durable bucket deletion must remain retryable so a later terminal outcome can clear the exact outstanding root"
        );
    }

    #[test]
    fn deferred_reclaim_selection_rotates_between_nonempty_classes() {
        let mut next = DeferredReclaimClass::ObjectPayload;
        assert_eq!(
            select_deferred_reclaim_class(&mut next, true, false, true),
            Some(DeferredReclaimClass::ObjectPayload)
        );
        assert_eq!(
            select_deferred_reclaim_class(&mut next, true, false, true),
            Some(DeferredReclaimClass::BucketDeleteFinalize)
        );
        assert_eq!(
            select_deferred_reclaim_class(&mut next, true, true, true),
            Some(DeferredReclaimClass::ObjectPayload)
        );
        assert_eq!(
            select_deferred_reclaim_class(&mut next, true, true, true),
            Some(DeferredReclaimClass::BucketDeleteBegin)
        );
        assert_eq!(
            select_deferred_reclaim_class(&mut next, true, true, true),
            Some(DeferredReclaimClass::BucketDeleteFinalize)
        );
    }

    #[test]
    fn missing_object_reclaim_root_finishes_queue_work() {
        assert!(!object_payload_reclaim_worker_should_defer(&Ok(
            ObjectPayloadReclaimAttempt::MissingRoot
        )));
    }
}
