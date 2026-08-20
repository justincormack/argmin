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
    ProcessLocalRegistryKey, ReclaimWorkItem, StorageCluster, StoreError,
};

use super::{StorageMaintenanceAdmission, StorageMaintenanceStartError, TRACE_TARGET};

const OBJECT_PAYLOAD_RECLAIM_PG_RETRY_COOLDOWN: Duration = Duration::from_millis(100);
const BUCKET_DELETE_BEGIN_RETRY_COOLDOWN: Duration = Duration::from_millis(100);
const BUCKET_DELETE_FINALIZE_RETRY_COOLDOWN: Duration = Duration::from_millis(100);
const BUCKET_DELETE_FINALIZE_ERROR_RETRY_COOLDOWN: Duration = Duration::from_secs(1);
const BUCKET_DELETE_FINALIZE_CONTINUATION_BURST: usize = 4;
const RECLAIM_DURABLE_SCAN_BATCH_PGS: usize = 8;
const RECLAIM_DURABLE_SCAN_SAFETY_INTERVAL: Duration = Duration::from_secs(60);
const RECLAIM_DURABLE_SCAN_ROUTE_REFRESH_BACKOFF: Duration = Duration::from_secs(1);
const RECLAIM_DURABLE_SCAN_INCOMPLETE_RETRY: Duration = Duration::from_secs(1);
const RECLAIM_WORKER_MAX_PARALLELISM: usize = 8;
// Outstanding delete roots retain durable drains, so bound accepted roots while
// leaving each worker enough queued work to hide retry and scan latency.
const BUCKET_DELETE_FINALIZE_OUTSTANDING_PER_WORKER: usize = 4;

type ObjectPayloadReclaimRoot = (BucketName, ObjectKey, GenerationId);

pub(crate) fn reclaim_worker_parallelism(storage: &StorageCluster) -> usize {
    std::thread::available_parallelism()
        .map_or(1, usize::from)
        .min(RECLAIM_WORKER_MAX_PARALLELISM)
        .min(storage.metadata_pg_ids().len().max(1))
}

pub(crate) fn bucket_delete_finalize_admission_capacity(storage: &StorageCluster) -> usize {
    reclaim_worker_parallelism(storage) * BUCKET_DELETE_FINALIZE_OUTSTANDING_PER_WORKER
}

static RECLAIM_SWEEPER_REGISTRY: OnceLock<Mutex<Vec<Weak<StorageReclaimSweeper>>>> =
    OnceLock::new();

#[cfg(feature = "test-hooks")]
#[derive(Default, Clone)]
pub struct StorageReclaimWorkerTestHooks {
    pub target_registry_key: Option<crate::ProcessLocalRegistryKey>,
    pub worker_parallelism_override: Option<usize>,
    pub durable_scan_delay_override: Option<Duration>,
    pub after_idle_return: Option<Arc<dyn Fn() + Send + Sync>>,
    pub after_work_dequeued: Option<Arc<dyn Fn() + Send + Sync>>,
    pub before_work_execute: Option<Arc<dyn Fn(Arc<StorageCluster>) + Send + Sync>>,
    pub after_work_deferred: Option<Arc<dyn Fn() + Send + Sync>>,
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

#[cfg(feature = "test-hooks")]
fn notify_reclaim_work_deferred(queue_owner: &StorageCluster) {
    if let Some(hook) =
        reclaim_worker_test_hooks(queue_owner.process_local_registry_key()).after_work_deferred
    {
        hook();
    }
}

#[cfg(not(feature = "test-hooks"))]
fn notify_reclaim_work_deferred(_queue_owner: &StorageCluster) {}

struct DeferredReclaimWork<T> {
    queue_owner: Arc<StorageCluster>,
    root: T,
}

#[derive(Default)]
struct ActiveReclaimRoots {
    objects: HashMap<ObjectPayloadReclaimRoot, ProcessLocalRegistryKey>,
    begins: HashMap<BucketDeleteBeginRoot, ProcessLocalRegistryKey>,
    finalizers: HashMap<BucketDeleteFinalizeRoot, ProcessLocalRegistryKey>,
}

impl ActiveReclaimRoots {
    fn try_acquire(
        &mut self,
        queue_owner: &StorageCluster,
        work: &ReclaimWorkItem,
    ) -> Result<(), ProcessLocalRegistryKey> {
        let owner = queue_owner.process_local_registry_key();
        let existing = match work {
            ReclaimWorkItem::ObjectPayload(root) => self.objects.get(root).copied(),
            ReclaimWorkItem::BucketDeleteBegin(root) => self.begins.get(root).copied(),
            ReclaimWorkItem::BucketDelete(root) => self.finalizers.get(root).copied(),
        };
        if let Some(existing) = existing {
            return Err(existing);
        }
        match work {
            ReclaimWorkItem::ObjectPayload(root) => {
                self.objects.insert(root.clone(), owner);
            }
            ReclaimWorkItem::BucketDeleteBegin(root) => {
                self.begins.insert(root.clone(), owner);
            }
            ReclaimWorkItem::BucketDelete(root) => {
                self.finalizers.insert(root.clone(), owner);
            }
        }
        Ok(())
    }

    fn release(&mut self, work: &ReclaimWorkItem) {
        match work {
            ReclaimWorkItem::ObjectPayload(root) => {
                self.objects.remove(root);
            }
            ReclaimWorkItem::BucketDeleteBegin(root) => {
                self.begins.remove(root);
            }
            ReclaimWorkItem::BucketDelete(root) => {
                self.finalizers.remove(root);
            }
        }
    }

    fn exclusions(
        &self,
    ) -> (
        HashSet<ObjectPayloadReclaimRoot>,
        HashSet<BucketDeleteBeginRoot>,
        HashSet<BucketDeleteFinalizeRoot>,
    ) {
        (
            self.objects.keys().cloned().collect(),
            self.begins.keys().cloned().collect(),
            self.finalizers.keys().cloned().collect(),
        )
    }
}

struct DeferredReclaimQueues {
    objects: VecDeque<DeferredReclaimWork<ObjectPayloadReclaimRoot>>,
    object_roots: HashSet<ObjectPayloadReclaimRoot>,
    begins: VecDeque<DeferredReclaimWork<BucketDeleteBeginRoot>>,
    begin_roots: HashSet<BucketDeleteBeginRoot>,
    finalizers: VecDeque<DeferredReclaimWork<BucketDeleteFinalizeRoot>>,
    finalizer_roots: HashSet<BucketDeleteFinalizeRoot>,
    next_class: DeferredReclaimClass,
}

impl DeferredReclaimQueues {
    fn new() -> Self {
        Self {
            objects: VecDeque::new(),
            object_roots: HashSet::new(),
            begins: VecDeque::new(),
            begin_roots: HashSet::new(),
            finalizers: VecDeque::new(),
            finalizer_roots: HashSet::new(),
            next_class: DeferredReclaimClass::ObjectPayload,
        }
    }

    fn is_empty(&self) -> bool {
        self.objects.is_empty() && self.begins.is_empty() && self.finalizers.is_empty()
    }

    fn take(
        &mut self,
        current: &Arc<StorageCluster>,
        object_retry_after: &HashMap<u32, Instant>,
        begin_retry_after: &HashMap<BucketDeleteBeginRoot, Instant>,
        finalize_retry_after: &HashMap<BucketDeleteFinalizeRoot, Instant>,
    ) -> Option<(Arc<StorageCluster>, ReclaimWorkItem)> {
        let now = Instant::now();
        let object_position = self.objects.iter().position(|deferred| {
            let pg_id = current.object_payload_reclaim_pg_id(&deferred.root.0, &deferred.root.1);
            object_retry_after
                .get(&pg_id)
                .is_none_or(|retry_after| *retry_after <= now)
        });
        let begin_position = self.begins.iter().position(|root| {
            begin_retry_after
                .get(&root.root)
                .is_none_or(|retry_after| *retry_after <= now)
        });
        let finalizer_position = self.finalizers.iter().position(|deferred| {
            finalize_retry_after
                .get(&deferred.root)
                .is_none_or(|retry_after| *retry_after <= now)
        });
        let class = select_deferred_reclaim_class(
            &mut self.next_class,
            object_position.is_some(),
            begin_position.is_some(),
            finalizer_position.is_some(),
        )?;
        Some(match class {
            DeferredReclaimClass::ObjectPayload => {
                let deferred = self
                    .objects
                    .remove(object_position.expect("selected ready deferred object reclaim"))
                    .expect("selected deferred object reclaim");
                self.object_roots.remove(&deferred.root);
                (
                    deferred.queue_owner,
                    ReclaimWorkItem::ObjectPayload(deferred.root),
                )
            }
            DeferredReclaimClass::BucketDeleteBegin => {
                let deferred = self
                    .begins
                    .remove(begin_position.expect("selected ready deferred bucket-delete begin"))
                    .expect("selected deferred bucket-delete begin");
                self.begin_roots.remove(&deferred.root);
                (
                    deferred.queue_owner,
                    ReclaimWorkItem::BucketDeleteBegin(deferred.root),
                )
            }
            DeferredReclaimClass::BucketDeleteFinalize => {
                let deferred = self
                    .finalizers
                    .remove(finalizer_position.expect("selected ready deferred bucket finalizer"))
                    .expect("selected deferred bucket finalizer");
                self.finalizer_roots.remove(&deferred.root);
                (
                    deferred.queue_owner,
                    ReclaimWorkItem::BucketDelete(deferred.root),
                )
            }
        })
    }
}

#[derive(Default)]
struct ObjectPayloadReclaimRetrySchedule {
    retry_after_by_pg: HashMap<u32, Instant>,
}

impl ObjectPayloadReclaimRetrySchedule {
    fn observe(&self, pg_id: u32) -> Option<Instant> {
        self.retry_after_by_pg.get(&pg_id).copied()
    }

    fn defer_until(&mut self, pg_id: u32, retry_after: Instant) {
        self.retry_after_by_pg.insert(pg_id, retry_after);
    }

    fn clear_observed(&mut self, pg_id: u32, observed_retry_after: Option<Instant>) -> bool {
        if self.observe(pg_id) == observed_retry_after {
            self.retry_after_by_pg.remove(&pg_id);
            false
        } else {
            self.retry_after_by_pg.contains_key(&pg_id)
        }
    }

    fn deadlines(&self) -> &HashMap<u32, Instant> {
        &self.retry_after_by_pg
    }
}

struct ReclaimWorkerSchedule {
    object_retries: ObjectPayloadReclaimRetrySchedule,
    begin_retry_after: HashMap<BucketDeleteBeginRoot, Instant>,
    finalize_retry_after: HashMap<BucketDeleteFinalizeRoot, Instant>,
    deferred: DeferredReclaimQueues,
    next_immediate_source: ImmediateReclaimSource,
    finalizer_continuation_bursts: HashMap<BucketDeleteFinalizeRoot, usize>,
}

impl ReclaimWorkerSchedule {
    fn new() -> Self {
        Self {
            object_retries: ObjectPayloadReclaimRetrySchedule::default(),
            begin_retry_after: HashMap::new(),
            finalize_retry_after: HashMap::new(),
            deferred: DeferredReclaimQueues::new(),
            next_immediate_source: ImmediateReclaimSource::Queued,
            finalizer_continuation_bursts: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ImmediateReclaimSource {
    Queued,
    Deferred,
}

impl ImmediateReclaimSource {
    fn next(self) -> Self {
        match self {
            Self::Queued => Self::Deferred,
            Self::Deferred => Self::Queued,
        }
    }
}

fn take_immediate_reclaim_work(
    current: &Arc<StorageCluster>,
    deferred: &mut DeferredReclaimQueues,
    next_source: &mut ImmediateReclaimSource,
    object_retry_after: &HashMap<u32, Instant>,
    begin_retry_after: &HashMap<BucketDeleteBeginRoot, Instant>,
    finalize_retry_after: &HashMap<BucketDeleteFinalizeRoot, Instant>,
) -> Option<(Arc<StorageCluster>, ReclaimWorkItem, bool)> {
    for source in [*next_source, (*next_source).next()] {
        let selected = match source {
            ImmediateReclaimSource::Queued => current
                .try_take_reclaim_work()
                .map(|work| (Arc::clone(current), work, false)),
            ImmediateReclaimSource::Deferred => deferred
                .take(
                    current,
                    object_retry_after,
                    begin_retry_after,
                    finalize_retry_after,
                )
                .map(|(owner, work)| (owner, work, true)),
        };
        if selected.is_some() {
            *next_source = source.next();
            return selected;
        }
    }
    None
}

fn take_scheduled_reclaim_work(
    current: &Arc<StorageCluster>,
    schedule: &Mutex<ReclaimWorkerSchedule>,
) -> Option<(Arc<StorageCluster>, ReclaimWorkItem, bool)> {
    let mut schedule = schedule.lock().unwrap_or_else(|error| error.into_inner());
    let ReclaimWorkerSchedule {
        object_retries,
        begin_retry_after,
        finalize_retry_after,
        deferred,
        next_immediate_source,
        ..
    } = &mut *schedule;
    take_immediate_reclaim_work(
        current,
        deferred,
        next_immediate_source,
        object_retries.deadlines(),
        begin_retry_after,
        finalize_retry_after,
    )
}

fn enqueue_durable_reclaim_work_with_active_exclusions_if_due(
    storage_node: &Arc<StorageCluster>,
    admission: &Arc<StorageMaintenanceAdmission>,
    active_roots: &Mutex<ActiveReclaimRoots>,
    schedule: &mut DurableReclaimScanSchedule,
) {
    let (objects, begins, finalizers) = active_roots
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .exclusions();
    enqueue_durable_reclaim_work_if_due(
        storage_node,
        admission,
        &objects,
        &begins,
        &finalizers,
        schedule,
    );
}

fn acknowledge_duplicate_reclaim_hint(
    queue_owner: &StorageCluster,
    work: &ReclaimWorkItem,
    active_owner: ProcessLocalRegistryKey,
) {
    if queue_owner.process_local_registry_key() == active_owner {
        return;
    }
    match work {
        ReclaimWorkItem::ObjectPayload((bucket, key, generation_id)) => {
            queue_owner.finish_object_payload_reclaim_work(bucket, key, *generation_id);
        }
        ReclaimWorkItem::BucketDelete(root) => {
            queue_owner.finish_bucket_delete_finalize_work(root);
        }
        ReclaimWorkItem::BucketDeleteBegin(root) => {
            queue_owner.finish_bucket_delete_begin_work(root);
        }
    }
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
    deferred_work: &mut VecDeque<DeferredReclaimWork<BucketDeleteBeginRoot>>,
    deferred_roots: &mut HashSet<BucketDeleteBeginRoot>,
    queue_owner: Arc<StorageCluster>,
    root: BucketDeleteBeginRoot,
) {
    if deferred_roots.insert(root.clone()) {
        deferred_work.push_back(DeferredReclaimWork { queue_owner, root });
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
    deferred_work: &VecDeque<DeferredReclaimWork<BucketDeleteBeginRoot>>,
    retry_after_by_root: &HashMap<BucketDeleteBeginRoot, Instant>,
) -> Option<Duration> {
    let now = Instant::now();
    let mut earliest_retry: Option<Instant> = None;
    for deferred in deferred_work {
        let retry_after = retry_after_by_root.get(&deferred.root)?;
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
            | ObjectPgActionError::SnapshotReinspectionConflict
            | ObjectPgActionError::StaleMultipartCompletionSnapshot
            | ObjectPgActionError::MultipartConditionalRequestConflict
            | ObjectPgActionError::MultipartPrepublicationBarrierExhausted,
        ) => false,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BucketDeleteFinalizeWorkerDisposition {
    Finish,
    Continue,
    RetryAfter(Duration),
}

fn bucket_write_error_is_retryable(error: &BucketWriteDrainError) -> bool {
    match error {
        BucketWriteDrainError::Store(error) => store_error_is_retryable(error),
        BucketWriteDrainError::Metadata(error) => metadata_error_is_command_contention(error),
        BucketWriteDrainError::BucketDeleteFinalizeBackpressure => true,
    }
}

fn bucket_delete_finalize_worker_disposition(
    result: &Result<BucketDeleteFinalizeOutcome, BucketWriteDrainError>,
) -> BucketDeleteFinalizeWorkerDisposition {
    match result {
        Ok(outcome) if outcome.is_terminal() => BucketDeleteFinalizeWorkerDisposition::Finish,
        Ok(BucketDeleteFinalizeOutcome::Continue) => {
            BucketDeleteFinalizeWorkerDisposition::Continue
        }
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
    handles: Mutex<Vec<JoinHandle<()>>>,
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
        let worker_count = {
            let default = reclaim_worker_parallelism(&storage_handle.current());
            #[cfg(feature = "test-hooks")]
            {
                reclaim_worker_test_hooks(storage_handle.current().process_local_registry_key())
                    .worker_parallelism_override
                    .unwrap_or(default)
                    .clamp(1, RECLAIM_WORKER_MAX_PARALLELISM)
            }
            #[cfg(not(feature = "test-hooks"))]
            {
                default
            }
        };
        let active_roots = Arc::new(Mutex::new(ActiveReclaimRoots::default()));
        let schedule = Arc::new(Mutex::new(ReclaimWorkerSchedule::new()));
        let sweeper = Arc::new(Self {
            storage_handle: storage_handle.clone(),
            stop,
            handles: Mutex::new(Vec::new()),
        });
        let mut handles: Vec<JoinHandle<()>> = Vec::with_capacity(worker_count);
        for worker_index in 0..worker_count {
            let worker_handle = storage_handle.clone();
            let worker_admission = Arc::clone(&admission);
            let worker_stop = Arc::clone(&sweeper.stop);
            let worker_active_roots = Arc::clone(&active_roots);
            let worker_schedule = Arc::clone(&schedule);
            let handle = match std::thread::Builder::new()
                .name(format!("argmin-reclaim-{worker_index}"))
                .spawn(move || {
                    run_reclaim_worker(
                        worker_handle,
                        worker_admission,
                        worker_stop,
                        worker_active_roots,
                        worker_schedule,
                        worker_index == 0,
                    );
                }) {
                Ok(handle) => handle,
                Err(error) => {
                    sweeper.stop.store(true, Ordering::SeqCst);
                    storage_handle.current().wake_reclaim_workers();
                    for handle in handles {
                        let _ = handle.join();
                    }
                    return Err(StorageMaintenanceStartError::worker_spawn("reclaim", error));
                }
            };
            handles.push(handle);
        }
        *sweeper
            .handles
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = handles;
        Ok(sweeper)
    }

    #[must_use]
    pub fn disabled(storage_handle: StorageClusterRouteHandle) -> Arc<Self> {
        Arc::new(Self {
            storage_handle,
            stop: Arc::new(AtomicBool::new(true)),
            handles: Mutex::new(Vec::new()),
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
        let handles = std::mem::take(
            &mut *self
                .handles
                .lock()
                .unwrap_or_else(|error| error.into_inner()),
        );
        for handle in handles {
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
        let handles = std::mem::take(
            &mut *self
                .handles
                .lock()
                .unwrap_or_else(|error| error.into_inner()),
        );
        for handle in handles {
            let _ = handle.join();
        }
    }
}

fn run_reclaim_worker(
    storage_handle: StorageClusterRouteHandle,
    admission: Arc<StorageMaintenanceAdmission>,
    stop: Arc<AtomicBool>,
    active_roots: Arc<Mutex<ActiveReclaimRoots>>,
    schedule: Arc<Mutex<ReclaimWorkerSchedule>>,
    scans_durable_roots: bool,
) {
    let mut scan_schedule = DurableReclaimScanSchedule::immediate();
    let mut pending_work: Option<(Arc<StorageCluster>, ReclaimWorkItem, bool)> = None;

    while !stop.load(Ordering::SeqCst) {
        let current = storage_handle.current();
        if scans_durable_roots {
            enqueue_durable_reclaim_work_with_active_exclusions_if_due(
                &current,
                &admission,
                &active_roots,
                &mut scan_schedule,
            );
        }
        let Some((queue_owner, work, already_owned)) = pending_work
            .take()
            .or_else(|| take_scheduled_reclaim_work(&current, &schedule))
            .or_else(|| {
                wait_for_runtime_map_reclaim_work(&storage_handle, &stop)
                    .map(|(owner, work)| (owner, work, false))
            })
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

        if !already_owned {
            let acquisition = active_roots
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .try_acquire(&queue_owner, &work);
            if let Err(active_owner) = acquisition {
                acknowledge_duplicate_reclaim_hint(&queue_owner, &work, active_owner);
                continue;
            }
        }
        let owned_work = work.clone();

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
            pending_work = Some((queue_owner, work, true));
            std::thread::sleep(OBJECT_PAYLOAD_RECLAIM_PG_RETRY_COOLDOWN);
            continue;
        };

        match work {
            ReclaimWorkItem::ObjectPayload((bucket, key, generation_id)) => {
                let root = (bucket, key, generation_id);
                let pg_id = execution_node.object_payload_reclaim_pg_id(&root.0, &root.1);
                let observed_retry_after = schedule
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .object_retries
                    .observe(pg_id);
                let cooling_down =
                    observed_retry_after.is_some_and(|retry_after| retry_after > Instant::now());
                if cooling_down {
                    let mut schedule = schedule.lock().unwrap_or_else(|error| error.into_inner());
                    let deferred = &mut schedule.deferred;
                    defer_object_payload_reclaim(
                        &mut deferred.objects,
                        &mut deferred.object_roots,
                        Arc::clone(&queue_owner),
                        root.clone(),
                    );
                    drop(schedule);
                    notify_reclaim_work_deferred(&queue_owner);
                } else {
                    let result = execution_node
                        .reclaim_object_payload_if_unleased_with_outcome(&root.0, &root.1, root.2);
                    if object_payload_reclaim_worker_should_defer(&result) {
                        let mut schedule =
                            schedule.lock().unwrap_or_else(|error| error.into_inner());
                        schedule.object_retries.defer_until(
                            pg_id,
                            Instant::now() + OBJECT_PAYLOAD_RECLAIM_PG_RETRY_COOLDOWN,
                        );
                        let deferred = &mut schedule.deferred;
                        defer_object_payload_reclaim(
                            &mut deferred.objects,
                            &mut deferred.object_roots,
                            Arc::clone(&queue_owner),
                            root.clone(),
                        );
                        drop(schedule);
                        notify_reclaim_work_deferred(&queue_owner);
                    } else {
                        queue_owner.finish_object_payload_reclaim_work(&root.0, &root.1, root.2);
                        schedule
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .object_retries
                            .clear_observed(pg_id, observed_retry_after);
                        active_roots
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .release(&owned_work);
                    }
                }
            }
            ReclaimWorkItem::BucketDelete(root) => {
                let cooling_down = schedule
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .finalize_retry_after
                    .get(&root)
                    .is_some_and(|retry_after| *retry_after > Instant::now());
                if cooling_down {
                    let mut schedule = schedule.lock().unwrap_or_else(|error| error.into_inner());
                    let deferred = &mut schedule.deferred;
                    defer_bucket_delete_finalize(
                        &mut deferred.finalizers,
                        &mut deferred.finalizer_roots,
                        Arc::clone(&queue_owner),
                        root.clone(),
                    );
                    drop(schedule);
                    notify_reclaim_work_deferred(&queue_owner);
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
                            let mut schedule =
                                schedule.lock().unwrap_or_else(|error| error.into_inner());
                            schedule.finalize_retry_after.remove(&root);
                            schedule.finalizer_continuation_bursts.remove(&root);
                            schedule.begin_retry_after.retain(|begin, _| {
                                begin.bucket() != &root.bucket
                                    || begin.bucket_incarnation_generation()
                                        != root.bucket_incarnation_generation
                            });
                            schedule.deferred.begin_roots.retain(|begin| {
                                begin.bucket() != &root.bucket
                                    || begin.bucket_incarnation_generation()
                                        != root.bucket_incarnation_generation
                            });
                            let mut removed_begins = Vec::new();
                            let begins = std::mem::take(&mut schedule.deferred.begins);
                            for begin in begins {
                                if begin.root.bucket() == &root.bucket
                                    && begin.root.bucket_incarnation_generation()
                                        == root.bucket_incarnation_generation
                                {
                                    removed_begins.push(begin);
                                } else {
                                    schedule.deferred.begins.push_back(begin);
                                }
                            }
                            drop(schedule);
                            for begin in &removed_begins {
                                begin
                                    .queue_owner
                                    .finish_bucket_delete_begin_work(&begin.root);
                            }
                            let mut active_roots = active_roots
                                .lock()
                                .unwrap_or_else(|error| error.into_inner());
                            for begin in removed_begins {
                                active_roots
                                    .release(&ReclaimWorkItem::BucketDeleteBegin(begin.root));
                            }
                            active_roots.release(&owned_work);
                        }
                        BucketDeleteFinalizeWorkerDisposition::Continue => {
                            let mut schedule =
                                schedule.lock().unwrap_or_else(|error| error.into_inner());
                            schedule.finalize_retry_after.remove(&root);
                            let burst = schedule
                                .finalizer_continuation_bursts
                                .entry(root.clone())
                                .or_insert(0);
                            *burst += 1;
                            if *burst < BUCKET_DELETE_FINALIZE_CONTINUATION_BURST {
                                drop(schedule);
                                pending_work = Some((
                                    Arc::clone(&queue_owner),
                                    ReclaimWorkItem::BucketDelete(root),
                                    true,
                                ));
                            } else {
                                *burst = 0;
                                let deferred = &mut schedule.deferred;
                                defer_bucket_delete_finalize(
                                    &mut deferred.finalizers,
                                    &mut deferred.finalizer_roots,
                                    Arc::clone(&queue_owner),
                                    root.clone(),
                                );
                                drop(schedule);
                                notify_reclaim_work_deferred(&queue_owner);
                            }
                        }
                        BucketDeleteFinalizeWorkerDisposition::RetryAfter(delay) => {
                            let mut schedule =
                                schedule.lock().unwrap_or_else(|error| error.into_inner());
                            schedule.finalizer_continuation_bursts.remove(&root);
                            schedule
                                .finalize_retry_after
                                .insert(root.clone(), Instant::now() + delay);
                            let deferred = &mut schedule.deferred;
                            defer_bucket_delete_finalize(
                                &mut deferred.finalizers,
                                &mut deferred.finalizer_roots,
                                Arc::clone(&queue_owner),
                                root.clone(),
                            );
                            drop(schedule);
                            notify_reclaim_work_deferred(&queue_owner);
                        }
                    }
                }
            }
            ReclaimWorkItem::BucketDeleteBegin(root) => {
                let cooling_down = schedule
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .begin_retry_after
                    .get(&root)
                    .is_some_and(|retry_after| *retry_after > Instant::now());
                if cooling_down {
                    let mut schedule = schedule.lock().unwrap_or_else(|error| error.into_inner());
                    let deferred = &mut schedule.deferred;
                    defer_bucket_delete_begin(
                        &mut deferred.begins,
                        &mut deferred.begin_roots,
                        Arc::clone(&queue_owner),
                        root.clone(),
                    );
                    drop(schedule);
                    notify_reclaim_work_deferred(&queue_owner);
                } else {
                    let result = execution_node.continue_adopted_bucket_delete(&root);
                    let _ = observability::event(
                        TRACE_TARGET,
                        "bucket_delete_begin_worker_result",
                        Some(format_args!("root={root:?} result={result:?}")),
                    );
                    match result {
                        Ok(()) => {
                            schedule
                                .lock()
                                .unwrap_or_else(|error| error.into_inner())
                                .begin_retry_after
                                .remove(&root);
                            queue_owner.promote_bucket_delete_begin_to_finalize(&root);
                            active_roots
                                .lock()
                                .unwrap_or_else(|error| error.into_inner())
                                .release(&owned_work);
                        }
                        Err(error) => {
                            if bucket_delete_begin_root_is_stale(&execution_node, &root) {
                                schedule
                                    .lock()
                                    .unwrap_or_else(|error| error.into_inner())
                                    .begin_retry_after
                                    .remove(&root);
                                queue_owner.finish_bucket_delete_begin_work(&root);
                                active_roots
                                    .lock()
                                    .unwrap_or_else(|error| error.into_inner())
                                    .release(&owned_work);
                            } else if bucket_write_error_is_retryable(&error) {
                                let mut schedule =
                                    schedule.lock().unwrap_or_else(|error| error.into_inner());
                                schedule.begin_retry_after.insert(
                                    root.clone(),
                                    Instant::now() + BUCKET_DELETE_BEGIN_RETRY_COOLDOWN,
                                );
                                let deferred = &mut schedule.deferred;
                                defer_bucket_delete_begin(
                                    &mut deferred.begins,
                                    &mut deferred.begin_roots,
                                    Arc::clone(&queue_owner),
                                    root.clone(),
                                );
                                drop(schedule);
                                notify_reclaim_work_deferred(&queue_owner);
                            } else {
                                schedule
                                    .lock()
                                    .unwrap_or_else(|error| error.into_inner())
                                    .begin_retry_after
                                    .remove(&root);
                                active_roots
                                    .lock()
                                    .unwrap_or_else(|error| error.into_inner())
                                    .release(&owned_work);
                            }
                        }
                    }
                }
            }
        }

        if pending_work.is_none() {
            if scans_durable_roots {
                enqueue_durable_reclaim_work_with_active_exclusions_if_due(
                    &execution_node,
                    &admission,
                    &active_roots,
                    &mut scan_schedule,
                );
            }
            if let Some(work) = take_scheduled_reclaim_work(&execution_node, &schedule) {
                pending_work = Some(work);
            } else {
                let sleep_for = {
                    let schedule = schedule.lock().unwrap_or_else(|error| error.into_inner());
                    if schedule.deferred.is_empty() {
                        None
                    } else {
                        shortest_retry_sleep(
                            shortest_retry_sleep(
                                earliest_object_payload_reclaim_retry_sleep(
                                    &execution_node,
                                    &schedule.deferred.objects,
                                    schedule.object_retries.deadlines(),
                                ),
                                earliest_bucket_delete_begin_retry_sleep(
                                    &schedule.deferred.begins,
                                    &schedule.begin_retry_after,
                                ),
                            ),
                            earliest_bucket_delete_finalize_retry_sleep(
                                schedule
                                    .deferred
                                    .finalizers
                                    .iter()
                                    .map(|deferred| &deferred.root),
                                &schedule.finalize_retry_after,
                            ),
                        )
                    }
                };
                if let Some(sleep_for) = sleep_for {
                    std::thread::sleep(sleep_for);
                }
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
    fn successful_reclaim_does_not_clear_a_newer_pg_retry_deadline() {
        let pg_id = 7;
        let mut retries = ObjectPayloadReclaimRetrySchedule::default();
        let observed_retry_after = retries.observe(pg_id);
        let newer_retry_after = Instant::now() + OBJECT_PAYLOAD_RECLAIM_PG_RETRY_COOLDOWN;
        retries.defer_until(pg_id, newer_retry_after);

        assert!(retries.clear_observed(pg_id, observed_retry_after));
        assert_eq!(retries.observe(pg_id), Some(newer_retry_after));

        assert!(!retries.clear_observed(pg_id, Some(newer_retry_after)));
        assert_eq!(retries.observe(pg_id), None);
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
    fn bucket_delete_finalize_worker_continues_productive_scan_without_cooldown() {
        assert_eq!(
            bucket_delete_finalize_worker_disposition(&Ok(BucketDeleteFinalizeOutcome::Continue)),
            BucketDeleteFinalizeWorkerDisposition::Continue
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
