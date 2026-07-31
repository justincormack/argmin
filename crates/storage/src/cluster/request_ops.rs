use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
#[cfg(any(test, feature = "test-hooks"))]
use std::sync::MutexGuard;
#[cfg(any(test, feature = "test-hooks"))]
use std::sync::{Mutex, OnceLock};

use placement::NodeId;

use super::{LocalClusterRuntimeState, MetadataCommandExecutionRoute, MetadataCommandRouteMode};
#[cfg(any(test, feature = "test-hooks"))]
use super::{
    MetadataCommandApplyContextTestHook, MetadataCommandApplyContextTestHookGuard,
    MetadataCommandApplyTestContext, MetadataCommandApplyTestKind,
};
use crate::metadata_command::{
    BucketPropertyMutation, BucketSubresourceMutation, BucketWriteReservationProof,
    CommitMultipartObjectCommand, CommitStreamPartCommand, DeleteFinalizedBucketCommand,
    DeleteObjectPayloadReclaimCommand, DeleteObjectVersionTarget, MetadataCommandAcceptance,
    MetadataCommandEnvelope, MetadataCommandId, MetadataCommandPayload,
    ObjectPayloadReclaimClaimProof, ObjectPayloadReclaimCommand, PutObjectMetadataCommand,
    PutObjectMetadataMutation, ABORT_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
    COMPLETE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
    CREATE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
    DELETE_CURRENT_OBJECT_BUCKET_WRITE_OPERATION_KIND,
    DELETE_OBJECT_VERSION_BUCKET_WRITE_OPERATION_KIND,
    INSERT_DELETE_MARKER_BUCKET_WRITE_OPERATION_KIND,
    PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
    PUT_OBJECT_METADATA_BUCKET_WRITE_OPERATION_KIND,
    PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
    UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
    UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND,
};
use crate::node::ReclaimQueueInsert;
use crate::node_client::{
    complete_multipart_expected_object_parts, BucketWriteReservationNodeClient,
    BuildCompleteMultipartObjectCommandReq, BuildCreateMultipartUploadCommandReq,
    BuildCreateStreamUploadCommandReq, BuildDeleteCurrentObjectCommandReq,
    BuildDeleteSpecificObjectVersionCommandReq, BuildInsertDeleteMarkerCommandReq,
    BuildPutObjectMetadataCommandReq, BuildStreamPartCommitCommandReq,
    BuildStreamPutCommitCommandReq, CreateBucketCommandBuild, CreateStreamUploadPrecondition,
    InsertDeleteMarkerStalePayload, MarkBucketDeletingCommandBuild,
    UpdateStreamUploadBucketWriteReservationReq,
};
use crate::storage_rpc::StorageRpcErrorCode;
use crate::traits::DurableBucketWriteReservationAcquire;
#[cfg(any(test, feature = "test-hooks"))]
use crate::traits::PgMetadataStore;
use crate::types::{
    AdmittedRouteEffectFence, BucketDeleteDebugBucketRow, BucketDeleteDebugDrain,
    BucketDeleteDebugFinalizeClaim, BucketDeleteDebugObjectVersionKind,
    BucketDeleteDebugObjectVersionSample, BucketDeleteDebugObjectVersionSampleError,
    BucketDeleteDebugPayloadReclaimClaim, BucketDeleteDebugPayloadReclaimClaimError,
    BucketDeleteDebugPayloadReclaimRoot, BucketDeleteDebugPayloadReclaimRootError,
    BucketDeleteDebugPendingCommand, BucketDeleteDebugSnapshot,
};
#[cfg(any(test, feature = "test-hooks"))]
use crate::types::{MultipartReclaimRecord, ObjectSegmentsReclaimRecord};
use crate::*;

const INTERNAL_LIST_PAGE_SIZE: u32 = 1_000;
const ORPHAN_OBJECT_PAYLOAD_RECLAIM_BUCKET_INCARNATION: u64 = 0;
const BUCKET_DELETE_FINALIZE_SCAN_LIMIT_PER_PG: usize = 16;
const BUCKET_DELETE_FINALIZE_SCAN_PG_BATCH: usize = 8;
const BUCKET_DELETE_BEGIN_SCAN_LIMIT_PER_PG: usize = 16;
const BUCKET_DELETE_BEGIN_WORK_BUDGET_MILLIS: u64 = 10_000;
const BUCKET_DELETE_FINALIZE_WORK_BUDGET_MILLIS: u64 = 10_000;
const BUCKET_DELETE_EXACT_BUCKET_PENDING_PROBE_PARALLELISM: usize = 8;
const BUCKET_DELETE_RESERVATION_DRAIN_WAIT_MILLIS: u64 = 1_000;
const BUCKET_DELETE_DRAIN_LEASE_MILLIS: u64 = BUCKET_DELETE_BEGIN_WORK_BUDGET_MILLIS + 5_000;
const BUCKET_DELETE_EXACT_BUCKET_DRAIN_BUDGET_EXHAUSTED_CONTEXT: &str =
    "bucket delete exact-bucket drain budget exhausted";
const BUCKET_DELETE_RESERVATION_WAIT_BLOCKED_CONTEXT: &str =
    "bucket delete reservation wait blocked by durable bucket write reservation";
const LIFECYCLE_SWEEP_ROOT_SCAN_LIMIT_PER_PG: usize = 1_024;
const OBJECT_READ_SNAPSHOT_STALE_RETRY_BUDGET: std::time::Duration =
    std::time::Duration::from_secs(10);
const METADATA_COMMAND_APPLY_RETRY_BUDGET_MILLIS: u64 = 10_000;
const BUCKET_WRITE_RESERVATION_LEASE_MILLIS: u64 = 15_000;
// HTTP streaming PutObject heartbeats active sessions every 10s. Keep the
// durable create reservation only slightly longer than that so abandoned
// sessions stop blocking DeleteBucket well before common 30s client attempt
// timeouts, while still allowing one delayed heartbeat under contention.
const PUT_OBJECT_STREAM_CREATE_LEASE_MILLIS: u64 = BUCKET_WRITE_RESERVATION_LEASE_MILLIS;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct DurableObjectPayloadReclaimScan {
    pub queued: usize,
    pub errors: usize,
    pub route_refresh_required: bool,
    pub retry_required: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct DurableBucketDeleteFinalizeScan {
    pub queued: usize,
    pub errors: usize,
    pub route_refresh_required: bool,
    pub retry_required: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct DurableBucketDeleteBeginScan {
    pub queued: usize,
    pub errors: usize,
    pub route_refresh_required: bool,
    pub retry_required: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub(crate) enum DurableReclaimScanOutcome {
    Complete,
    RouteRefreshRequired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub(crate) struct DurableReclaimScanBatch {
    pub outcome: DurableReclaimScanOutcome,
    pub next_pg_id: Option<u32>,
    pub scanned_pgs: usize,
    pub retry_pass_required: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BoundedPgScanWindow {
    start: usize,
    end: usize,
    next_pg_id: Option<u32>,
}

fn bounded_pg_scan_window(
    pg_ids: &[u32],
    next_pg_id: Option<u32>,
    max_pgs: usize,
) -> BoundedPgScanWindow {
    let start = next_pg_id.map_or(0, |next_pg_id| {
        pg_ids.partition_point(|pg_id| *pg_id < next_pg_id)
    });
    let end = start.saturating_add(max_pgs.max(1)).min(pg_ids.len());
    BoundedPgScanWindow {
        start,
        end,
        next_pg_id: pg_ids.get(end).copied(),
    }
}

fn durable_reclaim_scan_requires_route_refresh(error: &StoreError) -> bool {
    match error {
        StoreError::StalePayloadOperation { .. }
        | StoreError::StaleMetadataPrimaryBridge { .. }
        | StoreError::StaleMetadataOperation { .. }
        | StoreError::StaleMetadataRoute { .. }
        | StoreError::RouteMapExpired { .. }
        | StoreError::StaleShardOperation { .. }
        | StoreError::StaleShardLocation { .. } => true,
        StoreError::ShardStore { source, .. } => {
            durable_reclaim_scan_requires_route_refresh(source)
        }
        StoreError::StorageRpc { failure: code, .. } => matches!(
            *code,
            StorageRpcErrorCode::StaleShardLocation | StorageRpcErrorCode::WrongClusterEpoch
        ),
        _ => false,
    }
}

fn durable_reclaim_bucket_scan_requires_route_refresh(error: &BucketSnapshotLoadError) -> bool {
    match error {
        BucketSnapshotLoadError::Store(error) => durable_reclaim_scan_requires_route_refresh(error),
        BucketSnapshotLoadError::Metadata(_) => false,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BucketIdentityGenerations {
    pub bucket_execution_generation: u64,
    pub bucket_incarnation_generation: u64,
}

impl BucketIdentityGenerations {
    pub fn from_bucket_info(bucket_info: &BucketInfo) -> Self {
        Self {
            bucket_execution_generation: bucket_info.bucket_execution_generation,
            bucket_incarnation_generation: bucket_info.bucket_incarnation_generation,
        }
    }
}

const LIFECYCLE_SWEEP_CLAIM_LEASE_MILLIS: u64 = 60_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AbortMultipartUploadDrainMode {
    Wait,
    Stop,
}

#[derive(Clone, Copy)]
struct BucketDeleteExactDrainProgress<'a> {
    client: &'a dyn BucketWriteReservationNodeClient,
    drain: &'a super::DurableBucketWriteDrain,
    phase: BucketDeleteAttemptPhase,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BucketVisibleDataSource {
    ObjectVersion { pg_id: PgId },
    MultipartUpload { pg_id: PgId },
    StreamUpload { pg_id: PgId },
}

impl BucketVisibleDataSource {
    fn label(self) -> &'static str {
        match self {
            Self::ObjectVersion { .. } => "object_version",
            Self::MultipartUpload { .. } => "multipart_upload",
            Self::StreamUpload { .. } => "stream_upload",
        }
    }

    fn pg_id(self) -> PgId {
        match self {
            Self::ObjectVersion { pg_id }
            | Self::MultipartUpload { pg_id }
            | Self::StreamUpload { pg_id } => pg_id,
        }
    }
}

enum PutObjectStreamUploadCleanup {
    Live(BucketVisibleDataSource),
    Aborted { count: usize },
}

fn bucket_delete_visible_data_diagnostics_enabled() -> bool {
    std::env::var_os("ARGMIN_BUCKET_DELETE_VISIBLE_DATA_DIAGNOSTICS").is_some()
}

fn metadata_command_is_matching_multipart_abort(
    command: &MetadataCommandEnvelope,
    bucket: &BucketName,
    key: &ObjectKey,
    upload_id: &UploadId,
) -> bool {
    matches!(
        command.payload(),
        MetadataCommandPayload::AbortMultipartUpload(abort)
            if abort.bucket == *bucket && abort.key == *key && abort.upload_id == *upload_id
    )
}

fn metadata_command_matches_bucket_incarnation(
    command: &MetadataCommandEnvelope,
    bucket_incarnation_generation: u64,
) -> bool {
    super::StorageCluster::metadata_command_bucket_write_reservation_proof(command)
        .is_some_and(|proof| proof.bucket_incarnation_generation == bucket_incarnation_generation)
}

fn lifecycle_sweep_root_source_rank(source: LifecycleSweepRootSource) -> u8 {
    match source {
        LifecycleSweepRootSource::ExpiredClaim => 0,
        LifecycleSweepRootSource::BusyClaim => 1,
        LifecycleSweepRootSource::LifecycleConfig => 2,
        LifecycleSweepRootSource::AbortingMultipartUpload => 3,
    }
}

struct BucketLifecycleContext {
    bucket_info: BucketInfo,
    bucket_incarnation_generation: u64,
    raw_lifecycle: Option<String>,
}

#[cfg(test)]
type MetadataCommandApplyTestHook =
    Arc<dyn Fn(NodeId, &MetadataCommandEnvelope) -> Result<(), StoreError> + Send + Sync>;

#[cfg(test)]
type AbortMultipartPendingInstallTestHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
pub type StreamPutCreatePendingInstallTestHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(test)]
type StreamPutCreateCommandIdTestHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
pub type StreamPutFinalizeCommandIdTestHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(test)]
type BucketDeleteCommandIdTestHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
pub type BucketDeleteFinalVisibilityStartTestHook =
    Arc<dyn Fn() -> Result<(), StoreError> + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
pub type BucketDeleteFinalVisibilityProvenTestHook =
    Arc<dyn Fn() -> Result<(), StoreError> + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
pub type BucketDeleteReservationWaitReadyTestHook =
    Arc<dyn Fn() -> Result<(), StoreError> + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
pub type BucketDeletePostReservationProgressTestHook =
    Arc<dyn Fn(u32) -> Result<(), StoreError> + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
pub type BucketDeleteExactDrainProgressTestHook =
    Arc<dyn Fn(crate::TestBucketDeleteAttemptPhase, u32) -> Result<(), StoreError> + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
pub type BucketDeleteExactDrainStartTestHook =
    Arc<dyn Fn(bool, u32) -> Result<(), StoreError> + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
#[cfg(test)]
type MultipartCompletionBarrierCommandIdTestHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MultipartCompletionStaleRetryTestEvent {
    BeforeStalePayloadSourceLoad,
    AfterStaleCommandBuild,
}

#[cfg(test)]
type MultipartCompletionStaleRetryTestHook =
    Arc<dyn Fn(MultipartCompletionStaleRetryTestEvent, &UploadId) + Send + Sync>;

#[cfg(test)]
static BEFORE_METADATA_COMMAND_APPLY_HOOKS: OnceLock<
    Mutex<HashMap<usize, MetadataCommandApplyTestHook>>,
> = OnceLock::new();

#[cfg(test)]
static BEFORE_ABORT_MULTIPART_PENDING_INSTALL_HOOKS: OnceLock<
    Mutex<HashMap<usize, AbortMultipartPendingInstallTestHook>>,
> = OnceLock::new();

#[cfg(any(test, feature = "test-hooks"))]
static BEFORE_STREAM_PUT_CREATE_PENDING_INSTALL_HOOKS: OnceLock<
    Mutex<HashMap<usize, StreamPutCreatePendingInstallTestHook>>,
> = OnceLock::new();

#[cfg(test)]
static BEFORE_STREAM_PUT_CREATE_COMMAND_ID_HOOKS: OnceLock<
    Mutex<HashMap<usize, StreamPutCreateCommandIdTestHook>>,
> = OnceLock::new();

#[cfg(any(test, feature = "test-hooks"))]
static BEFORE_STREAM_PUT_FINALIZE_COMMAND_ID_HOOKS: OnceLock<
    Mutex<HashMap<usize, StreamPutFinalizeCommandIdTestHook>>,
> = OnceLock::new();

#[cfg(test)]
static BEFORE_BUCKET_DELETE_COMMAND_ID_HOOKS: OnceLock<
    Mutex<HashMap<usize, BucketDeleteCommandIdTestHook>>,
> = OnceLock::new();

#[cfg(any(test, feature = "test-hooks"))]
static BEFORE_BUCKET_DELETE_FINAL_VISIBILITY_HOOKS: OnceLock<
    Mutex<HashMap<usize, BucketDeleteFinalVisibilityStartTestHook>>,
> = OnceLock::new();

#[cfg(any(test, feature = "test-hooks"))]
static AFTER_BUCKET_DELETE_FINAL_VISIBILITY_PROVEN_HOOKS: OnceLock<
    Mutex<HashMap<usize, BucketDeleteFinalVisibilityProvenTestHook>>,
> = OnceLock::new();

#[cfg(any(test, feature = "test-hooks"))]
static AFTER_BUCKET_DELETE_RESERVATION_WAIT_READY_HOOKS: OnceLock<
    Mutex<HashMap<usize, BucketDeleteReservationWaitReadyTestHook>>,
> = OnceLock::new();

#[cfg(any(test, feature = "test-hooks"))]
static AFTER_BUCKET_DELETE_POST_RESERVATION_PROGRESS_HOOKS: OnceLock<
    Mutex<HashMap<usize, BucketDeletePostReservationProgressTestHook>>,
> = OnceLock::new();

#[cfg(any(test, feature = "test-hooks"))]
static AFTER_BUCKET_DELETE_EXACT_DRAIN_PROGRESS_HOOKS: OnceLock<
    Mutex<HashMap<usize, BucketDeleteExactDrainProgressTestHook>>,
> = OnceLock::new();

#[cfg(any(test, feature = "test-hooks"))]
static BEFORE_BUCKET_DELETE_EXACT_DRAIN_HOOKS: OnceLock<
    Mutex<HashMap<usize, BucketDeleteExactDrainStartTestHook>>,
> = OnceLock::new();

#[cfg(test)]
static BEFORE_MULTIPART_COMPLETION_BARRIER_COMMAND_ID_HOOKS: OnceLock<
    Mutex<HashMap<usize, MultipartCompletionBarrierCommandIdTestHook>>,
> = OnceLock::new();

#[cfg(test)]
static MULTIPART_COMPLETION_STALE_RETRY_HOOKS: OnceLock<
    Mutex<HashMap<usize, MultipartCompletionStaleRetryTestHook>>,
> = OnceLock::new();

#[cfg(any(test, feature = "test-hooks"))]
static BEFORE_METADATA_COMMAND_APPLY_CONTEXT_HOOKS: OnceLock<
    Mutex<HashMap<usize, MetadataCommandApplyContextTestHook>>,
> = OnceLock::new();

#[cfg(test)]
pub(crate) struct MetadataCommandApplyTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct AbortMultipartPendingInstallTestHookGuard {
    scope_id: usize,
}

#[cfg(any(test, feature = "test-hooks"))]
pub struct StreamPutCreatePendingInstallTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct StreamPutCreateCommandIdTestHookGuard {
    scope_id: usize,
}

#[cfg(any(test, feature = "test-hooks"))]
pub struct StreamPutFinalizeCommandIdTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct BucketDeleteCommandIdTestHookGuard {
    scope_id: usize,
}

#[cfg(any(test, feature = "test-hooks"))]
pub struct BucketDeleteFinalVisibilityStartTestHookGuard {
    scope_id: usize,
}

#[cfg(any(test, feature = "test-hooks"))]
pub struct BucketDeleteFinalVisibilityProvenTestHookGuard {
    scope_id: usize,
}

#[cfg(any(test, feature = "test-hooks"))]
pub struct BucketDeleteReservationWaitReadyTestHookGuard {
    scope_id: usize,
}

#[cfg(any(test, feature = "test-hooks"))]
pub struct BucketDeletePostReservationProgressTestHookGuard {
    scope_id: usize,
}

#[cfg(any(test, feature = "test-hooks"))]
pub struct BucketDeleteExactDrainProgressTestHookGuard {
    scope_id: usize,
}

#[cfg(any(test, feature = "test-hooks"))]
pub struct BucketDeleteExactDrainStartTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct MultipartCompletionBarrierCommandIdTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct MultipartCompletionStaleRetryTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
impl Drop for MetadataCommandApplyTestHookGuard {
    fn drop(&mut self) {
        let hooks = BEFORE_METADATA_COMMAND_APPLY_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(test)]
impl Drop for AbortMultipartPendingInstallTestHookGuard {
    fn drop(&mut self) {
        let hooks =
            BEFORE_ABORT_MULTIPART_PENDING_INSTALL_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for StreamPutCreatePendingInstallTestHookGuard {
    fn drop(&mut self) {
        let hooks = BEFORE_STREAM_PUT_CREATE_PENDING_INSTALL_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(test)]
impl Drop for StreamPutCreateCommandIdTestHookGuard {
    fn drop(&mut self) {
        let hooks =
            BEFORE_STREAM_PUT_CREATE_COMMAND_ID_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for StreamPutFinalizeCommandIdTestHookGuard {
    fn drop(&mut self) {
        let hooks =
            BEFORE_STREAM_PUT_FINALIZE_COMMAND_ID_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(test)]
impl Drop for BucketDeleteCommandIdTestHookGuard {
    fn drop(&mut self) {
        let hooks =
            BEFORE_BUCKET_DELETE_COMMAND_ID_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for BucketDeleteFinalVisibilityStartTestHookGuard {
    fn drop(&mut self) {
        let hooks =
            BEFORE_BUCKET_DELETE_FINAL_VISIBILITY_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for BucketDeleteFinalVisibilityProvenTestHookGuard {
    fn drop(&mut self) {
        let hooks = AFTER_BUCKET_DELETE_FINAL_VISIBILITY_PROVEN_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for BucketDeleteReservationWaitReadyTestHookGuard {
    fn drop(&mut self) {
        let hooks = AFTER_BUCKET_DELETE_RESERVATION_WAIT_READY_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for BucketDeletePostReservationProgressTestHookGuard {
    fn drop(&mut self) {
        let hooks = AFTER_BUCKET_DELETE_POST_RESERVATION_PROGRESS_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for BucketDeleteExactDrainProgressTestHookGuard {
    fn drop(&mut self) {
        let hooks = AFTER_BUCKET_DELETE_EXACT_DRAIN_PROGRESS_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for BucketDeleteExactDrainStartTestHookGuard {
    fn drop(&mut self) {
        let hooks =
            BEFORE_BUCKET_DELETE_EXACT_DRAIN_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(test)]
impl Drop for MultipartCompletionBarrierCommandIdTestHookGuard {
    fn drop(&mut self) {
        let hooks = BEFORE_MULTIPART_COMPLETION_BARRIER_COMMAND_ID_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(test)]
impl Drop for MultipartCompletionStaleRetryTestHookGuard {
    fn drop(&mut self) {
        let hooks =
            MULTIPART_COMPLETION_STALE_RETRY_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for MetadataCommandApplyContextTestHookGuard {
    fn drop(&mut self) {
        let hooks =
            BEFORE_METADATA_COMMAND_APPLY_CONTEXT_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

fn maybe_run_before_metadata_command_apply_hook(
    _scope_id: usize,
    _node_id: NodeId,
    _command: &MetadataCommandEnvelope,
) -> Result<(), StoreError> {
    #[cfg(any(test, feature = "test-hooks"))]
    {
        let hook = BEFORE_METADATA_COMMAND_APPLY_CONTEXT_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&_scope_id)
            .cloned();
        if let Some(hook) = hook {
            hook(metadata_command_apply_test_context(_node_id, _command))?;
        }
    }
    #[cfg(test)]
    {
        let hook = BEFORE_METADATA_COMMAND_APPLY_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&_scope_id)
            .cloned();
        if let Some(hook) = hook {
            hook(_node_id, _command)?;
        }
    }
    Ok(())
}

#[cfg(test)]
fn maybe_run_before_abort_multipart_pending_install_hook(_scope_id: usize) {
    let hook = BEFORE_ABORT_MULTIPART_PENDING_INSTALL_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&_scope_id)
        .cloned();
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(any(test, feature = "test-hooks"))]
fn maybe_run_before_stream_put_create_pending_install_hook(_scope_id: usize) {
    let hook = BEFORE_STREAM_PUT_CREATE_PENDING_INSTALL_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&_scope_id)
        .cloned();
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
fn maybe_run_before_stream_put_create_command_id_hook(_scope_id: usize) {
    let hook = BEFORE_STREAM_PUT_CREATE_COMMAND_ID_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&_scope_id)
        .cloned();
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(any(test, feature = "test-hooks"))]
fn maybe_run_before_stream_put_finalize_command_id_hook(_scope_id: usize) {
    let hook = BEFORE_STREAM_PUT_FINALIZE_COMMAND_ID_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&_scope_id)
        .cloned();
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
fn maybe_run_before_bucket_delete_command_id_hook(_scope_id: usize) {
    let hook = BEFORE_BUCKET_DELETE_COMMAND_ID_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&_scope_id)
        .cloned();
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(any(test, feature = "test-hooks"))]
fn maybe_run_before_bucket_delete_final_visibility_hook(
    _scope_id: usize,
) -> Result<(), StoreError> {
    let hook = BEFORE_BUCKET_DELETE_FINAL_VISIBILITY_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&_scope_id)
        .cloned();
    if let Some(hook) = hook {
        hook()?;
    }
    Ok(())
}

#[cfg(any(test, feature = "test-hooks"))]
fn maybe_run_after_bucket_delete_final_visibility_proven_hook(
    _scope_id: usize,
) -> Result<(), StoreError> {
    let hook = AFTER_BUCKET_DELETE_FINAL_VISIBILITY_PROVEN_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&_scope_id)
        .cloned();
    if let Some(hook) = hook {
        hook()?;
    }
    Ok(())
}

#[cfg(any(test, feature = "test-hooks"))]
fn maybe_run_after_bucket_delete_reservation_wait_ready_hook(
    _scope_id: usize,
) -> Result<(), StoreError> {
    let hook = AFTER_BUCKET_DELETE_RESERVATION_WAIT_READY_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&_scope_id)
        .cloned();
    if let Some(hook) = hook {
        hook()?;
    }
    Ok(())
}

#[cfg(any(test, feature = "test-hooks"))]
fn maybe_run_after_bucket_delete_post_reservation_progress_hook(
    _scope_id: usize,
    _next_object_pg_id: u32,
) -> Result<(), StoreError> {
    let hook = AFTER_BUCKET_DELETE_POST_RESERVATION_PROGRESS_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&_scope_id)
        .cloned();
    if let Some(hook) = hook {
        hook(_next_object_pg_id)?;
    }
    Ok(())
}

#[cfg(any(test, feature = "test-hooks"))]
fn maybe_run_after_bucket_delete_exact_drain_progress_hook(
    _scope_id: usize,
    _phase: BucketDeleteAttemptPhase,
    _next_object_pg_id: u32,
) -> Result<(), StoreError> {
    let hook = AFTER_BUCKET_DELETE_EXACT_DRAIN_PROGRESS_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&_scope_id)
        .cloned();
    if let Some(hook) = hook {
        hook(_phase.into(), _next_object_pg_id)?;
    }
    Ok(())
}

#[cfg(any(test, feature = "test-hooks"))]
fn maybe_run_before_bucket_delete_exact_drain_hook(
    _scope_id: usize,
    _has_progress: bool,
    _next_object_pg_id: u32,
) -> Result<(), StoreError> {
    let hook = BEFORE_BUCKET_DELETE_EXACT_DRAIN_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&_scope_id)
        .cloned();
    if let Some(hook) = hook {
        hook(_has_progress, _next_object_pg_id)?;
    }
    Ok(())
}

#[cfg(test)]
fn maybe_run_before_multipart_completion_barrier_command_id_hook(_scope_id: usize) {
    let hook = BEFORE_MULTIPART_COMPLETION_BARRIER_COMMAND_ID_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&_scope_id)
        .cloned();
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
fn maybe_run_multipart_completion_stale_retry_hook(
    _scope_id: usize,
    _event: MultipartCompletionStaleRetryTestEvent,
    _upload_id: &UploadId,
) {
    let hook = MULTIPART_COMPLETION_STALE_RETRY_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&_scope_id)
        .cloned();
    if let Some(hook) = hook {
        hook(_event, _upload_id);
    }
}

#[cfg(any(test, feature = "test-hooks"))]
fn metadata_command_apply_test_context(
    node_id: NodeId,
    command: &MetadataCommandEnvelope,
) -> MetadataCommandApplyTestContext {
    let (kind, bucket, key) = match command.payload() {
        MetadataCommandPayload::CreateBucket(command) => (
            MetadataCommandApplyTestKind::CreateBucket,
            Some(command.bucket.name.clone()),
            None,
        ),
        MetadataCommandPayload::PutBucketVersioning(command) => (
            MetadataCommandApplyTestKind::PutBucketVersioning,
            Some(command.bucket.name.clone()),
            None,
        ),
        MetadataCommandPayload::PutBucketAcl(command) => (
            MetadataCommandApplyTestKind::PutBucketAcl,
            Some(command.bucket.name.clone()),
            None,
        ),
        MetadataCommandPayload::PutBucketProperty(command) => (
            MetadataCommandApplyTestKind::PutBucketProperty,
            Some(command.bucket.name.clone()),
            None,
        ),
        MetadataCommandPayload::PutBucketSubresource(command) => (
            MetadataCommandApplyTestKind::PutBucketSubresource,
            Some(command.name.clone()),
            None,
        ),
        MetadataCommandPayload::MarkBucketDeleting(command) => (
            MetadataCommandApplyTestKind::MarkBucketDeleting,
            Some(command.bucket.name.clone()),
            None,
        ),
        MetadataCommandPayload::DeleteFinalizedBucket(command) => (
            MetadataCommandApplyTestKind::DeleteFinalizedBucket,
            Some(command.bucket.clone()),
            None,
        ),
        MetadataCommandPayload::AdvanceMultipartCompletionBarrier(command) => (
            MetadataCommandApplyTestKind::AdvanceMultipartCompletionBarrier,
            Some(command.bucket.clone()),
            None,
        ),
        MetadataCommandPayload::ReserveObjectGeneration(command) => (
            MetadataCommandApplyTestKind::ReserveObjectGeneration,
            Some(command.bucket.clone()),
            Some(command.key.clone()),
        ),
        MetadataCommandPayload::ReleaseObjectGeneration(command) => (
            MetadataCommandApplyTestKind::ReleaseObjectGeneration,
            Some(command.bucket.clone()),
            Some(command.key.clone()),
        ),
        MetadataCommandPayload::ReserveObjectVersion(command) => (
            MetadataCommandApplyTestKind::ReserveObjectVersion,
            Some(command.bucket.clone()),
            Some(command.key.clone()),
        ),
        MetadataCommandPayload::CommitDirectPutObject(command) => (
            MetadataCommandApplyTestKind::CommitDirectPutObject,
            Some(command.object.bucket.clone()),
            Some(command.object.key.clone()),
        ),
        MetadataCommandPayload::CommitMultipartObject(command) => (
            MetadataCommandApplyTestKind::CommitMultipartObject,
            Some(command.object.bucket.clone()),
            Some(command.object.key.clone()),
        ),
        MetadataCommandPayload::DeleteObjectVersion(command) => (
            MetadataCommandApplyTestKind::DeleteObjectVersion,
            Some(command.bucket.clone()),
            Some(command.key.clone()),
        ),
        MetadataCommandPayload::InsertDeleteMarker(command) => (
            MetadataCommandApplyTestKind::InsertDeleteMarker,
            Some(command.bucket.clone()),
            Some(command.key.clone()),
        ),
        MetadataCommandPayload::PutObjectMetadata(command) => (
            MetadataCommandApplyTestKind::PutObjectMetadata,
            Some(command.object.bucket.clone()),
            Some(command.object.key.clone()),
        ),
        MetadataCommandPayload::CreateStreamUpload(command) => (
            MetadataCommandApplyTestKind::CreateStreamUpload,
            Some(command.session.bucket.clone()),
            Some(command.session.key.clone()),
        ),
        MetadataCommandPayload::AppendStreamSegment(command) => (
            MetadataCommandApplyTestKind::AppendStreamSegment,
            Some(command.bucket.clone()),
            Some(command.key.clone()),
        ),
        MetadataCommandPayload::AbortStreamUpload(command) => (
            MetadataCommandApplyTestKind::AbortStreamUpload,
            Some(command.bucket.clone()),
            Some(command.key.clone()),
        ),
        MetadataCommandPayload::CommitStreamPart(command) => (
            MetadataCommandApplyTestKind::CommitStreamPart,
            Some(command.bucket.clone()),
            Some(command.key.clone()),
        ),
        MetadataCommandPayload::CreateMultipartUpload(command) => (
            MetadataCommandApplyTestKind::CreateMultipartUpload,
            Some(command.upload.bucket.clone()),
            Some(command.upload.key.clone()),
        ),
        MetadataCommandPayload::AbortMultipartUpload(command) => (
            MetadataCommandApplyTestKind::AbortMultipartUpload,
            Some(command.bucket.clone()),
            Some(command.key.clone()),
        ),
        MetadataCommandPayload::DeleteObjectPayloadReclaim(command) => (
            MetadataCommandApplyTestKind::DeleteObjectPayloadReclaim,
            Some(command.bucket.clone()),
            Some(command.key.clone()),
        ),
    };
    MetadataCommandApplyTestContext {
        node_id,
        kind,
        bucket,
        key,
    }
}

#[derive(Clone)]
enum ListObjectsPageStart {
    After(ObjectKey),
    At(ObjectKey),
}

#[derive(Clone)]
enum ListVersionsPageStart {
    After {
        key_marker: ObjectKey,
        version_id_marker: Option<VersionId>,
    },
    At(ObjectKey),
}

struct ObjectCursor {
    pg_id: u32,
    objects: Vec<StoredObject>,
    next_index: usize,
    next_page_start: Option<ListObjectsPageStart>,
}

impl ObjectCursor {
    fn current(&self) -> Option<&StoredObject> {
        self.objects.get(self.next_index)
    }
}

struct VersionCursor {
    pg_id: u32,
    versions: Vec<StoredObject>,
    next_index: usize,
    next_page_start: Option<ListVersionsPageStart>,
}

struct MultipartUploadCursor {
    pg_id: u32,
    uploads: Vec<MultipartUploadRecord>,
    next_index: usize,
    next_page_start: Option<ListMultipartUploadsPageStart>,
}

impl MultipartUploadCursor {
    fn current(&self) -> Option<&MultipartUploadRecord> {
        self.uploads.get(self.next_index)
    }
}

fn multipart_upload_listing_position(upload: &MultipartUploadRecord) -> (u64, u64) {
    MultipartUploadIdKey::listing_position(&upload.upload_id)
        .filter(|position| *position != (0, 0))
        .unwrap_or_else(|| (1, upload.object_generation_id.get()))
}

struct BoundedSmallestRecords<K, V> {
    capacity: usize,
    records: BTreeMap<K, V>,
}

impl<K: Ord, V> BoundedSmallestRecords<K, V> {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            records: BTreeMap::new(),
        }
    }

    fn insert(&mut self, key: K, value: V) {
        if self.capacity == 0 || self.records.contains_key(&key) {
            return;
        }
        if self.records.len() == self.capacity {
            let retain = self
                .records
                .last_key_value()
                .is_some_and(|(largest, _)| key < *largest);
            if !retain {
                return;
            }
            self.records.pop_last();
        }
        self.records.insert(key, value);
    }

    fn into_values(self) -> Vec<V> {
        self.records.into_values().collect()
    }
}

#[cfg(test)]
mod bounded_smallest_records_tests {
    use std::collections::BTreeSet;

    use proptest::prelude::*;

    use super::BoundedSmallestRecords;

    #[test]
    fn production_scale_selection_is_bounded_across_one_hundred_pgs() {
        const PG_COUNT: u16 = 100;
        const RECORDS_PER_PG: u16 = 1_001;
        const CAPACITY: usize = 1_001;

        let mut smallest = BoundedSmallestRecords::new(CAPACITY);
        for pg_id in 0..PG_COUNT {
            for rank in 0..RECORDS_PER_PG {
                let key = (PG_COUNT - 1 - pg_id, rank);
                smallest.insert(key, key);
                assert!(smallest.records.len() <= CAPACITY);
            }
        }

        let selected = smallest.into_values();
        let expected = (0..RECORDS_PER_PG)
            .map(|rank| (0, rank))
            .collect::<Vec<_>>();
        assert_eq!(selected, expected);
    }

    proptest! {
        #[test]
        fn selection_matches_reference_global_sort(
            values in prop::collection::vec(any::<u16>(), 0..256),
            capacity in 0usize..32,
        ) {
            let mut smallest = BoundedSmallestRecords::new(capacity);
            for value in values.iter().copied() {
                smallest.insert(value, value);
                prop_assert!(smallest.records.len() <= capacity);
            }

            let expected = values
                .into_iter()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .take(capacity)
                .collect::<Vec<_>>();
            prop_assert_eq!(smallest.into_values(), expected);
        }
    }
}

impl VersionCursor {
    fn current(&self) -> Option<&StoredObject> {
        self.versions.get(self.next_index)
    }

    fn pop_current(&mut self) -> StoredObject {
        let version = self.versions[self.next_index].clone();
        self.next_index += 1;
        version
    }
}

fn conflicting_pending_metadata_command(context: &'static str) -> BucketSnapshotLoadError {
    StoreError::MetadataCommandContention { context }.into()
}

fn bucket_snapshot_error_to_bucket_write_drain_error(
    error: BucketSnapshotLoadError,
) -> BucketWriteDrainError {
    match error {
        BucketSnapshotLoadError::Store(error) => BucketWriteDrainError::Store(error),
        BucketSnapshotLoadError::Metadata(error) => BucketWriteDrainError::Metadata(error),
    }
}

#[derive(Debug)]
pub(super) struct MetadataCommandApplyFailure {
    pub(super) applied_nodes: usize,
    pub(super) source: BucketSnapshotLoadError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FinishPendingMetadataCommandResult {
    Applied,
    Abandoned,
    RetryPartialExactConflict,
}

// Metadata routing moves incrementally in Phase 6. Single-PG bucket/object
// operations route through the local metadata PG primary; composite scans fan
// out across routed PG primaries and merge locally.
impl super::StorageCluster {
    #[cfg(test)]
    pub(crate) fn test_install_before_metadata_command_apply_hook(
        &self,
        hook: MetadataCommandApplyTestHook,
    ) -> MetadataCommandApplyTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = BEFORE_METADATA_COMMAND_APPLY_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        MetadataCommandApplyTestHookGuard { scope_id }
    }

    #[cfg(test)]
    pub(crate) fn test_install_before_abort_multipart_pending_install_hook(
        &self,
        hook: AbortMultipartPendingInstallTestHook,
    ) -> AbortMultipartPendingInstallTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot =
            BEFORE_ABORT_MULTIPART_PENDING_INSTALL_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        AbortMultipartPendingInstallTestHookGuard { scope_id }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_install_before_stream_put_create_pending_install_hook(
        &self,
        hook: StreamPutCreatePendingInstallTestHook,
    ) -> StreamPutCreatePendingInstallTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = BEFORE_STREAM_PUT_CREATE_PENDING_INSTALL_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        StreamPutCreatePendingInstallTestHookGuard { scope_id }
    }

    #[cfg(test)]
    pub(crate) fn test_install_before_stream_put_create_command_id_hook(
        &self,
        hook: StreamPutCreateCommandIdTestHook,
    ) -> StreamPutCreateCommandIdTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot =
            BEFORE_STREAM_PUT_CREATE_COMMAND_ID_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        StreamPutCreateCommandIdTestHookGuard { scope_id }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_install_before_stream_put_finalize_command_id_hook(
        &self,
        hook: StreamPutFinalizeCommandIdTestHook,
    ) -> StreamPutFinalizeCommandIdTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot =
            BEFORE_STREAM_PUT_FINALIZE_COMMAND_ID_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        StreamPutFinalizeCommandIdTestHookGuard { scope_id }
    }

    #[cfg(test)]
    pub(crate) fn test_install_before_bucket_delete_command_id_hook(
        &self,
        hook: BucketDeleteCommandIdTestHook,
    ) -> BucketDeleteCommandIdTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = BEFORE_BUCKET_DELETE_COMMAND_ID_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        BucketDeleteCommandIdTestHookGuard { scope_id }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_install_before_bucket_delete_final_visibility_hook(
        &self,
        hook: BucketDeleteFinalVisibilityStartTestHook,
    ) -> BucketDeleteFinalVisibilityStartTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot =
            BEFORE_BUCKET_DELETE_FINAL_VISIBILITY_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        BucketDeleteFinalVisibilityStartTestHookGuard { scope_id }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_install_after_bucket_delete_final_visibility_proven_hook(
        &self,
        hook: BucketDeleteFinalVisibilityProvenTestHook,
    ) -> BucketDeleteFinalVisibilityProvenTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = AFTER_BUCKET_DELETE_FINAL_VISIBILITY_PROVEN_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        BucketDeleteFinalVisibilityProvenTestHookGuard { scope_id }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_install_after_bucket_delete_reservation_wait_ready_hook(
        &self,
        hook: BucketDeleteReservationWaitReadyTestHook,
    ) -> BucketDeleteReservationWaitReadyTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = AFTER_BUCKET_DELETE_RESERVATION_WAIT_READY_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        BucketDeleteReservationWaitReadyTestHookGuard { scope_id }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_install_after_bucket_delete_post_reservation_progress_hook(
        &self,
        hook: BucketDeletePostReservationProgressTestHook,
    ) -> BucketDeletePostReservationProgressTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = AFTER_BUCKET_DELETE_POST_RESERVATION_PROGRESS_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        BucketDeletePostReservationProgressTestHookGuard { scope_id }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_install_after_bucket_delete_exact_drain_progress_hook(
        &self,
        hook: BucketDeleteExactDrainProgressTestHook,
    ) -> BucketDeleteExactDrainProgressTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = AFTER_BUCKET_DELETE_EXACT_DRAIN_PROGRESS_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        BucketDeleteExactDrainProgressTestHookGuard { scope_id }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_install_before_bucket_delete_exact_drain_hook(
        &self,
        hook: BucketDeleteExactDrainStartTestHook,
    ) -> BucketDeleteExactDrainStartTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot =
            BEFORE_BUCKET_DELETE_EXACT_DRAIN_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        BucketDeleteExactDrainStartTestHookGuard { scope_id }
    }

    #[cfg(test)]
    pub(crate) fn test_install_before_multipart_completion_barrier_command_id_hook(
        &self,
        hook: MultipartCompletionBarrierCommandIdTestHook,
    ) -> MultipartCompletionBarrierCommandIdTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot = BEFORE_MULTIPART_COMPLETION_BARRIER_COMMAND_ID_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        MultipartCompletionBarrierCommandIdTestHookGuard { scope_id }
    }

    #[cfg(test)]
    pub(crate) fn test_install_multipart_completion_stale_retry_hook(
        &self,
        hook: MultipartCompletionStaleRetryTestHook,
    ) -> MultipartCompletionStaleRetryTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot =
            MULTIPART_COMPLETION_STALE_RETRY_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        MultipartCompletionStaleRetryTestHookGuard { scope_id }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_install_before_metadata_command_apply_context_hook(
        &self,
        hook: MetadataCommandApplyContextTestHook,
    ) -> MetadataCommandApplyContextTestHookGuard {
        let scope_id = self.metadata_command_apply_test_hook_scope_id();
        let slot =
            BEFORE_METADATA_COMMAND_APPLY_CONTEXT_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        slot.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope_id, hook);
        MetadataCommandApplyContextTestHookGuard { scope_id }
    }

    fn metadata_command_apply_test_hook_scope_id(&self) -> usize {
        std::sync::Arc::as_ptr(&self.local_map) as usize
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_pg_ids(&self) -> &[u32] {
        self.local_map.pg_ids()
    }

    fn metadata_pg_ids(&self) -> Vec<u32> {
        let mut pg_ids = self.local_map.pg_ids().to_vec();
        pg_ids.sort_unstable();
        pg_ids
    }

    fn terminal_bucket_delete_post_reservation_next_object_pg_id(&self) -> u32 {
        self.metadata_pg_ids()
            .into_iter()
            .max()
            .map_or(0, |pg_id| pg_id.saturating_add(1))
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn metadata_pg(&self, pg_id: u32) -> Result<MutexGuard<'_, PgStore>, StoreError> {
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), PgId::new(pg_id))?;
        node.test_node().get_pg(pg_id)
    }

    fn list_objects_page(
        &self,
        pg_id: u32,
        req: &ListObjectsReq,
    ) -> Result<ListObjectsResp, ObjectPgActionError> {
        let pg_id = PgId::new(pg_id);
        self.metadata_pg_primary_object_listing_route(pg_id)
            .and_then(|listing_route| listing_route.list_objects_page(req))
            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)
    }

    fn list_object_versions_page(
        &self,
        pg_id: u32,
        req: &ListObjectVersionsReq,
    ) -> Result<ListObjectVersionsResp, ObjectPgActionError> {
        let pg_id = PgId::new(pg_id);
        self.metadata_pg_primary_object_listing_route(pg_id)
            .and_then(|listing_route| listing_route.list_object_versions_page(req))
            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)
    }

    fn list_multipart_uploads_page(
        &self,
        pg_id: u32,
        req: &ListMultipartUploadsReq,
    ) -> Result<ListMultipartUploadsResp, ObjectPgActionError> {
        let pg_id = PgId::new(pg_id);
        self.metadata_pg_primary_object_listing_route(pg_id)
            .and_then(|listing_route| listing_route.list_multipart_uploads_page(req))
            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)
    }

    #[cfg(feature = "test-hooks")]
    pub fn try_probe_bucket_pg_available(
        &self,
        bucket: &BucketName,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let pg_id = PgId::new(self.bucket_metadata_pg_id(bucket));
        self.local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?
            .test_node()
            .try_probe_bucket_pg_available(bucket)
    }

    #[cfg(feature = "test-hooks")]
    pub fn try_probe_object_pg_available(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<bool, ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        self.local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?
            .test_node()
            .try_probe_object_pg_available(bucket, key)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn create_bucket_with_config_and_load_info(
        &self,
        config: &CreateBucketConfig<'_>,
    ) -> Result<BucketCreateAttemptOutcome, BucketSnapshotLoadError> {
        let bucket = BucketName::try_from(config.name).map_err(|reason| {
            MetadataError::InvalidBucketName {
                reason: reason.to_string(),
            }
        })?;
        self.create_bucket_with_config_and_load_info_with_route_validation(
            super::BucketMetadataMutationEffectRoute {
                pg_id: self.bucket_metadata_pg(&bucket),
                bucket: &bucket,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            || Ok(()),
            config,
        )
    }

    pub(super) fn create_bucket_with_config_and_load_info_with_route_validation(
        &self,
        route: super::BucketMetadataMutationEffectRoute<'_>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        config: &CreateBucketConfig<'_>,
    ) -> Result<BucketCreateAttemptOutcome, BucketSnapshotLoadError> {
        crate::metadata_command::metadata_command_publisher!(CreateBucket);
        let bucket = BucketName::try_from(config.name).map_err(|reason| {
            MetadataError::InvalidBucketName {
                reason: reason.to_string(),
            }
        })?;
        let super::BucketMetadataMutationEffectRoute {
            pg_id: bucket_pg_id,
            bucket: routed_bucket,
            effect_fence,
        } = route;
        if bucket != *routed_bucket {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "create bucket",
            }
            .into());
        }
        let pg_id = bucket_pg_id.pg_id();
        let primary_store = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let mut work_budget = super::RequestWorkBudget::new(
            std::time::Duration::from_millis(METADATA_COMMAND_APPLY_RETRY_BUDGET_MILLIS),
            None,
        )
        .for_operation("create_bucket_metadata")
        .for_pg(pg_id);
        loop {
            work_budget.check("create bucket metadata command budget exhausted")?;
            require_valid_route()?;
            let (command, clear_pending_on_zero_apply) = match self
                .pending_metadata_command_for_bucket(pg_id, &bucket)?
            {
                Some(command) => {
                    if self.drain_unrelated_pending_metadata_command_for_bucket_with_work_budget(
                        pg_id,
                        &bucket,
                        &command,
                        &mut work_budget,
                    )? {
                        continue;
                    }
                    match command.payload() {
                        MetadataCommandPayload::CreateBucket(create)
                            if create.matches_create_config(config) =>
                        {
                            (command, false)
                        }
                        _ => {
                            self.drain_pending_metadata_command_pg_slot_with_work_budget(
                                pg_id,
                                &bucket,
                                &command,
                                &mut work_budget,
                            )?;
                            continue;
                        }
                    }
                }
                None => {
                    require_valid_route()?;
                    match primary_store
                        .bucket_metadata_client()
                        .head_bucket_raw(bucket_pg_id, &bucket)
                    {
                        Ok(info) => {
                            return Ok(BucketCreateAttemptOutcome::Exists(info));
                        }
                        Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketNotFound {
                            ..
                        })) => {}
                        Err(other) => return Err(other),
                    }
                    let Some(command_id) = self
                        .next_bucket_metadata_command_id_or_drain_with_work_budget(
                            pg_id,
                            &bucket,
                            &mut work_budget,
                        )?
                    else {
                        continue;
                    };
                    require_valid_route()?;
                    let command = match primary_store
                        .bucket_metadata_client()
                        .build_create_bucket_command(bucket_pg_id, &bucket, command_id, config)?
                    {
                        CreateBucketCommandBuild::Exists(info) => {
                            return Ok(BucketCreateAttemptOutcome::Exists(info));
                        }
                        CreateBucketCommandBuild::Command(command) => *command,
                    };
                    if !self
                        .try_set_bucket_pg_pending_command_or_retry_with_work_budget_and_effect_fence(
                        pg_id,
                        &bucket,
                        &command,
                        Some(effect_fence),
                        &mut work_budget,
                    )? {
                        continue;
                    }
                    (command, true)
                }
            };
            let outcome = self
                .finish_pending_metadata_command_to_acting_set_allow_partial_exact_conflict_retry_with_work_budget(
                    pg_id,
                    &command,
                    clear_pending_on_zero_apply,
                    &mut work_budget,
                )?;
            match outcome {
                FinishPendingMetadataCommandResult::Applied => {}
                FinishPendingMetadataCommandResult::RetryPartialExactConflict => {
                    return Err(conflicting_pending_metadata_command(
                        "retryable partial pending create bucket command",
                    ));
                }
                FinishPendingMetadataCommandResult::Abandoned => continue,
            }

            let info = primary_store
                .bucket_metadata_client()
                .head_bucket_info(bucket_pg_id, &bucket)?;
            return Ok(BucketCreateAttemptOutcome::Created(info));
        }
    }

    pub(super) fn apply_metadata_command_to_acting_set(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), MetadataCommandApplyFailure> {
        self.apply_metadata_command_to_acting_set_with_route_mode(
            command,
            MetadataCommandRouteMode::Normal,
            self,
            None,
            None,
        )
    }

    pub(super) fn apply_metadata_command_to_acting_set_with_reservation_authority(
        &self,
        command: &MetadataCommandEnvelope,
        reservation_authority: &StorageCluster,
    ) -> Result<(), MetadataCommandApplyFailure> {
        self.apply_metadata_command_to_acting_set_with_route_mode(
            command,
            MetadataCommandRouteMode::Normal,
            reservation_authority,
            None,
            None,
        )
    }

    pub(super) fn apply_reissued_metadata_command_to_acting_set_for_recovery(
        &self,
        authorized_source: &MetadataCommandEnvelope,
        abandoned_source: Option<&MetadataCommandEnvelope>,
        command: &MetadataCommandEnvelope,
        reservation_authority: &StorageCluster,
    ) -> Result<(), MetadataCommandApplyFailure> {
        self.apply_metadata_command_to_acting_set_with_route_mode(
            command,
            MetadataCommandRouteMode::Recovery,
            reservation_authority,
            Some(authorized_source),
            abandoned_source,
        )
    }

    pub(super) fn apply_metadata_command_to_acting_set_for_recovery(
        &self,
        command: &MetadataCommandEnvelope,
        reservation_authority: &StorageCluster,
    ) -> Result<(), MetadataCommandApplyFailure> {
        self.apply_metadata_command_to_acting_set_with_route_mode(
            command,
            MetadataCommandRouteMode::Recovery,
            reservation_authority,
            None,
            None,
        )
    }

    fn apply_metadata_command_to_acting_set_with_route_mode(
        &self,
        command: &MetadataCommandEnvelope,
        route_mode: MetadataCommandRouteMode,
        reservation_authority: &StorageCluster,
        authorized_source: Option<&MetadataCommandEnvelope>,
        abandoned_source: Option<&MetadataCommandEnvelope>,
    ) -> Result<(), MetadataCommandApplyFailure> {
        let pg_id = command.id().pg_id();
        let primary_node_id = match route_mode {
            MetadataCommandRouteMode::Normal => self
                .local_map
                .metadata_pg_primary_node(command.id().cluster_epoch(), pg_id),
            MetadataCommandRouteMode::Recovery => self
                .local_map
                .metadata_pg_primary_node_for_metadata_command_recovery(
                    command.id().cluster_epoch(),
                    pg_id,
                ),
        }
        .map_err(|source| MetadataCommandApplyFailure {
            applied_nodes: 0,
            source: source.into(),
        })?
        .node_id();
        self.apply_metadata_command_to_acting_set_from_origin_with_route_mode(
            primary_node_id,
            command,
            route_mode,
            reservation_authority,
            authorized_source,
            abandoned_source,
        )
    }

    fn apply_metadata_command_to_acting_set_from_origin_with_route_mode(
        &self,
        origin_node_id: NodeId,
        command: &MetadataCommandEnvelope,
        route_mode: MetadataCommandRouteMode,
        reservation_authority: &StorageCluster,
        authorized_source: Option<&MetadataCommandEnvelope>,
        abandoned_source: Option<&MetadataCommandEnvelope>,
    ) -> Result<(), MetadataCommandApplyFailure> {
        let pg_id = command.id().pg_id();
        let pg_lock = self
            .local_map
            .runtime_state()
            .metadata_command_pg_lock(pg_id);
        let _pg_guard = pg_lock.lock().unwrap_or_else(|e| e.into_inner());
        let mut nodes = match route_mode {
            MetadataCommandRouteMode::Normal => self
                .local_map
                .metadata_pg_acting_nodes(command.id().cluster_epoch(), pg_id),
            MetadataCommandRouteMode::Recovery => self
                .local_map
                .metadata_pg_acting_nodes_for_metadata_command_recovery(
                    command.id().cluster_epoch(),
                    pg_id,
                ),
        }
        .map_err(|source| MetadataCommandApplyFailure {
            applied_nodes: 0,
            source: source.into(),
        })?;
        let primary_node_id = match route_mode {
            MetadataCommandRouteMode::Normal => self
                .local_map
                .metadata_pg_primary_node(command.id().cluster_epoch(), pg_id),
            MetadataCommandRouteMode::Recovery => self
                .local_map
                .metadata_pg_primary_node_for_metadata_command_recovery(
                    command.id().cluster_epoch(),
                    pg_id,
                ),
        }
        .map_err(|source| MetadataCommandApplyFailure {
            applied_nodes: 0,
            source: source.into(),
        })?
        .node_id();
        if origin_node_id != primary_node_id {
            return Err(MetadataCommandApplyFailure {
                applied_nodes: 0,
                source: StoreError::MetadataCommandFromNonPrimary {
                    node_id: primary_node_id.as_u32(),
                    pg_id: pg_id.get(),
                    cluster_epoch: command.id().cluster_epoch(),
                    origin_node_id: origin_node_id.as_u32(),
                    primary_node_id: primary_node_id.as_u32(),
                }
                .into(),
            });
        }
        nodes.sort_by_key(|node| node.node_id() != primary_node_id);
        // Fanout is primary-first. Once the primary has durably applied this
        // exact command, that log entry is the admission witness for replica
        // completion even if the original reservation expires meanwhile.
        let mut admission_witnessed = false;
        for (applied_nodes, node) in nodes.into_iter().enumerate() {
            if node.node_id() == primary_node_id {
                if let Some(source) = authorized_source {
                    let primary_critical_section = node
                        .metadata_command_recovery_client()
                        .open_metadata_command_recovery_critical_section(
                            pg_id,
                            command.id().cluster_epoch(),
                        )
                        .map_err(|source| MetadataCommandApplyFailure {
                            applied_nodes,
                            source: BucketSnapshotLoadError::Store(source),
                        })?;
                    let metadata_client = primary_critical_section.as_ref();
                    let acceptance = metadata_client
                        .metadata_command_acceptance(command)
                        .map_err(|source| MetadataCommandApplyFailure {
                            applied_nodes,
                            source: BucketSnapshotLoadError::Store(source),
                        })?;
                    if acceptance == MetadataCommandAcceptance::AlreadyApplied {
                        metadata_client
                            .apply_metadata_command_and_record_for_recovery(
                                source,
                                abandoned_source,
                                command,
                            )
                            .map_err(|source| MetadataCommandApplyFailure {
                                applied_nodes,
                                source,
                            })?;
                        admission_witnessed = true;
                        continue;
                    }
                    reservation_authority
                        .validate_metadata_command_bucket_write_reservation(command)
                        .map_err(|source| MetadataCommandApplyFailure {
                            applied_nodes,
                            source,
                        })?;
                    maybe_run_before_metadata_command_apply_hook(
                        self.metadata_command_apply_test_hook_scope_id(),
                        node.node_id(),
                        command,
                    )
                    .map_err(|source| MetadataCommandApplyFailure {
                        applied_nodes,
                        source: source.into(),
                    })?;
                    metadata_client
                        .apply_metadata_command_and_record_for_recovery(
                            source,
                            abandoned_source,
                            command,
                        )
                        .map_err(|source| MetadataCommandApplyFailure {
                            applied_nodes,
                            source,
                        })?;
                } else {
                    let primary_critical_section = node
                        .metadata_command_client()
                        .open_metadata_command_critical_section(pg_id, command.id().cluster_epoch())
                        .map_err(|source| MetadataCommandApplyFailure {
                            applied_nodes,
                            source: BucketSnapshotLoadError::Store(source),
                        })?;
                    let metadata_client = primary_critical_section.as_ref();
                    let acceptance = metadata_client
                        .metadata_command_acceptance(command)
                        .map_err(|source| MetadataCommandApplyFailure {
                            applied_nodes,
                            source: BucketSnapshotLoadError::Store(source),
                        })?;
                    if acceptance == MetadataCommandAcceptance::AlreadyApplied {
                        metadata_client
                            .apply_metadata_command_and_record(command)
                            .map_err(|source| MetadataCommandApplyFailure {
                                applied_nodes,
                                source,
                            })?;
                        admission_witnessed = true;
                        continue;
                    }
                    reservation_authority
                        .validate_metadata_command_bucket_write_reservation(command)
                        .map_err(|source| MetadataCommandApplyFailure {
                            applied_nodes,
                            source,
                        })?;
                    maybe_run_before_metadata_command_apply_hook(
                        self.metadata_command_apply_test_hook_scope_id(),
                        node.node_id(),
                        command,
                    )
                    .map_err(|source| MetadataCommandApplyFailure {
                        applied_nodes,
                        source: source.into(),
                    })?;
                    metadata_client
                        .apply_metadata_command_and_record(command)
                        .map_err(|source| MetadataCommandApplyFailure {
                            applied_nodes,
                            source,
                        })?;
                }
                admission_witnessed = true;
                continue;
            }

            debug_assert!(
                admission_witnessed,
                "metadata primary must be visited first"
            );
            let acceptance = match route_mode {
                MetadataCommandRouteMode::Normal => {
                    self.local_map.validate_metadata_command_for_replica(
                        origin_node_id,
                        node.node_id(),
                        pg_id,
                        command,
                    )
                }
                MetadataCommandRouteMode::Recovery => self
                    .local_map
                    .validate_metadata_command_for_replica_for_metadata_command_recovery(
                        origin_node_id,
                        node.node_id(),
                        pg_id,
                        command,
                    ),
            }
            .map_err(|source| MetadataCommandApplyFailure {
                applied_nodes,
                source: source.into(),
            })?;
            if acceptance == MetadataCommandAcceptance::AlreadyApplied {
                let apply = match authorized_source {
                    Some(source) => node
                        .metadata_command_recovery_client()
                        .apply_metadata_command_and_record_on_recovery_replica(
                            pg_id,
                            command.id().cluster_epoch(),
                            source,
                            abandoned_source,
                            command,
                        ),
                    None => node
                        .metadata_command_client()
                        .apply_metadata_command_and_record(pg_id, command),
                };
                apply.map_err(|source| MetadataCommandApplyFailure {
                    applied_nodes,
                    source,
                })?;
                continue;
            }
            maybe_run_before_metadata_command_apply_hook(
                self.metadata_command_apply_test_hook_scope_id(),
                node.node_id(),
                command,
            )
            .map_err(|source| MetadataCommandApplyFailure {
                applied_nodes,
                source: source.into(),
            })?;
            let apply = match authorized_source {
                Some(source) => node
                    .metadata_command_recovery_client()
                    .apply_metadata_command_and_record_on_recovery_replica(
                        pg_id,
                        command.id().cluster_epoch(),
                        source,
                        abandoned_source,
                        command,
                    ),
                None => node
                    .metadata_command_client()
                    .apply_metadata_command_and_record(pg_id, command),
            };
            apply.map_err(|source| MetadataCommandApplyFailure {
                applied_nodes,
                source,
            })?;
        }
        Ok(())
    }

    pub(super) fn record_abandoned_metadata_command_to_acting_set(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), MetadataCommandApplyFailure> {
        self.record_abandoned_metadata_command_to_acting_set_with_route_mode(
            command,
            MetadataCommandRouteMode::Normal,
            None,
            None,
        )
    }

    pub(super) fn record_abandoned_metadata_command_to_acting_set_for_recovery(
        &self,
        command: &MetadataCommandEnvelope,
        authorized_source: Option<&MetadataCommandEnvelope>,
        abandoned_source: Option<&MetadataCommandEnvelope>,
    ) -> Result<(), MetadataCommandApplyFailure> {
        self.record_abandoned_metadata_command_to_acting_set_with_route_mode(
            command,
            MetadataCommandRouteMode::Recovery,
            authorized_source,
            abandoned_source,
        )
    }

    fn record_abandoned_metadata_command_to_acting_set_with_route_mode(
        &self,
        command: &MetadataCommandEnvelope,
        route_mode: MetadataCommandRouteMode,
        recovery_authorized_source: Option<&MetadataCommandEnvelope>,
        recovery_abandoned_source: Option<&MetadataCommandEnvelope>,
    ) -> Result<(), MetadataCommandApplyFailure> {
        let pg_id = command.id().pg_id();
        let pg_lock = self
            .local_map
            .runtime_state()
            .metadata_command_pg_lock(pg_id);
        let _pg_guard = pg_lock.lock().unwrap_or_else(|e| e.into_inner());
        let primary_node_id = match route_mode {
            MetadataCommandRouteMode::Normal => self
                .local_map
                .metadata_pg_primary_node(command.id().cluster_epoch(), pg_id),
            MetadataCommandRouteMode::Recovery => self
                .local_map
                .metadata_pg_primary_node_for_metadata_command_recovery(
                    command.id().cluster_epoch(),
                    pg_id,
                ),
        }
        .map_err(|source| MetadataCommandApplyFailure {
            applied_nodes: 0,
            source: source.into(),
        })?
        .node_id();
        let mut nodes = match route_mode {
            MetadataCommandRouteMode::Normal => self
                .local_map
                .metadata_pg_acting_nodes(command.id().cluster_epoch(), pg_id),
            MetadataCommandRouteMode::Recovery => self
                .local_map
                .metadata_pg_acting_nodes_for_metadata_command_recovery(
                    command.id().cluster_epoch(),
                    pg_id,
                ),
        }
        .map_err(|source| MetadataCommandApplyFailure {
            applied_nodes: 0,
            source: source.into(),
        })?;
        nodes.sort_by_key(|node| node.node_id() != primary_node_id);
        for (applied_nodes, node) in nodes.into_iter().enumerate() {
            if node.node_id() == primary_node_id {
                let primary_critical_section = node
                    .metadata_command_recovery_client()
                    .open_metadata_command_recovery_critical_section(
                        pg_id,
                        command.id().cluster_epoch(),
                    )
                    .map_err(|source| MetadataCommandApplyFailure {
                        applied_nodes,
                        source: BucketSnapshotLoadError::Store(source),
                    })?;
                let metadata_client = primary_critical_section.as_ref();
                let acceptance = metadata_client
                    .metadata_command_abandon_acceptance(command)
                    .map_err(|source| MetadataCommandApplyFailure {
                        applied_nodes,
                        source: BucketSnapshotLoadError::Store(source),
                    })?;
                if acceptance != MetadataCommandAcceptance::AlreadyApplied {
                    metadata_client
                        .record_metadata_command_abandoned(command)
                        .map_err(|source| MetadataCommandApplyFailure {
                            applied_nodes,
                            source: source.into(),
                        })?;
                }
                continue;
            }
            let acceptance = self
                .local_map
                .validate_metadata_command_abandon_for_replica(
                    primary_node_id,
                    node.node_id(),
                    pg_id,
                    command,
                )
                .map_err(|source| MetadataCommandApplyFailure {
                    applied_nodes,
                    source: source.into(),
                })?;
            if acceptance == MetadataCommandAcceptance::AlreadyApplied {
                continue;
            }
            let result = match route_mode {
                MetadataCommandRouteMode::Normal => node
                    .metadata_command_client()
                    .record_metadata_command_abandoned_on_replica(pg_id, command),
                MetadataCommandRouteMode::Recovery => node
                    .metadata_command_recovery_client()
                    .record_metadata_command_abandoned_on_recovery_replica(
                        pg_id,
                        command.id().cluster_epoch(),
                        recovery_authorized_source.unwrap_or(command),
                        recovery_abandoned_source,
                        command,
                    ),
            };
            result.map_err(|source| MetadataCommandApplyFailure {
                applied_nodes,
                source: source.into(),
            })?;
        }
        Ok(())
    }

    pub(super) fn metadata_command_has_abandoned_log_on_acting_set(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, MetadataCommandApplyFailure> {
        self.metadata_command_has_abandoned_log_on_acting_set_with_route_mode(
            command,
            MetadataCommandRouteMode::Normal,
        )
    }

    pub(super) fn metadata_command_has_abandoned_log_on_acting_set_for_recovery(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, MetadataCommandApplyFailure> {
        self.metadata_command_has_abandoned_log_on_acting_set_with_route_mode(
            command,
            MetadataCommandRouteMode::Recovery,
        )
    }

    fn metadata_command_has_abandoned_log_on_acting_set_with_route_mode(
        &self,
        command: &MetadataCommandEnvelope,
        route_mode: MetadataCommandRouteMode,
    ) -> Result<bool, MetadataCommandApplyFailure> {
        let pg_id = command.id().pg_id();
        let nodes = match route_mode {
            MetadataCommandRouteMode::Normal => self
                .local_map
                .metadata_pg_acting_nodes(command.id().cluster_epoch(), pg_id),
            MetadataCommandRouteMode::Recovery => self
                .local_map
                .metadata_pg_acting_nodes_for_metadata_command_recovery(
                    command.id().cluster_epoch(),
                    pg_id,
                ),
        }
        .map_err(|source| MetadataCommandApplyFailure {
            applied_nodes: 0,
            source: source.into(),
        })?;
        for (applied_nodes, node) in nodes.into_iter().enumerate() {
            if node
                .metadata_command_inspection_client()
                .metadata_command_abandoned(pg_id, command)
                .map_err(|source| MetadataCommandApplyFailure {
                    applied_nodes,
                    source: source.into(),
                })?
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    #[cfg(test)]
    pub(crate) fn test_apply_metadata_command_to_acting_set_from_origin(
        &self,
        origin_node_id: NodeId,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.apply_metadata_command_to_acting_set_from_origin_with_route_mode(
            origin_node_id,
            command,
            MetadataCommandRouteMode::Normal,
            self,
            None,
            None,
        )
        .map_err(|error| error.source)
    }

    #[cfg(test)]
    pub(super) fn finish_pending_metadata_command_to_acting_set(
        &self,
        pg_id: PgId,
        _bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        clear_pending_on_zero_apply: bool,
    ) -> Result<super::PendingMetadataCommandOutcome, BucketSnapshotLoadError> {
        let mut work_budget = super::RequestWorkBudget::new(
            std::time::Duration::from_millis(METADATA_COMMAND_APPLY_RETRY_BUDGET_MILLIS),
            None,
        )
        .for_operation("metadata_command_apply")
        .for_pg(pg_id);
        self.finish_pending_metadata_command_to_acting_set_with_work_budget(
            pg_id,
            command,
            clear_pending_on_zero_apply,
            &mut work_budget,
        )
    }

    fn finish_pending_metadata_command_to_acting_set_with_work_budget(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        clear_pending_on_zero_apply: bool,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<super::PendingMetadataCommandOutcome, BucketSnapshotLoadError> {
        match self.finish_pending_metadata_command_to_acting_set_inner(
            pg_id,
            command,
            clear_pending_on_zero_apply,
            false,
            MetadataCommandExecutionRoute::normal(),
            work_budget,
        )? {
            FinishPendingMetadataCommandResult::Applied => {
                Ok(super::PendingMetadataCommandOutcome::Applied)
            }
            FinishPendingMetadataCommandResult::Abandoned => {
                Ok(super::PendingMetadataCommandOutcome::Abandoned)
            }
            FinishPendingMetadataCommandResult::RetryPartialExactConflict => {
                unreachable!("partial exact conflict retry is disabled for this caller")
            }
        }
    }

    pub(super) fn finish_pending_metadata_command_to_acting_set_allow_partial_exact_conflict_retry_with_work_budget(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        clear_pending_on_zero_apply: bool,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<FinishPendingMetadataCommandResult, BucketSnapshotLoadError> {
        self.finish_pending_metadata_command_to_acting_set_inner(
            pg_id,
            command,
            clear_pending_on_zero_apply,
            true,
            MetadataCommandExecutionRoute::normal(),
            work_budget,
        )
    }

    pub(super) fn finish_pending_metadata_command_to_acting_set_for_recovery_with_work_budget(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        clear_pending_on_zero_apply: bool,
        recovery_authorized_source: Option<&MetadataCommandEnvelope>,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<FinishPendingMetadataCommandResult, BucketSnapshotLoadError> {
        self.finish_pending_metadata_command_to_acting_set_inner(
            pg_id,
            command,
            clear_pending_on_zero_apply,
            true,
            MetadataCommandExecutionRoute::recovery(recovery_authorized_source, None),
            work_budget,
        )
    }

    fn finish_pending_metadata_command_to_acting_set_inner(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        clear_pending_on_zero_apply: bool,
        retry_partial_exact_conflict: bool,
        execution_route: MetadataCommandExecutionRoute<'_>,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<FinishPendingMetadataCommandResult, BucketSnapshotLoadError> {
        let route_mode = execution_route.mode;
        let recovery_authorized_source = execution_route.recovery_authorized_source.cloned();
        let mut command = command.clone();
        loop {
            work_budget.check("metadata command apply retry budget exhausted")?;
            let command_bucket = command.bucket_name();
            let abandoned_on_acting_set = match route_mode {
                MetadataCommandRouteMode::Normal => {
                    self.metadata_command_has_abandoned_log_on_acting_set(&command)
                }
                MetadataCommandRouteMode::Recovery => {
                    self.metadata_command_has_abandoned_log_on_acting_set_for_recovery(&command)
                }
            }
            .map_err(|error| error.source)?;
            if abandoned_on_acting_set {
                match route_mode {
                    MetadataCommandRouteMode::Normal => {
                        self.record_abandoned_metadata_command_to_acting_set(&command)
                    }
                    MetadataCommandRouteMode::Recovery => self
                        .record_abandoned_metadata_command_to_acting_set_for_recovery(
                            &command,
                            recovery_authorized_source.as_ref(),
                            None,
                        ),
                }
                .map_err(|error| error.source)?;
                self.release_metadata_command_bucket_write_reservation(&command)?;
                match route_mode {
                    MetadataCommandRouteMode::Normal => self
                        .remove_pending_metadata_command_for_bucket(
                            pg_id,
                            command_bucket,
                            &command,
                        ),
                    MetadataCommandRouteMode::Recovery => self
                        .remove_pending_metadata_command_for_bucket_recovery(
                            pg_id,
                            command_bucket,
                            &command,
                        ),
                }?;
                return Ok(FinishPendingMetadataCommandResult::Abandoned);
            }
            let apply_result = match route_mode {
                MetadataCommandRouteMode::Normal => {
                    self.apply_metadata_command_to_acting_set(&command)
                }
                MetadataCommandRouteMode::Recovery => match recovery_authorized_source.as_ref() {
                    Some(authorized_source) => self
                        .apply_reissued_metadata_command_to_acting_set_for_recovery(
                            authorized_source,
                            None,
                            &command,
                            self,
                        ),
                    None => self.apply_metadata_command_to_acting_set_for_recovery(&command, self),
                },
            };
            match apply_result {
                Ok(()) => {
                    self.release_applied_metadata_command_bucket_write_reservations(&command)?;
                    match route_mode {
                        MetadataCommandRouteMode::Normal => self
                            .remove_pending_metadata_command_for_bucket(
                                pg_id,
                                command_bucket,
                                &command,
                            ),
                        MetadataCommandRouteMode::Recovery => self
                            .remove_pending_metadata_command_for_bucket_recovery(
                                pg_id,
                                command_bucket,
                                &command,
                            ),
                    }?;
                    return Ok(FinishPendingMetadataCommandResult::Applied);
                }
                Err(error) => {
                    let MetadataCommandApplyFailure {
                        applied_nodes,
                        source,
                    } = error;
                    if retry_partial_exact_conflict
                        && super::StorageCluster::metadata_command_log_conflict_matches(
                            &command, &source,
                        )
                    {
                        let exact_conflict_retryable = applied_nodes == 0
                            || self
                                .partial_exact_metadata_command_conflict_is_retryable_with_route_mode(
                                    pg_id,
                                    &command,
                                    applied_nodes,
                                    &source,
                                    route_mode,
                                )?;
                        if exact_conflict_retryable
                            && self
                                .metadata_command_is_applied_on_all_acting_nodes_with_route_mode(
                                    pg_id, &command, route_mode,
                                )?
                        {
                            self.release_applied_metadata_command_bucket_write_reservations(
                                &command,
                            )?;
                            match route_mode {
                                MetadataCommandRouteMode::Normal => self
                                    .remove_pending_metadata_command_for_bucket(
                                        pg_id,
                                        command_bucket,
                                        &command,
                                    ),
                                MetadataCommandRouteMode::Recovery => self
                                    .remove_pending_metadata_command_for_bucket_recovery(
                                        pg_id,
                                        command_bucket,
                                        &command,
                                    ),
                            }?;
                            return Ok(FinishPendingMetadataCommandResult::Applied);
                        }
                        if exact_conflict_retryable && applied_nodes > 0 {
                            return Ok(
                                FinishPendingMetadataCommandResult::RetryPartialExactConflict,
                            );
                        }
                    }
                    if applied_nodes == 0
                        && super::StorageCluster::metadata_command_log_conflict_matches(
                            &command, &source,
                        )
                    {
                        let Some(reissued) = self
                            .reissue_pending_metadata_command_with_route_mode(
                                pg_id,
                                &command,
                                route_mode,
                                recovery_authorized_source.as_ref(),
                                None,
                                command.payload(),
                            )?
                        else {
                            return Ok(FinishPendingMetadataCommandResult::Abandoned);
                        };
                        command = reissued;
                        continue;
                    }
                    if clear_pending_on_zero_apply && applied_nodes == 0 {
                        match route_mode {
                            MetadataCommandRouteMode::Normal => {
                                self.record_abandoned_metadata_command_to_acting_set(&command)
                            }
                            MetadataCommandRouteMode::Recovery => self
                                .record_abandoned_metadata_command_to_acting_set_for_recovery(
                                    &command,
                                    recovery_authorized_source.as_ref(),
                                    None,
                                ),
                        }
                        .map_err(|error| error.source)?;
                        match route_mode {
                            MetadataCommandRouteMode::Normal => self
                                .remove_pending_metadata_command_for_bucket(
                                    pg_id,
                                    command_bucket,
                                    &command,
                                ),
                            MetadataCommandRouteMode::Recovery => self
                                .remove_pending_metadata_command_for_bucket_recovery(
                                    pg_id,
                                    command_bucket,
                                    &command,
                                ),
                        }?;
                    }
                    return Err(source);
                }
            }
        }
    }

    #[cfg(test)]
    pub(super) fn drain_bucket_pg_pending_metadata_command(
        &self,
        pg_id: PgId,
        _bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        clear_pending_on_zero_apply: bool,
    ) -> Result<super::PendingMetadataCommandOutcome, BucketSnapshotLoadError> {
        let mut work_budget = super::RequestWorkBudget::new(
            std::time::Duration::from_millis(METADATA_COMMAND_APPLY_RETRY_BUDGET_MILLIS),
            None,
        )
        .for_operation("metadata_command_pg_slot_drain")
        .for_pg(pg_id);
        self.drain_bucket_pg_pending_metadata_command_with_work_budget(
            pg_id,
            command,
            clear_pending_on_zero_apply,
            &mut work_budget,
        )
    }

    fn drain_bucket_pg_pending_metadata_command_with_work_budget(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        clear_pending_on_zero_apply: bool,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<super::PendingMetadataCommandOutcome, BucketSnapshotLoadError> {
        self.emit_pending_slot_action_for_command(pg_id, command, "drain_attempt");
        self.finish_pending_metadata_command_to_acting_set_with_work_budget(
            pg_id,
            command,
            clear_pending_on_zero_apply,
            work_budget,
        )
    }

    fn metadata_command_bucket_name(command: &MetadataCommandEnvelope) -> &BucketName {
        command.bucket_name()
    }

    fn drain_unrelated_pending_metadata_command_for_bucket_with_work_budget(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let pending_bucket = Self::metadata_command_bucket_name(command).clone();
        if pending_bucket == *bucket {
            return Ok(false);
        }
        self.drain_pending_metadata_command_pg_slot_with_work_budget(
            pg_id,
            &pending_bucket,
            command,
            work_budget,
        )?;
        Ok(true)
    }

    #[cfg(test)]
    pub(super) fn drain_pending_metadata_command_pg_slot(
        &self,
        pg_id: PgId,
        _pending_bucket: &BucketName,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), BucketSnapshotLoadError> {
        let _ = self
            .drain_pending_metadata_command_with_recovery_gate(pg_id, command)
            .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
        Ok(())
    }

    fn drain_pending_metadata_command_pg_slot_with_work_budget(
        &self,
        pg_id: PgId,
        _pending_bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<(), BucketSnapshotLoadError> {
        if Self::metadata_command_is_bucket_pg_command(command) {
            let outcome = self
                .finish_pending_metadata_command_to_acting_set_allow_partial_exact_conflict_retry_with_work_budget(
                    pg_id,
                    command,
                    false,
                    work_budget,
                )?;
            return match outcome {
                FinishPendingMetadataCommandResult::Applied
                | FinishPendingMetadataCommandResult::Abandoned => Ok(()),
                FinishPendingMetadataCommandResult::RetryPartialExactConflict => {
                    Err(conflicting_pending_metadata_command(
                        "retryable partial pending metadata command drain",
                    ))
                }
            };
        }

        let _ = self
            .drain_pending_metadata_command_with_recovery_gate_and_work_budget(
                pg_id,
                command,
                work_budget,
            )
            .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
        Ok(())
    }

    fn drain_pending_multipart_completion_barrier_command_with_work_budget(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<(), BucketSnapshotLoadError> {
        let _ = self.drain_bucket_pg_pending_metadata_command_with_work_budget(
            pg_id,
            command,
            false,
            work_budget,
        )?;
        Ok(())
    }

    fn next_bucket_metadata_command_id_or_drain_with_work_budget(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<Option<MetadataCommandId>, BucketSnapshotLoadError> {
        self.next_bucket_metadata_command_id_or_drain_inner(pg_id, bucket, false, work_budget)
    }

    fn next_completion_bucket_metadata_command_id_or_drain_with_work_budget(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<Option<MetadataCommandId>, BucketSnapshotLoadError> {
        self.next_bucket_metadata_command_id_or_drain_inner(pg_id, bucket, true, work_budget)
    }

    fn next_bucket_metadata_command_id_or_drain_inner(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        completion_admission: bool,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<Option<MetadataCommandId>, BucketSnapshotLoadError> {
        let command_id_result = if completion_admission {
            self.next_completion_bucket_metadata_command_id(pg_id)
        } else {
            self.next_bucket_metadata_command_id(pg_id)
        };
        match command_id_result {
            Ok(command_id) => Ok(Some(command_id)),
            Err(BucketSnapshotLoadError::Store(StoreError::MetadataCommandLogConflict {
                ..
            })) => {
                if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                    let pending_bucket = Self::metadata_command_bucket_name(&command).clone();
                    self.drain_pending_metadata_command_pg_slot_with_work_budget(
                        pg_id,
                        &pending_bucket,
                        &command,
                        work_budget,
                    )?;
                }
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    fn try_set_bucket_pg_pending_command_or_retry_with_work_budget(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<bool, BucketSnapshotLoadError> {
        self.try_set_bucket_pg_pending_command_or_retry_with_work_budget_and_effect_fence(
            pg_id,
            bucket,
            command,
            None,
            work_budget,
        )
    }

    fn try_set_bucket_pg_pending_command_or_retry_with_work_budget_and_effect_fence(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        effect_fence: Option<AdmittedRouteEffectFence>,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<bool, BucketSnapshotLoadError> {
        if effect_fence.is_some() {
            self.maybe_run_before_metadata_command_pending_install_hook();
        }
        match self.try_set_pending_metadata_command_for_bucket_with_effect_fence(
            pg_id,
            bucket,
            command,
            effect_fence,
        ) {
            Ok(Some(())) => Ok(true),
            Ok(None) => {
                if let Some(pending) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                    let pending_bucket = Self::metadata_command_bucket_name(&pending).clone();
                    self.drain_pending_metadata_command_pg_slot_with_work_budget(
                        pg_id,
                        &pending_bucket,
                        &pending,
                        work_budget,
                    )?;
                }
                Ok(false)
            }
            Err(StoreError::MetadataCommandLogConflict { .. }) => {
                if let Some(pending) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                    let pending_bucket = Self::metadata_command_bucket_name(&pending).clone();
                    self.drain_pending_metadata_command_pg_slot_with_work_budget(
                        pg_id,
                        &pending_bucket,
                        &pending,
                        work_budget,
                    )?;
                }
                Ok(false)
            }
            Err(error) => Err(error.into()),
        }
    }

    #[cfg(test)]
    pub(super) fn try_set_bucket_control_pending_command_or_retry(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let primary = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        match primary
            .metadata_command_client()
            .try_insert_bucket_control_pending_metadata_command_slot(pg_id, command, bucket)
        {
            Ok(true) => Ok(true),
            Ok(false) => {
                if let Some(pending) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                    let pending_bucket = Self::metadata_command_bucket_name(&pending).clone();
                    self.drain_pending_metadata_command_pg_slot(pg_id, &pending_bucket, &pending)?;
                    return Ok(false);
                }

                if primary
                    .bucket_write_reservation_client()
                    .durable_bucket_write_drain_exists(
                        self.validated_bucket_metadata_pg(pg_id),
                        bucket,
                    )?
                {
                    self.wait_for_durable_bucket_write_drain(bucket)?;
                    return Ok(false);
                }
                Ok(false)
            }
            Err(StoreError::MetadataCommandLogConflict { .. }) => {
                if let Some(pending) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                    let pending_bucket = Self::metadata_command_bucket_name(&pending).clone();
                    self.drain_pending_metadata_command_pg_slot(pg_id, &pending_bucket, &pending)?;
                }
                Ok(false)
            }
            Err(error) => Err(error.into()),
        }
    }

    fn try_set_bucket_control_pending_command_or_retry_with_work_budget(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        effect_fence: Option<AdmittedRouteEffectFence>,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let primary = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        self.maybe_run_before_metadata_command_pending_install_hook();
        let insert = match effect_fence {
            Some(effect_fence) => primary
                .metadata_command_client()
                .try_insert_bucket_control_pending_metadata_command_slot_with_effect_fence(
                    pg_id,
                    command,
                    bucket,
                    effect_fence,
                ),
            None => primary
                .metadata_command_client()
                .try_insert_bucket_control_pending_metadata_command_slot(pg_id, command, bucket),
        };
        match insert {
            Ok(true) => Ok(true),
            Ok(false) => {
                if let Some(pending) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                    let pending_bucket = Self::metadata_command_bucket_name(&pending).clone();
                    self.drain_pending_metadata_command_pg_slot_with_work_budget(
                        pg_id,
                        &pending_bucket,
                        &pending,
                        work_budget,
                    )?;
                    return Ok(false);
                }

                if primary
                    .bucket_write_reservation_client()
                    .durable_bucket_write_drain_exists(
                        self.validated_bucket_metadata_pg(pg_id),
                        bucket,
                    )?
                {
                    self.wait_for_durable_bucket_write_drain(bucket)?;
                    return Ok(false);
                }
                Ok(false)
            }
            Err(StoreError::MetadataCommandLogConflict { .. }) => {
                if let Some(pending) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                    let pending_bucket = Self::metadata_command_bucket_name(&pending).clone();
                    self.drain_pending_metadata_command_pg_slot_with_work_budget(
                        pg_id,
                        &pending_bucket,
                        &pending,
                        work_budget,
                    )?;
                }
                Ok(false)
            }
            Err(error) => Err(error.into()),
        }
    }

    fn finish_pending_command_for_multipart_completion_barrier(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<super::PendingMetadataCommandOutcome, ObjectPgActionError> {
        match command.payload() {
            MetadataCommandPayload::ReserveObjectGeneration(_)
            | MetadataCommandPayload::ReleaseObjectGeneration(_)
            | MetadataCommandPayload::ReserveObjectVersion(_)
            | MetadataCommandPayload::CommitDirectPutObject(_)
            | MetadataCommandPayload::CommitMultipartObject(_)
            | MetadataCommandPayload::DeleteObjectVersion(_)
            | MetadataCommandPayload::InsertDeleteMarker(_)
            | MetadataCommandPayload::PutObjectMetadata(_)
            | MetadataCommandPayload::CreateStreamUpload(_)
            | MetadataCommandPayload::AppendStreamSegment(_)
            | MetadataCommandPayload::AbortStreamUpload(_)
            | MetadataCommandPayload::CommitStreamPart(_)
            | MetadataCommandPayload::CreateMultipartUpload(_)
            | MetadataCommandPayload::AbortMultipartUpload(_)
            | MetadataCommandPayload::DeleteObjectPayloadReclaim(_) => {
                self.finish_object_pg_pending_slot(pg_id, command)
            }
            MetadataCommandPayload::CreateBucket(_)
            | MetadataCommandPayload::PutBucketVersioning(_)
            | MetadataCommandPayload::PutBucketAcl(_)
            | MetadataCommandPayload::PutBucketProperty(_)
            | MetadataCommandPayload::PutBucketSubresource(_)
            | MetadataCommandPayload::MarkBucketDeleting(_)
            | MetadataCommandPayload::DeleteFinalizedBucket(_)
            | MetadataCommandPayload::AdvanceMultipartCompletionBarrier(_) => self
                .drain_bucket_pg_pending_metadata_command_with_work_budget(
                    pg_id,
                    command,
                    false,
                    work_budget,
                )
                .map_err(super::bucket_snapshot_error_to_object_pg_action_error),
        }
    }

    fn delete_bucket_from_acting_set(
        &self,
        pg_id: PgId,
        root: &BucketDeleteFinalizeRoot,
    ) -> Result<BucketDeleteFinalizeOutcome, BucketWriteDrainError> {
        let bucket = &root.bucket;
        crate::metadata_command::metadata_command_publisher!(DeleteBucketFromActingSet);
        let _ = observability::event(
            super::TRACE_TARGET,
            "bucket_finalize_delete_start",
            Some(format_args!("bucket={:?} pg_id={}", bucket, pg_id.get())),
        );
        let mut work_budget = super::RequestWorkBudget::new(
            std::time::Duration::from_millis(BUCKET_DELETE_FINALIZE_WORK_BUDGET_MILLIS),
            None,
        )
        .for_operation("bucket_finalize_delete_command")
        .for_pg(pg_id);
        loop {
            work_budget
                .check("bucket finalized delete command budget exhausted")
                .map_err(BucketWriteDrainError::Store)?;
            let primary = self
                .local_map
                .metadata_pg_primary_node(self.operation_epoch(), pg_id)
                .map_err(BucketWriteDrainError::Store)?;
            let current_info = match primary
                .bucket_metadata_client()
                .head_bucket_raw(self.validated_bucket_metadata_pg(pg_id), bucket)
                .map_err(bucket_snapshot_error_to_bucket_write_drain_error)
            {
                Ok(info) => {
                    if info.bucket_incarnation_generation != root.bucket_incarnation_generation {
                        return Ok(BucketDeleteFinalizeOutcome::StaleIncarnation);
                    }
                    if info.state != BucketState::Deleting {
                        return Err(BucketWriteDrainError::Metadata(
                            MetadataError::BucketNotFinalizedForDelete { state: info.state },
                        ));
                    }
                    Some(info)
                }
                Err(BucketWriteDrainError::Metadata(MetadataError::BucketNotFound { .. })) => None,
                Err(other) => return Err(other),
            };
            let (command, clear_pending_on_zero_apply) = if let Some(command) = self
                .pending_metadata_command_for_bucket(pg_id, bucket)
                .map_err(BucketWriteDrainError::Store)?
            {
                if self
                    .drain_unrelated_pending_metadata_command_for_bucket_with_work_budget(
                        pg_id,
                        bucket,
                        &command,
                        &mut work_budget,
                    )
                    .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?
                {
                    continue;
                }
                match command.payload() {
                    MetadataCommandPayload::DeleteFinalizedBucket(delete)
                        if delete.bucket == *bucket
                            && delete.bucket_incarnation_generation
                                == root.bucket_incarnation_generation =>
                    {
                        (command, false)
                    }
                    MetadataCommandPayload::DeleteFinalizedBucket(_) => {
                        let _ = self
                            .drain_bucket_pg_pending_metadata_command_with_work_budget(
                                pg_id,
                                &command,
                                false,
                                &mut work_budget,
                            )
                            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
                        continue;
                    }
                    _ => {
                        self.drain_pending_metadata_command_pg_slot_with_work_budget(
                            pg_id,
                            bucket,
                            &command,
                            &mut work_budget,
                        )
                        .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
                        continue;
                    }
                }
            } else {
                let Some(info) = current_info else {
                    let _ = observability::event(
                        super::TRACE_TARGET,
                        "bucket_finalize_delete_primary_missing",
                        Some(format_args!("bucket={:?} pg_id={}", bucket, pg_id.get())),
                    );
                    return if self.finalized_bucket_deleted_on_acting_set(pg_id, root)? {
                        Ok(BucketDeleteFinalizeOutcome::NotFound)
                    } else {
                        Ok(BucketDeleteFinalizeOutcome::Pending)
                    };
                };
                let Some(command_id) = self
                    .next_bucket_metadata_command_id_or_drain_with_work_budget(
                        pg_id,
                        bucket,
                        &mut work_budget,
                    )
                    .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?
                else {
                    continue;
                };
                let command = MetadataCommandEnvelope::new(
                    command_id,
                    MetadataCommandPayload::DeleteFinalizedBucket(
                        DeleteFinalizedBucketCommand::new(
                            bucket.clone(),
                            info.bucket_execution_generation,
                            root.bucket_incarnation_generation,
                        ),
                    ),
                );
                if !self
                    .try_set_bucket_pg_pending_command_or_retry_with_work_budget(
                        pg_id,
                        bucket,
                        &command,
                        &mut work_budget,
                    )
                    .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?
                {
                    continue;
                }
                (command, true)
            };
            let outcome = self
                .finish_pending_metadata_command_to_acting_set_allow_partial_exact_conflict_retry_with_work_budget(
                    pg_id,
                    &command,
                    clear_pending_on_zero_apply,
                    &mut work_budget,
                )
                .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
            if matches!(
                outcome,
                FinishPendingMetadataCommandResult::Abandoned
                    | FinishPendingMetadataCommandResult::RetryPartialExactConflict
            ) {
                continue;
            }
            if self.finalized_bucket_deleted_on_acting_set_for_recovery(pg_id, root)? {
                break;
            }
        }
        let _ = observability::event(
            super::TRACE_TARGET,
            "bucket_finalize_delete_done",
            Some(format_args!("bucket={:?} pg_id={}", bucket, pg_id.get())),
        );
        Ok(BucketDeleteFinalizeOutcome::Finalized)
    }

    fn finalized_bucket_deleted_on_acting_set(
        &self,
        pg_id: PgId,
        root: &BucketDeleteFinalizeRoot,
    ) -> Result<bool, BucketWriteDrainError> {
        self.finalized_bucket_deleted_on_acting_set_with_route_mode(
            pg_id,
            root,
            MetadataCommandRouteMode::Normal,
        )
    }

    fn finalized_bucket_deleted_on_acting_set_for_recovery(
        &self,
        pg_id: PgId,
        root: &BucketDeleteFinalizeRoot,
    ) -> Result<bool, BucketWriteDrainError> {
        self.finalized_bucket_deleted_on_acting_set_with_route_mode(
            pg_id,
            root,
            MetadataCommandRouteMode::Recovery,
        )
    }

    fn finalized_bucket_deleted_on_acting_set_with_route_mode(
        &self,
        pg_id: PgId,
        root: &BucketDeleteFinalizeRoot,
        route_mode: MetadataCommandRouteMode,
    ) -> Result<bool, BucketWriteDrainError> {
        let bucket = &root.bucket;
        let nodes = match route_mode {
            MetadataCommandRouteMode::Normal => self
                .local_map
                .metadata_pg_acting_nodes(self.operation_epoch(), pg_id),
            MetadataCommandRouteMode::Recovery => self
                .local_map
                .metadata_pg_acting_nodes_for_metadata_command_recovery(
                    self.operation_epoch(),
                    pg_id,
                ),
        }
        .map_err(BucketWriteDrainError::Store)?;
        let mut found_deleting = false;
        for node in nodes {
            match node
                .bucket_metadata_client()
                .head_bucket_replica_for_delete(self.validated_bucket_metadata_pg(pg_id), bucket)
                .map_err(bucket_snapshot_error_to_bucket_write_drain_error)
            {
                Ok(info)
                    if info.bucket_incarnation_generation != root.bucket_incarnation_generation => {
                }
                Ok(info) if info.state == BucketState::Deleting => {
                    found_deleting = true;
                }
                Ok(info) => {
                    return Err(BucketWriteDrainError::Metadata(
                        MetadataError::BucketNotFinalizedForDelete { state: info.state },
                    ));
                }
                Err(BucketWriteDrainError::Metadata(MetadataError::BucketNotFound { .. })) => {}
                Err(other) => return Err(other),
            }
        }
        Ok(!found_deleting)
    }

    fn bucket_name_absent_on_acting_set(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<bool, BucketWriteDrainError> {
        let nodes = self
            .local_map
            .metadata_pg_acting_nodes(self.operation_epoch(), pg_id)
            .map_err(BucketWriteDrainError::Store)?;
        let mut found_deleting = false;
        for node in nodes {
            match node
                .bucket_metadata_client()
                .head_bucket_replica_for_delete(self.validated_bucket_metadata_pg(pg_id), bucket)
                .map_err(bucket_snapshot_error_to_bucket_write_drain_error)
            {
                Ok(info) if info.state == BucketState::Deleting => {
                    found_deleting = true;
                }
                Ok(info) => {
                    return Err(BucketWriteDrainError::Metadata(
                        MetadataError::BucketNotFinalizedForDelete { state: info.state },
                    ));
                }
                Err(BucketWriteDrainError::Metadata(MetadataError::BucketNotFound { .. })) => {}
                Err(other) => return Err(other),
            }
        }
        Ok(!found_deleting)
    }

    pub fn load_bucket_snapshot(
        &self,
        bucket: &BucketName,
        request: BucketSnapshotRequest,
    ) -> Result<BucketSnapshot, BucketSnapshotLoadError> {
        let pg_id = PgId::new(self.bucket_metadata_pg_id(bucket));
        self.local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?
            .bucket_metadata_client()
            .load_bucket_snapshot(self.validated_bucket_metadata_pg(pg_id), bucket, request)
    }

    /// Load the raw authorization snapshot used only by DeleteBucket retries
    /// after normal write-snapshot authorization found no visible bucket.
    ///
    /// DeleteBucket itself installs or observes the bucket write drain. Once a
    /// previous request has marked the bucket `Deleting`, normal bucket
    /// snapshots intentionally hide it as not found, but an idempotent retry
    /// still needs enough metadata to perform authorization and reach
    /// `begin_bucket_delete`, where `AlreadyDeleting` is handled. Callers must
    /// only accept this raw snapshot for an already-`Deleting` bucket; use
    /// `load_active_bucket_delete_attempt_authorization_snapshot` for the
    /// narrower active-bucket preserved-attempt path.
    pub fn load_bucket_delete_authorization_snapshot(
        &self,
        bucket: &BucketName,
        request: BucketSnapshotRequest,
    ) -> Result<BucketSnapshot, BucketSnapshotLoadError> {
        self.load_bucket_delete_authorization_snapshot_with_route_validation(
            super::BucketMetadataMutationEffectRoute {
                pg_id: self.bucket_metadata_pg(bucket),
                bucket,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            || Ok(()),
            request,
        )
    }

    pub(super) fn load_bucket_delete_authorization_snapshot_with_route_validation(
        &self,
        route: super::BucketMetadataMutationEffectRoute<'_>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        request: BucketSnapshotRequest,
    ) -> Result<BucketSnapshot, BucketSnapshotLoadError> {
        let super::BucketMetadataMutationEffectRoute {
            pg_id: bucket_pg_id,
            bucket,
            effect_fence: _,
        } = route;
        require_valid_route()?;
        let pg_id = bucket_pg_id.pg_id();
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let client = node.bucket_metadata_client();
        require_valid_route()?;
        let bucket_info = client.head_bucket_raw(bucket_pg_id, bucket)?;
        require_valid_route()?;
        let policy = Self::load_bucket_delete_authorization_subresource(
            &**client,
            bucket_pg_id,
            &bucket_info.name,
            request.policy,
            BucketSubresourceKind::Policy,
        )?;
        require_valid_route()?;
        let tags = Self::load_bucket_delete_authorization_tags(
            &**client,
            bucket_pg_id,
            &bucket_info.name,
            request.tags.should_load(&bucket_info),
        )?;
        require_valid_route()?;
        let lifecycle = Self::load_bucket_delete_authorization_subresource(
            &**client,
            bucket_pg_id,
            &bucket_info.name,
            request.lifecycle,
            BucketSubresourceKind::Lifecycle,
        )?;
        require_valid_route()?;
        let cors = Self::load_bucket_delete_authorization_subresource(
            &**client,
            bucket_pg_id,
            &bucket_info.name,
            request.cors,
            BucketSubresourceKind::Cors,
        )?;

        Ok(BucketSnapshot {
            bucket: bucket_info,
            request,
            policy,
            tags,
            lifecycle,
            cors,
        })
    }

    /// Load a raw Active-bucket authorization snapshot only after proving that
    /// a preserved DeleteBucket attempt has become a stable write fence.
    ///
    /// The ordering matters: older bucket-write reservations must be drained
    /// before reading policy/tag authorization inputs. Once the live drain is in
    /// place and the reservation list is empty, new bucket writes cannot acquire
    /// a reservation and older writes cannot commit after the snapshot.
    pub fn load_active_bucket_delete_attempt_authorization_snapshot(
        &self,
        bucket: &BucketName,
        request: BucketSnapshotRequest,
    ) -> Result<Option<BucketSnapshot>, BucketSnapshotLoadError> {
        self.load_active_bucket_delete_attempt_authorization_snapshot_with_route_validation(
            super::BucketMetadataMutationEffectRoute {
                pg_id: self.bucket_metadata_pg(bucket),
                bucket,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            || Ok(()),
            request,
        )
    }

    pub(super) fn load_active_bucket_delete_attempt_authorization_snapshot_with_route_validation(
        &self,
        route: super::BucketMetadataMutationEffectRoute<'_>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        request: BucketSnapshotRequest,
    ) -> Result<Option<BucketSnapshot>, BucketSnapshotLoadError> {
        let super::BucketMetadataMutationEffectRoute {
            pg_id: bucket_pg_id,
            bucket,
            effect_fence,
        } = route;
        require_valid_route()?;
        let pg_id = bucket_pg_id.pg_id();
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let reservation_client = node.bucket_write_reservation_client();
        require_valid_route()?;
        let Some(drain) = reservation_client.durable_bucket_write_drain(bucket_pg_id, bucket)?
        else {
            return Ok(None);
        };
        if drain.lease_deadline <= crate::clock::current_time_millis() {
            return Ok(None);
        }
        if !reservation_client
            .durable_bucket_write_reservations(bucket_pg_id, bucket)?
            .is_empty()
        {
            return Ok(None);
        }
        require_valid_route()?;
        let pending = self.pending_metadata_command_for_bucket(pg_id, bucket)?;
        if pending.is_some_and(|command| {
            !matches!(
                command.payload(),
                MetadataCommandPayload::MarkBucketDeleting(mark) if mark.bucket_name() == bucket
            )
        }) {
            return Ok(None);
        }

        let snapshot = match self.load_bucket_delete_authorization_snapshot_with_route_validation(
            super::BucketMetadataMutationEffectRoute {
                pg_id: bucket_pg_id,
                bucket,
                effect_fence,
            },
            &mut require_valid_route,
            request,
        ) {
            Ok(snapshot) => snapshot,
            Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketNotFound { .. })) => {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        if snapshot.bucket.state != BucketState::Active
            || snapshot.bucket.bucket_execution_generation != drain.bucket_execution_generation
        {
            return Ok(None);
        }
        Ok(Some(snapshot))
    }

    fn load_bucket_delete_authorization_subresource(
        client: &dyn crate::node_client::BucketMetadataNodeClient,
        pg_id: BucketPgId,
        bucket: &BucketName,
        requested: bool,
        kind: BucketSubresourceKind,
    ) -> Result<LoadedBucketSubresource<String>, BucketSnapshotLoadError> {
        if !requested {
            return Ok(LoadedBucketSubresource::NotRequested);
        }
        Ok(match client.get_bucket_subresource(pg_id, bucket, kind)? {
            Some(body) => LoadedBucketSubresource::Loaded(body),
            None => LoadedBucketSubresource::Missing,
        })
    }

    fn load_bucket_delete_authorization_tags(
        client: &dyn crate::node_client::BucketMetadataNodeClient,
        pg_id: BucketPgId,
        bucket: &BucketName,
        requested: bool,
    ) -> Result<LoadedBucketSubresource<SerializedBucketTagSet>, BucketSnapshotLoadError> {
        if !requested {
            return Ok(LoadedBucketSubresource::NotRequested);
        }
        Ok(match client.get_bucket_tags(pg_id, bucket)? {
            Some(tags) => LoadedBucketSubresource::Loaded(tags),
            None => LoadedBucketSubresource::Missing,
        })
    }

    pub fn load_available_bucket_execution_generation_batches(
        &self,
        buckets: &[BucketName],
    ) -> Vec<(Vec<BucketName>, HashMap<BucketName, u64>)> {
        let mut buckets_by_pg = HashMap::<u32, Vec<BucketName>>::new();
        for bucket in buckets {
            buckets_by_pg
                .entry(self.bucket_metadata_pg_id(bucket))
                .or_default()
                .push(bucket.clone());
        }

        let mut batches = Vec::new();
        for (pg_id, buckets) in buckets_by_pg {
            let pg_id = PgId::new(pg_id);
            let Ok(node) = self
                .local_map
                .metadata_pg_primary_node(self.operation_epoch(), pg_id)
            else {
                continue;
            };
            let Ok(generations) = node
                .bucket_metadata_client()
                .load_bucket_execution_generations(
                    self.validated_bucket_metadata_pg(pg_id),
                    &buckets,
                )
            else {
                continue;
            };
            batches.push((buckets, generations));
        }
        batches
    }

    pub fn load_bucket_fast_path_identity(
        &self,
        bucket: &BucketName,
    ) -> Result<Option<BucketFastPathIdentity>, BucketSnapshotLoadError> {
        let pg_id = PgId::new(self.bucket_metadata_pg_id(bucket));
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let mut identities = node
            .bucket_metadata_client()
            .load_bucket_fast_path_identities(
                self.validated_bucket_metadata_pg(pg_id),
                std::slice::from_ref(bucket),
            )?;
        Ok(identities.remove(bucket))
    }

    pub fn with_bucket_write_snapshot<T, E>(
        &self,
        bucket: &BucketName,
        request: BucketSnapshotRequest,
        action: impl FnOnce(BucketSnapshot) -> Result<T, E>,
    ) -> Result<Result<T, E>, BucketSnapshotLoadError> {
        self.with_bucket_write_snapshot_with_route_validation(
            super::BucketMetadataMutationEffectRoute {
                pg_id: self.bucket_metadata_pg(bucket),
                bucket,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            || Ok(()),
            request,
            action,
        )
    }

    pub(super) fn with_bucket_write_snapshot_with_route_validation<T, E>(
        &self,
        route: super::BucketMetadataMutationEffectRoute<'_>,
        require_valid_route: impl FnMut() -> Result<(), StoreError>,
        request: BucketSnapshotRequest,
        action: impl FnOnce(BucketSnapshot) -> Result<T, E>,
    ) -> Result<Result<T, E>, BucketSnapshotLoadError> {
        self.with_bucket_write_reservation_snapshot_with_route_validation(
            route,
            require_valid_route,
            request,
            |snapshot| Ok(action(snapshot)),
        )
    }

    pub fn with_bucket_write_snapshot_for_command<T, E>(
        &self,
        bucket: &BucketName,
        request: BucketSnapshotRequest,
        action: impl FnOnce(
            BucketSnapshot,
            BucketWriteReservationProof,
        )
            -> Result<super::BucketWriteSnapshotAction<T, E>, BucketSnapshotLoadError>,
    ) -> Result<Result<T, E>, BucketSnapshotLoadError> {
        let pg_id = PgId::new(self.bucket_metadata_pg_id(bucket));
        let mut work_budget =
            super::RequestWorkBudget::new(super::BUCKET_WRITE_DRAIN_RETRY_BUDGET, None)
                .for_operation("bucket_write_snapshot_for_command")
                .for_pg(pg_id);
        loop {
            work_budget.check("bucket write snapshot retry budget exhausted")?;
            let reservation = match self.acquire_durable_bucket_write_reservation(
                bucket,
                "bucket-write-snapshot",
                None,
            ) {
                Ok(reservation) => reservation,
                Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteDraining)) => {
                    self.wait_for_durable_bucket_write_drain(bucket)?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let proof = BucketWriteReservationProof::from(&reservation.record);

            let result = (|| {
                let snapshot = reservation.node.load_bucket_snapshot(
                    self.validated_bucket_metadata_pg(PgId::new(reservation.pg_id)),
                    bucket,
                    request,
                )?;
                action(snapshot, proof)
            })();
            let (result, release_result) = match result {
                Ok(super::BucketWriteSnapshotAction::Release(result)) => (
                    Ok(result),
                    self.release_durable_bucket_write_reservation(reservation),
                ),
                Ok(super::BucketWriteSnapshotAction::TransferredToCommand(result)) => {
                    (Ok(result), Ok(()))
                }
                Err(error) => (
                    Err(error),
                    self.release_durable_bucket_write_reservation(reservation),
                ),
            };
            return Self::finish_bucket_write_snapshot_operation(result, release_result);
        }
    }

    pub(super) fn with_put_object_bucket_write_snapshot_for_command_with_route_validation<T, E>(
        &self,
        route: super::PutObjectMutationEffectRoute<'_>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        request: BucketSnapshotRequest,
        action: impl FnOnce(
            BucketSnapshot,
            BucketWriteReservationProof,
        ) -> super::BucketWriteSnapshotAction<T, E>,
    ) -> Result<Result<T, E>, BucketSnapshotLoadError> {
        let super::PutObjectMutationEffectRoute {
            bucket_pg_id,
            bucket,
            effect_fence,
            ..
        } = route;
        let pg_id = bucket_pg_id.pg_id();
        let mut work_budget =
            super::RequestWorkBudget::new(super::BUCKET_WRITE_DRAIN_RETRY_BUDGET, None)
                .for_operation("put_object_bucket_write_snapshot_for_command")
                .for_pg(pg_id);
        loop {
            work_budget.check("put object bucket write snapshot retry budget exhausted")?;
            require_valid_route()?;
            let reservation = match self.acquire_durable_bucket_write_reservation_with_effect_fence(
                bucket,
                PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
                Some(route.key.as_str()),
                Some(effect_fence),
            ) {
                Ok(reservation) => reservation,
                Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteDraining)) => {
                    self.wait_for_durable_bucket_write_drain(bucket)?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let proof = BucketWriteReservationProof::from(&reservation.record);

            let result = (|| {
                require_valid_route()?;
                let storage_client = &reservation.node;
                let snapshot =
                    storage_client.load_bucket_snapshot(bucket_pg_id, bucket, request)?;
                Ok(action(snapshot, proof))
            })();
            let (result, release_result) = match result {
                Ok(super::BucketWriteSnapshotAction::Release(result)) => (
                    Ok(result),
                    self.release_durable_bucket_write_reservation(reservation),
                ),
                Ok(super::BucketWriteSnapshotAction::TransferredToCommand(result)) => {
                    (Ok(result), Ok(()))
                }
                Err(error) => (
                    Err(error),
                    self.release_durable_bucket_write_reservation(reservation),
                ),
            };
            return Self::finish_bucket_write_snapshot_operation(result, release_result);
        }
    }

    fn with_bucket_write_reservation_snapshot_with_route_validation<T, E>(
        &self,
        route: super::BucketMetadataMutationEffectRoute<'_>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        request: BucketSnapshotRequest,
        action: impl FnOnce(BucketSnapshot) -> Result<Result<T, E>, BucketSnapshotLoadError>,
    ) -> Result<Result<T, E>, BucketSnapshotLoadError> {
        let super::BucketMetadataMutationEffectRoute {
            pg_id: bucket_pg_id,
            bucket,
            effect_fence,
        } = route;
        let pg_id = bucket_pg_id.pg_id();
        let mut work_budget =
            super::RequestWorkBudget::new(super::BUCKET_WRITE_DRAIN_RETRY_BUDGET, None)
                .for_operation("bucket_write_reservation_snapshot")
                .for_pg(pg_id);
        loop {
            work_budget.check("bucket write reservation snapshot retry budget exhausted")?;
            require_valid_route()?;
            let reservation = match self.acquire_durable_bucket_write_reservation_with_effect_fence(
                bucket,
                "bucket-write-snapshot",
                None,
                Some(effect_fence),
            ) {
                Ok(reservation) => reservation,
                Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteDraining)) => {
                    self.wait_for_durable_bucket_write_drain(bucket)?;
                    continue;
                }
                Err(error) => return Err(error),
            };

            let result = (|| {
                require_valid_route()?;
                let storage_client = &reservation.node;
                let snapshot =
                    storage_client.load_bucket_snapshot(bucket_pg_id, bucket, request)?;
                action(snapshot)
            })();
            let release_result = self.release_durable_bucket_write_reservation(reservation);
            return Self::finish_bucket_write_snapshot_operation(result, release_result);
        }
    }

    pub(super) fn acquire_durable_bucket_write_reservation(
        &self,
        bucket: &BucketName,
        operation_kind: &'static str,
        target_context: Option<&str>,
    ) -> Result<super::DurableBucketWriteReservation, BucketSnapshotLoadError> {
        self.acquire_durable_bucket_write_reservation_with_effect_fence(
            bucket,
            operation_kind,
            target_context,
            None,
        )
    }

    pub(super) fn acquire_durable_bucket_write_reservation_with_effect_fence(
        &self,
        bucket: &BucketName,
        operation_kind: &'static str,
        target_context: Option<&str>,
        effect_fence: Option<AdmittedRouteEffectFence>,
    ) -> Result<super::DurableBucketWriteReservation, BucketSnapshotLoadError> {
        let pg_id = self.bucket_metadata_pg_id(bucket);
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), PgId::new(pg_id))?;
        let reservation_id = self.next_bucket_write_reservation_id()?;
        let owner_token = self.bucket_write_owner_token();
        let client = node.bucket_write_reservation_client();
        let acquire = DurableBucketWriteReservationAcquire {
            name: bucket,
            reservation_id: &reservation_id,
            owner_token: &owner_token,
            cluster_epoch: self.operation_epoch(),
            operation_kind,
            created_at: crate::clock::current_time_millis(),
            lease_deadline: self.bucket_write_reservation_lease_deadline(),
            target_context,
        };
        let record = match effect_fence {
            Some(effect_fence) => client
                .acquire_durable_bucket_write_reservation_with_effect_fence(
                    self.validated_bucket_metadata_pg(PgId::new(pg_id)),
                    acquire,
                    effect_fence,
                )?,
            None => client.acquire_durable_bucket_write_reservation(
                self.validated_bucket_metadata_pg(PgId::new(pg_id)),
                acquire,
            )?,
        };
        Ok(super::DurableBucketWriteReservation {
            node: Arc::clone(node.bucket_metadata_client()),
            pg_id,
            record,
        })
    }

    #[cfg(test)]
    pub(super) fn acquire_completion_durable_bucket_write_reservation(
        &self,
        bucket: &BucketName,
        operation_kind: &'static str,
        target_context: Option<&str>,
    ) -> Result<super::DurableBucketWriteReservation, BucketSnapshotLoadError> {
        let pg_id = self.bucket_metadata_pg_id(bucket);
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), PgId::new(pg_id))?;
        let reservation_id = self.next_bucket_write_reservation_id()?;
        let owner_token = self.bucket_write_owner_token();
        let record = node
            .bucket_write_reservation_client()
            .acquire_completion_durable_bucket_write_reservation(
                self.validated_bucket_metadata_pg(PgId::new(pg_id)),
                DurableBucketWriteReservationAcquire {
                    name: bucket,
                    reservation_id: &reservation_id,
                    owner_token: &owner_token,
                    cluster_epoch: self.operation_epoch(),
                    operation_kind,
                    created_at: crate::clock::current_time_millis(),
                    lease_deadline: self.bucket_write_reservation_lease_deadline(),
                    target_context,
                },
            )?;
        Ok(super::DurableBucketWriteReservation {
            node: Arc::clone(node.bucket_metadata_client()),
            pg_id,
            record,
        })
    }

    pub(super) fn acquire_completion_durable_bucket_write_reservation_with_effect_fence(
        &self,
        bucket: &BucketName,
        operation_kind: &'static str,
        target_context: Option<&str>,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<super::DurableBucketWriteReservation, BucketSnapshotLoadError> {
        let pg_id = self.bucket_metadata_pg_id(bucket);
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), PgId::new(pg_id))?;
        let reservation_id = self.next_bucket_write_reservation_id()?;
        let owner_token = self.bucket_write_owner_token();
        let record = node
            .bucket_write_reservation_client()
            .acquire_completion_durable_bucket_write_reservation_with_effect_fence(
                self.validated_bucket_metadata_pg(PgId::new(pg_id)),
                DurableBucketWriteReservationAcquire {
                    name: bucket,
                    reservation_id: &reservation_id,
                    owner_token: &owner_token,
                    cluster_epoch: self.operation_epoch(),
                    operation_kind,
                    created_at: crate::clock::current_time_millis(),
                    lease_deadline: self.bucket_write_reservation_lease_deadline(),
                    target_context,
                },
                effect_fence,
            )?;
        Ok(super::DurableBucketWriteReservation {
            node: Arc::clone(node.bucket_metadata_client()),
            pg_id,
            record,
        })
    }

    pub(super) fn put_object_stream_create_lease_deadline(&self) -> u64 {
        crate::clock::current_time_millis().saturating_add(PUT_OBJECT_STREAM_CREATE_LEASE_MILLIS)
    }

    pub(super) fn bucket_write_reservation_lease_deadline(&self) -> u64 {
        crate::clock::current_time_millis().saturating_add(BUCKET_WRITE_RESERVATION_LEASE_MILLIS)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn heartbeat_put_object_stream_session(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<(), ObjectPgActionError> {
        self.heartbeat_put_object_stream_session_with_route_validation(
            super::PutObjectMutationEffectRoute {
                bucket_pg_id: self.bucket_metadata_pg(bucket),
                object_pg_id: self.object_metadata_pg(bucket, key),
                bucket,
                key,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            session_id,
            || Ok(()),
        )
    }

    pub(super) fn heartbeat_put_object_stream_session_with_route_validation(
        &self,
        route: super::PutObjectMutationEffectRoute<'_>,
        session_id: &SessionId,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<(), ObjectPgActionError> {
        let super::PutObjectMutationEffectRoute {
            bucket_pg_id,
            object_pg_id,
            bucket,
            key,
            effect_fence,
        } = route;
        require_valid_route()?;
        let upload = self
            .object_mutation_metadata_primary_client(bucket, key)?
            .load_stream_upload_session(object_pg_id, bucket, key, session_id)?;
        if upload.target != StreamUploadTarget::PutObject {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "stream session is not a PutObject session".to_string(),
            });
        }
        let Some(stored_proof) = upload.bucket_write_reservation.as_ref() else {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "PutObject stream session is missing bucket write proof".to_string(),
            });
        };
        let mut proof = stored_proof.clone();
        require_valid_route()?;
        let pg_id = bucket_pg_id.pg_id();
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)
            .map_err(ObjectPgActionError::from)?;
        let renewed = node
            .bucket_write_reservation_client()
            .heartbeat_durable_bucket_write_reservation_with_effect_fence(
                bucket_pg_id,
                &proof,
                self.put_object_stream_create_lease_deadline(),
                effect_fence,
            );
        let renewed = match renewed {
            Ok(record) => record,
            Err(BucketSnapshotLoadError::Metadata(
                MetadataError::BucketWriteReservationConflict { .. },
            )) => {
                let Some(refreshed) = self
                    .refresh_stream_upload_bucket_write_reservation_for_object_action_with_route_validation(
                        route,
                        &upload,
                        &proof,
                        &mut require_valid_route,
                    )?
                else {
                    return Err(ObjectPgActionError::Metadata(
                        MetadataError::BucketWriteReservationConflict {
                            reservation_id: proof.reservation_id.clone(),
                        },
                    ));
                };
                proof = refreshed;
                require_valid_route()?;
                node.bucket_write_reservation_client()
                    .heartbeat_durable_bucket_write_reservation_with_effect_fence(
                        bucket_pg_id,
                        &proof,
                        self.put_object_stream_create_lease_deadline(),
                        effect_fence,
                    )
                    .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?
            }
            Err(error) => {
                return Err(super::bucket_snapshot_error_to_object_pg_action_error(
                    error,
                ))
            }
        };
        let renewed_proof = BucketWriteReservationProof::from(&renewed);
        require_valid_route()?;
        self.object_mutation_metadata_primary_client(bucket, key)?
            .update_stream_upload_bucket_write_reservation_with_effect_fence(
                UpdateStreamUploadBucketWriteReservationReq {
                    pg_id: object_pg_id,
                    bucket,
                    key,
                    session_id,
                    current: &proof,
                    renewed: &renewed_proof,
                    effect_fence,
                },
            )?;
        Ok(())
    }

    pub(super) fn release_durable_bucket_write_reservation(
        &self,
        reservation: super::DurableBucketWriteReservation,
    ) -> Result<(), BucketSnapshotLoadError> {
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), PgId::new(reservation.pg_id))?;
        node.retained_bucket_write_reservation_client()
            .open_retained_bucket_write_reservation_route(
                self.validated_bucket_metadata_pg(PgId::new(reservation.pg_id)),
                &reservation.record.bucket,
            )?
            .release_durable_bucket_write_reservation(&reservation.record)?;
        Ok(())
    }

    pub(super) fn wait_for_durable_bucket_write_drain(
        &self,
        bucket: &BucketName,
    ) -> Result<(), BucketSnapshotLoadError> {
        let pg_id = PgId::new(self.bucket_metadata_pg_id(bucket));
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        match node
            .bucket_metadata_client()
            .head_bucket_info(self.validated_bucket_metadata_pg(pg_id), bucket)
        {
            Ok(_) => {}
            Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketNotFound { .. })) => {
                return Err(MetadataError::BucketNotFound {
                    name: bucket.clone(),
                }
                .into());
            }
            Err(other) => return Err(other),
        }
        if let Some(expired) = node
            .bucket_write_reservation_client()
            .clear_expired_durable_bucket_write_drain(
                self.validated_bucket_metadata_pg(pg_id),
                bucket,
                crate::clock::current_time_millis(),
            )?
        {
            let _ = observability::event(
                super::TRACE_TARGET,
                "bucket_write_expired_drain_rollback",
                Some(format_args!(
                    "bucket={:?} pg_id={} drain_id={}",
                    bucket,
                    pg_id.get(),
                    expired.drain_id
                )),
            );
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(super) fn begin_durable_bucket_delete_drain(
        &self,
        bucket: &BucketName,
    ) -> Result<super::DurableBucketDeleteDrainBegin, BucketWriteDrainError> {
        self.begin_durable_bucket_delete_drain_with_budget(bucket, None)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_seed_bucket_delete_attempt_outcome(
        &self,
        bucket: &BucketName,
        outcome: crate::TestBucketDeleteAttemptOutcomeKind,
        phase: crate::TestBucketDeleteAttemptPhase,
        detail: String,
        post_reservation_next_object_pg_id: Option<u32>,
    ) -> Result<(), BucketWriteDrainError> {
        let drain = match self.begin_durable_bucket_delete_drain(bucket)? {
            super::DurableBucketDeleteDrainBegin::Acquired(drain) => drain,
            super::DurableBucketDeleteDrainBegin::AlreadyDeleting => {
                return Err(StoreError::MetadataCommandContention {
                    context: "test seed bucket delete attempt outcome already deleting",
                }
                .into());
            }
        };
        let pg_id = PgId::new(drain.pg_id);
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)
            .map_err(BucketWriteDrainError::from)?;
        let record = BucketDeleteAttemptOutcomeRecord {
            bucket: drain.record.bucket.clone(),
            drain_id: drain.record.drain_id.clone(),
            cluster_epoch: drain.record.cluster_epoch,
            bucket_execution_generation: drain.record.bucket_execution_generation,
            outcome: outcome.into(),
            phase: phase.into(),
            detail,
            post_reservation_next_object_pg_id,
            finalizer_next_object_pg_id: None,
            updated_at: crate::clock::current_time_millis(),
        };
        node.bucket_write_reservation_client()
            .record_bucket_delete_attempt_outcome(self.validated_bucket_metadata_pg(pg_id), &record)
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn begin_durable_bucket_delete_drain_with_budget(
        &self,
        bucket: &BucketName,
        started: Option<std::time::Instant>,
    ) -> Result<super::DurableBucketDeleteDrainBegin, BucketWriteDrainError> {
        let mut require_valid_route = || Ok(());
        self.begin_durable_bucket_delete_drain_with_budget_and_route_validation(
            bucket,
            started,
            None,
            &mut require_valid_route,
        )
    }

    fn begin_durable_bucket_delete_drain_with_budget_and_route_validation(
        &self,
        bucket: &BucketName,
        started: Option<std::time::Instant>,
        effect_fence: Option<AdmittedRouteEffectFence>,
        require_valid_route: &mut impl FnMut() -> Result<(), StoreError>,
    ) -> Result<super::DurableBucketDeleteDrainBegin, BucketWriteDrainError> {
        loop {
            self.check_bucket_delete_begin_work_budget(
                bucket,
                started,
                "bucket delete durable drain acquisition budget exhausted",
            )?;
            require_valid_route()?;
            let pg_id = self.bucket_metadata_pg_id(bucket);
            let node = self
                .local_map
                .metadata_pg_primary_node(self.operation_epoch(), PgId::new(pg_id))?;
            let drain_id = self.next_bucket_write_drain_id()?;
            let owner_token = self.bucket_write_owner_token();
            let now = crate::clock::current_time_millis();
            // DeleteBucket begin work is bounded. Give a live caller a small
            // grace window, but make an abandoned Active-bucket drain
            // recoverable by later write-snapshot waiters and delete retries.
            let lease_deadline = now.saturating_add(BUCKET_DELETE_DRAIN_LEASE_MILLIS);
            require_valid_route()?;
            crate::node::maybe_run_before_begin_bucket_delete_drain_hook(bucket);
            let begin_result = match effect_fence {
                Some(effect_fence) => node
                    .bucket_write_reservation_client()
                    .begin_durable_bucket_write_drain_with_effect_fence(
                        self.validated_bucket_metadata_pg(PgId::new(pg_id)),
                        bucket,
                        &drain_id,
                        &owner_token,
                        self.operation_epoch(),
                        now,
                        lease_deadline,
                        effect_fence,
                    ),
                None => node
                    .bucket_write_reservation_client()
                    .begin_durable_bucket_write_drain(
                        self.validated_bucket_metadata_pg(PgId::new(pg_id)),
                        bucket,
                        &drain_id,
                        &owner_token,
                        self.operation_epoch(),
                        now,
                        lease_deadline,
                    ),
            };
            match begin_result {
                Ok(record) => {
                    return Ok(super::DurableBucketDeleteDrainBegin::Acquired(
                        super::DurableBucketWriteDrain { pg_id, record },
                    ))
                }
                Err(BucketSnapshotLoadError::Metadata(
                    MetadataError::BucketWriteDrainConflict { .. },
                )) => {
                    if let Some(expired) = node
                        .bucket_write_reservation_client()
                        .clear_expired_durable_bucket_write_drain(
                            self.validated_bucket_metadata_pg(PgId::new(pg_id)),
                            bucket,
                            crate::clock::current_time_millis(),
                        )
                        .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?
                    {
                        let _ = observability::event(
                            super::TRACE_TARGET,
                            "bucket_delete_expired_drain_rollback",
                            Some(format_args!(
                                "bucket={:?} pg_id={} drain_id={}",
                                bucket, pg_id, expired.drain_id
                            )),
                        );
                        continue;
                    }
                    if let Some(existing) = node
                        .bucket_write_reservation_client()
                        .durable_bucket_write_drain(
                            self.validated_bucket_metadata_pg(PgId::new(pg_id)),
                            bucket,
                        )
                        .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?
                    {
                        match node.bucket_metadata_client().head_bucket_raw(
                            self.validated_bucket_metadata_pg(PgId::new(pg_id)),
                            bucket,
                        ) {
                            Ok(current)
                                if current.state == BucketState::Active
                                    && current.bucket_execution_generation
                                        == existing.bucket_execution_generation =>
                            {
                                let _ = observability::event(
                                    super::TRACE_TARGET,
                                    "bucket_delete_drain_adopted",
                                    Some(format_args!(
                                        "bucket={:?} pg_id={} drain_id={}",
                                        bucket, pg_id, existing.drain_id
                                    )),
                                );
                                let renewed = self.heartbeat_durable_bucket_delete_drain(
                                    &super::DurableBucketWriteDrain {
                                        pg_id,
                                        record: existing,
                                    },
                                )?;
                                return Ok(super::DurableBucketDeleteDrainBegin::Acquired(renewed));
                            }
                            Ok(current)
                                if current.state == BucketState::Active
                                    && current.bucket_execution_generation
                                        != existing.bucket_execution_generation =>
                            {
                                self.record_bucket_delete_attempt_outcome_for_record(
                                    PgId::new(pg_id),
                                    &existing,
                                    BucketDeleteAttemptOutcomeKind::StaleGeneration,
                                    BucketDeleteAttemptPhase::Initial,
                                    format!(
                                        "stale drain generation {} current generation {}",
                                        existing.bucket_execution_generation,
                                        current.bucket_execution_generation
                                    ),
                                );
                                node.retained_bucket_write_reservation_client()
                                    .open_retained_bucket_write_reservation_route(
                                        self.validated_bucket_metadata_pg(PgId::new(pg_id)),
                                        &existing.bucket,
                                    )
                                    .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?
                                    .clear_durable_bucket_write_drain(&existing)
                                    .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
                                let _ = observability::event(
                                    super::TRACE_TARGET,
                                    "bucket_delete_stale_drain_rollback",
                                    Some(format_args!(
                                        "bucket={:?} pg_id={} drain_id={} drain_generation={} current_generation={}",
                                        bucket,
                                        pg_id,
                                        existing.drain_id,
                                        existing.bucket_execution_generation,
                                        current.bucket_execution_generation
                                    )),
                                );
                                continue;
                            }
                            Ok(_) => {}
                            Err(BucketSnapshotLoadError::Metadata(
                                MetadataError::BucketNotFound { .. },
                            )) => {
                                return Err(MetadataError::BucketNotFound {
                                    name: bucket.clone(),
                                }
                                .into());
                            }
                            Err(error) => {
                                return Err(bucket_snapshot_error_to_bucket_write_drain_error(
                                    error,
                                ));
                            }
                        }
                    }
                    match node.bucket_metadata_client().head_bucket_raw(
                        self.validated_bucket_metadata_pg(PgId::new(pg_id)),
                        bucket,
                    ) {
                        Ok(current) if current.state == BucketState::Deleting => {
                            return Ok(super::DurableBucketDeleteDrainBegin::AlreadyDeleting);
                        }
                        Ok(_) => {}
                        Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketNotFound {
                            ..
                        })) => {
                            return Err(MetadataError::BucketNotFound {
                                name: bucket.clone(),
                            }
                            .into());
                        }
                        Err(error) => {
                            return Err(bucket_snapshot_error_to_bucket_write_drain_error(error));
                        }
                    }
                    std::thread::sleep(std::time::Duration::from_millis(1));
                    continue;
                }
                Err(error) => return Err(bucket_snapshot_error_to_bucket_write_drain_error(error)),
            }
        }
    }

    pub(super) fn clear_durable_bucket_delete_drain(
        &self,
        drain: &super::DurableBucketWriteDrain,
    ) -> Result<(), BucketWriteDrainError> {
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), PgId::new(drain.pg_id))?;
        node.retained_bucket_write_reservation_client()
            .open_retained_bucket_write_reservation_route(
                self.validated_bucket_metadata_pg(PgId::new(drain.pg_id)),
                &drain.record.bucket,
            )
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?
            .clear_durable_bucket_write_drain(&drain.record)
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        Ok(())
    }

    fn rollback_durable_bucket_delete_drain(
        &self,
        drain: &super::DurableBucketWriteDrain,
    ) -> Result<(), BucketWriteDrainError> {
        match self.clear_durable_bucket_delete_drain(drain) {
            Ok(()) => Ok(()),
            Err(BucketWriteDrainError::Metadata(
                MetadataError::BucketWriteDrainNotFound { .. }
                | MetadataError::BucketNotFound { .. },
            )) => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn bounded_bucket_delete_attempt_detail(mut detail: String) -> String {
        if detail.len() > BUCKET_DELETE_ATTEMPT_OUTCOME_DETAIL_MAX_LEN {
            let mut end = BUCKET_DELETE_ATTEMPT_OUTCOME_DETAIL_MAX_LEN;
            while !detail.is_char_boundary(end) {
                end -= 1;
            }
            detail.truncate(end);
        }
        detail
    }

    fn record_bucket_delete_attempt_outcome_for_record(
        &self,
        pg_id: PgId,
        record: &BucketWriteDrainRecord,
        outcome: BucketDeleteAttemptOutcomeKind,
        phase: BucketDeleteAttemptPhase,
        detail: String,
    ) {
        let result = (|| {
            let node = self
                .local_map
                .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
            self.record_bucket_delete_attempt_outcome_with_client(
                node.bucket_write_reservation_client().as_ref(),
                self.validated_bucket_metadata_pg(pg_id),
                record,
                outcome,
                phase,
                detail,
            );
            Ok::<(), BucketWriteDrainError>(())
        })();
        if let Err(error) = result {
            let _ = observability::event(
                super::TRACE_TARGET,
                "bucket_delete_attempt_outcome_route_failed",
                Some(format_args!(
                    "bucket={:?} pg_id={} drain_id={} outcome={:?} error={:?}",
                    record.bucket,
                    pg_id.get(),
                    record.drain_id,
                    outcome,
                    error
                )),
            );
        }
    }

    fn record_bucket_delete_attempt_outcome_with_client(
        &self,
        client: &dyn BucketWriteReservationNodeClient,
        pg_id: BucketPgId,
        record: &BucketWriteDrainRecord,
        outcome: BucketDeleteAttemptOutcomeKind,
        phase: BucketDeleteAttemptPhase,
        detail: String,
    ) {
        let detail = Self::bounded_bucket_delete_attempt_detail(detail);
        let existing = match client.bucket_delete_attempt_outcome(pg_id, &record.bucket) {
            Ok(existing) => existing.filter(|existing| {
                existing.drain_id == record.drain_id
                    && existing.cluster_epoch == record.cluster_epoch
                    && existing.bucket_execution_generation == record.bucket_execution_generation
            }),
            Err(error) => {
                let _ = observability::event(
                    super::TRACE_TARGET,
                    "bucket_delete_attempt_outcome_progress_load_failed",
                    Some(format_args!(
                        "bucket={:?} pg_id={} drain_id={} outcome={:?} error={:?}",
                        record.bucket,
                        pg_id.get(),
                        record.drain_id,
                        outcome,
                        error
                    )),
                );
                None
            }
        };
        let post_reservation_next_object_pg_id = existing
            .as_ref()
            .and_then(|existing| existing.post_reservation_next_object_pg_id);
        let finalizer_next_object_pg_id = existing
            .as_ref()
            .and_then(|existing| existing.finalizer_next_object_pg_id);
        let outcome_record = BucketDeleteAttemptOutcomeRecord {
            bucket: record.bucket.clone(),
            drain_id: record.drain_id.clone(),
            cluster_epoch: record.cluster_epoch,
            bucket_execution_generation: record.bucket_execution_generation,
            outcome,
            phase,
            detail,
            post_reservation_next_object_pg_id,
            finalizer_next_object_pg_id,
            updated_at: crate::clock::current_time_millis(),
        };
        if let Err(error) = client.record_bucket_delete_attempt_outcome(pg_id, &outcome_record) {
            let _ = observability::event(
                super::TRACE_TARGET,
                "bucket_delete_attempt_outcome_record_failed",
                Some(format_args!(
                    "bucket={:?} pg_id={} drain_id={} outcome={:?} error={:?}",
                    record.bucket,
                    pg_id.get(),
                    record.drain_id,
                    outcome,
                    error
                )),
            );
        }
    }

    fn record_bucket_delete_attempt_outcome_for_drain_with_client(
        &self,
        client: &dyn BucketWriteReservationNodeClient,
        drain: &super::DurableBucketWriteDrain,
        outcome: BucketDeleteAttemptOutcomeKind,
        phase: BucketDeleteAttemptPhase,
        detail: String,
    ) {
        self.record_bucket_delete_attempt_outcome_with_client(
            client,
            self.validated_bucket_metadata_pg(PgId::new(drain.pg_id)),
            &drain.record,
            outcome,
            phase,
            detail,
        );
    }

    fn heartbeat_durable_bucket_delete_drain(
        &self,
        drain: &super::DurableBucketWriteDrain,
    ) -> Result<super::DurableBucketWriteDrain, BucketWriteDrainError> {
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), PgId::new(drain.pg_id))?;
        let lease_deadline =
            crate::clock::current_time_millis().saturating_add(BUCKET_DELETE_DRAIN_LEASE_MILLIS);
        match node
            .bucket_write_reservation_client()
            .heartbeat_durable_bucket_write_drain(
                self.validated_bucket_metadata_pg(PgId::new(drain.pg_id)),
                &drain.record,
                lease_deadline,
            ) {
            Ok(record) => Ok(super::DurableBucketWriteDrain {
                pg_id: drain.pg_id,
                record,
            }),
            Err(BucketSnapshotLoadError::Metadata(
                MetadataError::BucketWriteDrainConflict { .. }
                | MetadataError::BucketWriteDrainNotFound { .. },
            )) => Err(StoreError::MetadataCommandContention {
                context: "stale bucket delete drain before mark deleting",
            }
            .into()),
            Err(error) => Err(bucket_snapshot_error_to_bucket_write_drain_error(error)),
        }
    }

    fn wait_for_durable_bucket_write_reservations_empty(
        &self,
        bucket: &BucketName,
        client: &dyn BucketWriteReservationNodeClient,
        drain: &super::DurableBucketWriteDrain,
        delete_started: std::time::Instant,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<(), BucketWriteDrainError> {
        let started = std::time::Instant::now();
        loop {
            let pg_id = self.bucket_metadata_pg_id(bucket);
            let node = self
                .local_map
                .metadata_pg_primary_node(self.operation_epoch(), PgId::new(pg_id))?;
            let reservations = node
                .bucket_write_reservation_client()
                .durable_bucket_write_reservations(
                    self.validated_bucket_metadata_pg(PgId::new(pg_id)),
                    bucket,
                )
                .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
            let now = crate::clock::current_time_millis();
            let expired: Vec<_> = reservations
                .iter()
                .filter(|reservation| reservation.lease_deadline <= now)
                .cloned()
                .collect();
            if !expired.is_empty() {
                for reservation in expired {
                    match node
                        .retained_bucket_write_reservation_client()
                        .open_retained_bucket_write_reservation_route(
                            self.validated_bucket_metadata_pg(PgId::new(pg_id)),
                            &reservation.bucket,
                        )
                        .and_then(|route| {
                            route.release_durable_bucket_write_reservation(&reservation)
                        }) {
                        Ok(()) => {
                            let _ = observability::event(
                                super::TRACE_TARGET,
                                "bucket_write_expired_reservation_release",
                                Some(format_args!(
                                    "bucket={:?} pg_id={} reservation_id={} operation_kind={}",
                                    bucket,
                                    pg_id,
                                    reservation.reservation_id,
                                    reservation.operation_kind
                                )),
                            );
                        }
                        Err(BucketSnapshotLoadError::Metadata(
                            MetadataError::BucketWriteReservationNotFound { .. },
                        )) => {}
                        Err(error) => {
                            return Err(bucket_snapshot_error_to_bucket_write_drain_error(error));
                        }
                    }
                }
                continue;
            }
            if reservations.is_empty() {
                return Ok(());
            }
            if let Err(error) = self.check_bucket_delete_begin_work_budget(
                bucket,
                Some(delete_started),
                "bucket delete reservation wait begin budget exhausted",
            ) {
                self.record_bucket_delete_reservation_wait_blocker(
                    client,
                    drain,
                    &reservations,
                    "begin budget exhausted",
                );
                let _ = error;
                return Err(bucket_snapshot_error_to_bucket_write_drain_error(
                    conflicting_pending_metadata_command(
                        BUCKET_DELETE_RESERVATION_WAIT_BLOCKED_CONTEXT,
                    ),
                ));
            }
            if started.elapsed()
                >= std::time::Duration::from_millis(BUCKET_DELETE_RESERVATION_DRAIN_WAIT_MILLIS)
            {
                let _ = observability::event(
                    super::TRACE_TARGET,
                    "bucket_delete_reservation_wait_timeout",
                    Some(format_args!(
                        "bucket={:?} pg_id={} {}",
                        bucket,
                        pg_id,
                        Self::bucket_delete_reservation_wait_blocker_detail(
                            &reservations,
                            "timeout",
                        )
                    )),
                );
                self.record_bucket_delete_reservation_wait_blocker(
                    client,
                    drain,
                    &reservations,
                    "timeout",
                );
                return Err(bucket_snapshot_error_to_bucket_write_drain_error(
                    conflicting_pending_metadata_command(
                        BUCKET_DELETE_RESERVATION_WAIT_BLOCKED_CONTEXT,
                    ),
                ));
            }
            self.drain_pending_object_metadata_commands_for_exact_bucket_on_all_pgs_with_budget(
                bucket,
                Some(delete_started),
                work_budget,
                None,
            )?;
            crate::node::maybe_run_bucket_write_drain_wait_hook(bucket);
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    fn record_bucket_delete_reservation_wait_blocker(
        &self,
        client: &dyn BucketWriteReservationNodeClient,
        drain: &super::DurableBucketWriteDrain,
        reservations: &[BucketWriteReservationRecord],
        reason: &'static str,
    ) {
        self.record_bucket_delete_attempt_outcome_for_drain_with_client(
            client,
            drain,
            BucketDeleteAttemptOutcomeKind::Retryable,
            BucketDeleteAttemptPhase::ReservationWait,
            Self::bucket_delete_reservation_wait_blocker_detail(reservations, reason),
        );
    }

    fn bucket_delete_reservation_wait_blocker_detail(
        reservations: &[BucketWriteReservationRecord],
        reason: &'static str,
    ) -> String {
        let now = crate::clock::current_time_millis();
        let first = reservations
            .first()
            .expect("reservation-wait blocker detail requires at least one reservation");
        let first_lease_state = if first.lease_deadline <= now {
            "expired"
        } else {
            "live"
        };
        format!(
            "reservation wait {reason}: reservations={} first_reservation_id={} first_operation_kind={} first_target_context={:?} first_lease_state={}",
            reservations.len(),
            first.reservation_id,
            first.operation_kind,
            first.target_context,
            first_lease_state
        )
    }

    pub(super) fn stream_upload_has_live_bucket_write_reservation(
        &self,
        upload: &StreamUploadRecord,
    ) -> Result<bool, BucketWriteDrainError> {
        let Some(proof) = upload.bucket_write_reservation.as_ref() else {
            return Ok(false);
        };
        let pg_id = PgId::new(self.bucket_metadata_pg_id(&proof.bucket));
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        match node
            .bucket_write_reservation_client()
            .validate_bucket_write_reservation_proof(
                self.validated_bucket_metadata_pg(pg_id),
                proof,
            ) {
            Ok(()) => self.refresh_stream_upload_bucket_write_reservation(upload, proof),
            Err(BucketSnapshotLoadError::Metadata(
                MetadataError::BucketWriteReservationNotFound { .. },
            )) => Ok(false),
            Err(BucketSnapshotLoadError::Metadata(
                MetadataError::BucketWriteReservationConflict { .. },
            )) => self.refresh_stream_upload_bucket_write_reservation(upload, proof),
            Err(error) => Err(bucket_snapshot_error_to_bucket_write_drain_error(error)),
        }
    }

    fn refresh_stream_upload_bucket_write_reservation(
        &self,
        upload: &StreamUploadRecord,
        proof: &BucketWriteReservationProof,
    ) -> Result<bool, BucketWriteDrainError> {
        let pg_id = PgId::new(self.bucket_metadata_pg_id(&proof.bucket));
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let now = crate::clock::current_time_millis();
        let reservations = node
            .bucket_write_reservation_client()
            .durable_bucket_write_reservations(
                self.validated_bucket_metadata_pg(pg_id),
                &proof.bucket,
            )
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        let Some(current) = reservations
            .into_iter()
            .find(|record| proof.matches_record(record) && record.lease_deadline > now)
        else {
            return Ok(false);
        };
        let renewed = BucketWriteReservationProof::from(&current);
        if *proof == renewed {
            return Ok(true);
        }
        self.object_mutation_metadata_primary_client(&upload.bucket, &upload.key)
            .map_err(BucketWriteDrainError::Store)?
            .update_stream_upload_bucket_write_reservation(
                self.object_metadata_pg(&upload.bucket, &upload.key),
                &upload.bucket,
                &upload.key,
                &upload.session_id,
                proof,
                &renewed,
            )
            .map_err(super::object_pg_action_error_to_bucket_snapshot_error)
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        Ok(true)
    }

    fn refresh_stream_upload_bucket_write_reservation_for_object_action_with_route_validation(
        &self,
        route: super::PutObjectMutationEffectRoute<'_>,
        upload: &StreamUploadRecord,
        proof: &BucketWriteReservationProof,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<Option<BucketWriteReservationProof>, ObjectPgActionError> {
        let super::PutObjectMutationEffectRoute {
            bucket_pg_id,
            object_pg_id,
            bucket,
            key,
            effect_fence,
        } = route;
        if upload.bucket != *bucket || upload.key != *key || proof.bucket != *bucket {
            return Err(ObjectPgActionError::Store(
                StoreError::RouteCapabilitySubjectMismatch {
                    operation: "refresh put object stream reservation",
                },
            ));
        }
        require_valid_route()?;
        let pg_id = bucket_pg_id.pg_id();
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)
            .map_err(ObjectPgActionError::from)?;
        let now = crate::clock::current_time_millis();
        let reservations = node
            .bucket_write_reservation_client()
            .durable_bucket_write_reservations(bucket_pg_id, bucket)
            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
        let Some(current) = reservations
            .into_iter()
            .find(|record| proof.matches_record(record) && record.lease_deadline > now)
        else {
            return Ok(None);
        };
        let renewed = BucketWriteReservationProof::from(&current);
        require_valid_route()?;
        self.object_mutation_metadata_primary_client(bucket, key)?
            .update_stream_upload_bucket_write_reservation_with_effect_fence(
                UpdateStreamUploadBucketWriteReservationReq {
                    pg_id: object_pg_id,
                    bucket,
                    key,
                    session_id: &upload.session_id,
                    current: proof,
                    renewed: &renewed,
                    effect_fence,
                },
            )?;
        Ok(Some(renewed))
    }

    fn active_put_object_stream_upload_source(
        &self,
        bucket: &BucketName,
    ) -> Result<Option<BucketVisibleDataSource>, BucketWriteDrainError> {
        const STREAM_UPLOAD_SCAN_PAGE_LIMIT: u32 = 128;

        for raw_pg_id in self.metadata_pg_ids() {
            let pg_id = PgId::new(raw_pg_id);
            let scan_pg_id = self.object_metadata_scan_pg(pg_id);
            let mut marker = None;
            loop {
                let node = self
                    .local_map
                    .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
                let page = node
                    .object_mutation_metadata_client()
                    .list_stream_uploads_for_bucket_page(
                        scan_pg_id,
                        bucket,
                        marker.as_ref(),
                        STREAM_UPLOAD_SCAN_PAGE_LIMIT,
                    )
                    .map_err(super::object_pg_action_error_to_bucket_snapshot_error)
                    .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
                if page.uploads.is_empty() {
                    break;
                }

                for upload in page.uploads {
                    if self.object_metadata_pg_id(&upload.bucket, &upload.key) != raw_pg_id {
                        return Err(BucketWriteDrainError::Store(StoreError::Io {
                            context: "bucket delete active stream upload PG validation",
                            source: std::io::Error::other(format!(
                                "stream upload session {:?} for bucket {:?} key {:?} is stored on PG {}",
                                upload.session_id,
                                upload.bucket,
                                upload.key,
                                raw_pg_id
                            )),
                        }));
                    }
                    if upload.target != crate::StreamUploadTarget::PutObject {
                        continue;
                    }
                    if self.stream_upload_has_live_bucket_write_reservation(&upload)? {
                        return Ok(Some(BucketVisibleDataSource::StreamUpload { pg_id }));
                    }
                }

                let Some(next_marker) = page.next_session_id_marker else {
                    break;
                };
                marker = Some(next_marker);
            }
        }

        Ok(None)
    }

    fn bucket_delete_not_empty_error(
        &self,
        bucket: &BucketName,
        pg_id: PgId,
        source: BucketVisibleDataSource,
    ) -> BucketWriteDrainError {
        let _ = observability::emit_flight_event(
            super::TRACE_TARGET,
            "bucket_delete_begin_not_empty",
            format!(
                "bucket={:?} pg_id={} source={} source_pg_id={}",
                bucket,
                pg_id.get(),
                source.label(),
                source.pg_id().get()
            ),
        );
        if bucket_delete_visible_data_diagnostics_enabled() {
            eprintln!(
                "bucket delete begin found visible data source={} source_pg_id={}",
                source.label(),
                source.pg_id().get()
            );
        }
        crate::error::MetadataError::BucketNotEmpty.into()
    }

    fn emit_bucket_delete_begin_loop_step(
        bucket: &BucketName,
        pg_id: PgId,
        started: std::time::Instant,
        step: &'static str,
        detail: impl Into<String>,
    ) {
        let detail = detail.into();
        let suffix = if detail.is_empty() {
            String::new()
        } else {
            format!(" {detail}")
        };
        let _ = observability::emit_flight_event(
            super::TRACE_TARGET,
            "bucket_delete_begin_loop_step",
            format!(
                "bucket={:?} pg_id={} step={} elapsed_us={}{}",
                bucket,
                pg_id.get(),
                step,
                started.elapsed().as_micros(),
                suffix
            ),
        );
    }

    fn drain_pending_object_metadata_commands_for_exact_bucket_on_all_pgs_with_budget(
        &self,
        bucket: &BucketName,
        started: Option<std::time::Instant>,
        work_budget: &mut super::RequestWorkBudget,
        progress: Option<BucketDeleteExactDrainProgress<'_>>,
    ) -> Result<(), BucketWriteDrainError> {
        let object_pg_ids: Vec<PgId> = self.metadata_pg_ids().into_iter().map(PgId::new).collect();
        let next_object_pg_id = match progress {
            Some(progress) => self
                .bucket_delete_post_reservation_next_object_pg_id(progress)?
                .unwrap_or(0),
            None => 0,
        };
        #[cfg(any(test, feature = "test-hooks"))]
        maybe_run_before_bucket_delete_exact_drain_hook(
            self.metadata_command_apply_test_hook_scope_id(),
            progress.is_some(),
            next_object_pg_id,
        )
        .map_err(BucketWriteDrainError::from)?;
        let mut scanned_count = 0usize;
        let mut drained_count = 0usize;
        for object_pg_id in object_pg_ids
            .iter()
            .copied()
            .filter(|object_pg_id| object_pg_id.get() >= next_object_pg_id)
        {
            self.check_bucket_delete_begin_work_budget(
                bucket,
                started,
                BUCKET_DELETE_EXACT_BUCKET_DRAIN_BUDGET_EXHAUSTED_CONTEXT,
            )?;
            if let Some(started) = started {
                Self::emit_bucket_delete_begin_loop_step(
                    bucket,
                    self.bucket_metadata_pg_id(bucket).into(),
                    started,
                    "probe_exact_bucket_object_pg_start",
                    format!("object_pg_id={}", object_pg_id.get()),
                );
            }
        }
        for chunk in object_pg_ids
            .iter()
            .copied()
            .filter(|object_pg_id| object_pg_id.get() >= next_object_pg_id)
            .collect::<Vec<_>>()
            .chunks(BUCKET_DELETE_EXACT_BUCKET_PENDING_PROBE_PARALLELISM)
        {
            let chunk_pending =
                self.pending_exact_bucket_metadata_commands_on_pgs(bucket, chunk, started)?;
            scanned_count += chunk.len();
            if let Some(started) = started {
                Self::emit_bucket_delete_begin_loop_step(
                    bucket,
                    self.bucket_metadata_pg_id(bucket).into(),
                    started,
                    "probe_exact_bucket_object_pg_chunk_done",
                    format!(
                        "chunk_pg_count={} chunk_exact_pending_count={} scanned_count={}",
                        chunk.len(),
                        chunk_pending.len(),
                        scanned_count
                    ),
                );
            }
            for (object_pg_id, command) in chunk_pending {
                self.check_bucket_delete_begin_work_budget(
                    bucket,
                    started,
                    BUCKET_DELETE_EXACT_BUCKET_DRAIN_BUDGET_EXHAUSTED_CONTEXT,
                )?;
                if let Some(started) = started {
                    Self::emit_bucket_delete_begin_loop_step(
                        bucket,
                        self.bucket_metadata_pg_id(bucket).into(),
                        started,
                        "drain_exact_bucket_object_pg_start",
                        format!(
                            "object_pg_id={} command_kind={}",
                            object_pg_id.get(),
                            command.payload().kind_name()
                        ),
                    );
                }
                self.drain_pending_object_metadata_commands_for_exact_bucket_with_work_budget(
                    object_pg_id,
                    bucket,
                    work_budget,
                )
                .map_err(super::object_pg_action_error_to_bucket_snapshot_error)
                .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
                drained_count += 1;
                if let Some(started) = started {
                    Self::emit_bucket_delete_begin_loop_step(
                        bucket,
                        self.bucket_metadata_pg_id(bucket).into(),
                        started,
                        "drain_exact_bucket_object_pg_done",
                        format!("object_pg_id={}", object_pg_id.get()),
                    );
                }
            }
            if let (Some(progress), Some(last_pg)) = (progress, chunk.last()) {
                let next_object_pg_id = last_pg.get().saturating_add(1);
                self.record_bucket_delete_post_reservation_next_object_pg_id(
                    progress,
                    next_object_pg_id,
                )?;
                #[cfg(any(test, feature = "test-hooks"))]
                if progress.phase == BucketDeleteAttemptPhase::PostReservationObjectDrain {
                    maybe_run_after_bucket_delete_post_reservation_progress_hook(
                        self.metadata_command_apply_test_hook_scope_id(),
                        next_object_pg_id,
                    )
                    .map_err(BucketWriteDrainError::from)?;
                }
            }
        }
        self.check_bucket_delete_begin_work_budget(
            bucket,
            started,
            BUCKET_DELETE_EXACT_BUCKET_DRAIN_BUDGET_EXHAUSTED_CONTEXT,
        )?;
        if let Some(started) = started {
            Self::emit_bucket_delete_begin_loop_step(
                bucket,
                self.bucket_metadata_pg_id(bucket).into(),
                started,
                "probe_exact_bucket_object_pgs_done",
                format!(
                    "object_pg_count={} skipped_before_pg={} scanned_count={} drained_count={}",
                    object_pg_ids.len(),
                    next_object_pg_id,
                    scanned_count,
                    drained_count
                ),
            );
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn test_record_bucket_delete_post_reservation_next_object_pg_id(
        &self,
        drain: &super::DurableBucketWriteDrain,
        next_object_pg_id: u32,
    ) -> Result<(), BucketWriteDrainError> {
        let pg_id = PgId::new(drain.pg_id);
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        self.record_bucket_delete_post_reservation_next_object_pg_id(
            BucketDeleteExactDrainProgress {
                client: node.bucket_write_reservation_client().as_ref(),
                drain,
                phase: BucketDeleteAttemptPhase::PostReservationObjectDrain,
            },
            next_object_pg_id,
        )
    }

    #[cfg(test)]
    pub(crate) fn test_bucket_delete_post_reservation_next_object_pg_id(
        &self,
        drain: &super::DurableBucketWriteDrain,
    ) -> Result<Option<u32>, BucketWriteDrainError> {
        let pg_id = PgId::new(drain.pg_id);
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        self.bucket_delete_post_reservation_next_object_pg_id(BucketDeleteExactDrainProgress {
            client: node.bucket_write_reservation_client().as_ref(),
            drain,
            phase: BucketDeleteAttemptPhase::PostReservationObjectDrain,
        })
    }

    #[cfg(test)]
    pub(crate) fn test_drain_pending_object_metadata_commands_for_exact_bucket_after_reservation(
        &self,
        bucket: &BucketName,
        drain: &super::DurableBucketWriteDrain,
    ) -> Result<(), BucketWriteDrainError> {
        let pg_id = PgId::new(drain.pg_id);
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let mut work_budget = super::RequestWorkBudget::new(
            std::time::Duration::from_millis(BUCKET_DELETE_BEGIN_WORK_BUDGET_MILLIS),
            None,
        )
        .for_operation("bucket_delete_begin")
        .for_pg(pg_id);
        self.drain_pending_object_metadata_commands_for_exact_bucket_on_all_pgs_with_budget(
            bucket,
            None,
            &mut work_budget,
            Some(BucketDeleteExactDrainProgress {
                client: node.bucket_write_reservation_client().as_ref(),
                drain,
                phase: BucketDeleteAttemptPhase::PostReservationObjectDrain,
            }),
        )
    }

    fn pending_exact_bucket_metadata_commands_on_pgs(
        &self,
        bucket: &BucketName,
        object_pg_ids: &[PgId],
        started: Option<std::time::Instant>,
    ) -> Result<Vec<(PgId, MetadataCommandEnvelope)>, BucketWriteDrainError> {
        let mut exact_pending = Vec::new();
        let mut chunk_pending = std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(object_pg_ids.len());
            for &object_pg_id in object_pg_ids {
                handles.push((
                    object_pg_id,
                    scope.spawn(move || {
                        self.pending_metadata_command_for_bucket(object_pg_id, bucket)
                    }),
                ));
            }

            let mut chunk_pending = Vec::new();
            for (object_pg_id, handle) in handles {
                let pending = match handle.join() {
                    Ok(result) => result.map_err(BucketWriteDrainError::from)?,
                    Err(payload) => std::panic::resume_unwind(payload),
                };
                if let Some(command) = pending.filter(|command| command.bucket_name() == bucket) {
                    chunk_pending.push((object_pg_id, command));
                }
            }
            Ok::<_, BucketWriteDrainError>(chunk_pending)
        })?;
        exact_pending.append(&mut chunk_pending);
        self.check_bucket_delete_begin_work_budget(
            bucket,
            started,
            BUCKET_DELETE_EXACT_BUCKET_DRAIN_BUDGET_EXHAUSTED_CONTEXT,
        )?;
        Ok(exact_pending)
    }

    fn bucket_delete_post_reservation_next_object_pg_id(
        &self,
        progress: BucketDeleteExactDrainProgress<'_>,
    ) -> Result<Option<u32>, BucketWriteDrainError> {
        let record = self.bucket_delete_matching_attempt_outcome(
            progress.client,
            self.validated_bucket_metadata_pg(PgId::new(progress.drain.pg_id)),
            progress.drain,
        )?;
        Ok(record.and_then(|record| record.post_reservation_next_object_pg_id))
    }

    fn bucket_delete_matching_attempt_outcome(
        &self,
        client: &dyn BucketWriteReservationNodeClient,
        pg_id: BucketPgId,
        drain: &super::DurableBucketWriteDrain,
    ) -> Result<Option<BucketDeleteAttemptOutcomeRecord>, BucketWriteDrainError> {
        let record = client
            .bucket_delete_attempt_outcome(pg_id, &drain.record.bucket)
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        Ok(record.filter(|record| {
            record.drain_id == drain.record.drain_id
                && record.cluster_epoch == drain.record.cluster_epoch
                && record.bucket_execution_generation == drain.record.bucket_execution_generation
        }))
    }

    fn record_bucket_delete_post_reservation_next_object_pg_id(
        &self,
        progress: BucketDeleteExactDrainProgress<'_>,
        next_object_pg_id: u32,
    ) -> Result<(), BucketWriteDrainError> {
        let existing = match progress.client.bucket_delete_attempt_outcome(
            self.validated_bucket_metadata_pg(PgId::new(progress.drain.pg_id)),
            &progress.drain.record.bucket,
        ) {
            Ok(existing) => existing,
            Err(error) => {
                let _ = observability::event(
                    super::TRACE_TARGET,
                    "bucket_delete_attempt_progress_load_failed",
                    Some(format_args!(
                        "bucket={:?} pg_id={} drain_id={} next_object_pg_id={} error={:?}",
                        progress.drain.record.bucket,
                        progress.drain.pg_id,
                        progress.drain.record.drain_id,
                        next_object_pg_id,
                        error
                    )),
                );
                return Err(bucket_snapshot_error_to_bucket_write_drain_error(error));
            }
        };
        let existing = existing.filter(|record| {
            record.drain_id == progress.drain.record.drain_id
                && record.cluster_epoch == progress.drain.record.cluster_epoch
                && record.bucket_execution_generation
                    == progress.drain.record.bucket_execution_generation
        });
        let finalizer_next_object_pg_id = existing
            .as_ref()
            .and_then(|record| record.finalizer_next_object_pg_id);
        let (outcome, phase, detail) = existing
            .map(|record| (record.outcome, record.phase, record.detail))
            .unwrap_or_else(|| {
                (
                    BucketDeleteAttemptOutcomeKind::Retryable,
                    progress.phase,
                    format!(
                        "{:?} exact-bucket drain progressed to object PG {next_object_pg_id}",
                        progress.phase
                    ),
                )
            });
        let detail = Self::bounded_bucket_delete_attempt_detail(detail);
        let record = BucketDeleteAttemptOutcomeRecord {
            bucket: progress.drain.record.bucket.clone(),
            drain_id: progress.drain.record.drain_id.clone(),
            cluster_epoch: progress.drain.record.cluster_epoch,
            bucket_execution_generation: progress.drain.record.bucket_execution_generation,
            outcome,
            phase,
            detail,
            post_reservation_next_object_pg_id: Some(next_object_pg_id),
            finalizer_next_object_pg_id,
            updated_at: crate::clock::current_time_millis(),
        };
        if let Err(error) = progress.client.record_bucket_delete_attempt_outcome(
            self.validated_bucket_metadata_pg(PgId::new(progress.drain.pg_id)),
            &record,
        ) {
            let _ = observability::event(
                super::TRACE_TARGET,
                "bucket_delete_attempt_progress_record_failed",
                Some(format_args!(
                    "bucket={:?} pg_id={} drain_id={} next_object_pg_id={} error={:?}",
                    progress.drain.record.bucket,
                    progress.drain.pg_id,
                    progress.drain.record.drain_id,
                    next_object_pg_id,
                    error
                )),
            );
            return Err(bucket_snapshot_error_to_bucket_write_drain_error(error));
        }
        #[cfg(any(test, feature = "test-hooks"))]
        maybe_run_after_bucket_delete_exact_drain_progress_hook(
            self.metadata_command_apply_test_hook_scope_id(),
            progress.phase,
            next_object_pg_id,
        )
        .map_err(BucketWriteDrainError::from)?;
        Ok(())
    }

    fn check_bucket_delete_begin_work_budget(
        &self,
        bucket: &BucketName,
        started: Option<std::time::Instant>,
        context: &'static str,
    ) -> Result<(), BucketWriteDrainError> {
        self.require_route_map_valid_now()?;
        let Some(started) = started else {
            return Ok(());
        };
        if started.elapsed()
            < std::time::Duration::from_millis(BUCKET_DELETE_BEGIN_WORK_BUDGET_MILLIS)
        {
            return Ok(());
        }
        let _ = observability::emit_flight_event(
            super::TRACE_TARGET,
            "bucket_delete_begin_work_budget_exhausted",
            format!("bucket={:?} context={}", bucket, context),
        );
        let _ = observability::emit_metadata_command_budget_exhausted(
            super::TRACE_TARGET,
            observability::MetadataCommandBudgetExhaustedSummary {
                pg_id: Some(self.bucket_metadata_pg_id(bucket)),
                operation: "bucket_delete_begin",
                context,
                elapsed_us: started.elapsed().as_micros(),
                budget_us: u128::from(BUCKET_DELETE_BEGIN_WORK_BUDGET_MILLIS) * 1_000,
                attempts: 0,
                max_attempts: None,
            },
        );
        Err(bucket_snapshot_error_to_bucket_write_drain_error(
            conflicting_pending_metadata_command(context),
        ))
    }

    fn finish_bucket_write_snapshot_operation<T, E>(
        result: Result<Result<T, E>, BucketSnapshotLoadError>,
        release_result: Result<(), BucketSnapshotLoadError>,
    ) -> Result<Result<T, E>, BucketSnapshotLoadError> {
        match (result, release_result) {
            (Ok(Ok(value)), Ok(())) => Ok(Ok(value)),
            (Ok(Ok(_)), Err(err)) => Err(err),
            (Ok(Err(err)), Ok(())) => Ok(Err(err)),
            (Ok(Err(err)), Err(_)) => Ok(Err(err)),
            (Err(err), Ok(())) => Err(err),
            (Err(err), Err(_)) => Err(err),
        }
    }

    pub fn load_bucket_snapshot_pair(
        &self,
        source: (&BucketName, BucketSnapshotRequest),
        destination: (&BucketName, BucketSnapshotRequest),
    ) -> Result<BucketSnapshotPair, BucketSnapshotLoadError> {
        if source.0 == destination.0 {
            let merged_request = source.1.union(destination.1);
            let bucket = self.load_bucket_snapshot(source.0, merged_request)?;
            return Ok(BucketSnapshotPair::Same {
                bucket: Box::new(bucket),
            });
        }

        let source_bucket_pg_id = self.bucket_metadata_pg(source.0);
        let destination_bucket_pg_id = self.bucket_metadata_pg(destination.0);
        let source_pg_id = source_bucket_pg_id.pg_id();
        let destination_pg_id = destination_bucket_pg_id.pg_id();
        let source_node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), source_pg_id)?;
        let destination_node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), destination_pg_id)?;
        if source_node.node_id() == destination_node.node_id() {
            return source_node
                .bucket_metadata_client()
                .load_bucket_snapshot_pair(
                    source_bucket_pg_id,
                    source,
                    destination_bucket_pg_id,
                    destination,
                );
        }

        let (source_snapshot, destination_snapshot) = if source_pg_id.get()
            < destination_pg_id.get()
        {
            (
                source_node.bucket_metadata_client().load_bucket_snapshot(
                    source_bucket_pg_id,
                    source.0,
                    source.1,
                )?,
                destination_node
                    .bucket_metadata_client()
                    .load_bucket_snapshot(destination_bucket_pg_id, destination.0, destination.1)?,
            )
        } else {
            let destination_snapshot = destination_node
                .bucket_metadata_client()
                .load_bucket_snapshot(destination_bucket_pg_id, destination.0, destination.1)?;
            let source_snapshot = source_node.bucket_metadata_client().load_bucket_snapshot(
                source_bucket_pg_id,
                source.0,
                source.1,
            )?;
            (source_snapshot, destination_snapshot)
        };

        Ok(BucketSnapshotPair::Distinct {
            source: Box::new(source_snapshot),
            destination: Box::new(destination_snapshot),
        })
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn begin_bucket_delete_if_current(
        &self,
        bucket: &BucketName,
        bucket_identity: BucketIdentityGenerations,
    ) -> Result<(), BucketWriteDrainError> {
        self.begin_bucket_delete_if_current_with_route_validation(
            super::BucketMetadataMutationEffectRoute {
                pg_id: self.bucket_metadata_pg(bucket),
                bucket,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            || Ok(()),
            bucket_identity,
        )
    }

    /// Continue a durably recorded DeleteBucket attempt adopted by background
    /// recovery on the currently installed route.
    ///
    /// This is convergence authority for an already authorized attempt, not a
    /// frontend request entry point. New DeleteBucket requests must use an
    /// admitted [`super::ActiveBucketRoute`].
    pub(crate) fn continue_adopted_bucket_delete(
        &self,
        root: &crate::BucketDeleteBeginRoot,
    ) -> Result<(), BucketWriteDrainError> {
        let bucket = &root.bucket;
        self.begin_bucket_delete_if_current_with_route_validation(
            super::BucketMetadataMutationEffectRoute {
                pg_id: self.bucket_metadata_pg(bucket),
                bucket,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            || Ok(()),
            BucketIdentityGenerations {
                bucket_execution_generation: root.bucket_execution_generation,
                bucket_incarnation_generation: root.bucket_incarnation_generation,
            },
        )
    }

    pub(super) fn begin_bucket_delete_if_current_with_route_validation(
        &self,
        route: super::BucketMetadataMutationEffectRoute<'_>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        expected_bucket_identity: BucketIdentityGenerations,
    ) -> Result<(), BucketWriteDrainError> {
        crate::metadata_command::metadata_command_publisher!(BeginBucketDelete);
        let super::BucketMetadataMutationEffectRoute {
            pg_id: bucket_pg_id,
            bucket,
            effect_fence,
        } = route;
        require_valid_route()?;
        let started = std::time::Instant::now();
        let pg_id = bucket_pg_id.pg_id();
        let _ = observability::emit_flight_event(
            super::TRACE_TARGET,
            "bucket_delete_begin_start",
            format!("bucket={:?} pg_id={}", bucket, pg_id.get()),
        );
        let node_store = match self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)
        {
            Ok(node_store) => node_store,
            Err(error) => {
                let _ = observability::emit_flight_event(
                    super::TRACE_TARGET,
                    "bucket_delete_begin_failed",
                    format!(
                        "bucket={:?} pg_id={} phase=primary_node elapsed_us={} error={:?}",
                        bucket,
                        pg_id.get(),
                        started.elapsed().as_micros(),
                        error
                    ),
                );
                return Err(error.into());
            }
        };
        let current_bucket_execution_generation;
        let current_bucket_incarnation_generation;
        {
            require_valid_route()?;
            let raw_snapshot_started = std::time::Instant::now();
            let _ = observability::emit_flight_event(
                super::TRACE_TARGET,
                "bucket_delete_begin_raw_snapshot_start",
                format!(
                    "bucket={:?} pg_id={} elapsed_us={}",
                    bucket,
                    pg_id.get(),
                    started.elapsed().as_micros()
                ),
            );
            let current = match node_store
                .bucket_metadata_client()
                .head_bucket_raw(bucket_pg_id, bucket)
            {
                Ok(current) => {
                    let _ = observability::emit_flight_event(
                        super::TRACE_TARGET,
                        "bucket_delete_begin_raw_snapshot_done",
                        format!(
                            "bucket={:?} pg_id={} elapsed_us={} total_elapsed_us={}",
                            bucket,
                            pg_id.get(),
                            raw_snapshot_started.elapsed().as_micros(),
                            started.elapsed().as_micros()
                        ),
                    );
                    current
                }
                Err(error) => {
                    let _ = observability::emit_flight_event(
                        super::TRACE_TARGET,
                        "bucket_delete_begin_raw_snapshot_failed",
                        format!(
                            "bucket={:?} pg_id={} elapsed_us={} total_elapsed_us={} error={:?}",
                            bucket,
                            pg_id.get(),
                            raw_snapshot_started.elapsed().as_micros(),
                            started.elapsed().as_micros(),
                            error
                        ),
                    );
                    return Err(bucket_snapshot_error_to_bucket_write_drain_error(error));
                }
            };
            if current.state == BucketState::Deleting {
                if current.bucket_incarnation_generation
                    != expected_bucket_identity.bucket_incarnation_generation
                {
                    return Err(StoreError::MetadataCommandContention {
                        context: "stale delete bucket authorization",
                    }
                    .into());
                }
                if let Some(command) = self
                    .pending_metadata_command_for_bucket(pg_id, bucket)
                    .map_err(BucketWriteDrainError::from)?
                {
                    if matches!(
                        command.payload(),
                        MetadataCommandPayload::MarkBucketDeleting(mark)
                            if mark.bucket_name() == bucket
                    ) {
                        let mut work_budget = super::RequestWorkBudget::new(
                            std::time::Duration::from_millis(
                                BUCKET_DELETE_BEGIN_WORK_BUDGET_MILLIS,
                            ),
                            None,
                        )
                        .for_operation("bucket_delete_begin")
                        .for_pg(pg_id);
                        let outcome = self
                            .finish_pending_metadata_command_to_acting_set_allow_partial_exact_conflict_retry_with_work_budget(
                                pg_id,
                                &command,
                                false,
                                &mut work_budget,
                            )
                            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
                        if matches!(
                            outcome,
                            FinishPendingMetadataCommandResult::RetryPartialExactConflict
                        ) {
                            return Err(bucket_snapshot_error_to_bucket_write_drain_error(
                                conflicting_pending_metadata_command(
                                    "retryable partial pending mark bucket deleting command",
                                ),
                            ));
                        }
                    }
                }
                let _ = observability::emit_flight_event(
                    super::TRACE_TARGET,
                    "bucket_delete_begin_done",
                    format!("bucket={:?} pg_id={}", bucket, pg_id.get()),
                );
                return Ok(());
            }
            if current.bucket_execution_generation
                != expected_bucket_identity.bucket_execution_generation
                || current.bucket_incarnation_generation
                    != expected_bucket_identity.bucket_incarnation_generation
            {
                return Err(StoreError::MetadataCommandContention {
                    context: "stale delete bucket authorization",
                }
                .into());
            }
            current_bucket_execution_generation = current.bucket_execution_generation;
            current_bucket_incarnation_generation = current.bucket_incarnation_generation;
        }
        let stream_check_started = std::time::Instant::now();
        let _ = observability::emit_flight_event(
            super::TRACE_TARGET,
            "bucket_delete_begin_stream_check_start",
            format!(
                "bucket={:?} pg_id={} elapsed_us={}",
                bucket,
                pg_id.get(),
                started.elapsed().as_micros()
            ),
        );
        match self.active_put_object_stream_upload_source(bucket) {
            Ok(Some(source)) => {
                let _ = observability::emit_flight_event(
                    super::TRACE_TARGET,
                    "bucket_delete_begin_stream_check_not_empty",
                    format!(
                        "bucket={:?} pg_id={} elapsed_us={} total_elapsed_us={} source={:?}",
                        bucket,
                        pg_id.get(),
                        stream_check_started.elapsed().as_micros(),
                        started.elapsed().as_micros(),
                        source
                    ),
                );
                if let Some(existing) = node_store
                    .bucket_write_reservation_client()
                    .durable_bucket_write_drain(self.validated_bucket_metadata_pg(pg_id), bucket)
                    .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?
                {
                    if existing.bucket_execution_generation == current_bucket_execution_generation {
                        self.record_bucket_delete_attempt_outcome_with_client(
                            node_store.bucket_write_reservation_client().as_ref(),
                            self.validated_bucket_metadata_pg(pg_id),
                            &existing,
                            BucketDeleteAttemptOutcomeKind::NotEmpty,
                            BucketDeleteAttemptPhase::StreamCleanup,
                            format!("live stream blocker before drain adoption: {source:?}"),
                        );
                        match node_store
                            .retained_bucket_write_reservation_client()
                            .open_retained_bucket_write_reservation_route(
                                self.validated_bucket_metadata_pg(pg_id),
                                &existing.bucket,
                            )
                            .and_then(|route| route.clear_durable_bucket_write_drain(&existing))
                            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)
                        {
                            Ok(()) => {}
                            Err(BucketWriteDrainError::Metadata(
                                MetadataError::BucketWriteDrainNotFound { .. }
                                | MetadataError::BucketNotFound { .. },
                            )) => {}
                            Err(error) => return Err(error),
                        }
                        let _ = observability::event(
                            super::TRACE_TARGET,
                            "bucket_delete_terminal_not_empty_drain_rollback",
                            Some(format_args!(
                                "bucket={:?} pg_id={} drain_id={} source={:?}",
                                bucket,
                                pg_id.get(),
                                existing.drain_id,
                                source
                            )),
                        );
                    }
                }
                return Err(self.bucket_delete_not_empty_error(bucket, pg_id, source));
            }
            Ok(None) => {
                let _ = observability::emit_flight_event(
                    super::TRACE_TARGET,
                    "bucket_delete_begin_stream_check_done",
                    format!(
                        "bucket={:?} pg_id={} elapsed_us={} total_elapsed_us={}",
                        bucket,
                        pg_id.get(),
                        stream_check_started.elapsed().as_micros(),
                        started.elapsed().as_micros()
                    ),
                );
            }
            Err(error) => {
                let _ = observability::emit_flight_event(
                    super::TRACE_TARGET,
                    "bucket_delete_begin_stream_check_failed",
                    format!(
                        "bucket={:?} pg_id={} elapsed_us={} total_elapsed_us={} error={:?}",
                        bucket,
                        pg_id.get(),
                        stream_check_started.elapsed().as_micros(),
                        started.elapsed().as_micros(),
                        error
                    ),
                );
                return Err(error);
            }
        }
        let durable_drain_started = std::time::Instant::now();
        let _ = observability::emit_flight_event(
            super::TRACE_TARGET,
            "bucket_delete_begin_durable_drain_start",
            format!(
                "bucket={:?} pg_id={} elapsed_us={}",
                bucket,
                pg_id.get(),
                started.elapsed().as_micros()
            ),
        );
        let mut durable_drain = match self
            .begin_durable_bucket_delete_drain_with_budget_and_route_validation(
                bucket,
                Some(started),
                Some(effect_fence),
                &mut require_valid_route,
            ) {
            Ok(super::DurableBucketDeleteDrainBegin::Acquired(drain)) => {
                let _ = observability::emit_flight_event(
                    super::TRACE_TARGET,
                    "bucket_delete_begin_durable_drain_acquired",
                    format!(
                        "bucket={:?} pg_id={} elapsed_us={} total_elapsed_us={}",
                        bucket,
                        pg_id.get(),
                        durable_drain_started.elapsed().as_micros(),
                        started.elapsed().as_micros()
                    ),
                );
                drain
            }
            Ok(super::DurableBucketDeleteDrainBegin::AlreadyDeleting) => {
                let _ = observability::emit_flight_event(
                    super::TRACE_TARGET,
                    "bucket_delete_begin_durable_drain_already_deleting",
                    format!(
                        "bucket={:?} pg_id={} elapsed_us={} total_elapsed_us={}",
                        bucket,
                        pg_id.get(),
                        durable_drain_started.elapsed().as_micros(),
                        started.elapsed().as_micros()
                    ),
                );
                let _ = observability::emit_flight_event(
                    super::TRACE_TARGET,
                    "bucket_delete_begin_done",
                    format!(
                        "bucket={:?} pg_id={} elapsed_us={}",
                        bucket,
                        pg_id.get(),
                        started.elapsed().as_micros()
                    ),
                );
                return Ok(());
            }
            Err(error) => {
                let _ = observability::emit_flight_event(
                    super::TRACE_TARGET,
                    "bucket_delete_begin_durable_drain_failed",
                    format!(
                        "bucket={:?} pg_id={} elapsed_us={} total_elapsed_us={} error={:?}",
                        bucket,
                        pg_id.get(),
                        durable_drain_started.elapsed().as_micros(),
                        started.elapsed().as_micros(),
                        error
                    ),
                );
                return Err(error);
            }
        };
        crate::node::maybe_run_after_begin_bucket_delete_drain_hook(bucket);

        let mut metadata_contention_retries = 0usize;
        let mut work_budget = super::RequestWorkBudget::new(
            std::time::Duration::from_millis(BUCKET_DELETE_BEGIN_WORK_BUDGET_MILLIS),
            None,
        )
        .for_operation("bucket_delete_begin")
        .for_pg(pg_id);
        let mut loop_iteration = 0u64;
        let mut attempt_phase = BucketDeleteAttemptPhase::Initial;
        let mut can_resume_at_mark_deleting = self
            .bucket_delete_matching_attempt_outcome(
                node_store.bucket_write_reservation_client().as_ref(),
                bucket_pg_id,
                &durable_drain,
            )?
            .is_some_and(|record| {
                record.outcome == BucketDeleteAttemptOutcomeKind::Retryable
                    && record.phase == BucketDeleteAttemptPhase::FinalVisibilityProven
            });
        let mut can_resume_at_final_visibility = self
            .bucket_delete_matching_attempt_outcome(
                node_store.bucket_write_reservation_client().as_ref(),
                bucket_pg_id,
                &durable_drain,
            )?
            .is_some_and(|record| {
                record.outcome == BucketDeleteAttemptOutcomeKind::Retryable
                    && record.phase == BucketDeleteAttemptPhase::FinalVisibilityCheck
            });
        let mut can_resume_at_stream_cleanup = self
            .bucket_delete_matching_attempt_outcome(
                node_store.bucket_write_reservation_client().as_ref(),
                bucket_pg_id,
                &durable_drain,
            )?
            .is_some_and(|record| {
                record.outcome == BucketDeleteAttemptOutcomeKind::Retryable
                    && record.phase == BucketDeleteAttemptPhase::StreamCleanup
            });
        let mut can_resume_at_reservation_wait = self
            .bucket_delete_matching_attempt_outcome(
                node_store.bucket_write_reservation_client().as_ref(),
                bucket_pg_id,
                &durable_drain,
            )?
            .is_some_and(|record| {
                record.outcome == BucketDeleteAttemptOutcomeKind::Retryable
                    && record.phase == BucketDeleteAttemptPhase::ReservationWait
            });
        let mut can_resume_at_post_reservation_object_drain = self
            .bucket_delete_matching_attempt_outcome(
                node_store.bucket_write_reservation_client().as_ref(),
                bucket_pg_id,
                &durable_drain,
            )?
            .is_some_and(|record| {
                record.outcome == BucketDeleteAttemptOutcomeKind::Retryable
                    && record.phase == BucketDeleteAttemptPhase::PostReservationObjectDrain
            });
        let result = (|| loop {
            loop_iteration += 1;
            attempt_phase = BucketDeleteAttemptPhase::Initial;
            require_valid_route()?;
            Self::emit_bucket_delete_begin_loop_step(
                bucket,
                pg_id,
                started,
                "iteration_start",
                format!("iteration={loop_iteration}"),
            );
            self.check_bucket_delete_begin_work_budget(
                bucket,
                Some(started),
                "bucket delete begin metadata convergence budget exhausted",
            )?;
            Self::emit_bucket_delete_begin_loop_step(
                bucket,
                pg_id,
                started,
                "pending_command_lookup_start",
                format!("iteration={loop_iteration}"),
            );
            let pending_command = self.pending_metadata_command_for_bucket(pg_id, bucket)?;
            Self::emit_bucket_delete_begin_loop_step(
                bucket,
                pg_id,
                started,
                "pending_command_lookup_done",
                format!(
                    "iteration={} has_pending={} command_kind={}",
                    loop_iteration,
                    pending_command.is_some(),
                    pending_command
                        .as_ref()
                        .map_or("none", |command| command.payload().kind_name())
                ),
            );
            let (command, clear_pending_on_zero_apply) = if let Some(command) = pending_command {
                can_resume_at_mark_deleting = false;
                can_resume_at_final_visibility = false;
                can_resume_at_stream_cleanup = false;
                can_resume_at_reservation_wait = false;
                can_resume_at_post_reservation_object_drain = false;
                if self
                    .drain_unrelated_pending_metadata_command_for_bucket_with_work_budget(
                        pg_id,
                        bucket,
                        &command,
                        &mut work_budget,
                    )
                    .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?
                {
                    super::sleep_after_metadata_contention_retry_for(
                        "bucket_delete_begin",
                        Some(pg_id),
                        "bucket delete drain unrelated bucket command",
                        &mut metadata_contention_retries,
                    );
                    continue;
                }
                match command.payload() {
                    MetadataCommandPayload::MarkBucketDeleting(mark)
                        if mark.bucket_name() == bucket =>
                    {
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "mark_matches_current_start",
                            format!("iteration={loop_iteration}"),
                        );
                        if !node_store
                            .bucket_metadata_client()
                            .pending_mark_bucket_deleting_command_matches_current(
                                self.validated_bucket_metadata_pg(pg_id),
                                bucket,
                                mark,
                            )
                            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?
                        {
                            return Err(bucket_snapshot_error_to_bucket_write_drain_error(
                                conflicting_pending_metadata_command(
                                    "conflicting pending mark bucket deleting command",
                                ),
                            ));
                        }
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "mark_matches_current_done",
                            format!("iteration={loop_iteration}"),
                        );
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "heartbeat_before_existing_mark_start",
                            format!("iteration={loop_iteration}"),
                        );
                        durable_drain =
                            self.heartbeat_durable_bucket_delete_drain(&durable_drain)?;
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "heartbeat_before_existing_mark_done",
                            format!("iteration={loop_iteration}"),
                        );
                        (command, false)
                    }
                    MetadataCommandPayload::MarkBucketDeleting(_) => {
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "drain_other_mark_deleting_start",
                            format!("iteration={loop_iteration}"),
                        );
                        let _ = self
                            .drain_bucket_pg_pending_metadata_command_with_work_budget(
                                pg_id,
                                &command,
                                false,
                                &mut work_budget,
                            )
                            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "drain_other_mark_deleting_done",
                            format!("iteration={loop_iteration}"),
                        );
                        super::sleep_after_metadata_contention_retry_for(
                            "bucket_delete_begin",
                            Some(pg_id),
                            "bucket delete drain other mark deleting",
                            &mut metadata_contention_retries,
                        );
                        continue;
                    }
                    MetadataCommandPayload::CreateBucket(_)
                    | MetadataCommandPayload::PutBucketVersioning(_)
                    | MetadataCommandPayload::PutBucketAcl(_)
                    | MetadataCommandPayload::PutBucketProperty(_)
                    | MetadataCommandPayload::PutBucketSubresource(_)
                    | MetadataCommandPayload::DeleteFinalizedBucket(_)
                    | MetadataCommandPayload::AdvanceMultipartCompletionBarrier(_) => {
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "drain_bucket_pg_command_start",
                            format!(
                                "iteration={} command_kind={}",
                                loop_iteration,
                                command.payload().kind_name()
                            ),
                        );
                        let _ = self
                            .drain_bucket_pg_pending_metadata_command_with_work_budget(
                                pg_id,
                                &command,
                                false,
                                &mut work_budget,
                            )
                            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "drain_bucket_pg_command_done",
                            format!(
                                "iteration={} command_kind={}",
                                loop_iteration,
                                command.payload().kind_name()
                            ),
                        );
                        super::sleep_after_metadata_contention_retry_for(
                            "bucket_delete_begin",
                            Some(pg_id),
                            "bucket delete drain bucket pg command",
                            &mut metadata_contention_retries,
                        );
                        continue;
                    }
                    MetadataCommandPayload::ReserveObjectGeneration(_)
                    | MetadataCommandPayload::ReleaseObjectGeneration(_)
                    | MetadataCommandPayload::ReserveObjectVersion(_)
                    | MetadataCommandPayload::CommitDirectPutObject(_)
                    | MetadataCommandPayload::CommitMultipartObject(_)
                    | MetadataCommandPayload::DeleteObjectVersion(_)
                    | MetadataCommandPayload::InsertDeleteMarker(_)
                    | MetadataCommandPayload::PutObjectMetadata(_)
                    | MetadataCommandPayload::CreateStreamUpload(_)
                    | MetadataCommandPayload::AppendStreamSegment(_)
                    | MetadataCommandPayload::AbortStreamUpload(_)
                    | MetadataCommandPayload::CommitStreamPart(_)
                    | MetadataCommandPayload::CreateMultipartUpload(_)
                    | MetadataCommandPayload::AbortMultipartUpload(_)
                    | MetadataCommandPayload::DeleteObjectPayloadReclaim(_) => {
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "drain_exact_bucket_object_commands_start",
                            format!(
                                "iteration={} command_kind={}",
                                loop_iteration,
                                command.payload().kind_name()
                            ),
                        );
                        match self
                            .drain_pending_object_metadata_commands_for_exact_bucket_on_all_pgs_with_budget(
                                bucket,
                                Some(started),
                                &mut work_budget,
                                Some(BucketDeleteExactDrainProgress {
                                    client: node_store.bucket_write_reservation_client().as_ref(),
                                    drain: &durable_drain,
                                    phase: BucketDeleteAttemptPhase::Initial,
                                }),
                            ) {
                            Ok(()) => {}
                            Err(error @ BucketWriteDrainError::Store(
                                StoreError::MetadataCommandContention {
                                    context:
                                        BUCKET_DELETE_EXACT_BUCKET_DRAIN_BUDGET_EXHAUSTED_CONTEXT,
                                },
                            )) => return Err(error),
                            Err(BucketWriteDrainError::Store(
                                StoreError::MetadataCommandContention { .. },
                            )) => {
                                super::sleep_after_metadata_contention_retry_for(
                                    "bucket_delete_begin",
                                    Some(pg_id),
                                    "bucket delete drain exact bucket object commands contention",
                                    &mut metadata_contention_retries,
                                );
                                continue;
                            }
                            Err(error) => return Err(error),
                        }
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "drain_exact_bucket_object_commands_done",
                            format!(
                                "iteration={} command_kind={}",
                                loop_iteration,
                                command.payload().kind_name()
                            ),
                        );
                        super::sleep_after_metadata_contention_retry_for(
                            "bucket_delete_begin",
                            Some(pg_id),
                            "bucket delete drain exact bucket object commands",
                            &mut metadata_contention_retries,
                        );
                        continue;
                    }
                }
            } else {
                if can_resume_at_mark_deleting {
                    Self::emit_bucket_delete_begin_loop_step(
                        bucket,
                        pg_id,
                        started,
                        "resume_mark_deleting_after_final_visibility",
                        format!("iteration={loop_iteration}"),
                    );
                } else {
                    if can_resume_at_final_visibility {
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "resume_final_visibility",
                            format!("iteration={loop_iteration}"),
                        );
                    } else {
                        if can_resume_at_post_reservation_object_drain {
                            Self::emit_bucket_delete_begin_loop_step(
                                bucket,
                                pg_id,
                                started,
                                "resume_post_reservation_object_drain",
                                format!("iteration={loop_iteration}"),
                            );
                        } else if can_resume_at_reservation_wait {
                            Self::emit_bucket_delete_begin_loop_step(
                                bucket,
                                pg_id,
                                started,
                                "resume_reservation_wait",
                                format!("iteration={loop_iteration}"),
                            );
                        } else if can_resume_at_stream_cleanup {
                            Self::emit_bucket_delete_begin_loop_step(
                                bucket,
                                pg_id,
                                started,
                                "resume_stream_cleanup",
                                format!("iteration={loop_iteration}"),
                            );
                        } else {
                            Self::emit_bucket_delete_begin_loop_step(
                                bucket,
                                pg_id,
                                started,
                                "drain_exact_bucket_object_commands_start",
                                format!(
                                    "iteration={} command_kind=none pass=initial",
                                    loop_iteration
                                ),
                            );
                            match self
                                .drain_pending_object_metadata_commands_for_exact_bucket_on_all_pgs_with_budget(
                                    bucket,
                                    Some(started),
                                    &mut work_budget,
                                    Some(BucketDeleteExactDrainProgress {
                                        client: node_store
                                            .bucket_write_reservation_client()
                                            .as_ref(),
                                        drain: &durable_drain,
                                        phase: BucketDeleteAttemptPhase::Initial,
                                    }),
                                ) {
                                Ok(()) => {}
                                Err(error @ BucketWriteDrainError::Store(
                                    StoreError::MetadataCommandContention {
                                        context:
                                            BUCKET_DELETE_EXACT_BUCKET_DRAIN_BUDGET_EXHAUSTED_CONTEXT,
                                    },
                                )) => return Err(error),
                                Err(BucketWriteDrainError::Store(
                                    StoreError::MetadataCommandContention { .. },
                                )) => {
                                    super::sleep_after_metadata_contention_retry_for(
                                        "bucket_delete_begin",
                                        Some(pg_id),
                                        "bucket delete initial exact bucket object drain contention",
                                        &mut metadata_contention_retries,
                                    );
                                    continue;
                                }
                                Err(error) => return Err(error),
                            }
                            Self::emit_bucket_delete_begin_loop_step(
                                bucket,
                                pg_id,
                                started,
                                "drain_exact_bucket_object_commands_done",
                                format!(
                                    "iteration={} command_kind=none pass=initial",
                                    loop_iteration
                                ),
                            );
                            Self::emit_bucket_delete_begin_loop_step(
                                bucket,
                                pg_id,
                                started,
                                "pending_command_recheck_start",
                                format!("iteration={} pass=after_object_drain", loop_iteration),
                            );
                            let pending_after_object_drain =
                                self.pending_metadata_command_for_bucket(pg_id, bucket)?;
                            Self::emit_bucket_delete_begin_loop_step(
                                bucket,
                                pg_id,
                                started,
                                "pending_command_recheck_done",
                                format!(
                                    "iteration={} pass=after_object_drain has_pending={} command_kind={}",
                                    loop_iteration,
                                    pending_after_object_drain.is_some(),
                                    pending_after_object_drain
                                        .as_ref()
                                        .map_or("none", |command| command.payload().kind_name())
                                ),
                            );
                            if pending_after_object_drain.is_some() {
                                super::sleep_after_metadata_contention_retry_for(
                                    "bucket_delete_begin",
                                    Some(pg_id),
                                    "bucket delete pending command remained after object drain",
                                    &mut metadata_contention_retries,
                                );
                                continue;
                            }
                        }
                        if !can_resume_at_post_reservation_object_drain
                            && !can_resume_at_reservation_wait
                        {
                            attempt_phase = BucketDeleteAttemptPhase::StreamCleanup;
                            Self::emit_bucket_delete_begin_loop_step(
                                bucket,
                                pg_id,
                                started,
                                "heartbeat_before_stream_cleanup_start",
                                format!("iteration={loop_iteration}"),
                            );
                            durable_drain =
                                self.heartbeat_durable_bucket_delete_drain(&durable_drain)?;
                            Self::emit_bucket_delete_begin_loop_step(
                                bucket,
                                pg_id,
                                started,
                                "heartbeat_before_stream_cleanup_done",
                                format!("iteration={loop_iteration}"),
                            );
                            Self::emit_bucket_delete_begin_loop_step(
                                bucket,
                                pg_id,
                                started,
                                "stream_cleanup_start",
                                format!("iteration={loop_iteration}"),
                            );
                            attempt_phase = BucketDeleteAttemptPhase::StreamCleanup;
                            self.record_bucket_delete_attempt_outcome_for_drain_with_client(
                                node_store.bucket_write_reservation_client().as_ref(),
                                &durable_drain,
                                BucketDeleteAttemptOutcomeKind::Retryable,
                                BucketDeleteAttemptPhase::StreamCleanup,
                                "stream cleanup started".to_string(),
                            );
                            self.record_bucket_delete_post_reservation_next_object_pg_id(
                                BucketDeleteExactDrainProgress {
                                    client: node_store.bucket_write_reservation_client().as_ref(),
                                    drain: &durable_drain,
                                    phase: BucketDeleteAttemptPhase::StreamCleanup,
                                },
                                0,
                            )?;
                            can_resume_at_stream_cleanup = true;
                            match self
                                .cleanup_abandoned_put_object_stream_uploads_for_bucket(bucket)?
                            {
                                PutObjectStreamUploadCleanup::Live(source) => {
                                    self.record_bucket_delete_attempt_outcome_for_drain_with_client(
                                        node_store.bucket_write_reservation_client().as_ref(),
                                        &durable_drain,
                                        BucketDeleteAttemptOutcomeKind::NotEmpty,
                                        BucketDeleteAttemptPhase::StreamCleanup,
                                        format!(
                                            "live stream blocker during cleanup before reservation wait: {source:?}"
                                        ),
                                    );
                                    return Err(
                                        self.bucket_delete_not_empty_error(bucket, pg_id, source)
                                    );
                                }
                                PutObjectStreamUploadCleanup::Aborted { count } => {
                                    Self::emit_bucket_delete_begin_loop_step(
                                        bucket,
                                        pg_id,
                                        started,
                                        "stream_cleanup_done",
                                        format!(
                                            "iteration={} pass=before_reservation_wait aborted_stream_uploads={}",
                                            loop_iteration, count
                                        ),
                                    );
                                }
                            }
                            attempt_phase = BucketDeleteAttemptPhase::ReservationWait;
                            self.record_bucket_delete_attempt_outcome_for_drain_with_client(
                                node_store.bucket_write_reservation_client().as_ref(),
                                &durable_drain,
                                BucketDeleteAttemptOutcomeKind::Retryable,
                                BucketDeleteAttemptPhase::ReservationWait,
                                "stream cleanup completed before reservation wait".to_string(),
                            );
                            can_resume_at_stream_cleanup = false;
                            can_resume_at_reservation_wait = true;
                            #[cfg(any(test, feature = "test-hooks"))]
                            maybe_run_after_bucket_delete_reservation_wait_ready_hook(
                                self.metadata_command_apply_test_hook_scope_id(),
                            )
                            .map_err(BucketWriteDrainError::from)?;
                        }
                        if !can_resume_at_post_reservation_object_drain {
                            Self::emit_bucket_delete_begin_loop_step(
                                bucket,
                                pg_id,
                                started,
                                "wait_reservations_empty_start",
                                format!("iteration={loop_iteration}"),
                            );
                            attempt_phase = BucketDeleteAttemptPhase::ReservationWait;
                            Self::emit_bucket_delete_begin_loop_step(
                                bucket,
                                pg_id,
                                started,
                                "heartbeat_before_reservation_wait_start",
                                format!("iteration={loop_iteration}"),
                            );
                            durable_drain =
                                self.heartbeat_durable_bucket_delete_drain(&durable_drain)?;
                            Self::emit_bucket_delete_begin_loop_step(
                                bucket,
                                pg_id,
                                started,
                                "heartbeat_before_reservation_wait_done",
                                format!("iteration={loop_iteration}"),
                            );
                            self.wait_for_durable_bucket_write_reservations_empty(
                                bucket,
                                node_store.bucket_write_reservation_client().as_ref(),
                                &durable_drain,
                                started,
                                &mut work_budget,
                            )?;
                            Self::emit_bucket_delete_begin_loop_step(
                                bucket,
                                pg_id,
                                started,
                                "wait_reservations_empty_done",
                                format!("iteration={loop_iteration}"),
                            );
                        }
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "drain_exact_bucket_object_commands_start",
                            format!(
                                "iteration={} command_kind=none pass=after_reservation_wait",
                                loop_iteration
                            ),
                        );
                        attempt_phase = BucketDeleteAttemptPhase::PostReservationObjectDrain;
                        match self
                            .drain_pending_object_metadata_commands_for_exact_bucket_on_all_pgs_with_budget(
                                bucket,
                                Some(started),
                                &mut work_budget,
                                Some(BucketDeleteExactDrainProgress {
                                    client: node_store.bucket_write_reservation_client().as_ref(),
                                    drain: &durable_drain,
                                    phase: BucketDeleteAttemptPhase::PostReservationObjectDrain,
                                }),
                            ) {
                            Ok(()) => {}
                            Err(error @ BucketWriteDrainError::Store(
                                StoreError::MetadataCommandContention {
                                    context:
                                        BUCKET_DELETE_EXACT_BUCKET_DRAIN_BUDGET_EXHAUSTED_CONTEXT,
                                },
                            )) => return Err(error),
                            Err(BucketWriteDrainError::Store(
                                StoreError::MetadataCommandContention { .. },
                            )) => {
                                super::sleep_after_metadata_contention_retry_for(
                                    "bucket_delete_begin",
                                    Some(pg_id),
                                    "bucket delete exact bucket object drain after reservation wait contention",
                                    &mut metadata_contention_retries,
                                );
                                continue;
                            }
                            Err(error) => return Err(error),
                        }
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "drain_exact_bucket_object_commands_done",
                            format!(
                                "iteration={} command_kind=none pass=after_reservation_wait",
                                loop_iteration
                            ),
                        );
                        let terminal_post_reservation_next_object_pg_id =
                            self.terminal_bucket_delete_post_reservation_next_object_pg_id();
                        attempt_phase = BucketDeleteAttemptPhase::StreamCleanup;
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "heartbeat_before_visibility_stream_cleanup_start",
                            format!("iteration={} pass=before_visibility_check", loop_iteration),
                        );
                        durable_drain =
                            self.heartbeat_durable_bucket_delete_drain(&durable_drain)?;
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "heartbeat_before_visibility_stream_cleanup_done",
                            format!("iteration={} pass=before_visibility_check", loop_iteration),
                        );
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "stream_cleanup_start",
                            format!("iteration={} pass=before_visibility_check", loop_iteration),
                        );
                        attempt_phase = BucketDeleteAttemptPhase::StreamCleanup;
                        self.record_bucket_delete_attempt_outcome_for_drain_with_client(
                            node_store.bucket_write_reservation_client().as_ref(),
                            &durable_drain,
                            BucketDeleteAttemptOutcomeKind::Retryable,
                            BucketDeleteAttemptPhase::StreamCleanup,
                            "stream cleanup started before visibility check".to_string(),
                        );
                        self.record_bucket_delete_post_reservation_next_object_pg_id(
                            BucketDeleteExactDrainProgress {
                                client: node_store.bucket_write_reservation_client().as_ref(),
                                drain: &durable_drain,
                                phase: BucketDeleteAttemptPhase::StreamCleanup,
                            },
                            0,
                        )?;
                        let aborted_stream_uploads = match self
                            .cleanup_abandoned_put_object_stream_uploads_for_bucket(bucket)?
                        {
                            PutObjectStreamUploadCleanup::Live(source) => {
                                self.record_bucket_delete_attempt_outcome_for_drain_with_client(
                                    node_store.bucket_write_reservation_client().as_ref(),
                                    &durable_drain,
                                    BucketDeleteAttemptOutcomeKind::NotEmpty,
                                    BucketDeleteAttemptPhase::StreamCleanup,
                                    format!(
                                        "live stream blocker during cleanup before visibility check: {source:?}"
                                    ),
                                );
                                return Err(
                                    self.bucket_delete_not_empty_error(bucket, pg_id, source)
                                );
                            }
                            PutObjectStreamUploadCleanup::Aborted { count } => count,
                        };
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "stream_cleanup_done",
                            format!(
                                "iteration={} pass=before_visibility_check aborted_stream_uploads={}",
                                loop_iteration, aborted_stream_uploads
                            ),
                        );
                        if aborted_stream_uploads > 0 {
                            Self::emit_bucket_delete_begin_loop_step(
                                bucket,
                                pg_id,
                                started,
                                "drain_exact_bucket_object_commands_start",
                                format!(
                                    "iteration={} command_kind=none pass=after_abandoned_stream_abort",
                                    loop_iteration
                                ),
                            );
                            match self
                                .drain_pending_object_metadata_commands_for_exact_bucket_on_all_pgs_with_budget(
                                    bucket,
                                    Some(started),
                                    &mut work_budget,
                                    None,
                                ) {
                                Ok(()) => {}
                                Err(error @ BucketWriteDrainError::Store(
                                    StoreError::MetadataCommandContention {
                                        context:
                                            BUCKET_DELETE_EXACT_BUCKET_DRAIN_BUDGET_EXHAUSTED_CONTEXT,
                                    },
                                )) => return Err(error),
                                Err(BucketWriteDrainError::Store(
                                    StoreError::MetadataCommandContention { .. },
                                )) => {
                                    super::sleep_after_metadata_contention_retry_for(
                                        "bucket_delete_begin",
                                        Some(pg_id),
                                        "bucket delete exact bucket object drain after abandoned stream abort contention",
                                        &mut metadata_contention_retries,
                                    );
                                    continue;
                                }
                                Err(error) => return Err(error),
                            }
                            Self::emit_bucket_delete_begin_loop_step(
                                bucket,
                                pg_id,
                                started,
                                "drain_exact_bucket_object_commands_done",
                                format!(
                                    "iteration={} command_kind=none pass=after_abandoned_stream_abort",
                                    loop_iteration
                                ),
                            );
                        }
                        self.record_bucket_delete_post_reservation_next_object_pg_id(
                            BucketDeleteExactDrainProgress {
                                client: node_store.bucket_write_reservation_client().as_ref(),
                                drain: &durable_drain,
                                phase: BucketDeleteAttemptPhase::StreamCleanup,
                            },
                            terminal_post_reservation_next_object_pg_id,
                        )?;
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "pending_command_recheck_start",
                            format!("iteration={} pass=before_visibility_check", loop_iteration),
                        );
                        let pending_before_visibility_check =
                            self.pending_metadata_command_for_bucket(pg_id, bucket)?;
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "pending_command_recheck_done",
                            format!(
                                "iteration={} pass=before_visibility_check has_pending={} command_kind={}",
                                loop_iteration,
                                pending_before_visibility_check.is_some(),
                                pending_before_visibility_check
                                    .as_ref()
                                    .map_or("none", |command| command.payload().kind_name())
                            ),
                        );
                        if pending_before_visibility_check.is_some() {
                            super::sleep_after_metadata_contention_retry_for(
                                "bucket_delete_begin",
                                Some(pg_id),
                                "bucket delete pending command remained before visibility check",
                                &mut metadata_contention_retries,
                            );
                            continue;
                        }
                    }
                    attempt_phase = BucketDeleteAttemptPhase::FinalVisibilityCheck;
                    Self::emit_bucket_delete_begin_loop_step(
                        bucket,
                        pg_id,
                        started,
                        "heartbeat_before_visibility_check_start",
                        format!("iteration={loop_iteration}"),
                    );
                    durable_drain = self.heartbeat_durable_bucket_delete_drain(&durable_drain)?;
                    Self::emit_bucket_delete_begin_loop_step(
                        bucket,
                        pg_id,
                        started,
                        "heartbeat_before_visibility_check_done",
                        format!("iteration={loop_iteration}"),
                    );
                    Self::emit_bucket_delete_begin_loop_step(
                        bucket,
                        pg_id,
                        started,
                        "visibility_check_start",
                        format!("iteration={loop_iteration}"),
                    );
                    self.record_bucket_delete_attempt_outcome_for_drain_with_client(
                        node_store.bucket_write_reservation_client().as_ref(),
                        &durable_drain,
                        BucketDeleteAttemptOutcomeKind::Retryable,
                        BucketDeleteAttemptPhase::FinalVisibilityCheck,
                        "final visibility check started".to_string(),
                    );
                    #[cfg(any(test, feature = "test-hooks"))]
                    maybe_run_before_bucket_delete_final_visibility_hook(
                        self.metadata_command_apply_test_hook_scope_id(),
                    )
                    .map_err(BucketWriteDrainError::from)?;
                    if let Some(source) = self.bucket_visible_data_source(bucket, true)? {
                        self.record_bucket_delete_attempt_outcome_for_drain_with_client(
                            node_store.bucket_write_reservation_client().as_ref(),
                            &durable_drain,
                            BucketDeleteAttemptOutcomeKind::NotEmpty,
                            BucketDeleteAttemptPhase::FinalVisibilityCheck,
                            format!("visible data blocker: {source:?}"),
                        );
                        return Err(self.bucket_delete_not_empty_error(bucket, pg_id, source));
                    }
                    Self::emit_bucket_delete_begin_loop_step(
                        bucket,
                        pg_id,
                        started,
                        "visibility_check_done",
                        format!("iteration={loop_iteration}"),
                    );
                    Self::emit_bucket_delete_begin_loop_step(
                        bucket,
                        pg_id,
                        started,
                        "heartbeat_after_visibility_check_start",
                        format!("iteration={loop_iteration}"),
                    );
                    durable_drain = self.heartbeat_durable_bucket_delete_drain(&durable_drain)?;
                    Self::emit_bucket_delete_begin_loop_step(
                        bucket,
                        pg_id,
                        started,
                        "heartbeat_after_visibility_check_done",
                        format!("iteration={loop_iteration}"),
                    );
                    attempt_phase = BucketDeleteAttemptPhase::FinalVisibilityProven;
                    self.record_bucket_delete_attempt_outcome_for_drain_with_client(
                        node_store.bucket_write_reservation_client().as_ref(),
                        &durable_drain,
                        BucketDeleteAttemptOutcomeKind::Retryable,
                        BucketDeleteAttemptPhase::FinalVisibilityProven,
                        "final visibility check proven".to_string(),
                    );
                    can_resume_at_mark_deleting = true;
                    #[cfg(any(test, feature = "test-hooks"))]
                    maybe_run_after_bucket_delete_final_visibility_proven_hook(
                        self.metadata_command_apply_test_hook_scope_id(),
                    )
                    .map_err(BucketWriteDrainError::from)?;
                }
                Self::emit_bucket_delete_begin_loop_step(
                    bucket,
                    pg_id,
                    started,
                    "heartbeat_before_build_mark_start",
                    format!("iteration={loop_iteration}"),
                );
                attempt_phase = BucketDeleteAttemptPhase::MarkDeleting;
                durable_drain = self.heartbeat_durable_bucket_delete_drain(&durable_drain)?;
                Self::emit_bucket_delete_begin_loop_step(
                    bucket,
                    pg_id,
                    started,
                    "heartbeat_before_build_mark_done",
                    format!("iteration={loop_iteration}"),
                );
                #[cfg(test)]
                maybe_run_before_bucket_delete_command_id_hook(
                    self.metadata_command_apply_test_hook_scope_id(),
                );
                Self::emit_bucket_delete_begin_loop_step(
                    bucket,
                    pg_id,
                    started,
                    "next_command_id_start",
                    format!("iteration={loop_iteration}"),
                );
                let command_id = match self.next_metadata_command_id(pg_id) {
                    Ok(command_id) => {
                        Self::emit_bucket_delete_begin_loop_step(
                            bucket,
                            pg_id,
                            started,
                            "next_command_id_done",
                            format!("iteration={loop_iteration} command_id={command_id:?}"),
                        );
                        command_id
                    }
                    Err(StoreError::MetadataCommandLogConflict { .. }) => {
                        super::sleep_after_metadata_contention_retry_for(
                            "bucket_delete_begin",
                            Some(pg_id),
                            "bucket delete next command id log conflict",
                            &mut metadata_contention_retries,
                        );
                        continue;
                    }
                    Err(error) => return Err(BucketWriteDrainError::from(error)),
                };
                Self::emit_bucket_delete_begin_loop_step(
                    bucket,
                    pg_id,
                    started,
                    "build_mark_deleting_start",
                    format!("iteration={loop_iteration} command_id={command_id:?}"),
                );
                let command = match node_store
                    .bucket_metadata_client()
                    .build_mark_bucket_deleting_command(
                        self.validated_bucket_metadata_pg(pg_id),
                        bucket,
                        command_id,
                    )
                    .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?
                {
                    MarkBucketDeletingCommandBuild::AlreadyDeleting => return Ok(()),
                    MarkBucketDeletingCommandBuild::Command(command) => *command,
                };
                Self::emit_bucket_delete_begin_loop_step(
                    bucket,
                    pg_id,
                    started,
                    "build_mark_deleting_done",
                    format!("iteration={loop_iteration} command_id={command_id:?}"),
                );
                Self::emit_bucket_delete_begin_loop_step(
                    bucket,
                    pg_id,
                    started,
                    "pending_install_start",
                    format!("iteration={loop_iteration} command_id={command_id:?}"),
                );
                require_valid_route()?;
                if !self
                    .try_set_bucket_pg_pending_command_or_retry_with_work_budget_and_effect_fence(
                        pg_id,
                        bucket,
                        &command,
                        Some(effect_fence),
                        &mut work_budget,
                    )
                    .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?
                {
                    super::sleep_after_metadata_contention_retry_for(
                        "bucket_delete_begin",
                        Some(pg_id),
                        "bucket delete pending install conflict",
                        &mut metadata_contention_retries,
                    );
                    continue;
                }
                Self::emit_bucket_delete_begin_loop_step(
                    bucket,
                    pg_id,
                    started,
                    "pending_install_done",
                    format!("iteration={loop_iteration} command_id={command_id:?}"),
                );
                (command, true)
            };
            Self::emit_bucket_delete_begin_loop_step(
                bucket,
                pg_id,
                started,
                "heartbeat_before_apply_start",
                format!(
                    "iteration={} command_kind={}",
                    loop_iteration,
                    command.payload().kind_name()
                ),
            );
            durable_drain = self.heartbeat_durable_bucket_delete_drain(&durable_drain)?;
            Self::emit_bucket_delete_begin_loop_step(
                bucket,
                pg_id,
                started,
                "heartbeat_before_apply_done",
                format!(
                    "iteration={} command_kind={}",
                    loop_iteration,
                    command.payload().kind_name()
                ),
            );
            Self::emit_bucket_delete_begin_loop_step(
                bucket,
                pg_id,
                started,
                "apply_mark_deleting_start",
                format!(
                    "iteration={} command_kind={} clear_pending_on_zero_apply={}",
                    loop_iteration,
                    command.payload().kind_name(),
                    clear_pending_on_zero_apply
                ),
            );
            let mut mark_apply_budget = super::RequestWorkBudget::new(
                std::time::Duration::from_millis(METADATA_COMMAND_APPLY_RETRY_BUDGET_MILLIS),
                None,
            )
            .for_operation("bucket_delete_mark_deleting_apply")
            .for_pg(pg_id);
            let outcome = self
                .finish_pending_metadata_command_to_acting_set_allow_partial_exact_conflict_retry_with_work_budget(
                    pg_id,
                    &command,
                    clear_pending_on_zero_apply,
                    &mut mark_apply_budget,
                )
                .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
            Self::emit_bucket_delete_begin_loop_step(
                bucket,
                pg_id,
                started,
                "apply_mark_deleting_done",
                format!("iteration={} outcome={outcome:?}", loop_iteration),
            );
            match outcome {
                FinishPendingMetadataCommandResult::Applied => {}
                FinishPendingMetadataCommandResult::RetryPartialExactConflict => {
                    return Err(bucket_snapshot_error_to_bucket_write_drain_error(
                        conflicting_pending_metadata_command(
                            "retryable partial pending mark bucket deleting command",
                        ),
                    ));
                }
                FinishPendingMetadataCommandResult::Abandoned => {
                    super::sleep_after_metadata_contention_retry_for(
                        "bucket_delete_begin",
                        Some(pg_id),
                        "bucket delete mark deleting abandoned",
                        &mut metadata_contention_retries,
                    );
                    continue;
                }
            }

            return Ok(());
        })();

        match result {
            Ok(()) => {
                self.record_bucket_delete_attempt_outcome_for_drain_with_client(
                    node_store.bucket_write_reservation_client().as_ref(),
                    &durable_drain,
                    BucketDeleteAttemptOutcomeKind::MarkDeleting,
                    BucketDeleteAttemptPhase::MarkDeleting,
                    format!("mark bucket deleting applied after {loop_iteration} iteration(s)"),
                );
                let _ = observability::emit_flight_event(
                    super::TRACE_TARGET,
                    "bucket_delete_begin_done",
                    format!(
                        "bucket={:?} pg_id={} elapsed_us={}",
                        bucket,
                        pg_id.get(),
                        started.elapsed().as_micros()
                    ),
                );
                Ok(())
            }
            Err(error) => {
                let reservation_wait_blocker_already_recorded = matches!(
                    error,
                    BucketWriteDrainError::Store(StoreError::MetadataCommandContention {
                        context: BUCKET_DELETE_RESERVATION_WAIT_BLOCKED_CONTEXT,
                    })
                );
                let _ = observability::emit_flight_event(
                    super::TRACE_TARGET,
                    "bucket_delete_begin_failed",
                    format!(
                        "bucket={:?} pg_id={} phase={:?} elapsed_us={} error={:?}",
                        bucket,
                        pg_id.get(),
                        attempt_phase,
                        started.elapsed().as_micros(),
                        error
                    ),
                );
                if Self::bucket_delete_begin_error_should_rollback_drain(&error) {
                    self.rollback_durable_bucket_delete_drain(&durable_drain)?;
                } else if matches!(
                    error,
                    BucketWriteDrainError::Store(StoreError::MetadataCommandContention { .. })
                ) {
                    match node_store
                        .bucket_metadata_client()
                        .head_bucket_raw(self.validated_bucket_metadata_pg(pg_id), bucket)
                    {
                        Ok(current)
                            if current.state == BucketState::Deleting
                                && current.bucket_incarnation_generation
                                    == current_bucket_incarnation_generation =>
                        {
                            match self.pending_metadata_command_for_bucket(pg_id, bucket) {
                                Ok(None) => {
                                    let _ = observability::emit_flight_event(
                                        super::TRACE_TARGET,
                                        "bucket_delete_begin_retryable_error_observed_deleting",
                                        format!(
                                            "bucket={:?} pg_id={} phase={:?} elapsed_us={} error={:?}",
                                            bucket,
                                            pg_id.get(),
                                            attempt_phase,
                                            started.elapsed().as_micros(),
                                            error
                                        ),
                                    );
                                    let _ = observability::emit_flight_event(
                                        super::TRACE_TARGET,
                                        "bucket_delete_begin_done",
                                        format!(
                                            "bucket={:?} pg_id={} elapsed_us={}",
                                            bucket,
                                            pg_id.get(),
                                            started.elapsed().as_micros()
                                        ),
                                    );
                                    return Ok(());
                                }
                                Ok(Some(_)) => {}
                                Err(pending_recheck_error) => {
                                    let _ = observability::emit_flight_event(
                                        super::TRACE_TARGET,
                                        "bucket_delete_begin_retryable_error_pending_recheck_failed",
                                        format!(
                                            "bucket={:?} pg_id={} phase={:?} elapsed_us={} original_error={:?} pending_recheck_error={:?}",
                                            bucket,
                                            pg_id.get(),
                                            attempt_phase,
                                            started.elapsed().as_micros(),
                                            error,
                                            pending_recheck_error
                                        ),
                                    );
                                }
                            }
                        }
                        Ok(_) => {}
                        Err(recheck_error) => {
                            let _ = observability::emit_flight_event(
                                super::TRACE_TARGET,
                                "bucket_delete_begin_retryable_error_deleting_recheck_failed",
                                format!(
                                    "bucket={:?} pg_id={} phase={:?} elapsed_us={} original_error={:?} recheck_error={:?}",
                                    bucket,
                                    pg_id.get(),
                                    attempt_phase,
                                    started.elapsed().as_micros(),
                                    error,
                                    recheck_error
                                ),
                            );
                        }
                    }
                    if !reservation_wait_blocker_already_recorded {
                        self.record_bucket_delete_attempt_outcome_for_drain_with_client(
                            node_store.bucket_write_reservation_client().as_ref(),
                            &durable_drain,
                            BucketDeleteAttemptOutcomeKind::Retryable,
                            attempt_phase,
                            format!("retryable begin error: {error:?}"),
                        );
                    }
                    let _ = observability::event(
                        super::TRACE_TARGET,
                        "bucket_delete_begin_preserved_retryable_attempt",
                        Some(format_args!(
                            "bucket={:?} pg_id={} drain_id={} error={:?}",
                            bucket, pg_id, durable_drain.record.drain_id, error
                        )),
                    );
                    self.enqueue_bucket_delete_begin(
                        bucket,
                        current_bucket_execution_generation,
                        current_bucket_incarnation_generation,
                    );
                } else {
                    if !reservation_wait_blocker_already_recorded {
                        self.record_bucket_delete_attempt_outcome_for_drain_with_client(
                            node_store.bucket_write_reservation_client().as_ref(),
                            &durable_drain,
                            BucketDeleteAttemptOutcomeKind::Retryable,
                            attempt_phase,
                            format!("retryable begin error: {error:?}"),
                        );
                    }
                    let _ = observability::event(
                        super::TRACE_TARGET,
                        "bucket_delete_begin_preserved_retryable_attempt",
                        Some(format_args!(
                            "bucket={:?} pg_id={} drain_id={} error={:?}",
                            bucket, pg_id, durable_drain.record.drain_id, error
                        )),
                    );
                    self.enqueue_bucket_delete_begin(
                        bucket,
                        current_bucket_execution_generation,
                        current_bucket_incarnation_generation,
                    );
                }
                Err(error)
            }
        }
    }

    fn bucket_delete_begin_error_should_rollback_drain(error: &BucketWriteDrainError) -> bool {
        match error {
            BucketWriteDrainError::Store(
                StoreError::MetadataCommandContention { .. }
                | StoreError::RouteMapExpired { .. }
                | StoreError::StaleMetadataOperation { .. }
                | StoreError::StaleMetadataRoute { .. },
            ) => false,
            BucketWriteDrainError::Store(StoreError::StorageRpc { failure: code, .. })
                if super::storage_rpc_code_is_retryable_pg_route_error(*code) =>
            {
                false
            }
            _ => true,
        }
    }

    fn cleanup_abandoned_put_object_stream_uploads_for_bucket(
        &self,
        bucket: &BucketName,
    ) -> Result<PutObjectStreamUploadCleanup, BucketWriteDrainError> {
        const STREAM_UPLOAD_DELETE_PAGE_LIMIT: u32 = 128;

        let mut aborted_count = 0usize;
        for raw_pg_id in self.metadata_pg_ids() {
            let pg_id = PgId::new(raw_pg_id);
            let scan_pg_id = self.object_metadata_scan_pg(pg_id);
            let mut marker = None;
            loop {
                let node = self
                    .local_map
                    .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
                let page = node
                    .object_mutation_metadata_client()
                    .list_stream_uploads_for_bucket_page(
                        scan_pg_id,
                        bucket,
                        marker.as_ref(),
                        STREAM_UPLOAD_DELETE_PAGE_LIMIT,
                    )
                    .map_err(super::object_pg_action_error_to_bucket_snapshot_error)
                    .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
                if page.uploads.is_empty() {
                    break;
                }

                let mut aborted_any = false;
                for upload in page.uploads {
                    if self.object_metadata_pg_id(&upload.bucket, &upload.key) != raw_pg_id {
                        return Err(BucketWriteDrainError::Store(StoreError::Io {
                            context: "bucket delete stream upload cleanup PG validation",
                            source: std::io::Error::other(format!(
                                "stream upload session {:?} for bucket {:?} key {:?} is stored on PG {}",
                                upload.session_id,
                                upload.bucket,
                                upload.key,
                                raw_pg_id
                            )),
                        }));
                    }
                    if upload.target != crate::StreamUploadTarget::PutObject {
                        continue;
                    }
                    if self.stream_upload_has_live_bucket_write_reservation(&upload)? {
                        return Ok(PutObjectStreamUploadCleanup::Live(
                            BucketVisibleDataSource::StreamUpload { pg_id },
                        ));
                    }
                    match self.abort_stream_upload_session(
                        &upload.bucket,
                        &upload.key,
                        &upload.session_id,
                    ) {
                        Ok(()) => {}
                        Err(ObjectPgActionError::Metadata(
                            MetadataError::StreamSessionNotFound { .. },
                        )) => {}
                        Err(error) => {
                            return Err(bucket_snapshot_error_to_bucket_write_drain_error(
                                super::object_pg_action_error_to_bucket_snapshot_error(error),
                            ));
                        }
                    }
                    self.release_object_generation_reservation(
                        &upload.bucket,
                        &upload.key,
                        &upload.session_id,
                    )
                    .map_err(super::object_pg_action_error_to_bucket_snapshot_error)
                    .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
                    aborted_any = true;
                    aborted_count += 1;
                }

                if aborted_any {
                    marker = None;
                    continue;
                }

                let Some(next_marker) = page.next_session_id_marker else {
                    break;
                };
                marker = Some(next_marker);
            }
        }

        Ok(PutObjectStreamUploadCleanup::Aborted {
            count: aborted_count,
        })
    }

    pub fn try_finalize_bucket_delete(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketDeleteFinalizeOutcome, BucketWriteDrainError> {
        let bucket_pg_id = self.bucket_metadata_pg_id(bucket);
        let bucket_store = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), PgId::new(bucket_pg_id))?;
        let info = match bucket_store
            .bucket_metadata_client()
            .head_bucket_raw(self.bucket_metadata_pg(bucket), bucket)
        {
            Ok(info) => info,
            Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketNotFound { .. })) => {
                return if self.bucket_name_absent_on_acting_set(PgId::new(bucket_pg_id), bucket)? {
                    Ok(BucketDeleteFinalizeOutcome::NotFound)
                } else {
                    Ok(BucketDeleteFinalizeOutcome::Pending)
                };
            }
            Err(other) => return Err(bucket_snapshot_error_to_bucket_write_drain_error(other)),
        };
        self.try_finalize_bucket_delete_root(&BucketDeleteFinalizeRoot {
            bucket: bucket.clone(),
            bucket_incarnation_generation: info.bucket_incarnation_generation,
        })
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_try_finalize_bucket_delete_root(
        &self,
        root: &crate::TestBucketDeleteFinalizeRoot,
    ) -> Result<BucketDeleteFinalizeOutcome, BucketWriteDrainError> {
        self.try_finalize_bucket_delete_root(&root.into())
    }

    pub(crate) fn try_finalize_bucket_delete_root(
        &self,
        root: &BucketDeleteFinalizeRoot,
    ) -> Result<BucketDeleteFinalizeOutcome, BucketWriteDrainError> {
        let bucket = &root.bucket;
        let bucket_pg_id = self.bucket_metadata_pg_id(bucket);
        let bucket_store = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), PgId::new(bucket_pg_id))?;
        let _ = observability::event(
            super::TRACE_TARGET,
            "bucket_finalize_start",
            Some(format_args!(
                "bucket={:?} bucket_pg_id={}",
                bucket,
                self.bucket_metadata_pg_id(bucket)
            )),
        );
        let bucket_incarnation_generation = {
            let info = match bucket_store
                .bucket_metadata_client()
                .head_bucket_raw(self.bucket_metadata_pg(bucket), bucket)
            {
                Ok(info) => info,
                Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketNotFound {
                    ..
                })) => {
                    let _ = observability::event(
                        super::TRACE_TARGET,
                        "bucket_finalize_not_found",
                        Some(format_args!("bucket={:?} pg_id={}", bucket, bucket_pg_id)),
                    );
                    if self.finalized_bucket_deleted_on_acting_set(PgId::new(bucket_pg_id), root)? {
                        self.finish_bucket_delete_finalize_work(root);
                        return Ok(BucketDeleteFinalizeOutcome::NotFound);
                    }
                    return Ok(BucketDeleteFinalizeOutcome::Pending);
                }
                Err(other) => return Err(bucket_snapshot_error_to_bucket_write_drain_error(other)),
            };
            if info.bucket_incarnation_generation != root.bucket_incarnation_generation {
                self.finish_bucket_delete_finalize_work(root);
                return Ok(BucketDeleteFinalizeOutcome::StaleIncarnation);
            }
            if info.state != BucketState::Deleting {
                let _ = observability::event(
                    super::TRACE_TARGET,
                    "bucket_finalize_not_deleting",
                    Some(format_args!(
                        "bucket={:?} pg_id={} state={:?}",
                        bucket, bucket_pg_id, info.state
                    )),
                );
                self.finish_bucket_delete_finalize_work(root);
                return Ok(BucketDeleteFinalizeOutcome::NotDeleting);
            }
            info.bucket_incarnation_generation
        };

        let claim_id = self.next_bucket_delete_finalize_claim_id()?;
        let owner_token = self.bucket_write_owner_token();
        let claimed_at = crate::clock::current_time_millis();
        let bucket_write_reservation_client =
            Arc::clone(bucket_store.bucket_write_reservation_client());
        let retained_bucket_write_reservation_client =
            Arc::clone(bucket_store.retained_bucket_write_reservation_client());
        let claim = bucket_write_reservation_client
            .acquire_bucket_delete_finalize_claim(
                self.validated_bucket_metadata_pg(PgId::new(bucket_pg_id)),
                bucket,
                bucket_incarnation_generation,
                &claim_id,
                &owner_token,
                self.operation_epoch(),
                claimed_at,
                claimed_at.checked_add(60_000),
                claimed_at,
            )
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        let Some(claim) = claim else {
            let _ = observability::event(
                super::TRACE_TARGET,
                "bucket_finalize_claim_busy",
                Some(format_args!("bucket={:?} pg_id={}", bucket, bucket_pg_id)),
            );
            return Ok(BucketDeleteFinalizeOutcome::Pending);
        };
        crate::node::maybe_run_after_bucket_delete_finalize_claim_hook(bucket);

        let retained_bucket_write_reservation_route = retained_bucket_write_reservation_client
            .open_retained_bucket_write_reservation_route(
                self.validated_bucket_metadata_pg(PgId::new(bucket_pg_id)),
                bucket,
            )
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;

        let release_finalizer_claim = || -> Result<(), BucketWriteDrainError> {
            retained_bucket_write_reservation_route
                .release_bucket_delete_finalize_claim(&claim)
                .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
            Ok(())
        };
        let stale_finalizer_claim_release = |error: &BucketWriteDrainError| {
            matches!(
                error,
                BucketWriteDrainError::Metadata(
                    MetadataError::ReclaimClaimNotFound { .. }
                        | MetadataError::ReclaimClaimConflict { .. }
                )
            )
        };

        let mut work_budget = super::RequestWorkBudget::new(
            std::time::Duration::from_millis(BUCKET_DELETE_FINALIZE_WORK_BUDGET_MILLIS),
            None,
        )
        .for_operation("bucket_delete_finalize")
        .for_pg(PgId::new(bucket_pg_id));
        let result = self.try_finalize_bucket_delete_claimed(
            bucket,
            bucket_pg_id,
            bucket_incarnation_generation,
            &mut work_budget,
        );
        match result {
            Ok(
                outcome @ (BucketDeleteFinalizeOutcome::Finalized
                | BucketDeleteFinalizeOutcome::NotFound),
            ) => match release_finalizer_claim() {
                Ok(()) => {
                    crate::node::maybe_run_after_bucket_delete_finalize_hook(bucket);
                    self.finish_bucket_delete_finalize_work(root);
                    Ok(outcome)
                }
                Err(BucketWriteDrainError::Metadata(MetadataError::ReclaimClaimNotFound {
                    ..
                })) => {
                    crate::node::maybe_run_after_bucket_delete_finalize_hook(bucket);
                    self.finish_bucket_delete_finalize_work(root);
                    Ok(outcome)
                }
                Err(BucketWriteDrainError::Metadata(MetadataError::ReclaimClaimConflict {
                    ..
                })) => {
                    crate::node::maybe_run_after_bucket_delete_finalize_hook(bucket);
                    self.finish_bucket_delete_finalize_work(root);
                    Ok(outcome)
                }
                Err(error) => Err(error),
            },
            Ok(outcome) => {
                if let Err(error) = release_finalizer_claim() {
                    if !stale_finalizer_claim_release(&error) {
                        return Err(error);
                    }
                }
                Ok(outcome)
            }
            Err(error) => {
                if let Err(release_error) = release_finalizer_claim() {
                    if !stale_finalizer_claim_release(&release_error) {
                        return Err(release_error);
                    }
                }
                Err(error)
            }
        }
    }

    fn try_finalize_bucket_delete_claimed(
        &self,
        bucket: &BucketName,
        bucket_pg_id: u32,
        bucket_incarnation_generation: u64,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<BucketDeleteFinalizeOutcome, BucketWriteDrainError> {
        self.require_route_map_valid_now()?;
        work_budget.check("bucket delete finalize work budget exhausted")?;
        let bucket_pg_id = PgId::new(bucket_pg_id);
        let bucket_store = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), bucket_pg_id)?;
        let progress_client = bucket_store.bucket_write_reservation_client();
        let bucket_info = bucket_store
            .bucket_metadata_client()
            .head_bucket_raw(self.validated_bucket_metadata_pg(bucket_pg_id), bucket)
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        if bucket_info.bucket_incarnation_generation != bucket_incarnation_generation
            || bucket_info.state != BucketState::Deleting
        {
            return Ok(BucketDeleteFinalizeOutcome::StaleIncarnation);
        }
        let existing_progress = progress_client
            .bucket_delete_attempt_outcome(self.validated_bucket_metadata_pg(bucket_pg_id), bucket)
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        let trusted_progress = existing_progress.as_ref().filter(|record| {
            record.bucket_execution_generation == bucket_info.bucket_execution_generation
                && record.outcome == BucketDeleteAttemptOutcomeKind::MarkDeleting
                && record.phase == BucketDeleteAttemptPhase::MarkDeleting
        });
        let pg_ids = self.metadata_pg_ids();
        let next_pg_id = trusted_progress
            .and_then(|record| record.finalizer_next_object_pg_id)
            .unwrap_or_else(|| pg_ids.first().copied().unwrap_or(0));
        let window = bounded_pg_scan_window(
            &pg_ids,
            Some(next_pg_id),
            BUCKET_DELETE_FINALIZE_SCAN_PG_BATCH,
        );

        for &raw_pg_id in &pg_ids[window.start..window.end] {
            self.require_route_map_valid_now()?;
            work_budget.check("bucket delete finalize work budget exhausted")?;
            let pg_id = PgId::new(raw_pg_id);
            if let Some(source) = self.bucket_visible_data_source_for_pg(bucket, pg_id, false)? {
                self.record_bucket_delete_finalizer_next_object_pg_id(
                    progress_client.as_ref(),
                    bucket_pg_id,
                    &bucket_info,
                    existing_progress.as_ref(),
                    raw_pg_id,
                )?;
                let _ = observability::event(
                    super::TRACE_TARGET,
                    "bucket_finalize_pending_visible_data",
                    Some(format_args!(
                        "bucket={:?} pg_id={} source={} source_pg_id={}",
                        bucket,
                        bucket_pg_id.get(),
                        source.label(),
                        source.pg_id().get()
                    )),
                );
                if bucket_delete_visible_data_diagnostics_enabled() {
                    eprintln!(
                        "bucket delete finalize found visible data source={} source_pg_id={}",
                        source.label(),
                        source.pg_id().get()
                    );
                }
                return Ok(BucketDeleteFinalizeOutcome::Pending);
            }

            loop {
                work_budget.check("bucket delete finalize work budget exhausted")?;
                let Some(root) = self.bucket_payload_reclaim_root_for_pg(bucket, pg_id)? else {
                    break;
                };
                let lease_count = self.local_map.object_payload_lease_count(
                    &root.bucket,
                    &root.key,
                    root.generation_id,
                )?;
                if lease_count == 0
                    && self
                        .reclaim_object_payload_if_unleased(
                            &root.bucket,
                            &root.key,
                            root.generation_id,
                        )
                        .map_err(super::object_pg_action_error_to_bucket_snapshot_error)
                        .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?
                {
                    continue;
                }
                if lease_count == 0 {
                    self.enqueue_object_payload_reclaim(
                        &root.bucket,
                        &root.key,
                        root.generation_id,
                    );
                }
                self.record_bucket_delete_finalizer_next_object_pg_id(
                    progress_client.as_ref(),
                    bucket_pg_id,
                    &bucket_info,
                    existing_progress.as_ref(),
                    raw_pg_id,
                )?;
                let _ = observability::event(
                    super::TRACE_TARGET,
                    "bucket_finalize_pending_reclaim",
                    Some(format_args!(
                        "bucket={:?} bucket_pg_id={} object_pg_id={}",
                        bucket,
                        bucket_pg_id.get(),
                        raw_pg_id
                    )),
                );
                return Ok(BucketDeleteFinalizeOutcome::Pending);
            }
        }

        let next_pg_id = window.next_pg_id.unwrap_or_else(|| {
            pg_ids
                .last()
                .copied()
                .map_or(0, |pg_id| pg_id.saturating_add(1))
        });
        self.record_bucket_delete_finalizer_next_object_pg_id(
            progress_client.as_ref(),
            bucket_pg_id,
            &bucket_info,
            existing_progress.as_ref(),
            next_pg_id,
        )?;
        if window.next_pg_id.is_some() {
            return Ok(BucketDeleteFinalizeOutcome::Pending);
        }

        self.delete_bucket_from_acting_set(
            bucket_pg_id,
            &BucketDeleteFinalizeRoot {
                bucket: bucket.clone(),
                bucket_incarnation_generation,
            },
        )
    }

    fn record_bucket_delete_finalizer_next_object_pg_id(
        &self,
        client: &dyn BucketWriteReservationNodeClient,
        bucket_pg_id: PgId,
        bucket_info: &BucketInfo,
        existing: Option<&BucketDeleteAttemptOutcomeRecord>,
        next_object_pg_id: u32,
    ) -> Result<(), BucketWriteDrainError> {
        let matching = existing.filter(|record| {
            record.bucket_execution_generation == bucket_info.bucket_execution_generation
                && record.outcome == BucketDeleteAttemptOutcomeKind::MarkDeleting
                && record.phase == BucketDeleteAttemptPhase::MarkDeleting
        });
        if matching.and_then(|record| record.finalizer_next_object_pg_id) == Some(next_object_pg_id)
        {
            return Ok(());
        }
        let record = BucketDeleteAttemptOutcomeRecord {
            bucket: bucket_info.name.clone(),
            drain_id: matching.map_or_else(
                || {
                    format!(
                        "finalizer-{}-{}",
                        bucket_info.bucket_execution_generation,
                        bucket_info.bucket_incarnation_generation
                    )
                },
                |record| record.drain_id.clone(),
            ),
            cluster_epoch: matching.map_or(self.operation_epoch(), |record| record.cluster_epoch),
            bucket_execution_generation: bucket_info.bucket_execution_generation,
            outcome: BucketDeleteAttemptOutcomeKind::MarkDeleting,
            phase: BucketDeleteAttemptPhase::MarkDeleting,
            detail: format!("bucket finalizer advanced to object PG {next_object_pg_id}"),
            post_reservation_next_object_pg_id: matching
                .and_then(|record| record.post_reservation_next_object_pg_id),
            finalizer_next_object_pg_id: Some(next_object_pg_id),
            updated_at: crate::clock::current_time_millis(),
        };
        client
            .record_bucket_delete_attempt_outcome(
                self.validated_bucket_metadata_pg(bucket_pg_id),
                &record,
            )
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)
    }

    fn bucket_visible_data_source(
        &self,
        bucket: &BucketName,
        include_stream_uploads: bool,
    ) -> Result<Option<BucketVisibleDataSource>, BucketWriteDrainError> {
        for raw_pg_id in self.metadata_pg_ids() {
            if let Some(source) = self.bucket_visible_data_source_for_pg(
                bucket,
                PgId::new(raw_pg_id),
                include_stream_uploads,
            )? {
                return Ok(Some(source));
            }
        }
        Ok(None)
    }

    fn bucket_visible_data_source_for_pg(
        &self,
        bucket: &BucketName,
        pg_id: PgId,
        include_stream_uploads: bool,
    ) -> Result<Option<BucketVisibleDataSource>, BucketWriteDrainError> {
        let listing_route = self
            .metadata_pg_primary_object_listing_route(pg_id)
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        let versions = listing_route
            .list_object_versions_page(&ListObjectVersionsReq {
                bucket: bucket.clone(),
                prefix: None,
                key_marker: None,
                version_id_marker: None,
                start_at: None,
                max_keys: 1,
            })
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        if !versions.versions.is_empty() {
            return Ok(Some(BucketVisibleDataSource::ObjectVersion { pg_id }));
        }

        let uploads = listing_route
            .list_multipart_uploads_page(&ListMultipartUploadsReq {
                bucket: bucket.clone(),
                prefix: None,
                page_start: None,
                max_uploads: 1,
            })
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        if !uploads.uploads.is_empty() {
            return Ok(Some(BucketVisibleDataSource::MultipartUpload { pg_id }));
        }

        if include_stream_uploads {
            let scan_pg_id = self.object_metadata_scan_pg(pg_id);
            let node = self
                .local_map
                .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
            let page = node
                .object_mutation_metadata_client()
                .list_stream_uploads_for_bucket_page(scan_pg_id, bucket, None, 1)
                .map_err(super::object_pg_action_error_to_bucket_snapshot_error)
                .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
            if !page.uploads.is_empty() {
                return Ok(Some(BucketVisibleDataSource::StreamUpload { pg_id }));
            }
        }
        Ok(None)
    }

    fn bucket_payload_reclaim_root_for_pg(
        &self,
        bucket: &BucketName,
        pg_id: PgId,
    ) -> Result<Option<PayloadReclaimRoot>, BucketWriteDrainError> {
        let scan_pg_id = self.object_metadata_scan_pg(pg_id);
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let root = node
            .object_mutation_metadata_client()
            .get_bucket_payload_reclaim_root(scan_pg_id, bucket)
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        if let Some(root) = root.as_ref() {
            self.validate_bucket_payload_reclaim_root_for_pg(pg_id, root, node.node_id())?;
        }
        Ok(root)
    }

    pub(crate) fn validate_bucket_payload_reclaim_root_for_pg(
        &self,
        requested_pg_id: PgId,
        root: &PayloadReclaimRoot,
        node_id: NodeId,
    ) -> Result<(), BucketWriteDrainError> {
        if self.object_metadata_pg_id(&root.bucket, &root.key) != requested_pg_id.get() {
            return Err(BucketWriteDrainError::Store(StoreError::StorageRpc {
                node_id: node_id.as_u32(),
                operation: "object bucket payload reclaim root",
                failure: StorageRpcErrorCode::Internal,
                detail: crate::StorageNodeFailureDetail::new(
                    "payload reclaim root does not belong to requested object metadata PG",
                ),
            }));
        }
        Ok(())
    }

    pub fn head_bucket_info(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        let pg_id = PgId::new(self.bucket_metadata_pg_id(bucket));
        self.local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?
            .bucket_metadata_client()
            .head_bucket_info(self.validated_bucket_metadata_pg(pg_id), bucket)
    }

    pub fn get_bucket_subresource(
        &self,
        bucket: &BucketName,
        kind: OpaqueBucketSubresourceKind,
    ) -> Result<Option<String>, BucketSnapshotLoadError> {
        let pg_id = PgId::new(self.bucket_metadata_pg_id(bucket));
        self.local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?
            .bucket_metadata_client()
            .get_bucket_subresource(
                self.validated_bucket_metadata_pg(pg_id),
                bucket,
                kind.stored_kind(),
            )
    }

    pub fn get_bucket_tags(
        &self,
        bucket: &BucketName,
    ) -> Result<Option<SerializedBucketTagSet>, BucketSnapshotLoadError> {
        let pg_id = PgId::new(self.bucket_metadata_pg_id(bucket));
        self.local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?
            .bucket_metadata_client()
            .get_bucket_tags(self.validated_bucket_metadata_pg(pg_id), bucket)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn put_bucket_versioning_and_load_info(
        &self,
        bucket: &BucketName,
        state: BucketVersioningState,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.put_bucket_versioning_and_load_info_with_route_validation(
            super::BucketMetadataMutationEffectRoute {
                pg_id: self.bucket_metadata_pg(bucket),
                bucket,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            || Ok(()),
            state,
        )
    }

    pub(super) fn put_bucket_versioning_and_load_info_with_route_validation(
        &self,
        route: super::BucketMetadataMutationEffectRoute<'_>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        state: BucketVersioningState,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        crate::metadata_command::metadata_command_publisher!(PutBucketVersioning);
        let super::BucketMetadataMutationEffectRoute {
            pg_id: bucket_pg_id,
            bucket,
            effect_fence,
        } = route;
        let pg_id = bucket_pg_id.pg_id();
        let primary_store = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        {
            require_valid_route()?;
            let info = primary_store
                .bucket_metadata_client()
                .head_bucket_raw(bucket_pg_id, bucket)?;
            if state == BucketVersioningState::Disabled
                && info.versioning != BucketVersioningState::Disabled
            {
                return Err(MetadataError::InvalidVersioningTransition {
                    from: info.versioning,
                    to: state,
                }
                .into());
            }
        }

        let mut work_budget = super::RequestWorkBudget::new(
            std::time::Duration::from_millis(METADATA_COMMAND_APPLY_RETRY_BUDGET_MILLIS),
            None,
        )
        .for_operation("put_bucket_versioning")
        .for_pg(pg_id);
        loop {
            work_budget.check("put bucket versioning command budget exhausted")?;
            require_valid_route()?;
            let (command, clear_pending_on_zero_apply) = if let Some(command) =
                self.pending_metadata_command_for_bucket(pg_id, bucket)?
            {
                if self.drain_unrelated_pending_metadata_command_for_bucket_with_work_budget(
                    pg_id,
                    bucket,
                    &command,
                    &mut work_budget,
                )? {
                    continue;
                }
                match command.payload() {
                    MetadataCommandPayload::PutBucketVersioning(versioning)
                        if versioning.bucket_name() == bucket =>
                    {
                        let same_request = versioning.bucket.versioning == state;
                        if !primary_store
                            .bucket_metadata_client()
                            .pending_put_bucket_versioning_command_matches_current(
                                self.validated_bucket_metadata_pg(pg_id),
                                bucket,
                                versioning,
                                state,
                            )?
                        {
                            if same_request {
                                return Err(conflicting_pending_metadata_command(
                                    "conflicting pending put bucket versioning command",
                                ));
                            }
                            self.drain_pending_metadata_command_pg_slot_with_work_budget(
                                pg_id,
                                bucket,
                                &command,
                                &mut work_budget,
                            )?;
                            continue;
                        }
                        (command, false)
                    }
                    MetadataCommandPayload::AdvanceMultipartCompletionBarrier(_) => {
                        self.drain_pending_multipart_completion_barrier_command_with_work_budget(
                            pg_id,
                            &command,
                            &mut work_budget,
                        )?;
                        continue;
                    }
                    _ => {
                        self.drain_pending_metadata_command_pg_slot_with_work_budget(
                            pg_id,
                            bucket,
                            &command,
                            &mut work_budget,
                        )?;
                        continue;
                    }
                }
            } else {
                let Some(command_id) = self
                    .next_bucket_metadata_command_id_or_drain_with_work_budget(
                        pg_id,
                        bucket,
                        &mut work_budget,
                    )?
                else {
                    continue;
                };
                let command = primary_store
                    .bucket_metadata_client()
                    .build_put_bucket_versioning_command(bucket_pg_id, bucket, command_id, state)?;
                require_valid_route()?;
                if !self.try_set_bucket_control_pending_command_or_retry_with_work_budget(
                    pg_id,
                    bucket,
                    &command,
                    Some(effect_fence),
                    &mut work_budget,
                )? {
                    continue;
                }
                (command, true)
            };
            let outcome = self.finish_pending_metadata_command_to_acting_set_with_work_budget(
                pg_id,
                &command,
                clear_pending_on_zero_apply,
                &mut work_budget,
            )?;
            if outcome == super::PendingMetadataCommandOutcome::Abandoned {
                continue;
            }

            let info = primary_store
                .bucket_metadata_client()
                .head_bucket_raw(bucket_pg_id, bucket)?;
            return Ok(info);
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn put_bucket_object_lock_and_load_info(
        &self,
        bucket: &BucketName,
        config: BucketObjectLockConfig,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.put_bucket_property_command_and_load_info(
            bucket,
            BucketPropertyMutation::ObjectLock(config),
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn put_bucket_encryption_and_load_info(
        &self,
        bucket: &BucketName,
        config: BucketEncryptionConfig,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.put_bucket_property_command_and_load_info(
            bucket,
            BucketPropertyMutation::Encryption(config),
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn put_bucket_public_access_block_and_load_info(
        &self,
        bucket: &BucketName,
        config: PublicAccessBlockConfig,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.put_bucket_property_command_and_load_info(
            bucket,
            BucketPropertyMutation::PublicAccessBlock(Some(config)),
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn delete_bucket_public_access_block_and_load_info(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.put_bucket_property_command_and_load_info(
            bucket,
            BucketPropertyMutation::PublicAccessBlock(None),
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn put_bucket_ownership_controls_and_load_info(
        &self,
        bucket: &BucketName,
        config: BucketOwnershipControls,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.put_bucket_property_command_and_load_info(
            bucket,
            BucketPropertyMutation::OwnershipControls(Some(config)),
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn delete_bucket_ownership_controls_and_load_info(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.put_bucket_property_command_and_load_info(
            bucket,
            BucketPropertyMutation::OwnershipControls(None),
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn put_bucket_abac_enabled_and_load_info(
        &self,
        bucket: &BucketName,
        enabled: bool,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.put_bucket_property_command_and_load_info(
            bucket,
            BucketPropertyMutation::AbacEnabled(enabled),
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn put_bucket_acl_and_load_info(
        &self,
        bucket: &BucketName,
        acl_grants: &AclGrants,
        summary: BucketAclSummary,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.put_bucket_acl_and_load_info_with_route_validation(
            super::BucketMetadataMutationEffectRoute {
                pg_id: self.bucket_metadata_pg(bucket),
                bucket,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            || Ok(()),
            acl_grants,
            summary,
        )
    }

    pub(super) fn put_bucket_acl_and_load_info_with_route_validation(
        &self,
        route: super::BucketMetadataMutationEffectRoute<'_>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        acl_grants: &AclGrants,
        summary: BucketAclSummary,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        crate::metadata_command::metadata_command_publisher!(PutBucketAcl);
        let super::BucketMetadataMutationEffectRoute {
            pg_id: bucket_pg_id,
            bucket,
            effect_fence,
        } = route;
        let pg_id = bucket_pg_id.pg_id();
        let primary_store = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        {
            require_valid_route()?;
            primary_store
                .bucket_metadata_client()
                .head_bucket_raw(bucket_pg_id, bucket)?;
        }

        let mut work_budget = super::RequestWorkBudget::new(
            std::time::Duration::from_millis(METADATA_COMMAND_APPLY_RETRY_BUDGET_MILLIS),
            None,
        )
        .for_operation("put_bucket_acl")
        .for_pg(pg_id);
        loop {
            work_budget.check("put bucket acl command budget exhausted")?;
            require_valid_route()?;
            let (command, clear_pending_on_zero_apply) = if let Some(command) =
                self.pending_metadata_command_for_bucket(pg_id, bucket)?
            {
                if self.drain_unrelated_pending_metadata_command_for_bucket_with_work_budget(
                    pg_id,
                    bucket,
                    &command,
                    &mut work_budget,
                )? {
                    continue;
                }
                match command.payload() {
                    MetadataCommandPayload::PutBucketAcl(acl) if acl.bucket_name() == bucket => {
                        let same_request = acl.bucket.acl_grants == *acl_grants
                            && acl.bucket.public_read == summary.public_read
                            && acl.bucket.public_write == summary.public_write;
                        if !primary_store
                            .bucket_metadata_client()
                            .pending_put_bucket_acl_command_matches_current(
                                self.validated_bucket_metadata_pg(pg_id),
                                bucket,
                                acl,
                                acl_grants,
                                summary,
                            )?
                        {
                            if same_request {
                                return Err(conflicting_pending_metadata_command(
                                    "conflicting pending put bucket acl command",
                                ));
                            }
                            self.drain_pending_metadata_command_pg_slot_with_work_budget(
                                pg_id,
                                bucket,
                                &command,
                                &mut work_budget,
                            )?;
                            continue;
                        }
                        (command, false)
                    }
                    MetadataCommandPayload::AdvanceMultipartCompletionBarrier(_) => {
                        self.drain_pending_multipart_completion_barrier_command_with_work_budget(
                            pg_id,
                            &command,
                            &mut work_budget,
                        )?;
                        continue;
                    }
                    _ => {
                        self.drain_pending_metadata_command_pg_slot_with_work_budget(
                            pg_id,
                            bucket,
                            &command,
                            &mut work_budget,
                        )?;
                        continue;
                    }
                }
            } else {
                let Some(command_id) = self
                    .next_bucket_metadata_command_id_or_drain_with_work_budget(
                        pg_id,
                        bucket,
                        &mut work_budget,
                    )?
                else {
                    continue;
                };
                let command = primary_store
                    .bucket_metadata_client()
                    .build_put_bucket_acl_command(
                        bucket_pg_id,
                        bucket,
                        command_id,
                        acl_grants,
                        summary,
                    )?;
                require_valid_route()?;
                if !self.try_set_bucket_control_pending_command_or_retry_with_work_budget(
                    pg_id,
                    bucket,
                    &command,
                    Some(effect_fence),
                    &mut work_budget,
                )? {
                    continue;
                }
                (command, true)
            };
            let outcome = self.finish_pending_metadata_command_to_acting_set_with_work_budget(
                pg_id,
                &command,
                clear_pending_on_zero_apply,
                &mut work_budget,
            )?;
            if outcome == super::PendingMetadataCommandOutcome::Abandoned {
                continue;
            }

            let info = primary_store
                .bucket_metadata_client()
                .head_bucket_raw(bucket_pg_id, bucket)?;
            return Ok(info);
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn put_bucket_property_command_and_load_info(
        &self,
        bucket: &BucketName,
        mutation: BucketPropertyMutation,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.put_bucket_property_command_and_load_info_with_route_validation(
            super::BucketMetadataMutationEffectRoute {
                pg_id: self.bucket_metadata_pg(bucket),
                bucket,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            || Ok(()),
            mutation,
        )
    }

    pub(super) fn put_bucket_property_command_and_load_info_with_route_validation(
        &self,
        route: super::BucketMetadataMutationEffectRoute<'_>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        mutation: BucketPropertyMutation,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        crate::metadata_command::metadata_command_publisher!(PutBucketProperty);
        let super::BucketMetadataMutationEffectRoute {
            pg_id: bucket_pg_id,
            bucket,
            effect_fence,
        } = route;
        let pg_id = bucket_pg_id.pg_id();
        let primary_store = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        {
            require_valid_route()?;
            primary_store
                .bucket_metadata_client()
                .head_bucket_raw(bucket_pg_id, bucket)?;
        }

        let mut work_budget = super::RequestWorkBudget::new(
            std::time::Duration::from_millis(METADATA_COMMAND_APPLY_RETRY_BUDGET_MILLIS),
            None,
        )
        .for_operation("put_bucket_property")
        .for_pg(pg_id);
        loop {
            work_budget.check("put bucket property command budget exhausted")?;
            require_valid_route()?;
            let (command, clear_pending_on_zero_apply) = if let Some(command) =
                self.pending_metadata_command_for_bucket(pg_id, bucket)?
            {
                if self.drain_unrelated_pending_metadata_command_for_bucket_with_work_budget(
                    pg_id,
                    bucket,
                    &command,
                    &mut work_budget,
                )? {
                    continue;
                }
                match command.payload() {
                    MetadataCommandPayload::PutBucketProperty(property)
                        if property.bucket_name() == bucket
                            && property.effect == mutation.effect() =>
                    {
                        if !primary_store
                            .bucket_metadata_client()
                            .pending_put_bucket_property_command_matches_current(
                                self.validated_bucket_metadata_pg(pg_id),
                                bucket,
                                property,
                                &mutation,
                            )?
                        {
                            self.drain_pending_metadata_command_pg_slot_with_work_budget(
                                pg_id,
                                bucket,
                                &command,
                                &mut work_budget,
                            )?;
                            continue;
                        }
                        (command, false)
                    }
                    MetadataCommandPayload::AdvanceMultipartCompletionBarrier(_) => {
                        self.drain_pending_multipart_completion_barrier_command_with_work_budget(
                            pg_id,
                            &command,
                            &mut work_budget,
                        )?;
                        continue;
                    }
                    _ => {
                        self.drain_pending_metadata_command_pg_slot_with_work_budget(
                            pg_id,
                            bucket,
                            &command,
                            &mut work_budget,
                        )?;
                        continue;
                    }
                }
            } else {
                let Some(command_id) = self
                    .next_bucket_metadata_command_id_or_drain_with_work_budget(
                        pg_id,
                        bucket,
                        &mut work_budget,
                    )?
                else {
                    continue;
                };
                let command = primary_store
                    .bucket_metadata_client()
                    .build_put_bucket_property_command(
                        bucket_pg_id,
                        bucket,
                        command_id,
                        &mutation,
                    )?;
                require_valid_route()?;
                if !self.try_set_bucket_control_pending_command_or_retry_with_work_budget(
                    pg_id,
                    bucket,
                    &command,
                    Some(effect_fence),
                    &mut work_budget,
                )? {
                    continue;
                }
                (command, true)
            };
            let outcome = self.finish_pending_metadata_command_to_acting_set_with_work_budget(
                pg_id,
                &command,
                clear_pending_on_zero_apply,
                &mut work_budget,
            )?;
            if outcome == super::PendingMetadataCommandOutcome::Abandoned {
                continue;
            }

            let info = primary_store
                .bucket_metadata_client()
                .head_bucket_raw(bucket_pg_id, bucket)?;
            return Ok(info);
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn put_bucket_subresource_and_load_info(
        &self,
        bucket: &BucketName,
        req: PutBucketSubresource<'_>,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.put_bucket_subresource_and_load_info_with_route_validation(
            super::BucketMetadataMutationEffectRoute {
                pg_id: self.bucket_metadata_pg(bucket),
                bucket,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            || Ok(()),
            req,
        )
    }

    pub(super) fn put_bucket_subresource_and_load_info_with_route_validation(
        &self,
        route: super::BucketMetadataMutationEffectRoute<'_>,
        require_valid_route: impl FnMut() -> Result<(), StoreError>,
        req: PutBucketSubresource<'_>,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        let mutation = BucketSubresourceMutation::from_put_request(req).map_err(|error| {
            MetadataError::InvariantViolation {
                context: "put bucket subresource",
                reason: format!("bucket subresource request is invalid: {error}"),
            }
        })?;
        self.put_bucket_subresource_command_and_load_info_with_route_validation(
            route,
            require_valid_route,
            mutation,
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn delete_bucket_subresource_and_load_info(
        &self,
        bucket: &BucketName,
        kind: OpaqueBucketSubresourceKind,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.delete_bucket_subresource_and_load_info_with_route_validation(
            super::BucketMetadataMutationEffectRoute {
                pg_id: self.bucket_metadata_pg(bucket),
                bucket,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            || Ok(()),
            kind.stored_kind(),
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn delete_bucket_tags_and_load_info(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.delete_bucket_subresource_and_load_info_with_route_validation(
            super::BucketMetadataMutationEffectRoute {
                pg_id: self.bucket_metadata_pg(bucket),
                bucket,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            || Ok(()),
            BucketSubresourceKind::Tagging,
        )
    }

    pub(super) fn delete_bucket_subresource_and_load_info_with_route_validation(
        &self,
        route: super::BucketMetadataMutationEffectRoute<'_>,
        require_valid_route: impl FnMut() -> Result<(), StoreError>,
        kind: BucketSubresourceKind,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.put_bucket_subresource_command_and_load_info_with_route_validation(
            route,
            require_valid_route,
            BucketSubresourceMutation::Delete { kind },
        )
    }

    fn put_bucket_subresource_command_and_load_info_with_route_validation(
        &self,
        route: super::BucketMetadataMutationEffectRoute<'_>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        mutation: BucketSubresourceMutation,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        crate::metadata_command::metadata_command_publisher!(PutBucketSubresource);
        let super::BucketMetadataMutationEffectRoute {
            pg_id: bucket_pg_id,
            bucket,
            effect_fence,
        } = route;
        let pg_id = bucket_pg_id.pg_id();
        let primary_store = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        {
            require_valid_route()?;
            primary_store
                .bucket_metadata_client()
                .head_bucket_raw(bucket_pg_id, bucket)?;
        }
        let mut work_budget = super::RequestWorkBudget::new(
            std::time::Duration::from_millis(METADATA_COMMAND_APPLY_RETRY_BUDGET_MILLIS),
            None,
        )
        .for_operation("put_bucket_subresource")
        .for_pg(pg_id);
        loop {
            work_budget.check("put bucket subresource command budget exhausted")?;
            require_valid_route()?;
            let (command, clear_pending_on_zero_apply) = if let Some(command) =
                self.pending_metadata_command_for_bucket(pg_id, bucket)?
            {
                if self.drain_unrelated_pending_metadata_command_for_bucket_with_work_budget(
                    pg_id,
                    bucket,
                    &command,
                    &mut work_budget,
                )? {
                    continue;
                }
                match command.payload() {
                    MetadataCommandPayload::PutBucketSubresource(subresource)
                        if subresource.matches_mutation(bucket, &mutation) =>
                    {
                        (command, false)
                    }
                    MetadataCommandPayload::AdvanceMultipartCompletionBarrier(_) => {
                        self.drain_pending_multipart_completion_barrier_command_with_work_budget(
                            pg_id,
                            &command,
                            &mut work_budget,
                        )?;
                        continue;
                    }
                    _ => {
                        self.drain_pending_metadata_command_pg_slot_with_work_budget(
                            pg_id,
                            bucket,
                            &command,
                            &mut work_budget,
                        )?;
                        continue;
                    }
                }
            } else {
                let Some(command_id) = self
                    .next_bucket_metadata_command_id_or_drain_with_work_budget(
                        pg_id,
                        bucket,
                        &mut work_budget,
                    )?
                else {
                    continue;
                };
                let command = primary_store
                    .bucket_metadata_client()
                    .build_put_bucket_subresource_command(
                        bucket_pg_id,
                        bucket,
                        command_id,
                        &mutation,
                    )?;
                require_valid_route()?;
                if !self.try_set_bucket_control_pending_command_or_retry_with_work_budget(
                    pg_id,
                    bucket,
                    &command,
                    Some(effect_fence),
                    &mut work_budget,
                )? {
                    continue;
                }
                (command, true)
            };
            let outcome = self.finish_pending_metadata_command_to_acting_set_with_work_budget(
                pg_id,
                &command,
                clear_pending_on_zero_apply,
                &mut work_budget,
            )?;
            if outcome == super::PendingMetadataCommandOutcome::Abandoned {
                continue;
            }

            let info = primary_store
                .bucket_metadata_client()
                .head_bucket_raw(bucket_pg_id, bucket)?;
            return Ok(info);
        }
    }

    pub(super) fn list_buckets_for_owner_with_route_validation(
        &self,
        owner_canonical_id: &str,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<Vec<BucketInfo>, ObjectPgActionError> {
        let mut buckets = Vec::new();
        for pg_id in self.metadata_pg_ids() {
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            let pg_id = PgId::new(pg_id);
            let node = self
                .local_map
                .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
            let mut page = node
                .bucket_metadata_client()
                .list_buckets(self.validated_bucket_metadata_pg(pg_id), owner_canonical_id)
                .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
            self.validate_bucket_list_page_for_pg(pg_id, node.node_id(), &page)?;
            #[cfg(any(test, feature = "test-hooks"))]
            self.maybe_run_after_metadata_listing_pg_complete_hook(pg_id.get());
            buckets.append(&mut page);
        }
        buckets.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(buckets)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn list_buckets_for_owner(
        &self,
        owner_canonical_id: &str,
    ) -> Result<Vec<BucketInfo>, ObjectPgActionError> {
        self.list_buckets_for_owner_with_route_validation(owner_canonical_id, || Ok(()))
    }

    pub(crate) fn validate_bucket_list_page_for_pg(
        &self,
        pg_id: PgId,
        node_id: NodeId,
        page: &[BucketInfo],
    ) -> Result<(), ObjectPgActionError> {
        for bucket in page {
            let expected_pg_id = self.bucket_metadata_pg_id(&bucket.name);
            if expected_pg_id != pg_id.get() {
                return Err(ObjectPgActionError::Store(StoreError::StorageRpc {
                    node_id: node_id.as_u32(),
                    operation: "validate bucket list response",
                    failure: StorageRpcErrorCode::Internal,
                    detail: crate::StorageNodeFailureDetail::new(format!(
                        "bucket {} belongs to bucket PG {}, not response PG {}",
                        bucket.name.as_str(),
                        expected_pg_id,
                        pg_id.get()
                    )),
                }));
            }
        }
        Ok(())
    }

    pub fn list_lifecycle_sweep_buckets(
        &self,
    ) -> Result<LifecycleSweepBuckets, ObjectPgActionError> {
        let mut lifecycle_buckets = Vec::new();
        let mut aborting_buckets = Vec::new();
        for pg_id in self.metadata_pg_ids() {
            let node = self
                .local_map
                .metadata_pg_primary_node(self.operation_epoch(), PgId::new(pg_id))?;
            let buckets = node
                .bucket_write_reservation_client()
                .list_lifecycle_sweep_buckets(self.validated_bucket_metadata_pg(PgId::new(pg_id)))
                .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
            lifecycle_buckets.extend(buckets.lifecycle_buckets);
            aborting_buckets.extend(buckets.aborting_buckets);
        }
        lifecycle_buckets.sort_by(|a, b| a.name.cmp(&b.name));
        aborting_buckets.sort();
        aborting_buckets.dedup();
        Ok(LifecycleSweepBuckets {
            lifecycle_buckets,
            aborting_buckets,
        })
    }

    pub fn list_lifecycle_sweep_roots(
        &self,
        now: u64,
    ) -> Result<Vec<LifecycleSweepRoot>, ObjectPgActionError> {
        let mut roots = Vec::new();
        for pg_id in self.metadata_pg_ids() {
            let node = self
                .local_map
                .metadata_pg_primary_node(self.operation_epoch(), PgId::new(pg_id))?;
            roots.extend(
                node.bucket_write_reservation_client()
                    .get_lifecycle_sweep_roots(
                        self.validated_bucket_metadata_pg(PgId::new(pg_id)),
                        now,
                        LIFECYCLE_SWEEP_ROOT_SCAN_LIMIT_PER_PG,
                    )
                    .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?,
            );
        }
        for bucket in self.list_lifecycle_sweep_buckets()?.aborting_buckets {
            match self.head_bucket_info(&bucket) {
                Ok(bucket_info) => roots.push(LifecycleSweepRoot {
                    bucket,
                    bucket_incarnation_generation: bucket_info.bucket_incarnation_generation,
                    source: LifecycleSweepRootSource::AbortingMultipartUpload,
                }),
                Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketNotFound {
                    ..
                })) => {}
                Err(BucketSnapshotLoadError::Metadata(error)) => return Err(error.into()),
                Err(BucketSnapshotLoadError::Store(error)) => return Err(error.into()),
            }
        }
        roots.sort_by(|left, right| {
            lifecycle_sweep_root_source_rank(left.source)
                .cmp(&lifecycle_sweep_root_source_rank(right.source))
                .then_with(|| left.bucket.cmp(&right.bucket))
                .then_with(|| {
                    left.bucket_incarnation_generation
                        .cmp(&right.bucket_incarnation_generation)
                })
        });
        roots.dedup_by(|left, right| {
            left.bucket == right.bucket
                && left.bucket_incarnation_generation == right.bucket_incarnation_generation
        });
        Ok(roots)
    }

    pub fn acquire_lifecycle_sweep_claim(
        &self,
        bucket: &BucketName,
        bucket_incarnation_generation: u64,
        now: u64,
    ) -> Result<Option<LifecycleSweepClaimRecord>, ObjectPgActionError> {
        let claim_id = self.next_lifecycle_sweep_claim_id()?;
        let owner_token = self.bucket_write_owner_token();
        let pg_id = self.bucket_metadata_pg_id(bucket);
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), PgId::new(pg_id))?;
        node.bucket_write_reservation_client()
            .acquire_lifecycle_sweep_claim(
                self.validated_bucket_metadata_pg(PgId::new(pg_id)),
                bucket,
                bucket_incarnation_generation,
                &claim_id,
                &owner_token,
                self.operation_epoch(),
                now,
                now.checked_add(LIFECYCLE_SWEEP_CLAIM_LEASE_MILLIS),
                now,
            )
            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)
    }

    pub fn heartbeat_lifecycle_sweep_claim(
        &self,
        claim: &LifecycleSweepClaimRecord,
        now: u64,
    ) -> Result<LifecycleSweepClaimRecord, ObjectPgActionError> {
        let pg_id = self.bucket_metadata_pg_id(&claim.bucket);
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), PgId::new(pg_id))?;
        node.bucket_write_reservation_client()
            .heartbeat_lifecycle_sweep_claim(
                self.validated_bucket_metadata_pg(PgId::new(pg_id)),
                claim,
                now,
                now.checked_add(LIFECYCLE_SWEEP_CLAIM_LEASE_MILLIS),
            )
            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)
    }

    pub fn record_lifecycle_sweep_claim_error(
        &self,
        claim: &LifecycleSweepClaimRecord,
        last_error: &str,
    ) -> Result<LifecycleSweepClaimRecord, ObjectPgActionError> {
        let pg_id = self.bucket_metadata_pg_id(&claim.bucket);
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), PgId::new(pg_id))?;
        node.bucket_write_reservation_client()
            .record_lifecycle_sweep_claim_error(
                self.validated_bucket_metadata_pg(PgId::new(pg_id)),
                claim,
                last_error,
            )
            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)
    }

    pub fn release_lifecycle_sweep_claim(
        &self,
        claim: &LifecycleSweepClaimRecord,
    ) -> Result<(), ObjectPgActionError> {
        let pg_id = self.bucket_metadata_pg_id(&claim.bucket);
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), PgId::new(pg_id))?;
        node.retained_bucket_write_reservation_client()
            .open_retained_bucket_write_reservation_route(
                self.validated_bucket_metadata_pg(PgId::new(pg_id)),
                &claim.bucket,
            )
            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?
            .release_lifecycle_sweep_claim(claim)
            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)
    }

    pub fn list_all_objects_for_bucket(
        &self,
        bucket: &BucketName,
    ) -> Result<Vec<StoredObject>, ObjectPgActionError> {
        let mut all_objects = Vec::new();
        for pg_id in self.metadata_pg_ids() {
            let mut start_after = None;
            loop {
                let resp = self.list_objects_page(
                    pg_id,
                    &ListObjectsReq {
                        bucket: bucket.clone(),
                        prefix: None,
                        start_after: start_after.clone(),
                        start_at: None,
                        max_keys: INTERNAL_LIST_PAGE_SIZE,
                    },
                )?;
                all_objects.extend(resp.objects);
                if !resp.is_truncated {
                    break;
                }
                start_after = resp.next_start_after;
            }
        }
        all_objects.sort_by(|a, b| a.key().cmp(b.key()));
        all_objects.dedup_by(|a, b| a.key() == b.key());
        Ok(all_objects)
    }

    pub fn list_all_object_versions_for_bucket(
        &self,
        bucket: &BucketName,
    ) -> Result<Vec<StoredObject>, ObjectPgActionError> {
        let mut cursors = Vec::new();
        for pg_id in self.metadata_pg_ids() {
            let mut key_marker = None;
            let mut version_id_marker = None;
            let mut versions = Vec::new();
            loop {
                let resp = self.list_object_versions_page(
                    pg_id,
                    &ListObjectVersionsReq {
                        bucket: bucket.clone(),
                        prefix: None,
                        key_marker: key_marker.clone(),
                        version_id_marker,
                        start_at: None,
                        max_keys: INTERNAL_LIST_PAGE_SIZE,
                    },
                )?;
                versions.extend(resp.versions);
                if !resp.is_truncated {
                    break;
                }
                key_marker = resp.next_key_marker;
                version_id_marker = resp.next_version_id_marker;
            }
            cursors.push(VersionCursor {
                pg_id,
                versions,
                next_index: 0,
                next_page_start: None,
            });
        }

        let mut merged_versions = Vec::new();
        while let Some((cursor_index, _)) = cursors
            .iter()
            .enumerate()
            .filter_map(|(cursor_index, cursor)| {
                cursor.current().map(|version| (cursor_index, version))
            })
            .min_by(|(left_index, left), (right_index, right)| {
                left.key()
                    .cmp(right.key())
                    .then_with(|| left_index.cmp(right_index))
            })
        {
            merged_versions.push(cursors[cursor_index].pop_current());
        }

        Ok(merged_versions)
    }

    pub fn list_all_multipart_uploads_for_bucket(
        &self,
        bucket: &BucketName,
    ) -> Result<Vec<MultipartUploadRecord>, ObjectPgActionError> {
        let mut uploads = Vec::new();
        for pg_id in self.metadata_pg_ids() {
            let mut key_marker = None;
            let mut upload_id_marker = None;
            loop {
                let resp = self.list_multipart_uploads_page(
                    pg_id,
                    &ListMultipartUploadsReq {
                        bucket: bucket.clone(),
                        prefix: None,
                        page_start: key_marker.clone().map(|key_marker| {
                            ListMultipartUploadsPageStart::After {
                                key_marker,
                                upload_id_marker: upload_id_marker.clone(),
                            }
                        }),
                        max_uploads: INTERNAL_LIST_PAGE_SIZE,
                    },
                )?;
                uploads.extend(resp.uploads);
                if !resp.is_truncated {
                    break;
                }
                key_marker = resp.next_key_marker;
                upload_id_marker = resp.next_upload_id_marker;
            }
        }
        uploads.sort_by(|a, b| {
            a.key
                .cmp(&b.key)
                .then_with(|| a.upload_id.cmp(&b.upload_id))
        });
        Ok(uploads)
    }

    pub(super) fn list_objects_for_bucket_with_route_validation(
        &self,
        scan: &super::ObjectMetadataScanRoute<'_>,
        prefix: Option<&ObjectKey>,
        delimiter: Option<&str>,
        continuation_token: Option<&ObjectKey>,
        max_keys: u32,
    ) -> Result<ListedBucketObjects, ObjectPgActionError> {
        let bucket = scan.bucket;
        if max_keys == 0 {
            return Ok(ListedBucketObjects {
                objects: Vec::new(),
                common_prefixes: Vec::new(),
                is_truncated: false,
                next_continuation_token: None,
            });
        }

        let fetch_limit = max_keys.saturating_add(1);
        let prefix = prefix.cloned();
        let continuation_token = continuation_token.cloned();

        if delimiter.is_none() {
            let max = max_keys as usize;
            let mut smallest = BoundedSmallestRecords::new(max.saturating_add(1));
            for pg_id in self.metadata_pg_ids() {
                scan.require_valid().map_err(ObjectPgActionError::Store)?;
                let resp = self.list_objects_page(
                    pg_id,
                    &ListObjectsReq {
                        bucket: bucket.clone(),
                        prefix: prefix.clone(),
                        start_after: continuation_token.clone(),
                        start_at: None,
                        max_keys: fetch_limit,
                    },
                )?;
                #[cfg(any(test, feature = "test-hooks"))]
                self.maybe_run_after_metadata_listing_pg_complete_hook(pg_id);
                for object in resp.objects {
                    smallest.insert(object.key().clone(), object);
                }
            }

            let mut objects = smallest.into_values();
            let is_truncated = objects.len() > max;
            objects.truncate(max);
            let next_continuation_token = is_truncated
                .then(|| objects.last().map(|object| object.key().clone()))
                .flatten();
            return Ok(ListedBucketObjects {
                objects,
                common_prefixes: Vec::new(),
                is_truncated,
                next_continuation_token,
            });
        }

        let prefix_str = prefix.as_ref().map_or("", ObjectKey::as_str);
        let delimiter = delimiter.expect("checked above");
        let initial_start = match continuation_token {
            Some(token) => {
                let token_str = token.as_str();
                if let Some(after_prefix) = token_str.strip_prefix(prefix_str) {
                    if after_prefix.ends_with(delimiter) {
                        if let Some(upper_bound) = crate::object_key_prefix_upper_bound(&token) {
                            Some(ListObjectsPageStart::At(upper_bound))
                        } else {
                            Some(ListObjectsPageStart::After(token))
                        }
                    } else {
                        Some(ListObjectsPageStart::After(token))
                    }
                } else {
                    Some(ListObjectsPageStart::After(token))
                }
            }
            None => None,
        };

        let fetch_objects_page = |cursor: &mut ObjectCursor,
                                  start: Option<ListObjectsPageStart>|
         -> Result<(), ObjectPgActionError> {
            let (start_after, start_at) = match start {
                Some(ListObjectsPageStart::After(key)) => (Some(key), None),
                Some(ListObjectsPageStart::At(key)) => (None, Some(key)),
                None => (None, None),
            };
            scan.require_valid().map_err(ObjectPgActionError::Store)?;
            let resp = self.list_objects_page(
                cursor.pg_id,
                &ListObjectsReq {
                    bucket: bucket.clone(),
                    prefix: prefix.clone(),
                    start_after,
                    start_at,
                    max_keys: fetch_limit,
                },
            )?;
            #[cfg(any(test, feature = "test-hooks"))]
            self.maybe_run_after_metadata_listing_pg_complete_hook(cursor.pg_id);
            cursor.objects = resp.objects;
            cursor.next_index = 0;
            cursor.next_page_start = resp.next_start_after.map(ListObjectsPageStart::After);
            Ok(())
        };

        let refill_cursor = |cursor: &mut ObjectCursor| -> Result<(), ObjectPgActionError> {
            while cursor.current().is_none() {
                let Some(next_start) = cursor.next_page_start.clone() else {
                    break;
                };
                fetch_objects_page(cursor, Some(next_start))?;
            }
            Ok(())
        };

        let jump_cursor_to = |cursor: &mut ObjectCursor,
                              start: ListObjectsPageStart|
         -> Result<(), ObjectPgActionError> {
            cursor.objects.clear();
            cursor.next_index = 0;
            cursor.next_page_start = Some(start);
            refill_cursor(cursor)
        };

        let skip_cursor_prefix =
            |cursor: &mut ObjectCursor, common_prefix: &str| -> Result<(), ObjectPgActionError> {
                while cursor
                    .current()
                    .is_some_and(|obj| obj.key().as_str().starts_with(common_prefix))
                {
                    cursor.next_index += 1;
                    refill_cursor(cursor)?;
                }
                Ok(())
            };

        let mut cursors = Vec::new();
        for pg_id in self.metadata_pg_ids() {
            let mut cursor = ObjectCursor {
                pg_id,
                objects: Vec::new(),
                next_index: 0,
                next_page_start: None,
            };
            fetch_objects_page(&mut cursor, initial_start.clone())?;
            cursors.push(cursor);
        }

        let max = max_keys as usize;
        let mut objects = Vec::new();
        let mut common_prefixes = Vec::new();
        let mut next_continuation_token = None;
        let mut is_truncated = false;
        let mut active_common_prefix: Option<(ObjectKey, Option<ObjectKey>)> = None;

        while let Some((cursor_index, current_key)) = cursors
            .iter()
            .enumerate()
            .filter_map(|(cursor_index, cursor)| {
                cursor
                    .current()
                    .map(|object| (cursor_index, object.key().clone()))
            })
            .min_by(|(left_index, left_key), (right_index, right_key)| {
                left_key
                    .cmp(right_key)
                    .then_with(|| left_index.cmp(right_index))
            })
        {
            if let Some((ref common_prefix, ref upper_bound)) = active_common_prefix {
                if current_key.as_str().starts_with(common_prefix.as_str()) {
                    if let Some(upper_bound) = upper_bound.clone() {
                        jump_cursor_to(
                            &mut cursors[cursor_index],
                            ListObjectsPageStart::At(upper_bound),
                        )?;
                    } else {
                        skip_cursor_prefix(&mut cursors[cursor_index], common_prefix.as_str())?;
                    }
                    continue;
                }
                active_common_prefix = None;
            }

            let current = cursors[cursor_index]
                .current()
                .expect("selected cursor should have a current object")
                .clone();
            if let Some(common_prefix_key) =
                crate::object_key_common_prefix(current.key(), prefix_str, delimiter)
            {
                let upper_bound = crate::object_key_prefix_upper_bound(&common_prefix_key);
                active_common_prefix = Some((common_prefix_key.clone(), upper_bound));
                if objects.len() + common_prefixes.len() >= max {
                    is_truncated = true;
                    break;
                }
                next_continuation_token = Some(common_prefix_key.clone());
                common_prefixes.push(common_prefix_key);
                continue;
            }

            if objects.len() + common_prefixes.len() >= max {
                is_truncated = true;
                break;
            }

            next_continuation_token = Some(current.key().clone());
            objects.push(current);
            cursors[cursor_index].next_index += 1;
            refill_cursor(&mut cursors[cursor_index])?;
        }

        Ok(ListedBucketObjects {
            objects,
            common_prefixes,
            is_truncated,
            next_continuation_token: if is_truncated {
                next_continuation_token
            } else {
                None
            },
        })
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn list_objects_for_bucket(
        &self,
        bucket: &BucketName,
        prefix: Option<&ObjectKey>,
        delimiter: Option<&str>,
        continuation_token: Option<&ObjectKey>,
        max_keys: u32,
    ) -> Result<ListedBucketObjects, ObjectPgActionError> {
        let require_valid_route = || Ok(());
        let scan = super::ObjectMetadataScanRoute {
            bucket,
            require_valid_route: &require_valid_route,
        };
        self.list_objects_for_bucket_with_route_validation(
            &scan,
            prefix,
            delimiter,
            continuation_token,
            max_keys,
        )
    }

    pub(super) fn list_object_versions_for_bucket_with_route_validation(
        &self,
        scan: &super::ObjectMetadataScanRoute<'_>,
        prefix: Option<&ObjectKey>,
        delimiter: Option<&str>,
        key_marker: Option<&ObjectKey>,
        version_id_marker: Option<VersionId>,
        max_keys: u32,
    ) -> Result<ListedBucketObjectVersions, ObjectPgActionError> {
        let bucket = scan.bucket;
        if max_keys == 0 {
            return Ok(ListedBucketObjectVersions {
                versions: Vec::new(),
                common_prefixes: Vec::new(),
                is_truncated: false,
                next_key_marker: None,
                next_version_id_marker: None,
            });
        }

        let fetch_limit = max_keys.saturating_add(1);
        let prefix = prefix.cloned();
        let key_marker = key_marker.cloned();
        let delimiter = delimiter.filter(|delimiter| !delimiter.is_empty());
        let prefix_str = prefix.as_ref().map_or("", ObjectKey::as_str);
        let fetch_versions_page = |cursor: &mut VersionCursor,
                                   start: Option<ListVersionsPageStart>|
         -> Result<(), ObjectPgActionError> {
            let (key_marker, version_id_marker, start_at) = match start {
                Some(ListVersionsPageStart::After {
                    key_marker,
                    version_id_marker,
                }) => (Some(key_marker), version_id_marker, None),
                Some(ListVersionsPageStart::At(key)) => (None, None, Some(key)),
                None => (None, None, None),
            };
            scan.require_valid().map_err(ObjectPgActionError::Store)?;
            let resp = self.list_object_versions_page(
                cursor.pg_id,
                &ListObjectVersionsReq {
                    bucket: bucket.clone(),
                    prefix: prefix.clone(),
                    key_marker,
                    version_id_marker,
                    start_at,
                    max_keys: fetch_limit,
                },
            )?;
            #[cfg(any(test, feature = "test-hooks"))]
            self.maybe_run_after_metadata_listing_pg_complete_hook(cursor.pg_id);
            cursor.versions = resp.versions;
            cursor.next_index = 0;
            cursor.next_page_start =
                resp.next_key_marker
                    .map(|key_marker| ListVersionsPageStart::After {
                        key_marker,
                        version_id_marker: resp.next_version_id_marker,
                    });
            Ok(())
        };

        let refill_cursor = |cursor: &mut VersionCursor| -> Result<(), ObjectPgActionError> {
            while cursor.current().is_none() {
                let Some(next_start) = cursor.next_page_start.clone() else {
                    break;
                };
                fetch_versions_page(cursor, Some(next_start))?;
            }
            Ok(())
        };

        let jump_cursor_to = |cursor: &mut VersionCursor,
                              start: ListVersionsPageStart|
         -> Result<(), ObjectPgActionError> {
            cursor.versions.clear();
            cursor.next_index = 0;
            cursor.next_page_start = Some(start);
            refill_cursor(cursor)
        };

        let skip_cursor_prefix =
            |cursor: &mut VersionCursor, common_prefix: &str| -> Result<(), ObjectPgActionError> {
                while cursor
                    .current()
                    .is_some_and(|obj| obj.key().as_str().starts_with(common_prefix))
                {
                    cursor.next_index += 1;
                    refill_cursor(cursor)?;
                }
                Ok(())
            };

        let mut cursors = Vec::new();
        for pg_id in self.metadata_pg_ids() {
            let mut cursor = VersionCursor {
                pg_id,
                versions: Vec::new(),
                next_index: 0,
                next_page_start: None,
            };
            let initial_start = key_marker
                .clone()
                .map(|key_marker| ListVersionsPageStart::After {
                    key_marker,
                    version_id_marker,
                });
            fetch_versions_page(&mut cursor, initial_start)?;
            cursors.push(cursor);
        }

        let max = max_keys as usize;
        let mut versions = Vec::new();
        let mut common_prefixes = Vec::new();
        let mut is_truncated = false;
        let mut next_key_marker = None;
        let mut next_version_id_marker = None;
        let mut active_common_prefix = key_marker.as_ref().and_then(|marker| {
            let delimiter = delimiter?;
            let after_prefix = marker.as_str().strip_prefix(prefix_str)?;
            after_prefix
                .ends_with(delimiter)
                .then(|| (marker.clone(), crate::object_key_prefix_upper_bound(marker)))
        });

        while let Some((cursor_index, current_key)) = cursors
            .iter()
            .enumerate()
            .filter_map(|(cursor_index, cursor)| {
                cursor
                    .current()
                    .map(|version| (cursor_index, version.key().clone()))
            })
            .min_by(|(left_index, left_key), (right_index, right_key)| {
                left_key
                    .cmp(right_key)
                    .then_with(|| left_index.cmp(right_index))
            })
        {
            if let Some((ref common_prefix, ref upper_bound)) = active_common_prefix {
                if current_key.as_str().starts_with(common_prefix.as_str()) {
                    if let Some(upper_bound) = upper_bound.clone() {
                        jump_cursor_to(
                            &mut cursors[cursor_index],
                            ListVersionsPageStart::At(upper_bound),
                        )?;
                    } else {
                        skip_cursor_prefix(&mut cursors[cursor_index], common_prefix.as_str())?;
                    }
                    continue;
                }
                active_common_prefix = None;
            }

            let current = cursors[cursor_index]
                .current()
                .expect("selected cursor should have a current object")
                .clone();
            if let Some(delimiter) = delimiter {
                if let Some(common_prefix_key) =
                    crate::object_key_common_prefix(current.key(), prefix_str, delimiter)
                {
                    let upper_bound = crate::object_key_prefix_upper_bound(&common_prefix_key);
                    active_common_prefix = Some((common_prefix_key.clone(), upper_bound.clone()));
                    if key_marker
                        .as_ref()
                        .is_some_and(|marker| common_prefix_key.as_str() <= marker.as_str())
                    {
                        if let Some(upper_bound) = upper_bound {
                            jump_cursor_to(
                                &mut cursors[cursor_index],
                                ListVersionsPageStart::At(upper_bound),
                            )?;
                        } else {
                            skip_cursor_prefix(
                                &mut cursors[cursor_index],
                                common_prefix_key.as_str(),
                            )?;
                        }
                        continue;
                    }
                    if versions.len() + common_prefixes.len() >= max {
                        is_truncated = true;
                        break;
                    }
                    next_key_marker = Some(common_prefix_key.clone());
                    next_version_id_marker = None;
                    common_prefixes.push(common_prefix_key);
                    continue;
                }
            }

            if versions.len() + common_prefixes.len() >= max {
                is_truncated = true;
                break;
            }

            next_key_marker = Some(current.key().clone());
            next_version_id_marker = Some(current.version_id());
            versions.push(current);
            cursors[cursor_index].next_index += 1;
            refill_cursor(&mut cursors[cursor_index])?;
        }

        Ok(ListedBucketObjectVersions {
            versions,
            common_prefixes,
            is_truncated,
            next_key_marker: if is_truncated { next_key_marker } else { None },
            next_version_id_marker: if is_truncated {
                next_version_id_marker
            } else {
                None
            },
        })
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn list_object_versions_for_bucket(
        &self,
        bucket: &BucketName,
        prefix: Option<&ObjectKey>,
        delimiter: Option<&str>,
        key_marker: Option<&ObjectKey>,
        version_id_marker: Option<VersionId>,
        max_keys: u32,
    ) -> Result<ListedBucketObjectVersions, ObjectPgActionError> {
        let require_valid_route = || Ok(());
        let scan = super::ObjectMetadataScanRoute {
            bucket,
            require_valid_route: &require_valid_route,
        };
        self.list_object_versions_for_bucket_with_route_validation(
            &scan,
            prefix,
            delimiter,
            key_marker,
            version_id_marker,
            max_keys,
        )
    }

    pub fn load_object_if<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        action: impl FnOnce(&StoredObject) -> Result<T, E>,
    ) -> Result<Result<T, E>, ObjectPgActionError> {
        let route = super::ObjectReadMetadataRoute {
            bucket,
            key,
            version_id,
            snapshot_mode: ObjectReadSnapshotMode::MetadataOnly,
            pg_id: self.object_metadata_pg(bucket, key),
        };
        self.load_object_if_on_route(&route, action, || Ok(()))
    }

    pub(super) fn load_object_if_on_route<T, E>(
        &self,
        route: &super::ObjectReadMetadataRoute<'_>,
        action: impl FnOnce(&StoredObject) -> Result<T, E>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<Result<T, E>, ObjectPgActionError> {
        let pg_id = route.pg_id.pg_id();
        require_valid_route()?;
        let object_read_client = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?
            .object_read_metadata_client();
        require_valid_route()?;
        let object_read_route = object_read_client.open_object_read_metadata_route(
            self.operation_epoch(),
            route.pg_id,
            route.bucket,
            route.key,
        )?;
        let subject = object_read_route.load_object_read_auth_subject(route.version_id)?;
        Ok(action(&subject.stored))
    }

    pub fn load_existing_live_object(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Option<StoredObject>, ObjectPgActionError> {
        let object_pg_id = self.object_metadata_pg(bucket, key);
        let pg_id = object_pg_id.pg_id();
        let object_read_client = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?
            .object_read_metadata_client();
        let object_read_route = object_read_client.open_object_read_metadata_route(
            self.operation_epoch(),
            object_pg_id,
            bucket,
            key,
        )?;
        match object_read_route.load_object_read_auth_subject(None) {
            Ok(subject) => match subject.stored {
                StoredObject::Live(_) => Ok(Some(subject.stored)),
                StoredObject::DeleteMarker(_) => Ok(None),
            },
            Err(ObjectPgActionError::Metadata(MetadataError::ObjectNotFound)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub fn load_object_read_snapshot_if<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        snapshot_mode: ObjectReadSnapshotMode,
        action: impl FnMut(&StoredObject) -> Result<T, E>,
    ) -> Result<Result<ObjectReadSnapshotOutcome<T>, E>, ObjectPgActionError> {
        let route = super::ObjectReadMetadataRoute {
            bucket,
            key,
            version_id,
            snapshot_mode,
            pg_id: self.object_metadata_pg(bucket, key),
        };
        self.load_object_read_snapshot_if_on_route(&route, action, || Ok(()))
    }

    pub(super) fn load_object_read_snapshot_if_on_route<T, E>(
        &self,
        route: &super::ObjectReadMetadataRoute<'_>,
        mut action: impl FnMut(&StoredObject) -> Result<T, E>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<Result<ObjectReadSnapshotOutcome<T>, E>, ObjectPgActionError> {
        let pg_id = route.pg_id.pg_id();
        require_valid_route()?;
        let object_read_client = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?
            .object_read_metadata_client();
        let object_read_route = object_read_client.open_object_read_metadata_route(
            self.operation_epoch(),
            route.pg_id,
            route.bucket,
            route.key,
        )?;

        let mut work_budget =
            super::RequestWorkBudget::new(OBJECT_READ_SNAPSHOT_STALE_RETRY_BUDGET, None)
                .for_operation("load_object_read_snapshot")
                .for_pg(pg_id);
        loop {
            work_budget
                .check("load object read snapshot stale retry budget exhausted")
                .map_err(ObjectPgActionError::Store)?;
            require_valid_route()?;
            let subject = object_read_route.load_object_read_auth_subject(route.version_id)?;
            let value = match action(&subject.stored) {
                Ok(value) => value,
                Err(error) => return Ok(Err(error)),
            };
            require_valid_route()?;
            match object_read_route.load_object_read_snapshot_for_subject(
                route.version_id,
                &subject.identity,
                route.snapshot_mode,
            ) {
                Ok(snapshot) => return Ok(Ok(ObjectReadSnapshotOutcome { value, snapshot })),
                Err(ObjectPgActionError::StaleObjectReadSubject) => {
                    work_budget
                        .sleep_after_contention(
                            "load object read snapshot stale retry budget exhausted",
                        )
                        .map_err(ObjectPgActionError::Store)?;
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub fn load_leased_object_read_snapshot_if<T, E>(
        self: &Arc<Self>,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        snapshot_mode: ObjectReadSnapshotMode,
        action: impl FnMut(&StoredObject) -> Result<T, E>,
    ) -> Result<Result<super::LeasedObjectReadSnapshotOutcome<T>, E>, ObjectPgActionError> {
        let route = super::ObjectReadMetadataRoute {
            bucket,
            key,
            version_id,
            snapshot_mode,
            pg_id: self.object_metadata_pg(bucket, key),
        };
        self.load_leased_object_read_snapshot_if_on_route(&route, action, || Ok(()))
    }

    pub(super) fn load_leased_object_read_snapshot_if_on_route<T, E>(
        self: &Arc<Self>,
        route: &super::ObjectReadMetadataRoute<'_>,
        mut action: impl FnMut(&StoredObject) -> Result<T, E>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<Result<super::LeasedObjectReadSnapshotOutcome<T>, E>, ObjectPgActionError> {
        let object_pg_id = route.pg_id;
        let pg_id = object_pg_id.pg_id();
        require_valid_route()?;
        let object_read_client = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?
            .object_read_metadata_client();
        let object_read_route = object_read_client.open_object_read_metadata_route(
            self.operation_epoch(),
            object_pg_id,
            route.bucket,
            route.key,
        )?;

        let mut work_budget =
            super::RequestWorkBudget::new(OBJECT_READ_SNAPSHOT_STALE_RETRY_BUDGET, None)
                .for_operation("load_leased_object_read_snapshot")
                .for_pg(pg_id);
        loop {
            work_budget
                .check("load leased object read snapshot stale retry budget exhausted")
                .map_err(ObjectPgActionError::Store)?;
            require_valid_route()?;
            let subject = object_read_route.load_object_read_auth_subject(route.version_id)?;
            let value = match action(&subject.stored) {
                Ok(value) => value,
                Err(error) => return Ok(Err(error)),
            };
            let payload_lease = if let Some(live) = subject.stored.as_live() {
                require_valid_route()?;
                match self.acquire_object_payload_lease(route.bucket, route.key, live.generation_id)
                {
                    Ok(lease) => Some(lease),
                    Err(StoreError::NotFound) => {
                        work_budget
                            .sleep_after_contention(
                                "load leased object read snapshot stale retry budget exhausted",
                            )
                            .map_err(ObjectPgActionError::Store)?;
                        continue;
                    }
                    Err(error) => return Err(ObjectPgActionError::Store(error)),
                }
            } else {
                None
            };
            require_valid_route()?;
            match object_read_route.load_object_read_snapshot_for_subject(
                route.version_id,
                &subject.identity,
                route.snapshot_mode,
            ) {
                Ok(snapshot) => {
                    require_valid_route()?;
                    return Ok(Ok(super::LeasedObjectReadSnapshotOutcome {
                        value,
                        leased_snapshot: super::LeasedObjectReadSnapshot {
                            cluster: Arc::clone(self),
                            bucket: route.bucket.clone(),
                            key: route.key.clone(),
                            version_id: route.version_id,
                            snapshot_mode: route.snapshot_mode,
                            pg_id: route.pg_id,
                            snapshot: Arc::new(snapshot),
                            payload_lease,
                            repair_fence: None,
                        },
                    }));
                }
                Err(ObjectPgActionError::StaleObjectReadSubject) => {
                    work_budget
                        .sleep_after_contention(
                            "load leased object read snapshot stale retry budget exhausted",
                        )
                        .map_err(ObjectPgActionError::Store)?;
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub fn payload_reclaim_exists(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<bool, ObjectPgActionError> {
        let object_pg_id = self.object_metadata_pg(bucket, key);
        let pg_id = object_pg_id.pg_id();
        self.local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?
            .object_mutation_metadata_client()
            .payload_reclaim_exists(object_pg_id, bucket, key, generation_id)
    }

    pub fn get_object_tags_if<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        mut action: impl FnMut(&StoredObject) -> Result<VersionId, E>,
    ) -> Result<Result<Option<SerializedTagSet>, E>, ObjectPgActionError> {
        let object_pg_id = self.object_metadata_pg(bucket, key);
        let pg_id = object_pg_id.pg_id();
        let object_read_client = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?
            .object_read_metadata_client();
        let object_read_route = object_read_client.open_object_read_metadata_route(
            self.operation_epoch(),
            object_pg_id,
            bucket,
            key,
        )?;

        let mut work_budget =
            super::RequestWorkBudget::new(OBJECT_READ_SNAPSHOT_STALE_RETRY_BUDGET, None)
                .for_operation("get_object_tags")
                .for_pg(pg_id);
        loop {
            work_budget
                .check("get object tags stale retry budget exhausted")
                .map_err(ObjectPgActionError::Store)?;
            let subject = object_read_route.load_object_read_auth_subject(version_id)?;
            let authorized_version_id = match action(&subject.stored) {
                Ok(authorized_version_id) => authorized_version_id,
                Err(error) => return Ok(Err(error)),
            };
            match object_read_route.get_object_tags_for_subject(
                version_id,
                &subject.identity,
                authorized_version_id,
            ) {
                Ok(tags) => return Ok(Ok(tags)),
                Err(ObjectPgActionError::StaleObjectReadSubject) => {
                    work_budget
                        .sleep_after_contention("get object tags stale retry budget exhausted")
                        .map_err(ObjectPgActionError::Store)?;
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn put_object_metadata_command_from_stored(
        stored: &StoredObject,
        version_id: VersionId,
        mutation: PutObjectMetadataMutation,
        bucket_write_reservation: BucketWriteReservationProof,
    ) -> Result<PutObjectMetadataCommand, ObjectPgActionError> {
        if stored.version_id() != version_id {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: format!(
                    "object metadata action returned version {:?} for stored version {:?}",
                    version_id,
                    stored.version_id()
                ),
            });
        }
        let live = stored
            .as_live()
            .ok_or(MetadataError::MethodNotAllowedOnDeleteMarker)?;
        Ok(PutObjectMetadataCommand::from_live_object_and_mutation(
            live.clone(),
            mutation,
            bucket_write_reservation,
        ))
    }

    pub(super) fn put_object_metadata_if_with_route_validation<T, E>(
        &self,
        route: super::ObjectMetadataMutationEffectRoute<'_>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        mut action: impl FnMut(&StoredObject) -> Result<(T, VersionId, PutObjectMetadataMutation), E>,
    ) -> Result<Result<T, E>, ObjectPgActionError> {
        crate::metadata_command::metadata_command_publisher!(PutObjectMetadataIf);
        let super::ObjectMetadataMutationEffectRoute {
            pg_id: object_pg_id,
            bucket,
            key,
            requested_version_id,
            effect_fence,
        } = route;
        let pg_id = object_pg_id.pg_id();

        loop {
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            let storage_client = self
                .local_map
                .metadata_pg_primary_node(self.operation_epoch(), pg_id)?
                .object_mutation_metadata_client();
            let put_object_metadata_route = storage_client.open_put_object_metadata_route(
                self.operation_epoch(),
                object_pg_id,
                bucket,
                key,
            )?;
            if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                if let MetadataCommandPayload::PutObjectMetadata(update) = command.payload() {
                    if update.object.bucket == *bucket && update.object.key == *key {
                        let snapshot_version_id = match requested_version_id {
                            Some(version_id) => {
                                if version_id != update.object.version_id {
                                    self.drain_pending_object_metadata_command(pg_id, &command)?;
                                    continue;
                                }
                                Some(version_id)
                            }
                            None => None,
                        };
                        let stored = put_object_metadata_route
                            .load_put_object_metadata_snapshot(snapshot_version_id)?;
                        if stored.version_id() != update.object.version_id {
                            self.drain_pending_object_metadata_command(pg_id, &command)?;
                            continue;
                        }
                        let (value, version_id, mutation) = match action(&stored) {
                            Ok(command) => command,
                            Err(error) => return Ok(Err(error)),
                        };
                        let expected = Self::put_object_metadata_command_from_stored(
                            &stored,
                            version_id,
                            mutation,
                            update.bucket_write_reservation.clone(),
                        )?;
                        if update.as_ref() != &expected {
                            return Err(super::conflicting_pending_object_metadata_command(
                                "conflicting pending command for object metadata update",
                            ));
                        }
                        require_valid_route().map_err(ObjectPgActionError::Store)?;
                        self.apply_exact_pending_object_metadata_command(
                            pg_id,
                            super::ExactPendingObjectMetadataCommand::for_checked_request(&command),
                        )?;
                        return Ok(Ok(value));
                    }
                }

                self.drain_pending_object_metadata_command(pg_id, &command)?;
                continue;
            }

            require_valid_route().map_err(ObjectPgActionError::Store)?;
            let reservation = match self.acquire_durable_bucket_write_reservation_with_effect_fence(
                bucket,
                PUT_OBJECT_METADATA_BUCKET_WRITE_OPERATION_KIND,
                Some(key.as_str()),
                Some(effect_fence),
            ) {
                Ok(reservation) => reservation,
                Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteDraining)) => {
                    self.wait_for_durable_bucket_write_drain(bucket)
                        .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
                    continue;
                }
                Err(error) => {
                    return Err(super::bucket_snapshot_error_to_object_pg_action_error(
                        error,
                    ));
                }
            };
            let bucket_write_reservation = BucketWriteReservationProof::from(&reservation.record);
            macro_rules! release_bucket_write_proof {
                () => {{
                    self.release_durable_bucket_write_reservation(reservation)
                        .map_err(super::bucket_snapshot_error_to_object_pg_action_error)
                }};
            }

            if self
                .pending_metadata_command_for_bucket(pg_id, bucket)?
                .is_some()
            {
                release_bucket_write_proof!()?;
                continue;
            }

            if let Err(error) = require_valid_route() {
                release_bucket_write_proof!()?;
                return Err(ObjectPgActionError::Store(error));
            }
            let stored = match put_object_metadata_route
                .load_put_object_metadata_snapshot(requested_version_id)
            {
                Ok(stored) => stored,
                Err(error) => {
                    release_bucket_write_proof!()?;
                    return Err(error);
                }
            };
            let (value, version_id, mutation) = match action(&stored) {
                Ok(command) => command,
                Err(error) => {
                    release_bucket_write_proof!()?;
                    return Ok(Err(error));
                }
            };
            if let Err(error) = require_valid_route() {
                release_bucket_write_proof!()?;
                return Err(ObjectPgActionError::Store(error));
            }
            let command = match put_object_metadata_route.build_put_object_metadata_command(
                BuildPutObjectMetadataCommandReq {
                    requested_version_id,
                    expected_stored: &stored,
                    version_id,
                    mutation,
                    bucket_write_reservation: &bucket_write_reservation,
                },
            ) {
                Err(ObjectPgActionError::StaleObjectReadSubject) => {
                    release_bucket_write_proof!()?;
                    continue;
                }
                Ok(command) => command,
                Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                })) => {
                    release_bucket_write_proof!()?;
                    self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
                    continue;
                }
                Err(error) => {
                    release_bucket_write_proof!()?;
                    return Err(error);
                }
            };
            if let Err(error) = require_valid_route() {
                release_bucket_write_proof!()?;
                return Err(ObjectPgActionError::Store(error));
            }
            let install = match self.install_snapshot_sensitive_metadata_command_or_drain(
                pg_id,
                bucket,
                &command,
                Some(effect_fence),
            ) {
                Ok(install) => install,
                Err(error) => {
                    release_bucket_write_proof!()?;
                    return Err(error);
                }
            };
            match install {
                super::SnapshotSensitiveCommandInstall::Installed => {}
                super::SnapshotSensitiveCommandInstall::ContenderDrained => {
                    release_bucket_write_proof!()?;
                    continue;
                }
            }
            self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
            return Ok(Ok(value));
        }
    }

    #[cfg(test)]
    fn put_object_metadata_if<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        requested_version_id: Option<VersionId>,
        action: impl FnMut(&StoredObject) -> Result<(T, VersionId, PutObjectMetadataMutation), E>,
    ) -> Result<Result<T, E>, ObjectPgActionError> {
        self.put_object_metadata_if_with_route_validation(
            super::ObjectMetadataMutationEffectRoute {
                pg_id: self.object_metadata_pg(bucket, key),
                bucket,
                key,
                requested_version_id,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            || Ok(()),
            action,
        )
    }

    #[cfg(test)]
    pub fn put_object_tags_if<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        tags: &str,
        mut action: impl FnMut(&StoredObject) -> Result<VersionId, E>,
    ) -> Result<Result<VersionId, E>, ObjectPgActionError> {
        self.put_object_metadata_if(bucket, key, version_id, |stored| {
            let version_id = action(stored)?;
            Ok((
                version_id,
                version_id,
                PutObjectMetadataMutation::PutTags(crate::tests::object_tags(tags)),
            ))
        })
    }

    #[cfg(test)]
    pub fn delete_object_tags_if<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        mut action: impl FnMut(&StoredObject) -> Result<VersionId, E>,
    ) -> Result<Result<(), E>, ObjectPgActionError> {
        self.put_object_metadata_if(bucket, key, version_id, |stored| {
            let version_id = action(stored)?;
            Ok(((), version_id, PutObjectMetadataMutation::DeleteTags))
        })
    }

    /// Returns the version id the retention was applied to.
    #[cfg(test)]
    pub fn put_object_retention_if<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        retention: ObjectRetention,
        mut action: impl FnMut(&StoredObject) -> Result<VersionId, E>,
    ) -> Result<Result<VersionId, E>, ObjectPgActionError> {
        self.put_object_metadata_if(bucket, key, version_id, |stored| {
            let version_id = action(stored)?;
            Ok((
                version_id,
                version_id,
                PutObjectMetadataMutation::PutRetention(retention),
            ))
        })
    }

    /// Returns the version id the legal hold was applied to.
    #[cfg(test)]
    pub fn put_object_legal_hold_if<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        legal_hold: StoredLegalHoldStatus,
        mut action: impl FnMut(&StoredObject) -> Result<VersionId, E>,
    ) -> Result<Result<VersionId, E>, ObjectPgActionError> {
        self.put_object_metadata_if(bucket, key, version_id, |stored| {
            let version_id = action(stored)?;
            Ok((
                version_id,
                version_id,
                PutObjectMetadataMutation::PutLegalHold(legal_hold),
            ))
        })
    }

    #[cfg(test)]
    pub fn put_object_acl_if<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        mut action: impl FnMut(&StoredObject) -> Result<(VersionId, AclGrants, bool), E>,
    ) -> Result<Result<VersionId, E>, ObjectPgActionError> {
        self.put_object_metadata_if(bucket, key, version_id, |stored| {
            let (version_id, acl_grants, public_read) = action(stored)?;
            Ok((
                version_id,
                version_id,
                PutObjectMetadataMutation::PutAcl {
                    acl_grants,
                    public_read,
                },
            ))
        })
    }

    pub fn get_object_legal_hold_if<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        action: impl FnOnce(&StoredObject) -> Result<Option<LegalHoldStatus>, E>,
    ) -> Result<Result<Option<LegalHoldStatus>, E>, ObjectPgActionError> {
        let object_pg_id = self.object_metadata_pg(bucket, key);
        let pg_id = object_pg_id.pg_id();
        let object_read_client = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?
            .object_read_metadata_client();
        let object_read_route = object_read_client.open_object_read_metadata_route(
            self.operation_epoch(),
            object_pg_id,
            bucket,
            key,
        )?;
        let subject = object_read_route.load_object_read_auth_subject(version_id)?;
        Ok(action(&subject.stored))
    }

    pub fn get_object_retention_if<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        action: impl FnOnce(&StoredObject) -> Result<Option<ObjectRetention>, E>,
    ) -> Result<Result<Option<ObjectRetention>, E>, ObjectPgActionError> {
        let object_pg_id = self.object_metadata_pg(bucket, key);
        let pg_id = object_pg_id.pg_id();
        let object_read_client = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?
            .object_read_metadata_client();
        let object_read_route = object_read_client.open_object_read_metadata_route(
            self.operation_epoch(),
            object_pg_id,
            bucket,
            key,
        )?;
        let subject = object_read_route.load_object_read_auth_subject(version_id)?;
        Ok(action(&subject.stored))
    }

    fn load_bucket_lifecycle_context(
        &self,
        bucket: &BucketName,
        expected_bucket_incarnation_generation: u64,
    ) -> Result<Option<BucketLifecycleContext>, ObjectPgActionError> {
        crate::node::maybe_run_before_lifecycle_context_load_hook(bucket);
        let pg_id = PgId::new(self.bucket_metadata_pg_id(bucket));
        let bucket_store = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let bucket_info = match bucket_store
            .bucket_metadata_client()
            .head_bucket_info(self.validated_bucket_metadata_pg(pg_id), bucket)
        {
            Ok(info) => info,
            Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketNotFound { .. })) => {
                return Ok(None)
            }
            Err(error) => {
                return Err(super::bucket_snapshot_error_to_object_pg_action_error(
                    error,
                ))
            }
        };
        let bucket_incarnation_generation = bucket_info.bucket_incarnation_generation;
        if bucket_incarnation_generation != expected_bucket_incarnation_generation {
            return Ok(None);
        }
        let raw_lifecycle = if bucket_info.bucket_lifecycle_present {
            bucket_store
                .bucket_metadata_client()
                .get_bucket_subresource(
                    self.validated_bucket_metadata_pg(pg_id),
                    bucket,
                    BucketSubresourceKind::Lifecycle,
                )
                .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?
        } else {
            None
        };
        Ok(Some(BucketLifecycleContext {
            bucket_info,
            bucket_incarnation_generation,
            raw_lifecycle,
        }))
    }

    pub(super) fn apply_new_object_metadata_command_for_bucket(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), ObjectPgActionError> {
        let mut command = command.clone();
        let mut work_budget = super::RequestWorkBudget::new(
            std::time::Duration::from_millis(METADATA_COMMAND_APPLY_RETRY_BUDGET_MILLIS),
            None,
        )
        .for_operation("new_object_metadata_command_apply")
        .for_pg(pg_id);
        loop {
            work_budget.check("object metadata command apply retry budget exhausted")?;
            match self.apply_metadata_command_to_acting_set(&command) {
                Ok(()) => {
                    self.release_applied_metadata_command_bucket_write_reservations(&command)
                        .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
                    self.remove_pending_metadata_command_for_bucket(
                        pg_id,
                        command.bucket_name(),
                        &command,
                    )
                    .map_err(ObjectPgActionError::from)?;
                    self.after_object_metadata_command_applied(&command);
                    #[cfg(any(test, feature = "test-hooks"))]
                    crate::node::maybe_run_after_object_metadata_command_publish_hook(
                        self.metadata_primary_test_hook_node().test_hook_scope_id(),
                    )?;
                    return Ok(());
                }
                Err(error) => {
                    let MetadataCommandApplyFailure {
                        applied_nodes,
                        source,
                    } = error;
                    match self
                        .retryable_partial_exact_metadata_command_conflict_applied_on_all_nodes(
                            pg_id,
                            &command,
                            applied_nodes,
                            &source,
                        )
                        .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?
                    {
                        Some(true) => {
                            self.release_applied_metadata_command_bucket_write_reservations(
                                &command,
                            )
                            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
                            self.remove_pending_metadata_command_for_bucket(
                                pg_id,
                                command.bucket_name(),
                                &command,
                            )
                            .map_err(ObjectPgActionError::from)?;
                            self.after_object_metadata_command_applied(&command);
                            return Ok(());
                        }
                        Some(false) => {
                            return Err(super::conflicting_pending_object_metadata_command(
                                "retryable partial object metadata command conflict",
                            ));
                        }
                        None => {}
                    }
                    if applied_nodes == 0
                        && super::StorageCluster::metadata_command_log_conflict_matches(
                            &command, &source,
                        )
                    {
                        let Some(reissued) = self
                            .reissue_pending_metadata_command(pg_id, &command)
                            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?
                        else {
                            return Err(super::conflicting_pending_object_metadata_command(
                                "pending object metadata command was displaced during reissue",
                            ));
                        };
                        command = reissued;
                        continue;
                    }
                    if applied_nodes == 0
                        && super::StorageCluster::reserve_object_version_conflict_matches(
                            &command, &source,
                        )
                    {
                        self.record_abandoned_metadata_command_to_acting_set(&command)
                            .map_err(|error| {
                                super::bucket_snapshot_error_to_object_pg_action_error(error.source)
                            })?;
                        let pending = self.pending_metadata_command_for_bucket(pg_id, bucket)?;
                        if pending.as_ref() != Some(&command) {
                            return Err(super::conflicting_pending_object_metadata_command(
                                "pending object metadata command changed before stale version cleanup",
                            ));
                        }
                        self.release_metadata_command_bucket_write_reservation(&command)
                            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
                        self.remove_pending_metadata_command_for_bucket(pg_id, bucket, &command)
                            .map_err(ObjectPgActionError::from)?;
                        return Err(super::bucket_snapshot_error_to_object_pg_action_error(
                            source,
                        ));
                    }
                    if applied_nodes == 0 {
                        self.record_abandoned_metadata_command_to_acting_set(&command)
                            .map_err(|error| {
                                super::bucket_snapshot_error_to_object_pg_action_error(error.source)
                            })?;
                        let pending = self.pending_metadata_command_for_bucket(pg_id, bucket)?;
                        if pending.as_ref() != Some(&command) {
                            return Err(super::conflicting_pending_object_metadata_command(
                                "pending object metadata command changed before abandoned cleanup",
                            ));
                        }
                        self.release_metadata_command_bucket_write_reservation(&command)
                            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
                        self.remove_pending_metadata_command_for_bucket(pg_id, bucket, &command)
                            .map_err(ObjectPgActionError::from)?;
                    }
                    return Err(super::bucket_snapshot_error_to_object_pg_action_error(
                        source,
                    ));
                }
            }
        }
    }

    fn acquire_bucket_write_proof_for_object_metadata_command_with_effect_fence(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        operation_kind: &'static str,
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<Option<BucketWriteReservationProof>, ObjectPgActionError> {
        match self.acquire_durable_bucket_write_reservation_with_effect_fence(
            bucket,
            operation_kind,
            Some(key.as_str()),
            Some(effect_fence),
        ) {
            Ok(reservation) => Ok(Some(BucketWriteReservationProof::from(&reservation.record))),
            Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteDraining)) => {
                self.wait_for_durable_bucket_write_drain(bucket)
                    .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
                Ok(None)
            }
            Err(error) => Err(super::bucket_snapshot_error_to_object_pg_action_error(
                error,
            )),
        }
    }

    fn try_acquire_bucket_write_proof_for_object_metadata_command(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        operation_kind: &'static str,
        wait_for_drain: bool,
    ) -> Result<Option<BucketWriteReservationProof>, ObjectPgActionError> {
        match self.acquire_durable_bucket_write_reservation(
            bucket,
            operation_kind,
            Some(key.as_str()),
        ) {
            Ok(reservation) => Ok(Some(BucketWriteReservationProof::from(&reservation.record))),
            Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteDraining)) => {
                if wait_for_drain {
                    self.wait_for_durable_bucket_write_drain(bucket)
                        .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
                }
                Ok(None)
            }
            Err(error) => Err(super::bucket_snapshot_error_to_object_pg_action_error(
                error,
            )),
        }
    }

    fn try_acquire_lifecycle_bucket_write_proof_for_object_metadata_command(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        operation_kind: &'static str,
        expected_bucket_incarnation_generation: u64,
    ) -> Result<Option<BucketWriteReservationProof>, ObjectPgActionError> {
        crate::node::maybe_run_before_lifecycle_bucket_write_proof_acquire_hook(bucket);
        let Some(proof) = self.try_acquire_bucket_write_proof_for_object_metadata_command(
            bucket,
            key,
            operation_kind,
            false,
        )?
        else {
            return Ok(None);
        };
        if proof.bucket_incarnation_generation == expected_bucket_incarnation_generation {
            return Ok(Some(proof));
        }
        self.release_bucket_write_proof_for_object_metadata_command(&proof)?;
        Ok(None)
    }

    fn release_bucket_write_proof_for_object_metadata_command(
        &self,
        proof: &BucketWriteReservationProof,
    ) -> Result<(), ObjectPgActionError> {
        self.release_bucket_write_reservation_proof(proof)
            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)
    }

    fn deleted_specific_from_command_target(
        target: &DeleteObjectVersionTarget,
    ) -> DeletedSpecificObjectVersion {
        match target {
            DeleteObjectVersionTarget::DeleteMarker { .. } => {
                DeletedSpecificObjectVersion::DeleteMarker
            }
            DeleteObjectVersionTarget::Live {
                generation_id,
                layout,
                ..
            } => DeletedSpecificObjectVersion::Live {
                generation_id: *generation_id,
                layout: *layout,
            },
        }
    }

    fn deleted_current_from_command_target(
        target: &DeleteObjectVersionTarget,
    ) -> DeletedCurrentObject {
        match target {
            DeleteObjectVersionTarget::DeleteMarker { .. } => DeletedCurrentObject::DeleteMarker,
            DeleteObjectVersionTarget::Live {
                generation_id,
                layout,
                ..
            } => DeletedCurrentObject::Live {
                generation_id: *generation_id,
                layout: *layout,
            },
        }
    }

    pub(super) fn delete_specific_object_version_if_with_route_validation<T, E>(
        &self,
        route: super::ObjectMetadataMutationEffectRoute<'_>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        mut action: impl FnMut(Option<&StoredObject>) -> Result<T, E>,
    ) -> Result<Result<DeleteSpecificObjectVersionOutcome<T>, E>, ObjectPgActionError> {
        crate::metadata_command::metadata_command_publisher!(DeleteSpecificObjectVersionIf);
        let super::ObjectMetadataMutationEffectRoute {
            pg_id: object_pg_id,
            bucket,
            key,
            requested_version_id,
            effect_fence,
        } = route;
        let Some(version_id) = requested_version_id else {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "specific-version delete route is missing its version id".to_string(),
            });
        };
        let pg_id = object_pg_id.pg_id();

        loop {
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            let storage_client = self.object_delete_metadata_primary_route(bucket, key)?;
            if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                if let MetadataCommandPayload::DeleteObjectVersion(delete) = command.payload() {
                    if delete.matches_request(bucket, key, version_id) {
                        require_valid_route().map_err(ObjectPgActionError::Store)?;
                        let snapshot =
                            storage_client.load_specific_object_delete_snapshot(version_id)?;
                        let value = match action(snapshot.stored.as_ref()) {
                            Ok(value) => value,
                            Err(error) => return Ok(Err(error)),
                        };
                        require_valid_route().map_err(ObjectPgActionError::Store)?;
                        self.apply_exact_pending_object_metadata_command(
                            pg_id,
                            super::ExactPendingObjectMetadataCommand::for_checked_request(&command),
                        )?;
                        return Ok(Ok(DeleteSpecificObjectVersionOutcome {
                            value,
                            deleted: Self::deleted_specific_from_command_target(&delete.target),
                        }));
                    }
                }
                self.drain_pending_object_metadata_command(pg_id, &command)?;
                continue;
            }

            require_valid_route().map_err(ObjectPgActionError::Store)?;
            let bucket_write_reservation = match self
                .acquire_bucket_write_proof_for_object_metadata_command_with_effect_fence(
                    bucket,
                    key,
                    DELETE_OBJECT_VERSION_BUCKET_WRITE_OPERATION_KIND,
                    effect_fence,
                )? {
                Some(proof) => proof,
                None => continue,
            };
            if let Err(error) = require_valid_route() {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Err(ObjectPgActionError::Store(error));
            }
            let snapshot = match storage_client.load_specific_object_delete_snapshot(version_id) {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Err(error);
                }
            };
            let value = match action(snapshot.stored.as_ref()) {
                Ok(value) => value,
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Ok(Err(error));
                }
            };
            if snapshot.stored.is_none() {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Ok(Ok(DeleteSpecificObjectVersionOutcome {
                    value,
                    deleted: DeletedSpecificObjectVersion::Missing,
                }));
            }
            if let Err(error) = require_valid_route() {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Err(ObjectPgActionError::Store(error));
            }
            let command = storage_client.build_delete_specific_object_version_command(
                BuildDeleteSpecificObjectVersionCommandReq {
                    version_id,
                    expected_stored: snapshot.stored.as_ref(),
                    expected_target: snapshot.target.as_ref(),
                    expected_version_list: None,
                    bucket_write_reservation: &bucket_write_reservation,
                },
            );
            let command = match command {
                Ok(Some(command)) => command,
                Ok(None) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Ok(Ok(DeleteSpecificObjectVersionOutcome {
                        value,
                        deleted: DeletedSpecificObjectVersion::Missing,
                    }));
                }
                Err(ObjectPgActionError::StaleObjectReadSubject) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    continue;
                }
                Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                })) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
                    continue;
                }
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Err(error);
                }
            };
            if let Err(error) = require_valid_route() {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Err(ObjectPgActionError::Store(error));
            }
            let install = match self.install_snapshot_sensitive_metadata_command_or_drain(
                pg_id,
                bucket,
                &command,
                Some(effect_fence),
            ) {
                Ok(install) => install,
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Err(error);
                }
            };
            match install {
                super::SnapshotSensitiveCommandInstall::Installed => {}
                super::SnapshotSensitiveCommandInstall::ContenderDrained => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    continue;
                }
            }
            self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
            let MetadataCommandPayload::DeleteObjectVersion(delete) = command.payload() else {
                unreachable!("new delete object command changed payload kind");
            };
            return Ok(Ok(DeleteSpecificObjectVersionOutcome {
                value,
                deleted: Self::deleted_specific_from_command_target(&delete.target),
            }));
        }
    }

    pub(super) fn delete_current_object_if_with_route_validation<T, E>(
        &self,
        route: super::ObjectMetadataMutationEffectRoute<'_>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        mut action: impl FnMut(Option<&StoredObject>) -> Result<T, E>,
    ) -> Result<Result<DeleteCurrentObjectOutcome<T>, E>, ObjectPgActionError> {
        crate::metadata_command::metadata_command_publisher!(DeleteCurrentObjectIf);
        let super::ObjectMetadataMutationEffectRoute {
            pg_id: object_pg_id,
            bucket,
            key,
            requested_version_id,
            effect_fence,
        } = route;
        if requested_version_id.is_some() {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "current-object delete route unexpectedly contains a version id"
                    .to_string(),
            });
        }
        let pg_id = object_pg_id.pg_id();

        loop {
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            let storage_client = self.object_delete_metadata_primary_route(bucket, key)?;
            if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                if let MetadataCommandPayload::DeleteObjectVersion(delete) = command.payload() {
                    if delete.bucket == *bucket && delete.key == *key {
                        require_valid_route().map_err(ObjectPgActionError::Store)?;
                        let snapshot = storage_client.load_current_object_delete_snapshot()?;
                        if snapshot
                            .stored
                            .as_ref()
                            .is_some_and(|stored| stored.version_id() == delete.version_id)
                        {
                            let value = match action(snapshot.stored.as_ref()) {
                                Ok(value) => value,
                                Err(error) => return Ok(Err(error)),
                            };
                            require_valid_route().map_err(ObjectPgActionError::Store)?;
                            self.apply_exact_pending_object_metadata_command(
                                pg_id,
                                super::ExactPendingObjectMetadataCommand::for_checked_request(
                                    &command,
                                ),
                            )?;
                            return Ok(Ok(DeleteCurrentObjectOutcome {
                                value,
                                deleted: Self::deleted_current_from_command_target(&delete.target),
                            }));
                        }
                    }
                }
                self.drain_pending_object_metadata_command(pg_id, &command)?;
                continue;
            }

            require_valid_route().map_err(ObjectPgActionError::Store)?;
            let bucket_write_reservation = match self
                .acquire_bucket_write_proof_for_object_metadata_command_with_effect_fence(
                    bucket,
                    key,
                    DELETE_CURRENT_OBJECT_BUCKET_WRITE_OPERATION_KIND,
                    effect_fence,
                )? {
                Some(proof) => proof,
                None => continue,
            };
            if let Err(error) = self.maybe_run_after_object_metadata_reservation_acquired_hook() {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Err(error);
            }
            if let Err(error) = require_valid_route() {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Err(ObjectPgActionError::Store(error));
            }
            let snapshot = match storage_client.load_current_object_delete_snapshot() {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Err(error);
                }
            };
            let value = match action(snapshot.stored.as_ref()) {
                Ok(value) => value,
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Ok(Err(error));
                }
            };
            let Some(stored) = snapshot.stored.as_ref() else {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Ok(Ok(DeleteCurrentObjectOutcome {
                    value,
                    deleted: DeletedCurrentObject::Missing,
                }));
            };
            let StoredObject::Live(_) = stored else {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Ok(Ok(DeleteCurrentObjectOutcome {
                    value,
                    deleted: DeletedCurrentObject::DeleteMarker,
                }));
            };
            if let Err(error) = require_valid_route() {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Err(ObjectPgActionError::Store(error));
            }
            let command = storage_client.build_delete_current_object_command(
                BuildDeleteCurrentObjectCommandReq {
                    expected_current: Some(stored),
                    expected_target: snapshot.target.as_ref(),
                    bucket_write_reservation: &bucket_write_reservation,
                },
            );
            let command = match command {
                Ok(Some(command)) => command,
                Ok(None) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Ok(Ok(DeleteCurrentObjectOutcome {
                        value,
                        deleted: DeletedCurrentObject::Missing,
                    }));
                }
                Err(ObjectPgActionError::StaleObjectReadSubject) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    continue;
                }
                Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                })) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
                    continue;
                }
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Err(error);
                }
            };
            if let Err(error) = require_valid_route() {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Err(ObjectPgActionError::Store(error));
            }
            let install = match self.install_snapshot_sensitive_metadata_command_or_drain(
                pg_id,
                bucket,
                &command,
                Some(effect_fence),
            ) {
                Ok(install) => install,
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Err(error);
                }
            };
            match install {
                super::SnapshotSensitiveCommandInstall::Installed => {}
                super::SnapshotSensitiveCommandInstall::ContenderDrained => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    continue;
                }
            }
            self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
            let MetadataCommandPayload::DeleteObjectVersion(delete) = command.payload() else {
                unreachable!("new delete object command changed payload kind");
            };
            return Ok(Ok(DeleteCurrentObjectOutcome {
                value,
                deleted: Self::deleted_current_from_command_target(&delete.target),
            }));
        }
    }

    pub(super) fn insert_current_delete_marker_if_with_route_validation<T, E>(
        &self,
        route: super::ObjectMetadataMutationEffectRoute<'_>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        versioning: BucketVersioningState,
        owner: OwnerIdentity,
        mut action: impl FnMut(Option<&StoredObject>) -> Result<T, E>,
    ) -> Result<Result<InsertCurrentDeleteMarkerOutcome<T>, E>, ObjectPgActionError> {
        crate::metadata_command::metadata_command_publisher!(InsertCurrentDeleteMarkerIf);
        let super::ObjectMetadataMutationEffectRoute {
            pg_id: object_pg_id,
            bucket,
            key,
            requested_version_id,
            effect_fence,
        } = route;
        if requested_version_id.is_some() {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "delete-marker insertion route unexpectedly contains a version id"
                    .to_string(),
            });
        }
        let pg_id = object_pg_id.pg_id();

        loop {
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            let storage_client = self.object_delete_metadata_primary_route(bucket, key)?;
            if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                if let MetadataCommandPayload::InsertDeleteMarker(marker) = command.payload() {
                    if marker.matches_request(bucket, key) {
                        require_valid_route().map_err(ObjectPgActionError::Store)?;
                        let snapshot = storage_client.load_current_object_delete_snapshot()?;
                        let value = match action(snapshot.stored.as_ref()) {
                            Ok(value) => value,
                            Err(error) => return Ok(Err(error)),
                        };
                        require_valid_route().map_err(ObjectPgActionError::Store)?;
                        self.apply_exact_pending_object_metadata_command(
                            pg_id,
                            super::ExactPendingObjectMetadataCommand::for_checked_request(&command),
                        )?;
                        return Ok(Ok(InsertCurrentDeleteMarkerOutcome {
                            value,
                            version_id: marker.version_id,
                        }));
                    }
                }
                self.drain_pending_object_metadata_command(pg_id, &command)?;
                continue;
            }

            require_valid_route().map_err(ObjectPgActionError::Store)?;
            let bucket_write_reservation = match self
                .acquire_bucket_write_proof_for_object_metadata_command_with_effect_fence(
                    bucket,
                    key,
                    INSERT_DELETE_MARKER_BUCKET_WRITE_OPERATION_KIND,
                    effect_fence,
                )? {
                Some(proof) => proof,
                None => continue,
            };
            if let Err(error) = require_valid_route() {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Err(ObjectPgActionError::Store(error));
            }
            let snapshot = match storage_client.load_current_object_delete_snapshot() {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Err(error);
                }
            };
            let value = match action(snapshot.stored.as_ref()) {
                Ok(value) => value,
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Ok(Err(error));
                }
            };
            let null_snapshot = if versioning == BucketVersioningState::Suspended {
                if let Err(error) = require_valid_route() {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Err(ObjectPgActionError::Store(error));
                }
                match storage_client.load_specific_object_delete_snapshot(VersionId::Null) {
                    Ok(snapshot) => Some(snapshot),
                    Err(error) => {
                        self.release_bucket_write_proof_for_object_metadata_command(
                            &bucket_write_reservation,
                        )?;
                        return Err(error);
                    }
                }
            } else {
                None
            };
            let marker_vid = match versioning {
                BucketVersioningState::Enabled => {
                    match self.reserve_next_object_version_with_effect_fence(
                        pg_id,
                        bucket,
                        key,
                        effect_fence,
                        &mut require_valid_route,
                    ) {
                        Ok(marker_vid) => marker_vid,
                        Err(error) => {
                            self.release_bucket_write_proof_for_object_metadata_command(
                                &bucket_write_reservation,
                            )?;
                            return Err(error);
                        }
                    }
                }
                BucketVersioningState::Suspended => VersionId::Null,
                BucketVersioningState::Disabled => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Err(MetadataError::InvariantViolation {
                        context: "insert delete marker with disabled versioning",
                        reason: "delete markers require enabled or suspended versioning".into(),
                    }
                    .into());
                }
            };
            let stale_payload = if versioning == BucketVersioningState::Suspended {
                InsertDeleteMarkerStalePayload::SnapshotCurrentNullLive {
                    created_at: crate::clock::current_time_millis(),
                }
            } else {
                InsertDeleteMarkerStalePayload::Explicit(None)
            };
            let command = storage_client.build_insert_delete_marker_command(
                BuildInsertDeleteMarkerCommandReq {
                    version_id: marker_vid,
                    owner: &owner,
                    expected_current: snapshot.stored.as_ref(),
                    stale_payload,
                    expected_stale_payload_source: null_snapshot.as_ref().and_then(|snapshot| {
                        match snapshot.stored.as_ref() {
                            Some(stored @ StoredObject::Live(_)) => Some(stored),
                            Some(StoredObject::DeleteMarker(_)) | None => None,
                        }
                    }),
                    bucket_write_reservation: &bucket_write_reservation,
                },
            );
            let command = match command {
                Ok(command) => command,
                Err(ObjectPgActionError::StaleObjectReadSubject) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    continue;
                }
                Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                })) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
                    continue;
                }
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Err(error);
                }
            };
            if let Err(error) = require_valid_route() {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Err(ObjectPgActionError::Store(error));
            }
            let install = match self.install_snapshot_sensitive_metadata_command_or_drain(
                pg_id,
                bucket,
                &command,
                Some(effect_fence),
            ) {
                Ok(install) => install,
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Err(error);
                }
            };
            match install {
                super::SnapshotSensitiveCommandInstall::Installed => {}
                super::SnapshotSensitiveCommandInstall::ContenderDrained => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    continue;
                }
            }
            self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
            return Ok(Ok(InsertCurrentDeleteMarkerOutcome {
                value,
                version_id: marker_vid,
            }));
        }
    }

    #[cfg(test)]
    pub fn delete_specific_object_version_if<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        action: impl FnMut(Option<&StoredObject>) -> Result<T, E>,
    ) -> Result<Result<DeleteSpecificObjectVersionOutcome<T>, E>, ObjectPgActionError> {
        self.delete_specific_object_version_if_with_route_validation(
            super::ObjectMetadataMutationEffectRoute {
                pg_id: self.object_metadata_pg(bucket, key),
                bucket,
                key,
                requested_version_id: Some(version_id),
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            || Ok(()),
            action,
        )
    }

    #[cfg(test)]
    pub fn delete_current_object_if<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        action: impl FnMut(Option<&StoredObject>) -> Result<T, E>,
    ) -> Result<Result<DeleteCurrentObjectOutcome<T>, E>, ObjectPgActionError> {
        self.delete_current_object_if_with_route_validation(
            super::ObjectMetadataMutationEffectRoute {
                pg_id: self.object_metadata_pg(bucket, key),
                bucket,
                key,
                requested_version_id: None,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            || Ok(()),
            action,
        )
    }

    #[cfg(test)]
    pub fn insert_current_delete_marker_if<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        versioning: BucketVersioningState,
        owner: OwnerIdentity,
        action: impl FnMut(Option<&StoredObject>) -> Result<T, E>,
    ) -> Result<Result<InsertCurrentDeleteMarkerOutcome<T>, E>, ObjectPgActionError> {
        self.insert_current_delete_marker_if_with_route_validation(
            super::ObjectMetadataMutationEffectRoute {
                pg_id: self.object_metadata_pg(bucket, key),
                bucket,
                key,
                requested_version_id: None,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            || Ok(()),
            versioning,
            owner,
            action,
        )
    }

    pub fn expire_current_object_if_due<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        expected_version_id: VersionId,
        expected_bucket_incarnation_generation: u64,
        mut should_expire: impl FnMut(Option<&str>, &LiveObjectRecord) -> Result<bool, E>,
    ) -> Result<Result<Option<ExpireCurrentObjectOutcome>, E>, ObjectPgActionError> {
        crate::metadata_command::metadata_command_publisher!(ExpireCurrentObjectIfDue);
        let Some(lifecycle_context) =
            self.load_bucket_lifecycle_context(bucket, expected_bucket_incarnation_generation)?
        else {
            return Ok(Ok(None));
        };
        let BucketLifecycleContext {
            bucket_info,
            bucket_incarnation_generation,
            raw_lifecycle,
        } = lifecycle_context;
        if raw_lifecycle.is_none() {
            return Ok(Ok(None));
        }

        let object_pg_id = self.object_metadata_pg(bucket, key);
        let pg_id = object_pg_id.pg_id();
        let storage_client = self.object_delete_metadata_primary_route(bucket, key)?;
        let owner = OwnerIdentity::new(
            bucket_info.owner_principal.clone(),
            bucket_info.owner_canonical_id.clone(),
        );
        loop {
            if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                match command.payload() {
                    MetadataCommandPayload::DeleteObjectVersion(delete)
                        if delete.matches_request(bucket, key, expected_version_id) =>
                    {
                        if !metadata_command_matches_bucket_incarnation(
                            &command,
                            bucket_incarnation_generation,
                        ) {
                            return Ok(Ok(None));
                        }
                        let snapshot = storage_client
                            .load_specific_object_delete_snapshot(expected_version_id)?;
                        let Some(StoredObject::Live(record)) = snapshot.stored else {
                            self.apply_exact_pending_object_metadata_command(
                                pg_id,
                                super::ExactPendingObjectMetadataCommand::for_checked_request(
                                    &command,
                                ),
                            )?;
                            return Ok(Ok(None));
                        };
                        let due = match should_expire(raw_lifecycle.as_deref(), &record) {
                            Ok(due) => due,
                            Err(error) => return Ok(Err(error)),
                        };
                        self.apply_exact_pending_object_metadata_command(
                            pg_id,
                            super::ExactPendingObjectMetadataCommand::for_checked_request(&command),
                        )?;
                        return Ok(Ok(due.then_some(ExpireCurrentObjectOutcome {
                            reclaim_generation_id: super::delete_object_version_reclaim_generation(
                                &delete.target,
                            ),
                        })));
                    }
                    MetadataCommandPayload::InsertDeleteMarker(marker)
                        if marker.matches_request(bucket, key) =>
                    {
                        if !metadata_command_matches_bucket_incarnation(
                            &command,
                            bucket_incarnation_generation,
                        ) {
                            return Ok(Ok(None));
                        }
                        let snapshot = storage_client.load_current_object_delete_snapshot()?;
                        let Some(StoredObject::Live(record)) = snapshot.stored else {
                            self.apply_exact_pending_object_metadata_command(
                                pg_id,
                                super::ExactPendingObjectMetadataCommand::for_checked_request(
                                    &command,
                                ),
                            )?;
                            return Ok(Ok(None));
                        };
                        if record.version_id != expected_version_id {
                            self.apply_exact_pending_object_metadata_command(
                                pg_id,
                                super::ExactPendingObjectMetadataCommand::for_checked_request(
                                    &command,
                                ),
                            )?;
                            return Ok(Ok(None));
                        }
                        let due = match should_expire(raw_lifecycle.as_deref(), &record) {
                            Ok(due) => due,
                            Err(error) => return Ok(Err(error)),
                        };
                        let reclaim_generation_id =
                            super::object_payload_reclaim_generation(&marker.stale_payload);
                        self.apply_exact_pending_object_metadata_command(
                            pg_id,
                            super::ExactPendingObjectMetadataCommand::for_checked_request(&command),
                        )?;
                        return Ok(Ok(due.then_some(ExpireCurrentObjectOutcome {
                            reclaim_generation_id,
                        })));
                    }
                    _ => {
                        self.drain_pending_object_metadata_command(pg_id, &command)?;
                        continue;
                    }
                }
            }

            let bucket_write_reservation = match self
                .try_acquire_lifecycle_bucket_write_proof_for_object_metadata_command(
                    bucket,
                    key,
                    match bucket_info.versioning {
                        BucketVersioningState::Disabled => {
                            DELETE_CURRENT_OBJECT_BUCKET_WRITE_OPERATION_KIND
                        }
                        BucketVersioningState::Enabled | BucketVersioningState::Suspended => {
                            INSERT_DELETE_MARKER_BUCKET_WRITE_OPERATION_KIND
                        }
                    },
                    bucket_incarnation_generation,
                )? {
                Some(proof) => proof,
                None => return Ok(Ok(None)),
            };
            let snapshot = match storage_client.load_current_object_delete_snapshot() {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Err(error);
                }
            };
            let Some(StoredObject::Live(record)) = snapshot.stored.as_ref() else {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Ok(Ok(None));
            };
            if record.version_id != expected_version_id {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Ok(Ok(None));
            }
            let due = match should_expire(raw_lifecycle.as_deref(), record) {
                Ok(due) => due,
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Ok(Err(error));
                }
            };
            if !due {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Ok(Ok(None));
            }

            let command = match bucket_info.versioning {
                BucketVersioningState::Disabled => storage_client
                    .build_delete_current_object_command(BuildDeleteCurrentObjectCommandReq {
                        expected_current: snapshot.stored.as_ref(),
                        expected_target: snapshot.target.as_ref(),
                        bucket_write_reservation: &bucket_write_reservation,
                    })
                    .and_then(|command| command.ok_or(ObjectPgActionError::StaleObjectReadSubject)),
                BucketVersioningState::Enabled => {
                    let marker_vid = match self.reserve_next_object_version(pg_id, bucket, key) {
                        Ok(marker_vid) => marker_vid,
                        Err(error) => {
                            self.release_bucket_write_proof_for_object_metadata_command(
                                &bucket_write_reservation,
                            )?;
                            return Err(error);
                        }
                    };
                    storage_client.build_insert_delete_marker_command(
                        BuildInsertDeleteMarkerCommandReq {
                            expected_current: snapshot.stored.as_ref(),
                            version_id: marker_vid,
                            owner: &owner,
                            stale_payload: InsertDeleteMarkerStalePayload::Explicit(None),
                            expected_stale_payload_source: None,
                            bucket_write_reservation: &bucket_write_reservation,
                        },
                    )
                }
                BucketVersioningState::Suspended => {
                    match storage_client.load_specific_object_delete_snapshot(VersionId::Null) {
                        Ok(null_snapshot) => storage_client.build_insert_delete_marker_command(
                            BuildInsertDeleteMarkerCommandReq {
                                expected_current: snapshot.stored.as_ref(),
                                version_id: VersionId::Null,
                                owner: &owner,
                                stale_payload:
                                    InsertDeleteMarkerStalePayload::SnapshotCurrentNullLive {
                                        created_at: crate::clock::current_time_millis(),
                                    },
                                expected_stale_payload_source: null_snapshot.stored.as_ref(),
                                bucket_write_reservation: &bucket_write_reservation,
                            },
                        ),
                        Err(error) => Err(error),
                    }
                }
            };
            let command = match command {
                Ok(command) => command,
                Err(ObjectPgActionError::StaleObjectReadSubject) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Ok(Ok(None));
                }
                Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                })) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
                    continue;
                }
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Err(error);
                }
            };
            let reclaim_generation_id = match command.payload() {
                MetadataCommandPayload::DeleteObjectVersion(delete) => {
                    super::delete_object_version_reclaim_generation(&delete.target)
                }
                MetadataCommandPayload::InsertDeleteMarker(marker) => {
                    super::object_payload_reclaim_generation(&marker.stale_payload)
                }
                _ => unreachable!("lifecycle current expiry command changed payload kind"),
            };
            let install = match self
                .install_snapshot_sensitive_metadata_command_or_drain(pg_id, bucket, &command, None)
            {
                Ok(install) => install,
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Err(error);
                }
            };
            match install {
                super::SnapshotSensitiveCommandInstall::Installed => {}
                super::SnapshotSensitiveCommandInstall::ContenderDrained => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    continue;
                }
            }
            self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
            return Ok(Ok(Some(ExpireCurrentObjectOutcome {
                reclaim_generation_id,
            })));
        }
    }

    pub fn delete_noncurrent_live_versions_if_due<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        expected_bucket_incarnation_generation: u64,
        mut select_versions: impl FnMut(Option<&str>, &[StoredObject]) -> Result<HashSet<VersionId>, E>,
    ) -> Result<Result<Vec<GenerationId>, E>, ObjectPgActionError> {
        crate::metadata_command::metadata_command_publisher!(DeleteNoncurrentLiveVersionsIfDue);
        let Some(lifecycle_context) =
            self.load_bucket_lifecycle_context(bucket, expected_bucket_incarnation_generation)?
        else {
            return Ok(Ok(Vec::new()));
        };
        let BucketLifecycleContext {
            bucket_info: _bucket_info,
            bucket_incarnation_generation,
            raw_lifecycle,
        } = lifecycle_context;
        if raw_lifecycle.is_none() {
            return Ok(Ok(Vec::new()));
        }

        let object_pg_id = self.object_metadata_pg(bucket, key);
        let pg_id = object_pg_id.pg_id();
        let storage_client = self.object_delete_metadata_primary_route(bucket, key)?;
        let mut completed_reclaimed_generation_ids = Vec::new();

        'retry: loop {
            if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                if let MetadataCommandPayload::DeleteObjectVersion(delete) = command.payload() {
                    if delete.bucket == *bucket && delete.key == *key {
                        if !metadata_command_matches_bucket_incarnation(
                            &command,
                            bucket_incarnation_generation,
                        ) {
                            return Ok(Ok(completed_reclaimed_generation_ids));
                        }
                        let versions = storage_client.list_object_versions_for_lifecycle()?;
                        if versions.is_empty() {
                            self.apply_exact_pending_object_metadata_command(
                                pg_id,
                                super::ExactPendingObjectMetadataCommand::for_checked_request(
                                    &command,
                                ),
                            )?;
                            return Ok(Ok(Vec::new()));
                        }
                        let due_version_ids =
                            match select_versions(raw_lifecycle.as_deref(), &versions) {
                                Ok(version_ids) => version_ids,
                                Err(error) => return Ok(Err(error)),
                            };
                        let reclaim_generation_id = due_version_ids
                            .contains(&delete.version_id)
                            .then(|| {
                                super::delete_object_version_reclaim_generation(&delete.target)
                            })
                            .flatten();
                        self.apply_exact_pending_object_metadata_command(
                            pg_id,
                            super::ExactPendingObjectMetadataCommand::for_checked_request(&command),
                        )?;
                        return Ok(Ok(reclaim_generation_id.into_iter().collect()));
                    }
                }
                self.drain_pending_object_metadata_command(pg_id, &command)?;
                continue;
            }

            let versions = storage_client.list_object_versions_for_lifecycle()?;
            if versions.is_empty() {
                return Ok(Ok(Vec::new()));
            }
            let due_version_ids = match select_versions(raw_lifecycle.as_deref(), &versions) {
                Ok(version_ids) => version_ids,
                Err(error) => return Ok(Err(error)),
            };
            if due_version_ids.is_empty() {
                return Ok(Ok(completed_reclaimed_generation_ids));
            }

            let mut delete_targets = Vec::new();
            for stored in &versions {
                let Some(record) = stored.as_live() else {
                    continue;
                };
                if !due_version_ids.contains(&record.version_id) {
                    continue;
                }
                delete_targets.push(stored.clone());
            }

            for stored in delete_targets {
                let version_id = stored.version_id();
                let snapshot = match storage_client.load_specific_object_delete_snapshot(version_id)
                {
                    Ok(snapshot) if snapshot.stored.as_ref() == Some(&stored) => snapshot,
                    Ok(_) | Err(ObjectPgActionError::StaleObjectReadSubject) => {
                        return Ok(Ok(completed_reclaimed_generation_ids));
                    }
                    Err(error) => return Err(error),
                };
                let bucket_write_reservation = match self
                    .try_acquire_lifecycle_bucket_write_proof_for_object_metadata_command(
                        bucket,
                        key,
                        DELETE_OBJECT_VERSION_BUCKET_WRITE_OPERATION_KIND,
                        bucket_incarnation_generation,
                    )? {
                    Some(proof) => proof,
                    None => return Ok(Ok(completed_reclaimed_generation_ids)),
                };
                let command = storage_client.build_delete_specific_object_version_command(
                    BuildDeleteSpecificObjectVersionCommandReq {
                        version_id,
                        expected_stored: snapshot.stored.as_ref(),
                        expected_target: snapshot.target.as_ref(),
                        expected_version_list: Some(&versions),
                        bucket_write_reservation: &bucket_write_reservation,
                    },
                );
                let command = match command {
                    Ok(Some(command)) => command,
                    Ok(None) | Err(ObjectPgActionError::StaleObjectReadSubject) => {
                        self.release_bucket_write_proof_for_object_metadata_command(
                            &bucket_write_reservation,
                        )?;
                        return Ok(Ok(completed_reclaimed_generation_ids));
                    }
                    Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                        ..
                    })) => {
                        self.release_bucket_write_proof_for_object_metadata_command(
                            &bucket_write_reservation,
                        )?;
                        self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
                        continue 'retry;
                    }
                    Err(error) => {
                        self.release_bucket_write_proof_for_object_metadata_command(
                            &bucket_write_reservation,
                        )?;
                        return Err(error);
                    }
                };
                let install = match self.install_snapshot_sensitive_metadata_command_or_drain(
                    pg_id, bucket, &command, None,
                ) {
                    Ok(install) => install,
                    Err(error) => {
                        self.release_bucket_write_proof_for_object_metadata_command(
                            &bucket_write_reservation,
                        )?;
                        return Err(error);
                    }
                };
                match install {
                    super::SnapshotSensitiveCommandInstall::Installed => {}
                    super::SnapshotSensitiveCommandInstall::ContenderDrained => {
                        self.release_bucket_write_proof_for_object_metadata_command(
                            &bucket_write_reservation,
                        )?;
                        continue 'retry;
                    }
                }
                self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
                if let MetadataCommandPayload::DeleteObjectVersion(delete) = command.payload() {
                    if let Some(generation_id) =
                        super::delete_object_version_reclaim_generation(&delete.target)
                    {
                        completed_reclaimed_generation_ids.push(generation_id);
                    }
                }
            }
            return Ok(Ok(completed_reclaimed_generation_ids));
        }
    }

    pub fn delete_expired_delete_marker_if_due<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        expected_version_id: VersionId,
        expected_bucket_incarnation_generation: u64,
        mut should_delete: impl FnMut(Option<&str>, &[StoredObject]) -> Result<bool, E>,
    ) -> Result<Result<bool, E>, ObjectPgActionError> {
        crate::metadata_command::metadata_command_publisher!(DeleteExpiredDeleteMarkerIfDue);
        let Some(lifecycle_context) =
            self.load_bucket_lifecycle_context(bucket, expected_bucket_incarnation_generation)?
        else {
            return Ok(Ok(false));
        };
        let BucketLifecycleContext {
            bucket_info: _bucket_info,
            bucket_incarnation_generation,
            raw_lifecycle,
        } = lifecycle_context;
        if raw_lifecycle.is_none() {
            return Ok(Ok(false));
        }

        let object_pg_id = self.object_metadata_pg(bucket, key);
        let pg_id = object_pg_id.pg_id();
        let storage_client = self.object_delete_metadata_primary_route(bucket, key)?;

        loop {
            if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                if let MetadataCommandPayload::DeleteObjectVersion(delete) = command.payload() {
                    if delete.matches_request(bucket, key, expected_version_id)
                        && matches!(
                            delete.target,
                            DeleteObjectVersionTarget::DeleteMarker { .. }
                        )
                    {
                        if !metadata_command_matches_bucket_incarnation(
                            &command,
                            bucket_incarnation_generation,
                        ) {
                            return Ok(Ok(false));
                        }
                        let versions = storage_client.list_object_versions_for_lifecycle()?;
                        if versions.is_empty() {
                            self.apply_exact_pending_object_metadata_command(
                                pg_id,
                                super::ExactPendingObjectMetadataCommand::for_checked_request(
                                    &command,
                                ),
                            )?;
                            return Ok(Ok(false));
                        }
                        let due = match should_delete(raw_lifecycle.as_deref(), &versions) {
                            Ok(due) => due,
                            Err(error) => return Ok(Err(error)),
                        };
                        self.apply_exact_pending_object_metadata_command(
                            pg_id,
                            super::ExactPendingObjectMetadataCommand::for_checked_request(&command),
                        )?;
                        return Ok(Ok(due));
                    }
                }
                self.drain_pending_object_metadata_command(pg_id, &command)?;
                continue;
            }

            let bucket_write_reservation = match self
                .try_acquire_lifecycle_bucket_write_proof_for_object_metadata_command(
                    bucket,
                    key,
                    DELETE_OBJECT_VERSION_BUCKET_WRITE_OPERATION_KIND,
                    bucket_incarnation_generation,
                )? {
                Some(proof) => proof,
                None => return Ok(Ok(false)),
            };
            let versions = match storage_client.list_object_versions_for_lifecycle() {
                Ok(versions) => versions,
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Err(error);
                }
            };
            if versions.is_empty() {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Ok(Ok(false));
            }
            let due = match should_delete(raw_lifecycle.as_deref(), &versions) {
                Ok(due) => due,
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Ok(Err(error));
                }
            };
            if !due {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Ok(Ok(false));
            }
            let Some(expected_marker) = versions
                .iter()
                .find(|stored| stored.version_id() == expected_version_id)
                .filter(|stored| matches!(stored, StoredObject::DeleteMarker(_)))
            else {
                self.release_bucket_write_proof_for_object_metadata_command(
                    &bucket_write_reservation,
                )?;
                return Ok(Ok(false));
            };
            let snapshot =
                match storage_client.load_specific_object_delete_snapshot(expected_version_id) {
                    Ok(snapshot) if snapshot.stored.as_ref() == Some(expected_marker) => snapshot,
                    Ok(_) | Err(ObjectPgActionError::StaleObjectReadSubject) => {
                        self.release_bucket_write_proof_for_object_metadata_command(
                            &bucket_write_reservation,
                        )?;
                        return Ok(Ok(false));
                    }
                    Err(error) => {
                        self.release_bucket_write_proof_for_object_metadata_command(
                            &bucket_write_reservation,
                        )?;
                        return Err(error);
                    }
                };
            let command = storage_client.build_delete_specific_object_version_command(
                BuildDeleteSpecificObjectVersionCommandReq {
                    version_id: expected_version_id,
                    expected_stored: snapshot.stored.as_ref(),
                    expected_target: snapshot.target.as_ref(),
                    expected_version_list: Some(&versions),
                    bucket_write_reservation: &bucket_write_reservation,
                },
            );
            let command = match command {
                Ok(Some(command)) => command,
                Ok(None) | Err(ObjectPgActionError::StaleObjectReadSubject) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Ok(Ok(false));
                }
                Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                })) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
                    continue;
                }
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Err(error);
                }
            };
            let install = match self
                .install_snapshot_sensitive_metadata_command_or_drain(pg_id, bucket, &command, None)
            {
                Ok(install) => install,
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    return Err(error);
                }
            };
            match install {
                super::SnapshotSensitiveCommandInstall::Installed => {}
                super::SnapshotSensitiveCommandInstall::ContenderDrained => {
                    self.release_bucket_write_proof_for_object_metadata_command(
                        &bucket_write_reservation,
                    )?;
                    continue;
                }
            }
            self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
            return Ok(Ok(true));
        }
    }

    pub fn acquire_object_payload_lease(
        self: &std::sync::Arc<Self>,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<ObjectPayloadLease, StoreError> {
        let runtime_state = self.ensure_object_payload_lease_allowed(bucket, key, generation_id)?;
        let node_leases =
            self.local_map
                .try_acquire_object_payload_lease(bucket, key, generation_id)?;
        if node_leases.is_empty() {
            return Err(StoreError::NotFound);
        }
        Ok(ObjectPayloadLease::new(
            std::sync::Arc::downgrade(self),
            node_leases,
            runtime_state,
            bucket.clone(),
            key.clone(),
            generation_id,
            self.object_metadata_pg_id(bucket, key),
        ))
    }

    pub fn acquire_object_payload_lease_for_shard_locations(
        self: &std::sync::Arc<Self>,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        locations: &[super::ShardLocation],
    ) -> Result<ObjectPayloadLease, StoreError> {
        let runtime_state = self.ensure_object_payload_lease_allowed(bucket, key, generation_id)?;
        let node_leases = self
            .local_map
            .try_acquire_object_payload_lease_on_locations(bucket, key, generation_id, locations)?;
        if !locations.is_empty() && node_leases.is_empty() {
            return Err(StoreError::NotFound);
        }
        Ok(ObjectPayloadLease::new(
            std::sync::Arc::downgrade(self),
            node_leases,
            runtime_state,
            bucket.clone(),
            key.clone(),
            generation_id,
            self.object_metadata_pg_id(bucket, key),
        ))
    }

    fn ensure_object_payload_lease_allowed(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<std::sync::Arc<LocalClusterRuntimeState>, StoreError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        self.local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let runtime_state = self.local_map.runtime_state();
        if self
            .pending_metadata_command_for_bucket(pg_id, bucket)?
            .is_some_and(|command| {
                matches!(
                    command.payload(),
                    MetadataCommandPayload::DeleteObjectPayloadReclaim(delete)
                        if delete.matches_request(bucket, key, generation_id)
                )
            })
        {
            return Err(StoreError::NotFound);
        }
        Ok(runtime_state)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn object_payload_lease_count(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> usize {
        if self.operation_epoch() != self.cluster_epoch() {
            return 0;
        }
        self.local_map
            .object_payload_lease_count(bucket, key, generation_id)
            .expect("test payload lease count should be readable")
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn object_payload_lease_holder_node_count(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> usize {
        if self.operation_epoch() != self.cluster_epoch() {
            return 0;
        }
        self.local_map
            .object_payload_lease_holder_node_count(bucket, key, generation_id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn bucket_object_payload_lease_count(&self, bucket: &BucketName) -> usize {
        if self.operation_epoch() != self.cluster_epoch() {
            return 0;
        }
        self.local_map.bucket_object_payload_lease_count(bucket)
    }

    pub fn enqueue_object_payload_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) {
        let _ = self.enqueue_object_payload_reclaim_for_pg(bucket, key, generation_id);
    }

    fn enqueue_object_payload_reclaim_for_pg(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Option<ReclaimQueueInsert> {
        if self.operation_epoch() != self.cluster_epoch() {
            return None;
        }
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        let outcome = self
            .local_map
            .runtime_state()
            .enqueue_object_payload_reclaim(bucket, key, generation_id, pg_id.get());
        let _ = observability::emit_object_payload_reclaim_event(
            super::TRACE_TARGET,
            observability::ObjectPayloadReclaimEventSummary {
                pg_id: pg_id.get(),
                event: match outcome {
                    ReclaimQueueInsert::Queued => "queued",
                    ReclaimQueueInsert::Deduplicated => "deduplicated",
                    ReclaimQueueInsert::PgCapacityDeferred => "pg_capacity_deferred",
                },
            },
        );
        Some(outcome)
    }

    pub(crate) fn finish_object_payload_reclaim_work(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) {
        if self.operation_epoch() != self.cluster_epoch() {
            return;
        }
        self.local_map
            .runtime_state()
            .finish_object_payload_reclaim_work(bucket, key, generation_id);
    }

    pub(crate) fn enqueue_bucket_delete_finalize(&self, root: BucketDeleteFinalizeRoot) {
        if self.operation_epoch() != self.cluster_epoch() {
            return;
        }
        let _ = self
            .local_map
            .runtime_state()
            .enqueue_bucket_delete_finalize(root);
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_enqueue_bucket_delete_finalize(&self, root: &crate::TestBucketDeleteFinalizeRoot) {
        self.enqueue_bucket_delete_finalize(root.into());
    }

    pub(crate) fn enqueue_bucket_delete_begin(
        &self,
        bucket: &BucketName,
        bucket_execution_generation: u64,
        bucket_incarnation_generation: u64,
    ) {
        if self.operation_epoch() != self.cluster_epoch() {
            return;
        }
        let root = crate::BucketDeleteBeginRoot {
            bucket: bucket.clone(),
            bucket_execution_generation,
            bucket_incarnation_generation,
        };
        let _ = self
            .local_map
            .runtime_state()
            .enqueue_bucket_delete_begin(root);
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_enqueue_bucket_delete_begin(
        &self,
        bucket: &BucketName,
        bucket_execution_generation: u64,
        bucket_incarnation_generation: u64,
    ) {
        self.enqueue_bucket_delete_begin(
            bucket,
            bucket_execution_generation,
            bucket_incarnation_generation,
        );
    }

    pub(crate) fn finish_bucket_delete_finalize_work(&self, root: &BucketDeleteFinalizeRoot) {
        self.local_map
            .runtime_state()
            .finish_bucket_delete_finalize_work(root);
    }

    pub(crate) fn try_take_reclaim_work(&self) -> Option<ReclaimWorkItem> {
        if self.operation_epoch() != self.cluster_epoch() {
            return None;
        }
        self.local_map.runtime_state().try_take_reclaim_work()
    }

    pub(crate) fn enqueue_durable_reclaim_work_batch_excluding(
        &self,
        next_pg_id: Option<u32>,
        max_pgs: usize,
        excluded_object_payload_roots: &HashSet<(BucketName, ObjectKey, GenerationId)>,
        excluded_bucket_delete_begin_roots: &HashSet<BucketDeleteBeginRoot>,
        excluded_bucket_delete_finalize_roots: &HashSet<BucketName>,
    ) -> DurableReclaimScanBatch {
        if self.operation_epoch() != self.cluster_epoch()
            || self.require_route_map_valid_now().is_err()
        {
            return DurableReclaimScanBatch {
                outcome: DurableReclaimScanOutcome::RouteRefreshRequired,
                next_pg_id,
                scanned_pgs: 0,
                retry_pass_required: false,
            };
        }

        let pg_ids = self.metadata_pg_ids();
        let window = bounded_pg_scan_window(&pg_ids, next_pg_id, max_pgs);
        let mut scanned_pgs = 0usize;
        let mut retry_pass_required = false;
        for &raw_pg_id in &pg_ids[window.start..window.end] {
            let pg_id = PgId::new(raw_pg_id);
            let object_payload = self.enqueue_durable_object_payload_reclaim_root_for_pg_excluding(
                pg_id,
                excluded_object_payload_roots,
            );
            if object_payload.route_refresh_required {
                return DurableReclaimScanBatch {
                    outcome: DurableReclaimScanOutcome::RouteRefreshRequired,
                    next_pg_id: Some(raw_pg_id),
                    scanned_pgs,
                    retry_pass_required,
                };
            }
            retry_pass_required |= object_payload.retry_required;
            let bucket_begin = self.enqueue_durable_bucket_delete_begin_roots_for_pg_excluding(
                pg_id,
                excluded_bucket_delete_begin_roots,
            );
            if bucket_begin.route_refresh_required {
                return DurableReclaimScanBatch {
                    outcome: DurableReclaimScanOutcome::RouteRefreshRequired,
                    next_pg_id: Some(raw_pg_id),
                    scanned_pgs,
                    retry_pass_required,
                };
            }
            retry_pass_required |= bucket_begin.retry_required;
            let bucket_finalize = self
                .enqueue_durable_bucket_delete_finalize_roots_for_pg_excluding(
                    pg_id,
                    excluded_bucket_delete_finalize_roots,
                );
            if bucket_finalize.route_refresh_required {
                return DurableReclaimScanBatch {
                    outcome: DurableReclaimScanOutcome::RouteRefreshRequired,
                    next_pg_id: Some(raw_pg_id),
                    scanned_pgs,
                    retry_pass_required,
                };
            }
            retry_pass_required |= bucket_finalize.retry_required;
            scanned_pgs += 1;
        }

        DurableReclaimScanBatch {
            outcome: DurableReclaimScanOutcome::Complete,
            next_pg_id: window.next_pg_id,
            scanned_pgs,
            retry_pass_required,
        }
    }

    /// Poll for work already present in the process-local reclaim queue.
    ///
    /// Durable discovery is owned by the caller's explicit scan cadence and is
    /// never performed by this queue wait.
    pub(crate) fn wait_for_queued_reclaim_work_poll(
        &self,
        stop: &AtomicBool,
    ) -> Option<ReclaimWorkItem> {
        if self.operation_epoch() != self.cluster_epoch() {
            return None;
        }
        self.local_map
            .runtime_state()
            .wait_for_reclaim_work_poll(stop)
    }

    pub(crate) fn wake_reclaim_workers(&self) {
        if self.operation_epoch() != self.cluster_epoch() {
            return;
        }
        self.local_map.runtime_state().wake_reclaim_workers();
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_try_take_reclaim_work(&self) -> Option<crate::TestReclaimWorkItem> {
        self.try_take_reclaim_work().map(Into::into)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_finish_object_payload_reclaim_work(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) {
        self.finish_object_payload_reclaim_work(bucket, key, generation_id);
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_wake_reclaim_worker(&self) {
        self.wake_reclaim_workers();
    }

    pub(crate) fn reclaim_object_payload_if_unleased(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<bool, ObjectPgActionError> {
        Ok(matches!(
            self.reclaim_object_payload_if_unleased_with_outcome(bucket, key, generation_id)?,
            super::ObjectPayloadReclaimAttempt::Completed
        ))
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_reclaim_object_payload_if_unleased(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<bool, ObjectPgActionError> {
        self.reclaim_object_payload_if_unleased(bucket, key, generation_id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_reclaim_object_payload_if_unleased_with_outcome(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<super::TestObjectPayloadReclaimAttempt, ObjectPgActionError> {
        self.reclaim_object_payload_if_unleased_with_outcome(bucket, key, generation_id)
            .map(Into::into)
    }

    pub(crate) fn reclaim_object_payload_if_unleased_with_outcome(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<super::ObjectPayloadReclaimAttempt, ObjectPgActionError> {
        crate::metadata_command::metadata_command_publisher!(ReclaimObjectPayloadIfUnleased);
        let object_pg_id = self.object_metadata_pg(bucket, key);
        let pg_id = object_pg_id.pg_id();
        let emit_outcome = |outcome: &'static str| {
            let _ = observability::emit_object_payload_reclaim_event(
                super::TRACE_TARGET,
                observability::ObjectPayloadReclaimEventSummary {
                    pg_id: pg_id.get(),
                    event: outcome,
                },
            );
        };
        emit_outcome("started");
        let mutation_client = self.object_mutation_metadata_primary_client(bucket, key)?;
        let retained_mutation_client =
            self.retained_object_mutation_metadata_primary_client(bucket, key)?;
        if self
            .local_map
            .object_payload_lease_count(bucket, key, generation_id)?
            != 0
        {
            emit_outcome("deferred_lease");
            return Ok(super::ObjectPayloadReclaimAttempt::Deferred);
        }

        while let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
            let matching_reclaim_delete = matches!(
                command.payload(),
                MetadataCommandPayload::DeleteObjectPayloadReclaim(delete)
                    if delete.matches_request(bucket, key, generation_id)
            );
            if matching_reclaim_delete {
                let reclaim_authority = match command.payload() {
                    MetadataCommandPayload::DeleteObjectPayloadReclaim(delete) => {
                        &delete.reclaim_claim
                    }
                    _ => unreachable!("matching payload reclaim command was already selected"),
                };
                match self.finish_exact_pending_object_metadata_command(
                    pg_id,
                    super::ExactPendingObjectMetadataCommand::for_checked_request(&command),
                )? {
                    super::PendingMetadataCommandOutcome::Applied => {
                        self.local_map.clear_object_payload_reclaim_fence(
                            bucket,
                            key,
                            generation_id,
                            reclaim_authority,
                        )?;
                        emit_outcome("completed_existing_pending");
                        return Ok(super::ObjectPayloadReclaimAttempt::Completed);
                    }
                    super::PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                        emit_outcome("error");
                        return Err(super::conflicting_pending_object_metadata_command(
                            "retryable partial pending payload reclaim command",
                        ));
                    }
                    super::PendingMetadataCommandOutcome::Abandoned => continue,
                }
            }
            self.emit_pending_slot_action_for_command(pg_id, &command, "reclaim_defer");
            emit_outcome("deferred_pending_command");
            return Ok(super::ObjectPayloadReclaimAttempt::Deferred);
        }

        let reclaim = {
            if self
                .local_map
                .object_payload_lease_count(bucket, key, generation_id)?
                != 0
            {
                emit_outcome("deferred_lease");
                return Ok(super::ObjectPayloadReclaimAttempt::Deferred);
            }

            mutation_client
                .get_object_payload_reclaim(object_pg_id, bucket, key, generation_id)
                .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?
        };

        let Some(reclaim) = reclaim else {
            emit_outcome("missing_root");
            return Ok(super::ObjectPayloadReclaimAttempt::MissingRoot);
        };

        let bucket_incarnation_generation = {
            let bucket_pg_id = PgId::new(self.bucket_metadata_pg_id(bucket));
            let bucket_store = self
                .local_map
                .metadata_pg_primary_node(self.operation_epoch(), bucket_pg_id)?;
            match bucket_store
                .bucket_metadata_client()
                .head_bucket_raw(self.validated_bucket_metadata_pg(bucket_pg_id), bucket)
                .map_err(super::bucket_snapshot_error_to_object_pg_action_error)
            {
                Ok(bucket) => bucket.bucket_incarnation_generation,
                Err(ObjectPgActionError::Metadata(MetadataError::BucketNotFound { .. })) => {
                    ORPHAN_OBJECT_PAYLOAD_RECLAIM_BUCKET_INCARNATION
                }
                Err(error) => return Err(error),
            }
        };
        let reclaim_kind = reclaim.kind();
        let claim_id = self.next_object_payload_reclaim_claim_id()?;
        let owner_token = self.bucket_write_owner_token();
        let claimed_at = crate::clock::current_time_millis();
        let claim = mutation_client
            .acquire_object_payload_reclaim_claim(
                object_pg_id,
                bucket,
                bucket_incarnation_generation,
                key,
                generation_id,
                reclaim_kind,
                &claim_id,
                &owner_token,
                self.operation_epoch(),
                claimed_at,
                claimed_at.checked_add(60_000),
                claimed_at,
            )
            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
        let Some(claim) = claim else {
            emit_outcome("deferred_claim_busy");
            return Ok(super::ObjectPayloadReclaimAttempt::Deferred);
        };
        let reclaim_authority = ObjectPayloadReclaimClaimProof::from(&claim);

        let retained_reclaim_route = retained_mutation_client
            .open_retained_object_mutation_route(object_pg_id, self.operation_epoch(), bucket, key)
            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
        let release_reclaim_claim = || -> Result<(), ObjectPgActionError> {
            self.maybe_run_before_reclaim_claim_release_hook()?;
            retained_reclaim_route
                .release_object_payload_reclaim_claim(&claim)
                .map_err(super::bucket_snapshot_error_to_object_pg_action_error)
        };

        let mut reclaim_fence_started = false;
        let mut payload_delete_started = false;
        let mut command_owns_reclaim_claim = false;
        let result = (|| -> Result<super::ObjectPayloadReclaimAttempt, ObjectPgActionError> {
            self.maybe_run_after_reclaim_claim_acquired_hook()?;
            if !self.local_map.try_begin_object_payload_reclaim(
                bucket,
                key,
                generation_id,
                &reclaim_authority,
            )? {
                return Ok(super::ObjectPayloadReclaimAttempt::Deferred);
            }
            reclaim_fence_started = true;
            match &reclaim {
                ObjectPayloadReclaimCommand::Segments(reclaim) => {
                    for segment in &reclaim.segments {
                        payload_delete_started = true;
                        self.delete_payload_shard_set(
                            segment.data_pg_id,
                            segment.ec,
                            &segment.segment_okh,
                            segment.segment_vid,
                        )?;
                    }
                }
                ObjectPayloadReclaimCommand::Multipart(reclaim) => {
                    for part in &reclaim.parts {
                        for segment in &part.segments {
                            payload_delete_started = true;
                            self.delete_payload_shard_set(
                                segment.data_pg_id,
                                segment.ec,
                                &segment.segment_okh,
                                segment.segment_vid,
                            )?;
                        }
                    }
                }
            }
            loop {
                if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                    let matching_reclaim_delete = matches!(
                        command.payload(),
                        MetadataCommandPayload::DeleteObjectPayloadReclaim(delete)
                            if delete.matches_request(bucket, key, generation_id)
                    );
                    if matching_reclaim_delete {
                        match self.finish_exact_pending_object_metadata_command(
                            pg_id,
                            super::ExactPendingObjectMetadataCommand::for_checked_request(&command),
                        )? {
                            super::PendingMetadataCommandOutcome::Applied => {
                                return Ok(super::ObjectPayloadReclaimAttempt::Completed);
                            }
                            super::PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                                return Err(super::conflicting_pending_object_metadata_command(
                                    "retryable partial pending payload reclaim command",
                                ));
                            }
                            super::PendingMetadataCommandOutcome::Abandoned => continue,
                        }
                    }
                    self.drain_pending_object_metadata_command(pg_id, &command)?;
                    continue;
                }

                let command_id = match self.next_object_metadata_command_id(pg_id) {
                    Ok(command_id) => command_id,
                    Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                        ..
                    })) => {
                        self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                let command = MetadataCommandEnvelope::new(
                    command_id,
                    MetadataCommandPayload::DeleteObjectPayloadReclaim(Box::new(
                        DeleteObjectPayloadReclaimCommand::new(
                            bucket.clone(),
                            key.clone(),
                            generation_id,
                            reclaim.clone(),
                            reclaim_authority.clone(),
                        ),
                    )),
                );
                if !self.try_install_object_pg_pending_command_or_drain(pg_id, bucket, &command)? {
                    continue;
                }
                command_owns_reclaim_claim = true;
                self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
                return Ok(super::ObjectPayloadReclaimAttempt::Completed);
            }
        })();
        let (pending_command_owns_reclaim_claim, reclaim_ownership_unknown) =
            if result.is_err() && command_owns_reclaim_claim {
                match self
                    .maybe_run_before_reclaim_ownership_lookup_hook()
                    .and_then(|()| {
                        self.pending_metadata_command_for_bucket(pg_id, bucket)
                            .map_err(Into::into)
                    }) {
                    Ok(command) => (
                        command.is_some_and(|command| {
                            matches!(
                                command.payload(),
                                MetadataCommandPayload::DeleteObjectPayloadReclaim(delete)
                                    if delete.matches_request(bucket, key, generation_id)
                                        && delete.reclaim_claim == reclaim_authority
                            )
                        }),
                        false,
                    ),
                    Err(_) => (true, true),
                }
            } else {
                (command_owns_reclaim_claim, false)
            };
        let mut reclaim_release_unknown = false;
        let release_claim = (result.is_err() && !pending_command_owns_reclaim_claim)
            || matches!(
                &result,
                Ok(super::ObjectPayloadReclaimAttempt::Deferred) if !reclaim_fence_started
            );
        let result = if release_claim {
            match release_reclaim_claim() {
                Ok(()) => result,
                Err(release_error) => {
                    reclaim_release_unknown = true;
                    Err(release_error)
                }
            }
        } else {
            result
        };
        match &result {
            Ok(super::ObjectPayloadReclaimAttempt::Completed) => emit_outcome("completed"),
            Ok(super::ObjectPayloadReclaimAttempt::Deferred) => emit_outcome("deferred_active"),
            Ok(super::ObjectPayloadReclaimAttempt::MissingRoot) => emit_outcome("missing_root"),
            Err(_) => emit_outcome("error"),
        }
        let keep_reclaim_fence = result.is_err()
            && (payload_delete_started || reclaim_ownership_unknown || reclaim_release_unknown);
        if reclaim_fence_started {
            self.local_map.finish_object_payload_reclaim(
                bucket,
                key,
                generation_id,
                &reclaim_authority,
                keep_reclaim_fence,
            )?;
        }
        result
    }

    #[cfg(test)]
    pub(crate) fn enqueue_durable_object_payload_reclaim_roots_excluding(
        &self,
        excluded_roots: &HashSet<(BucketName, ObjectKey, GenerationId)>,
    ) -> DurableObjectPayloadReclaimScan {
        if self.operation_epoch() != self.cluster_epoch() {
            return DurableObjectPayloadReclaimScan::default();
        }

        let mut scan = DurableObjectPayloadReclaimScan::default();
        for pg_id in self.metadata_pg_ids() {
            let pg_scan = self.enqueue_durable_object_payload_reclaim_root_for_pg_excluding(
                PgId::new(pg_id),
                excluded_roots,
            );
            scan.queued += pg_scan.queued;
            scan.errors += pg_scan.errors;
            scan.retry_required |= pg_scan.retry_required;
            if pg_scan.route_refresh_required {
                scan.route_refresh_required = true;
                break;
            }
        }
        scan
    }

    fn enqueue_durable_object_payload_reclaim_root_for_pg_excluding(
        &self,
        pg_id: PgId,
        excluded_roots: &HashSet<(BucketName, ObjectKey, GenerationId)>,
    ) -> DurableObjectPayloadReclaimScan {
        let mut scan = DurableObjectPayloadReclaimScan::default();
        let scan_pg_id = self.object_metadata_scan_pg(pg_id);
        let emit_scan = |outcome: &'static str| {
            let _ = observability::emit_object_payload_reclaim_durable_scan(
                super::TRACE_TARGET,
                observability::ObjectPayloadReclaimEventSummary {
                    pg_id: pg_id.get(),
                    event: outcome,
                },
            );
        };
        let node = match self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)
        {
            Ok(node) => node,
            Err(error) => {
                scan.errors += 1;
                emit_scan("error");
                let _ = observability::event(
                    super::TRACE_TARGET,
                    "object_reclaim_durable_scan_pg_error",
                    Some(format_args!("pg_id={} error={:?}", pg_id.get(), error)),
                );
                scan.route_refresh_required = durable_reclaim_scan_requires_route_refresh(&error);
                scan.retry_required = !scan.route_refresh_required;
                return scan;
            }
        };
        let root = match node
            .object_mutation_metadata_client()
            .get_payload_reclaim_root(scan_pg_id)
        {
            Ok(root) => root,
            Err(error) => {
                scan.errors += 1;
                emit_scan("error");
                let _ = observability::event(
                    super::TRACE_TARGET,
                    "object_reclaim_durable_scan_pg_error",
                    Some(format_args!("pg_id={} error={:?}", pg_id.get(), error)),
                );
                scan.route_refresh_required =
                    durable_reclaim_bucket_scan_requires_route_refresh(&error);
                scan.retry_required = !scan.route_refresh_required;
                return scan;
            }
        };
        let Some(root) = root else {
            return scan;
        };
        // The metadata query returns one row, not an exhaustion proof. A
        // follow-up pass is required even when this row is queued successfully.
        scan.retry_required = true;
        if excluded_roots.contains(&(root.bucket.clone(), root.key.clone(), root.generation_id)) {
            emit_scan("deferred_locally");
            return scan;
        }
        if self.object_metadata_pg_id(&root.bucket, &root.key) != pg_id.get() {
            scan.errors += 1;
            emit_scan("wrong_pg");
            let _ = observability::event(
                super::TRACE_TARGET,
                "object_reclaim_durable_scan_wrong_pg_root",
                Some(format_args!(
                    "pg_id={} root_bucket={} root_key={}",
                    pg_id.get(),
                    root.bucket,
                    root.key
                )),
            );
            scan.retry_required = true;
            return scan;
        }
        let lease_count = match self.local_map.object_payload_lease_count(
            &root.bucket,
            &root.key,
            root.generation_id,
        ) {
            Ok(count) => count,
            Err(error) => {
                scan.errors += 1;
                emit_scan("error");
                let _ = observability::event(
                    super::TRACE_TARGET,
                    "object_reclaim_durable_scan_lease_error",
                    Some(format_args!("pg_id={} error={:?}", pg_id.get(), error)),
                );
                scan.route_refresh_required = durable_reclaim_scan_requires_route_refresh(&error);
                scan.retry_required = !scan.route_refresh_required;
                return scan;
            }
        };
        if lease_count != 0 {
            emit_scan("leased");
            return scan;
        }
        match self.enqueue_object_payload_reclaim_for_pg(
            &root.bucket,
            &root.key,
            root.generation_id,
        ) {
            Some(ReclaimQueueInsert::Queued) => {
                emit_scan("queued");
                scan.queued += 1;
            }
            Some(ReclaimQueueInsert::Deduplicated) => emit_scan("deduplicated"),
            Some(ReclaimQueueInsert::PgCapacityDeferred) => {
                emit_scan("pg_capacity_deferred");
            }
            None => {}
        }
        scan
    }

    #[cfg(test)]
    pub(crate) fn enqueue_durable_bucket_delete_begin_roots_excluding(
        &self,
        excluded_roots: &HashSet<BucketDeleteBeginRoot>,
    ) -> DurableBucketDeleteBeginScan {
        if self.operation_epoch() != self.cluster_epoch() {
            return DurableBucketDeleteBeginScan::default();
        }

        let mut scan = DurableBucketDeleteBeginScan::default();
        for pg_id in self.metadata_pg_ids() {
            let pg_scan = self.enqueue_durable_bucket_delete_begin_roots_for_pg_excluding(
                PgId::new(pg_id),
                excluded_roots,
            );
            scan.queued += pg_scan.queued;
            scan.errors += pg_scan.errors;
            scan.retry_required |= pg_scan.retry_required;
            if pg_scan.route_refresh_required {
                scan.route_refresh_required = true;
                break;
            }
        }
        scan
    }

    fn enqueue_durable_bucket_delete_begin_roots_for_pg_excluding(
        &self,
        pg_id: PgId,
        excluded_roots: &HashSet<BucketDeleteBeginRoot>,
    ) -> DurableBucketDeleteBeginScan {
        let mut scan = DurableBucketDeleteBeginScan::default();
        let node = match self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)
        {
            Ok(node) => node,
            Err(error) => {
                scan.errors += 1;
                let _ = observability::event(
                    super::TRACE_TARGET,
                    "bucket_begin_durable_scan_pg_error",
                    Some(format_args!("pg_id={} error={:?}", pg_id.get(), error)),
                );
                scan.route_refresh_required = durable_reclaim_scan_requires_route_refresh(&error);
                scan.retry_required = !scan.route_refresh_required;
                return scan;
            }
        };
        let now = crate::clock::current_time_millis();
        let mut start_after_bucket = None;
        let mut queued_for_pg = 0usize;
        loop {
            let roots = match node
                .bucket_write_reservation_client()
                .get_bucket_delete_begin_roots(
                    self.validated_bucket_metadata_pg(pg_id),
                    now,
                    start_after_bucket.as_ref(),
                    BUCKET_DELETE_BEGIN_SCAN_LIMIT_PER_PG,
                ) {
                Ok(roots) => roots,
                Err(error) => {
                    scan.errors += 1;
                    let _ = observability::event(
                        super::TRACE_TARGET,
                        "bucket_begin_durable_scan_pg_error",
                        Some(format_args!("pg_id={} error={:?}", pg_id.get(), error)),
                    );
                    scan.route_refresh_required =
                        durable_reclaim_bucket_scan_requires_route_refresh(&error);
                    scan.retry_required = !scan.route_refresh_required;
                    return scan;
                }
            };
            if roots.is_empty() {
                break;
            }
            let page_len = roots.len();
            for root in roots {
                start_after_bucket = Some(root.bucket.clone());
                if excluded_roots.contains(&root) {
                    continue;
                }
                self.local_map
                    .runtime_state()
                    .enqueue_bucket_delete_begin(root);
                scan.queued += 1;
                queued_for_pg += 1;
                if queued_for_pg >= BUCKET_DELETE_BEGIN_SCAN_LIMIT_PER_PG {
                    scan.retry_required = true;
                    break;
                }
            }
            if queued_for_pg >= BUCKET_DELETE_BEGIN_SCAN_LIMIT_PER_PG
                || page_len < BUCKET_DELETE_BEGIN_SCAN_LIMIT_PER_PG
            {
                break;
            }
        }
        scan
    }

    #[cfg(test)]
    pub(crate) fn enqueue_durable_bucket_delete_finalize_roots(
        &self,
    ) -> DurableBucketDeleteFinalizeScan {
        self.enqueue_durable_bucket_delete_finalize_roots_excluding(&HashSet::new())
    }

    #[cfg(test)]
    pub(crate) fn enqueue_durable_bucket_delete_finalize_roots_excluding(
        &self,
        excluded_bucket_delete_finalize_roots: &HashSet<BucketName>,
    ) -> DurableBucketDeleteFinalizeScan {
        if self.operation_epoch() != self.cluster_epoch() {
            return DurableBucketDeleteFinalizeScan::default();
        }

        let mut scan = DurableBucketDeleteFinalizeScan::default();
        for pg_id in self.metadata_pg_ids() {
            let pg_scan = self.enqueue_durable_bucket_delete_finalize_roots_for_pg_excluding(
                PgId::new(pg_id),
                excluded_bucket_delete_finalize_roots,
            );
            scan.queued += pg_scan.queued;
            scan.errors += pg_scan.errors;
            scan.retry_required |= pg_scan.retry_required;
            if pg_scan.route_refresh_required {
                scan.route_refresh_required = true;
                break;
            }
        }
        scan
    }

    fn enqueue_durable_bucket_delete_finalize_roots_for_pg_excluding(
        &self,
        pg_id: PgId,
        excluded_bucket_delete_finalize_roots: &HashSet<BucketName>,
    ) -> DurableBucketDeleteFinalizeScan {
        let mut scan = DurableBucketDeleteFinalizeScan::default();
        let node = match self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)
        {
            Ok(node) => node,
            Err(error) => {
                scan.errors += 1;
                let _ = observability::event(
                    super::TRACE_TARGET,
                    "bucket_finalize_durable_scan_pg_error",
                    Some(format_args!("pg_id={} error={:?}", pg_id.get(), error)),
                );
                scan.route_refresh_required = durable_reclaim_scan_requires_route_refresh(&error);
                scan.retry_required = !scan.route_refresh_required;
                return scan;
            }
        };
        let roots = match node
            .bucket_write_reservation_client()
            .get_bucket_delete_finalize_roots(
                self.validated_bucket_metadata_pg(pg_id),
                crate::clock::current_time_millis(),
                BUCKET_DELETE_FINALIZE_SCAN_LIMIT_PER_PG,
            ) {
            Ok(roots) => roots,
            Err(error) => {
                scan.errors += 1;
                let _ = observability::event(
                    super::TRACE_TARGET,
                    "bucket_finalize_durable_scan_pg_error",
                    Some(format_args!("pg_id={} error={:?}", pg_id.get(), error)),
                );
                scan.route_refresh_required =
                    durable_reclaim_bucket_scan_requires_route_refresh(&error);
                scan.retry_required = !scan.route_refresh_required;
                return scan;
            }
        };
        if roots.len() >= BUCKET_DELETE_FINALIZE_SCAN_LIMIT_PER_PG {
            scan.retry_required = true;
        }
        for root in roots {
            if excluded_bucket_delete_finalize_roots.contains(&root.bucket) {
                continue;
            }
            self.enqueue_bucket_delete_finalize(root);
            scan.queued += 1;
        }
        scan
    }

    pub(super) fn delete_complete_multipart_cleanup_best_effort(
        &self,
        cleanup: &CompleteMultipartCommitCleanup,
    ) {
        self.delete_multipart_part_segments_best_effort(&cleanup.omitted_streaming_segments);
        self.delete_staged_stream_segment_payload_shards_best_effort(
            &cleanup.stream_upload_segments,
        );
    }

    pub(super) fn delete_finalize_upload_part_cleanup_best_effort(
        &self,
        cleanup: &FinalizeStreamPartCleanup,
    ) {
        self.delete_multipart_part_segments_best_effort(&cleanup.displaced_segments);
    }

    pub(super) fn delete_abort_multipart_cleanup_best_effort(
        &self,
        cleanup: &AbortMultipartUploadCleanup,
    ) {
        self.delete_multipart_part_segments_best_effort(&cleanup.streaming_segments);
        self.delete_staged_stream_segment_payload_shards_best_effort(
            &cleanup.stream_upload_segments,
        );
    }

    fn delete_multipart_part_segments_best_effort(&self, segments: &[MultipartPartSegmentRecord]) {
        for segment in segments {
            self.delete_multipart_shard_set_best_effort(
                segment.placement_cluster_epoch,
                segment.data_pg_id,
                &segment.segment_okh,
                segment.segment_vid,
                EcShape {
                    k: segment.ec_k,
                    m: segment.ec_m,
                },
            );
        }
    }

    fn delete_multipart_shard_set_best_effort(
        &self,
        operation_epoch: ClusterEpoch,
        data_pg_id: u32,
        okh: &[u8; 16],
        generation_id: GenerationId,
        ec: EcShape,
    ) {
        self.delete_payload_shard_set_best_effort_at_epoch(
            operation_epoch,
            data_pg_id,
            ec,
            okh,
            generation_id,
        );
    }

    pub fn create_put_object_stream_session<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        request: BucketSnapshotRequest,
        action: impl FnMut(
            BucketSnapshot,
            Option<StoredObject>,
        ) -> Result<(T, CreateStreamUploadReq), E>,
    ) -> Result<Result<T, E>, BucketSnapshotLoadError> {
        self.create_put_object_stream_session_with_cleanup_deadline(
            bucket, key, request, None, action,
        )
    }

    pub fn create_put_object_stream_session_with_cleanup_deadline<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        request: BucketSnapshotRequest,
        cleanup_after: Option<u64>,
        action: impl FnMut(
            BucketSnapshot,
            Option<StoredObject>,
        ) -> Result<(T, CreateStreamUploadReq), E>,
    ) -> Result<Result<T, E>, BucketSnapshotLoadError> {
        self.create_put_object_stream_session_with_route_validation(
            super::PutObjectMutationEffectRoute {
                bucket_pg_id: self.bucket_metadata_pg(bucket),
                object_pg_id: self.object_metadata_pg(bucket, key),
                bucket,
                key,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            || Ok(()),
            request,
            cleanup_after,
            action,
        )
    }

    pub(super) fn create_put_object_stream_session_with_route_validation<T, E>(
        &self,
        route: super::PutObjectMutationEffectRoute<'_>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        request: BucketSnapshotRequest,
        cleanup_after: Option<u64>,
        mut action: impl FnMut(
            BucketSnapshot,
            Option<StoredObject>,
        ) -> Result<(T, CreateStreamUploadReq), E>,
    ) -> Result<Result<T, E>, BucketSnapshotLoadError> {
        crate::metadata_command::metadata_command_publisher!(CreatePutObjectStreamSession);
        enum Attempt<T> {
            Complete(T),
            Retry,
        }

        let super::PutObjectMutationEffectRoute {
            bucket_pg_id,
            object_pg_id,
            bucket,
            key,
            effect_fence,
        } = route;
        let pg_id = object_pg_id.pg_id();
        loop {
            require_valid_route()?;
            let applied_commands = self
                .drain_pending_object_metadata_commands_for_bucket_collect(pg_id, bucket)
                .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
            require_valid_route()?;
            let reservation = match self.acquire_durable_bucket_write_reservation_with_effect_fence(
                bucket,
                PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
                Some(key.as_str()),
                Some(effect_fence),
            ) {
                Ok(reservation) => reservation,
                Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteDraining)) => {
                    self.wait_for_durable_bucket_write_drain(bucket)?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let proof = BucketWriteReservationProof::from(&reservation.record);
            let mut disposition = super::BucketWriteReservationDisposition::ReleaseByCaller;
            let result = (|| {
                require_valid_route()?;
                let snapshot =
                    reservation
                        .node
                        .load_bucket_snapshot(bucket_pg_id, bucket, request)?;

                let mutation_client = self.object_mutation_metadata_primary_client(bucket, key)?;
                require_valid_route()?;
                let current_object = mutation_client
                    .open_object_delete_metadata_route(
                        self.operation_epoch(),
                        object_pg_id,
                        bucket,
                        key,
                    )
                    .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?
                    .load_current_object_delete_snapshot()
                    .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
                let existing_object = match current_object.stored.as_ref() {
                    Some(StoredObject::Live(record)) => Some(StoredObject::Live(record.clone())),
                    Some(StoredObject::DeleteMarker(_)) | None => None,
                };

                let (value, create) = match action(snapshot, existing_object) {
                    Ok(prepared) => prepared,
                    Err(error) => return Ok(Err(error)),
                };
                if create.bucket != *bucket || create.key != *key {
                    return Err(BucketSnapshotLoadError::Store(
                        StoreError::RouteCapabilitySubjectMismatch {
                            operation: "create put object stream session",
                        },
                    ));
                }
                require_valid_route()?;
                if mutation_client
                    .matching_stream_upload_exists(
                        object_pg_id,
                        &create,
                        super::applied_stream_create_command(&applied_commands, &create),
                    )
                    .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?
                {
                    return Ok(Ok(Attempt::Complete(value)));
                }
                self.reserve_put_object_generation_with_route_validation(
                    route,
                    &create.session_id,
                    &mut require_valid_route,
                )
                .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;

                #[cfg(test)]
                maybe_run_before_stream_put_create_command_id_hook(
                    self.metadata_command_apply_test_hook_scope_id(),
                );
                if let Err(error) = require_valid_route() {
                    let _ =
                        self.release_object_generation_reservation(bucket, key, &create.session_id);
                    return Err(error.into());
                }
                let command = match mutation_client.build_create_stream_upload_command(
                    BuildCreateStreamUploadCommandReq {
                        pg_id: object_pg_id,
                        cluster_epoch: self.operation_epoch(),
                        request: &create,
                        cleanup_after,
                        precondition: CreateStreamUploadPrecondition::PutObject {
                            expected_current: current_object.stored.as_ref(),
                            require_generation_reservation: true,
                        },
                        bucket_write_reservation: &proof,
                    },
                ) {
                    Ok(command) => command,
                    Err(ObjectPgActionError::StaleObjectReadSubject) => {
                        let cleanup = self.release_object_generation_reservation(
                            bucket,
                            key,
                            &create.session_id,
                        );
                        if let Err(cleanup_error) = cleanup {
                            return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                                cleanup_error,
                            ));
                        }
                        return Ok(Ok(Attempt::Retry));
                    }
                    Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                        ..
                    })) => {
                        let cleanup = self
                            .drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)
                            .and_then(|_| {
                                self.release_object_generation_reservation(
                                    bucket,
                                    key,
                                    &create.session_id,
                                )
                            });
                        if let Err(cleanup_error) = cleanup {
                            return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                                cleanup_error,
                            ));
                        }
                        return Ok(Ok(Attempt::Retry));
                    }
                    Err(error) => {
                        let _ = self.release_object_generation_reservation(
                            bucket,
                            key,
                            &create.session_id,
                        );
                        return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                            error,
                        ));
                    }
                };
                #[cfg(any(test, feature = "test-hooks"))]
                maybe_run_before_stream_put_create_pending_install_hook(
                    self.metadata_command_apply_test_hook_scope_id(),
                );
                if let Err(error) = require_valid_route() {
                    let _ =
                        self.release_object_generation_reservation(bucket, key, &create.session_id);
                    return Err(error.into());
                }
                self.maybe_run_before_metadata_command_pending_install_hook();
                let installed = match self
                    .try_install_pending_metadata_command_for_bucket_with_effect_fence(
                        pg_id,
                        bucket,
                        &command,
                        Some(effect_fence),
                    ) {
                    Ok(installed) => installed,
                    Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                        ..
                    })) => {
                        let cleanup = self
                            .drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)
                            .and_then(|_| {
                                self.release_object_generation_reservation(
                                    bucket,
                                    key,
                                    &create.session_id,
                                )
                            });
                        if let Err(cleanup_error) = cleanup {
                            return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                                cleanup_error,
                            ));
                        }
                        return Ok(Ok(Attempt::Retry));
                    }
                    Err(error) => {
                        let _ = self.release_object_generation_reservation(
                            bucket,
                            key,
                            &create.session_id,
                        );
                        return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                            error,
                        ));
                    }
                };
                if !installed {
                    let cleanup = self
                        .drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)
                        .and_then(|_| {
                            self.release_object_generation_reservation(
                                bucket,
                                key,
                                &create.session_id,
                            )
                        });
                    if let Err(cleanup_error) = cleanup {
                        return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                            cleanup_error,
                        ));
                    }
                    return Ok(Ok(Attempt::Retry));
                }
                if let Err(error) =
                    self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)
                {
                    match self.pending_metadata_command_uses_bucket_write_reservation(
                        pg_id, bucket, &proof,
                    ) {
                        Ok(true) => {
                            disposition =
                                super::BucketWriteReservationDisposition::TransferredToCommand;
                        }
                        Ok(false) => {
                            let _ = self.release_object_generation_reservation(
                                bucket,
                                key,
                                &create.session_id,
                            );
                        }
                        Err(lookup_error) => {
                            disposition = super::BucketWriteReservationDisposition::PreserveForOwnershipCheckFailure;
                            return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                                lookup_error,
                            ));
                        }
                    }
                    return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                        error,
                    ));
                }

                disposition = super::BucketWriteReservationDisposition::TransferredToCommand;
                Ok(Ok(Attempt::Complete(value)))
            })();
            let release_result = match disposition {
                super::BucketWriteReservationDisposition::TransferredToCommand => Ok(()),
                super::BucketWriteReservationDisposition::PreserveForOwnershipCheckFailure => {
                    Ok(())
                }
                super::BucketWriteReservationDisposition::ReleaseByCaller => {
                    self.release_durable_bucket_write_reservation(reservation)
                }
            };
            let attempt = Self::finish_bucket_write_snapshot_operation(result, release_result)?;
            match attempt {
                Ok(Attempt::Complete(value)) => return Ok(Ok(value)),
                Ok(Attempt::Retry) => continue,
                Err(error) => return Ok(Err(error)),
            }
        }
    }

    pub fn finalize_put_object_stream<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        total_size: u64,
        action: impl FnMut(StreamPutFinalizeSnapshot) -> Result<PreparedStreamPutCommit<T>, E>,
    ) -> Result<Result<FinalizeStreamPutOutcome<T>, E>, ObjectPgActionError> {
        self.finalize_put_object_stream_with_route_validation(
            super::PutObjectMutationEffectRoute {
                bucket_pg_id: self.bucket_metadata_pg(bucket),
                object_pg_id: self.object_metadata_pg(bucket, key),
                bucket,
                key,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            session_id,
            total_size,
            || Ok(()),
            action,
        )
    }

    pub(super) fn finalize_put_object_stream_with_route_validation<T, E>(
        &self,
        route: super::PutObjectMutationEffectRoute<'_>,
        session_id: &SessionId,
        total_size: u64,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        mut action: impl FnMut(StreamPutFinalizeSnapshot) -> Result<PreparedStreamPutCommit<T>, E>,
    ) -> Result<Result<FinalizeStreamPutOutcome<T>, E>, ObjectPgActionError> {
        crate::metadata_command::metadata_command_publisher!(FinalizePutObjectStream);
        let super::PutObjectMutationEffectRoute {
            object_pg_id,
            bucket,
            key,
            effect_fence,
            ..
        } = route;
        let pg_id = object_pg_id.pg_id();
        let mutation_client = self.object_mutation_metadata_primary_client(bucket, key)?;
        let mut stale_snapshot_work_budget =
            super::RequestWorkBudget::new(super::STREAM_PUT_STALE_COMMIT_RETRY_BUDGET, None)
                .for_operation("finalize_stream_put")
                .for_pg(pg_id);

        let (command, new_pending_command, prepared) = loop {
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            while let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                let is_matching_stream_commit = matches!(
                    command.payload(),
                    MetadataCommandPayload::CommitDirectPutObject(commit)
                        if commit.matches_stream_session(bucket, key, session_id)
                );
                if is_matching_stream_commit {
                    break;
                }
                self.drain_pending_object_metadata_command(pg_id, &command)?;
            }

            let pending_command = self
                .pending_metadata_command_for_bucket(pg_id, bucket)?
                .filter(|command| {
                    matches!(
                        command.payload(),
                        MetadataCommandPayload::CommitDirectPutObject(commit)
                            if commit.matches_stream_session(bucket, key, session_id)
                    )
                });

            let storage_snapshot = match mutation_client.load_stream_put_finalize_snapshot(
                object_pg_id,
                bucket,
                key,
                session_id,
            ) {
                Ok(snapshot) => snapshot,
                Err(
                    error @ ObjectPgActionError::Metadata(MetadataError::StreamSessionNotFound {
                        ..
                    }),
                ) => {
                    if let Some(command) = pending_command.clone() {
                        self.apply_exact_pending_object_metadata_command(
                            pg_id,
                            super::ExactPendingObjectMetadataCommand::for_checked_request(&command),
                        )?;
                        continue;
                    }
                    return Err(error);
                }
                Err(error) => return Err(error),
            };
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            let Some(effective_bucket_write_reservation) =
                storage_snapshot.session.bucket_write_reservation.as_ref()
            else {
                return Err(ObjectPgActionError::InvalidRequest {
                    reason: "PutObject stream session is missing bucket write proof".to_string(),
                });
            };
            if pending_command.as_ref().is_some_and(|command| {
                matches!(
                    command.payload(),
                    MetadataCommandPayload::CommitDirectPutObject(commit)
                        if commit.bucket_write_reservation != *effective_bucket_write_reservation
                )
            }) {
                return Err(ObjectPgActionError::InvalidRequest {
                    reason: "pending stream PUT commit reservation proof does not match the durable stream session"
                        .to_string(),
                });
            }
            let prepared = match action(StreamPutFinalizeSnapshot {
                session: storage_snapshot.session.clone(),
                existing_etag: storage_snapshot.existing_etag.clone(),
            }) {
                Ok(prepared) => prepared,
                Err(error) => return Ok(Err(error)),
            };

            let (command, new_pending_command) = match pending_command {
                Some(command) => (command, false),
                None => {
                    let version_id = if prepared.versioning == BucketVersioningState::Enabled {
                        self.reserve_next_object_version_for_completion_with_effect_fence(
                            pg_id,
                            bucket,
                            key,
                            effect_fence,
                            &mut require_valid_route,
                        )?
                    } else {
                        VersionId::Null
                    };
                    let commit = StreamPutCommitInput {
                        versioning: prepared.versioning,
                        version_id,
                        owner: prepared.owner.clone(),
                        acl_grants: prepared.acl_grants.clone(),
                        public_read: prepared.public_read,
                        size: prepared.size,
                        etag_crc64: prepared.etag_crc64,
                        tags: prepared.tags.clone(),
                        metadata_blob: prepared.metadata_blob.clone(),
                        system_metadata_blob: prepared.system_metadata_blob.clone(),
                        object_lock: prepared.object_lock,
                        encryption: prepared.encryption.clone(),
                    };
                    #[cfg(any(test, feature = "test-hooks"))]
                    maybe_run_before_stream_put_finalize_command_id_hook(
                        self.metadata_command_apply_test_hook_scope_id(),
                    );
                    require_valid_route().map_err(ObjectPgActionError::Store)?;
                    let command = match mutation_client.build_stream_put_commit_command(
                        BuildStreamPutCommitCommandReq {
                            pg_id: object_pg_id,
                            cluster_epoch: self.operation_epoch(),
                            bucket,
                            key,
                            session_id,
                            total_size,
                            expected_snapshot: &storage_snapshot,
                            commit: &commit,
                            bucket_write_reservation: effective_bucket_write_reservation,
                        },
                    ) {
                        Ok(command) => command,
                        Err(ObjectPgActionError::StaleStreamFinalizeSnapshot) => {
                            if let Err(error) = stale_snapshot_work_budget.sleep_after_contention(
                                "stream PUT stale commit snapshot retry budget exhausted",
                            ) {
                                return Err(ObjectPgActionError::Store(error));
                            }
                            continue;
                        }
                        Err(ObjectPgActionError::Store(
                            StoreError::MetadataCommandLogConflict { .. },
                        )) => {
                            self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
                            continue;
                        }
                        Err(error) => return Err(error),
                    };
                    require_valid_route().map_err(ObjectPgActionError::Store)?;
                    let installed = match self
                        .try_install_pending_metadata_command_for_bucket_with_effect_fence(
                            pg_id,
                            bucket,
                            &command,
                            Some(effect_fence),
                        ) {
                        Ok(installed) => installed,
                        Err(ObjectPgActionError::Store(
                            StoreError::MetadataCommandLogConflict { .. },
                        )) => {
                            self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
                            continue;
                        }
                        Err(error) => return Err(error),
                    };
                    if !installed {
                        continue;
                    }
                    (command, true)
                }
            };
            break (command, new_pending_command, prepared);
        };

        if new_pending_command {
            self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
        } else {
            self.apply_exact_pending_object_metadata_command(
                pg_id,
                super::ExactPendingObjectMetadataCommand::for_checked_request(&command),
            )?;
        }

        let MetadataCommandPayload::CommitDirectPutObject(commit) = command.payload() else {
            unreachable!("stream put commit pending command kind changed");
        };
        Ok(Ok(FinalizeStreamPutOutcome {
            value: prepared.value,
            version_id: commit.object.version_id,
            encryption: commit.object.encryption.clone(),
            live_tags: commit.object.tags.clone(),
            live_size: commit.object.size,
            live_last_modified: commit.last_modified_millis,
            stale_generation_id: super::object_payload_reclaim_generation(&commit.stale_payload),
        }))
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn create_multipart_upload<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        request: BucketSnapshotRequest,
        mut action: impl FnMut(
            BucketSnapshot,
            Option<StoredObject>,
        ) -> Result<(T, CreateMultipartUploadReq), E>,
    ) -> Result<Result<CreateMultipartUploadOutcome<T>, E>, BucketSnapshotLoadError> {
        self.create_multipart_upload_inner_with_route_validation(
            super::MultipartObjectMutationEffectRoute {
                pg_id: self.object_metadata_pg(bucket, key),
                bucket,
                key,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            || Ok(()),
            request,
            |snapshot, existing| {
                action(snapshot, existing).map(|(value, create)| (value, create, None))
            },
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn create_multipart_upload_with_ordered_id<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        request: BucketSnapshotRequest,
        action: impl FnMut(
            BucketSnapshot,
            Option<StoredObject>,
        ) -> Result<(T, CreateMultipartUploadReq, MultipartUploadIdKey), E>,
    ) -> Result<Result<CreateMultipartUploadOutcome<T>, E>, BucketSnapshotLoadError> {
        self.create_multipart_upload_with_ordered_id_with_route_validation(
            super::MultipartObjectMutationEffectRoute {
                pg_id: self.object_metadata_pg(bucket, key),
                bucket,
                key,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            || Ok(()),
            request,
            action,
        )
    }

    pub(super) fn create_multipart_upload_with_ordered_id_with_route_validation<T, E>(
        &self,
        route: super::MultipartObjectMutationEffectRoute<'_>,
        require_valid_route: impl FnMut() -> Result<(), StoreError>,
        request: BucketSnapshotRequest,
        mut action: impl FnMut(
            BucketSnapshot,
            Option<StoredObject>,
        )
            -> Result<(T, CreateMultipartUploadReq, MultipartUploadIdKey), E>,
    ) -> Result<Result<CreateMultipartUploadOutcome<T>, E>, BucketSnapshotLoadError> {
        self.create_multipart_upload_inner_with_route_validation(
            route,
            require_valid_route,
            request,
            |snapshot, existing| {
                action(snapshot, existing)
                    .map(|(value, create, upload_id_key)| (value, create, Some(upload_id_key)))
            },
        )
    }

    fn create_multipart_upload_inner_with_route_validation<T, E>(
        &self,
        route: super::MultipartObjectMutationEffectRoute<'_>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        request: BucketSnapshotRequest,
        mut action: impl FnMut(
            BucketSnapshot,
            Option<StoredObject>,
        )
            -> Result<(T, CreateMultipartUploadReq, Option<MultipartUploadIdKey>), E>,
    ) -> Result<Result<CreateMultipartUploadOutcome<T>, E>, BucketSnapshotLoadError> {
        crate::metadata_command::metadata_command_publisher!(CreateMultipartUpload);
        enum Attempt<T> {
            Complete(CreateMultipartUploadOutcome<T>),
            Retry,
        }

        let super::MultipartObjectMutationEffectRoute {
            pg_id: object_pg_id,
            bucket,
            key,
            effect_fence,
        } = route;
        let pg_id = object_pg_id.pg_id();
        loop {
            require_valid_route()?;
            let applied_commands = self
                .drain_pending_object_metadata_commands_for_bucket_collect(pg_id, bucket)
                .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
            require_valid_route()?;
            let reservation = match self.acquire_durable_bucket_write_reservation_with_effect_fence(
                bucket,
                CREATE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
                Some(key.as_str()),
                Some(effect_fence),
            ) {
                Ok(reservation) => reservation,
                Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteDraining)) => {
                    self.wait_for_durable_bucket_write_drain(bucket)?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let proof = BucketWriteReservationProof::from(&reservation.record);
            let mut disposition = super::BucketWriteReservationDisposition::ReleaseByCaller;
            let result = (|| {
                require_valid_route()?;
                let snapshot = reservation.node.load_bucket_snapshot(
                    self.validated_bucket_metadata_pg(PgId::new(reservation.pg_id)),
                    bucket,
                    request,
                )?;

                let mutation_client = self.object_mutation_metadata_primary_client(bucket, key)?;
                let multipart_creation_route = mutation_client
                    .open_multipart_upload_creation_metadata_route(
                        self.operation_epoch(),
                        object_pg_id,
                        bucket,
                        key,
                    )
                    .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
                require_valid_route()?;
                let current_object = mutation_client
                    .open_object_delete_metadata_route(
                        self.operation_epoch(),
                        object_pg_id,
                        bucket,
                        key,
                    )
                    .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?
                    .load_current_object_delete_snapshot()
                    .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
                let existing_object = match current_object.stored.as_ref() {
                    Some(StoredObject::Live(record)) => Some(StoredObject::Live(record.clone())),
                    Some(StoredObject::DeleteMarker(_)) | None => None,
                };

                let (value, create, upload_id_key) = match action(snapshot, existing_object) {
                    Ok(prepared) => prepared,
                    Err(error) => return Ok(Err(error)),
                };
                if create.bucket != *bucket || create.key != *key {
                    return Err(BucketSnapshotLoadError::Store(
                        StoreError::RouteCapabilitySubjectMismatch {
                            operation: "create multipart upload",
                        },
                    ));
                }
                require_valid_route()?;
                let applied_create =
                    super::applied_multipart_create_command(&applied_commands, &create);
                if let Some(initiated_at) = multipart_creation_route
                    .matching_multipart_upload_initiated_at(&create, applied_create)
                    .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?
                {
                    return Ok(Ok(Attempt::Complete(CreateMultipartUploadOutcome {
                        value,
                        upload_id: applied_create.map_or_else(
                            || create.upload_id.clone(),
                            |command| command.upload.upload_id.clone(),
                        ),
                        initiated_at,
                    })));
                }

                let mut command = match multipart_creation_route
                    .build_create_multipart_upload_command(BuildCreateMultipartUploadCommandReq {
                        request: &create,
                        expected_current: current_object.stored.as_ref(),
                        bucket_write_reservation: &proof,
                    }) {
                    Ok(command) => command,
                    Err(ObjectPgActionError::StaleObjectReadSubject) => {
                        return Ok(Ok(Attempt::Retry));
                    }
                    Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                        ..
                    })) => {
                        self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)
                            .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
                        return Ok(Ok(Attempt::Retry));
                    }
                    Err(error) => {
                        return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                            error,
                        ));
                    }
                };
                if let Some(upload_id_key) = upload_id_key {
                    let MetadataCommandPayload::CreateMultipartUpload(provisional_command) =
                        command.payload()
                    else {
                        unreachable!("multipart create command changed payload kind");
                    };
                    let ordered_upload_id = upload_id_key.with_listing_position(
                        &create.bucket,
                        &create.key,
                        &create.upload_id,
                        command.id().cluster_epoch().get(),
                        command.id().log_index().get(),
                    );
                    let mut ordered_command = provisional_command.as_ref().clone();
                    ordered_command.upload.upload_id = ordered_upload_id;
                    command = MetadataCommandEnvelope::new(
                        command.id(),
                        MetadataCommandPayload::CreateMultipartUpload(Box::new(ordered_command)),
                    );
                }
                require_valid_route()?;
                match self
                    .install_snapshot_sensitive_metadata_command_or_drain(
                        pg_id,
                        bucket,
                        &command,
                        Some(effect_fence),
                    )
                    .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?
                {
                    super::SnapshotSensitiveCommandInstall::Installed => {}
                    super::SnapshotSensitiveCommandInstall::ContenderDrained => {
                        return Ok(Ok(Attempt::Retry));
                    }
                }
                if let Err(error) =
                    self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)
                {
                    match self.pending_metadata_command_uses_bucket_write_reservation(
                        pg_id, bucket, &proof,
                    ) {
                        Ok(true) => {
                            disposition =
                                super::BucketWriteReservationDisposition::TransferredToCommand;
                        }
                        Ok(false) => {}
                        Err(lookup_error) => {
                            disposition = super::BucketWriteReservationDisposition::PreserveForOwnershipCheckFailure;
                            return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                                lookup_error,
                            ));
                        }
                    }
                    return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                        error,
                    ));
                }
                disposition = super::BucketWriteReservationDisposition::TransferredToCommand;

                let MetadataCommandPayload::CreateMultipartUpload(create_command) =
                    command.payload()
                else {
                    unreachable!("multipart create command changed payload kind");
                };
                let initiated_at = create_command.upload.initiated_at;
                Ok(Ok(Attempt::Complete(CreateMultipartUploadOutcome {
                    value,
                    upload_id: create_command.upload.upload_id.clone(),
                    initiated_at,
                })))
            })();
            let release_result = match disposition {
                super::BucketWriteReservationDisposition::TransferredToCommand => Ok(()),
                super::BucketWriteReservationDisposition::PreserveForOwnershipCheckFailure => {
                    Ok(())
                }
                super::BucketWriteReservationDisposition::ReleaseByCaller => {
                    self.release_durable_bucket_write_reservation(reservation)
                }
            };
            let attempt = Self::finish_bucket_write_snapshot_operation(result, release_result)?;
            match attempt {
                Ok(Attempt::Complete(outcome)) => return Ok(Ok(outcome)),
                Ok(Attempt::Retry) => continue,
                Err(error) => return Ok(Err(error)),
            }
        }
    }

    pub fn load_multipart_upload(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, BucketSnapshotLoadError> {
        let pg_id = self.object_metadata_pg(bucket, key);
        let mutation_client = self.object_mutation_metadata_primary_client(bucket, key)?;
        mutation_client
            .open_multipart_upload_lookup_metadata_route(self.operation_epoch(), pg_id, bucket, key)
            .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?
            .load_multipart_upload(upload_id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn begin_upload_part_stream_session<T, E>(
        &self,
        req: BeginUploadPartStreamSessionReq,
        action: impl FnMut(&MultipartUploadRecord) -> Result<(AuthorizedMultipartUploadRecord, T), E>,
    ) -> Result<Result<T, E>, BucketSnapshotLoadError> {
        self.begin_upload_part_stream_session_with_cleanup_deadline(req, None, action)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn begin_upload_part_stream_session_with_cleanup_deadline<T, E>(
        &self,
        req: BeginUploadPartStreamSessionReq,
        cleanup_after: Option<u64>,
        mut action: impl FnMut(
            &MultipartUploadRecord,
        ) -> Result<(AuthorizedMultipartUploadRecord, T), E>,
    ) -> Result<Result<T, E>, BucketSnapshotLoadError> {
        crate::metadata_command::metadata_command_publisher!(CreateUploadPartStreamSession);
        let BeginUploadPartStreamSessionReq {
            bucket,
            key,
            upload_id,
            part_number,
            session_id,
            bucket_write_reservation,
        } = req;
        let object_pg_id = self.object_metadata_pg(&bucket, &key);
        let pg_id = object_pg_id.pg_id();
        let mutation_client = self.object_mutation_metadata_primary_client(&bucket, &key)?;
        let multipart_lookup_route = mutation_client
            .open_multipart_upload_lookup_metadata_route(
                self.operation_epoch(),
                object_pg_id,
                &bucket,
                &key,
            )
            .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
        macro_rules! release_caller_bucket_write_proof {
            () => {{
                self.release_bucket_write_reservation_proof(&bucket_write_reservation)
            }};
        }
        loop {
            let applied_commands = match self
                .drain_pending_object_metadata_commands_for_bucket_collect(pg_id, &bucket)
            {
                Ok(applied_commands) => applied_commands,
                Err(error) => {
                    release_caller_bucket_write_proof!()?;
                    return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                        error,
                    ));
                }
            };
            let upload = match multipart_lookup_route.load_in_progress_multipart_upload(&upload_id)
            {
                Ok(upload) => upload,
                Err(error) => {
                    release_caller_bucket_write_proof!()?;
                    return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                        error,
                    ));
                }
            };
            let (authorized_upload, result) = match action(&upload) {
                Ok((authorized_upload, result)) => (authorized_upload, result),
                Err(error) => {
                    release_caller_bucket_write_proof!()?;
                    return Ok(Err(error));
                }
            };
            if authorized_upload.record() != &upload {
                release_caller_bucket_write_proof!()?;
                return Err(BucketSnapshotLoadError::Metadata(
                    MetadataError::NoSuchUpload {
                        upload_id: upload_id.to_string(),
                    },
                ));
            }
            let create = CreateStreamUploadReq {
                session_id: session_id.clone(),
                bucket: bucket.clone(),
                key: key.clone(),
                target: StreamUploadTarget::UploadPart {
                    upload_id: upload_id.clone(),
                    part_number,
                },
                encryption: upload.encryption.clone(),
            };
            match mutation_client.matching_stream_upload_exists(
                object_pg_id,
                &create,
                super::applied_stream_create_command(&applied_commands, &create),
            ) {
                Ok(true) => {
                    release_caller_bucket_write_proof!()?;
                    return Ok(Ok(result));
                }
                Ok(false) => {}
                Err(error) => {
                    release_caller_bucket_write_proof!()?;
                    return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                        error,
                    ));
                }
            }
            let command = match mutation_client.build_create_stream_upload_command(
                BuildCreateStreamUploadCommandReq {
                    pg_id: object_pg_id,
                    cluster_epoch: self.operation_epoch(),
                    request: &create,
                    cleanup_after,
                    precondition: CreateStreamUploadPrecondition::UploadPart {
                        expected_upload: &upload,
                    },
                    bucket_write_reservation: &bucket_write_reservation,
                },
            ) {
                Ok(command) => command,
                Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                })) => {
                    if let Err(error) =
                        self.drain_pending_object_metadata_commands_for_bucket(pg_id, &bucket)
                    {
                        release_caller_bucket_write_proof!()?;
                        return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                            error,
                        ));
                    }
                    continue;
                }
                Err(error) => {
                    release_caller_bucket_write_proof!()?;
                    return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                        error,
                    ));
                }
            };
            let install_result = self.install_snapshot_sensitive_metadata_command_or_drain(
                pg_id, &bucket, &command, None,
            );
            match install_result {
                Ok(super::SnapshotSensitiveCommandInstall::Installed) => {}
                Ok(super::SnapshotSensitiveCommandInstall::ContenderDrained) => continue,
                Err(error) => {
                    release_caller_bucket_write_proof!()?;
                    return Err(super::object_pg_action_error_to_bucket_snapshot_error(
                        error,
                    ));
                }
            }
            self.apply_new_object_metadata_command_for_bucket(pg_id, &bucket, &command)
                .map_err(super::object_pg_action_error_to_bucket_snapshot_error)?;
            return Ok(Ok(result));
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn create_upload_part_stream_session(
        &self,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        part_number: u32,
        session_id: &SessionId,
    ) -> Result<SessionId, ObjectPgActionError> {
        let bucket = &authorized_upload.record().bucket;
        let key = &authorized_upload.record().key;
        self.create_upload_part_stream_session_with_route_validation(
            super::MultipartObjectMutationEffectRoute {
                pg_id: self.object_metadata_pg(bucket, key),
                bucket,
                key,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            authorized_upload,
            part_number,
            session_id,
            None,
            || Ok(()),
        )
    }

    pub(super) fn create_upload_part_stream_session_with_route_validation(
        &self,
        route: super::MultipartObjectMutationEffectRoute<'_>,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        part_number: u32,
        session_id: &SessionId,
        cleanup_after: Option<u64>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<SessionId, ObjectPgActionError> {
        crate::metadata_command::metadata_command_publisher!(CreateUploadPartStreamSession);
        let super::MultipartObjectMutationEffectRoute {
            pg_id: object_pg_id,
            bucket,
            key,
            effect_fence,
        } = route;
        if authorized_upload.record().bucket != *bucket || authorized_upload.record().key != *key {
            return Err(ObjectPgActionError::Store(
                StoreError::RouteCapabilitySubjectMismatch {
                    operation: "create UploadPart stream session",
                },
            ));
        }
        let upload_id = &authorized_upload.record().upload_id;
        let pg_id = object_pg_id.pg_id();
        let mutation_client = self.object_mutation_metadata_primary_client(bucket, key)?;
        let multipart_lookup_route = mutation_client.open_multipart_upload_lookup_metadata_route(
            self.operation_epoch(),
            object_pg_id,
            bucket,
            key,
        )?;
        loop {
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            let reservation = match self.acquire_durable_bucket_write_reservation_with_effect_fence(
                bucket,
                UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
                Some(key.as_str()),
                Some(effect_fence),
            ) {
                Ok(reservation) => reservation,
                Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteDraining)) => {
                    self.wait_for_durable_bucket_write_drain(bucket)
                        .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
                    continue;
                }
                Err(error) => {
                    return Err(super::bucket_snapshot_error_to_object_pg_action_error(
                        error,
                    ))
                }
            };
            let bucket_write_reservation = BucketWriteReservationProof::from(&reservation.record);
            macro_rules! release_caller_bucket_write_proof {
                () => {{
                    self.release_bucket_write_reservation_proof(&bucket_write_reservation)
                        .map_err(super::bucket_snapshot_error_to_object_pg_action_error)
                }};
            }
            if let Err(error) = require_valid_route() {
                release_caller_bucket_write_proof!()?;
                return Err(ObjectPgActionError::Store(error));
            }
            let applied_commands = match self
                .drain_pending_object_metadata_commands_for_bucket_collect(pg_id, bucket)
            {
                Ok(applied_commands) => applied_commands,
                Err(error) => {
                    release_caller_bucket_write_proof!()?;
                    return Err(error);
                }
            };
            if let Err(error) = require_valid_route() {
                release_caller_bucket_write_proof!()?;
                return Err(ObjectPgActionError::Store(error));
            }
            let upload = match multipart_lookup_route.load_in_progress_multipart_upload(upload_id) {
                Ok(upload) => upload,
                Err(error) => {
                    release_caller_bucket_write_proof!()?;
                    return Err(error);
                }
            };
            if upload != *authorized_upload.record() {
                release_caller_bucket_write_proof!()?;
                return Err(MetadataError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                }
                .into());
            }
            let create = CreateStreamUploadReq {
                session_id: session_id.clone(),
                bucket: bucket.clone(),
                key: key.clone(),
                target: StreamUploadTarget::UploadPart {
                    upload_id: upload_id.clone(),
                    part_number,
                },
                encryption: upload.encryption.clone(),
            };
            if let Err(error) = require_valid_route() {
                release_caller_bucket_write_proof!()?;
                return Err(ObjectPgActionError::Store(error));
            }
            match mutation_client.matching_stream_upload_exists(
                object_pg_id,
                &create,
                super::applied_stream_create_command(&applied_commands, &create),
            ) {
                Ok(true) => {
                    release_caller_bucket_write_proof!()?;
                    return Ok(session_id.clone());
                }
                Ok(false) => {}
                Err(error) => {
                    release_caller_bucket_write_proof!()?;
                    return Err(error);
                }
            }
            if let Err(error) = require_valid_route() {
                release_caller_bucket_write_proof!()?;
                return Err(ObjectPgActionError::Store(error));
            }
            let command = match mutation_client.build_create_stream_upload_command(
                BuildCreateStreamUploadCommandReq {
                    pg_id: object_pg_id,
                    cluster_epoch: self.operation_epoch(),
                    request: &create,
                    cleanup_after,
                    precondition: CreateStreamUploadPrecondition::UploadPart {
                        expected_upload: &upload,
                    },
                    bucket_write_reservation: &bucket_write_reservation,
                },
            ) {
                Ok(command) => command,
                Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                })) => {
                    if let Err(error) =
                        self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)
                    {
                        release_caller_bucket_write_proof!()?;
                        return Err(error);
                    }
                    release_caller_bucket_write_proof!()?;
                    continue;
                }
                Err(error) => {
                    release_caller_bucket_write_proof!()?;
                    return Err(error);
                }
            };
            if let Err(error) = require_valid_route() {
                release_caller_bucket_write_proof!()?;
                return Err(ObjectPgActionError::Store(error));
            }
            match self.install_snapshot_sensitive_metadata_command_or_drain(
                pg_id,
                bucket,
                &command,
                Some(effect_fence),
            ) {
                Ok(super::SnapshotSensitiveCommandInstall::Installed) => {}
                Ok(super::SnapshotSensitiveCommandInstall::ContenderDrained) => {
                    release_caller_bucket_write_proof!()?;
                    continue;
                }
                Err(error) => {
                    release_caller_bucket_write_proof!()?;
                    return Err(error);
                }
            }
            self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
            return Ok(session_id.clone());
        }
    }

    pub fn load_in_progress_multipart_upload(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError> {
        self.load_in_progress_multipart_upload_with_route_validation(
            super::MultipartObjectMutationEffectRoute {
                pg_id: self.object_metadata_pg(bucket, key),
                bucket,
                key,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            upload_id,
            || Ok(()),
        )
    }

    pub(super) fn load_in_progress_multipart_upload_with_route_validation(
        &self,
        route: super::MultipartObjectMutationEffectRoute<'_>,
        upload_id: &UploadId,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError> {
        let super::MultipartObjectMutationEffectRoute {
            pg_id,
            bucket,
            key,
            effect_fence: _,
        } = route;
        require_valid_route().map_err(ObjectPgActionError::Store)?;
        let mutation_client = self.object_mutation_metadata_primary_client(bucket, key)?;
        mutation_client
            .open_multipart_upload_lookup_metadata_route(
                self.operation_epoch(),
                pg_id,
                bucket,
                key,
            )?
            .load_in_progress_multipart_upload(upload_id)
    }

    #[cfg(feature = "test-hooks")]
    pub fn try_load_in_progress_multipart_upload(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<Option<MultipartUploadRecord>, ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        self.local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?
            .test_node()
            .try_load_in_progress_multipart_upload(bucket, key, upload_id)
    }

    pub fn load_in_progress_multipart_upload_for_listing(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError> {
        let pg_id = self.object_metadata_pg(bucket, key);
        let mutation_client = self.object_mutation_metadata_primary_client(bucket, key)?;
        mutation_client
            .open_multipart_upload_lookup_metadata_route(
                self.operation_epoch(),
                pg_id,
                bucket,
                key,
            )?
            .load_in_progress_multipart_upload_for_listing(upload_id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn load_multipart_completion_snapshot(
        &self,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        requested_part_numbers: &[u32],
    ) -> Result<MultipartCompletionSnapshot, ObjectPgActionError> {
        let bucket = &authorized_upload.record().bucket;
        let key = &authorized_upload.record().key;
        self.load_multipart_completion_snapshot_with_route_validation(
            super::MultipartObjectMutationEffectRoute {
                pg_id: self.object_metadata_pg(bucket, key),
                bucket,
                key,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            authorized_upload,
            requested_part_numbers,
            || Ok(()),
        )
    }

    pub(super) fn load_multipart_completion_snapshot_with_route_validation(
        &self,
        route: super::MultipartObjectMutationEffectRoute<'_>,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        requested_part_numbers: &[u32],
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<MultipartCompletionSnapshot, ObjectPgActionError> {
        let super::MultipartObjectMutationEffectRoute {
            pg_id,
            bucket,
            key,
            effect_fence: _,
        } = route;
        if authorized_upload.record().bucket != *bucket || authorized_upload.record().key != *key {
            return Err(ObjectPgActionError::Store(
                StoreError::RouteCapabilitySubjectMismatch {
                    operation: "load multipart completion snapshot",
                },
            ));
        }
        require_valid_route().map_err(ObjectPgActionError::Store)?;
        self.object_mutation_metadata_primary_client(bucket, key)?
            .open_authorized_multipart_upload_metadata_route(
                self.operation_epoch(),
                pg_id,
                authorized_upload,
            )?
            .load_multipart_completion_snapshot(requested_part_numbers)
    }

    pub fn load_multipart_completion_preflight(
        &self,
        authorized_upload: &AuthorizedMultipartUploadRecord,
    ) -> Result<MultipartCompletionPreflight, ObjectPgActionError> {
        let bucket = &authorized_upload.record().bucket;
        let key = &authorized_upload.record().key;
        let pg_id = self.object_metadata_pg(bucket, key);
        self.object_mutation_metadata_primary_client(bucket, key)?
            .open_authorized_multipart_upload_metadata_route(
                self.operation_epoch(),
                pg_id,
                authorized_upload,
            )?
            .load_multipart_completion_preflight()
    }

    fn complete_multipart_outcome_from_command(
        command: &CommitMultipartObjectCommand,
    ) -> CompleteMultipartCommitOutcome {
        CompleteMultipartCommitOutcome {
            version_id: command.object.version_id,
            stale_payload: command
                .stale_payload
                .as_ref()
                .map(Self::completed_multipart_stale_payload_from_reclaim_command),
            live_tags: command.object.tags.clone(),
            live_size: command.object.size,
            live_last_modified: command.last_modified_millis,
        }
    }

    fn completed_multipart_stale_payload_from_reclaim_command(
        command: &ObjectPayloadReclaimCommand,
    ) -> CompletedMultipartStalePayload {
        match command {
            ObjectPayloadReclaimCommand::Segments(reclaim) => {
                CompletedMultipartStalePayload::Segments {
                    generation_id: reclaim.generation_id,
                    segments: Vec::new(),
                }
            }
            ObjectPayloadReclaimCommand::Multipart(reclaim) => {
                CompletedMultipartStalePayload::Multipart {
                    generation_id: reclaim.generation_id,
                    parts: Vec::new(),
                    streaming_segments: Vec::new(),
                }
            }
        }
    }

    pub(super) fn complete_multipart_command_cleanup(
        command: &CommitMultipartObjectCommand,
    ) -> CompleteMultipartCommitCleanup {
        CompleteMultipartCommitCleanup {
            omitted_parts: command.omitted_parts.clone(),
            omitted_streaming_segments: command.omitted_streaming_segments.clone(),
            stream_uploads: command.stream_uploads.clone(),
            stream_upload_segments: command.stream_upload_segments.clone(),
        }
    }

    /// Replicate the bucket-write dependency before publishing completion on the object PG.
    ///
    /// The returned sequence is only an idempotence token for the bucket-PG command; replay
    /// semantics are stored with the completed object version. A barrier already pending on
    /// entry is drained as contention and never satisfies the current reservation, because the
    /// command intentionally carries no reservation identity.
    fn establish_multipart_completion_barrier(
        &self,
        bucket: &BucketName,
        completion_target_context: &str,
        bucket_write_reservation: &BucketWriteReservationProof,
        effect_fence: Option<AdmittedRouteEffectFence>,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        work_budget: &mut super::RequestWorkBudget,
    ) -> Result<u64, ObjectPgActionError> {
        crate::metadata_command::metadata_command_publisher!(EstablishMultipartCompletionBarrier);
        let pg_id = PgId::new(self.bucket_metadata_pg_id(bucket));
        let bucket_metadata_client = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?
            .bucket_metadata_client()
            .clone();
        loop {
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            work_budget.check("multipart completion barrier reservation budget exhausted")?;
            if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                if self
                    .drain_unrelated_pending_metadata_command_for_bucket_with_work_budget(
                        pg_id,
                        bucket,
                        &command,
                        work_budget,
                    )
                    .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?
                {
                    continue;
                }
                if let MetadataCommandPayload::AdvanceMultipartCompletionBarrier(_) =
                    command.payload()
                {
                    match self
                        .drain_bucket_pg_pending_metadata_command_with_work_budget(
                            pg_id,
                            &command,
                            false,
                            work_budget,
                        )
                        .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?
                    {
                        super::PendingMetadataCommandOutcome::Applied => continue,
                        super::PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                            return Err(super::conflicting_pending_object_metadata_command(
                                "retryable partial pending multipart completion barrier command",
                            ));
                        }
                        super::PendingMetadataCommandOutcome::Abandoned => continue,
                    }
                }
                match self.finish_pending_command_for_multipart_completion_barrier(
                    pg_id,
                    &command,
                    work_budget,
                )? {
                    super::PendingMetadataCommandOutcome::Applied => continue,
                    super::PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                        return Err(super::conflicting_pending_object_metadata_command(
                            "retryable partial pending multipart completion barrier dependency command",
                        ));
                    }
                    super::PendingMetadataCommandOutcome::Abandoned => continue,
                }
            }

            #[cfg(test)]
            maybe_run_before_multipart_completion_barrier_command_id_hook(
                self.metadata_command_apply_test_hook_scope_id(),
            );
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            let Some(command_id) = self
                .next_completion_bucket_metadata_command_id_or_drain_with_work_budget(
                    pg_id,
                    bucket,
                    work_budget,
                )
                .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?
            else {
                continue;
            };
            let (barrier_sequence, command) = bucket_metadata_client
                .build_advance_multipart_completion_barrier_command(
                    self.validated_bucket_metadata_pg(pg_id),
                    bucket,
                    command_id,
                    completion_target_context,
                    bucket_write_reservation,
                )
                .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
            if !self
                .try_set_bucket_pg_pending_command_or_retry_with_work_budget_and_effect_fence(
                    pg_id,
                    bucket,
                    &command,
                    effect_fence,
                    work_budget,
                )
                .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?
            {
                continue;
            }
            match self.finish_pending_metadata_command_to_acting_set_with_work_budget(
                pg_id,
                &command,
                true,
                work_budget,
            ) {
                Ok(super::PendingMetadataCommandOutcome::Applied) => return Ok(barrier_sequence),
                Ok(
                    super::PendingMetadataCommandOutcome::Abandoned
                    | super::PendingMetadataCommandOutcome::RetryPartialExactConflict,
                ) => continue,
                Err(error) => {
                    return Err(super::bucket_snapshot_error_to_object_pg_action_error(
                        error,
                    ))
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn test_establish_multipart_completion_barrier(
        &self,
        bucket: &BucketName,
    ) -> Result<u64, ObjectPgActionError> {
        let mut work_budget = super::RequestWorkBudget::new(
            std::time::Duration::from_millis(METADATA_COMMAND_APPLY_RETRY_BUDGET_MILLIS),
            None,
        )
        .for_operation("test_multipart_completion_barrier")
        .for_pg(PgId::new(self.bucket_metadata_pg_id(bucket)));
        let reservation = self
            .acquire_completion_durable_bucket_write_reservation(
                bucket,
                COMPLETE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
                Some("test-completed-multipart-order"),
            )
            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
        let proof = BucketWriteReservationProof::from(&reservation.record);
        let result = self.establish_multipart_completion_barrier(
            bucket,
            "test-completed-multipart-order",
            &proof,
            None,
            || Ok(()),
            &mut work_budget,
        );
        let release = self
            .release_durable_bucket_write_reservation(reservation)
            .map_err(super::bucket_snapshot_error_to_object_pg_action_error);
        match (result, release) {
            (Ok(order), Ok(())) => Ok(order),
            (Ok(_), Err(error)) | (Err(error), Ok(())) | (Err(error), Err(_)) => Err(error),
        }
    }

    fn apply_multipart_completion_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), ObjectPgActionError> {
        let mut command = command.clone();
        loop {
            match self.apply_metadata_command_to_acting_set(&command) {
                Ok(()) => {
                    self.release_metadata_command_bucket_write_reservation(&command)
                        .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
                    self.remove_pending_metadata_command_for_bucket(pg_id, bucket, &command)
                        .map_err(ObjectPgActionError::from)?;
                    self.after_object_metadata_command_applied(&command);
                    return Ok(());
                }
                Err(error)
                    if matches!(
                        self.retryable_partial_exact_metadata_command_conflict_applied_on_all_nodes(
                            pg_id,
                            &command,
                            error.applied_nodes,
                            &error.source,
                        )
                        .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?,
                        Some(true)
                    ) =>
                {
                    self.release_metadata_command_bucket_write_reservation(&command)
                        .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
                    self.remove_pending_metadata_command_for_bucket(pg_id, bucket, &command)
                        .map_err(ObjectPgActionError::from)?;
                    self.after_object_metadata_command_applied(&command);
                    return Ok(());
                }
                Err(error)
                    if matches!(
                        self.retryable_partial_exact_metadata_command_conflict_applied_on_all_nodes(
                            pg_id,
                            &command,
                            error.applied_nodes,
                            &error.source,
                        )
                        .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?,
                        Some(false)
                    ) =>
                {
                    return Err(super::conflicting_pending_object_metadata_command(
                        "retryable partial multipart completion command conflict",
                    ));
                }
                Err(error)
                    if error.applied_nodes == 0
                        && super::StorageCluster::metadata_command_log_conflict_matches(
                            &command,
                            &error.source,
                        ) =>
                {
                    let Some(reissued) = self
                        .reissue_pending_metadata_command(pg_id, &command)
                        .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?
                    else {
                        return Err(super::conflicting_pending_object_metadata_command(
                            "pending multipart completion command was displaced during reissue",
                        ));
                    };
                    command = reissued;
                }
                Err(error) => {
                    return Err(super::bucket_snapshot_error_to_object_pg_action_error(
                        error.source,
                    ))
                }
            }
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn complete_multipart_upload_commit_serialized(
        &self,
        req: CompleteMultipartCommitRequest,
    ) -> Result<CompleteMultipartCommitOutcome, ObjectPgActionError> {
        let bucket = req.bucket.clone();
        let key = req.key.clone();
        self.complete_multipart_upload_commit_serialized_with_route_validation(
            super::MultipartObjectMutationEffectRoute {
                pg_id: self.object_metadata_pg(&bucket, &key),
                bucket: &bucket,
                key: &key,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            req,
            || Ok(()),
        )
    }

    pub(super) fn complete_multipart_upload_commit_serialized_with_route_validation(
        &self,
        route: super::MultipartObjectMutationEffectRoute<'_>,
        mut req: CompleteMultipartCommitRequest,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<CompleteMultipartCommitOutcome, ObjectPgActionError> {
        crate::metadata_command::metadata_command_publisher!(
            CompleteMultipartUploadCommitSerialized
        );
        let super::MultipartObjectMutationEffectRoute {
            pg_id: object_pg_id,
            bucket: route_bucket,
            key: route_key,
            effect_fence,
        } = route;
        if req.bucket != *route_bucket || req.key != *route_key {
            return Err(ObjectPgActionError::Store(
                StoreError::RouteCapabilitySubjectMismatch {
                    operation: "complete multipart upload",
                },
            ));
        }
        let bucket = req.bucket.clone();
        let key = req.key.clone();
        let upload_id = req.upload_id.clone();
        let generation_id = req.generation_id;
        let pg_id = object_pg_id.pg_id();
        let mutation_client = self.object_mutation_metadata_primary_client(&bucket, &key)?;
        let multipart_lookup_route = mutation_client.open_multipart_upload_lookup_metadata_route(
            self.operation_epoch(),
            object_pg_id,
            &bucket,
            &key,
        )?;
        let multipart_completion_route = mutation_client
            .open_multipart_completion_mutation_metadata_route(
                self.operation_epoch(),
                object_pg_id,
                &bucket,
                &key,
            )?;
        let mut work_budget = super::RequestWorkBudget::new(
            std::time::Duration::from_millis(METADATA_COMMAND_APPLY_RETRY_BUDGET_MILLIS),
            None,
        )
        .for_operation("complete_multipart_upload_commit")
        .for_pg(pg_id);

        'retry_after_pending_conflict: loop {
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            work_budget.check("complete multipart commit budget exhausted")?;
            let reservation = match self
                .acquire_completion_durable_bucket_write_reservation_with_effect_fence(
                    &bucket,
                    COMPLETE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
                    Some(key.as_str()),
                    effect_fence,
                ) {
                Ok(reservation) => reservation,
                Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteDraining)) => {
                    self.wait_for_durable_bucket_write_drain(&bucket)
                        .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
                    continue;
                }
                Err(error) => {
                    return Err(super::bucket_snapshot_error_to_object_pg_action_error(
                        error,
                    ));
                }
            };
            let bucket_write_reservation = BucketWriteReservationProof::from(&reservation.record);
            macro_rules! release_bucket_write_proof {
                () => {{
                    self.release_bucket_write_reservation_proof(&bucket_write_reservation)
                        .map_err(super::bucket_snapshot_error_to_object_pg_action_error)
                }};
            }

            loop {
                if let Err(error) = require_valid_route() {
                    release_bucket_write_proof!()?;
                    return Err(ObjectPgActionError::Store(error));
                }
                let command = match self.pending_metadata_command_for_bucket(pg_id, &bucket) {
                    Ok(Some(command)) => command,
                    Ok(None) => break,
                    Err(error) => {
                        release_bucket_write_proof!()?;
                        return Err(error.into());
                    }
                };
                if let MetadataCommandPayload::CommitMultipartObject(commit) = command.payload() {
                    if commit.matches_request(
                        &bucket,
                        &key,
                        &upload_id,
                        generation_id,
                        req.completion_fingerprint,
                        &req.part_records,
                    ) {
                        let outcome = Self::complete_multipart_outcome_from_command(commit);
                        release_bucket_write_proof!()?;
                        self.apply_multipart_completion_command(pg_id, &bucket, &command)?;
                        return Ok(outcome);
                    }
                }
                if let Err(error) = self.drain_pending_object_metadata_command(pg_id, &command) {
                    release_bucket_write_proof!()?;
                    return Err(error);
                }
            }

            if req.part_records.is_empty() {
                release_bucket_write_proof!()?;
                return Err(MetadataError::InvariantViolation {
                    context: "complete multipart command empty parts",
                    reason: "multipart completion requires at least one part".into(),
                }
                .into());
            }

            if req.conditional_completion {
                if let Err(error) = require_valid_route() {
                    release_bucket_write_proof!()?;
                    return Err(ObjectPgActionError::Store(error));
                }
                let upload =
                    match multipart_lookup_route.load_in_progress_multipart_upload(&upload_id) {
                        Ok(upload) => upload,
                        Err(error) => {
                            release_bucket_write_proof!()?;
                            return Err(error);
                        }
                    };
                if req.expected_current_object_identity != upload.initiated_object_identity {
                    release_bucket_write_proof!()?;
                    return Err(ObjectPgActionError::MultipartConditionalRequestConflict);
                }
            }

            if req.versioning != BucketVersioningState::Enabled {
                if let Err(error) = require_valid_route() {
                    release_bucket_write_proof!()?;
                    return Err(ObjectPgActionError::Store(error));
                }
                #[cfg(test)]
                maybe_run_multipart_completion_stale_retry_hook(
                    self.metadata_command_apply_test_hook_scope_id(),
                    MultipartCompletionStaleRetryTestEvent::BeforeStalePayloadSourceLoad,
                    &upload_id,
                );
                match multipart_completion_route.load_stale_payload_source() {
                    Ok(current_stale_payload_source) => {
                        req.expected_stale_payload_source = current_stale_payload_source;
                    }
                    Err(error) => {
                        release_bucket_write_proof!()?;
                        return Err(error);
                    }
                }
            }

            let version_id = if req.versioning == BucketVersioningState::Enabled {
                match self.reserve_next_object_version_for_completion_with_effect_fence(
                    pg_id,
                    &bucket,
                    &key,
                    effect_fence,
                    &mut require_valid_route,
                ) {
                    Ok(version_id) => version_id,
                    Err(error) => {
                        release_bucket_write_proof!()?;
                        return Err(error);
                    }
                }
            } else {
                VersionId::Null
            };
            // Replicate the bucket-write dependency before the object-PG commit. This advances
            // one fixed-size scalar and never allocates or retains per-upload terminal records.
            if let Err(error) = self.establish_multipart_completion_barrier(
                &bucket,
                key.as_str(),
                &bucket_write_reservation,
                Some(effect_fence),
                &mut require_valid_route,
                &mut work_budget,
            ) {
                release_bucket_write_proof!()?;
                return Err(error);
            }
            let expected_object_parts = complete_multipart_expected_object_parts(
                &req,
                version_id,
                self.local_map.pg_topology(),
            );
            if let Err(error) = require_valid_route() {
                release_bucket_write_proof!()?;
                return Err(ObjectPgActionError::Store(error));
            }
            let command = match multipart_completion_route.build_complete_multipart_object_command(
                BuildCompleteMultipartObjectCommandReq {
                    request: &req,
                    version_id,
                    expected_object_parts: &expected_object_parts,
                    bucket_write_reservation: &bucket_write_reservation,
                },
            ) {
                Ok(command) => command,
                Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                })) => {
                    if let Err(error) =
                        self.drain_pending_object_metadata_commands_for_bucket(pg_id, &bucket)
                    {
                        release_bucket_write_proof!()?;
                        return Err(error);
                    }
                    release_bucket_write_proof!()?;
                    continue 'retry_after_pending_conflict;
                }
                Err(ObjectPgActionError::StaleMultipartCompletionSnapshot)
                    if version_id.is_null() =>
                {
                    #[cfg(test)]
                    maybe_run_multipart_completion_stale_retry_hook(
                        self.metadata_command_apply_test_hook_scope_id(),
                        MultipartCompletionStaleRetryTestEvent::AfterStaleCommandBuild,
                        &upload_id,
                    );
                    if let Err(error) = require_valid_route() {
                        release_bucket_write_proof!()?;
                        return Err(ObjectPgActionError::Store(error));
                    }
                    #[cfg(test)]
                    maybe_run_multipart_completion_stale_retry_hook(
                        self.metadata_command_apply_test_hook_scope_id(),
                        MultipartCompletionStaleRetryTestEvent::BeforeStalePayloadSourceLoad,
                        &upload_id,
                    );
                    let current_stale_payload_source =
                        match multipart_completion_route.load_stale_payload_source() {
                            Ok(source) => source,
                            Err(error) => {
                                release_bucket_write_proof!()?;
                                return Err(error);
                            }
                        };
                    release_bucket_write_proof!()?;
                    if current_stale_payload_source == req.expected_stale_payload_source {
                        return Err(ObjectPgActionError::StaleMultipartCompletionSnapshot);
                    }
                    req.expected_stale_payload_source = current_stale_payload_source;
                    continue 'retry_after_pending_conflict;
                }
                Err(error) => {
                    release_bucket_write_proof!()?;
                    return Err(error);
                }
            };
            // A matching completion contender carries the exact outcome this caller must return.
            // Let the retry loop observe it instead of draining it generically and losing that
            // request-shaped result.
            let installed = match self
                .try_install_pending_metadata_command_for_bucket_with_effect_fence(
                    pg_id,
                    &bucket,
                    &command,
                    Some(effect_fence),
                ) {
                Ok(installed) => installed,
                Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                })) => {
                    if let Err(error) =
                        self.drain_pending_object_metadata_commands_for_bucket(pg_id, &bucket)
                    {
                        release_bucket_write_proof!()?;
                        return Err(error);
                    }
                    release_bucket_write_proof!()?;
                    continue 'retry_after_pending_conflict;
                }
                Err(error) => {
                    release_bucket_write_proof!()?;
                    return Err(error);
                }
            };
            if !installed {
                let pending_owns_proof =
                    match self.pending_metadata_command_for_bucket(pg_id, &bucket) {
                        Ok(Some(pending)) => matches!(
                            pending.payload(),
                            MetadataCommandPayload::CommitMultipartObject(commit)
                                if commit.matches_request(
                                    &bucket,
                                    &key,
                                    &upload_id,
                                    generation_id,
                                    req.completion_fingerprint,
                                    &req.part_records,
                                ) && commit.bucket_write_reservation == bucket_write_reservation
                        ),
                        Ok(None) => false,
                        Err(error) => {
                            release_bucket_write_proof!()?;
                            return Err(error.into());
                        }
                    };
                if !pending_owns_proof {
                    release_bucket_write_proof!()?;
                }
                continue 'retry_after_pending_conflict;
            }
            self.apply_multipart_completion_command(pg_id, &bucket, &command)?;

            let MetadataCommandPayload::CommitMultipartObject(commit) = command.payload() else {
                unreachable!("new complete multipart command changed payload kind");
            };
            return Ok(Self::complete_multipart_outcome_from_command(commit));
        }
    }

    fn commit_stream_part_commands_match_retry(
        pending: &CommitStreamPartCommand,
        candidate: &CommitStreamPartCommand,
    ) -> bool {
        let mut adjusted = candidate.clone();
        adjusted.part.last_modified = pending.part.last_modified;
        pending == &adjusted
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn finalize_upload_part_stream<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        session_id: &SessionId,
        part_number: u32,
        action: impl FnMut(StreamUploadPartSnapshot) -> Result<PreparedStreamPartCommit<T>, E>,
    ) -> Result<Result<FinalizeStreamPartOutcome<T>, E>, ObjectPgActionError> {
        self.finalize_upload_part_stream_with_route_validation(
            super::MultipartObjectMutationEffectRoute {
                pg_id: self.object_metadata_pg(bucket, key),
                bucket,
                key,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            upload_id,
            session_id,
            part_number,
            || Ok(()),
            action,
        )
    }

    pub(super) fn finalize_upload_part_stream_with_route_validation<T, E>(
        &self,
        route: super::MultipartObjectMutationEffectRoute<'_>,
        upload_id: &UploadId,
        session_id: &SessionId,
        part_number: u32,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
        mut action: impl FnMut(StreamUploadPartSnapshot) -> Result<PreparedStreamPartCommit<T>, E>,
    ) -> Result<Result<FinalizeStreamPartOutcome<T>, E>, ObjectPgActionError> {
        crate::metadata_command::metadata_command_publisher!(FinalizeUploadPartStream);
        let super::MultipartObjectMutationEffectRoute {
            pg_id: object_pg_id,
            bucket,
            key,
            effect_fence,
        } = route;
        let pg_id = object_pg_id.pg_id();
        let mutation_client = self.object_mutation_metadata_primary_client(bucket, key)?;

        loop {
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            let mut pending_command = None;
            while let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                let is_matching_stream_part_commit = matches!(
                    command.payload(),
                    MetadataCommandPayload::CommitStreamPart(commit)
                        if commit.matches_request(bucket, key, upload_id, session_id, part_number)
                );
                if is_matching_stream_part_commit {
                    pending_command = Some(command);
                    break;
                }
                self.drain_pending_object_metadata_command(pg_id, &command)?;
            }

            let mut bucket_write_proof = None;
            if pending_command.is_none() {
                let reservation = match self
                    .acquire_durable_bucket_write_reservation_with_effect_fence(
                        bucket,
                        UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND,
                        Some(key.as_str()),
                        Some(effect_fence),
                    ) {
                    Ok(reservation) => reservation,
                    Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteDraining)) => {
                        self.wait_for_durable_bucket_write_drain(bucket)
                            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)?;
                        continue;
                    }
                    Err(error) => {
                        return Err(super::bucket_snapshot_error_to_object_pg_action_error(
                            error,
                        ));
                    }
                };
                bucket_write_proof = Some(BucketWriteReservationProof::from(&reservation.record));
            }
            macro_rules! release_bucket_write_proof_if_unowned {
                () => {{
                    if let Some(proof) = &bucket_write_proof {
                        self.release_bucket_write_reservation_proof(proof)
                            .map_err(super::bucket_snapshot_error_to_object_pg_action_error)
                    } else {
                        Ok(())
                    }
                }};
            }

            if let Err(error) = require_valid_route() {
                release_bucket_write_proof_if_unowned!()?;
                return Err(ObjectPgActionError::Store(error));
            }
            let storage_snapshot = match mutation_client.load_stream_part_finalize_snapshot(
                object_pg_id,
                bucket,
                key,
                upload_id,
                session_id,
                part_number,
            ) {
                Ok(snapshot) => snapshot,
                Err(
                    error @ ObjectPgActionError::Metadata(MetadataError::StreamSessionNotFound {
                        ..
                    }),
                ) => {
                    if let Some(command) = pending_command.clone() {
                        self.apply_exact_pending_object_metadata_command(
                            pg_id,
                            super::ExactPendingObjectMetadataCommand::for_checked_request(&command),
                        )?;
                        continue;
                    }
                    release_bucket_write_proof_if_unowned!()?;
                    return Err(error);
                }
                Err(error) => {
                    release_bucket_write_proof_if_unowned!()?;
                    return Err(error);
                }
            };

            let prepared = match action(storage_snapshot.auth_snapshot.clone()) {
                Ok(prepared) => prepared,
                Err(error) => {
                    release_bucket_write_proof_if_unowned!()?;
                    return Ok(Err(error));
                }
            };

            let command_bucket_write_reservation = pending_command
                .as_ref()
                .and_then(|command| match command.payload() {
                    MetadataCommandPayload::CommitStreamPart(commit) => {
                        Some(commit.bucket_write_reservation.clone())
                    }
                    _ => None,
                })
                .or_else(|| bucket_write_proof.clone())
                .expect("stream part commit command must carry a bucket-write proof");
            let expected_command_bucket_write_reservation =
                command_bucket_write_reservation.clone();
            let command_payload = CommitStreamPartCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                session_id: session_id.clone(),
                upload: storage_snapshot.auth_snapshot.upload.clone(),
                part: prepared.part.clone(),
                segments: prepared.segments.clone(),
                existing_part: storage_snapshot.existing_part.clone(),
                displaced_segments: storage_snapshot.displaced_segments.clone(),
                bucket_write_reservation: command_bucket_write_reservation,
            };
            let command_is_pending = pending_command.is_some();
            let command = if let Some(command) = pending_command {
                let MetadataCommandPayload::CommitStreamPart(pending) = command.payload() else {
                    unreachable!("filtered pending command changed kind");
                };
                if !Self::commit_stream_part_commands_match_retry(pending, &command_payload) {
                    release_bucket_write_proof_if_unowned!()?;
                    return Err(ObjectPgActionError::InvalidRequest {
                        reason: "pending stream part commit does not match retry".to_string(),
                    });
                }
                command
            } else {
                if let Err(error) = require_valid_route() {
                    release_bucket_write_proof_if_unowned!()?;
                    return Err(ObjectPgActionError::Store(error));
                }
                let command = match mutation_client.build_stream_part_commit_command(
                    BuildStreamPartCommitCommandReq {
                        pg_id: object_pg_id,
                        cluster_epoch: self.operation_epoch(),
                        bucket,
                        key,
                        upload_id,
                        session_id,
                        part_number,
                        expected_snapshot: &storage_snapshot,
                        part: &prepared.part,
                        segments: &prepared.segments,
                        bucket_write_reservation: &expected_command_bucket_write_reservation,
                    },
                ) {
                    Ok(command) => command,
                    Err(ObjectPgActionError::StaleStreamFinalizeSnapshot) => {
                        release_bucket_write_proof_if_unowned!()?;
                        continue;
                    }
                    Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                        ..
                    })) => {
                        if let Err(error) =
                            self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)
                        {
                            release_bucket_write_proof_if_unowned!()?;
                            return Err(error);
                        }
                        release_bucket_write_proof_if_unowned!()?;
                        continue;
                    }
                    Err(error) => {
                        release_bucket_write_proof_if_unowned!()?;
                        return Err(error);
                    }
                };
                if let Err(error) = require_valid_route() {
                    release_bucket_write_proof_if_unowned!()?;
                    return Err(ObjectPgActionError::Store(error));
                }
                let installed = match self
                    .try_install_pending_metadata_command_for_bucket_with_effect_fence(
                        pg_id,
                        bucket,
                        &command,
                        Some(effect_fence),
                    ) {
                    Ok(installed) => installed,
                    Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                        ..
                    })) => {
                        if let Err(error) =
                            self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)
                        {
                            release_bucket_write_proof_if_unowned!()?;
                            return Err(error);
                        }
                        release_bucket_write_proof_if_unowned!()?;
                        continue;
                    }
                    Err(error) => {
                        release_bucket_write_proof_if_unowned!()?;
                        return Err(error);
                    }
                };
                if !installed {
                    let pending_owns_proof = match self
                        .pending_metadata_command_for_bucket(pg_id, bucket)
                    {
                        Ok(Some(pending)) => matches!(
                            pending.payload(),
                            MetadataCommandPayload::CommitStreamPart(commit)
                                if commit.matches_request(bucket, key, upload_id, session_id, part_number)
                                    && commit.bucket_write_reservation == expected_command_bucket_write_reservation
                        ),
                        Ok(None) => false,
                        Err(error) => {
                            release_bucket_write_proof_if_unowned!()?;
                            return Err(error.into());
                        }
                    };
                    if !pending_owns_proof {
                        release_bucket_write_proof_if_unowned!()?;
                    }
                    continue;
                }
                command
            };

            let MetadataCommandPayload::CommitStreamPart(commit) = command.payload() else {
                unreachable!("stream part pending command kind changed");
            };
            let last_modified = commit.part.last_modified;
            if command_is_pending {
                self.apply_exact_pending_object_metadata_command(
                    pg_id,
                    super::ExactPendingObjectMetadataCommand::for_checked_request(&command),
                )?;
            } else {
                self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
            }
            return Ok(Ok(FinalizeStreamPartOutcome {
                value: prepared.value,
                last_modified,
            }));
        }
    }

    pub(super) fn list_multipart_uploads_for_bucket_with_route_validation(
        &self,
        scan: &super::ObjectMetadataScanRoute<'_>,
        prefix: Option<&ObjectKey>,
        delimiter: Option<&str>,
        key_marker: Option<&ObjectKey>,
        upload_id_marker: Option<&UploadId>,
        max_uploads: u32,
    ) -> Result<ListedBucketMultipartUploads, ObjectPgActionError> {
        let bucket = scan.bucket;
        if max_uploads == 0 {
            return Ok(ListedBucketMultipartUploads {
                uploads: Vec::new(),
                common_prefixes: Vec::new(),
                is_truncated: false,
                next_marker: None,
            });
        }

        let fetch_limit = max_uploads.saturating_add(1);
        let prefix = prefix.cloned();
        let key_marker = key_marker.cloned();
        let upload_id_marker = upload_id_marker.cloned();
        let delimiter = delimiter.filter(|delimiter| !delimiter.is_empty());

        if delimiter.is_none() {
            let max = max_uploads as usize;
            let mut smallest = BoundedSmallestRecords::new(max.saturating_add(1));
            for pg_id in self.metadata_pg_ids() {
                scan.require_valid().map_err(ObjectPgActionError::Store)?;
                let resp = self.list_multipart_uploads_page(
                    pg_id,
                    &ListMultipartUploadsReq {
                        bucket: bucket.clone(),
                        prefix: prefix.clone(),
                        page_start: key_marker.clone().map(|key_marker| {
                            ListMultipartUploadsPageStart::After {
                                key_marker,
                                upload_id_marker: upload_id_marker.clone(),
                            }
                        }),
                        max_uploads: fetch_limit,
                    },
                )?;
                #[cfg(any(test, feature = "test-hooks"))]
                self.maybe_run_after_metadata_listing_pg_complete_hook(pg_id);
                for upload in resp.uploads {
                    let order = (
                        upload.key.clone(),
                        multipart_upload_listing_position(&upload),
                        upload.upload_id.clone(),
                    );
                    smallest.insert(order, upload);
                }
            }
            let mut uploads = smallest.into_values();
            let is_truncated = uploads.len() > max;
            uploads.truncate(max);
            let next_marker = uploads
                .last()
                .map(|upload| MultipartUploadListMarker::Upload {
                    key: upload.key.clone(),
                    upload_id: upload.upload_id.clone(),
                });
            return Ok(ListedBucketMultipartUploads {
                uploads,
                common_prefixes: Vec::new(),
                is_truncated,
                next_marker,
            });
        }

        let prefix_str = prefix.as_ref().map_or("", ObjectKey::as_str);
        let delimiter = delimiter.expect("checked above");
        let fetch_uploads_page = |cursor: &mut MultipartUploadCursor,
                                  start: Option<ListMultipartUploadsPageStart>|
         -> Result<(), ObjectPgActionError> {
            scan.require_valid().map_err(ObjectPgActionError::Store)?;
            let resp = self.list_multipart_uploads_page(
                cursor.pg_id,
                &ListMultipartUploadsReq {
                    bucket: bucket.clone(),
                    prefix: prefix.clone(),
                    page_start: start,
                    max_uploads: fetch_limit,
                },
            )?;
            #[cfg(any(test, feature = "test-hooks"))]
            self.maybe_run_after_metadata_listing_pg_complete_hook(cursor.pg_id);
            cursor.uploads = resp.uploads;
            cursor.next_index = 0;
            cursor.next_page_start = if resp.is_truncated {
                resp.next_key_marker
                    .map(|key_marker| ListMultipartUploadsPageStart::After {
                        key_marker,
                        upload_id_marker: resp.next_upload_id_marker,
                    })
            } else {
                None
            };
            Ok(())
        };

        let refill_cursor =
            |cursor: &mut MultipartUploadCursor| -> Result<(), ObjectPgActionError> {
                while cursor.current().is_none() {
                    let Some(next_start) = cursor.next_page_start.clone() else {
                        break;
                    };
                    fetch_uploads_page(cursor, Some(next_start))?;
                }
                Ok(())
            };

        let jump_cursor_to = |cursor: &mut MultipartUploadCursor,
                              start: ListMultipartUploadsPageStart|
         -> Result<(), ObjectPgActionError> {
            cursor.uploads.clear();
            cursor.next_index = 0;
            cursor.next_page_start = Some(start);
            refill_cursor(cursor)
        };

        let skip_cursor_prefix = |cursor: &mut MultipartUploadCursor,
                                  common_prefix: &str|
         -> Result<(), ObjectPgActionError> {
            while cursor
                .current()
                .is_some_and(|upload| upload.key.as_str().starts_with(common_prefix))
            {
                cursor.next_index += 1;
                refill_cursor(cursor)?;
            }
            Ok(())
        };

        let initial_start =
            key_marker
                .clone()
                .map(|key_marker| ListMultipartUploadsPageStart::After {
                    key_marker,
                    upload_id_marker,
                });
        let mut cursors = Vec::new();
        for pg_id in self.metadata_pg_ids() {
            let mut cursor = MultipartUploadCursor {
                pg_id,
                uploads: Vec::new(),
                next_index: 0,
                next_page_start: None,
            };
            fetch_uploads_page(&mut cursor, initial_start.clone())?;
            cursors.push(cursor);
        }

        let max = max_uploads as usize;
        let mut uploads = Vec::new();
        let mut common_prefixes = Vec::new();
        let mut is_truncated = false;
        let mut next_marker = None;
        let mut active_common_prefix = key_marker.as_ref().and_then(|marker| {
            let after_prefix = marker.as_str().strip_prefix(prefix_str)?;
            after_prefix
                .ends_with(delimiter)
                .then(|| (marker.clone(), crate::object_key_prefix_upper_bound(marker)))
        });

        while let Some((cursor_index, _)) = cursors
            .iter()
            .enumerate()
            .filter_map(|(cursor_index, cursor)| {
                cursor.current().map(|upload| (cursor_index, upload))
            })
            .min_by(|(left_index, left), (right_index, right)| {
                left.key
                    .cmp(&right.key)
                    .then_with(|| {
                        multipart_upload_listing_position(left)
                            .cmp(&multipart_upload_listing_position(right))
                    })
                    .then_with(|| left.upload_id.cmp(&right.upload_id))
                    .then_with(|| left_index.cmp(right_index))
            })
        {
            let current_key = cursors[cursor_index]
                .current()
                .expect("selected cursor should have a current upload")
                .key
                .clone();
            if let Some((ref common_prefix, ref upper_bound)) = active_common_prefix {
                if current_key.as_str().starts_with(common_prefix.as_str()) {
                    if let Some(upper_bound) = upper_bound.clone() {
                        jump_cursor_to(
                            &mut cursors[cursor_index],
                            ListMultipartUploadsPageStart::At(upper_bound),
                        )?;
                    } else {
                        skip_cursor_prefix(&mut cursors[cursor_index], common_prefix.as_str())?;
                    }
                    continue;
                }
                active_common_prefix = None;
            }

            if let Some(common_prefix) =
                crate::object_key_common_prefix(&current_key, prefix_str, delimiter)
            {
                let upper_bound = crate::object_key_prefix_upper_bound(&common_prefix);
                active_common_prefix = Some((common_prefix.clone(), upper_bound.clone()));
                if key_marker
                    .as_ref()
                    .is_some_and(|marker| common_prefix.as_str() <= marker.as_str())
                {
                    if let Some(upper_bound) = upper_bound {
                        jump_cursor_to(
                            &mut cursors[cursor_index],
                            ListMultipartUploadsPageStart::At(upper_bound),
                        )?;
                    } else {
                        skip_cursor_prefix(&mut cursors[cursor_index], common_prefix.as_str())?;
                    }
                    continue;
                }
                if uploads.len() + common_prefixes.len() >= max {
                    is_truncated = true;
                    break;
                }
                next_marker = Some(MultipartUploadListMarker::CommonPrefix(
                    common_prefix.clone(),
                ));
                common_prefixes.push(common_prefix);
                continue;
            }

            if uploads.len() + common_prefixes.len() >= max {
                is_truncated = true;
                break;
            }
            let current = cursors[cursor_index]
                .current()
                .expect("selected cursor should have a current upload")
                .clone();
            next_marker = Some(MultipartUploadListMarker::Upload {
                key: current.key.clone(),
                upload_id: current.upload_id.clone(),
            });
            uploads.push(current);
            cursors[cursor_index].next_index += 1;
            refill_cursor(&mut cursors[cursor_index])?;
        }

        Ok(ListedBucketMultipartUploads {
            uploads,
            common_prefixes,
            is_truncated,
            next_marker,
        })
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn list_multipart_uploads_for_bucket(
        &self,
        bucket: &BucketName,
        prefix: Option<&ObjectKey>,
        delimiter: Option<&str>,
        key_marker: Option<&ObjectKey>,
        upload_id_marker: Option<&UploadId>,
        max_uploads: u32,
    ) -> Result<ListedBucketMultipartUploads, ObjectPgActionError> {
        let require_valid_route = || Ok(());
        let scan = super::ObjectMetadataScanRoute {
            bucket,
            require_valid_route: &require_valid_route,
        };
        self.list_multipart_uploads_for_bucket_with_route_validation(
            &scan,
            prefix,
            delimiter,
            key_marker,
            upload_id_marker,
            max_uploads,
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn list_multipart_parts_for_authorized_upload(
        &self,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        part_number_marker: Option<u32>,
        max_parts: u32,
    ) -> Result<ListedMultipartParts, ObjectPgActionError> {
        let bucket = &authorized_upload.record().bucket;
        let key = &authorized_upload.record().key;
        self.list_multipart_parts_for_authorized_upload_with_route_validation(
            super::MultipartObjectMutationEffectRoute {
                pg_id: self.object_metadata_pg(bucket, key),
                bucket,
                key,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            authorized_upload,
            part_number_marker,
            max_parts,
            || Ok(()),
        )
    }

    pub(super) fn list_multipart_parts_for_authorized_upload_with_route_validation(
        &self,
        route: super::MultipartObjectMutationEffectRoute<'_>,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        part_number_marker: Option<u32>,
        max_parts: u32,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<ListedMultipartParts, ObjectPgActionError> {
        let super::MultipartObjectMutationEffectRoute {
            pg_id,
            bucket,
            key,
            effect_fence: _,
        } = route;
        if authorized_upload.record().bucket != *bucket || authorized_upload.record().key != *key {
            return Err(ObjectPgActionError::Store(
                StoreError::RouteCapabilitySubjectMismatch {
                    operation: "list multipart parts",
                },
            ));
        }
        require_valid_route().map_err(ObjectPgActionError::Store)?;
        self.object_mutation_metadata_primary_client(bucket, key)?
            .open_authorized_multipart_upload_metadata_route(
                self.operation_epoch(),
                pg_id,
                authorized_upload,
            )?
            .list_multipart_parts(part_number_marker, max_parts)
    }

    pub fn lookup_multipart_upload_management(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadManagementLookup, ObjectPgActionError> {
        self.lookup_multipart_upload_management_with_route_validation(
            super::MultipartObjectMutationEffectRoute {
                pg_id: self.object_metadata_pg(bucket, key),
                bucket,
                key,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            upload_id,
            || Ok(()),
        )
    }

    pub(super) fn lookup_multipart_upload_management_with_route_validation(
        &self,
        route: super::MultipartObjectMutationEffectRoute<'_>,
        upload_id: &UploadId,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<MultipartUploadManagementLookup, ObjectPgActionError> {
        let super::MultipartObjectMutationEffectRoute {
            pg_id: object_pg_id,
            bucket,
            key,
            effect_fence: _,
        } = route;
        let pg_id = object_pg_id.pg_id();
        require_valid_route().map_err(ObjectPgActionError::Store)?;
        // A concurrent terminal command may have removed the active upload on
        // part of the acting set before its object-scoped completion replay is
        // visible everywhere. Finish the durable command before classifying
        // the upload for CompleteMultipartUpload or AbortMultipartUpload.
        self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
        require_valid_route().map_err(ObjectPgActionError::Store)?;
        let mutation_client = self.object_mutation_metadata_primary_client(bucket, key)?;
        mutation_client
            .open_multipart_upload_lookup_metadata_route(
                self.operation_epoch(),
                object_pg_id,
                bucket,
                key,
            )?
            .lookup_multipart_upload_management(upload_id)
    }

    pub fn abort_multipart_upload(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<bool, ObjectPgActionError> {
        let pg_id = self.object_metadata_pg(bucket, key);
        self.abort_multipart_upload_locked(
            pg_id,
            bucket,
            key,
            upload_id,
            AbortMultipartUploadDrainMode::Wait,
            None,
        )
    }

    pub fn abort_multipart_upload_for_lifecycle_sweep(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        expected_bucket_incarnation_generation: u64,
    ) -> Result<bool, ObjectPgActionError> {
        let pg_id = self.object_metadata_pg(bucket, key);
        self.abort_multipart_upload_locked(
            pg_id,
            bucket,
            key,
            upload_id,
            AbortMultipartUploadDrainMode::Stop,
            Some(expected_bucket_incarnation_generation),
        )
    }

    fn abort_multipart_upload_locked(
        &self,
        object_pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        drain_mode: AbortMultipartUploadDrainMode,
        expected_bucket_incarnation_generation: Option<u64>,
    ) -> Result<bool, ObjectPgActionError> {
        crate::metadata_command::metadata_command_publisher!(AbortMultipartUploadLocked);
        let pg_id = object_pg_id.pg_id();
        'retry_after_pending_conflict: loop {
            while let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                if metadata_command_is_matching_multipart_abort(&command, bucket, key, upload_id) {
                    if expected_bucket_incarnation_generation.is_some_and(|expected| {
                        !metadata_command_matches_bucket_incarnation(&command, expected)
                    }) {
                        return Ok(false);
                    }
                    self.apply_exact_pending_object_metadata_command(
                        pg_id,
                        super::ExactPendingObjectMetadataCommand::for_checked_request(&command),
                    )?;
                    return Ok(true);
                }
                self.drain_pending_object_metadata_command(pg_id, &command)?;
            }

            let proof = match match expected_bucket_incarnation_generation {
                Some(expected) => self
                    .try_acquire_lifecycle_bucket_write_proof_for_object_metadata_command(
                        bucket,
                        key,
                        ABORT_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
                        expected,
                    )?,
                None => self.try_acquire_bucket_write_proof_for_object_metadata_command(
                    bucket,
                    key,
                    ABORT_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
                    drain_mode == AbortMultipartUploadDrainMode::Wait,
                )?,
            } {
                Some(proof) => proof,
                None if drain_mode == AbortMultipartUploadDrainMode::Wait => {
                    continue 'retry_after_pending_conflict;
                }
                None => return Ok(false),
            };
            let command = match self.prepare_abort_multipart_upload_command(
                object_pg_id,
                bucket,
                key,
                upload_id,
                proof.clone(),
            ) {
                Ok(Some(command)) => command,
                Ok(None) => {
                    self.release_bucket_write_proof_for_object_metadata_command(&proof)?;
                    return Ok(false);
                }
                Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                })) => {
                    self.release_bucket_write_proof_for_object_metadata_command(&proof)?;
                    self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
                    continue 'retry_after_pending_conflict;
                }
                Err(ObjectPgActionError::StaleObjectReadSubject) => {
                    self.release_bucket_write_proof_for_object_metadata_command(&proof)?;
                    continue 'retry_after_pending_conflict;
                }
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(&proof)?;
                    return Err(error);
                }
            };
            let MetadataCommandPayload::AbortMultipartUpload(_) = command.payload() else {
                unreachable!("prepared abort multipart command changed payload kind");
            };
            #[cfg(test)]
            maybe_run_before_abort_multipart_pending_install_hook(
                self.metadata_command_apply_test_hook_scope_id(),
            );
            self.maybe_run_before_metadata_command_pending_install_hook();
            match self.try_set_pending_metadata_command_for_bucket(pg_id, bucket, &command) {
                Ok(Some(())) => {}
                Ok(None) | Err(StoreError::MetadataCommandLogConflict { .. }) => {
                    self.release_bucket_write_proof_for_object_metadata_command(&proof)?;
                    if let Some(pending) =
                        self.pending_metadata_command_for_bucket(pg_id, bucket)?
                    {
                        if metadata_command_is_matching_multipart_abort(
                            &pending, bucket, key, upload_id,
                        ) {
                            if expected_bucket_incarnation_generation.is_some_and(|expected| {
                                !metadata_command_matches_bucket_incarnation(&pending, expected)
                            }) {
                                return Ok(false);
                            }
                            self.apply_exact_pending_object_metadata_command(
                                pg_id,
                                super::ExactPendingObjectMetadataCommand::for_checked_request(
                                    &pending,
                                ),
                            )?;
                            return Ok(true);
                        }
                    }
                    self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
                    continue 'retry_after_pending_conflict;
                }
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(&proof)?;
                    return Err(error.into());
                }
            }
            if let Err(error) =
                self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)
            {
                match self
                    .pending_metadata_command_uses_bucket_write_reservation(pg_id, bucket, &proof)
                {
                    Ok(true) => {}
                    Ok(false) => {
                        self.release_bucket_write_proof_for_object_metadata_command(&proof)?;
                    }
                    Err(lookup_error) => return Err(lookup_error),
                }
                return Err(error);
            }
            return Ok(true);
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn abort_authorized_multipart_upload(
        &self,
        authorized_upload: &AuthorizedMultipartUploadRecord,
    ) -> Result<bool, ObjectPgActionError> {
        let bucket = &authorized_upload.record().bucket;
        let key = &authorized_upload.record().key;
        self.abort_authorized_multipart_upload_locked(
            super::MultipartObjectMutationEffectRoute {
                pg_id: self.object_metadata_pg(bucket, key),
                bucket,
                key,
                effect_fence: AdmittedRouteEffectFence::unbounded(self.operation_epoch()),
            },
            authorized_upload,
            || Ok(()),
        )
    }

    pub(super) fn abort_authorized_multipart_upload_locked(
        &self,
        route: super::MultipartObjectMutationEffectRoute<'_>,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        mut require_valid_route: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<bool, ObjectPgActionError> {
        crate::metadata_command::metadata_command_publisher!(AbortAuthorizedMultipartUploadLocked);
        let super::MultipartObjectMutationEffectRoute {
            pg_id: object_pg_id,
            bucket,
            key,
            effect_fence,
        } = route;
        if authorized_upload.record().bucket != *bucket || authorized_upload.record().key != *key {
            return Err(ObjectPgActionError::Store(
                StoreError::RouteCapabilitySubjectMismatch {
                    operation: "abort multipart upload",
                },
            ));
        }
        let pg_id = object_pg_id.pg_id();
        let upload_id = &authorized_upload.record().upload_id;
        'retry_after_pending_conflict: loop {
            require_valid_route().map_err(ObjectPgActionError::Store)?;
            while let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                if metadata_command_is_matching_multipart_abort(&command, bucket, key, upload_id) {
                    self.apply_exact_pending_object_metadata_command(
                        pg_id,
                        super::ExactPendingObjectMetadataCommand::for_checked_request(&command),
                    )?;
                    return Ok(true);
                }
                self.drain_pending_object_metadata_command(pg_id, &command)?;
            }

            require_valid_route().map_err(ObjectPgActionError::Store)?;
            let proof = match self
                .acquire_bucket_write_proof_for_object_metadata_command_with_effect_fence(
                    bucket,
                    key,
                    ABORT_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
                    effect_fence,
                )? {
                Some(proof) => proof,
                None => continue 'retry_after_pending_conflict,
            };
            if let Err(error) = require_valid_route() {
                self.release_bucket_write_proof_for_object_metadata_command(&proof)?;
                return Err(ObjectPgActionError::Store(error));
            }
            let command = match self.prepare_authorized_abort_multipart_upload_command(
                object_pg_id,
                authorized_upload,
                proof.clone(),
            ) {
                Ok(Some(command)) => command,
                Ok(None) => {
                    self.release_bucket_write_proof_for_object_metadata_command(&proof)?;
                    return Ok(false);
                }
                Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                })) => {
                    self.release_bucket_write_proof_for_object_metadata_command(&proof)?;
                    self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
                    continue 'retry_after_pending_conflict;
                }
                Err(ObjectPgActionError::StaleObjectReadSubject) => {
                    self.release_bucket_write_proof_for_object_metadata_command(&proof)?;
                    continue 'retry_after_pending_conflict;
                }
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(&proof)?;
                    return Err(error);
                }
            };
            let MetadataCommandPayload::AbortMultipartUpload(_) = command.payload() else {
                unreachable!("prepared abort multipart command changed payload kind");
            };
            #[cfg(test)]
            maybe_run_before_abort_multipart_pending_install_hook(
                self.metadata_command_apply_test_hook_scope_id(),
            );
            self.maybe_run_before_metadata_command_pending_install_hook();
            match self.try_set_pending_metadata_command_for_bucket_with_effect_fence(
                pg_id,
                bucket,
                &command,
                Some(effect_fence),
            ) {
                Ok(Some(())) => {}
                Ok(None) | Err(StoreError::MetadataCommandLogConflict { .. }) => {
                    self.release_bucket_write_proof_for_object_metadata_command(&proof)?;
                    if let Some(pending) =
                        self.pending_metadata_command_for_bucket(pg_id, bucket)?
                    {
                        if metadata_command_is_matching_multipart_abort(
                            &pending, bucket, key, upload_id,
                        ) {
                            self.apply_exact_pending_object_metadata_command(
                                pg_id,
                                super::ExactPendingObjectMetadataCommand::for_checked_request(
                                    &pending,
                                ),
                            )?;
                            return Ok(true);
                        }
                    }
                    self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
                    continue 'retry_after_pending_conflict;
                }
                Err(error) => {
                    self.release_bucket_write_proof_for_object_metadata_command(&proof)?;
                    return Err(error.into());
                }
            }
            if let Err(error) =
                self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)
            {
                match self
                    .pending_metadata_command_uses_bucket_write_reservation(pg_id, bucket, &proof)
                {
                    Ok(true) => {}
                    Ok(false) => {
                        self.release_bucket_write_proof_for_object_metadata_command(&proof)?;
                    }
                    Err(lookup_error) => return Err(lookup_error),
                }
                return Err(error);
            }
            return Ok(true);
        }
    }

    fn prepare_abort_multipart_upload_command(
        &self,
        pg_id: ObjectMetadataPgId,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        bucket_write_reservation: BucketWriteReservationProof,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError> {
        let mutation_client = self.object_mutation_metadata_primary_client(bucket, key)?;
        let expected_cleanup =
            mutation_client.load_abort_multipart_upload_cleanup(pg_id, bucket, key, upload_id)?;
        mutation_client.build_abort_multipart_upload_command(
            crate::node_client::BuildAbortMultipartUploadCommandReq {
                pg_id,
                cluster_epoch: self.operation_epoch(),
                bucket,
                key,
                upload_id,
                expected_cleanup: expected_cleanup.as_ref(),
                bucket_write_reservation,
            },
        )
    }

    fn prepare_authorized_abort_multipart_upload_command(
        &self,
        pg_id: ObjectMetadataPgId,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        bucket_write_reservation: BucketWriteReservationProof,
    ) -> Result<Option<MetadataCommandEnvelope>, ObjectPgActionError> {
        let mutation_client = self.object_mutation_metadata_primary_client(
            &authorized_upload.record().bucket,
            &authorized_upload.record().key,
        )?;
        let expected_cleanup = mutation_client.load_abort_multipart_upload_cleanup(
            pg_id,
            &authorized_upload.record().bucket,
            &authorized_upload.record().key,
            &authorized_upload.record().upload_id,
        )?;
        if expected_cleanup
            .as_ref()
            .is_some_and(|cleanup| cleanup.upload != *authorized_upload.record())
        {
            return Ok(None);
        }
        mutation_client.build_authorized_abort_multipart_upload_command(
            crate::node_client::BuildAuthorizedAbortMultipartUploadCommandReq {
                pg_id,
                cluster_epoch: self.operation_epoch(),
                authorized_upload,
                expected_cleanup: expected_cleanup.as_ref(),
                bucket_write_reservation,
            },
        )
    }

    pub fn abort_multipart_upload_if_due<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        expected_bucket_incarnation_generation: u64,
        should_abort: impl FnOnce(Option<&str>, &MultipartUploadRecord) -> Result<bool, E>,
    ) -> Result<Result<bool, E>, ObjectPgActionError> {
        let Some(lifecycle_context) =
            self.load_bucket_lifecycle_context(bucket, expected_bucket_incarnation_generation)?
        else {
            return Ok(Ok(false));
        };
        let BucketLifecycleContext {
            bucket_incarnation_generation,
            raw_lifecycle,
            ..
        } = lifecycle_context;

        let object_pg_id = self.object_metadata_pg(bucket, key);
        let pg_id = object_pg_id.pg_id();
        while let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
            let matching_abort = matches!(
                command.payload(),
                MetadataCommandPayload::AbortMultipartUpload(abort)
                    if abort.bucket == *bucket
                        && abort.key == *key
                        && abort.upload_id == *upload_id
            );
            if matching_abort {
                if !metadata_command_matches_bucket_incarnation(
                    &command,
                    bucket_incarnation_generation,
                ) {
                    return Ok(Ok(false));
                }
                self.apply_exact_pending_object_metadata_command(
                    pg_id,
                    super::ExactPendingObjectMetadataCommand::for_checked_request(&command),
                )?;
                return Ok(Ok(true));
            }
            self.drain_pending_object_metadata_command(pg_id, &command)?;
        }

        let mutation_client = self.object_mutation_metadata_primary_client(bucket, key)?;
        let multipart_lookup_route = mutation_client.open_multipart_upload_lookup_metadata_route(
            self.operation_epoch(),
            object_pg_id,
            bucket,
            key,
        )?;
        let upload = match multipart_lookup_route.load_multipart_upload(upload_id) {
            Ok(upload) => upload,
            Err(BucketSnapshotLoadError::Metadata(MetadataError::NoSuchUpload { .. })) => {
                return Ok(Ok(false));
            }
            Err(error) => {
                return Err(super::bucket_snapshot_error_to_object_pg_action_error(
                    error,
                ))
            }
        };

        if upload.state == UploadState::Aborting {
            return self
                .abort_multipart_upload_locked(
                    object_pg_id,
                    bucket,
                    key,
                    upload_id,
                    AbortMultipartUploadDrainMode::Stop,
                    Some(bucket_incarnation_generation),
                )
                .map(Ok);
        }
        if upload.state != UploadState::InProgress || raw_lifecycle.is_none() {
            return Ok(Ok(false));
        }

        let should_abort = match should_abort(raw_lifecycle.as_deref(), &upload) {
            Ok(should_abort) => should_abort,
            Err(error) => return Ok(Err(error)),
        };
        if !should_abort {
            return Ok(Ok(false));
        }

        self.abort_multipart_upload_locked(
            object_pg_id,
            bucket,
            key,
            upload_id,
            AbortMultipartUploadDrainMode::Stop,
            Some(bucket_incarnation_generation),
        )
        .map(Ok)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_ec_scratch_allocation_count(&self, shape: EcShape) -> usize {
        self.metadata_primary_bridge_node()
            .expect("test hook requires a current storage cluster handle")
            .test_ec_scratch_allocation_count(shape)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_bucket_pg_id_for(&self, bucket: &BucketName) -> u32 {
        self.bucket_metadata_pg_id(bucket)
    }

    pub fn bucket_pg_id_for(&self, bucket: &BucketName) -> u32 {
        self.bucket_metadata_pg_id(bucket)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_head_bucket_raw(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.metadata_primary_bridge_node()?
            .test_head_bucket_raw(bucket)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_begin_bucket_delete_if_current(
        &self,
        bucket: &BucketName,
    ) -> Result<(), BucketWriteDrainError> {
        let bucket_info = self
            .head_bucket_info(bucket)
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        self.begin_bucket_delete_if_current(
            bucket,
            BucketIdentityGenerations::from_bucket_info(&bucket_info),
        )
    }

    pub fn bucket_delete_diagnostic(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketDeleteDiagnostic, BucketSnapshotLoadError> {
        self.bucket_delete_debug_snapshot(bucket)
            .map(|snapshot| BucketDeleteDiagnostic::from_snapshot(&snapshot))
    }

    fn bucket_delete_debug_snapshot(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketDeleteDebugSnapshot, BucketSnapshotLoadError> {
        let pg_id = PgId::new(self.bucket_metadata_pg_id(bucket));
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;

        let bucket_row = match node
            .bucket_metadata_client()
            .head_bucket_raw(self.validated_bucket_metadata_pg(pg_id), bucket)
        {
            Ok(info) => Some(BucketDeleteDebugBucketRow {
                state: info.state,
                bucket_execution_generation: info.bucket_execution_generation,
                bucket_incarnation_generation: info.bucket_incarnation_generation,
            }),
            Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketNotFound { .. })) => None,
            Err(error) => return Err(error),
        };

        let durable_write_drain = node
            .bucket_write_reservation_client()
            .durable_bucket_write_drain(self.validated_bucket_metadata_pg(pg_id), bucket)?
            .map(|record| BucketDeleteDebugDrain {
                drain_id: record.drain_id,
                cluster_epoch: record.cluster_epoch,
                bucket_execution_generation: record.bucket_execution_generation,
                created_at: record.created_at,
                lease_deadline: record.lease_deadline,
            });

        let pending_metadata_command = self
            .pending_metadata_command_for_bucket(pg_id, bucket)?
            .map(|command| {
                let id = command.id();
                let target_bucket = command.bucket_name().clone();
                BucketDeleteDebugPendingCommand {
                    kind: command.payload().kind_name(),
                    matches_bucket: target_bucket == *bucket,
                    target_bucket,
                    cluster_epoch: id.cluster_epoch(),
                    pg_id: id.pg_id().get(),
                    log_index: id.log_index().get(),
                }
            });

        let finalize_claim = node
            .bucket_write_reservation_client()
            .bucket_delete_finalize_claim(self.validated_bucket_metadata_pg(pg_id), bucket)?
            .map(|record| BucketDeleteDebugFinalizeClaim {
                matches_bucket: record.bucket == *bucket,
                bucket: record.bucket,
                bucket_incarnation_generation: record.bucket_incarnation_generation,
                claim_id: record.claim_id,
                cluster_epoch: record.cluster_epoch,
                pg_id: record.pg_id,
                claimed_at: record.claimed_at,
                lease_deadline: record.lease_deadline,
                attempt_count: record.attempt_count,
                last_error: record.last_error,
            });

        let attempt_outcome = node
            .bucket_write_reservation_client()
            .bucket_delete_attempt_outcome(self.validated_bucket_metadata_pg(pg_id), bucket)?;

        let mut object_version_samples = Vec::new();
        let mut object_version_sample_errors = Vec::new();
        let mut payload_reclaim_roots = Vec::new();
        let mut payload_reclaim_root_errors = Vec::new();
        let mut payload_reclaim_claims = Vec::new();
        let mut payload_reclaim_claim_errors = Vec::new();
        for raw_pg_id in self.metadata_pg_ids() {
            let object_pg_id = PgId::new(raw_pg_id);
            let scan_pg_id = self.object_metadata_scan_pg(object_pg_id);
            let node = match self
                .local_map
                .metadata_pg_primary_node(self.operation_epoch(), object_pg_id)
            {
                Ok(node) => node,
                Err(error) => {
                    let detail = error.to_string();
                    object_version_sample_errors.push(BucketDeleteDebugObjectVersionSampleError {
                        object_pg_id: raw_pg_id,
                        detail: detail.clone(),
                    });
                    payload_reclaim_claim_errors.push(BucketDeleteDebugPayloadReclaimClaimError {
                        object_pg_id: raw_pg_id,
                        detail: detail.clone(),
                    });
                    payload_reclaim_root_errors.push(BucketDeleteDebugPayloadReclaimRootError {
                        object_pg_id: raw_pg_id,
                        detail,
                    });
                    continue;
                }
            };
            match node
                .object_listing_metadata_client()
                .open_object_listing_metadata_route(self.operation_epoch(), scan_pg_id)
                .and_then(|route| {
                    route.list_object_versions_page(&ListObjectVersionsReq {
                        bucket: bucket.clone(),
                        prefix: None,
                        key_marker: None,
                        version_id_marker: None,
                        start_at: None,
                        max_keys: 1,
                    })
                }) {
                Ok(resp) => {
                    if let Some(stored) = resp.versions.into_iter().next() {
                        object_version_samples
                            .push(Self::bucket_delete_debug_object_sample(raw_pg_id, stored));
                    }
                }
                Err(error) => {
                    object_version_sample_errors.push(BucketDeleteDebugObjectVersionSampleError {
                        object_pg_id: raw_pg_id,
                        detail: error.to_string(),
                    });
                }
            }
            match node
                .object_mutation_metadata_client()
                .object_payload_reclaim_claim(scan_pg_id)
            {
                Ok(Some(claim)) => {
                    payload_reclaim_claims.push(BucketDeleteDebugPayloadReclaimClaim {
                        object_pg_id: raw_pg_id,
                        matches_bucket: claim.bucket == *bucket,
                        bucket: claim.bucket,
                        bucket_incarnation_generation: claim.bucket_incarnation_generation,
                        key: claim.key,
                        generation_id: claim.generation_id,
                        reclaim_kind: claim.reclaim_kind,
                        claim_id: claim.claim_id,
                        cluster_epoch: claim.cluster_epoch,
                        claimed_at: claim.claimed_at,
                        lease_deadline: claim.lease_deadline,
                        attempt_count: claim.attempt_count,
                        last_error: claim.last_error,
                    });
                }
                Ok(None) => {}
                Err(error) => {
                    payload_reclaim_claim_errors.push(BucketDeleteDebugPayloadReclaimClaimError {
                        object_pg_id: raw_pg_id,
                        detail: error.to_string(),
                    });
                }
            }
            let root = match node
                .object_mutation_metadata_client()
                .get_bucket_payload_reclaim_root(scan_pg_id, bucket)
            {
                Ok(Some(root)) => root,
                Ok(None) => continue,
                Err(error) => {
                    payload_reclaim_root_errors.push(BucketDeleteDebugPayloadReclaimRootError {
                        object_pg_id: raw_pg_id,
                        detail: error.to_string(),
                    });
                    continue;
                }
            };
            if let Err(error) = self.validate_bucket_payload_reclaim_root_for_pg(
                object_pg_id,
                &root,
                node.node_id(),
            ) {
                payload_reclaim_root_errors.push(BucketDeleteDebugPayloadReclaimRootError {
                    object_pg_id: raw_pg_id,
                    detail: error.to_string(),
                });
                continue;
            }
            let reclaim_details = match node
                .object_mutation_metadata_client()
                .get_object_payload_reclaim(
                    self.object_metadata_pg(&root.bucket, &root.key),
                    &root.bucket,
                    &root.key,
                    root.generation_id,
                ) {
                Ok(Some(reclaim)) => Some(Self::bucket_delete_debug_reclaim_details(&reclaim)),
                Ok(None) => None,
                Err(error) => {
                    payload_reclaim_root_errors.push(BucketDeleteDebugPayloadReclaimRootError {
                        object_pg_id: raw_pg_id,
                        detail: error.to_string(),
                    });
                    None
                }
            };
            let (reclaim_kind, reclaim_created_at, reclaim_item_count) =
                reclaim_details.unwrap_or((None, None, None));
            payload_reclaim_roots.push(BucketDeleteDebugPayloadReclaimRoot {
                object_pg_id: raw_pg_id,
                key: root.key,
                generation_id: root.generation_id,
                reclaim_kind,
                reclaim_created_at,
                reclaim_item_count,
            });
        }

        Ok(BucketDeleteDebugSnapshot {
            bucket: bucket.clone(),
            pg_id: pg_id.get(),
            cluster_epoch: self.cluster_epoch(),
            operation_epoch: self.operation_epoch(),
            route_map_valid_until_ms: self.route_map_valid_until_ms(),
            bucket_pg_primary_node_id: node.node_id().as_u32(),
            bucket_row,
            durable_write_drain,
            pending_metadata_command,
            finalize_claim,
            object_version_samples,
            object_version_sample_errors,
            payload_reclaim_roots,
            payload_reclaim_root_errors,
            payload_reclaim_claims,
            payload_reclaim_claim_errors,
            attempt_outcome,
        })
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_bucket_delete_progress(
        &self,
        bucket: &BucketName,
    ) -> Result<crate::TestBucketDeleteProgress, BucketSnapshotLoadError> {
        let snapshot = self.bucket_delete_debug_snapshot(bucket)?;
        Ok(crate::TestBucketDeleteProgress {
            bucket_state: snapshot.bucket_row.map(|row| row.state),
            has_durable_write_drain: snapshot.durable_write_drain.is_some(),
            has_pending_metadata_command: snapshot.pending_metadata_command.is_some(),
        })
    }

    fn bucket_delete_debug_object_sample(
        object_pg_id: u32,
        stored: StoredObject,
    ) -> BucketDeleteDebugObjectVersionSample {
        match stored {
            StoredObject::Live(record) => BucketDeleteDebugObjectVersionSample {
                object_pg_id,
                kind: BucketDeleteDebugObjectVersionKind::Live,
                key: record.key,
                version_id: record.version_id,
                generation_id: Some(record.generation_id),
                size: Some(record.size),
                layout: Some(record.layout),
                last_modified: record.last_modified,
                became_noncurrent_at: record.became_noncurrent_at,
            },
            StoredObject::DeleteMarker(record) => BucketDeleteDebugObjectVersionSample {
                object_pg_id,
                kind: BucketDeleteDebugObjectVersionKind::DeleteMarker,
                key: record.key,
                version_id: record.version_id,
                generation_id: None,
                size: None,
                layout: None,
                last_modified: record.last_modified,
                became_noncurrent_at: None,
            },
        }
    }

    fn bucket_delete_debug_reclaim_details(
        reclaim: &ObjectPayloadReclaimCommand,
    ) -> (Option<ObjectPayloadReclaimKind>, Option<u64>, Option<usize>) {
        match reclaim {
            ObjectPayloadReclaimCommand::Segments(record) => (
                Some(ObjectPayloadReclaimKind::ObjectSegments),
                Some(record.created_at),
                Some(record.segments.len()),
            ),
            ObjectPayloadReclaimCommand::Multipart(record) => (
                Some(ObjectPayloadReclaimKind::Multipart),
                Some(record.created_at),
                Some(record.parts.len()),
            ),
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_object_pg_id_for(&self, bucket: &BucketName, key: &ObjectKey) -> u32 {
        self.object_metadata_pg_id(bucket, key)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_data_pg_id_for(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> u32 {
        self.local_map
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
        self.metadata_primary_bridge_node()?
            .test_object_generation_reservation_for(bucket, key, reservation_id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_multipart_part_data_pg_id_for(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        object_generation_id: GenerationId,
        part_number: u32,
    ) -> u32 {
        self.local_map
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
        self.metadata_primary_bridge_node()?
            .test_get_object_meta(bucket, key)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_get_multipart_upload(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_get_multipart_upload(bucket, key, upload_id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_get_multipart_part(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        part_number: u16,
    ) -> Result<MultipartPartRecord, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_get_multipart_part(bucket, key, upload_id, part_number)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_list_multipart_parts(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        req: &ListPartsReq,
    ) -> Result<ListPartsResp, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_list_multipart_parts(bucket, key, req)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_list_multipart_uploads_for_bucket(
        &self,
        bucket: &BucketName,
    ) -> Result<Vec<MultipartUploadRecord>, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_list_multipart_uploads_for_bucket(bucket)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_get_object_segments(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<Vec<ObjectSegmentRecord>, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_get_object_segments(bucket, key, version_id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_replace_live_object_segments(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        segments: &[ObjectSegmentRecord],
    ) -> Result<(), ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_replace_live_object_segments(bucket, key, version_id, segments)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_get_object_parts(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<Vec<ObjectPartRecord>, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_get_object_parts(bucket, key, version_id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_replace_object_parts(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        parts: &[ObjectPartRecord],
    ) -> Result<(), ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_replace_object_parts(bucket, key, version_id, parts)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_get_object_version(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<StoredObject, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_get_object_version(bucket, key, version_id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_get_object_segments_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<Option<crate::TestObjectSegmentsReclaimRecord>, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_get_object_segments_reclaim(bucket, key, generation_id)
            .map(|reclaim| reclaim.map(Into::into))
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_put_object_segments_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reclaim: &crate::TestObjectSegmentsReclaimRecord,
    ) -> Result<(), ObjectPgActionError> {
        let reclaim = ObjectSegmentsReclaimRecord::from(reclaim);
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        for node in self
            .local_map
            .metadata_pg_acting_nodes(self.operation_epoch(), pg_id)?
        {
            let pg = node.test_node().get_pg(pg_id.get())?;
            pg.put_object_segments_reclaim(&reclaim)?;
            pg.refresh_metadata_command_state_digest()?;
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_put_multipart_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reclaim: &crate::TestMultipartReclaimRecord,
    ) -> Result<(), ObjectPgActionError> {
        let reclaim = MultipartReclaimRecord::from(reclaim);
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        for node in self
            .local_map
            .metadata_pg_acting_nodes(self.operation_epoch(), pg_id)?
        {
            let pg = node.test_node().get_pg(pg_id.get())?;
            pg.put_multipart_reclaim(&reclaim)?;
            pg.refresh_metadata_command_state_digest()?;
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_payload_reclaim_exists(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<bool, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_payload_reclaim_exists(bucket, key, generation_id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_object_payload_reclaim_is_active(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> bool {
        self.local_map
            .test_object_payload_reclaim_is_active(bucket, key, generation_id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_list_bucket_payload_reclaim_roots(
        &self,
        bucket: &BucketName,
    ) -> Result<Vec<crate::TestPayloadReclaimRoot>, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_list_bucket_payload_reclaim_roots(bucket)
            .map(|roots| roots.into_iter().map(Into::into).collect())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_force_became_noncurrent_at(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        became_noncurrent_at: u64,
    ) -> Result<(), ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        self.local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?
            .test_node()
            .test_force_became_noncurrent_at(bucket, key, version_id, became_noncurrent_at)
    }

    #[cfg(any(test, feature = "test-hooks"))]
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
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        self.test_begin_bucket_delete_if_current(bucket)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_delete_bucket_metadata(
        &self,
        bucket: &BucketName,
    ) -> Result<(), BucketWriteDrainError> {
        let pg_id = PgId::new(self.bucket_metadata_pg_id(bucket));
        let info = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?
            .bucket_metadata_client()
            .head_bucket_raw(self.validated_bucket_metadata_pg(pg_id), bucket)
            .map_err(bucket_snapshot_error_to_bucket_write_drain_error)?;
        let root = BucketDeleteFinalizeRoot {
            bucket: bucket.clone(),
            bucket_incarnation_generation: info.bucket_incarnation_generation,
        };
        match self.delete_bucket_from_acting_set(pg_id, &root)? {
            BucketDeleteFinalizeOutcome::Finalized => Ok(()),
            BucketDeleteFinalizeOutcome::NotFound => {
                Err(crate::error::MetadataError::BucketNotFound {
                    name: bucket.clone(),
                }
                .into())
            }
            BucketDeleteFinalizeOutcome::NotDeleting
            | BucketDeleteFinalizeOutcome::StaleIncarnation
            | BucketDeleteFinalizeOutcome::Pending => {
                unreachable!("test bucket metadata delete bypasses finalization checks")
            }
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_get_all_multipart_part_segments_for_upload(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<Vec<MultipartPartSegmentRecord>, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_get_all_multipart_part_segments_for_upload(bucket, key, upload_id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_insert_lifecycle_sweep_claim(
        &self,
        bucket: &BucketName,
        bucket_incarnation_generation: u64,
        lease_deadline: Option<u64>,
    ) -> Result<(), ObjectPgActionError> {
        let pg = self.metadata_pg(self.bucket_metadata_pg_id(bucket))?;
        pg.test_insert_lifecycle_sweep_claim(
            bucket,
            bucket_incarnation_generation,
            "test-lifecycle-claim",
            "test-owner-token",
            self.operation_epoch(),
            lease_deadline,
        )?;
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_begin_durable_bucket_delete_drain(
        &self,
        bucket: &BucketName,
    ) -> Result<(), BucketWriteDrainError> {
        match self.begin_durable_bucket_delete_drain(bucket)? {
            super::DurableBucketDeleteDrainBegin::Acquired(_)
            | super::DurableBucketDeleteDrainBegin::AlreadyDeleting => Ok(()),
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_set_upload_state(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        state: UploadState,
    ) -> Result<(), ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        for node in self
            .local_map
            .metadata_pg_acting_nodes(self.operation_epoch(), pg_id)?
        {
            node.test_node()
                .test_set_upload_state(bucket, key, upload_id, state)?;
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_list_stream_segments(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<Vec<StreamUploadSegmentRecord>, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_list_stream_segments(bucket, key, session_id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_force_stream_upload_created_at(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        created_at: u64,
    ) -> Result<(), ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        for node in self
            .local_map
            .metadata_pg_acting_nodes(self.operation_epoch(), pg_id)?
        {
            node.test_node()
                .test_force_stream_upload_created_at(bucket, key, session_id, created_at)?;
            let pg = node.test_node().get_pg(pg_id.get())?;
            pg.refresh_metadata_command_state_digest()?;
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_list_all_stream_uploads(
        &self,
    ) -> Result<Vec<StreamUploadRecord>, ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_list_all_stream_uploads()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_create_stream_upload(
        &self,
        req: &CreateStreamUploadReq,
    ) -> Result<(), ObjectPgActionError> {
        self.metadata_primary_bridge_node()?
            .test_create_stream_upload(req)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_shard_exists(&self, pg_id: u32, key: &ShardKey) -> Result<bool, StoreError> {
        self.metadata_primary_bridge_node()?
            .test_shard_exists(pg_id, key)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_lock_bucket_pg(
        &self,
        bucket: &BucketName,
    ) -> Result<crate::node::BucketPgTestGuard<'_>, StoreError> {
        self.metadata_primary_bridge_node()?
            .test_lock_bucket_pg(bucket)
    }
}

#[cfg(test)]
mod bounded_pg_scan_tests {
    use super::bounded_pg_scan_window;

    #[test]
    fn bounded_pg_scan_visits_200_pgs_once_in_linear_batches() {
        let pg_ids = (0..200).collect::<Vec<_>>();
        let mut visited = Vec::new();
        let mut next_pg_id = None;
        let mut batches = 0usize;

        loop {
            let window = bounded_pg_scan_window(&pg_ids, next_pg_id, 8);
            visited.extend_from_slice(&pg_ids[window.start..window.end]);
            batches += 1;
            next_pg_id = window.next_pg_id;
            if next_pg_id.is_none() {
                break;
            }
        }

        assert_eq!(visited, pg_ids);
        assert_eq!(batches, 25);
    }
}
