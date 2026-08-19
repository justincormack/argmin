// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

const INTERNAL_LIST_PAGE_SIZE: u32 = 1_000;
const ORPHAN_OBJECT_PAYLOAD_RECLAIM_BUCKET_INCARNATION: u64 = 0;
#[cfg(any(test, feature = "test-hooks"))]
const MISSING_BUCKET_DELETE_FINALIZE_INCARNATION: u64 = 0;
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
pub(super) const METADATA_COMMAND_APPLY_RETRY_BUDGET_MILLIS: u64 = 10_000;
// Cross-process operation deadlines subtract the supported wall-clock skew.
// Include that allowance so a remote actor still receives one usable second
// for bounded publication confirmation and terminal cleanup.
pub(crate) const METADATA_COMMAND_PUBLICATION_CONFIRM_BUDGET: Duration = Duration::from_millis(
    crate::control_plane_lease::CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS + 1_000,
);
const BUCKET_WRITE_RESERVATION_LEASE_MILLIS: u64 = 15_000;
// HTTP streaming PutObject heartbeats active sessions every 10s. Keep the
// durable create reservation only slightly longer than that so abandoned
// sessions stop blocking DeleteBucket well before common 30s client attempt
// timeouts, while still allowing one delayed heartbeat under contention.
const PUT_OBJECT_STREAM_CREATE_LEASE_MILLIS: u64 = BUCKET_WRITE_RESERVATION_LEASE_MILLIS;

fn object_payload_placement_failure(
    error: ObjectPgActionError,
) -> ObjectPayloadPlacementDiagnostic {
    ObjectPayloadPlacementDiagnostic::conflict(format!(
        "object payload placement unavailable: {}\n",
        error.diagnostic_cause_label()
    ))
}

fn parse_metadata_checkpoint_selector(
    selector: &str,
) -> Result<PgId, MetadataCheckpointDiagnostic> {
    selector.parse::<u32>().map(PgId::new).map_err(|_| {
        MetadataCheckpointDiagnostic::new(
            MetadataCheckpointDiagnosticOutcome::InvalidInput,
            "invalid pg id\n".to_string(),
        )
    })
}

fn metadata_checkpoint_diagnostic_from_result(
    pg_id: PgId,
    result: Result<MetadataCommandCheckpointRecordSummary, StoreError>,
) -> MetadataCheckpointDiagnostic {
    match result {
        Ok(summary) => {
            let outcome = if summary.compaction_failed == 0 && summary.failed == 0 {
                MetadataCheckpointDiagnosticOutcome::Success
            } else {
                MetadataCheckpointDiagnosticOutcome::Conflict
            };
            MetadataCheckpointDiagnostic::new(
                outcome,
                format!(
                    "pg_id={} scanned={} recorded={} already_current={} skipped_cadence={} skipped_inactive={} skipped_empty={} skipped_stale_epoch={} compacted={} compaction_deleted_entries={} compaction_noop={} compaction_no_checkpoint={} compaction_pending={} compaction_failed={} failed={} limit_reached={}\n",
                    pg_id.get(),
                    summary.scanned,
                    summary.recorded,
                    summary.already_current,
                    summary.skipped_cadence,
                    summary.skipped_inactive,
                    summary.skipped_empty,
                    summary.skipped_stale_epoch,
                    summary.compacted,
                    summary.compaction_deleted_entries,
                    summary.compaction_noop,
                    summary.compaction_no_checkpoint,
                    summary.compaction_pending,
                    summary.compaction_failed,
                    summary.failed,
                    summary.limit_reached
                ),
            )
        }
        Err(error) => MetadataCheckpointDiagnostic::new(
            MetadataCheckpointDiagnosticOutcome::Conflict,
            format!(
                "pg_id={} checkpoint_record_failed={}\n",
                pg_id.get(),
                error.diagnostic_cause_label()
            ),
        ),
    }
}

fn metadata_command_terminal_cleanup_error_is_retryable(error: &StoreError) -> bool {
    matches!(
        error,
        StoreError::Io { .. }
            | StoreError::MetadataCommandTerminalEntryPending { .. }
            | StoreError::OperationDeadlineExceeded { .. }
            | StoreError::StorageRpcResourceExhausted { .. }
            | StoreError::MetadataCommandContention { .. }
            | StoreError::MetadataCommandIrrevocableConvergencePending { .. }
            | StoreError::MetadataCommandDependencyConvergencePending { .. }
            | StoreError::PgNotActive { .. }
            | StoreError::RouteMapExpired { .. }
            | StoreError::StaleMetadataOperation { .. }
            | StoreError::StaleMetadataRoute { .. }
            | StoreError::StorageRpc {
                failure: StorageRpcErrorCode::TransportTimeout
                    | StorageRpcErrorCode::TransportClosed
                    | StorageRpcErrorCode::MetadataCommandContention
                    | StorageRpcErrorCode::StaleShardLocation
                    | StorageRpcErrorCode::WrongClusterEpoch,
                ..
            }
    )
}

pub(super) fn metadata_command_apply_transport_error_is_retryable(
    error: &BucketSnapshotLoadError,
) -> bool {
    matches!(
        error,
        BucketSnapshotLoadError::Store(
            StoreError::StorageRpcResourceExhausted { .. }
                | StoreError::StorageRpc {
                    failure: StorageRpcErrorCode::TransportTimeout
                        | StorageRpcErrorCode::TransportClosed
                        | StorageRpcErrorCode::MetadataCommandContention,
                    ..
                }
        )
    )
}

pub(super) fn store_error_is_metadata_command_contention(error: &StoreError) -> bool {
    matches!(
        error,
        StoreError::MetadataCommandContention { .. }
            | StoreError::StorageRpc {
                failure: StorageRpcErrorCode::MetadataCommandContention,
                ..
            }
    )
}

pub(super) fn metadata_command_apply_error_is_contention(
    error: &BucketSnapshotLoadError,
) -> bool {
    matches!(
        error,
        BucketSnapshotLoadError::Store(error)
            if store_error_is_metadata_command_contention(error)
    )
}

pub(super) fn metadata_command_apply_error_can_reinspect_after_abandonment(
    error: &BucketSnapshotLoadError,
) -> bool {
    match error {
        BucketSnapshotLoadError::Store(error) => matches!(
            error.operation_failure_class(),
            StoreOperationFailureClass::ResourceExhausted
                | StoreOperationFailureClass::MetadataCommandContention
                | StoreOperationFailureClass::RetryableConvergence
        ),
        BucketSnapshotLoadError::Metadata(error) => error.is_command_contention(),
    }
}

pub(super) fn object_pg_action_error_is_retryable_command_observation(
    error: &ObjectPgActionError,
) -> bool {
    match error {
        ObjectPgActionError::Store(error) => {
            store_error_is_retryable_command_observation(error)
        }
        ObjectPgActionError::Metadata(error) => error.is_command_contention(),
        ObjectPgActionError::InvalidRequest { .. }
        | ObjectPgActionError::StaleObjectReadSubject
        | ObjectPgActionError::StaleDirectPutCommitSnapshot
        | ObjectPgActionError::StaleStreamFinalizeSnapshot
        | ObjectPgActionError::SnapshotReinspectionConflict
        | ObjectPgActionError::StaleMultipartCompletionSnapshot
        | ObjectPgActionError::MultipartConditionalRequestConflict
        | ObjectPgActionError::MultipartPrepublicationBarrierExhausted => false,
    }
}

pub(super) fn metadata_command_abandonment_observation_error_is_retryable(
    error: &BucketSnapshotLoadError,
) -> bool {
    match error {
        BucketSnapshotLoadError::Store(error) => {
            store_error_is_retryable_command_observation(error)
        }
        BucketSnapshotLoadError::Metadata(error) => error.is_command_contention(),
    }
}

pub(super) fn object_pg_action_error_is_retryable_pending_drain(
    error: &ObjectPgActionError,
) -> bool {
    match error {
        ObjectPgActionError::Store(
            StoreError::MetadataCommandOutcomeUnconfirmed { .. }
            | StoreError::MetadataCommandIrrevocableConvergencePending { .. }
            | StoreError::MetadataCommandDependencyConvergencePending { .. },
        ) => true,
        ObjectPgActionError::Store(error) => matches!(
            error.operation_failure_class(),
            StoreOperationFailureClass::ResourceExhausted
                | StoreOperationFailureClass::MetadataCommandContention
                | StoreOperationFailureClass::RetryableConvergence
        ),
        ObjectPgActionError::Metadata(error) => error.is_command_contention(),
        ObjectPgActionError::InvalidRequest { .. }
        | ObjectPgActionError::StaleObjectReadSubject
        | ObjectPgActionError::StaleDirectPutCommitSnapshot
        | ObjectPgActionError::StaleStreamFinalizeSnapshot
        | ObjectPgActionError::SnapshotReinspectionConflict
        | ObjectPgActionError::StaleMultipartCompletionSnapshot
        | ObjectPgActionError::MultipartConditionalRequestConflict
        | ObjectPgActionError::MultipartPrepublicationBarrierExhausted => false,
    }
}

pub(super) fn bucket_snapshot_error_is_deferred_pending_drain(
    error: &BucketSnapshotLoadError,
) -> bool {
    matches!(
        error,
        BucketSnapshotLoadError::Store(
            StoreError::MetadataCommandOutcomeUnconfirmed { .. }
                | StoreError::MetadataCommandIrrevocableConvergencePending { .. }
                | StoreError::MetadataCommandDependencyConvergencePending { .. }
        )
    )
}

fn store_error_is_retryable_command_observation(error: &StoreError) -> bool {
    match error {
        StoreError::ClusterMapHistoryReferenceLimitExceeded { .. }
        | StoreError::StorageRpcResourceExhausted { .. }
        | StoreError::MetadataCommandPendingConflict { .. }
        | StoreError::MetadataCommandContention { .. }
        | StoreError::StalePayloadOperation { .. }
        | StoreError::StaleMetadataPrimaryBridge { .. }
        | StoreError::StaleMetadataOperation { .. }
        | StoreError::StaleMetadataRoute { .. }
        | StoreError::StaleMetadataReadProof { .. }
        | StoreError::RouteMapExpired { .. }
        | StoreError::RouteAdmissionClusterMismatch { .. }
        | StoreError::StaleMetadataCommand { .. }
        | StoreError::StaleShardOperation { .. }
        | StoreError::StaleShardLocation { .. }
        | StoreError::PgNotActive { .. }
        | StoreError::ShardPgNotActive { .. }
        | StoreError::MetadataCommandTerminalEntryPending { .. }
        | StoreError::OperationDeadlineExceeded { .. } => true,
        StoreError::ShardStore { source, .. } => {
            store_error_is_retryable_command_observation(source)
        }
        StoreError::StorageRpc { failure, .. } => matches!(
            failure.wire_code(),
            StorageRpcWireErrorCode::ResourceExhausted
                | StorageRpcWireErrorCode::MetadataCommandContention
                | StorageRpcWireErrorCode::StaleShardLocation
                | StorageRpcWireErrorCode::InactivePgRoute
                | StorageRpcWireErrorCode::NonActingSetAccess
                | StorageRpcWireErrorCode::WrongClusterEpoch
                | StorageRpcWireErrorCode::TransportTimeout
                | StorageRpcWireErrorCode::TransportClosed
                | StorageRpcWireErrorCode::MetadataCommandMutationUncertain
        ),
        _ => false,
    }
}

pub(super) fn metadata_command_apply_error_requires_exact_confirmation(
    error: &BucketSnapshotLoadError,
) -> bool {
    matches!(
        error,
        BucketSnapshotLoadError::Store(
            StoreError::MetadataCommandOutcomeUnconfirmed { .. }
                | StoreError::MetadataCommandIrrevocableConvergencePending { .. }
        )
    )
}

fn metadata_command_probe_error_is_fatal_integrity(error: &BucketSnapshotLoadError) -> bool {
    matches!(
        error,
        BucketSnapshotLoadError::Store(
            StoreError::MetadataCommandLogChecksumMismatch { .. }
                | StoreError::MetadataCommandLogHashMismatch { .. }
                | StoreError::MetadataCommandReplicaStateEncodingVersion { .. }
                | StoreError::MetadataCommandReplicaStateDiverged { .. }
                | StoreError::MetadataStateDigestMismatch { .. }
                | StoreError::MetadataCheckpointInvalid { .. }
                | StoreError::StorageRpc {
                    failure: StorageRpcErrorCode::MetadataCommandIntegrity,
                    ..
                }
        )
    )
}

pub(super) fn metadata_command_apply_error_can_handoff_to_recovery(
    error: &BucketSnapshotLoadError,
) -> bool {
    if metadata_command_apply_transport_error_is_retryable(error) {
        return true;
    }
    match error {
        BucketSnapshotLoadError::Store(error) => {
            matches!(
                error,
                StoreError::Io { .. } | StoreError::MetadataCommandContention { .. }
            )
                || matches!(
                    error.operation_failure_class(),
                    StoreOperationFailureClass::ResourceExhausted
                        | StoreOperationFailureClass::RetryableConvergence
                )
        }
        BucketSnapshotLoadError::Metadata(_) => false,
    }
}

pub(super) fn applied_metadata_command_cleanup_error_is_retryable(
    error: &BucketSnapshotLoadError,
) -> bool {
    match error {
        BucketSnapshotLoadError::Store(error) => {
            metadata_command_terminal_cleanup_error_is_retryable(error)
        }
        BucketSnapshotLoadError::Metadata(error) => error.is_command_contention(),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PendingMetadataCommandTerminalCleanup {
    Removed,
    AlreadyAbsent,
    Deferred,
}

pub(super) fn emit_metadata_command_terminal_cleanup_deferred(
    pg_id: PgId,
    reason: &'static str,
    error: Option<&StoreError>,
) {
    let _ = observability::event(
        super::TRACE_TARGET,
        "metadata_command_terminal_cleanup_deferred",
        Some(format_args!(
            "pg_id={} reason={} error={:?}",
            pg_id.get(),
            reason,
            error
        )),
    );
}

pub(super) fn remove_pending_metadata_command_slot_after_terminal_outcome(
    pg_id: PgId,
    mut work_budget: Option<&mut super::RequestWorkBudget>,
    mut remove: impl FnMut() -> Result<bool, StoreError>,
) -> Result<PendingMetadataCommandTerminalCleanup, StoreError> {
    loop {
        if let Some(work_budget) = work_budget.as_deref_mut() {
            if work_budget
                .check("metadata command pending-slot remove retry budget exhausted")
                .is_err()
            {
                emit_metadata_command_terminal_cleanup_deferred(
                    pg_id,
                    "caller budget exhausted",
                    None,
                );
                return Ok(PendingMetadataCommandTerminalCleanup::Deferred);
            }
        }
        match remove() {
            Ok(true) => return Ok(PendingMetadataCommandTerminalCleanup::Removed),
            Ok(false) => return Ok(PendingMetadataCommandTerminalCleanup::AlreadyAbsent),
            Err(error) if metadata_command_terminal_cleanup_error_is_retryable(&error) => {
                let Some(work_budget) = work_budget.as_deref_mut() else {
                    emit_metadata_command_terminal_cleanup_deferred(
                        pg_id,
                        "one-shot cleanup failed transiently",
                        Some(&error),
                    );
                    return Ok(PendingMetadataCommandTerminalCleanup::Deferred);
                };
                if work_budget
                    .sleep_after_contention(
                        "metadata command pending-slot remove retry budget exhausted",
                    )
                    .is_err()
                {
                    emit_metadata_command_terminal_cleanup_deferred(
                        pg_id,
                        "caller budget exhausted after transient failure",
                        Some(&error),
                    );
                    return Ok(PendingMetadataCommandTerminalCleanup::Deferred);
                }
            }
            Err(error) => return Err(error),
        }
    }
}

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
        | StoreError::StaleMetadataReadProof { .. }
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
    Aborted { count: usize, any_aborted: bool },
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

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) type MetadataCommandAfterApplyTestHook =
    Arc<dyn Fn(NodeId, &MetadataCommandEnvelope) -> Result<(), StoreError> + Send + Sync>;

#[cfg(test)]
type MetadataCommandApplyAttemptTestHook =
    Arc<dyn Fn(&MetadataCommandEnvelope) -> Result<(), StoreError> + Send + Sync>;

#[cfg(test)]
type MetadataCommandProgressReconstructionTestHook =
    Arc<dyn Fn(&MetadataCommandEnvelope) -> Result<(), StoreError> + Send + Sync>;

#[cfg(test)]
type AbortMultipartPendingInstallTestHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
pub type StreamPutCreatePendingInstallTestHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(test)]
type StreamPutCreateCommandIdTestHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
pub type StreamPutFinalizeCommandIdTestHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StreamPutPendingDrainTestEvent {
    Initial,
    LateConflict,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StreamPutPendingDrainTestAction {
    Continue,
    RetryableFailure,
    IrrevocableFailure,
    ExpireOuterBudget,
}

#[cfg(test)]
type StreamPutPendingDrainTestHook =
    Arc<dyn Fn(StreamPutPendingDrainTestEvent) -> StreamPutPendingDrainTestAction + Send + Sync>;

#[cfg(test)]
type DirectPutPendingDrainTestHook = Arc<dyn Fn() -> bool + Send + Sync>;

#[cfg(test)]
type DirectPutSnapshotReadTestHook = Arc<dyn Fn() -> Result<(), ObjectPgActionError> + Send + Sync>;

#[cfg(test)]
type DirectPutPendingInstallUncertaintyTestHook = Arc<dyn Fn() -> bool + Send + Sync>;

#[cfg(test)]
type PendingObjectMetadataCommandDrainAttemptTestHook =
    Arc<
        dyn Fn(&MetadataCommandEnvelope, &mut super::RequestWorkBudget)
                -> Result<(), ObjectPgActionError>
            + Send
            + Sync,
    >;

#[cfg(test)]
type BucketDeleteCommandIdTestHook = Arc<dyn Fn() -> bool + Send + Sync>;

#[cfg(test)]
type BucketDeletePendingInstallResponseLossTestHook =
    Arc<dyn Fn(&MetadataCommandEnvelope) -> bool + Send + Sync>;

#[cfg(test)]
type BucketDeleteAdoptedMarkValidationTestHook =
    Arc<dyn Fn(&MetadataCommandEnvelope) -> Result<(), StoreError> + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) type BucketDeleteFinalVisibilityStartTestHook =
    Arc<dyn Fn() -> Result<(), StoreError> + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) type BucketDeleteFinalVisibilityProvenTestHook =
    Arc<dyn Fn() -> Result<(), StoreError> + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
pub type BucketDeleteReservationWaitReadyTestHook =
    Arc<dyn Fn() -> Result<(), StoreError> + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) type BucketDeletePostReservationProgressTestHook =
    Arc<dyn Fn(u32) -> Result<(), StoreError> + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) type BucketDeleteSemanticPostReservationProgressTestHook =
    Arc<dyn Fn(bool) -> Result<(), StoreError> + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
pub type BucketDeleteStreamCleanupProgressTestHook =
    Arc<dyn Fn(crate::TestBucketDeleteAttemptPhase, u32) -> Result<(), StoreError> + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
pub type BucketDeleteFinalVisibilityProgressTestHook =
    Arc<dyn Fn(u32) -> Result<(), StoreError> + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
pub type BucketDeleteExactDrainProgressTestHook =
    Arc<dyn Fn(crate::TestBucketDeleteAttemptPhase, u32) -> Result<(), StoreError> + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) type BucketDeleteExactDrainStartTestHook =
    Arc<dyn Fn(bool, u32) -> Result<(), StoreError> + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
#[cfg(test)]
type MultipartCompletionBarrierCommandIdTestHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(test)]
type MultipartCompletionPendingBarrierObservedTestHook =
    Arc<dyn Fn(&MetadataCommandEnvelope) -> bool + Send + Sync>;

#[cfg(test)]
type MultipartCompletionBarrierDrainedTestHook = Arc<dyn Fn() -> bool + Send + Sync>;

#[cfg(test)]
type PendingObjectMetadataPartialConflictTestHook =
    Arc<dyn Fn(&MetadataCommandEnvelope) -> bool + Send + Sync>;

#[cfg(test)]
type ObjectMetadataCommandDefinitiveRetryTestHook =
    Arc<dyn Fn(&MetadataCommandEnvelope, &BucketSnapshotLoadError) -> bool + Send + Sync>;

#[cfg(test)]
type BeforeObjectMetadataCommandApplyTestHook =
    Arc<dyn Fn(&MetadataCommandEnvelope) -> bool + Send + Sync>;

#[cfg(test)]
type DirectPutPendingInstalledTestHook =
    Arc<dyn Fn(&MetadataCommandEnvelope) -> bool + Send + Sync>;

#[cfg(test)]
type SnapshotReinspectionTestHook = Arc<dyn Fn() -> Duration + Send + Sync>;

#[cfg(test)]
type BeforeSnapshotReinspectionActionTestHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(test)]
type BeforeObjectMetadataCommandReissueTestHook =
    Arc<dyn Fn(&MetadataCommandEnvelope) + Send + Sync>;

#[cfg(test)]
type ObjectMetadataCommandAbandonedTestHook =
    Arc<dyn Fn(&MetadataCommandEnvelope) + Send + Sync>;

#[cfg(test)]
type PostBudgetMetadataCommandInspectionTestHook = Arc<
    dyn Fn(NodeId, Instant) -> Option<Result<Option<(u64, u64)>, StoreError>> + Send + Sync,
>;

#[cfg(test)]
type PendingCommandRecoveryTimeoutTestHook =
    Arc<dyn Fn(&MetadataCommandEnvelope) -> bool + Send + Sync>;

#[cfg(test)]
type PendingCommandRecoveryWaitedTestHook =
    Arc<dyn Fn(&MetadataCommandEnvelope) -> bool + Send + Sync>;

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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MultipartCompletionAuxiliaryReservationTestEvent {
    ExactPending,
    AfterAuxiliaryRelease,
    PendingDrain,
    MatchingContender,
    BeforeCommandBuild,
    NoSuchUploadCleanup,
    AfterApplyFailure,
}

#[cfg(test)]
type MultipartCompletionAuxiliaryReservationTestHook = Arc<
    dyn Fn(
            MultipartCompletionAuxiliaryReservationTestEvent,
            &BucketWriteReservationProof,
        ) -> bool
        + Send
        + Sync,
>;

#[cfg(test)]
type MetadataCommandTerminalReservationReleaseTestHook =
    Arc<dyn Fn(&MetadataCommandEnvelope) -> Result<(), BucketSnapshotLoadError> + Send + Sync>;

#[cfg(test)]
type MetadataCommandTerminalSlotRemovalTestHook =
    Arc<dyn Fn(&MetadataCommandEnvelope) -> bool + Send + Sync>;

#[cfg(test)]
const GLOBAL_METADATA_COMMAND_TEST_HOOK_SCOPE_ID: usize = 0;

#[cfg(test)]
static BEFORE_METADATA_COMMAND_APPLY_HOOKS: OnceLock<
    Mutex<HashMap<usize, MetadataCommandApplyTestHook>>,
> = OnceLock::new();

#[cfg(any(test, feature = "test-hooks"))]
static AFTER_METADATA_COMMAND_APPLY_HOOKS: OnceLock<
    Mutex<HashMap<usize, MetadataCommandAfterApplyTestHook>>,
> = OnceLock::new();

#[cfg(test)]
static METADATA_COMMAND_APPLY_ATTEMPT_HOOKS: OnceLock<
    Mutex<HashMap<usize, MetadataCommandApplyAttemptTestHook>>,
> = OnceLock::new();

#[cfg(test)]
static METADATA_COMMAND_PROGRESS_RECONSTRUCTION_HOOKS: OnceLock<
    Mutex<HashMap<usize, MetadataCommandProgressReconstructionTestHook>>,
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
static STREAM_PUT_PENDING_DRAIN_HOOKS: OnceLock<
    Mutex<HashMap<usize, StreamPutPendingDrainTestHook>>,
> = OnceLock::new();

#[cfg(test)]
static DIRECT_PUT_PENDING_DRAIN_HOOKS: OnceLock<
    Mutex<HashMap<usize, DirectPutPendingDrainTestHook>>,
> = OnceLock::new();

#[cfg(test)]
static DIRECT_PUT_SNAPSHOT_READ_HOOKS: OnceLock<
    Mutex<HashMap<usize, DirectPutSnapshotReadTestHook>>,
> = OnceLock::new();

#[cfg(test)]
static DIRECT_PUT_PENDING_INSTALL_UNCERTAINTY_HOOKS: OnceLock<
    Mutex<HashMap<usize, DirectPutPendingInstallUncertaintyTestHook>>,
> = OnceLock::new();

#[cfg(test)]
static PENDING_OBJECT_METADATA_COMMAND_DRAIN_ATTEMPT_HOOKS: OnceLock<
    Mutex<HashMap<usize, PendingObjectMetadataCommandDrainAttemptTestHook>>,
> = OnceLock::new();

#[cfg(test)]
static BEFORE_BUCKET_DELETE_COMMAND_ID_HOOKS: OnceLock<
    Mutex<HashMap<usize, BucketDeleteCommandIdTestHook>>,
> = OnceLock::new();

#[cfg(test)]
static AFTER_BUCKET_DELETE_PENDING_INSTALL_RESPONSE_LOSS_HOOKS: OnceLock<
    Mutex<HashMap<usize, BucketDeletePendingInstallResponseLossTestHook>>,
> = OnceLock::new();

#[cfg(test)]
static BEFORE_BUCKET_DELETE_ADOPTED_MARK_VALIDATION_HOOKS: OnceLock<
    Mutex<HashMap<usize, BucketDeleteAdoptedMarkValidationTestHook>>,
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
static AFTER_BUCKET_DELETE_STREAM_CLEANUP_PROGRESS_HOOKS: OnceLock<
    Mutex<HashMap<usize, BucketDeleteStreamCleanupProgressTestHook>>,
> = OnceLock::new();

#[cfg(any(test, feature = "test-hooks"))]
static AFTER_BUCKET_DELETE_FINAL_VISIBILITY_PROGRESS_HOOKS: OnceLock<
    Mutex<HashMap<usize, BucketDeleteFinalVisibilityProgressTestHook>>,
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
static MULTIPART_COMPLETION_PENDING_BARRIER_OBSERVED_HOOKS: OnceLock<
    Mutex<HashMap<usize, MultipartCompletionPendingBarrierObservedTestHook>>,
> = OnceLock::new();

#[cfg(test)]
static MULTIPART_COMPLETION_BARRIER_DRAINED_HOOKS: OnceLock<
    Mutex<HashMap<usize, MultipartCompletionBarrierDrainedTestHook>>,
> = OnceLock::new();

#[cfg(test)]
static PENDING_OBJECT_METADATA_PARTIAL_CONFLICT_HOOKS: OnceLock<
    Mutex<HashMap<usize, PendingObjectMetadataPartialConflictTestHook>>,
> = OnceLock::new();

#[cfg(test)]
static OBJECT_METADATA_COMMAND_DEFINITIVE_RETRY_HOOKS: OnceLock<
    Mutex<HashMap<usize, ObjectMetadataCommandDefinitiveRetryTestHook>>,
> = OnceLock::new();

#[cfg(test)]
static BEFORE_OBJECT_METADATA_COMMAND_APPLY_HOOKS: OnceLock<
    Mutex<HashMap<usize, BeforeObjectMetadataCommandApplyTestHook>>,
> = OnceLock::new();

#[cfg(test)]
static DIRECT_PUT_PENDING_INSTALLED_HOOKS: OnceLock<
    Mutex<HashMap<usize, DirectPutPendingInstalledTestHook>>,
> = OnceLock::new();

#[cfg(test)]
static SNAPSHOT_REINSPECTION_HOOKS: OnceLock<Mutex<HashMap<usize, SnapshotReinspectionTestHook>>> =
    OnceLock::new();

#[cfg(test)]
static BEFORE_SNAPSHOT_REINSPECTION_ACTION_HOOKS: OnceLock<
    Mutex<HashMap<usize, BeforeSnapshotReinspectionActionTestHook>>,
> = OnceLock::new();

#[cfg(test)]
static BEFORE_OBJECT_METADATA_COMMAND_REISSUE_HOOKS: OnceLock<
    Mutex<HashMap<usize, BeforeObjectMetadataCommandReissueTestHook>>,
> = OnceLock::new();

#[cfg(test)]
static OBJECT_METADATA_COMMAND_ABANDONED_HOOKS: OnceLock<
    Mutex<HashMap<usize, ObjectMetadataCommandAbandonedTestHook>>,
> = OnceLock::new();

#[cfg(test)]
static POST_BUDGET_METADATA_COMMAND_INSPECTION_HOOKS: OnceLock<
    Mutex<HashMap<usize, PostBudgetMetadataCommandInspectionTestHook>>,
> = OnceLock::new();

#[cfg(test)]
static PENDING_COMMAND_RECOVERY_TIMEOUT_HOOKS: OnceLock<
    Mutex<HashMap<usize, PendingCommandRecoveryTimeoutTestHook>>,
> = OnceLock::new();

#[cfg(test)]
static PENDING_COMMAND_RECOVERY_WAITED_HOOKS: OnceLock<
    Mutex<HashMap<usize, PendingCommandRecoveryWaitedTestHook>>,
> = OnceLock::new();

#[cfg(test)]
static MULTIPART_COMPLETION_STALE_RETRY_HOOKS: OnceLock<
    Mutex<HashMap<usize, MultipartCompletionStaleRetryTestHook>>,
> = OnceLock::new();

#[cfg(test)]
static MULTIPART_COMPLETION_AUXILIARY_RESERVATION_HOOKS: OnceLock<
    Mutex<HashMap<usize, MultipartCompletionAuxiliaryReservationTestHook>>,
> = OnceLock::new();

#[cfg(test)]
static METADATA_COMMAND_TERMINAL_RESERVATION_RELEASE_HOOKS: OnceLock<
    Mutex<HashMap<usize, MetadataCommandTerminalReservationReleaseTestHook>>,
> = OnceLock::new();

#[cfg(test)]
static METADATA_COMMAND_TERMINAL_SLOT_REMOVAL_HOOKS: OnceLock<
    Mutex<HashMap<usize, MetadataCommandTerminalSlotRemovalTestHook>>,
> = OnceLock::new();

#[cfg(any(test, feature = "test-hooks"))]
static BEFORE_METADATA_COMMAND_APPLY_CONTEXT_HOOKS: OnceLock<
    Mutex<HashMap<usize, MetadataCommandApplyContextTestHook>>,
> = OnceLock::new();

#[cfg(test)]
pub(crate) struct MetadataCommandApplyTestHookGuard {
    scope_id: usize,
}

#[cfg(any(test, feature = "test-hooks"))]
pub struct MetadataCommandAfterApplyTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct MetadataCommandApplyAttemptTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct MetadataCommandProgressReconstructionTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct AbortMultipartPendingInstallTestHookGuard {
    scope_id: usize,
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) struct StreamPutCreatePendingInstallTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct StreamPutCreateCommandIdTestHookGuard {
    scope_id: usize,
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) struct StreamPutFinalizeCommandIdTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct StreamPutPendingDrainTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct DirectPutPendingDrainTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct DirectPutSnapshotReadTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct DirectPutPendingInstallUncertaintyTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct PendingObjectMetadataCommandDrainAttemptTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct BucketDeleteCommandIdTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct BucketDeletePendingInstallResponseLossTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct BucketDeleteAdoptedMarkValidationTestHookGuard {
    scope_id: usize,
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) struct BucketDeleteFinalVisibilityStartTestHookGuard {
    scope_id: usize,
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) struct BucketDeleteFinalVisibilityProvenTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct BucketDeleteReservationWaitReadyTestHookGuard {
    scope_id: usize,
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) struct BucketDeletePostReservationProgressTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct BucketDeleteStreamCleanupProgressTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct BucketDeleteFinalVisibilityProgressTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct BucketDeleteExactDrainProgressTestHookGuard {
    scope_id: usize,
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) struct BucketDeleteExactDrainStartTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct MultipartCompletionBarrierCommandIdTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct MultipartCompletionPendingBarrierObservedTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct MultipartCompletionBarrierDrainedTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct PendingObjectMetadataPartialConflictTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct ObjectMetadataCommandDefinitiveRetryTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct BeforeObjectMetadataCommandApplyTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct DirectPutPendingInstalledTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct SnapshotReinspectionTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct BeforeSnapshotReinspectionActionTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct BeforeObjectMetadataCommandReissueTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct ObjectMetadataCommandAbandonedTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct PostBudgetMetadataCommandInspectionTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct PendingCommandRecoveryTimeoutTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct PendingCommandRecoveryWaitedTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct MultipartCompletionStaleRetryTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct MultipartCompletionAuxiliaryReservationTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct MetadataCommandTerminalReservationReleaseTestHookGuard {
    scope_id: usize,
}

#[cfg(test)]
pub(crate) struct MetadataCommandTerminalSlotRemovalTestHookGuard {
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

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for MetadataCommandAfterApplyTestHookGuard {
    fn drop(&mut self) {
        let hooks = AFTER_METADATA_COMMAND_APPLY_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(test)]
impl Drop for MetadataCommandApplyAttemptTestHookGuard {
    fn drop(&mut self) {
        let hooks =
            METADATA_COMMAND_APPLY_ATTEMPT_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(test)]
impl Drop for MetadataCommandProgressReconstructionTestHookGuard {
    fn drop(&mut self) {
        let hooks = METADATA_COMMAND_PROGRESS_RECONSTRUCTION_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
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
impl Drop for StreamPutPendingDrainTestHookGuard {
    fn drop(&mut self) {
        let hooks = STREAM_PUT_PENDING_DRAIN_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(test)]
impl Drop for DirectPutPendingDrainTestHookGuard {
    fn drop(&mut self) {
        let hooks = DIRECT_PUT_PENDING_DRAIN_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(test)]
impl Drop for DirectPutSnapshotReadTestHookGuard {
    fn drop(&mut self) {
        let hooks = DIRECT_PUT_SNAPSHOT_READ_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(test)]
impl Drop for DirectPutPendingInstallUncertaintyTestHookGuard {
    fn drop(&mut self) {
        let hooks = DIRECT_PUT_PENDING_INSTALL_UNCERTAINTY_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(test)]
impl Drop for PendingObjectMetadataCommandDrainAttemptTestHookGuard {
    fn drop(&mut self) {
        let hooks = PENDING_OBJECT_METADATA_COMMAND_DRAIN_ATTEMPT_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
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

#[cfg(test)]
impl Drop for BucketDeletePendingInstallResponseLossTestHookGuard {
    fn drop(&mut self) {
        let hooks = AFTER_BUCKET_DELETE_PENDING_INSTALL_RESPONSE_LOSS_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(test)]
impl Drop for BucketDeleteAdoptedMarkValidationTestHookGuard {
    fn drop(&mut self) {
        let hooks = BEFORE_BUCKET_DELETE_ADOPTED_MARK_VALIDATION_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
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

#[cfg(test)]
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

#[cfg(test)]
impl Drop for BucketDeleteStreamCleanupProgressTestHookGuard {
    fn drop(&mut self) {
        let hooks = AFTER_BUCKET_DELETE_STREAM_CLEANUP_PROGRESS_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(test)]
impl Drop for BucketDeleteFinalVisibilityProgressTestHookGuard {
    fn drop(&mut self) {
        let hooks = AFTER_BUCKET_DELETE_FINAL_VISIBILITY_PROGRESS_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(test)]
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
impl Drop for MultipartCompletionPendingBarrierObservedTestHookGuard {
    fn drop(&mut self) {
        let hooks = MULTIPART_COMPLETION_PENDING_BARRIER_OBSERVED_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(test)]
impl Drop for MultipartCompletionBarrierDrainedTestHookGuard {
    fn drop(&mut self) {
        let hooks = MULTIPART_COMPLETION_BARRIER_DRAINED_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(test)]
impl Drop for PendingObjectMetadataPartialConflictTestHookGuard {
    fn drop(&mut self) {
        let hooks = PENDING_OBJECT_METADATA_PARTIAL_CONFLICT_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(test)]
impl Drop for ObjectMetadataCommandDefinitiveRetryTestHookGuard {
    fn drop(&mut self) {
        let hooks = OBJECT_METADATA_COMMAND_DEFINITIVE_RETRY_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(test)]
impl Drop for BeforeObjectMetadataCommandApplyTestHookGuard {
    fn drop(&mut self) {
        let hooks = BEFORE_OBJECT_METADATA_COMMAND_APPLY_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(test)]
impl Drop for DirectPutPendingInstalledTestHookGuard {
    fn drop(&mut self) {
        let hooks = DIRECT_PUT_PENDING_INSTALLED_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(test)]
impl Drop for SnapshotReinspectionTestHookGuard {
    fn drop(&mut self) {
        let hooks = SNAPSHOT_REINSPECTION_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(test)]
impl Drop for BeforeSnapshotReinspectionActionTestHookGuard {
    fn drop(&mut self) {
        let hooks = BEFORE_SNAPSHOT_REINSPECTION_ACTION_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(test)]
impl Drop for BeforeObjectMetadataCommandReissueTestHookGuard {
    fn drop(&mut self) {
        let hooks = BEFORE_OBJECT_METADATA_COMMAND_REISSUE_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(test)]
impl Drop for ObjectMetadataCommandAbandonedTestHookGuard {
    fn drop(&mut self) {
        let hooks = OBJECT_METADATA_COMMAND_ABANDONED_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(test)]
impl Drop for PostBudgetMetadataCommandInspectionTestHookGuard {
    fn drop(&mut self) {
        let hooks = POST_BUDGET_METADATA_COMMAND_INSPECTION_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(test)]
impl Drop for PendingCommandRecoveryTimeoutTestHookGuard {
    fn drop(&mut self) {
        let hooks = PENDING_COMMAND_RECOVERY_TIMEOUT_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(test)]
impl Drop for PendingCommandRecoveryWaitedTestHookGuard {
    fn drop(&mut self) {
        let hooks = PENDING_COMMAND_RECOVERY_WAITED_HOOKS
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

#[cfg(test)]
impl Drop for MultipartCompletionAuxiliaryReservationTestHookGuard {
    fn drop(&mut self) {
        let hooks = MULTIPART_COMPLETION_AUXILIARY_RESERVATION_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(test)]
impl Drop for MetadataCommandTerminalReservationReleaseTestHookGuard {
    fn drop(&mut self) {
        let hooks = METADATA_COMMAND_TERMINAL_RESERVATION_RELEASE_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
        hooks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.scope_id);
    }
}

#[cfg(test)]
impl Drop for MetadataCommandTerminalSlotRemovalTestHookGuard {
    fn drop(&mut self) {
        let hooks = METADATA_COMMAND_TERMINAL_SLOT_REMOVAL_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()));
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

fn maybe_run_after_metadata_command_apply_hook(
    _scope_id: usize,
    _node_id: NodeId,
    _command: &MetadataCommandEnvelope,
) -> Result<(), StoreError> {
    #[cfg(any(test, feature = "test-hooks"))]
    {
        let hook = AFTER_METADATA_COMMAND_APPLY_HOOKS
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

fn maybe_run_metadata_command_apply_attempt_hook(
    _scope_id: usize,
    _command: &MetadataCommandEnvelope,
) -> Result<(), StoreError> {
    #[cfg(test)]
    {
        let hook = METADATA_COMMAND_APPLY_ATTEMPT_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&_scope_id)
            .cloned();
        if let Some(hook) = hook {
            hook(_command)?;
        }
    }
    Ok(())
}

fn maybe_run_metadata_command_progress_reconstruction_hook(
    _scope_id: usize,
    _command: &MetadataCommandEnvelope,
) -> Result<(), StoreError> {
    #[cfg(test)]
    {
        let hook = METADATA_COMMAND_PROGRESS_RECONSTRUCTION_HOOKS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&_scope_id)
            .cloned();
        if let Some(hook) = hook {
            hook(_command)?;
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
fn maybe_run_stream_put_pending_drain_hook(
    scope_id: usize,
    event: StreamPutPendingDrainTestEvent,
    work_budget: &mut super::RequestWorkBudget,
) -> Result<(), ObjectPgActionError> {
    let action = STREAM_PUT_PENDING_DRAIN_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&scope_id)
        .map_or(StreamPutPendingDrainTestAction::Continue, |hook| hook(event));
    match action {
        StreamPutPendingDrainTestAction::Continue => Ok(()),
        StreamPutPendingDrainTestAction::RetryableFailure => Err(ObjectPgActionError::Store(
            StoreError::MetadataCommandContention {
                context: "injected stream PUT pending drain contention",
            },
        )),
        StreamPutPendingDrainTestAction::IrrevocableFailure => Err(ObjectPgActionError::Store(
            StoreError::MetadataCommandIrrevocableConvergencePending {
                pg_id: 1,
                cluster_epoch: ClusterEpoch::INITIAL,
                log_index: 1,
            },
        )),
        StreamPutPendingDrainTestAction::ExpireOuterBudget => {
            work_budget.expire_for_test();
            Ok(())
        }
    }
}

#[cfg(test)]
pub(super) fn maybe_run_direct_put_pending_drain_hook(
    scope_id: usize,
    work_budget: &mut super::RequestWorkBudget,
) {
    let expire = DIRECT_PUT_PENDING_DRAIN_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&scope_id)
        .is_some_and(|hook| hook());
    if expire {
        work_budget.expire_for_test();
    }
}

#[cfg(test)]
pub(super) fn maybe_run_direct_put_snapshot_read_hook(
    scope_id: usize,
) -> Result<(), ObjectPgActionError> {
    let hook = DIRECT_PUT_SNAPSHOT_READ_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&scope_id)
        .cloned();
    hook.map_or(Ok(()), |hook| hook())
}

#[cfg(test)]
pub(super) fn maybe_run_direct_put_pending_install_uncertainty_hook(
    scope_id: usize,
    work_budget: &mut super::RequestWorkBudget,
) {
    let expire = DIRECT_PUT_PENDING_INSTALL_UNCERTAINTY_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&scope_id)
        .is_some_and(|hook| hook());
    if expire {
        work_budget.expire_for_test();
    }
}

#[cfg(test)]
pub(super) fn maybe_run_pending_object_metadata_command_drain_attempt_hook(
    scope_id: usize,
    command: &MetadataCommandEnvelope,
    work_budget: &mut super::RequestWorkBudget,
) -> Result<(), ObjectPgActionError> {
    let hook = PENDING_OBJECT_METADATA_COMMAND_DRAIN_ATTEMPT_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&scope_id)
        .cloned();
    match hook {
        Some(hook) => hook(command, work_budget),
        None => Ok(()),
    }
}

#[cfg(test)]
fn maybe_run_before_bucket_delete_command_id_hook(
    scope_id: usize,
    work_budget: &mut super::RequestWorkBudget,
) {
    let expire = BEFORE_BUCKET_DELETE_COMMAND_ID_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&scope_id)
        .is_some_and(|hook| hook());
    if expire {
        work_budget.expire_for_test();
    }
}

#[cfg(test)]
fn maybe_run_after_bucket_delete_pending_install_response_loss_hook(
    scope_id: usize,
    command: &MetadataCommandEnvelope,
) -> bool {
    AFTER_BUCKET_DELETE_PENDING_INSTALL_RESPONSE_LOSS_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&scope_id)
        .is_some_and(|hook| hook(command))
}

#[cfg(test)]
fn maybe_run_before_bucket_delete_adopted_mark_validation_hook(
    scope_id: usize,
    command: &MetadataCommandEnvelope,
) -> Result<(), StoreError> {
    let hook = BEFORE_BUCKET_DELETE_ADOPTED_MARK_VALIDATION_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&scope_id)
        .cloned();
    match hook {
        Some(hook) => hook(command),
        None => Ok(()),
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
fn maybe_run_after_bucket_delete_stream_cleanup_progress_hook(
    _scope_id: usize,
    _phase: BucketDeleteAttemptPhase,
    _next_object_pg_id: u32,
) -> Result<(), StoreError> {
    let hook = AFTER_BUCKET_DELETE_STREAM_CLEANUP_PROGRESS_HOOKS
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
fn maybe_run_after_bucket_delete_final_visibility_progress_hook(
    _scope_id: usize,
    _next_object_pg_id: u32,
) -> Result<(), StoreError> {
    let hook = AFTER_BUCKET_DELETE_FINAL_VISIBILITY_PROGRESS_HOOKS
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
fn maybe_run_multipart_completion_pending_barrier_observed_hook(
    scope_id: usize,
    command: &MetadataCommandEnvelope,
    work_budget: &mut super::RequestWorkBudget,
) {
    let hook = MULTIPART_COMPLETION_PENDING_BARRIER_OBSERVED_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&scope_id)
        .cloned();
    if hook.is_some_and(|hook| hook(command)) {
        work_budget.expire_for_test();
    }
}

#[cfg(test)]
fn maybe_run_multipart_completion_barrier_drained_hook(
    scope_id: usize,
    work_budget: &mut super::RequestWorkBudget,
) {
    let hook = MULTIPART_COMPLETION_BARRIER_DRAINED_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&scope_id)
        .cloned();
    if hook.is_some_and(|hook| hook()) {
        work_budget.expire_for_test();
    }
}

#[cfg(test)]
pub(super) fn maybe_force_pending_object_metadata_partial_conflict_hook(
    scope_id: usize,
    command: &MetadataCommandEnvelope,
) -> bool {
    let hook = PENDING_OBJECT_METADATA_PARTIAL_CONFLICT_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&scope_id)
        .cloned();
    hook.is_some_and(|hook| hook(command))
}

#[cfg(test)]
fn maybe_run_object_metadata_command_definitive_retry_hook(
    scope_id: usize,
    command: &MetadataCommandEnvelope,
    source: &BucketSnapshotLoadError,
    work_budget: &mut super::RequestWorkBudget,
) {
    let hook = OBJECT_METADATA_COMMAND_DEFINITIVE_RETRY_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&scope_id)
        .cloned();
    if hook.is_some_and(|hook| hook(command, source)) {
        work_budget.expire_for_test();
    }
}

#[cfg(test)]
fn maybe_run_before_object_metadata_command_apply_hook(
    scope_id: usize,
    command: &MetadataCommandEnvelope,
    work_budget: &mut super::RequestWorkBudget,
) {
    let hook = BEFORE_OBJECT_METADATA_COMMAND_APPLY_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&scope_id)
        .cloned();
    if hook.is_some_and(|hook| hook(command)) {
        work_budget.expire_for_test();
    }
}

#[cfg(test)]
pub(super) fn maybe_run_direct_put_pending_installed_hook(
    scope_id: usize,
    command: &MetadataCommandEnvelope,
    work_budget: &mut super::RequestWorkBudget,
) {
    let hook = DIRECT_PUT_PENDING_INSTALLED_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&scope_id)
        .cloned();
    if hook.is_some_and(|hook| hook(command)) {
        work_budget.expire_for_test();
    }
}

#[cfg(test)]
pub(super) fn maybe_run_snapshot_reinspection_hook(
    scope_id: usize,
    work_budget: &mut super::RequestWorkBudget,
) {
    let hook = SNAPSHOT_REINSPECTION_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&scope_id)
        .cloned();
    if let Some(hook) = hook {
        work_budget.reset_for_test(hook());
    }
}

#[cfg(test)]
pub(super) fn maybe_run_before_snapshot_reinspection_action_hook(scope_id: usize) {
    let hook = BEFORE_SNAPSHOT_REINSPECTION_ACTION_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&scope_id)
        .cloned();
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
fn maybe_run_before_object_metadata_command_reissue_hook(
    scope_id: usize,
    command: &MetadataCommandEnvelope,
) {
    let hook = BEFORE_OBJECT_METADATA_COMMAND_REISSUE_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(&scope_id)
        .cloned();
    if let Some(hook) = hook {
        hook(command);
    }
}

#[cfg(test)]
fn maybe_run_object_metadata_command_abandoned_hook(
    scope_id: usize,
    command: &MetadataCommandEnvelope,
) {
    let hook = OBJECT_METADATA_COMMAND_ABANDONED_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&scope_id)
        .cloned();
    if let Some(hook) = hook {
        hook(command);
    }
}

#[cfg(test)]
pub(super) fn maybe_run_post_budget_metadata_command_inspection_hook(
    scope_id: usize,
    node_id: NodeId,
    deadline: Instant,
) -> Option<Result<Option<(u64, u64)>, StoreError>> {
    POST_BUDGET_METADATA_COMMAND_INSPECTION_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&scope_id)
        .cloned()
        .and_then(|hook| hook(node_id, deadline))
}

#[cfg(test)]
pub(super) fn maybe_run_pending_command_recovery_timeout_hook(
    scope_id: usize,
    command: &MetadataCommandEnvelope,
    work_budget: &mut super::RequestWorkBudget,
) {
    let hook = PENDING_COMMAND_RECOVERY_TIMEOUT_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&scope_id)
        .cloned();
    if hook.is_some_and(|hook| hook(command)) {
        work_budget.expire_for_test();
    }
}

#[cfg(test)]
pub(super) fn maybe_run_pending_command_recovery_waited_hook(
    scope_id: usize,
    command: &MetadataCommandEnvelope,
    work_budget: &mut super::RequestWorkBudget,
) {
    let hook = PENDING_COMMAND_RECOVERY_WAITED_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&scope_id)
        .cloned();
    if hook.is_some_and(|hook| hook(command)) {
        work_budget.expire_for_test();
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

#[cfg(test)]
fn maybe_run_multipart_completion_auxiliary_reservation_hook(
    scope_id: usize,
    event: MultipartCompletionAuxiliaryReservationTestEvent,
    proof: &BucketWriteReservationProof,
    work_budget: &mut super::RequestWorkBudget,
) -> bool {
    let hook = MULTIPART_COMPLETION_AUXILIARY_RESERVATION_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&scope_id)
        .cloned();
    if hook.is_some_and(|hook| hook(event, proof)) {
        work_budget.expire_for_test();
        true
    } else {
        false
    }
}

#[cfg(test)]
pub(super) fn maybe_run_metadata_command_terminal_reservation_release_hook(
    scope_id: usize,
    command: &MetadataCommandEnvelope,
) -> Result<(), BucketSnapshotLoadError> {
    let hooks = METADATA_COMMAND_TERMINAL_RESERVATION_RELEASE_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let hook = hooks
        .get(&scope_id)
        .or_else(|| hooks.get(&GLOBAL_METADATA_COMMAND_TEST_HOOK_SCOPE_ID))
        .cloned();
    match hook {
        Some(hook) => hook(command),
        None => Ok(()),
    }
}

#[cfg(test)]
pub(super) fn maybe_run_metadata_command_terminal_slot_removal_hook(
    scope_id: usize,
    command: &MetadataCommandEnvelope,
) -> bool {
    let hooks = METADATA_COMMAND_TERMINAL_SLOT_REMOVAL_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let hook = hooks
        .get(&scope_id)
        .or_else(|| hooks.get(&GLOBAL_METADATA_COMMAND_TEST_HOOK_SCOPE_ID))
        .cloned();
    hook.is_some_and(|hook| hook(command))
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn metadata_command_apply_test_context(
    node_id: NodeId,
    command: &MetadataCommandEnvelope,
) -> MetadataCommandApplyTestContext {
    let (kind, bucket, key) = match command.payload() {
        MetadataCommandPayload::CreateBucket(command) => (
            MetadataCommandApplyTestKind::CreateBucket,
            Some(command.bucket().name.clone()),
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
            Some(command.upload().bucket.clone()),
            Some(command.upload().key.clone()),
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
    pub(super) progress: MetadataCommandApplyProgress,
    pub(super) may_have_applied: bool,
    pub(super) source: BucketSnapshotLoadError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MetadataCommandApplyProgress {
    Abortable,
    PublicationStarted,
    Witnessed,
    PublicationUnconfirmed,
    Published,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MetadataCommandApplyProgressProvenance {
    /// The caller owns this command's fanout and has retained all progress
    /// observed by earlier attempts in this process.
    Authoritative,
    /// The command was recovered from a durable pending slot, so an Abortable
    /// value is provisional until acting-set state is reconstructed.
    RecoveredPending,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MetadataCommandPublicationStartPolicy {
    Required,
    #[cfg(test)]
    RawFanoutTestBypass,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct MetadataCommandApplyAttemptContext {
    pub(super) progress: MetadataCommandApplyProgress,
    pub(super) deadline: Instant,
    pub(super) provenance: MetadataCommandApplyProgressProvenance,
    pub(super) publication_start: MetadataCommandPublicationStartPolicy,
}

impl MetadataCommandApplyProgress {
    fn after_dispatch(self, is_primary: bool) -> Self {
        if is_primary {
            self.merge(Self::PublicationUnconfirmed)
        } else {
            self.merge(Self::Witnessed)
        }
    }

    pub(super) fn merge(self, other: Self) -> Self {
        fn rank(progress: MetadataCommandApplyProgress) -> u8 {
            match progress {
                MetadataCommandApplyProgress::Abortable => 0,
                MetadataCommandApplyProgress::PublicationStarted => 1,
                MetadataCommandApplyProgress::Witnessed => 2,
                MetadataCommandApplyProgress::PublicationUnconfirmed => 3,
                MetadataCommandApplyProgress::Published => 4,
            }
        }
        if rank(self) >= rank(other) {
            self
        } else {
            other
        }
    }

    pub(super) fn is_abortable(self) -> bool {
        self == Self::Abortable
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MetadataCommandApplyOutcome {
    Converged,
    PublishedPendingRecovery,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MetadataCommandConvergenceRequirement {
    AllowRecoveryHandoff,
    RequireAllReplicas,
}

pub(super) enum MetadataCommandBudgetExhaustionOutcome {
    PublishedPendingRecovery,
    Error(StoreError),
}

pub(super) enum MetadataCommandAbandonmentObservation {
    Observed(bool),
    Retry,
    PublishedPendingRecovery,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct MetadataCommandFinishPolicy {
    pub(super) clear_pending_on_zero_apply: bool,
    pub(super) retry_partial_exact_conflict: bool,
    pub(super) convergence_requirement: MetadataCommandConvergenceRequirement,
    pub(super) progress_provenance: MetadataCommandApplyProgressProvenance,
}

#[derive(Debug)]
struct MetadataCommandApplyAttemptFailure {
    failure: MetadataCommandApplyFailure,
    apply_error_kind: Option<MetadataCommandApplyErrorKind>,
}

impl MetadataCommandApplyAttemptFailure {
    fn before_apply_with_progress(
        applied_nodes: usize,
        progress: MetadataCommandApplyProgress,
        source: impl Into<BucketSnapshotLoadError>,
    ) -> Self {
        Self {
            failure: MetadataCommandApplyFailure {
                applied_nodes,
                progress,
                may_have_applied: false,
                source: source.into(),
            },
            apply_error_kind: None,
        }
    }

    fn after_apply_dispatch_with_progress(
        applied_nodes: usize,
        progress: MetadataCommandApplyProgress,
        is_primary: bool,
        source: impl Into<BucketSnapshotLoadError>,
    ) -> Self {
        Self {
            failure: MetadataCommandApplyFailure {
                applied_nodes,
                progress: progress.after_dispatch(is_primary),
                may_have_applied: true,
                source: source.into(),
            },
            apply_error_kind: Some(MetadataCommandApplyErrorKind::MayHaveApplied),
        }
    }

    fn from_apply_call_error_with_progress(
        applied_nodes: usize,
        progress: MetadataCommandApplyProgress,
        is_primary: bool,
        source: MetadataCommandApplyError,
    ) -> Self {
        let kind = source.kind();
        let source = source.into_source();
        let mut failure = if kind == MetadataCommandApplyErrorKind::MayHaveApplied {
            Self::after_apply_dispatch_with_progress(
                applied_nodes,
                progress,
                is_primary,
                source,
            )
        } else {
            Self::before_apply_with_progress(applied_nodes, progress, source)
        };
        failure.apply_error_kind = Some(kind);
        failure
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FinishPendingMetadataCommandResult {
    Applied,
    PublishedPendingRecovery,
    Abandoned,
    TerminalCleanupPending { applied: bool },
    RetryPartialExactConflict,
}

enum MultipartUploadCreatePreparation {
    Authorized(CreateMultipartUploadInput),
    #[cfg(test)]
    Durable {
        request: CreateMultipartUploadReq,
        ordered_id_key: Option<MultipartUploadIdKey>,
    },
}

#[derive(Default)]
struct AuthorizedMultipartUploadCreateIssuance {
    issued: Option<(
        MultipartUploadIdKey,
        CreateMultipartUploadInput,
        CreateMultipartUploadReq,
    )>,
}

impl AuthorizedMultipartUploadCreateIssuance {
    fn prepare(
        &mut self,
        upload_id_key: &MultipartUploadIdKey,
        bucket: &BucketName,
        key: &ObjectKey,
        input: CreateMultipartUploadInput,
    ) -> Result<CreateMultipartUploadReq, StoreError> {
        if let Some((issued_key, issued_input, request)) = &self.issued {
            if issued_key == upload_id_key
                && issued_input == &input
                && request.bucket == *bucket
                && request.key == *key
            {
                return Ok(request.clone());
            }
        }

        let upload_id = upload_id_key
            .issue(bucket, key, &input.initiator.principal)
            .map_err(|_| StoreError::MultipartUploadIdIssuanceFailed)?;
        let request = CreateMultipartUploadReq::from_authorized_input(
            upload_id,
            bucket.clone(),
            key.clone(),
            input.clone(),
        );
        self.issued = Some((upload_id_key.clone(), input, request.clone()));
        Ok(request)
    }
}

#[cfg(test)]
mod authorized_multipart_upload_create_issuance_tests {
    use super::*;

    fn input() -> CreateMultipartUploadInput {
        CreateMultipartUploadInput {
            tags: None,
            metadata_blob: SerializedMetadataBlob::default(),
            system_metadata_blob: SerializedSystemMetadataBlob::default(),
            initiator: OwnerIdentity::from_principal("initiator"),
            owner: OwnerIdentity::from_principal("owner"),
            acl_grants: AclGrants::default(),
            public_read: false,
            object_lock: ObjectLockState::default(),
            checksum: None,
            encryption: ObjectEncryption::None,
        }
    }

    #[test]
    fn authorized_multipart_create_issuance_is_stable_only_for_the_same_input_and_route() {
        let upload_id_key = MultipartUploadIdKey::from_bytes([0x5a; 32]);
        let bucket = BucketName::try_from("bucket".to_string()).unwrap();
        let key = ObjectKey::try_from("key".to_string()).unwrap();
        let other_key = ObjectKey::try_from("other-key".to_string()).unwrap();
        let mut issuance = AuthorizedMultipartUploadCreateIssuance::default();

        let first = issuance
            .prepare(&upload_id_key, &bucket, &key, input())
            .unwrap();
        let retried = issuance
            .prepare(&upload_id_key, &bucket, &key, input())
            .unwrap();
        assert_eq!(retried, first);

        let crossed_route = issuance
            .prepare(&upload_id_key, &bucket, &other_key, input())
            .unwrap();
        assert_eq!(crossed_route.bucket, bucket);
        assert_eq!(crossed_route.key, other_key);
        assert!(upload_id_key.authenticates(
            &crossed_route.bucket,
            &crossed_route.key,
            &crossed_route.upload_id
        ));
        assert!(!upload_id_key.authenticates(&first.bucket, &first.key, &crossed_route.upload_id));

        let replacement_upload_id_key = MultipartUploadIdKey::from_bytes([0xa5; 32]);
        let replaced_bucket = issuance
            .prepare(&replacement_upload_id_key, &bucket, &other_key, input())
            .unwrap();
        assert!(replacement_upload_id_key.authenticates(
            &replaced_bucket.bucket,
            &replaced_bucket.key,
            &replaced_bucket.upload_id
        ));
        assert!(!upload_id_key.authenticates(
            &replaced_bucket.bucket,
            &replaced_bucket.key,
            &replaced_bucket.upload_id
        ));
    }
}

// Metadata routing moves incrementally in Phase 6. Single-PG bucket/object
// operations route through the local metadata PG primary; composite scans fan
// out across routed PG primaries and merge locally.
