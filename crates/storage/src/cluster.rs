use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Condvar, Mutex, RwLock, Weak};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use ec::{EcConfig, ErasureCodec};
use placement::NodeId;
use ring::rand::SecureRandom;

pub use crate::node_client::LocalUnixStorageNodeClientAdmissionSettings;
pub use local::{
    LocalClusterMap, LocalNodeStore, LocalNodeStoreConfig, LocalPgRoute,
    LocalUnixBucketWriteReservationNodeClientConfig, LocalUnixMetadataCommandNodeClientConfig,
    LocalUnixObjectGenerationMetadataNodeClientConfig,
    LocalUnixObjectListingMetadataNodeClientConfig, LocalUnixObjectVersionMetadataNodeClientConfig,
    LocalUnixShardNodeClientConfig, LocalUnixStorageNodeClientConfig,
};
use local::{LocalClusterRuntimeState, MetadataCommandRecoveryAdmission};
pub use request_ops::BucketIdentityGenerations;

use crate::control_plane::{
    ClusterRuntimeMapSnapshot, ControlPlaneError, ControlPlaneRuntimeMapSource, PgMetadataProof,
    PgRouteSnapshot,
};
use crate::error::{ClusterBuildError, PgMetadataTransferError, ShardIoError, StoreError};
#[cfg(test)]
use crate::metadata_command::CommitDirectPutObjectCommand;
use crate::metadata_command::{
    metadata_command_log_hash, AbortStreamUploadCommand, AppendStreamSegmentCommand,
    BucketWriteReservationProof, CreateMultipartUploadCommand, CreateStreamUploadCommand,
    DeleteObjectVersionTarget, MetadataCommandEnvelope, MetadataCommandId, MetadataCommandLogIndex,
    MetadataCommandPayload, MetadataCommandReplicaState, MetadataTransferCommand,
    ObjectPayloadReclaimCommand, ReleaseObjectGenerationCommand, ReserveObjectGenerationCommand,
    ReserveObjectVersionCommand,
};
#[cfg(any(test, feature = "test-hooks"))]
use crate::node::SharedStorageNode;
use crate::node_client::{
    BuildCreateStreamUploadCommandReq, BuildDirectPutCommitCommandReq,
    CreateStreamUploadPrecondition, MetadataCommandNodeClient, ObjectListingMetadataNodeClient,
    ShardAckNodeClient, StorageNodeClient,
};
pub use crate::peering::PgMetadataTransferArtifact;
use crate::peering::{
    build_pg_metadata_transfer_artifact_from_retained_log_entries,
    build_pg_peering_replay_plan_from_retained_log_entries,
    rebase_pg_metadata_transfer_artifact_commands,
    reconstruct_pg_peering_from_primary_retained_log, PgMetadataTransferBaseKind,
    PgPeeringReconstructionDecision, PgPeeringReconstructionError, PgPeeringReconstructionFailure,
    PgPeeringReplicaReconstructionInput,
};
use crate::pg_store::{
    MetadataCommandCheckpoint, MetadataCommandLogCompactionStatus,
    PgClusterMapHistoryReferenceSummary,
};
use crate::storage_rpc::{
    StorageRpcErrorCode, STORAGE_RPC_MAX_METADATA_COMMAND_CHECKPOINT_CANDIDATES,
    STORAGE_RPC_MAX_METADATA_COMMAND_LOG_ENTRY_RANGE_ENTRIES, STORAGE_RPC_MAX_PAYLOAD_LEN,
};
#[cfg(test)]
use crate::traits::PgMetadataStore;
use crate::types::{
    BucketName, BucketWriteDrainRecord, BucketWriteReservationRecord, ClusterEpoch,
    CommitDirectPutObjectReq, CreateStreamUploadReq, DataPgId, DirectPutCommitSnapshot,
    DirectPutWrittenSegment, EcShape, FinalizeDirectPutObjectOutcome, GenerationId,
    MultipartUploadRecord, ObjectEncryption, ObjectKey, ObjectLayout, ObjectSegmentRecord, PgId,
    PgState, PlacedSegmentShardBackfillClaimAcquire, PlacedSegmentShardBackfillClaimAcquireParams,
    PlacedSegmentShardBackfillClaimRecord, PlacedSegmentShardBackfillRecord,
    PlacedSegmentShardBackfillWorkItem, PlacedSegmentShardRepairClaimAcquire,
    PlacedSegmentShardRepairClaimAcquireParams, PlacedSegmentShardRepairClaimRecord,
    PlacedSegmentShardRepairRecord, PlacedSegmentShardRepairWorkItem,
    PrepareStreamUploadSegmentAppendReq, RouteMapValidity, SegmentStoredBytesRequest, SessionId,
    ShardIndex, ShardKey, ShardScavengerObservation, ShardScavengerObservationKey,
    ShardScavengerObservationReason, ShardScavengerObservationRecord,
    ShardScavengerPayloadReference, ShardScavengerPlacedShardSetReference,
    StreamUploadCommandRecord, StreamUploadRecord, StreamUploadSegmentRecord, StreamUploadState,
    StreamUploadTarget, VersionId, WriteAck, WrittenShardAck,
};
#[cfg(test)]
use crate::types::{
    MultipartReclaimRecord, ObjectSegmentsReclaimRecord, ObjectSegmentsReclaimSegmentRecord,
    PutLiveObjectReq,
};
use crate::ObjectEtag;
use crate::{BucketSnapshotLoadError, MetadataError, ObjectPgActionError};

mod local;
mod request_ops;

const DIRECT_PUT_STALE_COMMIT_RETRIES: usize = 16;
const DIRECT_PUT_STALE_COMMIT_RETRY_BUDGET: Duration = Duration::from_secs(1);
const DIRECT_PUT_METADATA_RETRY_BUDGET: Duration = Duration::from_secs(10);
const OBJECT_PG_EMPTY_LOG_CONFLICT_RETRIES: usize = 16;
const OBJECT_GENERATION_RESERVATION_RETRY_BUDGET: Duration = Duration::from_secs(10);
const OBJECT_VERSION_RESERVATION_RETRY_BUDGET: Duration = Duration::from_secs(10);
const OBJECT_VERSION_RESERVATION_RETRY_ATTEMPTS: usize = 64;
pub(super) const BUCKET_WRITE_DRAIN_RETRY_BUDGET: Duration = Duration::from_secs(10);
const PUT_OBJECT_STREAM_CREATE_RETRY_BUDGET: Duration = Duration::from_secs(10);
const METADATA_CONTENTION_BACKOFF_INITIAL: Duration = Duration::from_millis(1);
const METADATA_CONTENTION_BACKOFF_MAX: Duration = Duration::from_millis(25);
const PLACED_SEGMENT_SHARD_BACKFILL_CANDIDATE_SCAN_LIMIT: usize = 256;
const METADATA_COMMAND_CHECKPOINT_RECORD_LIMIT: usize = 4;
const METADATA_COMMAND_CHECKPOINT_MIN_LOG_DISTANCE: u64 = 64;
const METADATA_COMMAND_CHECKPOINT_FRAME_RISK_BYTES: usize = STORAGE_RPC_MAX_PAYLOAD_LEN * 3 / 4;

#[derive(Debug)]
pub(super) struct RequestWorkBudget {
    started: Instant,
    budget: Duration,
    attempts: usize,
    contention_retries: usize,
    max_attempts: Option<usize>,
    operation: &'static str,
    pg_id: Option<PgId>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DurablePlacedSegmentShardRepairEnqueueSummary {
    pub scanned: usize,
    pub enqueued: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MetadataCommandCheckpointRecordSummary {
    pub scanned: usize,
    pub recorded: usize,
    pub already_current: usize,
    pub skipped_cadence: usize,
    pub skipped_inactive: usize,
    pub skipped_empty: usize,
    pub skipped_stale_epoch: usize,
    pub compacted: usize,
    pub compaction_deleted_entries: u64,
    pub compaction_noop: usize,
    pub compaction_no_checkpoint: usize,
    pub compaction_pending: usize,
    pub compaction_failed: usize,
    pub failed: usize,
    pub limit_reached: bool,
}

impl MetadataCommandCheckpointRecordSummary {
    fn mutations(&self) -> usize {
        self.recorded + self.compacted
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MetadataCommandCheckpointRecordDecision {
    Record,
    AlreadyCurrent,
    SkipCadence,
}

impl RequestWorkBudget {
    fn new(budget: Duration, max_attempts: Option<usize>) -> Self {
        Self {
            started: Instant::now(),
            budget,
            attempts: 0,
            contention_retries: 0,
            max_attempts,
            operation: "unknown",
            pg_id: None,
        }
    }

    fn for_operation(mut self, operation: &'static str) -> Self {
        self.operation = operation;
        self
    }

    fn for_pg(mut self, pg_id: PgId) -> Self {
        self.pg_id = Some(pg_id);
        self
    }

    fn check(&mut self, context: &'static str) -> Result<(), StoreError> {
        if self.started.elapsed() >= self.budget
            || self
                .max_attempts
                .is_some_and(|max_attempts| self.attempts >= max_attempts)
        {
            self.emit_budget_exhausted(context);
            return Err(StoreError::MetadataCommandContention { context });
        }
        self.attempts += 1;
        Ok(())
    }

    fn sleep_after_contention(&mut self, context: &'static str) -> Result<(), StoreError> {
        if self.started.elapsed() >= self.budget
            || self
                .max_attempts
                .is_some_and(|max_attempts| self.attempts >= max_attempts)
        {
            self.emit_budget_exhausted(context);
            return Err(StoreError::MetadataCommandContention { context });
        }
        self.contention_retries = self.contention_retries.saturating_add(1);
        let cap = metadata_contention_backoff_cap(self.contention_retries);
        let remaining = self
            .budget
            .checked_sub(self.started.elapsed())
            .unwrap_or(Duration::ZERO);
        let cap = cap.min(remaining);
        let delay = sleep_for_metadata_contention_cap(cap);
        emit_metadata_contention_backoff(self.operation, self.pg_id, context, delay);
        Ok(())
    }

    fn emit_budget_exhausted(&self, context: &'static str) {
        let _ = observability::emit_metadata_command_budget_exhausted(
            TRACE_TARGET,
            observability::MetadataCommandBudgetExhaustedSummary {
                pg_id: self.pg_id.map(|pg_id| pg_id.get()),
                operation: self.operation,
                context,
                elapsed_us: self.started.elapsed().as_micros(),
                budget_us: self.budget.as_micros(),
                attempts: self.attempts,
                max_attempts: self.max_attempts,
            },
        );
    }
}

pub(super) fn sleep_after_metadata_contention_retry_for(
    operation: &'static str,
    pg_id: Option<PgId>,
    context: &'static str,
    contention_retries: &mut usize,
) {
    *contention_retries = (*contention_retries).saturating_add(1);
    let delay =
        sleep_for_metadata_contention_cap(metadata_contention_backoff_cap(*contention_retries));
    emit_metadata_contention_backoff(operation, pg_id, context, delay);
}

fn sleep_for_metadata_contention_cap(cap: Duration) -> Duration {
    let delay = jittered_metadata_contention_backoff_delay(cap);
    if delay > Duration::ZERO {
        std::thread::sleep(delay);
    }
    delay
}

fn emit_metadata_contention_backoff(
    operation: &'static str,
    pg_id: Option<PgId>,
    context: &'static str,
    delay: Duration,
) {
    let _ = observability::emit_metadata_command_backoff(
        TRACE_TARGET,
        observability::MetadataCommandBackoffSummary {
            pg_id: pg_id.map(|pg_id| pg_id.get()),
            operation,
            context,
            sleep_us: delay.as_micros(),
        },
    );
}

fn jittered_metadata_contention_backoff_delay(cap: Duration) -> Duration {
    let max_nanos = cap.as_nanos().min(u128::from(u64::MAX)) as u64;
    if max_nanos == 0 {
        return Duration::ZERO;
    }
    let mut bytes = [0u8; 8];
    if ring::rand::SystemRandom::new().fill(&mut bytes).is_err() {
        return Duration::from_nanos((max_nanos / 2).max(1));
    }
    Duration::from_nanos((u64::from_le_bytes(bytes) % max_nanos.saturating_add(1)).max(1))
}

fn metadata_contention_backoff_cap(contention_retries: usize) -> Duration {
    let multiplier = 1u32 << contention_retries.saturating_sub(1).min(8);
    METADATA_CONTENTION_BACKOFF_INITIAL
        .saturating_mul(multiplier)
        .min(METADATA_CONTENTION_BACKOFF_MAX)
}

#[cfg(any(test, feature = "test-hooks"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataCommandApplyTestKind {
    CreateBucket,
    PutBucketVersioning,
    PutBucketAcl,
    PutBucketProperty,
    PutBucketSubresource,
    MarkBucketDeleting,
    DeleteFinalizedBucket,
    AdvanceCompletedMultipartUploadSequence,
    ReserveObjectGeneration,
    ReleaseObjectGeneration,
    ReserveObjectVersion,
    CommitDirectPutObject,
    CommitMultipartObject,
    DeleteObjectVersion,
    InsertDeleteMarker,
    PutObjectMetadata,
    CreateStreamUpload,
    AppendStreamSegment,
    AbortStreamUpload,
    CommitStreamPart,
    CreateMultipartUpload,
    AbortMultipartUpload,
    DeleteObjectPayloadReclaim,
    DeleteCompletedMultipartUpload,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectPayloadReclaimAttempt {
    Completed,
    Deferred,
    MissingRoot,
}

#[cfg(any(test, feature = "test-hooks"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataCommandApplyTestContext {
    pub node_id: NodeId,
    pub kind: MetadataCommandApplyTestKind,
    pub bucket: Option<BucketName>,
    pub key: Option<ObjectKey>,
}

#[cfg(any(test, feature = "test-hooks"))]
pub type MetadataCommandApplyContextTestHook =
    Arc<dyn Fn(MetadataCommandApplyTestContext) -> Result<(), StoreError> + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
pub struct MetadataCommandApplyContextTestHookGuard {
    pub(super) scope_id: usize,
}

#[cfg(test)]
pub(crate) struct MetadataCommandRecoveryTestGuard {
    _guard: Box<dyn Send>,
}

const TRACE_TARGET: &str = "storage";

type ShardScavengerLocationIdentity = (u32, u32, ShardKey);
type ShardScavengerRepairIdentity = (u32, ShardKey);

#[derive(Debug, Default)]
struct ShardScavengerReferenceScan {
    locations: HashSet<ShardScavengerLocationIdentity>,
    repair_work_by_shard: HashMap<ShardScavengerRepairIdentity, PlacedSegmentShardRepairWorkItem>,
}

fn conflicting_pending_object_metadata_command(context: &'static str) -> ObjectPgActionError {
    ObjectPgActionError::Store(StoreError::MetadataCommandContention { context })
}

#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PendingMetadataCommandOutcome {
    Applied,
    Abandoned,
    RetryPartialExactConflict,
}

impl PendingMetadataCommandOutcome {
    fn metric_label(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Abandoned => "abandoned",
            Self::RetryPartialExactConflict => "retry_partial_exact_conflict",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetadataCommandRecoveryWaiterOutcome {
    StillPending,
    Applied,
    MissingNotApplied,
    ReplacedNotApplied,
}

impl MetadataCommandRecoveryWaiterOutcome {
    fn metric_label(self) -> &'static str {
        match self {
            Self::StillPending => "waiter_still_pending",
            Self::Applied => "waiter_applied",
            Self::MissingNotApplied => "waiter_missing_not_applied",
            Self::ReplacedNotApplied => "waiter_replaced_not_applied",
        }
    }

    #[cfg(test)]
    fn pending_outcome(self) -> Option<PendingMetadataCommandOutcome> {
        match self {
            Self::StillPending => None,
            Self::Applied => Some(PendingMetadataCommandOutcome::Applied),
            Self::MissingNotApplied | Self::ReplacedNotApplied => {
                Some(PendingMetadataCommandOutcome::RetryPartialExactConflict)
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ExactPendingObjectMetadataCommand<'a> {
    command: &'a MetadataCommandEnvelope,
}

impl<'a> ExactPendingObjectMetadataCommand<'a> {
    /// Caller has already matched this PG-slot command to the request whose
    /// result will be returned to the client.
    pub(super) fn for_checked_request(command: &'a MetadataCommandEnvelope) -> Self {
        Self { command }
    }
}

fn object_payload_reclaim_generation(
    stale_payload: &Option<ObjectPayloadReclaimCommand>,
) -> Option<GenerationId> {
    match stale_payload {
        None => None,
        Some(ObjectPayloadReclaimCommand::Segments(reclaim)) => Some(reclaim.generation_id),
        Some(ObjectPayloadReclaimCommand::Multipart(reclaim)) => Some(reclaim.generation_id),
    }
}

fn delete_object_version_reclaim_generation(
    target: &DeleteObjectVersionTarget,
) -> Option<GenerationId> {
    match target {
        DeleteObjectVersionTarget::DeleteMarker { .. } => None,
        DeleteObjectVersionTarget::Live { generation_id, .. } => Some(*generation_id),
    }
}

fn bucket_snapshot_error_to_object_pg_action_error(
    error: BucketSnapshotLoadError,
) -> ObjectPgActionError {
    match error {
        BucketSnapshotLoadError::Store(error) => ObjectPgActionError::Store(error),
        BucketSnapshotLoadError::Metadata(error) => ObjectPgActionError::Metadata(error),
    }
}

fn object_pg_action_error_to_bucket_snapshot_error(
    error: ObjectPgActionError,
) -> BucketSnapshotLoadError {
    match error {
        ObjectPgActionError::Store(error) => BucketSnapshotLoadError::Store(error),
        ObjectPgActionError::Metadata(error) => BucketSnapshotLoadError::Metadata(error),
        ObjectPgActionError::InvalidRequest { reason } => {
            BucketSnapshotLoadError::Store(StoreError::Io {
                context: "object PG action failed during bucket snapshot operation",
                source: std::io::Error::other(reason),
            })
        }
        ObjectPgActionError::StaleObjectReadSubject => {
            BucketSnapshotLoadError::Store(StoreError::Io {
                context: "object PG action failed during bucket snapshot operation",
                source: std::io::Error::other("stale object read subject"),
            })
        }
        ObjectPgActionError::StaleDirectPutCommitSnapshot => {
            BucketSnapshotLoadError::Store(StoreError::Io {
                context: "object PG action failed during bucket snapshot operation",
                source: std::io::Error::other("stale direct PUT commit snapshot"),
            })
        }
        ObjectPgActionError::StaleStreamFinalizeSnapshot => {
            BucketSnapshotLoadError::Store(StoreError::Io {
                context: "object PG action failed during bucket snapshot operation",
                source: std::io::Error::other("stale stream finalize snapshot"),
            })
        }
        ObjectPgActionError::StaleMultipartCompletionSnapshot => {
            BucketSnapshotLoadError::Store(StoreError::Io {
                context: "object PG action failed during bucket snapshot operation",
                source: std::io::Error::other("stale multipart completion snapshot"),
            })
        }
    }
}

fn stream_create_request_matches_session(
    session: &StreamUploadCommandRecord,
    create: &CreateStreamUploadReq,
) -> bool {
    session.session_id == create.session_id
        && session.bucket == create.bucket
        && session.key == create.key
        && session.target == create.target
        && session.state == StreamUploadState::InProgress
        && session.encryption == create.encryption
}

fn applied_stream_create_command<'a>(
    applied_commands: &'a [MetadataCommandEnvelope],
    create: &CreateStreamUploadReq,
) -> Option<&'a CreateStreamUploadCommand> {
    applied_commands.iter().rev().find_map(|command| {
        let MetadataCommandPayload::CreateStreamUpload(create_command) = command.payload() else {
            return None;
        };
        stream_create_request_matches_session(&create_command.session, create)
            .then_some(create_command.as_ref())
    })
}

fn multipart_create_request_matches_upload(
    upload: &MultipartUploadRecord,
    create: &crate::CreateMultipartUploadReq,
) -> bool {
    upload.upload_id == create.upload_id
        && upload.bucket == create.bucket
        && upload.key == create.key
        && upload.state == crate::UploadState::InProgress
        && upload.tags == create.tags
        && upload.metadata_blob == create.metadata_blob
        && upload.system_metadata_blob == create.system_metadata_blob
        && upload.initiator == create.initiator
        && upload.owner == create.owner
        && upload.acl_grants == create.acl_grants
        && upload.public_read == create.public_read
        && upload.object_lock == create.object_lock
        && upload.checksum == create.checksum
        && upload.encryption == create.encryption
}

fn applied_multipart_create_command<'a>(
    applied_commands: &'a [MetadataCommandEnvelope],
    create: &crate::CreateMultipartUploadReq,
) -> Option<&'a CreateMultipartUploadCommand> {
    applied_commands.iter().rev().find_map(|command| {
        let MetadataCommandPayload::CreateMultipartUpload(create_command) = command.payload()
        else {
            return None;
        };
        multipart_create_request_matches_upload(&create_command.upload, create)
            .then_some(create_command.as_ref())
    })
}

fn pending_command_completes_stream_session(
    command: &MetadataCommandEnvelope,
    bucket: &BucketName,
    key: &ObjectKey,
    session_id: &SessionId,
) -> bool {
    match command.payload() {
        MetadataCommandPayload::CommitDirectPutObject(commit) => {
            commit.matches_stream_session(bucket, key, session_id)
        }
        MetadataCommandPayload::AbortStreamUpload(abort) => {
            abort.bucket == *bucket && abort.key == *key && abort.session_id == *session_id
        }
        MetadataCommandPayload::CommitStreamPart(commit) => {
            commit.bucket == *bucket && commit.key == *key && commit.session_id == *session_id
        }
        MetadataCommandPayload::CommitMultipartObject(commit) => {
            commit.object.bucket == *bucket
                && commit.object.key == *key
                && commit
                    .stream_uploads
                    .iter()
                    .any(|session| session.session_id == *session_id)
        }
        _ => false,
    }
}

#[cfg(any(test, feature = "test-hooks"))]
type StreamAbortHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
type MetadataCommandPendingInstallHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
type DirectPutCommandIdHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
type ObjectGenerationCommandIdHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
type StreamAppendCommandIdHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
type ObjectMetadataReservationAcquiredHook =
    Arc<dyn Fn() -> Result<(), ObjectPgActionError> + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
pub type PayloadShardCleanupTestHook =
    Arc<dyn Fn(&ShardKey) -> Result<(), StoreError> + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
pub type PayloadShardReadTestHook =
    Arc<dyn Fn(&ShardLocation, &ShardKey) -> Result<(), StoreError> + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
pub type PayloadCleanupErrorTestHook = Arc<dyn Fn(&'static str, &StoreError) + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
#[derive(Default)]
struct StorageClusterTestHooks {
    before_stream_abort_storage: Option<StreamAbortHook>,
    before_metadata_command_pending_install: Option<MetadataCommandPendingInstallHook>,
    before_direct_put_command_id: Option<DirectPutCommandIdHook>,
    before_object_generation_command_id: Option<ObjectGenerationCommandIdHook>,
    before_stream_append_command_id: Option<StreamAppendCommandIdHook>,
    after_object_metadata_reservation_acquired: Option<ObjectMetadataReservationAcquiredHook>,
    before_placed_payload_shard_read: Option<PayloadShardReadTestHook>,
    before_placed_payload_shard_delete: Option<PayloadShardCleanupTestHook>,
    before_metadata_primary_payload_ack_delete: Option<PayloadShardCleanupTestHook>,
    best_effort_payload_cleanup_error: Option<PayloadCleanupErrorTestHook>,
}

#[cfg(any(test, feature = "test-hooks"))]
pub struct StreamAbortTestHookGuard {
    hooks: Arc<Mutex<StorageClusterTestHooks>>,
}

#[cfg(any(test, feature = "test-hooks"))]
pub struct MetadataCommandPendingInstallHookGuard {
    hooks: Arc<Mutex<StorageClusterTestHooks>>,
}

#[cfg(any(test, feature = "test-hooks"))]
pub struct DirectPutCommandIdHookGuard {
    hooks: Arc<Mutex<StorageClusterTestHooks>>,
}

#[cfg(any(test, feature = "test-hooks"))]
pub struct ObjectGenerationCommandIdHookGuard {
    hooks: Arc<Mutex<StorageClusterTestHooks>>,
}

#[cfg(any(test, feature = "test-hooks"))]
pub struct StreamAppendCommandIdHookGuard {
    hooks: Arc<Mutex<StorageClusterTestHooks>>,
}

#[cfg(any(test, feature = "test-hooks"))]
pub struct ObjectMetadataReservationAcquiredHookGuard {
    hooks: Arc<Mutex<StorageClusterTestHooks>>,
}

#[cfg(any(test, feature = "test-hooks"))]
pub struct PayloadShardReadTestHookGuard {
    hooks: Arc<Mutex<StorageClusterTestHooks>>,
}

#[cfg(any(test, feature = "test-hooks"))]
enum PayloadCleanupTestHookKind {
    PlacedShardDelete,
    MetadataPrimaryAckDelete,
    BestEffortError,
}

#[cfg(any(test, feature = "test-hooks"))]
pub struct PayloadCleanupTestHookGuard {
    hooks: Arc<Mutex<StorageClusterTestHooks>>,
    kind: PayloadCleanupTestHookKind,
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for StreamAbortTestHookGuard {
    fn drop(&mut self) {
        self.hooks.lock().unwrap().before_stream_abort_storage = None;
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for MetadataCommandPendingInstallHookGuard {
    fn drop(&mut self) {
        self.hooks
            .lock()
            .unwrap()
            .before_metadata_command_pending_install = None;
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for DirectPutCommandIdHookGuard {
    fn drop(&mut self) {
        self.hooks.lock().unwrap().before_direct_put_command_id = None;
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for ObjectGenerationCommandIdHookGuard {
    fn drop(&mut self) {
        self.hooks
            .lock()
            .unwrap()
            .before_object_generation_command_id = None;
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for StreamAppendCommandIdHookGuard {
    fn drop(&mut self) {
        self.hooks.lock().unwrap().before_stream_append_command_id = None;
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for ObjectMetadataReservationAcquiredHookGuard {
    fn drop(&mut self) {
        self.hooks
            .lock()
            .unwrap()
            .after_object_metadata_reservation_acquired = None;
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for PayloadShardReadTestHookGuard {
    fn drop(&mut self) {
        self.hooks.lock().unwrap().before_placed_payload_shard_read = None;
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for PayloadCleanupTestHookGuard {
    fn drop(&mut self) {
        let mut hooks = self.hooks.lock().unwrap();
        match self.kind {
            PayloadCleanupTestHookKind::PlacedShardDelete => {
                hooks.before_placed_payload_shard_delete = None;
            }
            PayloadCleanupTestHookKind::MetadataPrimaryAckDelete => {
                hooks.before_metadata_primary_payload_ack_delete = None;
            }
            PayloadCleanupTestHookKind::BestEffortError => {
                hooks.best_effort_payload_cleanup_error = None;
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardLocation {
    cluster_epoch: ClusterEpoch,
    data_pg_id: DataPgId,
    shard_index: ShardIndex,
    node_id: NodeId,
}

impl ShardLocation {
    pub(crate) fn new(
        cluster_epoch: ClusterEpoch,
        data_pg_id: DataPgId,
        shard_index: ShardIndex,
        node_id: NodeId,
    ) -> Self {
        Self {
            cluster_epoch,
            data_pg_id,
            shard_index,
            node_id,
        }
    }

    pub fn cluster_epoch(&self) -> ClusterEpoch {
        self.cluster_epoch
    }

    pub fn data_pg_id(&self) -> DataPgId {
        self.data_pg_id
    }

    pub fn shard_index(&self) -> ShardIndex {
        self.shard_index
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlacedSegmentShardValidation {
    Valid,
    MissingAck,
    WrongSize { expected: u64, actual: u64 },
    Unreadable { reason: String },
}

impl PlacedSegmentShardValidation {
    #[must_use]
    pub fn is_valid(&self) -> bool {
        matches!(self, Self::Valid)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacedSegmentShardHealth {
    pub shard_index: ShardIndex,
    pub shard_key: ShardKey,
    pub location: ShardLocation,
    pub validation: PlacedSegmentShardValidation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlacedSegmentShardSetRisk {
    Healthy,
    Degraded { tolerance_remaining: usize },
    Unrecoverable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacedSegmentShardSetHealth {
    pub total_shards: usize,
    pub required_shards: usize,
    pub valid_shards: usize,
    pub risk: PlacedSegmentShardSetRisk,
    pub shards: Vec<PlacedSegmentShardHealth>,
}

impl PlacedSegmentShardSetHealth {
    #[must_use]
    pub fn repair_targets(&self) -> Vec<ShardIndex> {
        self.shards
            .iter()
            .filter(|shard| !shard.validation.is_valid())
            .map(|shard| shard.shard_index)
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacedSegmentShardBackfillCopyTarget {
    pub shard_index: ShardIndex,
    pub shard_key: ShardKey,
    pub source: ShardLocation,
    pub destination: ShardLocation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacedSegmentShardBackfillPlan {
    pub source_health: PlacedSegmentShardSetHealth,
    pub desired_health: PlacedSegmentShardSetHealth,
    pub already_present: Vec<ShardIndex>,
    pub copy_targets: Vec<PlacedSegmentShardBackfillCopyTarget>,
    pub reconstruction_targets: Vec<ShardIndex>,
    pub unrecoverable_targets: Vec<ShardIndex>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PlacedSegmentShardBackfillCandidateEnqueueSummary {
    pub scanned: usize,
    pub current_epoch: usize,
    pub already_queued: usize,
    pub already_complete: usize,
    pub enqueued: usize,
    pub unrecoverable: usize,
    pub deferred: usize,
    pub failed: usize,
    pub limit_reached: bool,
}

impl PlacedSegmentShardBackfillPlan {
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.copy_targets.is_empty()
            && self.reconstruction_targets.is_empty()
            && self.unrecoverable_targets.is_empty()
    }

    #[must_use]
    pub fn source_remaining_tolerance(&self) -> u8 {
        let tolerance = match self.source_health.risk {
            PlacedSegmentShardSetRisk::Healthy => self
                .source_health
                .total_shards
                .saturating_sub(self.source_health.required_shards),
            PlacedSegmentShardSetRisk::Degraded {
                tolerance_remaining,
            } => tolerance_remaining,
            PlacedSegmentShardSetRisk::Unrecoverable => 0,
        };
        u8::try_from(tolerance).unwrap_or(u8::MAX)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlacedSegmentShardHealthReadMode {
    CurrentRoute,
    HistoricalInspection,
}

pub struct ObjectPayloadLease {
    cluster: Weak<StorageCluster>,
    storage_clients: Vec<Arc<dyn StorageNodeClient>>,
    runtime_state: Arc<LocalClusterRuntimeState>,
    bucket: BucketName,
    key: ObjectKey,
    generation_id: GenerationId,
    pg_id: u32,
    released: bool,
}

impl ObjectPayloadLease {
    fn new(
        cluster: Weak<StorageCluster>,
        storage_clients: Vec<Arc<dyn StorageNodeClient>>,
        runtime_state: Arc<LocalClusterRuntimeState>,
        bucket: BucketName,
        key: ObjectKey,
        generation_id: GenerationId,
        pg_id: u32,
    ) -> Self {
        Self {
            cluster,
            storage_clients,
            runtime_state,
            bucket,
            key,
            generation_id,
            pg_id,
            released: false,
        }
    }

    pub fn release(mut self) -> ReleasedObjectPayloadLease {
        let remaining = release_object_payload_lease_from_storage_clients(
            &self.storage_clients,
            &self.bucket,
            &self.key,
            self.generation_id,
        );
        self.released = true;
        ReleasedObjectPayloadLease {
            cluster: self.cluster.clone(),
            runtime_state: Arc::clone(&self.runtime_state),
            bucket: self.bucket.clone(),
            key: self.key.clone(),
            generation_id: self.generation_id,
            pg_id: self.pg_id,
            remaining,
        }
    }
}

impl Drop for ObjectPayloadLease {
    fn drop(&mut self) {
        if !self.released {
            let _ = release_object_payload_lease_from_storage_clients(
                &self.storage_clients,
                &self.bucket,
                &self.key,
                self.generation_id,
            );
        }
    }
}

fn release_object_payload_lease_from_storage_clients(
    storage_clients: &[Arc<dyn StorageNodeClient>],
    bucket: &BucketName,
    key: &ObjectKey,
    generation_id: GenerationId,
) -> usize {
    storage_clients
        .iter()
        .map(|storage_node| storage_node.release_object_payload_lease(bucket, key, generation_id))
        .max()
        .unwrap_or(0)
}

pub struct ReleasedObjectPayloadLease {
    cluster: Weak<StorageCluster>,
    runtime_state: Arc<LocalClusterRuntimeState>,
    bucket: BucketName,
    key: ObjectKey,
    generation_id: GenerationId,
    pg_id: u32,
    remaining: usize,
}

impl ReleasedObjectPayloadLease {
    pub fn remaining(&self) -> usize {
        self.remaining
    }

    pub fn payload_reclaim_exists(&self) -> Result<bool, ObjectPgActionError> {
        let Some(cluster) = self.cluster.upgrade() else {
            // The already-acquired lease has been released; if its original
            // cluster handle is gone, conservatively let the caller enqueue a
            // reclaim retry. A worker will drop the item if no reclaim row
            // exists.
            return Ok(true);
        };
        cluster.payload_reclaim_exists(&self.bucket, &self.key, self.generation_id)
    }

    pub fn enqueue_object_payload_reclaim(&self) {
        if let Some(cluster) = self.cluster.upgrade() {
            cluster.enqueue_object_payload_reclaim(&self.bucket, &self.key, self.generation_id);
            return;
        }

        // The cluster handle can be gone after shutdown; keep the existing
        // conservative retry behavior for any worker still draining the queue.
        let _ = self.runtime_state.enqueue_object_payload_reclaim(
            &self.bucket,
            &self.key,
            self.generation_id,
            self.pg_id,
        );
    }
}

/// Cluster-shaped storage handle.
#[derive(Debug, thiserror::Error)]
pub enum StorageClusterRuntimeMapRefreshError {
    #[error("control-plane runtime map refresh failed: {0}")]
    ControlPlane(#[from] ControlPlaneError),
    #[error("refreshed runtime map did not build a storage cluster: {0}")]
    Build(#[from] ClusterBuildError),
    #[error(
        "refreshed runtime map would downgrade storage cluster epoch from {current} to {candidate}"
    )]
    EpochDowngrade {
        current: ClusterEpoch,
        candidate: ClusterEpoch,
    },
    #[error("refreshed runtime map for epoch {candidate} has unbounded route-map validity")]
    UnboundedRouteMapValidity { candidate: ClusterEpoch },
    #[error("storage cluster runtime-map refresh loop interval must be non-zero")]
    RefreshLoopZeroInterval,
    #[error("spawn storage cluster runtime-map refresh loop")]
    RefreshLoopSpawn {
        #[source]
        source: io::Error,
    },
}

#[derive(Clone)]
pub struct StorageCluster {
    local_map: Arc<LocalClusterMap>,
    operation_epoch: ClusterEpoch,
    #[cfg(any(test, feature = "test-hooks"))]
    test_hooks: Arc<Mutex<StorageClusterTestHooks>>,
}

#[derive(Clone)]
pub struct StorageClusterRuntimeMapHandle {
    cluster: Arc<RwLock<Arc<StorageCluster>>>,
    same_epoch_generations: Arc<Mutex<Vec<Weak<StorageCluster>>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageClusterRuntimeMapRefreshLoopSuccess {
    pub cluster_epoch: ClusterEpoch,
    pub route_map_validity: RouteMapValidity,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StorageClusterRuntimeMapRefreshLoopStatus {
    pub attempts: u64,
    pub successes: u64,
    pub failures: u64,
    pub last_success: Option<StorageClusterRuntimeMapRefreshLoopSuccess>,
    pub last_error: Option<String>,
}

pub struct StorageClusterRuntimeMapRefreshLoop {
    stop: Arc<(Mutex<bool>, Condvar)>,
    status: Arc<Mutex<StorageClusterRuntimeMapRefreshLoopStatus>>,
    handle: Option<JoinHandle<()>>,
}

impl StorageClusterRuntimeMapRefreshLoop {
    pub fn status(&self) -> StorageClusterRuntimeMapRefreshLoopStatus {
        self.status
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn stop(&mut self) {
        {
            let (lock, cvar) = &*self.stop;
            let mut stopped = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            *stopped = true;
            cvar.notify_all();
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for StorageClusterRuntimeMapRefreshLoop {
    fn drop(&mut self) {
        self.stop();
    }
}

impl StorageClusterRuntimeMapHandle {
    pub fn new(initial: Arc<StorageCluster>) -> Self {
        Self {
            same_epoch_generations: Arc::new(Mutex::new(vec![Arc::downgrade(&initial)])),
            cluster: Arc::new(RwLock::new(initial)),
        }
    }

    pub fn current(&self) -> Arc<StorageCluster> {
        self.cluster
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn install(
        &self,
        candidate: Arc<StorageCluster>,
    ) -> Result<(), StorageClusterRuntimeMapRefreshError> {
        let mut current = self
            .cluster
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if candidate.cluster_epoch() < current.cluster_epoch() {
            return Err(StorageClusterRuntimeMapRefreshError::EpochDowngrade {
                current: current.cluster_epoch(),
                candidate: candidate.cluster_epoch(),
            });
        }
        if candidate.route_map_valid_until_ms().is_none() {
            return Err(
                StorageClusterRuntimeMapRefreshError::UnboundedRouteMapValidity {
                    candidate: candidate.cluster_epoch(),
                },
            );
        }
        if candidate.cluster_epoch() == current.cluster_epoch() {
            let candidate_validity = candidate.route_map_validity();
            let mut generations = self
                .same_epoch_generations
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            // Same-epoch authoritative refreshes update the control-plane
            // lease for all pinned generations. Requests that pinned an older
            // generation must see both lease extensions and bounded shrinks.
            generations.retain(|generation| {
                let Some(generation) = generation.upgrade() else {
                    return false;
                };
                if generation.cluster_epoch() == candidate.cluster_epoch() {
                    match (
                        generation.route_map_valid_until_ms(),
                        candidate_validity.valid_until_ms(),
                    ) {
                        (Some(current), Some(candidate)) if candidate < current => {
                            generation.cap_route_map_validity(candidate_validity);
                        }
                        (None, Some(_)) => {
                            generation.cap_route_map_validity(candidate_validity);
                        }
                        _ => {
                            generation.extend_route_map_validity(candidate_validity);
                        }
                    }
                    true
                } else {
                    false
                }
            });
            generations.push(Arc::downgrade(&candidate));
        } else {
            let mut generations = self
                .same_epoch_generations
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            generations.clear();
            generations.push(Arc::downgrade(&candidate));
        }
        *current = candidate;
        Ok(())
    }

    fn expire_same_epoch_generations(&self, now_ms: u64) {
        let current_epoch = self.current().cluster_epoch();
        let expiry = RouteMapValidity::until_ms_saturating(now_ms);
        let mut generations = self
            .same_epoch_generations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        generations.retain(|generation| {
            let Some(generation) = generation.upgrade() else {
                return false;
            };
            if generation.cluster_epoch() == current_epoch {
                generation.cap_route_map_validity(expiry);
                true
            } else {
                false
            }
        });
    }

    pub fn refresh_from_control_plane_runtime_map(
        &self,
        control_plane: &impl ControlPlaneRuntimeMapSource,
        authority_now_ms: u64,
    ) -> Result<Arc<StorageCluster>, StorageClusterRuntimeMapRefreshError> {
        let candidate = self
            .current()
            .refresh_from_control_plane_runtime_map(control_plane, authority_now_ms)?;
        self.install(Arc::clone(&candidate))?;
        Ok(candidate)
    }

    pub fn refresh_from_control_plane_runtime_map_with_unix_storage_node_clients(
        &self,
        control_plane: &impl ControlPlaneRuntimeMapSource,
        authority_now_ms: u64,
        admission_settings: LocalUnixStorageNodeClientAdmissionSettings,
    ) -> Result<Arc<StorageCluster>, StorageClusterRuntimeMapRefreshError> {
        let candidate = self
            .current()
            .refresh_from_control_plane_runtime_map_with_unix_storage_node_clients(
                control_plane,
                authority_now_ms,
                admission_settings,
            )?;
        self.install(Arc::clone(&candidate))?;
        Ok(candidate)
    }

    pub fn spawn_control_plane_refresh_loop<S, F>(
        self,
        control_plane: S,
        refresh_interval: Duration,
        authority_now_ms: F,
    ) -> Result<StorageClusterRuntimeMapRefreshLoop, StorageClusterRuntimeMapRefreshError>
    where
        S: ControlPlaneRuntimeMapSource + Send + 'static,
        F: Fn() -> u64 + Send + 'static,
    {
        self.spawn_control_plane_refresh_loop_inner(
            control_plane,
            refresh_interval,
            authority_now_ms,
            None,
        )
    }

    pub fn spawn_control_plane_refresh_loop_with_unix_storage_node_clients<S, F>(
        self,
        control_plane: S,
        refresh_interval: Duration,
        authority_now_ms: F,
        admission_settings: LocalUnixStorageNodeClientAdmissionSettings,
    ) -> Result<StorageClusterRuntimeMapRefreshLoop, StorageClusterRuntimeMapRefreshError>
    where
        S: ControlPlaneRuntimeMapSource + Send + 'static,
        F: Fn() -> u64 + Send + 'static,
    {
        self.spawn_control_plane_refresh_loop_inner(
            control_plane,
            refresh_interval,
            authority_now_ms,
            Some(admission_settings),
        )
    }

    fn spawn_control_plane_refresh_loop_inner<S, F>(
        self,
        control_plane: S,
        refresh_interval: Duration,
        authority_now_ms: F,
        admission_settings: Option<LocalUnixStorageNodeClientAdmissionSettings>,
    ) -> Result<StorageClusterRuntimeMapRefreshLoop, StorageClusterRuntimeMapRefreshError>
    where
        S: ControlPlaneRuntimeMapSource + Send + 'static,
        F: Fn() -> u64 + Send + 'static,
    {
        if refresh_interval.is_zero() {
            return Err(StorageClusterRuntimeMapRefreshError::RefreshLoopZeroInterval);
        }

        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let status = Arc::new(Mutex::new(
            StorageClusterRuntimeMapRefreshLoopStatus::default(),
        ));
        let worker_stop = Arc::clone(&stop);
        let worker_status = Arc::clone(&status);
        let handle = thread::Builder::new()
            .name("argmin-storage-cluster-control-plane-refresh".to_string())
            .spawn(move || loop {
                let now_ms = authority_now_ms();
                let result = match admission_settings {
                    Some(admission_settings) => self
                        .refresh_from_control_plane_runtime_map_with_unix_storage_node_clients(
                            &control_plane,
                            now_ms,
                            admission_settings,
                        ),
                    None => self.refresh_from_control_plane_runtime_map(&control_plane, now_ms),
                };
                let recovery_result = result.as_ref().err().map(|error| {
                    if runtime_map_refresh_error_requires_current_map_invalidation(error) {
                        self.expire_same_epoch_generations(now_ms);
                    }
                    self.current()
                        .drain_pending_metadata_commands_for_current_map()
                });
                {
                    let mut status = worker_status
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    status.attempts += 1;
                    match result {
                        Ok(cluster) => {
                            status.successes += 1;
                            status.last_success =
                                Some(StorageClusterRuntimeMapRefreshLoopSuccess {
                                    cluster_epoch: cluster.cluster_epoch(),
                                    route_map_validity: cluster.route_map_validity(),
                                });
                            status.last_error = None;
                        }
                        Err(error) => {
                            status.failures += 1;
                            let mut error = error.to_string();
                            match recovery_result {
                                Some(Ok(drained)) if drained > 0 => {
                                    error.push_str(&format!(
                                        "; drained {drained} pending metadata command(s) from current map"
                                    ));
                                }
                                Some(Err(recovery_error)) => {
                                    error.push_str(&format!(
                                        "; pending metadata command recovery failed: {recovery_error}"
                                    ));
                                }
                                _ => {}
                            }
                            status.last_error = Some(error);
                        }
                    }
                }

                let (lock, cvar) = &*worker_stop;
                let stopped = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                if *stopped {
                    break;
                }
                let (stopped, _) = cvar
                    .wait_timeout(stopped, refresh_interval)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if *stopped {
                    break;
                }
            })
            .map_err(|source| StorageClusterRuntimeMapRefreshError::RefreshLoopSpawn { source })?;

        Ok(StorageClusterRuntimeMapRefreshLoop {
            stop,
            status,
            handle: Some(handle),
        })
    }
}

fn runtime_map_refresh_error_requires_current_map_invalidation(
    error: &StorageClusterRuntimeMapRefreshError,
) -> bool {
    match error {
        StorageClusterRuntimeMapRefreshError::ControlPlane(
            ControlPlaneError::PgPeeringPendingMetadataCommand { .. },
        ) => true,
        StorageClusterRuntimeMapRefreshError::ControlPlane(ControlPlaneError::RpcRemote {
            message,
        }) => message.contains("reported unresolved pending metadata command"),
        _ => false,
    }
}

#[cfg(test)]
mod runtime_map_refresh_invalidation_tests {
    use super::*;

    fn active_test_cluster(validity: RouteMapValidity) -> Arc<StorageCluster> {
        let route = PgRouteSnapshot::reconstructed(
            ClusterEpoch::INITIAL,
            PgId::new(31),
            NodeId::new(1),
            vec![NodeId::new(1)],
            PgState::Active,
        );
        let local_map = LocalClusterMap::open_frontend_topology_only_with_pg_routes_and_validity(
            NodeId::new(1),
            [NodeId::new(1)],
            &[31],
            EcShape { k: 1, m: 0 },
            ClusterEpoch::INITIAL,
            [LocalPgRoute::from(&route)],
            validity,
        )
        .unwrap();
        StorageCluster::test_from_local_map_with_epoch(Arc::new(local_map), ClusterEpoch::INITIAL)
            .unwrap()
    }

    #[test]
    fn authoritative_pending_metadata_refresh_failure_expires_same_epoch_generations() {
        let pinned = active_test_cluster(RouteMapValidity::until_ms(10_000).unwrap());
        let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&pinned));
        let current = handle.current();

        handle.expire_same_epoch_generations(5_000);

        assert_eq!(pinned.route_map_valid_until_ms(), Some(5_000));
        assert_eq!(current.route_map_valid_until_ms(), Some(5_000));
        assert_eq!(handle.current().route_map_valid_until_ms(), Some(5_000));
        assert!(matches!(
            handle.current().require_route_map_valid_at(5_000),
            Err(StoreError::RouteMapExpired {
                cluster_epoch,
                valid_until_ms: 5_000,
                now_ms: 5_000,
            }) if cluster_epoch == ClusterEpoch::INITIAL
        ));
    }

    #[test]
    fn same_epoch_install_shrinks_pinned_generation_validity() {
        let pinned = active_test_cluster(RouteMapValidity::until_ms(5_000).unwrap());
        let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&pinned));
        let old_current = handle.current();
        let candidate = active_test_cluster(RouteMapValidity::until_ms(4_000).unwrap());

        handle.install(Arc::clone(&candidate)).unwrap();

        assert_eq!(pinned.route_map_valid_until_ms(), Some(4_000));
        assert_eq!(old_current.route_map_valid_until_ms(), Some(4_000));
        assert_eq!(candidate.route_map_valid_until_ms(), Some(4_000));
        assert_eq!(handle.current().route_map_valid_until_ms(), Some(4_000));
        assert!(matches!(
            pinned.require_route_map_valid_at(4_000),
            Err(StoreError::RouteMapExpired {
                cluster_epoch,
                valid_until_ms: 4_000,
                now_ms: 4_000,
            }) if cluster_epoch == ClusterEpoch::INITIAL
        ));
    }

    #[test]
    fn authoritative_pending_metadata_refresh_failure_predicate_is_narrow() {
        assert!(runtime_map_refresh_error_requires_current_map_invalidation(
            &StorageClusterRuntimeMapRefreshError::ControlPlane(
                ControlPlaneError::PgPeeringPendingMetadataCommand {
                    pg_id: 31,
                    node_id: 1,
                    cluster_epoch: ClusterEpoch::INITIAL,
                },
            ),
        ));
        assert!(runtime_map_refresh_error_requires_current_map_invalidation(
            &StorageClusterRuntimeMapRefreshError::ControlPlane(ControlPlaneError::RpcRemote {
                message: "node 1 reported unresolved pending metadata command for PG 31 in cluster epoch 1".to_string(),
            }),
        ));
        assert!(
            !runtime_map_refresh_error_requires_current_map_invalidation(
                &StorageClusterRuntimeMapRefreshError::ControlPlane(ControlPlaneError::RpcRemote {
                    message: "connect timeout".to_string(),
                }),
            )
        );
        assert!(
            !runtime_map_refresh_error_requires_current_map_invalidation(
                &StorageClusterRuntimeMapRefreshError::ControlPlane(ControlPlaneError::Io {
                    context: "connect control-plane RPC socket",
                    source: io::Error::new(io::ErrorKind::TimedOut, "timeout"),
                }),
            )
        );
    }
}

pub(super) struct DurableBucketWriteReservation {
    node: Arc<dyn crate::node_client::BucketMetadataNodeClient>,
    pg_id: u32,
    record: BucketWriteReservationRecord,
}

pub(super) struct DurableBucketWriteDrain {
    pg_id: u32,
    record: BucketWriteDrainRecord,
}

pub(super) enum DurableBucketDeleteDrainBegin {
    Acquired(DurableBucketWriteDrain),
    AlreadyDeleting,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReissuedPendingCommandReplicaMatch {
    BelowReplacement,
    MatchesHashChain,
    MissingOrMismatched,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReissuedPendingCommandPrimarySummary {
    node_id: NodeId,
    max_log_index: u64,
    applied_log_index: u64,
    applied_log_hash: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReissuedPendingCommandReplicaSummary {
    node_id: NodeId,
    max_log_index: u64,
    applied_log_index: u64,
    applied_log_hash: u64,
    replacement_match: ReissuedPendingCommandReplicaMatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReissuedPendingCommandDecision {
    StaleCommandDisplaced,
    ReloadCurrent,
    Conflict { node_id: NodeId, log_index: u64 },
}

#[must_use = "ContenderDrained must restart from a fresh snapshot"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnapshotSensitiveCommandInstall {
    Installed,
    ContenderDrained,
}

enum ObjectPgPendingCommandInstall {
    Installed(MetadataCommandEnvelope),
    Pending(MetadataCommandEnvelope),
    LogConflict { pending_visible: bool },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BucketWriteReservationDisposition {
    TransferredToCommand,
    ReleaseByCaller,
    PreserveForOwnershipCheckFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamAppendCommandApplyOutcome {
    Applied,
    RetryFromFreshSnapshot,
}

pub enum BucketWriteSnapshotAction<T, E> {
    Release(Result<T, E>),
    TransferredToCommand(Result<T, E>),
}

impl<T, E> BucketWriteSnapshotAction<T, E> {
    pub fn release(result: Result<T, E>) -> Self {
        Self::Release(result)
    }

    pub fn transferred_to_command(result: Result<T, E>) -> Self {
        Self::TransferredToCommand(result)
    }
}

fn decide_reissued_pending_command(
    primary: ReissuedPendingCommandPrimarySummary,
    acting_set_max_log_index: u64,
    current_log_index: u64,
    payload_matches: bool,
    replicas: &[ReissuedPendingCommandReplicaSummary],
) -> ReissuedPendingCommandDecision {
    if !payload_matches {
        return ReissuedPendingCommandDecision::StaleCommandDisplaced;
    }
    if primary.applied_log_index != primary.max_log_index {
        return ReissuedPendingCommandDecision::Conflict {
            node_id: primary.node_id,
            log_index: primary.max_log_index,
        };
    }
    let Some(expected_log_index) = primary.max_log_index.checked_add(1) else {
        return ReissuedPendingCommandDecision::Conflict {
            node_id: primary.node_id,
            log_index: u64::MAX,
        };
    };
    if current_log_index != expected_log_index || acting_set_max_log_index > current_log_index {
        return ReissuedPendingCommandDecision::Conflict {
            node_id: primary.node_id,
            log_index: acting_set_max_log_index.max(current_log_index),
        };
    }
    for replica in replicas {
        if replica.max_log_index < current_log_index {
            if replica.max_log_index != primary.applied_log_index
                || replica.applied_log_index != primary.applied_log_index
                || replica.applied_log_hash != primary.applied_log_hash
            {
                return ReissuedPendingCommandDecision::Conflict {
                    node_id: replica.node_id,
                    log_index: primary.applied_log_index.max(replica.max_log_index),
                };
            }
            continue;
        }
        if replica.replacement_match != ReissuedPendingCommandReplicaMatch::MatchesHashChain {
            return ReissuedPendingCommandDecision::Conflict {
                node_id: replica.node_id,
                log_index: current_log_index,
            };
        }
    }
    ReissuedPendingCommandDecision::ReloadCurrent
}

fn metadata_transfer_destination_proof(
    artifact: &PgMetadataTransferArtifact,
    commands: &[MetadataTransferCommand],
    destination_cluster_epoch: ClusterEpoch,
) -> PgMetadataProof {
    metadata_transfer_destination_proof_for_commands(
        artifact.pg_id,
        commands.len() as u64,
        artifact.proof.state_digest,
        commands,
        destination_cluster_epoch,
    )
}

fn metadata_transfer_destination_proof_for_commands(
    pg_id: PgId,
    applied_log_index: u64,
    state_digest: u64,
    commands: &[MetadataTransferCommand],
    destination_cluster_epoch: ClusterEpoch,
) -> PgMetadataProof {
    let mut applied_log_hash = 0;
    for transfer_command in commands {
        let command = &transfer_command.command;
        applied_log_hash = metadata_command_log_hash(
            destination_cluster_epoch,
            pg_id,
            command.id().log_index(),
            applied_log_hash,
            command.checksum_crc64(),
        );
    }
    PgMetadataProof {
        applied_log_index,
        applied_log_hash,
        state_digest,
    }
}

fn retained_log_export_failure_allows_checkpoint_fallback(
    error: &PgPeeringReconstructionFailure,
) -> bool {
    matches!(
        error,
        PgPeeringReconstructionFailure::Reconstruction(
            PgPeeringReconstructionError::MissingRetainedCommandLogEntry { .. }
                | PgPeeringReconstructionError::MissingRetainedCommandStateProof { .. }
                | PgPeeringReconstructionError::UnreplayableAbandonedCommandLogEntry { .. }
        )
    )
}

fn metadata_transfer_prefix_proof_at_epoch(
    pg_id: PgId,
    state_digest: u64,
    commands: &[MetadataTransferCommand],
    destination_cluster_epoch: ClusterEpoch,
) -> PgMetadataProof {
    let mut applied_log_hash = 0;
    for (index, transfer_command) in commands.iter().enumerate() {
        let log_index = MetadataCommandLogIndex::new((index + 1) as u64)
            .expect("metadata transfer prefix log indexes are non-zero");
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(destination_cluster_epoch, pg_id, log_index),
            transfer_command.command.payload().clone(),
        );
        applied_log_hash = metadata_command_log_hash(
            destination_cluster_epoch,
            pg_id,
            log_index,
            applied_log_hash,
            command.checksum_crc64(),
        );
    }
    PgMetadataProof {
        applied_log_index: commands.len() as u64,
        applied_log_hash,
        state_digest,
    }
}

impl StorageCluster {
    pub fn metadata_transfer_imported_proof_at_epoch(
        artifact: &PgMetadataTransferArtifact,
        destination_epoch: ClusterEpoch,
    ) -> Result<PgMetadataProof, PgMetadataTransferError> {
        let commands = rebase_pg_metadata_transfer_artifact_commands(artifact, destination_epoch)
            .map_err(|error| PgMetadataTransferError::Reconstruction {
            message: error.to_string(),
        })?;
        Ok(metadata_transfer_destination_proof(
            artifact,
            &commands,
            destination_epoch,
        ))
    }
}

enum MetadataTransferImportDestination {
    AlreadyImported(MetadataCommandReplicaState),
    Empty,
    AdoptBase,
    AdoptExisting,
    AdoptPrefix { prefix_len: usize },
}

fn checkpoint_import_resume_prefix_len(
    pg_id: PgId,
    checkpoint_destination_base_proof: PgMetadataProof,
    expected_import_proof: PgMetadataProof,
    current_proof: PgMetadataProof,
    commands: &[MetadataTransferCommand],
    cluster_epoch: ClusterEpoch,
) -> Option<usize> {
    if current_proof == checkpoint_destination_base_proof {
        return Some(0);
    }
    if current_proof == expected_import_proof {
        return Some(commands.len());
    }
    for (index, command) in commands.iter().enumerate() {
        let prefix_len = index + 1;
        let prefix_proof = metadata_transfer_destination_proof_for_commands(
            pg_id,
            prefix_len as u64,
            command.post_state_digest,
            &commands[..prefix_len],
            cluster_epoch,
        );
        if current_proof == prefix_proof {
            return Some(prefix_len);
        }
    }
    None
}

fn classify_metadata_transfer_import_destination(
    metadata_client: &dyn MetadataCommandNodeClient,
    node_id: NodeId,
    pg_id: PgId,
    cluster_epoch: ClusterEpoch,
    base_import_proof: PgMetadataProof,
    expected_import_proof: PgMetadataProof,
    commands: &[MetadataTransferCommand],
) -> Result<MetadataTransferImportDestination, PgPeeringReconstructionFailure> {
    if metadata_client
        .pending_metadata_command_envelope(pg_id, cluster_epoch)?
        .is_some()
    {
        return Err(PgPeeringReconstructionError::PendingMetadataCommand { node_id }.into());
    }

    let state = metadata_client.metadata_command_replica_state(pg_id)?;
    if state.cluster_epoch != cluster_epoch
        && metadata_client
            .pending_metadata_command_envelope(pg_id, state.cluster_epoch)?
            .is_some()
    {
        return Err(PgPeeringReconstructionError::PendingMetadataCommand { node_id }.into());
    }
    let proof = PgMetadataProof {
        applied_log_index: state.applied_log_index,
        applied_log_hash: state.applied_log_hash,
        state_digest: state.state_digest,
    };
    if state.cluster_epoch == cluster_epoch && proof == expected_import_proof {
        let validated = metadata_client
            .validate_metadata_command_replay_state_preserving_pending_slot(pg_id, cluster_epoch)?;
        let validated_proof = PgMetadataProof {
            applied_log_index: validated.applied_log_index,
            applied_log_hash: validated.applied_log_hash,
            state_digest: validated.state_digest,
        };
        if validated_proof == expected_import_proof {
            return Ok(MetadataTransferImportDestination::AlreadyImported(
                validated,
            ));
        }
    }

    if state.applied_log_index == 0
        && state.applied_log_hash == 0
        && metadata_client.metadata_command_replica_state_can_initialize(pg_id, cluster_epoch)?
    {
        let actual_proof = PgMetadataProof {
            applied_log_index: state.applied_log_index,
            applied_log_hash: state.applied_log_hash,
            state_digest: state.state_digest,
        };
        if actual_proof == base_import_proof {
            return Ok(MetadataTransferImportDestination::Empty);
        }
        return Err(
            PgPeeringReconstructionError::DirtyMetadataTransferDestination {
                node_id,
                pg_id,
                cluster_epoch,
                applied_log_index: state.applied_log_index,
                applied_log_hash: state.applied_log_hash,
                state_digest: state.state_digest,
                expected: base_import_proof,
            }
            .into(),
        );
    }

    if state.state_digest != expected_import_proof.state_digest {
        if let Some(first_command) = commands.first() {
            if state.state_digest == first_command.pre_state_digest {
                let actual_proof = PgMetadataProof {
                    applied_log_index: state.applied_log_index,
                    applied_log_hash: state.applied_log_hash,
                    state_digest: state.state_digest,
                };
                if base_import_proof.applied_log_index == 0
                    && base_import_proof.applied_log_hash == 0
                    && base_import_proof.state_digest == first_command.pre_state_digest
                {
                    let validated = metadata_client
                        .validate_metadata_command_replay_state_preserving_pending_slot(
                            pg_id,
                            state.cluster_epoch,
                        )?;
                    let validated_proof = PgMetadataProof {
                        applied_log_index: validated.applied_log_index,
                        applied_log_hash: validated.applied_log_hash,
                        state_digest: validated.state_digest,
                    };
                    if validated_proof == actual_proof {
                        return Ok(MetadataTransferImportDestination::AdoptBase);
                    }
                } else if actual_proof == base_import_proof {
                    let validated = metadata_client
                        .validate_metadata_command_replay_state_preserving_pending_slot(
                            pg_id,
                            state.cluster_epoch,
                        )?;
                    let validated_proof = PgMetadataProof {
                        applied_log_index: validated.applied_log_index,
                        applied_log_hash: validated.applied_log_hash,
                        state_digest: validated.state_digest,
                    };
                    if validated_proof == base_import_proof {
                        return Ok(MetadataTransferImportDestination::AdoptBase);
                    }
                }
                return Err(
                    PgPeeringReconstructionError::DirtyMetadataTransferDestination {
                        node_id,
                        pg_id,
                        cluster_epoch,
                        applied_log_index: state.applied_log_index,
                        applied_log_hash: state.applied_log_hash,
                        state_digest: state.state_digest,
                        expected: base_import_proof,
                    }
                    .into(),
                );
            }
        }
        for (index, command) in commands.iter().enumerate() {
            let prefix_len = index + 1;
            if state.state_digest != command.post_state_digest {
                continue;
            }
            let prefix_proof = metadata_transfer_destination_proof_for_commands(
                pg_id,
                prefix_len as u64,
                command.post_state_digest,
                &commands[..prefix_len],
                cluster_epoch,
            );
            let actual_proof = PgMetadataProof {
                applied_log_index: state.applied_log_index,
                applied_log_hash: state.applied_log_hash,
                state_digest: state.state_digest,
            };
            if state.cluster_epoch == cluster_epoch {
                if actual_proof == prefix_proof {
                    return Ok(MetadataTransferImportDestination::AdoptPrefix { prefix_len });
                }
                return Err(
                    PgPeeringReconstructionError::DirtyMetadataTransferDestination {
                        node_id,
                        pg_id,
                        cluster_epoch,
                        applied_log_index: state.applied_log_index,
                        applied_log_hash: state.applied_log_hash,
                        state_digest: state.state_digest,
                        expected: prefix_proof,
                    }
                    .into(),
                );
            }
            let historical_prefix_proof = metadata_transfer_prefix_proof_at_epoch(
                pg_id,
                command.post_state_digest,
                &commands[..prefix_len],
                state.cluster_epoch,
            );
            if actual_proof == historical_prefix_proof {
                let validated = metadata_client
                    .validate_metadata_command_replay_state_preserving_pending_slot(
                        pg_id,
                        state.cluster_epoch,
                    )?;
                let validated_proof = PgMetadataProof {
                    applied_log_index: validated.applied_log_index,
                    applied_log_hash: validated.applied_log_hash,
                    state_digest: validated.state_digest,
                };
                if validated_proof == historical_prefix_proof {
                    return Ok(MetadataTransferImportDestination::AdoptPrefix { prefix_len });
                }
            }
            return Err(
                PgPeeringReconstructionError::DirtyMetadataTransferDestination {
                    node_id,
                    pg_id,
                    cluster_epoch,
                    applied_log_index: state.applied_log_index,
                    applied_log_hash: state.applied_log_hash,
                    state_digest: state.state_digest,
                    expected: historical_prefix_proof,
                }
                .into(),
            );
        }
        return Err(
            PgPeeringReconstructionError::DirtyMetadataTransferDestination {
                node_id,
                pg_id,
                cluster_epoch,
                applied_log_index: state.applied_log_index,
                applied_log_hash: state.applied_log_hash,
                state_digest: state.state_digest,
                expected: expected_import_proof,
            }
            .into(),
        );
    }

    Ok(MetadataTransferImportDestination::AdoptExisting)
}

impl StorageCluster {
    fn emit_metadata_command_conflict(
        &self,
        node_id: Option<NodeId>,
        pg_id: PgId,
        log_index: Option<u64>,
        kind: &'static str,
        command_kind: Option<&'static str>,
    ) {
        let _ = observability::emit_metadata_command_conflict(
            TRACE_TARGET,
            observability::MetadataCommandConflictSummary {
                node_id: node_id.map(|node_id| node_id.as_u32()),
                pg_id: pg_id.get(),
                cluster_epoch: self.operation_epoch().get(),
                log_index,
                kind,
                command_kind,
            },
        );
        if std::env::var_os("ARGMIN_METADATA_COMMAND_CONFLICT_DIAGNOSTICS").is_some() {
            let node = node_id
                .map(|node_id| node_id.as_u32().to_string())
                .unwrap_or_else(|| "unknown".to_string());
            let log = log_index
                .map(|log_index| log_index.to_string())
                .unwrap_or_else(|| "unknown".to_string());
            eprintln!(
                "metadata command conflict source=frontend kind={kind} node_id={node} pg_id={} cluster_epoch={} log_index={log} command_kind={}",
                pg_id.get(),
                self.operation_epoch().get(),
                command_kind.unwrap_or("unknown")
            );
        }
    }

    fn emit_metadata_command_pending_slot_action(
        &self,
        node_id: Option<NodeId>,
        pg_id: PgId,
        log_index: Option<u64>,
        action: &'static str,
        command_kind: Option<&'static str>,
    ) {
        let _ = observability::emit_metadata_command_pending_slot_action(
            TRACE_TARGET,
            observability::MetadataCommandPendingSlotActionSummary {
                node_id: node_id.map(|node_id| node_id.as_u32()),
                pg_id: pg_id.get(),
                cluster_epoch: self.operation_epoch().get(),
                log_index,
                action,
                command_kind,
            },
        );
    }

    fn pending_slot_primary_node_id(&self, pg_id: PgId) -> Option<NodeId> {
        self.local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)
            .ok()
            .map(|node| node.node_id())
    }

    fn emit_pending_slot_action_for_command(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        action: &'static str,
    ) {
        self.emit_metadata_command_pending_slot_action(
            self.pending_slot_primary_node_id(pg_id),
            pg_id,
            Some(command.id().log_index().get()),
            action,
            Some(command.payload().kind_name()),
        );
    }

    fn emit_metadata_command_recovery_admission_for_command(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        admission: observability::MetadataCommandRecoveryAdmissionKind,
        wait_us: u128,
    ) {
        let _ = observability::emit_metadata_command_recovery_admission(
            TRACE_TARGET,
            observability::MetadataCommandRecoveryAdmissionSummary {
                node_id: self
                    .pending_slot_primary_node_id(pg_id)
                    .map(|node_id| node_id.as_u32()),
                pg_id: pg_id.get(),
                cluster_epoch: self.operation_epoch().get(),
                log_index: Some(command.id().log_index().get()),
                admission,
                command_kind: Some(command.payload().kind_name()),
                wait_us,
            },
        );
    }

    fn emit_metadata_command_recovery_outcome_for_command(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        outcome: &'static str,
    ) {
        let _ = observability::emit_metadata_command_recovery_outcome(
            TRACE_TARGET,
            observability::MetadataCommandRecoveryOutcomeSummary {
                node_id: self
                    .pending_slot_primary_node_id(pg_id)
                    .map(|node_id| node_id.as_u32()),
                pg_id: pg_id.get(),
                cluster_epoch: self.operation_epoch().get(),
                log_index: Some(command.id().log_index().get()),
                outcome,
                command_kind: Some(command.payload().kind_name()),
            },
        );
    }

    fn metadata_command_conflict(
        &self,
        node_id: NodeId,
        pg_id: PgId,
        log_index: u64,
    ) -> StoreError {
        self.emit_metadata_command_conflict(
            Some(node_id),
            pg_id,
            Some(log_index),
            "log_conflict",
            None,
        );
        StoreError::MetadataCommandLogConflict {
            node_id: node_id.as_u32(),
            pg_id: pg_id.get(),
            cluster_epoch: self.operation_epoch(),
            log_index,
        }
    }

    fn metadata_command_log_conflict_matches(
        command: &MetadataCommandEnvelope,
        error: &BucketSnapshotLoadError,
    ) -> bool {
        matches!(
            error,
            BucketSnapshotLoadError::Store(StoreError::MetadataCommandLogConflict {
                pg_id,
                cluster_epoch,
                log_index,
                ..
            }) if *pg_id == command.id().pg_id().get()
                && *cluster_epoch == command.id().cluster_epoch()
                && *log_index == command.id().log_index().get()
        )
    }

    fn reserve_object_generation_conflict_matches(
        command: &MetadataCommandEnvelope,
        error: &BucketSnapshotLoadError,
    ) -> bool {
        matches!(
            (command.payload(), error),
            (
                MetadataCommandPayload::ReserveObjectGeneration(reservation),
                BucketSnapshotLoadError::Metadata(
                    MetadataError::ObjectGenerationReservationConflict {
                        reservation_id,
                        generation_id,
                    },
                ),
            ) if reservation.reservation_id.as_str() == reservation_id
                && reservation.generation_id.get() == *generation_id
        )
    }

    fn reserve_object_version_conflict_matches(
        command: &MetadataCommandEnvelope,
        error: &BucketSnapshotLoadError,
    ) -> bool {
        matches!(
            (command.payload(), error),
            (
                MetadataCommandPayload::ReserveObjectVersion(reservation),
                BucketSnapshotLoadError::Metadata(
                    MetadataError::ObjectVersionReservationConflict { version_id },
                ),
            ) if reservation.version_id == *version_id
        )
    }

    fn metadata_command_is_bucket_pg_command(command: &MetadataCommandEnvelope) -> bool {
        matches!(
            command.payload(),
            MetadataCommandPayload::CreateBucket(_)
                | MetadataCommandPayload::PutBucketVersioning(_)
                | MetadataCommandPayload::PutBucketAcl(_)
                | MetadataCommandPayload::PutBucketProperty(_)
                | MetadataCommandPayload::PutBucketSubresource(_)
                | MetadataCommandPayload::MarkBucketDeleting(_)
                | MetadataCommandPayload::DeleteFinalizedBucket(_)
                | MetadataCommandPayload::DeleteCompletedMultipartUpload(_)
                | MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(_)
        )
    }

    fn partial_exact_metadata_command_conflict_is_retryable(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        applied_nodes: usize,
        source: &BucketSnapshotLoadError,
    ) -> Result<bool, BucketSnapshotLoadError> {
        fn entry_hashes_or_not_retryable(
            metadata_command_client: &dyn MetadataCommandNodeClient,
            pg_id: PgId,
            command: &MetadataCommandEnvelope,
        ) -> Result<Option<(u64, u64)>, BucketSnapshotLoadError> {
            match metadata_command_client.applied_metadata_command_log_entry_hashes(pg_id, command)
            {
                Ok(hashes) => Ok(hashes),
                Err(StoreError::MetadataCommandLogConflict { .. }) => Ok(None),
                Err(error) => Err(error.into()),
            }
        }

        let BucketSnapshotLoadError::Store(StoreError::MetadataCommandLogConflict {
            node_id: conflict_node_id,
            pg_id: conflict_pg_id,
            cluster_epoch,
            log_index,
        }) = source
        else {
            return Ok(false);
        };
        if *conflict_pg_id != command.id().pg_id().get()
            || *cluster_epoch != command.id().cluster_epoch()
            || *log_index != command.id().log_index().get()
        {
            return Ok(false);
        }

        let primary_node_id = self
            .local_map
            .pg_route(pg_id)
            .expect("validated metadata PG command route must exist")
            .primary_node_id();
        let mut nodes = self
            .local_map
            .metadata_pg_acting_nodes(command.id().cluster_epoch(), pg_id)?;
        nodes.sort_by_key(|node| node.node_id() != primary_node_id);
        let Some(conflict_index) = nodes
            .iter()
            .position(|node| node.node_id().as_u32() == *conflict_node_id)
        else {
            return Ok(false);
        };
        if conflict_index != applied_nodes {
            return Ok(false);
        }

        let primary = self
            .local_map
            .metadata_pg_primary_node(command.id().cluster_epoch(), pg_id)?;
        let primary_state = primary
            .metadata_command_client()
            .metadata_command_replica_state(pg_id)?;
        let command_log_index = command.id().log_index().get();
        let expected_previous_log_hash =
            if command_log_index == primary_state.applied_log_index.saturating_add(1) {
                primary_state.applied_log_hash
            } else if command_log_index == primary_state.applied_log_index {
                let Some((previous_log_hash, log_hash)) = entry_hashes_or_not_retryable(
                    primary.metadata_command_client().as_ref(),
                    pg_id,
                    command,
                )?
                else {
                    return Ok(false);
                };
                if log_hash != primary_state.applied_log_hash {
                    return Ok(false);
                }
                previous_log_hash
            } else {
                return Ok(false);
            };

        let mut expected_hashes = None;
        for (index, node) in nodes.into_iter().enumerate() {
            let hashes = entry_hashes_or_not_retryable(
                node.metadata_command_client().as_ref(),
                pg_id,
                command,
            )?;
            match (index <= conflict_index, hashes, expected_hashes) {
                (true, Some(hashes), None) if hashes.0 == expected_previous_log_hash => {
                    expected_hashes = Some(hashes)
                }
                (true, Some(hashes), Some(expected))
                    if hashes == expected && hashes.0 == expected_previous_log_hash => {}
                (true, _, _) => return Ok(false),
                (false, Some(hashes), Some(expected))
                    if hashes == expected && hashes.0 == expected_previous_log_hash => {}
                (false, Some(_), _) => return Ok(false),
                (false, None, _) => {}
            }
        }
        Ok(expected_hashes.is_some())
    }

    fn metadata_command_is_applied_on_all_acting_nodes(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let mut expected_hashes = None;
        for node in self
            .local_map
            .metadata_pg_acting_nodes(command.id().cluster_epoch(), pg_id)?
        {
            let Some(hashes) = node
                .metadata_command_client()
                .applied_metadata_command_log_entry_hashes(pg_id, command)?
            else {
                return Ok(false);
            };
            match expected_hashes {
                None => expected_hashes = Some(hashes),
                Some(expected) if hashes == expected => {}
                Some(_) => return Ok(false),
            }
        }
        Ok(expected_hashes.is_some())
    }

    fn retryable_partial_exact_metadata_command_conflict_applied_on_all_nodes(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        applied_nodes: usize,
        source: &BucketSnapshotLoadError,
    ) -> Result<Option<bool>, BucketSnapshotLoadError> {
        if !Self::metadata_command_log_conflict_matches(command, source)
            || !self.partial_exact_metadata_command_conflict_is_retryable(
                pg_id,
                command,
                applied_nodes,
                source,
            )?
        {
            return Ok(None);
        }
        self.metadata_command_is_applied_on_all_acting_nodes(pg_id, command)
            .map(Some)
    }

    #[allow(clippy::too_many_arguments)]
    fn matching_reissued_pending_command_if_safe(
        &self,
        pg_id: PgId,
        primary_node_id: NodeId,
        primary_metadata_client: &dyn MetadataCommandNodeClient,
        primary_max_log_index: u64,
        acting_set_max_log_index: u64,
        stale_command: &MetadataCommandEnvelope,
        current: MetadataCommandEnvelope,
    ) -> Result<Option<MetadataCommandEnvelope>, StoreError> {
        let payload_matches = current.payload() == stale_command.payload();
        let current_log_index = current.id().log_index().get();
        let primary_state = primary_metadata_client.metadata_command_replica_state(pg_id)?;
        if current_log_index == primary_state.applied_log_index {
            let Some((_, log_hash)) = primary_metadata_client
                .applied_metadata_command_log_entry_hashes(pg_id, &current)?
            else {
                return Err(self.metadata_command_conflict(
                    primary_node_id,
                    pg_id,
                    current_log_index,
                ));
            };
            if log_hash != primary_state.applied_log_hash {
                return Err(self.metadata_command_conflict(
                    primary_node_id,
                    pg_id,
                    current_log_index,
                ));
            }
            if !payload_matches {
                return Ok(None);
            }
            return self.matching_terminal_pending_command_if_safe(
                pg_id,
                primary_node_id,
                primary_metadata_client,
                acting_set_max_log_index,
                &primary_state,
                current,
            );
        }
        let primary_summary = ReissuedPendingCommandPrimarySummary {
            node_id: primary_node_id,
            max_log_index: primary_max_log_index,
            applied_log_index: primary_state.applied_log_index,
            applied_log_hash: primary_state.applied_log_hash,
        };
        match decide_reissued_pending_command(
            primary_summary,
            acting_set_max_log_index,
            current_log_index,
            payload_matches,
            &[],
        ) {
            ReissuedPendingCommandDecision::StaleCommandDisplaced => return Ok(None),
            ReissuedPendingCommandDecision::Conflict { node_id, log_index } => {
                let _ = observability::event(
                    TRACE_TARGET,
                    "metadata_command_reissue_conflict",
                    Some(format_args!(
                        "pg_id={} node_id={:?} log_index={} primary_node_id={:?} primary_max={} primary_applied={} acting_set_max={} current_index={} payload_matches={} phase=primary",
                        pg_id.get(),
                        node_id,
                        log_index,
                        primary_node_id,
                        primary_max_log_index,
                        primary_state.applied_log_index,
                        acting_set_max_log_index,
                        current_log_index,
                        payload_matches,
                    )),
                );
                return Err(self.metadata_command_conflict(node_id, pg_id, log_index));
            }
            ReissuedPendingCommandDecision::ReloadCurrent => {}
        }
        let mut replicas = Vec::new();
        for node in self
            .local_map
            .metadata_pg_acting_nodes(self.operation_epoch(), pg_id)?
        {
            let node_max_log_index = node
                .metadata_command_client()
                .max_metadata_command_log_index(pg_id, self.operation_epoch())?;
            let node_state = node
                .metadata_command_client()
                .metadata_command_replica_state(pg_id)?;
            let replacement_match = if node_max_log_index < current_log_index {
                ReissuedPendingCommandReplicaMatch::BelowReplacement
            } else if node
                .metadata_command_client()
                .has_matching_applied_metadata_command_log_entry(
                    pg_id,
                    &current,
                    primary_state.applied_log_hash,
                )?
            {
                ReissuedPendingCommandReplicaMatch::MatchesHashChain
            } else {
                ReissuedPendingCommandReplicaMatch::MissingOrMismatched
            };
            replicas.push(ReissuedPendingCommandReplicaSummary {
                node_id: node.node_id(),
                max_log_index: node_max_log_index,
                applied_log_index: node_state.applied_log_index,
                applied_log_hash: node_state.applied_log_hash,
                replacement_match,
            });
        }
        match decide_reissued_pending_command(
            primary_summary,
            acting_set_max_log_index,
            current_log_index,
            payload_matches,
            &replicas,
        ) {
            ReissuedPendingCommandDecision::StaleCommandDisplaced => Ok(None),
            ReissuedPendingCommandDecision::ReloadCurrent => Ok(Some(current)),
            ReissuedPendingCommandDecision::Conflict { node_id, log_index } => {
                let _ = observability::event(
                    TRACE_TARGET,
                    "metadata_command_reissue_conflict",
                    Some(format_args!(
                        "pg_id={} node_id={:?} log_index={} primary_node_id={:?} primary_max={} primary_applied={} acting_set_max={} current_index={} payload_matches={} phase=replica",
                        pg_id.get(),
                        node_id,
                        log_index,
                        primary_node_id,
                        primary_max_log_index,
                        primary_state.applied_log_index,
                        acting_set_max_log_index,
                        current_log_index,
                        payload_matches,
                    )),
                );
                Err(self.metadata_command_conflict(node_id, pg_id, log_index))
            }
        }
    }

    fn matching_terminal_pending_command_if_safe(
        &self,
        pg_id: PgId,
        primary_node_id: NodeId,
        primary_metadata_client: &dyn MetadataCommandNodeClient,
        acting_set_max_log_index: u64,
        primary_state: &MetadataCommandReplicaState,
        current: MetadataCommandEnvelope,
    ) -> Result<Option<MetadataCommandEnvelope>, StoreError> {
        let current_log_index = current.id().log_index().get();
        let Some(previous_log_index) = current_log_index.checked_sub(1) else {
            return Err(self.metadata_command_conflict(primary_node_id, pg_id, current_log_index));
        };
        if acting_set_max_log_index > current_log_index {
            return Err(self.metadata_command_conflict(
                primary_node_id,
                pg_id,
                acting_set_max_log_index,
            ));
        }
        let Some((previous_log_hash, terminal_log_hash)) =
            primary_metadata_client.applied_metadata_command_log_entry_hashes(pg_id, &current)?
        else {
            return Err(self.metadata_command_conflict(primary_node_id, pg_id, current_log_index));
        };
        if terminal_log_hash != primary_state.applied_log_hash {
            return Err(self.metadata_command_conflict(primary_node_id, pg_id, current_log_index));
        }

        for node in self
            .local_map
            .metadata_pg_acting_nodes(self.operation_epoch(), pg_id)?
        {
            let metadata_client: &dyn MetadataCommandNodeClient =
                if node.node_id() == primary_node_id {
                    primary_metadata_client
                } else {
                    node.metadata_command_client().as_ref()
                };
            let node_max_log_index =
                metadata_client.max_metadata_command_log_index(pg_id, self.operation_epoch())?;
            let node_state = metadata_client.metadata_command_replica_state(pg_id)?;
            if node_max_log_index > current_log_index {
                return Err(self.metadata_command_conflict(
                    node.node_id(),
                    pg_id,
                    node_max_log_index,
                ));
            }
            if node_max_log_index < current_log_index {
                if node_max_log_index != previous_log_index
                    || node_state.applied_log_index != previous_log_index
                    || node_state.applied_log_hash != previous_log_hash
                {
                    return Err(self.metadata_command_conflict(
                        node.node_id(),
                        pg_id,
                        previous_log_index.max(node_max_log_index),
                    ));
                }
                continue;
            }
            if !metadata_client.has_matching_applied_metadata_command_log_entry(
                pg_id,
                &current,
                previous_log_hash,
            )? {
                return Err(self.metadata_command_conflict(
                    node.node_id(),
                    pg_id,
                    current_log_index,
                ));
            }
        }
        Ok(Some(current))
    }

    fn reissue_pending_metadata_command(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<Option<MetadataCommandEnvelope>, BucketSnapshotLoadError> {
        let pg_lock = self
            .local_map
            .runtime_state()
            .metadata_command_pg_lock(pg_id);
        let _pg_guard = pg_lock.lock().unwrap_or_else(|e| e.into_inner());
        let bucket = command.bucket_name().clone();
        let primary = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        self.emit_metadata_command_pending_slot_action(
            Some(primary.node_id()),
            pg_id,
            Some(command.id().log_index().get()),
            "reissue_attempt",
            Some(command.payload().kind_name()),
        );
        let primary_metadata_client = primary.metadata_command_client();
        let acting_set_max_log_index =
            self.max_metadata_command_log_index_on_acting_set(pg_id, None)?;

        enum ReissueReplaceOutcome {
            Replaced(MetadataCommandEnvelope),
            Reload {
                current: MetadataCommandEnvelope,
                primary_max_log_index: u64,
            },
            Missing,
        }

        let replace_outcome = {
            let primary_critical_section = primary_metadata_client
                .open_metadata_command_critical_section(pg_id, self.operation_epoch())?;
            let primary_max_log_index = primary_critical_section
                .max_metadata_command_log_index(pg_id, self.operation_epoch())?;
            let Some(current) = primary_critical_section
                .pending_metadata_command_envelope(pg_id, self.operation_epoch())?
            else {
                return Ok(None);
            };
            if current != *command || acting_set_max_log_index > primary_max_log_index {
                ReissueReplaceOutcome::Reload {
                    current,
                    primary_max_log_index,
                }
            } else {
                let next_log_index = primary_max_log_index
                    .max(command.id().log_index().get())
                    .checked_add(1)
                    .and_then(MetadataCommandLogIndex::new)
                    .ok_or(StoreError::MetadataCommandLogConflict {
                        node_id: primary.node_id().as_u32(),
                        pg_id: pg_id.get(),
                        cluster_epoch: self.operation_epoch(),
                        log_index: u64::MAX,
                    })?;
                let replacement = MetadataCommandEnvelope::new(
                    MetadataCommandId::new(self.operation_epoch(), pg_id, next_log_index),
                    command.payload().clone(),
                );
                if primary_critical_section.replace_pending_metadata_command_slot_for_reissue(
                    pg_id,
                    command,
                    &replacement,
                    Some(&bucket),
                )? {
                    ReissueReplaceOutcome::Replaced(replacement)
                } else {
                    let current = primary_critical_section
                        .pending_metadata_command_envelope(pg_id, self.operation_epoch())?;
                    let primary_max_log_index = primary_critical_section
                        .max_metadata_command_log_index(pg_id, self.operation_epoch())?;
                    match current {
                        Some(current) => ReissueReplaceOutcome::Reload {
                            current,
                            primary_max_log_index,
                        },
                        None => ReissueReplaceOutcome::Missing,
                    }
                }
            }
        };
        match replace_outcome {
            ReissueReplaceOutcome::Replaced(replacement) => Ok(Some(replacement)),
            ReissueReplaceOutcome::Missing => Ok(None),
            ReissueReplaceOutcome::Reload {
                current,
                primary_max_log_index,
            } => {
                let acting_set_max_log_index =
                    self.max_metadata_command_log_index_on_acting_set(pg_id, None)?;
                self.matching_reissued_pending_command_if_safe(
                    pg_id,
                    primary.node_id(),
                    primary_metadata_client.as_ref(),
                    primary_max_log_index,
                    acting_set_max_log_index,
                    command,
                    current,
                )
                .map_err(BucketSnapshotLoadError::from)
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn test_reissue_pending_metadata_command(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<Option<MetadataCommandEnvelope>, BucketSnapshotLoadError> {
        self.reissue_pending_metadata_command(pg_id, command)
    }

    fn max_metadata_command_log_index_on_acting_set(
        &self,
        pg_id: PgId,
        primary_override: Option<(NodeId, &dyn MetadataCommandNodeClient)>,
    ) -> Result<u64, StoreError> {
        let mut max_log_index = 0;
        for node in self
            .local_map
            .metadata_pg_acting_nodes(self.operation_epoch(), pg_id)?
        {
            let metadata_client: &dyn MetadataCommandNodeClient = match primary_override.as_ref() {
                Some((primary_node_id, primary_client)) if *primary_node_id == node.node_id() => {
                    *primary_client
                }
                _ => node.metadata_command_client().as_ref(),
            };
            max_log_index = max_log_index.max(
                metadata_client.max_metadata_command_log_index(pg_id, self.operation_epoch())?,
            );
        }
        Ok(max_log_index)
    }

    #[allow(dead_code)]
    pub(crate) fn reconstruct_pg_peering_from_retained_metadata_log(
        &self,
        pg_id: PgId,
        primary: NodeId,
    ) -> Result<PgPeeringReconstructionDecision, PgPeeringReconstructionFailure> {
        let nodes = self
            .local_map
            .metadata_pg_acting_nodes_for_peering_inspection(self.operation_epoch(), pg_id)?;
        let primary_index = nodes
            .iter()
            .position(|node| node.node_id() == primary)
            .ok_or(PgPeeringReconstructionError::PrimaryMissing { primary })?;

        let mut min_applied_log_index = u64::MAX;
        let mut primary_applied_log_index = None;
        let mut replicas = Vec::with_capacity(nodes.len());
        for node in &nodes {
            let metadata_client = node.metadata_command_client();
            let state = metadata_client.metadata_command_replica_state(pg_id)?;
            min_applied_log_index = min_applied_log_index.min(state.applied_log_index);
            if node.node_id() == primary {
                primary_applied_log_index = Some(state.applied_log_index);
            }
            let has_pending_metadata_command = metadata_client
                .pending_metadata_command_envelope(pg_id, self.operation_epoch())?
                .is_some();
            replicas.push(PgPeeringReplicaReconstructionInput {
                node_id: node.node_id(),
                state,
                has_pending_metadata_command,
                retained_log_hashes: Vec::new(),
            });
        }

        let primary_applied_log_index = primary_applied_log_index
            .ok_or(PgPeeringReconstructionError::PrimaryMissing { primary })?;
        if min_applied_log_index < primary_applied_log_index {
            let first_log_index = MetadataCommandLogIndex::new(min_applied_log_index + 1)
                .expect("lagging metadata command log index range starts after zero");
            let last_log_index = MetadataCommandLogIndex::new(primary_applied_log_index)
                .expect("primary applied log index must be nonzero when a replica is behind");
            replicas[primary_index].retained_log_hashes = nodes[primary_index]
                .metadata_command_client()
                .retained_metadata_command_log_hashes(
                    pg_id,
                    self.operation_epoch(),
                    first_log_index,
                    last_log_index,
                )?;
        }

        Ok(reconstruct_pg_peering_from_primary_retained_log(
            self.operation_epoch(),
            pg_id,
            primary,
            &replicas,
        )?)
    }

    #[allow(dead_code)]
    pub(crate) fn replay_pg_peering_catchup_from_retained_metadata_log(
        &self,
        pg_id: PgId,
        primary: NodeId,
    ) -> Result<PgPeeringReconstructionDecision, PgPeeringReconstructionFailure> {
        let decision = self.reconstruct_pg_peering_from_retained_metadata_log(pg_id, primary)?;
        let PgPeeringReconstructionDecision::CatchUpRequired { replicas, .. } = decision else {
            return Ok(decision);
        };

        let nodes = self
            .local_map
            .metadata_pg_acting_nodes_for_peering_replay(self.operation_epoch(), pg_id)?;
        let primary_node = nodes
            .iter()
            .find(|node| node.node_id() == primary)
            .ok_or(PgPeeringReconstructionError::PrimaryMissing { primary })?;
        let first_log_index = replicas
            .iter()
            .map(|replica| replica.from_log_index + 1)
            .min()
            .expect("catch-up decision contains at least one replica");
        let last_log_index = replicas
            .iter()
            .map(|replica| replica.to_log_index)
            .max()
            .expect("catch-up decision contains at least one replica");
        let mut retained_log_entries = Vec::new();
        let mut batch_start = first_log_index;
        while batch_start <= last_log_index {
            let batch_end = last_log_index
                .min(batch_start + STORAGE_RPC_MAX_METADATA_COMMAND_LOG_ENTRY_RANGE_ENTRIES - 1);
            let first_log_index = MetadataCommandLogIndex::new(batch_start)
                .expect("catch-up retained entry range starts after zero");
            let last_log_index = MetadataCommandLogIndex::new(batch_end)
                .expect("catch-up retained entry range ends after zero");
            retained_log_entries.extend(
                primary_node
                    .metadata_command_client()
                    .retained_metadata_command_log_entries(
                        pg_id,
                        self.operation_epoch(),
                        first_log_index,
                        last_log_index,
                    )?,
            );
            batch_start = batch_end + 1;
        }

        let replay_plans = build_pg_peering_replay_plan_from_retained_log_entries(
            &replicas,
            &retained_log_entries,
        )?;
        for replay_plan in replay_plans {
            let target_node = nodes
                .iter()
                .find(|node| node.node_id() == replay_plan.node_id)
                .ok_or(PgPeeringReconstructionError::ReplayTargetMissing {
                    node_id: replay_plan.node_id,
                })?;
            let metadata_client = target_node.metadata_command_client();
            for command in replay_plan.commands {
                metadata_client.replay_metadata_command_for_peering(pg_id, &command)?;
            }
        }

        self.reconstruct_pg_peering_from_retained_metadata_log(pg_id, primary)
    }

    #[allow(dead_code)]
    pub(crate) fn export_pg_metadata_transfer_from_retained_log(
        &self,
        pg_id: PgId,
        source_node_id: NodeId,
    ) -> Result<PgMetadataTransferArtifact, PgPeeringReconstructionFailure> {
        let route = self
            .local_pg_route(pg_id)
            .ok_or(StoreError::ClusterPgNotFound {
                pg_id: pg_id.get(),
                cluster_epoch: self.operation_epoch(),
            })?;
        if route.cluster_epoch() != self.operation_epoch() {
            return Err(StoreError::StaleMetadataRoute {
                pg_id: pg_id.get(),
                route_epoch: route.cluster_epoch(),
                current_epoch: self.operation_epoch(),
            }
            .into());
        }
        if route.state() != PgState::Peering {
            return Err(PgPeeringReconstructionError::TransferSourceNotQuiesced {
                pg_id,
                cluster_epoch: self.operation_epoch(),
                state: route.state(),
            }
            .into());
        }
        if route.primary_node_id() != source_node_id {
            return Err(PgPeeringReconstructionError::TransferSourceNotPrimary {
                pg_id,
                cluster_epoch: self.operation_epoch(),
                source_node: source_node_id,
                primary: route.primary_node_id(),
            }
            .into());
        }
        let nodes = self
            .local_map
            .metadata_pg_acting_nodes_for_peering_inspection(self.operation_epoch(), pg_id)?;
        let source_node = nodes
            .iter()
            .find(|node| node.node_id() == source_node_id)
            .ok_or(PgPeeringReconstructionError::PrimaryMissing {
                primary: source_node_id,
            })?;
        let metadata_client = source_node.metadata_command_client();
        let state = metadata_client.metadata_command_replica_state(pg_id)?;
        if state.cluster_epoch > self.operation_epoch() {
            return Err(PgPeeringReconstructionError::StaleReplicaEpoch {
                node_id: source_node_id,
                replica_epoch: state.cluster_epoch,
                cluster_epoch: self.operation_epoch(),
            }
            .into());
        }
        let has_pending_metadata_command = metadata_client
            .pending_metadata_command_envelope(pg_id, state.cluster_epoch)?
            .is_some();

        let mut retained_log_entries = Vec::new();
        let mut batch_start = 1;
        while batch_start <= state.applied_log_index {
            let batch_end = state
                .applied_log_index
                .min(batch_start + STORAGE_RPC_MAX_METADATA_COMMAND_LOG_ENTRY_RANGE_ENTRIES - 1);
            let first_log_index = MetadataCommandLogIndex::new(batch_start)
                .expect("metadata transfer retained entry range starts after zero");
            let last_log_index = MetadataCommandLogIndex::new(batch_end)
                .expect("metadata transfer retained entry range ends after zero");
            retained_log_entries.extend(metadata_client.retained_metadata_command_log_entries(
                pg_id,
                state.cluster_epoch,
                first_log_index,
                last_log_index,
            )?);
            batch_start = batch_end + 1;
        }

        Ok(
            build_pg_metadata_transfer_artifact_from_retained_log_entries(
                state.cluster_epoch,
                pg_id,
                source_node_id,
                state,
                has_pending_metadata_command,
                retained_log_entries,
            )?,
        )
    }

    pub fn export_pg_metadata_transfer_artifact_from_retained_log(
        &self,
        pg_id: PgId,
        source_node_id: NodeId,
    ) -> Result<PgMetadataTransferArtifact, PgMetadataTransferError> {
        self.export_pg_metadata_transfer_from_retained_log(pg_id, source_node_id)
            .map_err(Into::into)
    }

    #[allow(dead_code)]
    pub(crate) fn export_pg_metadata_transfer_from_checkpoint(
        &self,
        pg_id: PgId,
        source_node_id: NodeId,
    ) -> Result<PgMetadataTransferArtifact, PgPeeringReconstructionFailure> {
        let route = self
            .local_pg_route(pg_id)
            .ok_or(StoreError::ClusterPgNotFound {
                pg_id: pg_id.get(),
                cluster_epoch: self.operation_epoch(),
            })?;
        if route.cluster_epoch() != self.operation_epoch() {
            return Err(StoreError::StaleMetadataRoute {
                pg_id: pg_id.get(),
                route_epoch: route.cluster_epoch(),
                current_epoch: self.operation_epoch(),
            }
            .into());
        }
        if route.state() != PgState::Peering {
            return Err(PgPeeringReconstructionError::TransferSourceNotQuiesced {
                pg_id,
                cluster_epoch: self.operation_epoch(),
                state: route.state(),
            }
            .into());
        }
        if route.primary_node_id() != source_node_id {
            return Err(PgPeeringReconstructionError::TransferSourceNotPrimary {
                pg_id,
                cluster_epoch: self.operation_epoch(),
                source_node: source_node_id,
                primary: route.primary_node_id(),
            }
            .into());
        }
        let nodes = self
            .local_map
            .metadata_pg_acting_nodes_for_peering_inspection(self.operation_epoch(), pg_id)?;
        let source_node = nodes
            .iter()
            .find(|node| node.node_id() == source_node_id)
            .ok_or(PgPeeringReconstructionError::PrimaryMissing {
                primary: source_node_id,
            })?;
        let metadata_client = source_node.metadata_command_client();
        let state = metadata_client.metadata_command_replica_state(pg_id)?;
        if state.cluster_epoch > self.operation_epoch() {
            return Err(PgPeeringReconstructionError::StaleReplicaEpoch {
                node_id: source_node_id,
                replica_epoch: state.cluster_epoch,
                cluster_epoch: self.operation_epoch(),
            }
            .into());
        }
        let checkpoint = metadata_client.metadata_command_checkpoint(pg_id, state.cluster_epoch)?;
        let proof = PgMetadataProof {
            applied_log_index: checkpoint.applied_log_index,
            applied_log_hash: checkpoint.applied_log_hash,
            state_digest: checkpoint.state_digest,
        };
        Ok(PgMetadataTransferArtifact {
            pg_id,
            source_node_id,
            cluster_epoch: checkpoint.cluster_epoch,
            base_kind: PgMetadataTransferBaseKind::Checkpoint,
            base_proof: proof,
            checkpoint_base: Some(checkpoint),
            proof,
            retained_log_entries: Vec::new(),
        })
    }

    pub fn export_pg_metadata_transfer_artifact_from_checkpoint(
        &self,
        pg_id: PgId,
        source_node_id: NodeId,
    ) -> Result<PgMetadataTransferArtifact, PgMetadataTransferError> {
        self.export_pg_metadata_transfer_from_checkpoint(pg_id, source_node_id)
            .map_err(Into::into)
    }

    fn metadata_transfer_source_state(
        &self,
        pg_id: PgId,
        source_node_id: NodeId,
    ) -> Result<MetadataCommandReplicaState, PgPeeringReconstructionFailure> {
        let route = self
            .local_pg_route(pg_id)
            .ok_or(StoreError::ClusterPgNotFound {
                pg_id: pg_id.get(),
                cluster_epoch: self.operation_epoch(),
            })?;
        if route.cluster_epoch() != self.operation_epoch() {
            return Err(StoreError::StaleMetadataRoute {
                pg_id: pg_id.get(),
                route_epoch: route.cluster_epoch(),
                current_epoch: self.operation_epoch(),
            }
            .into());
        }
        if route.state() != PgState::Peering {
            return Err(PgPeeringReconstructionError::TransferSourceNotQuiesced {
                pg_id,
                cluster_epoch: self.operation_epoch(),
                state: route.state(),
            }
            .into());
        }
        if route.primary_node_id() != source_node_id {
            return Err(PgPeeringReconstructionError::TransferSourceNotPrimary {
                pg_id,
                cluster_epoch: self.operation_epoch(),
                source_node: source_node_id,
                primary: route.primary_node_id(),
            }
            .into());
        }
        let nodes = self
            .local_map
            .metadata_pg_acting_nodes_for_peering_inspection(self.operation_epoch(), pg_id)?;
        let source_node = nodes
            .iter()
            .find(|node| node.node_id() == source_node_id)
            .ok_or(PgPeeringReconstructionError::PrimaryMissing {
                primary: source_node_id,
            })?;
        let state = source_node
            .metadata_command_client()
            .metadata_command_replica_state(pg_id)?;
        if state.cluster_epoch > self.operation_epoch() {
            return Err(PgPeeringReconstructionError::StaleReplicaEpoch {
                node_id: source_node_id,
                replica_epoch: state.cluster_epoch,
                cluster_epoch: self.operation_epoch(),
            }
            .into());
        }
        Ok(state)
    }

    fn metadata_transfer_source_checkpoint_candidates(
        &self,
        pg_id: PgId,
        source_node_id: NodeId,
        source_state: &MetadataCommandReplicaState,
        max_applied_log_index: u64,
        limit: usize,
    ) -> Result<Vec<MetadataCommandCheckpoint>, PgPeeringReconstructionFailure> {
        let nodes = self
            .local_map
            .metadata_pg_acting_nodes_for_peering_inspection(self.operation_epoch(), pg_id)?;
        let source_node = nodes
            .iter()
            .find(|node| node.node_id() == source_node_id)
            .ok_or(PgPeeringReconstructionError::PrimaryMissing {
                primary: source_node_id,
            })?;
        Ok(source_node
            .metadata_command_client()
            .metadata_command_checkpoint_candidates(
                pg_id,
                source_state.cluster_epoch,
                max_applied_log_index,
                limit,
            )?)
    }

    #[allow(dead_code)]
    pub(crate) fn export_pg_metadata_transfer_from_checkpoint_and_retained_suffix(
        &self,
        pg_id: PgId,
        source_node_id: NodeId,
        checkpoint: MetadataCommandCheckpoint,
    ) -> Result<PgMetadataTransferArtifact, PgPeeringReconstructionFailure> {
        let route = self
            .local_pg_route(pg_id)
            .ok_or(StoreError::ClusterPgNotFound {
                pg_id: pg_id.get(),
                cluster_epoch: self.operation_epoch(),
            })?;
        if route.cluster_epoch() != self.operation_epoch() {
            return Err(StoreError::StaleMetadataRoute {
                pg_id: pg_id.get(),
                route_epoch: route.cluster_epoch(),
                current_epoch: self.operation_epoch(),
            }
            .into());
        }
        if route.state() != PgState::Peering {
            return Err(PgPeeringReconstructionError::TransferSourceNotQuiesced {
                pg_id,
                cluster_epoch: self.operation_epoch(),
                state: route.state(),
            }
            .into());
        }
        if route.primary_node_id() != source_node_id {
            return Err(PgPeeringReconstructionError::TransferSourceNotPrimary {
                pg_id,
                cluster_epoch: self.operation_epoch(),
                source_node: source_node_id,
                primary: route.primary_node_id(),
            }
            .into());
        }
        checkpoint.verify().map_err(|_| {
            PgPeeringReconstructionError::MetadataTransferCheckpointProofMismatch {
                node_id: source_node_id,
                pg_id,
                checkpoint: PgMetadataProof {
                    applied_log_index: checkpoint.applied_log_index,
                    applied_log_hash: checkpoint.applied_log_hash,
                    state_digest: checkpoint.state_digest,
                },
                expected: PgMetadataProof {
                    applied_log_index: checkpoint.applied_log_index,
                    applied_log_hash: checkpoint.applied_log_hash,
                    state_digest: checkpoint.state_digest,
                },
            }
        })?;
        let checkpoint_proof = PgMetadataProof {
            applied_log_index: checkpoint.applied_log_index,
            applied_log_hash: checkpoint.applied_log_hash,
            state_digest: checkpoint.state_digest,
        };
        if checkpoint.pg_id != pg_id {
            return Err(
                PgPeeringReconstructionError::MetadataTransferCheckpointProofMismatch {
                    node_id: source_node_id,
                    pg_id,
                    checkpoint: checkpoint_proof,
                    expected: checkpoint_proof,
                }
                .into(),
            );
        }
        let nodes = self
            .local_map
            .metadata_pg_acting_nodes_for_peering_inspection(self.operation_epoch(), pg_id)?;
        let source_node = nodes
            .iter()
            .find(|node| node.node_id() == source_node_id)
            .ok_or(PgPeeringReconstructionError::PrimaryMissing {
                primary: source_node_id,
            })?;
        let metadata_client = source_node.metadata_command_client();
        let state = metadata_client.metadata_command_replica_state(pg_id)?;
        if state.cluster_epoch > self.operation_epoch() {
            return Err(PgPeeringReconstructionError::StaleReplicaEpoch {
                node_id: source_node_id,
                replica_epoch: state.cluster_epoch,
                cluster_epoch: self.operation_epoch(),
            }
            .into());
        }
        if state.cluster_epoch != checkpoint.cluster_epoch {
            return Err(PgPeeringReconstructionError::StaleReplicaEpoch {
                node_id: source_node_id,
                replica_epoch: state.cluster_epoch,
                cluster_epoch: checkpoint.cluster_epoch,
            }
            .into());
        }
        if metadata_client
            .pending_metadata_command_envelope(pg_id, state.cluster_epoch)?
            .is_some()
        {
            return Err(PgPeeringReconstructionError::PendingMetadataCommand {
                node_id: source_node_id,
            }
            .into());
        }

        let mut retained_log_entries = Vec::new();
        if let Some(mut batch_start) = checkpoint.applied_log_index.checked_add(1) {
            while batch_start <= state.applied_log_index {
                let batch_end =
                    state.applied_log_index.min(batch_start.saturating_add(
                        STORAGE_RPC_MAX_METADATA_COMMAND_LOG_ENTRY_RANGE_ENTRIES - 1,
                    ));
                let first_log_index = MetadataCommandLogIndex::new(batch_start)
                    .expect("metadata transfer checkpoint suffix range starts after checkpoint");
                let last_log_index = MetadataCommandLogIndex::new(batch_end)
                    .expect("metadata transfer checkpoint suffix range ends after checkpoint");
                retained_log_entries.extend(
                    metadata_client.retained_metadata_command_log_entries(
                        pg_id,
                        state.cluster_epoch,
                        first_log_index,
                        last_log_index,
                    )?,
                );
                if batch_end == u64::MAX {
                    break;
                }
                batch_start = batch_end + 1;
            }
        }

        let artifact = PgMetadataTransferArtifact {
            pg_id,
            source_node_id,
            cluster_epoch: checkpoint.cluster_epoch,
            base_kind: PgMetadataTransferBaseKind::Checkpoint,
            base_proof: checkpoint_proof,
            checkpoint_base: Some(checkpoint),
            proof: PgMetadataProof {
                applied_log_index: state.applied_log_index,
                applied_log_hash: state.applied_log_hash,
                state_digest: state.state_digest,
            },
            retained_log_entries,
        };
        rebase_pg_metadata_transfer_artifact_commands(&artifact, self.operation_epoch())?;
        Ok(artifact)
    }

    pub fn export_pg_metadata_transfer_artifact_from_checkpoint_and_retained_suffix(
        &self,
        pg_id: PgId,
        source_node_id: NodeId,
        checkpoint: MetadataCommandCheckpoint,
    ) -> Result<PgMetadataTransferArtifact, PgMetadataTransferError> {
        self.export_pg_metadata_transfer_from_checkpoint_and_retained_suffix(
            pg_id,
            source_node_id,
            checkpoint,
        )
        .map_err(Into::into)
    }

    #[allow(dead_code)]
    pub(crate) fn export_pg_metadata_transfer_artifact_for_live_transfer_with_checkpoints(
        &self,
        pg_id: PgId,
        source_node_id: NodeId,
        checkpoints: impl IntoIterator<Item = MetadataCommandCheckpoint>,
    ) -> Result<PgMetadataTransferArtifact, PgMetadataTransferError> {
        match self.export_pg_metadata_transfer_from_retained_log(pg_id, source_node_id) {
            Ok(artifact) => return Ok(artifact),
            Err(error) if retained_log_export_failure_allows_checkpoint_fallback(&error) => {}
            Err(error) => return Err(error.into()),
        }

        let source_state = self
            .metadata_transfer_source_state(pg_id, source_node_id)
            .map_err(PgMetadataTransferError::from)?;
        self.export_pg_metadata_transfer_artifact_from_checkpoint_candidates(
            pg_id,
            source_node_id,
            &source_state,
            checkpoints,
        )
    }

    fn export_pg_metadata_transfer_artifact_from_checkpoint_candidates(
        &self,
        pg_id: PgId,
        source_node_id: NodeId,
        source_state: &MetadataCommandReplicaState,
        checkpoints: impl IntoIterator<Item = MetadataCommandCheckpoint>,
    ) -> Result<PgMetadataTransferArtifact, PgMetadataTransferError> {
        let mut candidates: Vec<_> = checkpoints.into_iter().collect();
        candidates.sort_by(|left, right| {
            right
                .applied_log_index
                .cmp(&left.applied_log_index)
                .then_with(|| right.applied_log_hash.cmp(&left.applied_log_hash))
        });

        for checkpoint in candidates {
            if let Some(artifact) = self
                .try_export_pg_metadata_transfer_artifact_from_checkpoint_candidate(
                    pg_id,
                    source_node_id,
                    source_state,
                    checkpoint,
                )?
            {
                return Ok(artifact);
            }
        }

        self.export_pg_metadata_transfer_from_checkpoint(pg_id, source_node_id)
            .map_err(Into::into)
    }

    fn try_export_pg_metadata_transfer_artifact_from_checkpoint_candidate(
        &self,
        pg_id: PgId,
        source_node_id: NodeId,
        source_state: &MetadataCommandReplicaState,
        checkpoint: MetadataCommandCheckpoint,
    ) -> Result<Option<PgMetadataTransferArtifact>, PgMetadataTransferError> {
        if checkpoint.pg_id != pg_id
            || checkpoint.cluster_epoch != source_state.cluster_epoch
            || checkpoint.applied_log_index > source_state.applied_log_index
        {
            return Ok(None);
        }
        if checkpoint.verify().is_err() {
            return Ok(None);
        }
        match self.export_pg_metadata_transfer_from_checkpoint_and_retained_suffix(
            pg_id,
            source_node_id,
            checkpoint,
        ) {
            Ok(artifact) => Ok(Some(artifact)),
            Err(PgPeeringReconstructionFailure::Reconstruction(_)) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn export_pg_metadata_transfer_artifact_from_paged_checkpoint_candidates(
        &self,
        pg_id: PgId,
        source_node_id: NodeId,
        source_state: &MetadataCommandReplicaState,
    ) -> Result<PgMetadataTransferArtifact, PgMetadataTransferError> {
        let mut max_applied_log_index = source_state.applied_log_index;
        for _ in 0..STORAGE_RPC_MAX_METADATA_COMMAND_CHECKPOINT_CANDIDATES {
            let Some(checkpoint) = self
                .metadata_transfer_source_checkpoint_candidates(
                    pg_id,
                    source_node_id,
                    source_state,
                    max_applied_log_index,
                    1,
                )
                .map_err(PgMetadataTransferError::from)?
                .into_iter()
                .next()
            else {
                break;
            };
            let next_max_applied_log_index = checkpoint.applied_log_index.checked_sub(1);
            if let Some(artifact) = self
                .try_export_pg_metadata_transfer_artifact_from_checkpoint_candidate(
                    pg_id,
                    source_node_id,
                    source_state,
                    checkpoint,
                )?
            {
                return Ok(artifact);
            }
            let Some(next_max_applied_log_index) = next_max_applied_log_index else {
                break;
            };
            max_applied_log_index = next_max_applied_log_index;
        }
        self.export_pg_metadata_transfer_from_checkpoint(pg_id, source_node_id)
            .map_err(Into::into)
    }

    pub fn export_pg_metadata_transfer_artifact_for_live_transfer(
        &self,
        pg_id: PgId,
        source_node_id: NodeId,
    ) -> Result<PgMetadataTransferArtifact, PgMetadataTransferError> {
        match self.export_pg_metadata_transfer_from_retained_log(pg_id, source_node_id) {
            Ok(artifact)
                if artifact.source_base_kind() != PgMetadataTransferBaseKind::RetainedLogPrefix =>
            {
                return Ok(artifact);
            }
            Ok(_) => {}
            Err(error) if retained_log_export_failure_allows_checkpoint_fallback(&error) => {}
            Err(error) => return Err(error.into()),
        }

        let source_state = self
            .metadata_transfer_source_state(pg_id, source_node_id)
            .map_err(PgMetadataTransferError::from)?;
        self.export_pg_metadata_transfer_artifact_from_paged_checkpoint_candidates(
            pg_id,
            source_node_id,
            &source_state,
        )
    }

    #[allow(dead_code)]
    pub(crate) fn import_pg_metadata_transfer_from_retained_log(
        &self,
        artifact: &PgMetadataTransferArtifact,
    ) -> Result<PgMetadataProof, PgPeeringReconstructionFailure> {
        let pg_id = artifact.pg_id;
        let commands =
            rebase_pg_metadata_transfer_artifact_commands(artifact, self.operation_epoch())?;
        let expected_import_proof =
            metadata_transfer_destination_proof(artifact, &commands, self.operation_epoch());
        let base_import_proof = artifact.source_base_metadata_proof();
        let checkpoint_base = artifact.checkpoint_base();
        let nodes = self
            .local_map
            .metadata_pg_acting_nodes_for_peering_replay(self.operation_epoch(), pg_id)?;

        let mut reference: Option<(NodeId, PgMetadataProof)> = None;
        for node in nodes {
            let metadata_client = node.metadata_command_client();
            let state = if let Some(checkpoint) = checkpoint_base {
                let checkpoint_destination_base_proof = PgMetadataProof {
                    applied_log_index: 0,
                    applied_log_hash: 0,
                    state_digest: checkpoint.state_digest,
                };
                if metadata_client
                    .pending_metadata_command_envelope(pg_id, self.operation_epoch())?
                    .is_some()
                {
                    return Err(PgPeeringReconstructionError::PendingMetadataCommand {
                        node_id: node.node_id(),
                    }
                    .into());
                }
                let current = metadata_client.metadata_command_replica_state(pg_id)?;
                if current.cluster_epoch != self.operation_epoch()
                    && metadata_client
                        .pending_metadata_command_envelope(pg_id, current.cluster_epoch)?
                        .is_some()
                {
                    return Err(PgPeeringReconstructionError::PendingMetadataCommand {
                        node_id: node.node_id(),
                    }
                    .into());
                }
                let current_proof = PgMetadataProof {
                    applied_log_index: current.applied_log_index,
                    applied_log_hash: current.applied_log_hash,
                    state_digest: current.state_digest,
                };
                if current.cluster_epoch == self.operation_epoch() {
                    if let Some(prefix_len) = checkpoint_import_resume_prefix_len(
                        pg_id,
                        checkpoint_destination_base_proof,
                        expected_import_proof,
                        current_proof,
                        &commands,
                        self.operation_epoch(),
                    ) {
                        let validated = metadata_client
                            .validate_metadata_command_replay_state_preserving_pending_slot(
                                pg_id,
                                self.operation_epoch(),
                            )?;
                        let validated_proof = PgMetadataProof {
                            applied_log_index: validated.applied_log_index,
                            applied_log_hash: validated.applied_log_hash,
                            state_digest: validated.state_digest,
                        };
                        let Some(validated_prefix_len) = checkpoint_import_resume_prefix_len(
                            pg_id,
                            checkpoint_destination_base_proof,
                            expected_import_proof,
                            validated_proof,
                            &commands,
                            self.operation_epoch(),
                        ) else {
                            return Err(PgPeeringReconstructionError::MetadataFork {
                                node_id: node.node_id(),
                                reference_node_id: node.node_id(),
                                replica: validated_proof,
                                reference: expected_import_proof,
                            }
                            .into());
                        };
                        if validated_prefix_len != prefix_len {
                            return Err(PgPeeringReconstructionError::MetadataFork {
                                node_id: node.node_id(),
                                reference_node_id: node.node_id(),
                                replica: validated_proof,
                                reference: current_proof,
                            }
                            .into());
                        }
                        let mut state = validated;
                        for command in &commands[prefix_len..] {
                            state = metadata_client
                                .replay_metadata_command_for_peering(pg_id, &command.command)?;
                        }
                        state
                    } else if metadata_client.metadata_command_replica_state_can_initialize(
                        pg_id,
                        self.operation_epoch(),
                    )? {
                        let mut state = metadata_client.install_metadata_transfer_checkpoint_base(
                            pg_id,
                            self.operation_epoch(),
                            checkpoint,
                        )?;
                        for command in &commands {
                            state = metadata_client
                                .replay_metadata_command_for_peering(pg_id, &command.command)?;
                        }
                        state
                    } else {
                        return Err(
                            PgPeeringReconstructionError::DirtyMetadataTransferDestination {
                                node_id: node.node_id(),
                                pg_id,
                                cluster_epoch: self.operation_epoch(),
                                applied_log_index: current.applied_log_index,
                                applied_log_hash: current.applied_log_hash,
                                state_digest: current.state_digest,
                                expected: expected_import_proof,
                            }
                            .into(),
                        );
                    }
                } else {
                    let mut state = metadata_client.install_metadata_transfer_checkpoint_base(
                        pg_id,
                        self.operation_epoch(),
                        checkpoint,
                    )?;
                    for command in &commands {
                        state = metadata_client
                            .replay_metadata_command_for_peering(pg_id, &command.command)?;
                    }
                    state
                }
            } else {
                match classify_metadata_transfer_import_destination(
                    metadata_client.as_ref(),
                    node.node_id(),
                    pg_id,
                    self.operation_epoch(),
                    base_import_proof,
                    expected_import_proof,
                    &commands,
                )? {
                    MetadataTransferImportDestination::AlreadyImported(state) => state,
                    MetadataTransferImportDestination::Empty => {
                        if commands.is_empty() {
                            metadata_client.initialize_metadata_transfer_empty_state(
                                pg_id,
                                self.operation_epoch(),
                                artifact.proof.state_digest,
                            )?
                        } else {
                            let mut state =
                                metadata_client.metadata_command_replica_state(pg_id)?;
                            for command in &commands {
                                state = metadata_client
                                    .replay_metadata_command_for_peering(pg_id, &command.command)?;
                            }
                            state
                        }
                    }
                    MetadataTransferImportDestination::AdoptBase => {
                        metadata_client.initialize_metadata_transfer_matching_state(
                            pg_id,
                            self.operation_epoch(),
                            0,
                            0,
                            base_import_proof.state_digest,
                        )?;
                        let mut state = metadata_client.metadata_command_replica_state(pg_id)?;
                        for command in &commands {
                            state = metadata_client
                                .replay_metadata_command_for_peering(pg_id, &command.command)?;
                        }
                        state
                    }
                    MetadataTransferImportDestination::AdoptExisting => {
                        if commands.is_empty() {
                            metadata_client.initialize_metadata_transfer_matching_state(
                                pg_id,
                                self.operation_epoch(),
                                expected_import_proof.applied_log_index,
                                expected_import_proof.applied_log_hash,
                                expected_import_proof.state_digest,
                            )?
                        } else {
                            metadata_client.adopt_metadata_transfer_state_from_rebased_commands(
                                pg_id,
                                self.operation_epoch(),
                                &commands,
                                artifact.proof.state_digest,
                            )?
                        }
                    }
                    MetadataTransferImportDestination::AdoptPrefix { prefix_len } => {
                        if prefix_len > 0 {
                            metadata_client.adopt_metadata_transfer_state_from_rebased_commands(
                                pg_id,
                                self.operation_epoch(),
                                &commands[..prefix_len],
                                commands[prefix_len - 1].post_state_digest,
                            )?;
                        }
                        let mut state = metadata_client.metadata_command_replica_state(pg_id)?;
                        for command in &commands[prefix_len..] {
                            state = metadata_client
                                .replay_metadata_command_for_peering(pg_id, &command.command)?;
                        }
                        state
                    }
                }
            };
            if state.cluster_epoch != self.operation_epoch() {
                return Err(PgPeeringReconstructionError::StaleReplicaEpoch {
                    node_id: node.node_id(),
                    replica_epoch: state.cluster_epoch,
                    cluster_epoch: self.operation_epoch(),
                }
                .into());
            }
            let proof = PgMetadataProof {
                applied_log_index: state.applied_log_index,
                applied_log_hash: state.applied_log_hash,
                state_digest: state.state_digest,
            };
            if let Some((reference_node_id, reference_proof)) = reference {
                if proof != reference_proof {
                    return Err(PgPeeringReconstructionError::MetadataFork {
                        node_id: node.node_id(),
                        reference_node_id,
                        replica: proof,
                        reference: reference_proof,
                    }
                    .into());
                }
            } else {
                reference = Some((node.node_id(), proof));
            }
        }

        let (_reference_node_id, proof) =
            reference.ok_or(PgPeeringReconstructionError::PrimaryMissing {
                primary: artifact.source_node_id,
            })?;
        if proof != expected_import_proof {
            return Err(PgPeeringReconstructionError::MetadataFork {
                node_id: artifact.source_node_id,
                reference_node_id: artifact.source_node_id,
                replica: proof,
                reference: expected_import_proof,
            }
            .into());
        }
        Ok(proof)
    }

    pub fn import_pg_metadata_transfer_artifact_from_retained_log(
        &self,
        artifact: &PgMetadataTransferArtifact,
    ) -> Result<PgMetadataProof, PgMetadataTransferError> {
        self.import_pg_metadata_transfer_from_retained_log(artifact)
            .map_err(Into::into)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn maybe_run_before_stream_abort_storage_hook(&self) {
        let hook = self
            .test_hooks
            .lock()
            .unwrap()
            .before_stream_abort_storage
            .clone();
        if let Some(hook) = hook {
            hook();
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn maybe_run_before_metadata_command_pending_install_hook(&self) {
        let hook = self
            .test_hooks
            .lock()
            .unwrap()
            .before_metadata_command_pending_install
            .clone();
        if let Some(hook) = hook {
            hook();
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn maybe_run_before_direct_put_command_id_hook(&self) {
        let hook = self
            .test_hooks
            .lock()
            .unwrap()
            .before_direct_put_command_id
            .clone();
        if let Some(hook) = hook {
            hook();
        }
    }

    #[cfg(not(any(test, feature = "test-hooks")))]
    fn maybe_run_before_direct_put_command_id_hook(&self) {}

    #[cfg(any(test, feature = "test-hooks"))]
    fn maybe_run_before_object_generation_command_id_hook(&self) {
        let hook = self
            .test_hooks
            .lock()
            .unwrap()
            .before_object_generation_command_id
            .clone();
        if let Some(hook) = hook {
            hook();
        }
    }

    #[cfg(not(any(test, feature = "test-hooks")))]
    fn maybe_run_before_object_generation_command_id_hook(&self) {}

    #[cfg(any(test, feature = "test-hooks"))]
    fn maybe_run_before_stream_append_command_id_hook(&self) {
        let hook = self
            .test_hooks
            .lock()
            .unwrap()
            .before_stream_append_command_id
            .clone();
        if let Some(hook) = hook {
            hook();
        }
    }

    #[cfg(not(any(test, feature = "test-hooks")))]
    fn maybe_run_before_stream_append_command_id_hook(&self) {}

    #[cfg(any(test, feature = "test-hooks"))]
    fn maybe_run_after_object_metadata_reservation_acquired_hook(
        &self,
    ) -> Result<(), ObjectPgActionError> {
        let hook = self
            .test_hooks
            .lock()
            .unwrap()
            .after_object_metadata_reservation_acquired
            .clone();
        if let Some(hook) = hook {
            hook()?;
        }
        Ok(())
    }

    #[cfg(not(any(test, feature = "test-hooks")))]
    fn maybe_run_after_object_metadata_reservation_acquired_hook(
        &self,
    ) -> Result<(), ObjectPgActionError> {
        Ok(())
    }

    #[cfg(not(any(test, feature = "test-hooks")))]
    fn maybe_run_before_metadata_command_pending_install_hook(&self) {}

    #[cfg(any(test, feature = "test-hooks"))]
    fn maybe_run_before_placed_payload_shard_delete_hook(
        &self,
        shard_key: &ShardKey,
    ) -> Result<(), StoreError> {
        let hook = self
            .test_hooks
            .lock()
            .unwrap()
            .before_placed_payload_shard_delete
            .clone();
        if let Some(hook) = hook {
            hook(shard_key)?;
        }
        Ok(())
    }

    #[cfg(not(any(test, feature = "test-hooks")))]
    fn maybe_run_before_placed_payload_shard_delete_hook(
        &self,
        _shard_key: &ShardKey,
    ) -> Result<(), StoreError> {
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn maybe_run_before_placed_payload_shard_read_hook(
        &self,
        location: ShardLocation,
        shard_key: &ShardKey,
    ) -> Result<(), ShardIoError> {
        let hook = self
            .test_hooks
            .lock()
            .unwrap()
            .before_placed_payload_shard_read
            .clone();
        if let Some(hook) = hook {
            hook(&location, shard_key).map_err(|source| ShardIoError::Store {
                node_id: location.node_id().as_u32(),
                pg_id: location.data_pg_id().get(),
                cluster_epoch: location.cluster_epoch(),
                source,
            })?;
        }
        Ok(())
    }

    #[cfg(not(any(test, feature = "test-hooks")))]
    fn maybe_run_before_placed_payload_shard_read_hook(
        &self,
        _location: ShardLocation,
        _shard_key: &ShardKey,
    ) -> Result<(), ShardIoError> {
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn maybe_run_before_metadata_primary_payload_ack_delete_hook(
        &self,
        shard_key: &ShardKey,
    ) -> Result<(), StoreError> {
        let hook = self
            .test_hooks
            .lock()
            .unwrap()
            .before_metadata_primary_payload_ack_delete
            .clone();
        if let Some(hook) = hook {
            hook(shard_key)?;
        }
        Ok(())
    }

    #[cfg(not(any(test, feature = "test-hooks")))]
    fn maybe_run_before_metadata_primary_payload_ack_delete_hook(
        &self,
        _shard_key: &ShardKey,
    ) -> Result<(), StoreError> {
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn maybe_observe_best_effort_payload_cleanup_error(
        &self,
        operation: &'static str,
        error: &StoreError,
    ) {
        let hook = self
            .test_hooks
            .lock()
            .unwrap()
            .best_effort_payload_cleanup_error
            .clone();
        if let Some(hook) = hook {
            hook(operation, error);
        }
    }

    #[cfg(not(any(test, feature = "test-hooks")))]
    fn maybe_observe_best_effort_payload_cleanup_error(
        &self,
        _operation: &'static str,
        _error: &StoreError,
    ) {
    }

    pub fn open_local_nodes(
        data_dir: &std::path::Path,
        node_ids: &[NodeId],
        pg_ids: &[u32],
        default_ec_shape: EcShape,
    ) -> Result<Arc<Self>, ClusterBuildError> {
        let local_map = Arc::new(LocalClusterMap::open(
            data_dir,
            node_ids,
            pg_ids,
            default_ec_shape,
        )?);
        Self::from_local_map(local_map)
    }

    pub fn from_local_map(local_map: Arc<LocalClusterMap>) -> Result<Arc<Self>, ClusterBuildError> {
        Self::from_local_map_with_epoch(Arc::clone(&local_map), local_map.epoch())
    }

    pub fn from_runtime_map(
        metadata_primary_node_id: NodeId,
        runtime_map: &ClusterRuntimeMapSnapshot,
        default_ec_shape: EcShape,
    ) -> Result<Arc<Self>, ClusterBuildError> {
        let local_map = Arc::new(
            LocalClusterMap::open_frontend_topology_only_with_runtime_map(
                metadata_primary_node_id,
                runtime_map,
                default_ec_shape,
            )?,
        );
        Self::from_local_map(local_map)
    }

    pub fn from_runtime_map_with_unix_storage_node_clients(
        metadata_primary_node_id: NodeId,
        runtime_map: &ClusterRuntimeMapSnapshot,
        default_ec_shape: EcShape,
    ) -> Result<Arc<Self>, ClusterBuildError> {
        Self::from_runtime_map_with_unix_storage_node_client_admission_settings(
            metadata_primary_node_id,
            runtime_map,
            default_ec_shape,
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
        )
    }

    pub fn unix_storage_node_client_configs_from_runtime_map(
        runtime_map: &ClusterRuntimeMapSnapshot,
        admission_settings: LocalUnixStorageNodeClientAdmissionSettings,
    ) -> Vec<LocalUnixStorageNodeClientConfig> {
        runtime_map
            .nodes()
            .iter()
            .map(|node| {
                LocalUnixStorageNodeClientConfig::with_rpc_admission_settings_from_runtime_node_route(
                    node,
                    admission_settings,
                )
            })
            .collect()
    }

    pub fn from_runtime_map_with_unix_storage_node_client_admission_settings(
        metadata_primary_node_id: NodeId,
        runtime_map: &ClusterRuntimeMapSnapshot,
        default_ec_shape: EcShape,
        admission_settings: LocalUnixStorageNodeClientAdmissionSettings,
    ) -> Result<Arc<Self>, ClusterBuildError> {
        let mut local_map = LocalClusterMap::open_frontend_topology_only_with_runtime_map(
            metadata_primary_node_id,
            runtime_map,
            default_ec_shape,
        )?;
        let storage_node_configs = Self::unix_storage_node_client_configs_from_runtime_map(
            runtime_map,
            admission_settings,
        );
        local_map.install_unix_storage_node_clients(storage_node_configs)?;
        Self::from_local_map(Arc::new(local_map))
    }

    pub fn refresh_from_control_plane_runtime_map(
        &self,
        control_plane: &impl ControlPlaneRuntimeMapSource,
        authority_now_ms: u64,
    ) -> Result<Arc<Self>, StorageClusterRuntimeMapRefreshError> {
        let runtime_map = control_plane.runtime_map_snapshot(authority_now_ms)?;
        let mut local_map = LocalClusterMap::open_frontend_topology_only_with_runtime_map(
            self.metadata_node_id(),
            &runtime_map,
            self.default_payload_ec_shape(),
        )?;
        local_map.inherit_process_local_state_from(&self.local_map);
        Ok(Self::from_local_map(Arc::new(local_map))?)
    }

    pub fn refresh_from_control_plane_runtime_map_with_unix_storage_node_clients(
        &self,
        control_plane: &impl ControlPlaneRuntimeMapSource,
        authority_now_ms: u64,
        admission_settings: LocalUnixStorageNodeClientAdmissionSettings,
    ) -> Result<Arc<Self>, StorageClusterRuntimeMapRefreshError> {
        let runtime_map = control_plane.runtime_map_snapshot(authority_now_ms)?;
        let mut local_map = LocalClusterMap::open_frontend_topology_only_with_runtime_map(
            self.metadata_node_id(),
            &runtime_map,
            self.default_payload_ec_shape(),
        )?;
        local_map.inherit_process_local_state_from(&self.local_map);
        let storage_node_configs = Self::unix_storage_node_client_configs_from_runtime_map(
            &runtime_map,
            admission_settings,
        );
        local_map.install_unix_storage_node_clients(storage_node_configs)?;
        Ok(Self::from_local_map(Arc::new(local_map))?)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_from_local_map_with_epoch(
        local_map: Arc<LocalClusterMap>,
        operation_epoch: ClusterEpoch,
    ) -> Result<Arc<Self>, ClusterBuildError> {
        Self::from_local_map_with_epoch(local_map, operation_epoch)
    }

    fn from_local_map_with_epoch(
        local_map: Arc<LocalClusterMap>,
        operation_epoch: ClusterEpoch,
    ) -> Result<Arc<Self>, ClusterBuildError> {
        Ok(Arc::new(Self {
            local_map,
            operation_epoch,
            #[cfg(any(test, feature = "test-hooks"))]
            test_hooks: Arc::new(Mutex::new(StorageClusterTestHooks::default())),
        }))
    }

    pub fn cluster_epoch(&self) -> crate::ClusterEpoch {
        self.local_map.epoch()
    }

    pub fn operation_epoch(&self) -> ClusterEpoch {
        self.operation_epoch
    }

    pub fn route_map_valid_until_ms(&self) -> Option<u64> {
        self.local_map.route_map_valid_until_ms()
    }

    pub fn route_map_validity(&self) -> RouteMapValidity {
        self.local_map.route_map_validity()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_store_route_map_validity(&self, validity: RouteMapValidity) {
        self.local_map.test_store_route_map_validity(validity);
    }

    fn extend_route_map_validity(&self, candidate: RouteMapValidity) {
        self.local_map.extend_route_map_validity(candidate);
    }

    fn cap_route_map_validity(&self, candidate: RouteMapValidity) {
        self.local_map.cap_route_map_validity(candidate);
    }

    pub fn is_route_map_valid_at(&self, now_ms: u64) -> bool {
        self.local_map.is_route_map_valid_at(now_ms)
    }

    pub fn require_route_map_valid_at(&self, now_ms: u64) -> Result<(), StoreError> {
        self.local_map.require_route_map_valid_at(now_ms)
    }

    fn require_current_payload_operation_epoch(&self, pg_id: u32) -> Result<(), StoreError> {
        let current_epoch = self.cluster_epoch();
        if self.operation_epoch() != current_epoch {
            return Err(StoreError::StalePayloadOperation {
                pg_id,
                operation_epoch: self.operation_epoch(),
                current_epoch,
            });
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn require_current_metadata_primary_bridge_epoch(&self) -> Result<(), StoreError> {
        let current_epoch = self.cluster_epoch();
        if self.operation_epoch() != current_epoch {
            return Err(StoreError::StaleMetadataPrimaryBridge {
                metadata_node_id: self.metadata_node_id().as_u32(),
                operation_epoch: self.operation_epoch(),
                current_epoch,
            });
        }
        Ok(())
    }

    // Transitional metadata-primary test hook bridge. Production metadata paths
    // must use routed PG primaries.
    #[cfg(any(test, feature = "test-hooks"))]
    fn metadata_primary_bridge_node(&self) -> Result<&SharedStorageNode, StoreError> {
        self.require_current_metadata_primary_bridge_epoch()?;
        Ok(self.local_map.metadata_primary().storage_node().as_ref())
    }

    fn try_install_pending_metadata_command_for_bucket(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, ObjectPgActionError> {
        self.maybe_run_before_metadata_command_pending_install_hook();
        Ok(self
            .try_set_pending_metadata_command_for_bucket(pg_id, bucket, command)
            .map_err(ObjectPgActionError::from)?
            .is_some())
    }

    fn try_set_pending_metadata_command_for_bucket(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
    ) -> Result<Option<()>, StoreError> {
        let pg_lock = self
            .local_map
            .runtime_state()
            .metadata_command_pg_lock(pg_id);
        let _pg_guard = pg_lock.lock().unwrap_or_else(|e| e.into_inner());
        self.try_set_pending_metadata_command_for_bucket_locked(pg_id, bucket, command)
    }

    fn try_set_pending_metadata_command_for_bucket_locked(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
    ) -> Result<Option<()>, StoreError> {
        let primary = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        match primary
            .metadata_command_client()
            .try_insert_pending_metadata_command_slot(pg_id, command, Some(bucket))
        {
            Ok(()) => Ok(Some(())),
            Err(StoreError::MetadataCommandPendingConflict { .. }) => {
                self.emit_metadata_command_conflict(
                    Some(primary.node_id()),
                    pg_id,
                    Some(command.id().log_index().get()),
                    "pending_slot_conflict",
                    Some(command.payload().kind_name()),
                );
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    fn try_install_object_pg_pending_command_with_fresh_id(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        completion_admission: bool,
        build_command: impl FnOnce(MetadataCommandId) -> MetadataCommandEnvelope,
    ) -> Result<ObjectPgPendingCommandInstall, ObjectPgActionError> {
        let pg_lock = self
            .local_map
            .runtime_state()
            .metadata_command_pg_lock(pg_id);
        let _pg_guard = pg_lock.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
            return Ok(ObjectPgPendingCommandInstall::Pending(command));
        }
        let command_id = match self
            .next_object_metadata_command_id_with_completion_admission(pg_id, completion_admission)
        {
            Ok(command_id) => command_id,
            Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict { .. })) => {
                return Ok(ObjectPgPendingCommandInstall::LogConflict {
                    pending_visible: self
                        .pending_metadata_command_for_bucket(pg_id, bucket)?
                        .is_some(),
                });
            }
            Err(error) => return Err(error),
        };
        let command = build_command(command_id);
        match self.try_set_pending_metadata_command_for_bucket_locked(pg_id, bucket, &command) {
            Ok(Some(())) => Ok(ObjectPgPendingCommandInstall::Installed(command)),
            Ok(None) => {
                let Some(pending) = self.pending_metadata_command_for_bucket(pg_id, bucket)? else {
                    return Err(conflicting_pending_object_metadata_command(
                        "pending slot conflicted without visible command",
                    ));
                };
                Ok(ObjectPgPendingCommandInstall::Pending(pending))
            }
            Err(StoreError::MetadataCommandLogConflict { .. }) => {
                Ok(ObjectPgPendingCommandInstall::LogConflict {
                    pending_visible: self
                        .pending_metadata_command_for_bucket(pg_id, bucket)?
                        .is_some(),
                })
            }
            Err(error) => Err(error.into()),
        }
    }

    fn drain_after_object_pg_log_conflict(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        pending_visible: bool,
        empty_log_conflicts: &mut usize,
        context: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        let drained = if pending_visible {
            self.drain_one_pending_object_metadata_command(pg_id, bucket)?
        } else {
            false
        };
        if drained {
            *empty_log_conflicts = 0;
            return Ok(());
        }

        *empty_log_conflicts += 1;
        if *empty_log_conflicts >= OBJECT_PG_EMPTY_LOG_CONFLICT_RETRIES {
            return Err(conflicting_pending_object_metadata_command(context));
        }
        Ok(())
    }

    fn install_snapshot_sensitive_metadata_command_or_drain(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
    ) -> Result<SnapshotSensitiveCommandInstall, ObjectPgActionError> {
        match self.try_install_pending_metadata_command_for_bucket(pg_id, bucket, command) {
            Ok(true) => Ok(SnapshotSensitiveCommandInstall::Installed),
            Ok(false)
            | Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict { .. })) => {
                self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
                Ok(SnapshotSensitiveCommandInstall::ContenderDrained)
            }
            Err(error) => Err(error),
        }
    }

    fn try_set_object_pg_pending_command_or_drain(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, ObjectPgActionError> {
        match self.try_set_pending_metadata_command_for_bucket(pg_id, bucket, command) {
            Ok(Some(())) => Ok(true),
            Ok(None) | Err(StoreError::MetadataCommandLogConflict { .. }) => {
                self.drain_one_pending_object_metadata_command(pg_id, bucket)?;
                Ok(false)
            }
            Err(error) => Err(error.into()),
        }
    }

    fn try_install_object_pg_pending_command_or_drain(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, ObjectPgActionError> {
        self.maybe_run_before_metadata_command_pending_install_hook();
        self.try_set_object_pg_pending_command_or_drain(pg_id, bucket, command)
    }

    fn pending_metadata_command_for_bucket(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<Option<MetadataCommandEnvelope>, StoreError> {
        let _ = bucket;
        let primary = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        primary
            .metadata_command_client()
            .pending_metadata_command_envelope(pg_id, self.operation_epoch())
    }

    fn drain_pending_metadata_commands_for_current_map(
        &self,
    ) -> Result<usize, ObjectPgActionError> {
        let mut drained = 0usize;
        for raw_pg_id in self.local_map.pg_ids() {
            let pg_id = PgId::new(*raw_pg_id);
            let primary = self
                .local_map
                .metadata_pg_primary_node_for_metadata_command_recovery(
                    self.operation_epoch(),
                    pg_id,
                )
                .map_err(ObjectPgActionError::Store)?;
            let Some(command) = primary
                .metadata_command_client()
                .pending_metadata_command_envelope(pg_id, self.operation_epoch())
                .map_err(ObjectPgActionError::Store)?
            else {
                continue;
            };
            let _outcome =
                self.drain_pending_metadata_command_with_recovery_gate(pg_id, &command)?;
            drained += 1;
        }
        Ok(drained)
    }

    fn remove_pending_metadata_command_for_bucket(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, StoreError> {
        let primary = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let _ = bucket;
        primary
            .metadata_command_client()
            .remove_pending_metadata_command_slot(pg_id, command)
    }

    fn remove_pending_metadata_command_for_bucket_recovery(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, StoreError> {
        let primary = self
            .local_map
            .metadata_pg_primary_node_for_metadata_command_recovery(
                self.operation_epoch(),
                pg_id,
            )?;
        let _ = bucket;
        primary
            .metadata_command_client()
            .remove_pending_metadata_command_slot(pg_id, command)
    }

    fn next_metadata_command_id(&self, pg_id: PgId) -> Result<MetadataCommandId, StoreError> {
        self.next_metadata_command_id_at_least(
            pg_id,
            MetadataCommandLogIndex::new(1).expect("metadata command log index starts at one"),
        )
    }

    fn next_completion_metadata_command_id(
        &self,
        pg_id: PgId,
    ) -> Result<MetadataCommandId, StoreError> {
        self.next_completion_metadata_command_id_at_least(
            pg_id,
            MetadataCommandLogIndex::new(1).expect("metadata command log index starts at one"),
        )
    }

    fn next_metadata_command_id_at_least(
        &self,
        pg_id: PgId,
        min_log_index: MetadataCommandLogIndex,
    ) -> Result<MetadataCommandId, StoreError> {
        let primary = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        primary
            .metadata_command_client()
            .next_metadata_command_id_at_least(pg_id, self.operation_epoch(), min_log_index)
    }

    fn next_completion_metadata_command_id_at_least(
        &self,
        pg_id: PgId,
        min_log_index: MetadataCommandLogIndex,
    ) -> Result<MetadataCommandId, StoreError> {
        let primary = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        primary
            .metadata_command_client()
            .next_completion_metadata_command_id_at_least(
                pg_id,
                self.operation_epoch(),
                min_log_index,
            )
    }

    #[cfg(test)]
    fn next_metadata_command_id_from_locked_pg(
        &self,
        pg_id: PgId,
        pg: &crate::PgStore,
    ) -> Result<MetadataCommandId, StoreError> {
        self.next_metadata_command_id_from_locked_pg_at_least(
            pg_id,
            pg,
            MetadataCommandLogIndex::new(1).expect("metadata command log index starts at one"),
        )
    }

    #[cfg(test)]
    fn next_metadata_command_id_from_locked_pg_at_least(
        &self,
        pg_id: PgId,
        pg: &crate::PgStore,
        min_log_index: MetadataCommandLogIndex,
    ) -> Result<MetadataCommandId, StoreError> {
        let primary = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let max_log_index = pg.max_metadata_command_log_index(self.operation_epoch())?;
        if let Some(slot) =
            pg.pending_metadata_command_slot(primary.node_id().as_u32(), self.operation_epoch())?
        {
            return Err(StoreError::MetadataCommandLogConflict {
                node_id: primary.node_id().as_u32(),
                pg_id: pg_id.get(),
                cluster_epoch: self.operation_epoch(),
                log_index: slot.id.log_index().get(),
            });
        }
        let next_log_index = max_log_index
            .checked_add(1)
            .map(|next| next.max(min_log_index.get()))
            .and_then(MetadataCommandLogIndex::new)
            .ok_or(StoreError::MetadataCommandLogConflict {
                node_id: primary.node_id().as_u32(),
                pg_id: pg_id.get(),
                cluster_epoch: self.operation_epoch(),
                log_index: u64::MAX,
            })?;
        Ok(MetadataCommandId::new(
            self.operation_epoch(),
            pg_id,
            next_log_index,
        ))
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn metadata_primary_test_hook_node(&self) -> &SharedStorageNode {
        self.metadata_primary_bridge_node()
            .expect("test hook requires a current storage cluster handle")
    }

    pub fn metadata_node_id(&self) -> NodeId {
        self.local_map.metadata_primary_node_id()
    }

    pub fn local_node_count(&self) -> usize {
        self.local_map.node_count()
    }

    pub fn local_node_ids(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.local_map.node_ids()
    }

    pub fn cluster_map_history_reference_summary(
        &self,
    ) -> Result<PgClusterMapHistoryReferenceSummary, StoreError> {
        self.local_map.cluster_map_history_reference_summary()
    }

    pub fn local_pg_route(&self, pg_id: PgId) -> Option<&LocalPgRoute> {
        self.local_map.pg_route(pg_id)
    }

    pub fn local_pg_routes(&self) -> impl Iterator<Item = &LocalPgRoute> + '_ {
        self.local_map.pg_routes()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_clone_with_pg_routes(
        &self,
        cluster_epoch: ClusterEpoch,
        pg_routes: impl IntoIterator<Item = PgRouteSnapshot>,
        historical_pg_routes: impl IntoIterator<Item = PgRouteSnapshot>,
    ) -> Result<Arc<Self>, ClusterBuildError> {
        let local_map = self.local_map.test_clone_with_pg_routes(
            cluster_epoch,
            pg_routes,
            historical_pg_routes,
        )?;
        Self::from_local_map(Arc::new(local_map))
    }

    pub fn reconstructed_pg_route_at_epoch(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<PgRouteSnapshot, StoreError> {
        self.local_map
            .reconstructed_pg_route_at_epoch(pg_id, cluster_epoch)
            .ok_or_else(|| StoreError::PayloadShardSetMismatch {
                reason: format!(
                    "PG {} route for cluster epoch {} is not retained",
                    pg_id.get(),
                    cluster_epoch.get()
                ),
            })
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_object_pg_metadata_proof(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<PgMetadataProof, StoreError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        let primary = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let state = primary
            .metadata_command_client()
            .metadata_command_replica_state(pg_id)?;
        Ok(PgMetadataProof {
            applied_log_index: state.applied_log_index,
            applied_log_hash: state.applied_log_hash,
            state_digest: state.state_digest,
        })
    }

    /// Temporary process-local registry key for shared coordinator workers.
    ///
    /// Multiple `StorageCluster` handles backed by the same local node keep
    /// sharing process-local workers until a real cluster identity exists.
    pub fn process_local_registry_key(&self) -> usize {
        self.local_map.process_local_registry_key()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_metadata_command_pg_lock_ptr(&self, pg_id: PgId) -> usize {
        self.local_map
            .runtime_state()
            .test_metadata_command_pg_lock_ptr(pg_id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_metadata_command_recovery_flight_count(&self) -> usize {
        self.local_map
            .runtime_state()
            .test_metadata_command_recovery_flight_count()
    }

    #[cfg(test)]
    pub(crate) fn test_begin_metadata_command_recovery_leader(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> MetadataCommandRecoveryTestGuard {
        match self
            .local_map
            .runtime_state()
            .join_metadata_command_recovery(pg_id, command)
        {
            MetadataCommandRecoveryAdmission::Leader(guard) => MetadataCommandRecoveryTestGuard {
                _guard: Box::new(guard),
            },
            MetadataCommandRecoveryAdmission::Waited { .. } => {
                panic!("metadata command recovery admission unexpectedly waited")
            }
            MetadataCommandRecoveryAdmission::TimedOut { .. } => {
                panic!("metadata command recovery admission unexpectedly timed out")
            }
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn metadata_pg_primary_client(
        &self,
        pg_id: PgId,
    ) -> Result<&Arc<dyn StorageNodeClient>, StoreError> {
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        Ok(node.storage_client())
    }

    fn metadata_pg_primary_shard_ack_client(
        &self,
        pg_id: PgId,
    ) -> Result<&Arc<dyn ShardAckNodeClient>, StoreError> {
        self.metadata_pg_primary_shard_ack_client_at_epoch(self.operation_epoch(), pg_id)
    }

    fn metadata_pg_primary_shard_ack_client_at_epoch(
        &self,
        operation_epoch: ClusterEpoch,
        pg_id: PgId,
    ) -> Result<&Arc<dyn ShardAckNodeClient>, StoreError> {
        let node = self
            .local_map
            .metadata_pg_primary_node(operation_epoch, pg_id)?;
        Ok(node.shard_ack_client())
    }

    fn metadata_pg_primary_shard_ack_client_at_retained_epoch(
        &self,
        operation_epoch: ClusterEpoch,
        pg_id: PgId,
    ) -> Result<&Arc<dyn ShardAckNodeClient>, StoreError> {
        let node = self
            .local_map
            .metadata_pg_primary_node_at_retained_epoch(operation_epoch, pg_id)?;
        Ok(node.shard_ack_client())
    }

    pub fn record_routine_metadata_command_checkpoints(
        &self,
    ) -> Result<MetadataCommandCheckpointRecordSummary, StoreError> {
        self.record_routine_metadata_command_checkpoints_with_limit(
            METADATA_COMMAND_CHECKPOINT_RECORD_LIMIT,
        )
    }

    pub fn record_current_metadata_command_checkpoint_for_pg(
        &self,
        pg_id: PgId,
    ) -> Result<MetadataCommandCheckpointRecordSummary, StoreError> {
        let mut summary = MetadataCommandCheckpointRecordSummary::default();
        let route = self
            .local_pg_route(pg_id)
            .ok_or_else(|| StoreError::ClusterPgNotFound {
                pg_id: pg_id.get(),
                cluster_epoch: self.operation_epoch(),
            })?;
        summary.scanned += 1;
        if route.state() != PgState::Active {
            return Err(StoreError::PgNotActive {
                pg_id: pg_id.get(),
                cluster_epoch: route.cluster_epoch(),
                state: route.state(),
            });
        }
        let primary_node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let metadata_client = primary_node.metadata_command_client();
        let state = metadata_client.metadata_command_replica_state(pg_id)?;
        if state.cluster_epoch != route.cluster_epoch() {
            return Err(StoreError::StaleMetadataRoute {
                pg_id: pg_id.get(),
                route_epoch: state.cluster_epoch,
                current_epoch: route.cluster_epoch(),
            });
        }
        if state.applied_log_index == 0 && state.applied_log_hash == 0 {
            summary.skipped_empty += 1;
            return Ok(summary);
        }
        match metadata_client.record_current_metadata_command_checkpoint(pg_id, state.cluster_epoch)
        {
            Ok(_) => {
                summary.recorded += 1;
                compact_metadata_command_log_for_checkpoint_record(
                    metadata_client.as_ref(),
                    pg_id,
                    state.cluster_epoch,
                    &mut summary,
                );
            }
            Err(error) => {
                note_metadata_command_checkpoint_record_error(pg_id, "failed", &error);
                return Err(error);
            }
        }
        Ok(summary)
    }

    fn record_routine_metadata_command_checkpoints_with_limit(
        &self,
        limit: usize,
    ) -> Result<MetadataCommandCheckpointRecordSummary, StoreError> {
        let mut summary = MetadataCommandCheckpointRecordSummary::default();
        if limit == 0 {
            return Ok(summary);
        }

        for route in self.local_pg_routes() {
            if summary.mutations() >= limit {
                summary.limit_reached = true;
                break;
            }
            summary.scanned += 1;
            if route.state() != PgState::Active {
                summary.skipped_inactive += 1;
                continue;
            }
            let primary_node = self
                .local_map
                .metadata_pg_primary_node(self.operation_epoch(), route.pg_id())?;
            let metadata_client = primary_node.metadata_command_client();
            let state = match metadata_client.metadata_command_replica_state(route.pg_id()) {
                Ok(state) => state,
                Err(StoreError::MetadataCommandReplicaStateMissing { .. }) => {
                    summary.skipped_empty += 1;
                    continue;
                }
                Err(error) => {
                    if metadata_command_checkpoint_record_error_is_stale(&error) {
                        summary.skipped_stale_epoch += 1;
                        note_metadata_command_checkpoint_record_error(
                            route.pg_id(),
                            "skipped_stale",
                            &error,
                        );
                    } else {
                        note_metadata_command_checkpoint_record_error(
                            route.pg_id(),
                            "failed",
                            &error,
                        );
                        summary.failed += 1;
                    }
                    continue;
                }
            };
            if state.cluster_epoch != route.cluster_epoch() {
                summary.skipped_stale_epoch += 1;
                continue;
            }
            if state.applied_log_index == 0 && state.applied_log_hash == 0 {
                summary.skipped_empty += 1;
                continue;
            }
            let latest_checkpoint = match metadata_client.metadata_command_checkpoint_candidates(
                route.pg_id(),
                state.cluster_epoch,
                state.applied_log_index,
                1,
            ) {
                Ok(mut candidates) => candidates.pop(),
                Err(error) => {
                    if metadata_command_checkpoint_record_error_is_stale(&error) {
                        summary.skipped_stale_epoch += 1;
                        note_metadata_command_checkpoint_record_error(
                            route.pg_id(),
                            "skipped_stale",
                            &error,
                        );
                    } else {
                        note_metadata_command_checkpoint_record_error(
                            route.pg_id(),
                            "failed",
                            &error,
                        );
                        summary.failed += 1;
                    }
                    continue;
                }
            };
            match metadata_command_checkpoint_record_decision(
                &state,
                latest_checkpoint.as_ref(),
                METADATA_COMMAND_CHECKPOINT_MIN_LOG_DISTANCE,
                METADATA_COMMAND_CHECKPOINT_FRAME_RISK_BYTES,
            ) {
                Ok(MetadataCommandCheckpointRecordDecision::Record) => {}
                Ok(MetadataCommandCheckpointRecordDecision::AlreadyCurrent) => {
                    summary.already_current += 1;
                    compact_metadata_command_log_for_checkpoint_record(
                        metadata_client.as_ref(),
                        route.pg_id(),
                        state.cluster_epoch,
                        &mut summary,
                    );
                    continue;
                }
                Ok(MetadataCommandCheckpointRecordDecision::SkipCadence) => {
                    summary.skipped_cadence += 1;
                    compact_metadata_command_log_for_checkpoint_record(
                        metadata_client.as_ref(),
                        route.pg_id(),
                        state.cluster_epoch,
                        &mut summary,
                    );
                    continue;
                }
                Err(error) => {
                    note_metadata_command_checkpoint_record_error(route.pg_id(), "failed", &error);
                    summary.failed += 1;
                    continue;
                }
            }
            match metadata_client
                .record_current_metadata_command_checkpoint(route.pg_id(), state.cluster_epoch)
            {
                Ok(_) => {
                    summary.recorded += 1;
                    compact_metadata_command_log_for_checkpoint_record(
                        metadata_client.as_ref(),
                        route.pg_id(),
                        state.cluster_epoch,
                        &mut summary,
                    );
                }
                Err(error) => {
                    if metadata_command_checkpoint_record_error_is_stale(&error) {
                        summary.skipped_stale_epoch += 1;
                        note_metadata_command_checkpoint_record_error(
                            route.pg_id(),
                            "skipped_stale",
                            &error,
                        );
                    } else {
                        note_metadata_command_checkpoint_record_error(
                            route.pg_id(),
                            "failed",
                            &error,
                        );
                        summary.failed += 1;
                    }
                }
            }
        }

        Ok(summary)
    }

    fn metadata_pg_primary_object_listing_client(
        &self,
        pg_id: PgId,
    ) -> Result<&Arc<dyn ObjectListingMetadataNodeClient>, StoreError> {
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        Ok(node.object_listing_metadata_client())
    }

    fn bucket_metadata_pg_id(&self, bucket: &BucketName) -> u32 {
        self.local_map.bucket_pg_for(bucket)
    }

    pub fn object_payload_reclaim_pg_id(&self, bucket: &BucketName, key: &ObjectKey) -> u32 {
        self.object_metadata_pg_id(bucket, key)
    }

    fn object_metadata_pg_id(&self, bucket: &BucketName, key: &ObjectKey) -> u32 {
        self.local_map.object_pg_for(bucket, key)
    }

    fn metadata_command_bucket_write_reservation_proof(
        command: &MetadataCommandEnvelope,
    ) -> Option<&BucketWriteReservationProof> {
        match command.payload() {
            MetadataCommandPayload::CommitDirectPutObject(commit) => {
                Some(&commit.bucket_write_reservation)
            }
            MetadataCommandPayload::CommitMultipartObject(commit) => {
                Some(&commit.bucket_write_reservation)
            }
            MetadataCommandPayload::CreateStreamUpload(create) => {
                Some(&create.bucket_write_reservation)
            }
            MetadataCommandPayload::CommitStreamPart(commit) => {
                Some(&commit.bucket_write_reservation)
            }
            MetadataCommandPayload::PutObjectMetadata(update) => {
                Some(&update.bucket_write_reservation)
            }
            MetadataCommandPayload::DeleteObjectVersion(delete) => {
                Some(&delete.bucket_write_reservation)
            }
            MetadataCommandPayload::InsertDeleteMarker(marker) => {
                Some(&marker.bucket_write_reservation)
            }
            MetadataCommandPayload::CreateMultipartUpload(create) => {
                Some(&create.bucket_write_reservation)
            }
            MetadataCommandPayload::AbortMultipartUpload(abort) => {
                Some(&abort.bucket_write_reservation)
            }
            _ => None,
        }
    }

    pub(super) fn validate_metadata_command_bucket_write_reservation(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), BucketSnapshotLoadError> {
        let Some(proof) = Self::metadata_command_bucket_write_reservation_proof(command) else {
            return Ok(());
        };
        if proof.cluster_epoch != command.id().cluster_epoch() {
            return Err(MetadataError::BucketWriteReservationConflict {
                reservation_id: proof.reservation_id.clone(),
            }
            .into());
        }
        let pg_id = self.bucket_metadata_pg_id(&proof.bucket);
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), PgId::new(pg_id))?;
        node.bucket_write_reservation_client()
            .validate_bucket_write_reservation_proof(PgId::new(pg_id), proof)
    }

    fn release_metadata_command_bucket_write_reservation(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), BucketSnapshotLoadError> {
        let Some(proof) = Self::metadata_command_bucket_write_reservation_proof(command) else {
            return Ok(());
        };
        self.release_bucket_write_reservation_proof(proof)
    }

    fn release_applied_metadata_command_bucket_write_reservations(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), BucketSnapshotLoadError> {
        let preserve_command_reservation = matches!(
            command.payload(),
            MetadataCommandPayload::CreateStreamUpload(create)
                if create.session.target == StreamUploadTarget::PutObject
        );
        if !preserve_command_reservation {
            self.release_metadata_command_bucket_write_reservation(command)?;
        }
        match command.payload() {
            MetadataCommandPayload::CommitDirectPutObject(commit) => {
                if let Some(proof) = &commit.stream_create_bucket_write_reservation {
                    self.release_stream_create_bucket_write_reservation_proof(proof)?;
                }
            }
            MetadataCommandPayload::AbortStreamUpload(abort) => {
                if let Some(proof) = &abort.stream_create_bucket_write_reservation {
                    self.release_stream_create_bucket_write_reservation_proof(proof)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn release_stream_create_bucket_write_reservation_proof(
        &self,
        proof: &BucketWriteReservationProof,
    ) -> Result<(), BucketSnapshotLoadError> {
        match self.release_bucket_write_reservation_proof(proof) {
            Ok(()) => Ok(()),
            Err(BucketSnapshotLoadError::Metadata(
                MetadataError::BucketWriteReservationNotFound { .. },
            )) => Ok(()),
            Err(BucketSnapshotLoadError::Metadata(
                MetadataError::BucketWriteReservationConflict { .. },
            )) => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn release_bucket_write_reservation_proof(
        &self,
        proof: &BucketWriteReservationProof,
    ) -> Result<(), BucketSnapshotLoadError> {
        let pg_id = self.bucket_metadata_pg_id(&proof.bucket);
        let node = self
            .local_map
            .metadata_pg_primary_node_at_retained_epoch(proof.cluster_epoch, PgId::new(pg_id))?;
        node.bucket_write_reservation_client()
            .release_metadata_command_bucket_write_reservation(PgId::new(pg_id), proof)?;
        Ok(())
    }

    fn pending_metadata_command_uses_bucket_write_reservation(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        proof: &BucketWriteReservationProof,
    ) -> Result<bool, ObjectPgActionError> {
        Ok(self
            .pending_metadata_command_for_bucket(pg_id, bucket)?
            .as_ref()
            .and_then(Self::metadata_command_bucket_write_reservation_proof)
            == Some(proof))
    }

    fn next_bucket_write_reservation_id(&self) -> Result<String, StoreError> {
        self.next_bucket_write_coordination_id(
            "bucket-write-",
            "generate bucket write reservation id",
        )
    }

    fn next_bucket_write_drain_id(&self) -> Result<String, StoreError> {
        self.next_bucket_write_coordination_id("bucket-drain-", "generate bucket write drain id")
    }

    fn next_object_payload_reclaim_claim_id(&self) -> Result<String, StoreError> {
        self.next_bucket_write_coordination_id(
            "object-reclaim-",
            "generate object payload reclaim claim id",
        )
    }

    fn next_bucket_delete_finalize_claim_id(&self) -> Result<String, StoreError> {
        self.next_bucket_write_coordination_id(
            "bucket-finalize-",
            "generate bucket delete finalize claim id",
        )
    }

    fn next_lifecycle_sweep_claim_id(&self) -> Result<String, StoreError> {
        self.next_bucket_write_coordination_id(
            "lifecycle-sweep-",
            "generate lifecycle sweep claim id",
        )
    }

    fn next_bucket_write_coordination_id(
        &self,
        prefix: &'static str,
        context: &'static str,
    ) -> Result<String, StoreError> {
        const HEX: &[u8; 16] = b"0123456789abcdef";

        let rng = ring::rand::SystemRandom::new();
        let mut id_bytes = [0u8; 16];
        rng.fill(&mut id_bytes).map_err(|_| StoreError::Io {
            context,
            source: std::io::Error::other("failed to generate random reservation id"),
        })?;

        let mut encoded = String::with_capacity(prefix.len() + id_bytes.len() * 2);
        encoded.push_str(prefix);
        for byte in id_bytes {
            encoded.push(HEX[(byte >> 4) as usize] as char);
            encoded.push(HEX[(byte & 0x0f) as usize] as char);
        }
        Ok(encoded)
    }

    fn bucket_write_owner_token(&self) -> String {
        format!(
            "process:{}:cluster:{:p}",
            std::process::id(),
            Arc::as_ptr(&self.local_map)
        )
    }

    fn object_mutation_metadata_primary_client(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<&Arc<dyn crate::node_client::ObjectMutationMetadataNodeClient>, StoreError> {
        self.local_map
            .metadata_pg_primary_node(
                self.operation_epoch(),
                PgId::new(self.object_metadata_pg_id(bucket, key)),
            )
            .map(|node| node.object_mutation_metadata_client())
    }

    fn object_generation_metadata_primary_client(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<&Arc<dyn crate::node_client::ObjectGenerationMetadataNodeClient>, StoreError> {
        self.local_map
            .metadata_pg_primary_node(
                self.operation_epoch(),
                PgId::new(self.object_metadata_pg_id(bucket, key)),
            )
            .map(|node| node.object_generation_metadata_client())
    }

    fn direct_put_metadata_primary_client(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<&Arc<dyn crate::node_client::DirectPutMetadataNodeClient>, StoreError> {
        self.local_map
            .metadata_pg_primary_node(
                self.operation_epoch(),
                PgId::new(self.object_metadata_pg_id(bucket, key)),
            )
            .map(|node| node.direct_put_metadata_client())
    }

    pub fn default_payload_ec_shape(&self) -> EcShape {
        self.local_map.default_ec_shape()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_install_before_stream_abort_storage_hook(
        &self,
        hook: Arc<dyn Fn() + Send + Sync>,
    ) -> StreamAbortTestHookGuard {
        self.test_hooks.lock().unwrap().before_stream_abort_storage = Some(hook);
        StreamAbortTestHookGuard {
            hooks: Arc::clone(&self.test_hooks),
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_install_before_metadata_command_pending_install_hook(
        &self,
        hook: Arc<dyn Fn() + Send + Sync>,
    ) -> MetadataCommandPendingInstallHookGuard {
        self.test_hooks
            .lock()
            .unwrap()
            .before_metadata_command_pending_install = Some(hook);
        MetadataCommandPendingInstallHookGuard {
            hooks: Arc::clone(&self.test_hooks),
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_install_before_direct_put_command_id_hook(
        &self,
        hook: Arc<dyn Fn() + Send + Sync>,
    ) -> DirectPutCommandIdHookGuard {
        self.test_hooks.lock().unwrap().before_direct_put_command_id = Some(hook);
        DirectPutCommandIdHookGuard {
            hooks: Arc::clone(&self.test_hooks),
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_install_before_object_generation_command_id_hook(
        &self,
        hook: Arc<dyn Fn() + Send + Sync>,
    ) -> ObjectGenerationCommandIdHookGuard {
        self.test_hooks
            .lock()
            .unwrap()
            .before_object_generation_command_id = Some(hook);
        ObjectGenerationCommandIdHookGuard {
            hooks: Arc::clone(&self.test_hooks),
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_install_before_stream_append_command_id_hook(
        &self,
        hook: Arc<dyn Fn() + Send + Sync>,
    ) -> StreamAppendCommandIdHookGuard {
        self.test_hooks
            .lock()
            .unwrap()
            .before_stream_append_command_id = Some(hook);
        StreamAppendCommandIdHookGuard {
            hooks: Arc::clone(&self.test_hooks),
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_install_after_object_metadata_reservation_acquired_hook(
        &self,
        hook: Arc<dyn Fn() -> Result<(), ObjectPgActionError> + Send + Sync>,
    ) -> ObjectMetadataReservationAcquiredHookGuard {
        self.test_hooks
            .lock()
            .unwrap()
            .after_object_metadata_reservation_acquired = Some(hook);
        ObjectMetadataReservationAcquiredHookGuard {
            hooks: Arc::clone(&self.test_hooks),
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_install_before_placed_payload_shard_delete_hook(
        &self,
        hook: PayloadShardCleanupTestHook,
    ) -> PayloadCleanupTestHookGuard {
        self.test_hooks
            .lock()
            .unwrap()
            .before_placed_payload_shard_delete = Some(hook);
        PayloadCleanupTestHookGuard {
            hooks: Arc::clone(&self.test_hooks),
            kind: PayloadCleanupTestHookKind::PlacedShardDelete,
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_install_before_placed_payload_shard_read_hook(
        &self,
        hook: PayloadShardReadTestHook,
    ) -> PayloadShardReadTestHookGuard {
        self.test_hooks
            .lock()
            .unwrap()
            .before_placed_payload_shard_read = Some(hook);
        PayloadShardReadTestHookGuard {
            hooks: Arc::clone(&self.test_hooks),
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_install_before_metadata_primary_payload_ack_delete_hook(
        &self,
        hook: PayloadShardCleanupTestHook,
    ) -> PayloadCleanupTestHookGuard {
        self.test_hooks
            .lock()
            .unwrap()
            .before_metadata_primary_payload_ack_delete = Some(hook);
        PayloadCleanupTestHookGuard {
            hooks: Arc::clone(&self.test_hooks),
            kind: PayloadCleanupTestHookKind::MetadataPrimaryAckDelete,
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_install_best_effort_payload_cleanup_error_hook(
        &self,
        hook: PayloadCleanupErrorTestHook,
    ) -> PayloadCleanupTestHookGuard {
        self.test_hooks
            .lock()
            .unwrap()
            .best_effort_payload_cleanup_error = Some(hook);
        PayloadCleanupTestHookGuard {
            hooks: Arc::clone(&self.test_hooks),
            kind: PayloadCleanupTestHookKind::BestEffortError,
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_install_after_direct_put_metadata_publish_hook(
        &self,
        hook: Arc<dyn Fn() -> Result<(), ObjectPgActionError> + Send + Sync>,
    ) -> crate::node::DirectPutMetadataPublishTestHookGuard {
        self.metadata_primary_test_hook_node()
            .test_install_after_direct_put_metadata_publish_hook(hook)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_install_after_object_metadata_command_publish_hook(
        &self,
        hook: Arc<dyn Fn() -> Result<(), ObjectPgActionError> + Send + Sync>,
    ) -> crate::node::ObjectMetadataCommandPublishTestHookGuard {
        self.metadata_primary_test_hook_node()
            .test_install_after_object_metadata_command_publish_hook(hook)
    }

    pub fn place_payload_shards(
        &self,
        data_pg_id: DataPgId,
        ec_shape: EcShape,
        stable_placement_key: &[u8],
    ) -> Result<Vec<ShardLocation>, ClusterBuildError> {
        self.local_map.place_payload_shards(
            self.operation_epoch(),
            data_pg_id,
            ec_shape,
            stable_placement_key,
        )
    }

    pub fn place_payload_shards_for_pg_route(
        &self,
        cluster_epoch: ClusterEpoch,
        data_pg_id: DataPgId,
        ec_shape: EcShape,
        stable_placement_key: &[u8],
        acting_set: &[NodeId],
    ) -> Result<Vec<ShardLocation>, ClusterBuildError> {
        LocalClusterMap::place_payload_shards_for_pg_route(
            cluster_epoch,
            data_pg_id,
            ec_shape,
            stable_placement_key,
            acting_set,
        )
    }

    pub fn place_payload_shards_for_pg_route_snapshot(
        &self,
        route: &PgRouteSnapshot,
        data_pg_id: DataPgId,
        ec_shape: EcShape,
        stable_placement_key: &[u8],
    ) -> Result<Vec<ShardLocation>, ClusterBuildError> {
        if route.pg_id() != data_pg_id.pg_id() {
            return Err(ClusterBuildError::InvalidLocalPlacement {
                reason: format!(
                    "route PG {} does not match data PG {}",
                    route.pg_id().get(),
                    data_pg_id.pg_id().get()
                ),
            });
        }
        self.place_payload_shards_for_pg_route(
            route.cluster_epoch(),
            data_pg_id,
            ec_shape,
            stable_placement_key,
            route.acting_set(),
        )
    }

    pub fn payload_shard_node(
        &self,
        data_pg_id: DataPgId,
        shard_index: ShardIndex,
        ec_shape: EcShape,
        stable_placement_key: &[u8],
    ) -> Result<NodeId, ClusterBuildError> {
        self.local_map.payload_shard_node(
            self.operation_epoch(),
            data_pg_id,
            shard_index,
            ec_shape,
            stable_placement_key,
        )
    }

    pub fn write_payload_shard(
        &self,
        location: ShardLocation,
        key: &ShardKey,
        data: &[u8],
    ) -> Result<WriteAck, ShardIoError> {
        self.local_map
            .write_payload_shard(self.operation_epoch(), location, key, data)
    }

    pub(crate) fn repair_payload_shard(
        &self,
        location: ShardLocation,
        key: &ShardKey,
        data: &[u8],
    ) -> Result<WriteAck, ShardIoError> {
        self.local_map
            .repair_payload_shard(self.operation_epoch(), location, key, data)
    }

    pub(crate) fn read_payload_shard(
        &self,
        location: ShardLocation,
        key: &ShardKey,
        expected: WriteAck,
    ) -> Result<Vec<u8>, ShardIoError> {
        self.local_map
            .read_payload_shard(self.operation_epoch(), location, key, expected)
    }

    fn read_payload_shard_for_historical_inspection(
        &self,
        location: ShardLocation,
        key: &ShardKey,
        expected: WriteAck,
    ) -> Result<Vec<u8>, ShardIoError> {
        self.local_map
            .read_payload_shard_for_historical_inspection(location, key, expected)
    }

    #[cfg(test)]
    pub(crate) fn read_payload_shard_into(
        &self,
        location: ShardLocation,
        key: &ShardKey,
        expected: WriteAck,
        dst: &mut [u8],
    ) -> Result<(), ShardIoError> {
        self.local_map
            .read_payload_shard_into(self.operation_epoch(), location, key, expected, dst)
    }

    pub(crate) fn delete_payload_shard(
        &self,
        location: ShardLocation,
        key: &ShardKey,
    ) -> Result<(), ShardIoError> {
        self.local_map
            .delete_payload_shard(self.operation_epoch(), location, key)
    }

    pub fn write_direct_put_segment_payload_shards(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        segment_index: u32,
        segment_okh: &[u8; 16],
        data: &[u8],
    ) -> Result<DirectPutWrittenSegment, StoreError> {
        let ec = self.default_payload_ec_shape();
        let data_pg_id = self
            .local_map
            .object_generation_segment_data_pg(bucket, key, generation_id, segment_index)
            .get();
        let data_pg = DataPgId::new(PgId::new(data_pg_id));
        let segment_vid = generation_id;
        let written_shards =
            self.write_placed_segment_payload_shards(data_pg, ec, segment_okh, segment_vid, data)?;

        Ok(DirectPutWrittenSegment {
            data_pg_id,
            ec,
            written_shards,
        })
    }

    fn write_placed_segment_payload_shards(
        &self,
        data_pg: DataPgId,
        ec: EcShape,
        segment_okh: &[u8; 16],
        segment_vid: GenerationId,
        data: &[u8],
    ) -> Result<Vec<WrittenShardAck>, StoreError> {
        let placement_key = segment_payload_placement_key(segment_okh, segment_vid);
        let locations = self
            .place_payload_shards(data_pg, ec, &placement_key)
            .map_err(cluster_build_error_to_store)?;
        self.local_map.write_erasure_coded_segment_shards_with(
            segment_okh,
            segment_vid,
            data,
            ec,
            |shard_batch| {
                let mut written_acks = Vec::with_capacity(shard_batch.len());
                let mut written_for_cleanup = Vec::with_capacity(shard_batch.len());
                for (location, (shard_key, shard_payload)) in
                    locations.iter().zip(shard_batch.iter())
                {
                    match self.write_payload_shard(*location, shard_key, shard_payload) {
                        Ok(ack) => {
                            written_acks.push((shard_key.clone(), ack));
                            written_for_cleanup.push(WrittenShardAck {
                                key: shard_key.clone(),
                                ack,
                            });
                        }
                        Err(error) => {
                            self.delete_payload_shard_keys_best_effort(
                                data_pg.get(),
                                ec,
                                segment_okh,
                                segment_vid,
                                written_for_cleanup
                                    .iter()
                                    .map(|written| written.key.clone()),
                            );
                            return Err(shard_io_error_to_store(error));
                        }
                    }
                }
                Ok(written_acks)
            },
        )
    }

    pub fn reserve_put_object_generation(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
    ) -> Result<GenerationId, ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        let mut empty_log_conflicts = 0;
        let mut work_budget =
            RequestWorkBudget::new(OBJECT_GENERATION_RESERVATION_RETRY_BUDGET, None)
                .for_operation("reserve_object_generation")
                .for_pg(pg_id);
        loop {
            work_budget
                .check("object generation reservation retry budget exhausted")
                .map_err(ObjectPgActionError::Store)?;
            if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                match command.payload() {
                    MetadataCommandPayload::ReserveObjectGeneration(reservation)
                        if reservation.matches_request(bucket, key, reservation_id) =>
                    {
                        let generation_id = reservation.generation_id;
                        let exact =
                            ExactPendingObjectMetadataCommand::for_checked_request(&command);
                        match self.finish_exact_pending_object_metadata_command(pg_id, exact)? {
                            PendingMetadataCommandOutcome::Applied => return Ok(generation_id),
                            PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                                return Err(conflicting_pending_object_metadata_command(
                                    "retryable partial pending generation reservation command",
                                ));
                            }
                            PendingMetadataCommandOutcome::Abandoned => {
                                work_budget
                                    .sleep_after_contention(
                                        "object generation reservation abandoned pending retry budget exhausted",
                                    )
                                    .map_err(ObjectPgActionError::Store)?;
                                continue;
                            }
                        }
                    }
                    _ => {}
                }
                self.drain_pending_object_metadata_command(pg_id, &command)?;
                work_budget
                    .sleep_after_contention(
                        "object generation reservation pending drain retry budget exhausted",
                    )
                    .map_err(ObjectPgActionError::Store)?;
                continue;
            }

            match self
                .object_generation_metadata_primary_client(bucket, key)?
                .object_generation_reservation(pg_id, bucket, key, reservation_id)
            {
                Ok(generation_id) => return Ok(generation_id),
                Err(ObjectPgActionError::Metadata(
                    MetadataError::ObjectGenerationReservationNotFound { .. },
                )) => {}
                Err(error) => return Err(error),
            }
            let generation_id = self
                .object_generation_metadata_primary_client(bucket, key)?
                .next_object_generation_id(pg_id, bucket, key)?;
            self.maybe_run_before_object_generation_command_id_hook();
            if self
                .object_generation_metadata_primary_client(bucket, key)?
                .next_object_generation_id(pg_id, bucket, key)?
                != generation_id
            {
                work_budget
                    .sleep_after_contention(
                        "object generation reservation stale generation retry budget exhausted",
                    )
                    .map_err(ObjectPgActionError::Store)?;
                continue;
            }
            self.maybe_run_before_metadata_command_pending_install_hook();
            let command = match self.try_install_object_pg_pending_command_with_fresh_id(
                pg_id,
                bucket,
                false,
                |command_id| {
                    MetadataCommandEnvelope::new(
                        command_id,
                        MetadataCommandPayload::ReserveObjectGeneration(
                            ReserveObjectGenerationCommand::new(
                                bucket.clone(),
                                key.clone(),
                                reservation_id.clone(),
                                generation_id,
                                crate::clock::current_time_millis(),
                            ),
                        ),
                    )
                },
            )? {
                ObjectPgPendingCommandInstall::Installed(command) => command,
                ObjectPgPendingCommandInstall::Pending(command) => {
                    self.drain_pending_object_metadata_command(pg_id, &command)?;
                    work_budget
                        .sleep_after_contention(
                            "object generation reservation pending install retry budget exhausted",
                        )
                        .map_err(ObjectPgActionError::Store)?;
                    continue;
                }
                ObjectPgPendingCommandInstall::LogConflict { pending_visible } => {
                    self.drain_after_object_pg_log_conflict(
                        pg_id,
                        bucket,
                        pending_visible,
                        &mut empty_log_conflicts,
                        "object generation reservation log conflict without pending progress",
                    )?;
                    work_budget
                        .sleep_after_contention(
                            "object generation reservation log conflict retry budget exhausted",
                        )
                        .map_err(ObjectPgActionError::Store)?;
                    continue;
                }
            };
            let mut command = command;
            loop {
                match self.apply_metadata_command_to_acting_set(&command) {
                    Ok(()) => {
                        self.remove_pending_metadata_command_for_bucket(pg_id, bucket, &command)
                            .map_err(ObjectPgActionError::from)?;
                        match command.payload() {
                            MetadataCommandPayload::ReserveObjectGeneration(reservation) => {
                                return Ok(reservation.generation_id);
                            }
                            _ => {
                                unreachable!(
                                    "reserve object generation pending command kind changed"
                                )
                            }
                        }
                    }
                    Err(error)
                        if matches!(
                            self.retryable_partial_exact_metadata_command_conflict_applied_on_all_nodes(
                                pg_id,
                                &command,
                                error.applied_nodes,
                                &error.source,
                            )
                            .map_err(bucket_snapshot_error_to_object_pg_action_error)?,
                            Some(true)
                        ) =>
                    {
                        self.remove_pending_metadata_command_for_bucket(pg_id, bucket, &command)
                            .map_err(ObjectPgActionError::from)?;
                        match command.payload() {
                            MetadataCommandPayload::ReserveObjectGeneration(reservation) => {
                                return Ok(reservation.generation_id);
                            }
                            _ => {
                                unreachable!(
                                    "reserve object generation pending command kind changed"
                                )
                            }
                        }
                    }
                    Err(error)
                        if matches!(
                            self.retryable_partial_exact_metadata_command_conflict_applied_on_all_nodes(
                                pg_id,
                                &command,
                                error.applied_nodes,
                                &error.source,
                            )
                            .map_err(bucket_snapshot_error_to_object_pg_action_error)?,
                            Some(false)
                        ) =>
                    {
                        return Err(conflicting_pending_object_metadata_command(
                            "retryable partial reserve object generation command conflict",
                        ));
                    }
                    Err(error)
                        if error.applied_nodes == 0
                            && Self::reserve_object_generation_conflict_matches(
                                &command,
                                &error.source,
                            ) =>
                    {
                        if let Err(abandon_error) =
                            self.record_abandoned_metadata_command_to_acting_set(&command)
                        {
                            if !Self::metadata_command_log_conflict_matches(
                                &command,
                                &abandon_error.source,
                            ) {
                                return Err(bucket_snapshot_error_to_object_pg_action_error(
                                    abandon_error.source,
                                ));
                            }
                        }
                        self.remove_pending_metadata_command_for_bucket(pg_id, bucket, &command)
                            .map_err(ObjectPgActionError::from)?;
                        work_budget
                            .sleep_after_contention(
                                "object generation reservation conflict cleanup retry budget exhausted",
                            )
                            .map_err(ObjectPgActionError::Store)?;
                        continue;
                    }
                    Err(error)
                        if error.applied_nodes == 0
                            && Self::metadata_command_log_conflict_matches(
                                &command,
                                &error.source,
                            ) =>
                    {
                        let reissued = match self.reissue_pending_metadata_command(pg_id, &command)
                        {
                            Ok(Some(reissued)) => reissued,
                            Ok(None) => break,
                            Err(BucketSnapshotLoadError::Store(
                                StoreError::MetadataCommandLogConflict { .. },
                            )) => {
                                return Err(conflicting_pending_object_metadata_command(
                                    "retryable generation reservation reissue conflict",
                                ));
                            }
                            Err(error) => {
                                return Err(bucket_snapshot_error_to_object_pg_action_error(error));
                            }
                        };
                        work_budget
                            .sleep_after_contention(
                                "object generation reservation reissue retry budget exhausted",
                            )
                            .map_err(ObjectPgActionError::Store)?;
                        command = reissued;
                    }
                    Err(error) => {
                        if error.applied_nodes == 0 {
                            self.record_abandoned_metadata_command_to_acting_set(&command)
                                .map_err(|error| {
                                    bucket_snapshot_error_to_object_pg_action_error(error.source)
                                })?;
                            self.remove_pending_metadata_command_for_bucket(
                                pg_id, bucket, &command,
                            )
                            .map_err(ObjectPgActionError::from)?;
                        }
                        return Err(bucket_snapshot_error_to_object_pg_action_error(
                            error.source,
                        ));
                    }
                }
            }
        }
    }

    fn reserve_next_object_version(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<VersionId, ObjectPgActionError> {
        self.reserve_next_object_version_with_completion_admission(pg_id, bucket, key, false)
    }

    fn reserve_next_object_version_for_completion(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<VersionId, ObjectPgActionError> {
        self.reserve_next_object_version_with_completion_admission(pg_id, bucket, key, true)
    }

    fn reserve_next_object_version_with_completion_admission(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        completion_admission: bool,
    ) -> Result<VersionId, ObjectPgActionError> {
        let mut empty_log_conflicts = 0;
        let mut work_budget = RequestWorkBudget::new(
            OBJECT_VERSION_RESERVATION_RETRY_BUDGET,
            Some(OBJECT_VERSION_RESERVATION_RETRY_ATTEMPTS),
        )
        .for_operation("reserve_object_version")
        .for_pg(pg_id);
        loop {
            work_budget
                .check("object version reservation retry budget exhausted")
                .map_err(ObjectPgActionError::Store)?;
            if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                if let MetadataCommandPayload::ReserveObjectVersion(reservation) = command.payload()
                {
                    let reserved_version_id = reservation.version_id;
                    let matches_request = reservation.matches_request(bucket, key);
                    let outcome = if matches_request {
                        let exact =
                            ExactPendingObjectMetadataCommand::for_checked_request(&command);
                        match self.finish_exact_pending_object_metadata_command(pg_id, exact) {
                            Ok(outcome) => outcome,
                            Err(ObjectPgActionError::Metadata(
                                MetadataError::ObjectVersionReservationConflict { version_id },
                            )) if version_id == reserved_version_id => {
                                self.record_abandoned_metadata_command_to_acting_set(&command)
                                    .map_err(|error| {
                                        bucket_snapshot_error_to_object_pg_action_error(
                                            error.source,
                                        )
                                    })?;
                                let pending =
                                    self.pending_metadata_command_for_bucket(pg_id, bucket)?;
                                if pending.as_ref() != Some(&command) {
                                    return Err(conflicting_pending_object_metadata_command(
                                        "pending version reservation changed before stale cleanup",
                                    ));
                                }
                                self.remove_pending_metadata_command_for_bucket(
                                    pg_id, bucket, &command,
                                )
                                .map_err(ObjectPgActionError::from)?;
                                work_budget
                                    .sleep_after_contention(
                                        "object version reservation stale cleanup retry budget exhausted",
                                    )
                                    .map_err(ObjectPgActionError::Store)?;
                                continue;
                            }
                            Err(error) => return Err(error),
                        }
                    } else {
                        self.drain_pending_object_metadata_command(pg_id, &command)?;
                        work_budget
                            .sleep_after_contention(
                                "object version reservation pending drain retry budget exhausted",
                            )
                            .map_err(ObjectPgActionError::Store)?;
                        continue;
                    };
                    match outcome {
                        PendingMetadataCommandOutcome::Applied if matches_request => {
                            return Ok(reserved_version_id);
                        }
                        PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                            return Err(conflicting_pending_object_metadata_command(
                                "retryable partial pending version reservation command",
                            ));
                        }
                        PendingMetadataCommandOutcome::Applied
                        | PendingMetadataCommandOutcome::Abandoned => {
                            work_budget
                                .sleep_after_contention(
                                    "object version reservation pending completion retry budget exhausted",
                                )
                                .map_err(ObjectPgActionError::Store)?;
                            continue;
                        }
                    }
                }
                self.drain_pending_object_metadata_command(pg_id, &command)?;
                work_budget
                    .sleep_after_contention(
                        "object version reservation unrelated pending retry budget exhausted",
                    )
                    .map_err(ObjectPgActionError::Store)?;
                continue;
            }

            let version_id = self.max_next_object_version_id_on_acting_set(
                pg_id,
                bucket,
                key,
                completion_admission,
            )?;
            let command = match self.try_install_object_pg_pending_command_with_fresh_id(
                pg_id,
                bucket,
                completion_admission,
                |command_id| {
                    MetadataCommandEnvelope::new(
                        command_id,
                        MetadataCommandPayload::ReserveObjectVersion(
                            ReserveObjectVersionCommand::new(
                                bucket.clone(),
                                key.clone(),
                                version_id,
                            ),
                        ),
                    )
                },
            )? {
                ObjectPgPendingCommandInstall::Installed(command) => command,
                ObjectPgPendingCommandInstall::Pending(command) => {
                    self.drain_pending_object_metadata_command(pg_id, &command)?;
                    work_budget
                        .sleep_after_contention(
                            "object version reservation pending install retry budget exhausted",
                        )
                        .map_err(ObjectPgActionError::Store)?;
                    continue;
                }
                ObjectPgPendingCommandInstall::LogConflict { pending_visible } => {
                    self.drain_after_object_pg_log_conflict(
                        pg_id,
                        bucket,
                        pending_visible,
                        &mut empty_log_conflicts,
                        "object version reservation log conflict without pending progress",
                    )?;
                    work_budget
                        .sleep_after_contention(
                            "object version reservation log conflict retry budget exhausted",
                        )
                        .map_err(ObjectPgActionError::Store)?;
                    continue;
                }
            };
            match self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command) {
                Ok(()) => {}
                Err(ObjectPgActionError::Metadata(
                    MetadataError::ObjectVersionReservationConflict {
                        version_id: stale_version,
                    },
                )) if stale_version == version_id => {
                    work_budget
                        .sleep_after_contention(
                            "object version reservation stale version retry budget exhausted",
                        )
                        .map_err(ObjectPgActionError::Store)?;
                    continue;
                }
                Err(error) => return Err(error),
            }
            return Ok(version_id);
        }
    }

    fn max_next_object_version_id_on_acting_set(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        completion_admission: bool,
    ) -> Result<VersionId, ObjectPgActionError> {
        let mut version_id = VersionId::from_u64(1);
        for node in self
            .local_map
            .metadata_pg_acting_nodes(self.operation_epoch(), pg_id)?
        {
            let object_version_client = node.object_version_metadata_client();
            let candidate = if completion_admission {
                object_version_client.next_completion_object_version_id(pg_id, bucket, key)?
            } else {
                object_version_client.next_object_version_id(pg_id, bucket, key)?
            };
            if candidate.to_u64() > version_id.to_u64() {
                version_id = candidate;
            }
        }
        Ok(version_id)
    }

    fn finish_exact_pending_object_metadata_command(
        &self,
        pg_id: PgId,
        command: ExactPendingObjectMetadataCommand<'_>,
    ) -> Result<PendingMetadataCommandOutcome, ObjectPgActionError> {
        self.finish_object_pg_pending_slot(pg_id, command.command)
    }

    fn apply_exact_pending_object_metadata_command(
        &self,
        pg_id: PgId,
        command: ExactPendingObjectMetadataCommand<'_>,
    ) -> Result<(), ObjectPgActionError> {
        match self.finish_exact_pending_object_metadata_command(pg_id, command)? {
            PendingMetadataCommandOutcome::Applied => Ok(()),
            PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                Err(conflicting_pending_object_metadata_command(
                    "retryable partial pending object metadata command",
                ))
            }
            PendingMetadataCommandOutcome::Abandoned => {
                Err(conflicting_pending_object_metadata_command(
                    "abandoned pending object metadata command",
                ))
            }
        }
    }

    fn drain_pending_object_metadata_command(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), ObjectPgActionError> {
        match self.drain_pending_metadata_command_with_recovery_gate(pg_id, command)? {
            PendingMetadataCommandOutcome::Applied | PendingMetadataCommandOutcome::Abandoned => {
                Ok(())
            }
            PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                Err(conflicting_pending_object_metadata_command(
                    "retryable partial pending object metadata drain",
                ))
            }
        }
    }

    fn drain_pending_metadata_command_with_recovery_gate(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<PendingMetadataCommandOutcome, ObjectPgActionError> {
        self.drain_pending_metadata_command_with_recovery_gate_inner(pg_id, command, None)
    }

    fn drain_pending_metadata_command_with_recovery_gate_and_work_budget(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        work_budget: &mut RequestWorkBudget,
    ) -> Result<PendingMetadataCommandOutcome, ObjectPgActionError> {
        self.drain_pending_metadata_command_with_recovery_gate_inner(
            pg_id,
            command,
            Some(work_budget),
        )
    }

    fn drain_pending_metadata_command_with_recovery_gate_inner(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        mut work_budget: Option<&mut RequestWorkBudget>,
    ) -> Result<PendingMetadataCommandOutcome, ObjectPgActionError> {
        loop {
            if let Some(work_budget) = work_budget.as_deref_mut() {
                work_budget.check("pending command recovery gate budget exhausted")?;
            }
            let recovery = self
                .local_map
                .runtime_state()
                .join_metadata_command_recovery(pg_id, command);
            let _recovery_guard = match recovery {
                MetadataCommandRecoveryAdmission::Leader(guard) => {
                    self.emit_metadata_command_recovery_admission_for_command(
                        pg_id,
                        command,
                        observability::MetadataCommandRecoveryAdmissionKind::Leader,
                        0,
                    );
                    guard
                }
                MetadataCommandRecoveryAdmission::Waited { wait_us } => {
                    self.emit_metadata_command_recovery_admission_for_command(
                        pg_id,
                        command,
                        observability::MetadataCommandRecoveryAdmissionKind::Waited,
                        wait_us,
                    );
                    self.emit_pending_slot_action_for_command(pg_id, command, "drain_wait");
                    let waiter_outcome =
                        self.pending_command_recovery_waiter_outcome(pg_id, command)?;
                    self.emit_metadata_command_recovery_outcome_for_command(
                        pg_id,
                        command,
                        waiter_outcome.metric_label(),
                    );
                    match waiter_outcome {
                        MetadataCommandRecoveryWaiterOutcome::StillPending => continue,
                        MetadataCommandRecoveryWaiterOutcome::Applied => {
                            return Ok(PendingMetadataCommandOutcome::Applied);
                        }
                        MetadataCommandRecoveryWaiterOutcome::MissingNotApplied
                        | MetadataCommandRecoveryWaiterOutcome::ReplacedNotApplied => {
                            return Ok(PendingMetadataCommandOutcome::Abandoned);
                        }
                    }
                }
                MetadataCommandRecoveryAdmission::TimedOut { wait_us } => {
                    self.emit_metadata_command_recovery_admission_for_command(
                        pg_id,
                        command,
                        observability::MetadataCommandRecoveryAdmissionKind::TimedOut,
                        wait_us,
                    );
                    self.emit_metadata_command_recovery_outcome_for_command(
                        pg_id,
                        command,
                        "timed_out",
                    );
                    self.emit_pending_slot_action_for_command(pg_id, command, "drain_timeout");
                    return Err(conflicting_pending_object_metadata_command(
                        "pending command recovery timed out",
                    ));
                }
            };
            let outcome = match work_budget.as_deref_mut() {
                Some(work_budget) => self
                    .finish_pending_metadata_command_recovery_with_work_budget(
                        pg_id,
                        command,
                        work_budget,
                    )?,
                None => self.finish_pending_metadata_command_recovery(pg_id, command)?,
            };
            self.emit_metadata_command_recovery_outcome_for_command(
                pg_id,
                command,
                outcome.metric_label(),
            );
            return Ok(outcome);
        }
    }

    fn pending_command_recovery_waiter_outcome(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandRecoveryWaiterOutcome, ObjectPgActionError> {
        let pending = self.pending_metadata_command_for_bucket(pg_id, command.bucket_name())?;
        if pending.as_ref() == Some(command) {
            return Ok(MetadataCommandRecoveryWaiterOutcome::StillPending);
        }
        if self
            .metadata_command_is_applied_on_all_acting_nodes(pg_id, command)
            .map_err(bucket_snapshot_error_to_object_pg_action_error)?
        {
            return Ok(MetadataCommandRecoveryWaiterOutcome::Applied);
        }
        if pending.is_some() {
            Ok(MetadataCommandRecoveryWaiterOutcome::ReplacedNotApplied)
        } else {
            Ok(MetadataCommandRecoveryWaiterOutcome::MissingNotApplied)
        }
    }

    fn finish_pending_metadata_command_recovery(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<PendingMetadataCommandOutcome, ObjectPgActionError> {
        self.finish_pending_metadata_command_recovery_inner(pg_id, command, None)
    }

    fn finish_pending_metadata_command_recovery_with_work_budget(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        work_budget: &mut RequestWorkBudget,
    ) -> Result<PendingMetadataCommandOutcome, ObjectPgActionError> {
        self.finish_pending_metadata_command_recovery_inner(pg_id, command, Some(work_budget))
    }

    fn finish_pending_metadata_command_recovery_inner(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        work_budget: Option<&mut RequestWorkBudget>,
    ) -> Result<PendingMetadataCommandOutcome, ObjectPgActionError> {
        self.emit_pending_slot_action_for_command(pg_id, command, "drain_attempt");
        if Self::metadata_command_is_bucket_pg_command(command) {
            let outcome = match work_budget {
                Some(work_budget) => self
                    .finish_pending_metadata_command_to_acting_set_for_recovery_with_work_budget(
                        pg_id,
                        command,
                        false,
                        work_budget,
                    )
                    .map_err(bucket_snapshot_error_to_object_pg_action_error)?,
                None => {
                    let mut work_budget =
                        RequestWorkBudget::new(BUCKET_WRITE_DRAIN_RETRY_BUDGET, None)
                            .for_operation("metadata_command_apply_partial_retry")
                            .for_pg(pg_id);
                    self.finish_pending_metadata_command_to_acting_set_for_recovery_with_work_budget(
                        pg_id,
                        command,
                        false,
                        &mut work_budget,
                    )
                    .map_err(bucket_snapshot_error_to_object_pg_action_error)?
                }
            };
            return Ok(match outcome {
                request_ops::FinishPendingMetadataCommandResult::Applied => {
                    PendingMetadataCommandOutcome::Applied
                }
                request_ops::FinishPendingMetadataCommandResult::Abandoned => {
                    PendingMetadataCommandOutcome::Abandoned
                }
                request_ops::FinishPendingMetadataCommandResult::RetryPartialExactConflict => {
                    PendingMetadataCommandOutcome::RetryPartialExactConflict
                }
            });
        }
        self.finish_object_pg_pending_slot_inner(pg_id, command, true, work_budget)
    }

    fn metadata_command_recovery_applied_collectable_object_command(
        command: &MetadataCommandEnvelope,
        outcome: PendingMetadataCommandOutcome,
    ) -> bool {
        matches!(outcome, PendingMetadataCommandOutcome::Applied)
            && !Self::metadata_command_is_bucket_pg_command(command)
    }

    fn finish_object_pg_pending_slot(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<PendingMetadataCommandOutcome, ObjectPgActionError> {
        self.finish_object_pg_pending_slot_inner(pg_id, command, false, None)
    }

    fn finish_object_pg_pending_slot_inner(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        abandon_zero_apply_stale_reservation: bool,
        mut work_budget: Option<&mut RequestWorkBudget>,
    ) -> Result<PendingMetadataCommandOutcome, ObjectPgActionError> {
        let mut command = command.clone();
        loop {
            if let Some(work_budget) = work_budget.as_deref_mut() {
                work_budget.check("object metadata pending command apply budget exhausted")?;
            }
            let command_bucket = command.bucket_name();
            if self
                .metadata_command_has_abandoned_log_on_acting_set(&command)
                .map_err(|error| bucket_snapshot_error_to_object_pg_action_error(error.source))?
            {
                self.record_abandoned_metadata_command_to_acting_set(&command)
                    .map_err(|error| {
                        bucket_snapshot_error_to_object_pg_action_error(error.source)
                    })?;
                self.release_metadata_command_bucket_write_reservation(&command)
                    .map_err(bucket_snapshot_error_to_object_pg_action_error)?;
                self.remove_pending_metadata_command_for_bucket(pg_id, command_bucket, &command)
                    .map_err(ObjectPgActionError::from)?;
                self.after_object_metadata_command_abandoned(&command)?;
                return Ok(PendingMetadataCommandOutcome::Abandoned);
            }
            match self.apply_metadata_command_to_acting_set(&command) {
                Ok(()) => {
                    self.release_applied_metadata_command_bucket_write_reservations(&command)
                        .map_err(bucket_snapshot_error_to_object_pg_action_error)?;
                    self.remove_pending_metadata_command_for_bucket(
                        pg_id,
                        command_bucket,
                        &command,
                    )
                    .map_err(ObjectPgActionError::from)?;
                    self.after_object_metadata_command_applied(&command);
                    return Ok(PendingMetadataCommandOutcome::Applied);
                }
                Err(error)
                    if Self::metadata_command_log_conflict_matches(&command, &error.source)
                        && self
                            .partial_exact_metadata_command_conflict_is_retryable(
                                pg_id,
                                &command,
                                error.applied_nodes,
                                &error.source,
                            )
                            .map_err(bucket_snapshot_error_to_object_pg_action_error)? =>
                {
                    if self
                        .metadata_command_is_applied_on_all_acting_nodes(pg_id, &command)
                        .map_err(bucket_snapshot_error_to_object_pg_action_error)?
                    {
                        self.release_applied_metadata_command_bucket_write_reservations(&command)
                            .map_err(bucket_snapshot_error_to_object_pg_action_error)?;
                        self.remove_pending_metadata_command_for_bucket(
                            pg_id,
                            command_bucket,
                            &command,
                        )
                        .map_err(ObjectPgActionError::from)?;
                        self.after_object_metadata_command_applied(&command);
                        return Ok(PendingMetadataCommandOutcome::Applied);
                    }
                    return Ok(PendingMetadataCommandOutcome::RetryPartialExactConflict);
                }
                Err(error)
                    if error.applied_nodes == 0
                        && Self::metadata_command_log_conflict_matches(&command, &error.source) =>
                {
                    let reissued = match self.reissue_pending_metadata_command(pg_id, &command) {
                        Ok(Some(reissued)) => reissued,
                        Ok(None) => return Ok(PendingMetadataCommandOutcome::Abandoned),
                        Err(BucketSnapshotLoadError::Store(
                            StoreError::MetadataCommandLogConflict { .. },
                        )) => {
                            return Ok(PendingMetadataCommandOutcome::RetryPartialExactConflict);
                        }
                        Err(error) => {
                            return Err(bucket_snapshot_error_to_object_pg_action_error(error));
                        }
                    };
                    command = reissued;
                }
                Err(error)
                    if abandon_zero_apply_stale_reservation
                        && error.applied_nodes == 0
                        && (Self::reserve_object_generation_conflict_matches(
                            &command,
                            &error.source,
                        ) || Self::reserve_object_version_conflict_matches(
                            &command,
                            &error.source,
                        )) =>
                {
                    self.record_abandoned_metadata_command_to_acting_set(&command)
                        .map_err(|error| {
                            bucket_snapshot_error_to_object_pg_action_error(error.source)
                        })?;
                    self.release_metadata_command_bucket_write_reservation(&command)
                        .map_err(bucket_snapshot_error_to_object_pg_action_error)?;
                    self.remove_pending_metadata_command_for_bucket(
                        pg_id,
                        command_bucket,
                        &command,
                    )
                    .map_err(ObjectPgActionError::from)?;
                    self.after_object_metadata_command_abandoned(&command)?;
                    return Ok(PendingMetadataCommandOutcome::Abandoned);
                }
                Err(error) => {
                    return Err(bucket_snapshot_error_to_object_pg_action_error(
                        error.source,
                    ));
                }
            }
        }
    }

    fn after_object_metadata_command_abandoned(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), ObjectPgActionError> {
        match command.payload() {
            MetadataCommandPayload::CommitDirectPutObject(commit) => {
                let release_result = self.release_object_generation_reservation_command_required(
                    command.id().pg_id(),
                    &commit.object.bucket,
                    &commit.object.key,
                    &commit.generation_reservation_id,
                );
                for segment in &commit.segments {
                    self.delete_object_segment_payload_shards_best_effort(segment);
                }
                release_result?;
            }
            MetadataCommandPayload::AppendStreamSegment(append) => {
                self.delete_stream_segment_payload_shards_best_effort(&append.segment);
            }
            MetadataCommandPayload::CreateStreamUpload(create)
                if create.session.target == StreamUploadTarget::PutObject =>
            {
                self.release_object_generation_reservation_command_required(
                    command.id().pg_id(),
                    &create.session.bucket,
                    &create.session.key,
                    &create.session.session_id,
                )?;
            }
            _ => {}
        }
        Ok(())
    }

    fn release_object_generation_reservation_command_required(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
    ) -> Result<(), ObjectPgActionError> {
        let mut empty_log_conflicts = 0;
        loop {
            if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                match command.payload() {
                    MetadataCommandPayload::ReleaseObjectGeneration(reservation)
                        if reservation.matches_request(bucket, key, reservation_id) =>
                    {
                        let exact =
                            ExactPendingObjectMetadataCommand::for_checked_request(&command);
                        match self.finish_exact_pending_object_metadata_command(pg_id, exact)? {
                            PendingMetadataCommandOutcome::Applied => return Ok(()),
                            PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                                return Err(conflicting_pending_object_metadata_command(
                                    "retryable partial pending generation release command",
                                ));
                            }
                            PendingMetadataCommandOutcome::Abandoned => continue,
                        }
                    }
                    _ => {
                        self.drain_pending_object_metadata_command(pg_id, &command)?;
                        continue;
                    }
                }
            }

            let command = match self.try_install_object_pg_pending_command_with_fresh_id(
                pg_id,
                bucket,
                false,
                |command_id| {
                    MetadataCommandEnvelope::new(
                        command_id,
                        MetadataCommandPayload::ReleaseObjectGeneration(
                            ReleaseObjectGenerationCommand::new(
                                bucket.clone(),
                                key.clone(),
                                reservation_id.clone(),
                            ),
                        ),
                    )
                },
            )? {
                ObjectPgPendingCommandInstall::Installed(command) => command,
                ObjectPgPendingCommandInstall::Pending(command) => {
                    self.drain_pending_object_metadata_command(pg_id, &command)?;
                    continue;
                }
                ObjectPgPendingCommandInstall::LogConflict { pending_visible } => {
                    self.drain_after_object_pg_log_conflict(
                        pg_id,
                        bucket,
                        pending_visible,
                        &mut empty_log_conflicts,
                        "object generation release log conflict without pending progress",
                    )?;
                    continue;
                }
            };
            let mut command = command;
            loop {
                match self.apply_metadata_command_to_acting_set(&command) {
                    Ok(()) => {
                        self.remove_pending_metadata_command_for_bucket(pg_id, bucket, &command)
                            .map_err(ObjectPgActionError::from)?;
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
                            .map_err(bucket_snapshot_error_to_object_pg_action_error)?,
                            Some(true)
                        ) =>
                    {
                        self.remove_pending_metadata_command_for_bucket(pg_id, bucket, &command)
                            .map_err(ObjectPgActionError::from)?;
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
                            .map_err(bucket_snapshot_error_to_object_pg_action_error)?,
                            Some(false)
                        ) =>
                    {
                        return Err(conflicting_pending_object_metadata_command(
                            "retryable partial release object generation command conflict",
                        ));
                    }
                    Err(error)
                        if error.applied_nodes == 0
                            && Self::metadata_command_log_conflict_matches(
                                &command,
                                &error.source,
                            ) =>
                    {
                        let Some(reissued) = self
                            .reissue_pending_metadata_command(pg_id, &command)
                            .map_err(bucket_snapshot_error_to_object_pg_action_error)?
                        else {
                            break;
                        };
                        command = reissued;
                    }
                    Err(error) => {
                        if error.applied_nodes == 0 {
                            self.record_abandoned_metadata_command_to_acting_set(&command)
                                .map_err(|error| {
                                    bucket_snapshot_error_to_object_pg_action_error(error.source)
                                })?;
                            self.remove_pending_metadata_command_for_bucket(
                                pg_id, bucket, &command,
                            )
                            .map_err(ObjectPgActionError::from)?;
                            break;
                        }
                        return Err(bucket_snapshot_error_to_object_pg_action_error(
                            error.source,
                        ));
                    }
                }
            }
        }
    }

    fn drain_pending_object_metadata_commands_for_bucket(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<(), ObjectPgActionError> {
        self.drain_pending_object_metadata_commands_for_bucket_collect(pg_id, bucket)
            .map(|_| ())
    }

    fn drain_one_pending_object_metadata_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<bool, ObjectPgActionError> {
        let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? else {
            return Ok(false);
        };
        match self.drain_pending_metadata_command_with_recovery_gate(pg_id, &command)? {
            PendingMetadataCommandOutcome::Applied | PendingMetadataCommandOutcome::Abandoned => {}
            PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                return Err(conflicting_pending_object_metadata_command(
                    "retryable partial pending object metadata drain",
                ));
            }
        }
        Ok(true)
    }

    fn drain_pending_object_metadata_commands_for_bucket_collect(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<Vec<MetadataCommandEnvelope>, ObjectPgActionError> {
        let mut applied = Vec::new();
        while let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
            let outcome =
                self.drain_pending_metadata_command_with_recovery_gate(pg_id, &command)?;
            if matches!(
                outcome,
                PendingMetadataCommandOutcome::RetryPartialExactConflict
            ) {
                return Err(conflicting_pending_object_metadata_command(
                    "retryable partial pending object metadata drain",
                ));
            }
            if Self::metadata_command_recovery_applied_collectable_object_command(&command, outcome)
            {
                applied.push(command);
            }
        }
        Ok(applied)
    }

    fn drain_pending_object_metadata_commands_for_exact_bucket(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<(), ObjectPgActionError> {
        self.drain_pending_object_metadata_commands_for_exact_bucket_inner(pg_id, bucket, None)
    }

    fn drain_pending_object_metadata_commands_for_exact_bucket_with_work_budget(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        work_budget: &mut RequestWorkBudget,
    ) -> Result<(), ObjectPgActionError> {
        self.drain_pending_object_metadata_commands_for_exact_bucket_inner(
            pg_id,
            bucket,
            Some(work_budget),
        )
    }

    fn emit_exact_bucket_object_drain_step(
        bucket: &BucketName,
        pg_id: PgId,
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
            TRACE_TARGET,
            "bucket_delete_exact_object_drain_step",
            format!(
                "bucket={:?} object_pg_id={} step={}{}",
                bucket,
                pg_id.get(),
                step,
                suffix
            ),
        );
    }

    fn drain_pending_object_metadata_commands_for_exact_bucket_inner(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        mut work_budget: Option<&mut RequestWorkBudget>,
    ) -> Result<(), ObjectPgActionError> {
        let mut drain_iteration = 0u64;
        loop {
            drain_iteration += 1;
            Self::emit_exact_bucket_object_drain_step(
                bucket,
                pg_id,
                "pending_lookup_start",
                format!("iteration={drain_iteration}"),
            );
            let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? else {
                Self::emit_exact_bucket_object_drain_step(
                    bucket,
                    pg_id,
                    "pending_lookup_done",
                    format!("iteration={drain_iteration} has_pending=false"),
                );
                break;
            };
            Self::emit_exact_bucket_object_drain_step(
                bucket,
                pg_id,
                "pending_lookup_done",
                format!(
                    "iteration={} has_pending=true command_kind={} command_bucket={:?}",
                    drain_iteration,
                    command.payload().kind_name(),
                    command.bucket_name()
                ),
            );
            if let Some(work_budget) = work_budget.as_deref_mut() {
                work_budget.check("exact bucket object command drain budget exhausted")?;
            }
            if command.bucket_name() != bucket {
                Self::emit_exact_bucket_object_drain_step(
                    bucket,
                    pg_id,
                    "stop_foreign_bucket",
                    format!(
                        "iteration={} command_kind={} command_bucket={:?}",
                        drain_iteration,
                        command.payload().kind_name(),
                        command.bucket_name()
                    ),
                );
                return Ok(());
            }
            Self::emit_exact_bucket_object_drain_step(
                bucket,
                pg_id,
                "apply_start",
                format!(
                    "iteration={} command_kind={}",
                    drain_iteration,
                    command.payload().kind_name()
                ),
            );
            let outcome = match work_budget.as_deref_mut() {
                Some(work_budget) => self
                    .drain_pending_metadata_command_with_recovery_gate_and_work_budget(
                        pg_id,
                        &command,
                        work_budget,
                    )?,
                None => self.drain_pending_metadata_command_with_recovery_gate(pg_id, &command)?,
            };
            Self::emit_exact_bucket_object_drain_step(
                bucket,
                pg_id,
                "apply_done",
                format!("iteration={} outcome={outcome:?}", drain_iteration),
            );
            match outcome {
                PendingMetadataCommandOutcome::Applied
                | PendingMetadataCommandOutcome::Abandoned => {}
                PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                    return Err(conflicting_pending_object_metadata_command(
                        "retryable partial pending object metadata drain",
                    ));
                }
            }
        }
        Ok(())
    }

    fn pending_command_completes_stream_session(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<bool, ObjectPgActionError> {
        Ok(self
            .pending_metadata_command_for_bucket(pg_id, bucket)?
            .is_some_and(|command| {
                pending_command_completes_stream_session(&command, bucket, key, session_id)
            }))
    }

    fn next_object_metadata_command_id(
        &self,
        pg_id: PgId,
    ) -> Result<MetadataCommandId, ObjectPgActionError> {
        self.next_object_metadata_command_id_with_completion_admission(pg_id, false)
    }

    fn next_object_metadata_command_id_with_completion_admission(
        &self,
        pg_id: PgId,
        completion_admission: bool,
    ) -> Result<MetadataCommandId, ObjectPgActionError> {
        if completion_admission {
            return self
                .next_completion_metadata_command_id(pg_id)
                .map_err(ObjectPgActionError::from);
        }
        self.next_metadata_command_id(pg_id)
            .map_err(ObjectPgActionError::from)
    }

    fn next_object_metadata_command_id_or_drain(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<Option<MetadataCommandId>, ObjectPgActionError> {
        self.next_object_metadata_command_id_or_drain_with_completion_admission(
            pg_id, bucket, false,
        )
    }

    fn next_object_metadata_command_id_or_drain_with_completion_admission(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        completion_admission: bool,
    ) -> Result<Option<MetadataCommandId>, ObjectPgActionError> {
        match self
            .next_object_metadata_command_id_with_completion_admission(pg_id, completion_admission)
        {
            Ok(command_id) => Ok(Some(command_id)),
            Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict { .. })) => {
                self.drain_one_pending_object_metadata_command(pg_id, bucket)?;
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    #[cfg(test)]
    fn next_object_metadata_command_id_from_locked_pg(
        &self,
        pg_id: PgId,
        pg: &crate::PgStore,
    ) -> Result<MetadataCommandId, ObjectPgActionError> {
        self.next_metadata_command_id_from_locked_pg(pg_id, pg)
            .map_err(ObjectPgActionError::from)
    }

    fn next_bucket_metadata_command_id(
        &self,
        pg_id: PgId,
    ) -> Result<MetadataCommandId, BucketSnapshotLoadError> {
        self.next_metadata_command_id(pg_id)
            .map_err(BucketSnapshotLoadError::from)
    }

    fn next_completion_bucket_metadata_command_id(
        &self,
        pg_id: PgId,
    ) -> Result<MetadataCommandId, BucketSnapshotLoadError> {
        self.next_completion_metadata_command_id(pg_id)
            .map_err(BucketSnapshotLoadError::from)
    }

    fn apply_new_stream_append_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        segment_record: &StreamUploadSegmentRecord,
        shard_batch: &[(&ShardKey, WriteAck)],
    ) -> Result<StreamAppendCommandApplyOutcome, ObjectPgActionError> {
        let mut command = command.clone();
        loop {
            match self.apply_metadata_command_to_acting_set(&command) {
                Ok(()) => {
                    self.remove_pending_metadata_command_for_bucket(
                        pg_id,
                        command.bucket_name(),
                        &command,
                    )
                    .map_err(ObjectPgActionError::from)?;
                    return Ok(StreamAppendCommandApplyOutcome::Applied);
                }
                Err(error)
                    if matches!(
                        self.retryable_partial_exact_metadata_command_conflict_applied_on_all_nodes(
                            pg_id,
                            &command,
                            error.applied_nodes,
                            &error.source,
                        )
                        .map_err(bucket_snapshot_error_to_object_pg_action_error)?,
                        Some(true)
                    ) =>
                {
                    self.remove_pending_metadata_command_for_bucket(
                        pg_id,
                        command.bucket_name(),
                        &command,
                    )
                    .map_err(ObjectPgActionError::from)?;
                    return Ok(StreamAppendCommandApplyOutcome::Applied);
                }
                Err(error)
                    if matches!(
                        self.retryable_partial_exact_metadata_command_conflict_applied_on_all_nodes(
                            pg_id,
                            &command,
                            error.applied_nodes,
                            &error.source,
                        )
                        .map_err(bucket_snapshot_error_to_object_pg_action_error)?,
                        Some(false)
                    ) =>
                {
                    return Err(conflicting_pending_object_metadata_command(
                        "retryable partial stream append command conflict",
                    ));
                }
                Err(error)
                    if error.applied_nodes == 0
                        && Self::metadata_command_log_conflict_matches(&command, &error.source) =>
                {
                    let Some(reissued) = self
                        .reissue_pending_metadata_command(pg_id, &command)
                        .map_err(bucket_snapshot_error_to_object_pg_action_error)?
                    else {
                        return Ok(StreamAppendCommandApplyOutcome::RetryFromFreshSnapshot);
                    };
                    command = reissued;
                }
                Err(error) => {
                    if error.applied_nodes == 0 {
                        self.record_abandoned_metadata_command_to_acting_set(&command)
                            .map_err(|error| {
                                bucket_snapshot_error_to_object_pg_action_error(error.source)
                            })?;
                        self.remove_pending_metadata_command_for_bucket(pg_id, bucket, &command)
                            .map_err(ObjectPgActionError::from)?;
                        self.delete_stream_segment_payload_shard_keys_best_effort(
                            segment_record,
                            shard_batch.iter().map(|(key, _)| (*key).clone()),
                        );
                    }
                    return Err(bucket_snapshot_error_to_object_pg_action_error(
                        error.source,
                    ));
                }
            }
        }
    }

    fn after_object_metadata_command_applied(&self, command: &MetadataCommandEnvelope) {
        match command.payload() {
            MetadataCommandPayload::CommitDirectPutObject(commit) => {
                if let Some(stale_generation_id) =
                    object_payload_reclaim_generation(&commit.stale_payload)
                {
                    self.enqueue_object_payload_reclaim(
                        &commit.object.bucket,
                        &commit.object.key,
                        stale_generation_id,
                    );
                }
            }
            MetadataCommandPayload::CommitMultipartObject(commit) => {
                if let Some(stale_generation_id) =
                    object_payload_reclaim_generation(&commit.stale_payload)
                {
                    self.enqueue_object_payload_reclaim(
                        &commit.object.bucket,
                        &commit.object.key,
                        stale_generation_id,
                    );
                }
                self.delete_complete_multipart_cleanup_best_effort(
                    &commit.object.bucket,
                    &commit.object.key,
                    commit.object.generation_id,
                    &Self::complete_multipart_command_cleanup(commit),
                );
            }
            MetadataCommandPayload::DeleteObjectVersion(delete) => {
                if let Some(reclaim_generation_id) =
                    delete_object_version_reclaim_generation(&delete.target)
                {
                    self.enqueue_object_payload_reclaim(
                        &delete.bucket,
                        &delete.key,
                        reclaim_generation_id,
                    );
                }
            }
            MetadataCommandPayload::InsertDeleteMarker(marker) => {
                if let Some(stale_generation_id) =
                    object_payload_reclaim_generation(&marker.stale_payload)
                {
                    self.enqueue_object_payload_reclaim(
                        &marker.bucket,
                        &marker.key,
                        stale_generation_id,
                    );
                }
            }
            MetadataCommandPayload::CreateStreamUpload(_) => {}
            MetadataCommandPayload::AbortStreamUpload(abort) => {
                self.delete_staged_stream_segment_payload_shards_best_effort(
                    &abort.staged_segments,
                );
            }
            MetadataCommandPayload::CommitStreamPart(commit) => {
                self.delete_finalize_upload_part_cleanup_best_effort(
                    &crate::FinalizeStreamPartCleanup {
                        upload: commit.upload.clone(),
                        existing_part: commit.existing_part.clone(),
                        displaced_segments: commit.displaced_segments.clone(),
                    },
                );
            }
            MetadataCommandPayload::AbortMultipartUpload(abort) => {
                self.delete_abort_multipart_cleanup_best_effort(&abort.cleanup);
            }
            MetadataCommandPayload::DeleteObjectPayloadReclaim(delete) => {
                self.enqueue_bucket_delete_finalize(&delete.bucket);
            }
            _ => {}
        }
    }

    fn release_object_generation_reservation_after_pending_drain_best_effort(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
    ) {
        let _ = self
            .drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)
            .and_then(|_| self.release_object_generation_reservation(bucket, key, reservation_id));
    }

    pub fn release_object_generation_reservation(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
    ) -> Result<(), ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        let mut work_budget =
            RequestWorkBudget::new(OBJECT_GENERATION_RESERVATION_RETRY_BUDGET, None)
                .for_operation("release_object_generation")
                .for_pg(pg_id);
        loop {
            work_budget
                .check("object generation release retry budget exhausted")
                .map_err(ObjectPgActionError::Store)?;
            if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                match command.payload() {
                    MetadataCommandPayload::ReleaseObjectGeneration(reservation)
                        if reservation.matches_request(bucket, key, reservation_id) =>
                    {
                        let exact =
                            ExactPendingObjectMetadataCommand::for_checked_request(&command);
                        match self.finish_exact_pending_object_metadata_command(pg_id, exact)? {
                            PendingMetadataCommandOutcome::Applied => return Ok(()),
                            PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                                return Err(conflicting_pending_object_metadata_command(
                                    "retryable partial pending generation release command",
                                ));
                            }
                            PendingMetadataCommandOutcome::Abandoned => {
                                work_budget
                                    .sleep_after_contention(
                                        "object generation release abandoned pending retry budget exhausted",
                                    )
                                    .map_err(ObjectPgActionError::Store)?;
                                continue;
                            }
                        }
                    }
                    _ => {
                        self.drain_pending_object_metadata_command(pg_id, &command)?;
                        work_budget
                            .sleep_after_contention(
                                "object generation release pending drain retry budget exhausted",
                            )
                            .map_err(ObjectPgActionError::Store)?;
                        continue;
                    }
                }
            }
            let Some(command_id) = self.next_object_metadata_command_id_or_drain(pg_id, bucket)?
            else {
                work_budget
                    .sleep_after_contention(
                        "object generation release command id retry budget exhausted",
                    )
                    .map_err(ObjectPgActionError::Store)?;
                continue;
            };
            let command = MetadataCommandEnvelope::new(
                command_id,
                MetadataCommandPayload::ReleaseObjectGeneration(
                    ReleaseObjectGenerationCommand::new(
                        bucket.clone(),
                        key.clone(),
                        reservation_id.clone(),
                    ),
                ),
            );
            if !self.try_set_object_pg_pending_command_or_drain(pg_id, bucket, &command)? {
                work_budget
                    .sleep_after_contention(
                        "object generation release pending install retry budget exhausted",
                    )
                    .map_err(ObjectPgActionError::Store)?;
                continue;
            }
            let mut command = command;
            loop {
                match self.apply_metadata_command_to_acting_set(&command) {
                    Ok(()) => {
                        self.remove_pending_metadata_command_for_bucket(pg_id, bucket, &command)
                            .map_err(ObjectPgActionError::from)?;
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
                            .map_err(bucket_snapshot_error_to_object_pg_action_error)?,
                            Some(true)
                        ) =>
                    {
                        self.remove_pending_metadata_command_for_bucket(pg_id, bucket, &command)
                            .map_err(ObjectPgActionError::from)?;
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
                            .map_err(bucket_snapshot_error_to_object_pg_action_error)?,
                            Some(false)
                        ) =>
                    {
                        return Err(conflicting_pending_object_metadata_command(
                            "retryable partial release object generation command conflict",
                        ));
                    }
                    Err(error)
                        if error.applied_nodes == 0
                            && Self::metadata_command_log_conflict_matches(
                                &command,
                                &error.source,
                            ) =>
                    {
                        let Some(reissued) = self
                            .reissue_pending_metadata_command(pg_id, &command)
                            .map_err(bucket_snapshot_error_to_object_pg_action_error)?
                        else {
                            break;
                        };
                        work_budget
                            .sleep_after_contention(
                                "object generation release reissue retry budget exhausted",
                            )
                            .map_err(ObjectPgActionError::Store)?;
                        command = reissued;
                    }
                    Err(error) => {
                        if error.applied_nodes == 0 {
                            self.record_abandoned_metadata_command_to_acting_set(&command)
                                .map_err(|error| {
                                    bucket_snapshot_error_to_object_pg_action_error(error.source)
                                })?;
                            self.remove_pending_metadata_command_for_bucket(
                                pg_id, bucket, &command,
                            )
                            .map_err(ObjectPgActionError::from)?;
                            work_budget
                                .sleep_after_contention(
                                    "object generation release abandoned command retry budget exhausted",
                                )
                                .map_err(ObjectPgActionError::Store)?;
                            break;
                        }
                        return Err(bucket_snapshot_error_to_object_pg_action_error(
                            error.source,
                        ));
                    }
                }
            }
        }
    }

    pub fn commit_direct_put_object_from_payload_shards<E>(
        &self,
        req: &CommitDirectPutObjectReq,
        written_shards: &[WrittenShardAck],
        mut action: impl FnMut(DirectPutCommitSnapshot) -> Result<(), E>,
    ) -> Result<Result<FinalizeDirectPutObjectOutcome, E>, ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(&req.bucket, &req.key));
        let effective_bucket_write_reservation = req.bucket_write_reservation.clone();
        let mut bucket_write_proof_command_owned = false;
        macro_rules! release_caller_bucket_write_proof_if_unowned {
            () => {{
                if !bucket_write_proof_command_owned {
                    self.release_bucket_write_reservation_proof(&effective_bucket_write_reservation)
                        .map_err(bucket_snapshot_error_to_object_pg_action_error)
                } else {
                    Ok(())
                }
            }};
        }
        macro_rules! cleanup_direct_put_attempt_before_command_ownership {
            () => {{
                let release_result = release_caller_bucket_write_proof_if_unowned!();
                self.release_object_generation_reservation_after_pending_drain_best_effort(
                    pg_id,
                    &req.bucket,
                    &req.key,
                    &req.generation_reservation_id,
                );
                self.delete_direct_put_segment_payload_shards_at_epoch(
                    effective_bucket_write_reservation.cluster_epoch,
                    req.data_pg_id,
                    req.ec,
                    &req.segment_okh,
                    req.segment_vid,
                    written_shards,
                );
                release_result?;
            }};
        }
        let mut work_budget = RequestWorkBudget::new(DIRECT_PUT_METADATA_RETRY_BUDGET, None)
            .for_operation("commit_direct_put_metadata")
            .for_pg(pg_id);
        macro_rules! check_direct_put_work_before_command_ownership {
            ($context:literal) => {{
                if let Err(error) = work_budget.check($context) {
                    cleanup_direct_put_attempt_before_command_ownership!();
                    return Err(ObjectPgActionError::Store(error));
                }
            }};
        }
        macro_rules! sleep_direct_put_before_command_ownership_after_contention {
            ($context:literal) => {{
                if let Err(error) = work_budget.sleep_after_contention($context) {
                    cleanup_direct_put_attempt_before_command_ownership!();
                    return Err(ObjectPgActionError::Store(error));
                }
            }};
        }
        let shard_batch: Vec<(&ShardKey, WriteAck)> = written_shards
            .iter()
            .map(|written| (&written.key, written.ack))
            .collect();
        let direct_put_metadata_client =
            match self.direct_put_metadata_primary_client(&req.bucket, &req.key) {
                Ok(client) => client,
                Err(error) => {
                    cleanup_direct_put_attempt_before_command_ownership!();
                    return Err(error.into());
                }
            };

        let mut stale_commit_snapshot_retries = 0;
        let stale_commit_snapshot_deadline = Instant::now() + DIRECT_PUT_STALE_COMMIT_RETRY_BUDGET;
        let mut empty_log_conflicts = 0;
        let (command, new_pending_command) = loop {
            check_direct_put_work_before_command_ownership!(
                "direct PUT metadata retry budget exhausted"
            );
            let (command, new_pending_command, payload_acks_registered) = loop {
                check_direct_put_work_before_command_ownership!(
                    "direct PUT metadata pending retry budget exhausted"
                );
                let Some(command) =
                    (match self.pending_metadata_command_for_bucket(pg_id, &req.bucket) {
                        Ok(command) => command,
                        Err(error) => {
                            cleanup_direct_put_attempt_before_command_ownership!();
                            return Err(error.into());
                        }
                    })
                else {
                    let snapshot = match direct_put_metadata_client.load_direct_put_commit_snapshot(
                        pg_id,
                        &req.bucket,
                        &req.key,
                        &req.generation_reservation_id,
                        req.generation_id,
                    ) {
                        Ok(snapshot) => snapshot,
                        Err(error) => {
                            cleanup_direct_put_attempt_before_command_ownership!();
                            return Err(error);
                        }
                    };
                    if let Some(outcome) = Self::committed_direct_put_retry_outcome(req, &snapshot)?
                    {
                        return Ok(Ok(outcome));
                    }
                    match action(snapshot.auth_snapshot.clone()) {
                        Ok(()) => {}
                        Err(error) => {
                            cleanup_direct_put_attempt_before_command_ownership!();
                            return Ok(Err(error));
                        }
                    }

                    let version_id = if req.versioning == crate::BucketVersioningState::Enabled {
                        match self.reserve_next_object_version_for_completion(
                            pg_id,
                            &req.bucket,
                            &req.key,
                        ) {
                            Ok(version_id) => version_id,
                            Err(error) => {
                                cleanup_direct_put_attempt_before_command_ownership!();
                                return Err(error);
                            }
                        }
                    } else {
                        VersionId::Null
                    };
                    self.maybe_run_before_direct_put_command_id_hook();
                    if let Err(error) =
                        self.register_payload_shard_acks(req.data_pg_id, &shard_batch)
                    {
                        cleanup_direct_put_attempt_before_command_ownership!();
                        return Err(error);
                    }
                    let command = match direct_put_metadata_client.build_direct_put_commit_command(
                        BuildDirectPutCommitCommandReq {
                            pg_id,
                            cluster_epoch: self.operation_epoch(),
                            request: req,
                            version_id,
                            expected_snapshot: &snapshot,
                            bucket_write_reservation: &effective_bucket_write_reservation,
                        },
                    ) {
                        Ok(command) => command,
                        Err(ObjectPgActionError::StaleDirectPutCommitSnapshot)
                            if stale_commit_snapshot_retries < DIRECT_PUT_STALE_COMMIT_RETRIES
                                && Instant::now() < stale_commit_snapshot_deadline =>
                        {
                            stale_commit_snapshot_retries += 1;
                            sleep_direct_put_before_command_ownership_after_contention!(
                                "direct PUT stale snapshot retry budget exhausted"
                            );
                            continue;
                        }
                        Err(ObjectPgActionError::StaleDirectPutCommitSnapshot) => {
                            cleanup_direct_put_attempt_before_command_ownership!();
                            return Err(conflicting_pending_object_metadata_command(
                                "direct PUT stale commit snapshot retry budget exhausted",
                            ));
                        }
                        Err(ObjectPgActionError::Store(
                            StoreError::MetadataCommandLogConflict { .. },
                        )) => {
                            let pending_visible = match self
                                .pending_metadata_command_for_bucket(pg_id, &req.bucket)
                            {
                                Ok(pending) => pending.is_some(),
                                Err(error) => {
                                    cleanup_direct_put_attempt_before_command_ownership!();
                                    return Err(error.into());
                                }
                            };
                            let drain_result = self.drain_after_object_pg_log_conflict(
                                pg_id,
                                &req.bucket,
                                pending_visible,
                                &mut empty_log_conflicts,
                                "direct put commit command log conflict without pending progress",
                            );
                            if let Err(error) = drain_result {
                                cleanup_direct_put_attempt_before_command_ownership!();
                                return Err(error);
                            }
                            sleep_direct_put_before_command_ownership_after_contention!(
                                "direct PUT command log conflict retry budget exhausted"
                            );
                            continue;
                        }
                        Err(error) => {
                            cleanup_direct_put_attempt_before_command_ownership!();
                            return Err(error);
                        }
                    };
                    break (command, true, true);
                };

                let is_matching_direct_put = matches!(
                    command.payload(),
                    MetadataCommandPayload::CommitDirectPutObject(commit)
                        if commit.matches_request(
                            &req.bucket,
                            &req.key,
                            &req.generation_reservation_id,
                            req.generation_id,
                        )
                        && commit.bucket_write_reservation == effective_bucket_write_reservation
                );
                if is_matching_direct_put {
                    bucket_write_proof_command_owned = true;
                }
                let has_abandoned_log = match self
                    .metadata_command_has_abandoned_log_on_acting_set(&command)
                {
                    Ok(has_abandoned_log) => has_abandoned_log,
                    Err(error) => {
                        let error = bucket_snapshot_error_to_object_pg_action_error(error.source);
                        cleanup_direct_put_attempt_before_command_ownership!();
                        return Err(error);
                    }
                };
                if has_abandoned_log {
                    if is_matching_direct_put {
                        let exact =
                            ExactPendingObjectMetadataCommand::for_checked_request(&command);
                        let error =
                            match self.finish_exact_pending_object_metadata_command(pg_id, exact) {
                                Ok(PendingMetadataCommandOutcome::Applied) => {
                                    unreachable!(
                                        "already-classified abandoned metadata command was applied"
                                    )
                                }
                                Ok(PendingMetadataCommandOutcome::Abandoned) => {
                                    conflicting_pending_object_metadata_command(
                                        "abandoned pending command for direct put commit",
                                    )
                                }
                                Ok(PendingMetadataCommandOutcome::RetryPartialExactConflict) => {
                                    conflicting_pending_object_metadata_command(
                                        "retryable partial pending command for direct put commit",
                                    )
                                }
                                Err(error) => error,
                            };
                        self.delete_direct_put_segment_payload_shards(
                            req.data_pg_id,
                            req.ec,
                            &req.segment_okh,
                            req.segment_vid,
                            written_shards,
                        );
                        return Err(error);
                    }
                    if let Err(error) = self.drain_pending_object_metadata_command(pg_id, &command)
                    {
                        cleanup_direct_put_attempt_before_command_ownership!();
                        return Err(error);
                    }
                    sleep_direct_put_before_command_ownership_after_contention!(
                        "direct PUT abandoned pending drain retry budget exhausted"
                    );
                    continue;
                }
                if is_matching_direct_put {
                    break (command, false, false);
                }
                if let Err(error) = self.drain_pending_object_metadata_command(pg_id, &command) {
                    cleanup_direct_put_attempt_before_command_ownership!();
                    return Err(error);
                }
                sleep_direct_put_before_command_ownership_after_contention!(
                    "direct PUT unrelated pending drain retry budget exhausted"
                );
            };

            if !payload_acks_registered {
                if let Err(error) = self.register_payload_shard_acks(req.data_pg_id, &shard_batch) {
                    if new_pending_command {
                        let release_result =
                            self.release_metadata_command_bucket_write_reservation(&command);
                        self.release_object_generation_reservation_after_pending_drain_best_effort(
                            pg_id,
                            &req.bucket,
                            &req.key,
                            &req.generation_reservation_id,
                        );
                        self.delete_direct_put_segment_payload_shards(
                            req.data_pg_id,
                            req.ec,
                            &req.segment_okh,
                            req.segment_vid,
                            written_shards,
                        );
                        release_result.map_err(bucket_snapshot_error_to_object_pg_action_error)?;
                    }
                    return Err(error);
                }
            }
            if let Err(error) = self.validate_payload_shard_acks(
                req.data_pg_id,
                req.ec,
                &req.segment_okh,
                req.segment_vid,
                &shard_batch,
            ) {
                if new_pending_command {
                    let release_result =
                        self.release_metadata_command_bucket_write_reservation(&command);
                    self.release_object_generation_reservation_after_pending_drain_best_effort(
                        pg_id,
                        &req.bucket,
                        &req.key,
                        &req.generation_reservation_id,
                    );
                    self.delete_direct_put_segment_payload_shards(
                        req.data_pg_id,
                        req.ec,
                        &req.segment_okh,
                        req.segment_vid,
                        written_shards,
                    );
                    release_result.map_err(bucket_snapshot_error_to_object_pg_action_error)?;
                }
                return Err(error);
            }
            if new_pending_command {
                let installed = match self.try_install_object_pg_pending_command_or_drain(
                    pg_id,
                    &req.bucket,
                    &command,
                ) {
                    Ok(installed) => installed,
                    Err(error) => {
                        let release_result =
                            self.release_metadata_command_bucket_write_reservation(&command);
                        self.release_object_generation_reservation_after_pending_drain_best_effort(
                            pg_id,
                            &req.bucket,
                            &req.key,
                            &req.generation_reservation_id,
                        );
                        self.delete_direct_put_segment_payload_shards(
                            req.data_pg_id,
                            req.ec,
                            &req.segment_okh,
                            req.segment_vid,
                            written_shards,
                        );
                        release_result.map_err(bucket_snapshot_error_to_object_pg_action_error)?;
                        return Err(error);
                    }
                };
                if !installed {
                    sleep_direct_put_before_command_ownership_after_contention!(
                        "direct PUT pending install retry budget exhausted"
                    );
                    continue;
                }
            }
            break (command, new_pending_command);
        };

        let command = loop {
            let recovery = self
                .local_map
                .runtime_state()
                .join_metadata_command_recovery(pg_id, &command);
            let _recovery_guard = match recovery {
                MetadataCommandRecoveryAdmission::Leader(guard) => {
                    self.emit_metadata_command_recovery_admission_for_command(
                        pg_id,
                        &command,
                        observability::MetadataCommandRecoveryAdmissionKind::Leader,
                        0,
                    );
                    guard
                }
                MetadataCommandRecoveryAdmission::Waited { wait_us } => {
                    self.emit_metadata_command_recovery_admission_for_command(
                        pg_id,
                        &command,
                        observability::MetadataCommandRecoveryAdmissionKind::Waited,
                        wait_us,
                    );
                    self.emit_pending_slot_action_for_command(pg_id, &command, "drain_wait");
                    let waiter_outcome =
                        self.pending_command_recovery_waiter_outcome(pg_id, &command)?;
                    match waiter_outcome {
                        MetadataCommandRecoveryWaiterOutcome::Applied => {
                            self.emit_metadata_command_recovery_outcome_for_command(
                                pg_id,
                                &command,
                                waiter_outcome.metric_label(),
                            );
                            break command;
                        }
                        MetadataCommandRecoveryWaiterOutcome::MissingNotApplied
                        | MetadataCommandRecoveryWaiterOutcome::ReplacedNotApplied => {
                            let outcome = match (new_pending_command, waiter_outcome) {
                                (true, MetadataCommandRecoveryWaiterOutcome::MissingNotApplied) => {
                                    "cleanup_suppressed_waiter_missing_not_applied"
                                }
                                (
                                    true,
                                    MetadataCommandRecoveryWaiterOutcome::ReplacedNotApplied,
                                ) => "cleanup_suppressed_waiter_replaced_not_applied",
                                _ => waiter_outcome.metric_label(),
                            };
                            self.emit_metadata_command_recovery_outcome_for_command(
                                pg_id, &command, outcome,
                            );
                            // The recovery leader may have reissued and applied a matching
                            // command, so the owner cannot safely tear down payload state here.
                            return Err(conflicting_pending_object_metadata_command(
                                "retryable partial pending command for direct put commit",
                            ));
                        }
                        MetadataCommandRecoveryWaiterOutcome::StillPending => {
                            self.emit_metadata_command_recovery_outcome_for_command(
                                pg_id,
                                &command,
                                waiter_outcome.metric_label(),
                            );
                            work_budget
                                .sleep_after_contention(
                                    "direct PUT pending recovery retry budget exhausted",
                                )
                                .map_err(ObjectPgActionError::Store)?;
                            continue;
                        }
                    }
                }
                MetadataCommandRecoveryAdmission::TimedOut { wait_us } => {
                    self.emit_metadata_command_recovery_admission_for_command(
                        pg_id,
                        &command,
                        observability::MetadataCommandRecoveryAdmissionKind::TimedOut,
                        wait_us,
                    );
                    self.emit_metadata_command_recovery_outcome_for_command(
                        pg_id,
                        &command,
                        "timed_out",
                    );
                    self.emit_pending_slot_action_for_command(pg_id, &command, "drain_timeout");
                    return Err(conflicting_pending_object_metadata_command(
                        "pending direct PUT command recovery timed out",
                    ));
                }
            };

            let mut command = command;
            loop {
                match self.apply_metadata_command_to_acting_set(&command) {
                    Ok(()) => break,
                    Err(error)
                        if matches!(
                            self.retryable_partial_exact_metadata_command_conflict_applied_on_all_nodes(
                                pg_id,
                                &command,
                                error.applied_nodes,
                                &error.source,
                            )
                            .map_err(bucket_snapshot_error_to_object_pg_action_error)?,
                            Some(true)
                        ) =>
                    {
                        break;
                    }
                    Err(error)
                        if matches!(
                            self.retryable_partial_exact_metadata_command_conflict_applied_on_all_nodes(
                                pg_id,
                                &command,
                                error.applied_nodes,
                                &error.source,
                            )
                            .map_err(bucket_snapshot_error_to_object_pg_action_error)?,
                            Some(false)
                        ) =>
                    {
                        return Err(conflicting_pending_object_metadata_command(
                            "retryable partial direct PUT command conflict",
                        ));
                    }
                    Err(error)
                        if error.applied_nodes == 0
                            && Self::metadata_command_log_conflict_matches(
                                &command,
                                &error.source,
                            ) =>
                    {
                        let reissue_result = self.reissue_pending_metadata_command(pg_id, &command);
                        let Some(reissued) = (match reissue_result {
                            Ok(reissued) => reissued,
                            Err(BucketSnapshotLoadError::Store(
                                StoreError::MetadataCommandLogConflict { .. },
                            )) => {
                                return Err(conflicting_pending_object_metadata_command(
                                    "retryable direct PUT commit reissue conflict",
                                ));
                            }
                            Err(error) => {
                                return Err(bucket_snapshot_error_to_object_pg_action_error(error));
                            }
                        }) else {
                            if new_pending_command {
                                self.release_metadata_command_bucket_write_reservation(&command)
                                    .map_err(bucket_snapshot_error_to_object_pg_action_error)?;
                                self.release_object_generation_reservation_after_pending_drain_best_effort(
                                    pg_id,
                                    &req.bucket,
                                    &req.key,
                                    &req.generation_reservation_id,
                                );
                                self.delete_direct_put_segment_payload_shards(
                                    req.data_pg_id,
                                    req.ec,
                                    &req.segment_okh,
                                    req.segment_vid,
                                    written_shards,
                                );
                            }
                            return Err(conflicting_pending_object_metadata_command(
                                "pending direct PUT command was displaced during reissue",
                            ));
                        };
                        work_budget
                            .sleep_after_contention(
                                "direct PUT commit reissue retry budget exhausted",
                            )
                            .map_err(ObjectPgActionError::Store)?;
                        command = reissued;
                    }
                    Err(error) => {
                        if new_pending_command && error.applied_nodes == 0 {
                            self.record_abandoned_metadata_command_to_acting_set(&command)
                                .map_err(|error| {
                                    bucket_snapshot_error_to_object_pg_action_error(error.source)
                                })?;
                            self.release_metadata_command_bucket_write_reservation(&command)
                                .map_err(bucket_snapshot_error_to_object_pg_action_error)?;
                            self.remove_pending_metadata_command_for_bucket(
                                pg_id,
                                &req.bucket,
                                &command,
                            )
                            .map_err(ObjectPgActionError::from)?;
                            self.release_object_generation_reservation_after_pending_drain_best_effort(
                                pg_id,
                                &req.bucket,
                                &req.key,
                                &req.generation_reservation_id,
                            );
                            self.delete_direct_put_segment_payload_shards(
                                req.data_pg_id,
                                req.ec,
                                &req.segment_okh,
                                req.segment_vid,
                                written_shards,
                            );
                        }
                        return Err(bucket_snapshot_error_to_object_pg_action_error(
                            error.source,
                        ));
                    }
                }
            }

            self.release_metadata_command_bucket_write_reservation(&command)
                .map_err(bucket_snapshot_error_to_object_pg_action_error)?;
            self.remove_pending_metadata_command_for_bucket(pg_id, command.bucket_name(), &command)
                .map_err(ObjectPgActionError::from)?;
            self.emit_metadata_command_recovery_outcome_for_command(pg_id, &command, "applied");
            break command;
        };

        #[cfg(any(test, feature = "test-hooks"))]
        crate::node::maybe_run_after_direct_put_metadata_publish_hook(
            self.metadata_primary_test_hook_node().test_hook_scope_id(),
        )?;

        match command.payload() {
            MetadataCommandPayload::CommitDirectPutObject(commit) => {
                Ok(Ok(FinalizeDirectPutObjectOutcome {
                    version_id: commit.object.version_id,
                    encryption: commit.object.encryption.clone(),
                    live_tags: commit.object.tags.clone(),
                    live_size: commit.object.size,
                    live_last_modified: commit.last_modified_millis,
                    stale_generation_id: commit.stale_payload.as_ref().map(
                        |payload| match payload {
                            ObjectPayloadReclaimCommand::Segments(reclaim) => reclaim.generation_id,
                            ObjectPayloadReclaimCommand::Multipart(reclaim) => {
                                reclaim.generation_id
                            }
                        },
                    ),
                }))
            }
            _ => unreachable!("direct put commit pending command kind changed"),
        }
    }

    fn committed_direct_put_retry_outcome(
        req: &CommitDirectPutObjectReq,
        snapshot: &crate::DirectPutCommitStorageSnapshot,
    ) -> Result<Option<FinalizeDirectPutObjectOutcome>, ObjectPgActionError> {
        let Some(segments) = snapshot.committed_segments.as_ref() else {
            return Ok(None);
        };
        let Some(live) = snapshot
            .current
            .as_ref()
            .and_then(crate::StoredObject::as_live)
        else {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "direct PUT committed retry snapshot has segments but no live object"
                    .to_string(),
            });
        };
        let version_shape_matches = match req.versioning {
            crate::BucketVersioningState::Enabled => !live.version_id.is_null(),
            crate::BucketVersioningState::Disabled | crate::BucketVersioningState::Suspended => {
                live.version_id.is_null()
            }
        };
        let expected_etag = ObjectEtag::single_part(req.etag_crc64);
        if live.bucket != req.bucket
            || live.key != req.key
            || !version_shape_matches
            || live.owner != req.owner
            || live.acl_grants != req.acl_grants
            || live.public_read != req.public_read
            || live.generation_id != req.generation_id
            || live.size != req.size
            || live.etag != expected_etag
            || live.ec != req.ec
            || live.layout != ObjectLayout::Standard
            || live.tags != req.tags
            || live.metadata_blob.as_ref() != Some(&req.metadata_blob)
            || live.system_metadata_blob.as_ref() != Some(&req.system_metadata_blob)
            || live.object_lock != req.object_lock
            || live.encryption != req.encryption
        {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "direct PUT committed retry live object does not match request".to_string(),
            });
        }
        if segments.len() != 1 {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "direct PUT committed retry must have exactly one committed segment"
                    .to_string(),
            });
        }
        let segment = &segments[0];
        if segment.bucket != req.bucket
            || segment.key != req.key
            || segment.version_id != live.version_id
            || segment.segment_index != req.segment_index
            || segment.size != req.size
            || segment.segment_crc64 != req.segment_crc64
            || segment.segment_okh != req.segment_okh
            || segment.segment_vid != req.segment_vid
            || segment.data_pg_id != req.data_pg_id
            || segment.placement_cluster_epoch != req.bucket_write_reservation.cluster_epoch
            || segment.ec_k != req.ec.k
            || segment.ec_m != req.ec.m
        {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "direct PUT committed retry segment does not match request".to_string(),
            });
        }
        Ok(Some(FinalizeDirectPutObjectOutcome {
            version_id: live.version_id,
            encryption: live.encryption.clone(),
            live_tags: live.tags.clone(),
            live_size: live.size,
            live_last_modified: live.last_modified,
            stale_generation_id: snapshot.committed_stale_generation_id,
        }))
    }

    #[cfg(test)]
    fn prepare_commit_direct_put_object_command(
        &self,
        pg_id: PgId,
        object_pg: &crate::PgStore,
        req: &CommitDirectPutObjectReq,
        version_id: VersionId,
        bucket_write_reservation: BucketWriteReservationProof,
    ) -> Result<MetadataCommandEnvelope, ObjectPgActionError> {
        let reserved_generation = object_pg.get_object_generation_reservation(
            &req.bucket,
            &req.key,
            &req.generation_reservation_id,
        )?;
        if reserved_generation != req.generation_id {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: format!(
                    "object generation reservation mismatch: reserved {} but commit requested {}",
                    reserved_generation.get(),
                    req.generation_id.get()
                ),
            });
        }

        let last_modified_millis = crate::clock::current_time_millis();
        let write_sequence =
            object_pg.next_object_write_sequence(req.bucket.as_str(), req.key.as_str())?;
        let stale_payload = if version_id.is_null() {
            self.snapshot_direct_put_stale_payload_command(
                object_pg,
                &req.bucket,
                &req.key,
                last_modified_millis,
            )?
        } else {
            None
        };

        let segment_record = ObjectSegmentRecord {
            bucket: req.bucket.clone(),
            key: req.key.clone(),
            version_id,
            segment_index: req.segment_index,
            size: req.size,
            segment_crc64: req.segment_crc64,
            segment_okh: req.segment_okh,
            segment_vid: req.segment_vid,
            data_pg_id: req.data_pg_id,
            placement_cluster_epoch: self.operation_epoch(),
            ec_k: req.ec.k,
            ec_m: req.ec.m,
        };
        let object = PutLiveObjectReq {
            bucket: req.bucket.clone(),
            key: req.key.clone(),
            version_id,
            owner: req.owner.clone(),
            acl_grants: req.acl_grants.clone(),
            public_read: req.public_read,
            generation_id: req.generation_id,
            size: req.size,
            etag: ObjectEtag::single_part(req.etag_crc64),
            ec: req.ec,
            layout: ObjectLayout::Standard,
            tags: req.tags.clone(),
            metadata_blob: Some(req.metadata_blob.clone()),
            system_metadata_blob: Some(req.system_metadata_blob.clone()),
            object_lock: req.object_lock,
            encryption: req.encryption.clone(),
        };
        let command_id = self.next_object_metadata_command_id_from_locked_pg(pg_id, object_pg)?;
        let command = CommitDirectPutObjectCommand {
            object,
            segments: vec![segment_record],
            generation_reservation_id: req.generation_reservation_id.clone(),
            write_sequence,
            last_modified_millis,
            stale_payload,
            bucket_write_reservation,
            stream_create_bucket_write_reservation: None,
        };
        Ok(MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::CommitDirectPutObject(Box::new(command)),
        ))
    }

    #[cfg(test)]
    fn snapshot_direct_put_stale_payload_command(
        &self,
        pg: &crate::PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        created_at: u64,
    ) -> Result<Option<ObjectPayloadReclaimCommand>, MetadataError> {
        let stored = match PgMetadataStore::get_object_version(pg, bucket, key, VersionId::Null) {
            Ok(stored) => stored,
            Err(MetadataError::ObjectNotFound) => return Ok(None),
            Err(error) => return Err(error),
        };
        let record = match stored {
            crate::StoredObject::Live(record) => record,
            crate::StoredObject::DeleteMarker(_) => return Ok(None),
        };

        Ok(Some(Self::snapshot_live_object_payload_reclaim_command(
            pg, bucket, key, &record, created_at,
        )?))
    }

    #[cfg(test)]
    fn snapshot_live_object_payload_reclaim_command(
        pg: &crate::PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        record: &crate::LiveObjectRecord,
        created_at: u64,
    ) -> Result<ObjectPayloadReclaimCommand, MetadataError> {
        match record.layout {
            ObjectLayout::Standard => {
                let segments =
                    PgMetadataStore::get_object_segments(pg, bucket, key, record.version_id)?;
                Ok(ObjectPayloadReclaimCommand::Segments(
                    ObjectSegmentsReclaimRecord {
                        bucket: bucket.clone(),
                        key: key.clone(),
                        generation_id: record.generation_id,
                        created_at,
                        segments: segments
                            .into_iter()
                            .map(|segment| ObjectSegmentsReclaimSegmentRecord {
                                segment_index: segment.segment_index,
                                segment_okh: segment.segment_okh,
                                segment_vid: segment.segment_vid,
                                data_pg_id: segment.data_pg_id,
                                ec: EcShape {
                                    k: segment.ec_k,
                                    m: segment.ec_m,
                                },
                            })
                            .collect(),
                    },
                ))
            }
            ObjectLayout::MultipartManifest { .. } => {
                let parts = PgMetadataStore::get_object_parts(pg, bucket, key, record.version_id)?;
                let mut streaming_segments = Vec::new();
                for part in &parts {
                    if part.part_okh == [0u8; 16] {
                        streaming_segments.extend(PgMetadataStore::get_multipart_part_segments(
                            pg,
                            bucket,
                            key,
                            record.version_id,
                            part.part_number,
                        )?);
                    }
                }
                Ok(ObjectPayloadReclaimCommand::Multipart(
                    MultipartReclaimRecord::from_object_parts(
                        bucket,
                        key,
                        record.generation_id,
                        created_at,
                        &parts,
                        &streaming_segments,
                    ),
                ))
            }
        }
    }

    pub fn delete_direct_put_segment_payload_shards(
        &self,
        data_pg_id: u32,
        ec: EcShape,
        segment_okh: &[u8; 16],
        segment_vid: GenerationId,
        written_shards: &[WrittenShardAck],
    ) {
        self.delete_payload_shard_keys_best_effort(
            data_pg_id,
            ec,
            segment_okh,
            segment_vid,
            written_shards.iter().map(|written| written.key.clone()),
        );
    }

    fn delete_direct_put_segment_payload_shards_at_epoch(
        &self,
        operation_epoch: ClusterEpoch,
        data_pg_id: u32,
        ec: EcShape,
        segment_okh: &[u8; 16],
        segment_vid: GenerationId,
        written_shards: &[WrittenShardAck],
    ) {
        self.delete_payload_shard_keys_best_effort_at_epoch(
            operation_epoch,
            data_pg_id,
            ec,
            segment_okh,
            segment_vid,
            written_shards.iter().map(|written| written.key.clone()),
        );
    }

    pub fn create_put_object_stream_session_record(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        encryption: ObjectEncryption,
    ) -> Result<(), ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        let mut work_budget = RequestWorkBudget::new(PUT_OBJECT_STREAM_CREATE_RETRY_BUDGET, None)
            .for_operation("create_put_object_stream_session")
            .for_pg(pg_id);
        loop {
            work_budget
                .check("put object stream create retry budget exhausted")
                .map_err(ObjectPgActionError::Store)?;
            let reservation = match self
                .acquire_durable_put_object_stream_write_reservation(bucket, key)
            {
                Ok(reservation) => reservation,
                Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteDraining)) => {
                    self.wait_for_durable_bucket_write_drain(bucket)
                        .map_err(bucket_snapshot_error_to_object_pg_action_error)?;
                    continue;
                }
                Err(error) => return Err(bucket_snapshot_error_to_object_pg_action_error(error)),
            };
            let proof = BucketWriteReservationProof::from(&reservation.record);
            let result = self.create_put_object_stream_session_record_under_reservation(
                bucket,
                key,
                session_id,
                encryption,
                proof.clone(),
                &mut work_budget,
            );
            let release_result = match &result {
                Ok(BucketWriteReservationDisposition::TransferredToCommand) => Ok(()),
                Ok(BucketWriteReservationDisposition::PreserveForOwnershipCheckFailure) => Ok(()),
                Ok(BucketWriteReservationDisposition::ReleaseByCaller) => self
                    .release_durable_bucket_write_reservation(reservation)
                    .map_err(bucket_snapshot_error_to_object_pg_action_error),
                Err(_) => {
                    match self.pending_metadata_command_uses_bucket_write_reservation(
                        pg_id, bucket, &proof,
                    ) {
                        Ok(true) => Ok(()),
                        Ok(false) => self
                            .release_durable_bucket_write_reservation(reservation)
                            .map_err(bucket_snapshot_error_to_object_pg_action_error),
                        Err(error) => Err(error),
                    }
                }
            };
            return match (result, release_result) {
                (Ok(_), Ok(())) => Ok(()),
                (Ok(_), Err(error)) => Err(error),
                (Err(error), Ok(())) | (Err(error), Err(_)) => Err(error),
            };
        }
    }

    fn create_put_object_stream_session_record_under_reservation(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        encryption: ObjectEncryption,
        bucket_write_reservation: BucketWriteReservationProof,
        work_budget: &mut RequestWorkBudget,
    ) -> Result<BucketWriteReservationDisposition, ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        let request = CreateStreamUploadReq {
            session_id: session_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            target: StreamUploadTarget::PutObject,
            encryption,
        };
        loop {
            work_budget
                .check("put object stream create retry budget exhausted")
                .map_err(ObjectPgActionError::Store)?;
            let applied_commands =
                self.drain_pending_object_metadata_commands_for_bucket_collect(pg_id, bucket)?;
            let expected_command = applied_stream_create_command(&applied_commands, &request);
            let mutation_client = self.object_mutation_metadata_primary_client(bucket, key)?;
            if mutation_client.matching_stream_upload_exists(pg_id, &request, expected_command)? {
                return Ok(BucketWriteReservationDisposition::ReleaseByCaller);
            }
            self.reserve_put_object_generation(bucket, key, session_id)?;
            let command = match mutation_client.build_create_stream_upload_command(
                BuildCreateStreamUploadCommandReq {
                    pg_id,
                    cluster_epoch: self.operation_epoch(),
                    request: &request,
                    precondition: CreateStreamUploadPrecondition::PutObjectNoCurrentCheck {
                        require_generation_reservation: true,
                    },
                    bucket_write_reservation: &bucket_write_reservation,
                },
            ) {
                Ok(command) => command,
                Err(ObjectPgActionError::StaleObjectReadSubject) => {
                    self.release_object_generation_reservation(bucket, key, session_id)?;
                    work_budget
                        .sleep_after_contention(
                            "put object stream create stale read retry budget exhausted",
                        )
                        .map_err(ObjectPgActionError::Store)?;
                    continue;
                }
                Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                })) => {
                    self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
                    self.release_object_generation_reservation(bucket, key, session_id)?;
                    work_budget
                        .sleep_after_contention(
                            "put object stream create log conflict retry budget exhausted",
                        )
                        .map_err(ObjectPgActionError::Store)?;
                    continue;
                }
                Err(error) => {
                    let _ = self.release_object_generation_reservation(bucket, key, session_id);
                    return Err(error);
                }
            };
            if !self.try_install_object_pg_pending_command_or_drain(pg_id, bucket, &command)? {
                let cleanup = self
                    .drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)
                    .and_then(|_| {
                        self.release_object_generation_reservation(bucket, key, session_id)
                    });
                cleanup?;
                work_budget
                    .sleep_after_contention(
                        "put object stream create pending install retry budget exhausted",
                    )
                    .map_err(ObjectPgActionError::Store)?;
                continue;
            }
            if let Err(error) =
                self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)
            {
                if self
                    .pending_metadata_command_for_bucket(pg_id, bucket)?
                    .is_none()
                {
                    let _ = self.release_object_generation_reservation(bucket, key, session_id);
                }
                return Err(error);
            }
            return Ok(BucketWriteReservationDisposition::TransferredToCommand);
        }
    }

    pub fn load_stream_upload_session(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<StreamUploadRecord, ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        let mutation_client = self.object_mutation_metadata_primary_client(bucket, key)?;
        mutation_client.load_stream_upload_session(pg_id, bucket, key, session_id)
    }

    pub fn prepare_stream_segment_append(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        request: &PrepareStreamUploadSegmentAppendReq,
    ) -> Result<(StreamUploadTarget, StreamUploadSegmentRecord), ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
        let mutation_client = self.object_mutation_metadata_primary_client(bucket, key)?;
        let (target, mut segment_record) =
            mutation_client.prepare_stream_segment_append(pg_id, bucket, key, request)?;
        segment_record.placement_cluster_epoch = self.operation_epoch();
        Ok((target, segment_record))
    }

    pub fn write_stream_segment_payload_shards(
        &self,
        segment_record: &StreamUploadSegmentRecord,
        data: &[u8],
    ) -> Result<Vec<WrittenShardAck>, StoreError> {
        let ec = EcShape {
            k: segment_record.ec_k,
            m: segment_record.ec_m,
        };
        self.write_placed_segment_payload_shards(
            DataPgId::new(PgId::new(segment_record.data_pg_id)),
            ec,
            &segment_record.segment_okh,
            segment_record.segment_vid,
            data,
        )
    }

    pub fn commit_stream_segment_append(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        segment_index: u32,
        segment_record: &StreamUploadSegmentRecord,
        shard_batch: &[(&ShardKey, WriteAck)],
    ) -> Result<(), ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        let ec = EcShape {
            k: segment_record.ec_k,
            m: segment_record.ec_m,
        };
        macro_rules! cleanup_stream_append_payload {
            () => {
                self.delete_stream_segment_payload_shard_keys_best_effort(
                    segment_record,
                    shard_batch.iter().map(|(key, _)| (*key).clone()),
                )
            };
        }
        let mutation_client = match self.object_mutation_metadata_primary_client(bucket, key) {
            Ok(client) => client,
            Err(error) => {
                cleanup_stream_append_payload!();
                return Err(error.into());
            }
        };
        let mut empty_log_conflicts = 0;
        loop {
            if let Err(error) =
                self.drain_pending_object_metadata_commands_for_exact_bucket(pg_id, bucket)
            {
                cleanup_stream_append_payload!();
                return Err(error);
            }
            let existing_stream_segment =
                match mutation_client.load_stream_upload_segments(pg_id, bucket, key, session_id) {
                    Ok(segments) => segments
                        .into_iter()
                        .find(|segment| segment.segment_index == segment_index),
                    Err(error) => {
                        cleanup_stream_append_payload!();
                        return Err(error);
                    }
                };
            match existing_stream_segment {
                Some(existing) if existing == *segment_record => return Ok(()),
                Some(_) => {
                    cleanup_stream_append_payload!();
                    return Err(ObjectPgActionError::InvalidRequest {
                        reason: format!("duplicate segment_index {segment_index}"),
                    });
                }
                None => {}
            }

            self.maybe_run_before_stream_append_command_id_hook();
            if let Err(error) =
                self.register_payload_shard_acks(segment_record.data_pg_id, shard_batch)
            {
                cleanup_stream_append_payload!();
                return Err(error);
            }
            if let Err(error) = self.validate_payload_shard_acks(
                segment_record.data_pg_id,
                ec,
                &segment_record.segment_okh,
                segment_record.segment_vid,
                shard_batch,
            ) {
                cleanup_stream_append_payload!();
                return Err(error);
            }

            self.maybe_run_before_metadata_command_pending_install_hook();
            let command = match self.try_install_object_pg_pending_command_with_fresh_id(
                pg_id,
                bucket,
                false,
                |command_id| {
                    MetadataCommandEnvelope::new(
                        command_id,
                        MetadataCommandPayload::AppendStreamSegment(Box::new(
                            AppendStreamSegmentCommand {
                                bucket: bucket.clone(),
                                key: key.clone(),
                                segment: segment_record.clone(),
                            },
                        )),
                    )
                },
            ) {
                Ok(ObjectPgPendingCommandInstall::Installed(command)) => command,
                Ok(ObjectPgPendingCommandInstall::Pending(command)) => {
                    if let Err(error) = self.drain_pending_object_metadata_command(pg_id, &command)
                    {
                        cleanup_stream_append_payload!();
                        return Err(error);
                    }
                    continue;
                }
                Ok(ObjectPgPendingCommandInstall::LogConflict { pending_visible }) => {
                    if let Err(error) = self.drain_after_object_pg_log_conflict(
                        pg_id,
                        bucket,
                        pending_visible,
                        &mut empty_log_conflicts,
                        "stream append command log conflict without pending progress",
                    ) {
                        cleanup_stream_append_payload!();
                        return Err(error);
                    }
                    continue;
                }
                Err(error) => {
                    cleanup_stream_append_payload!();
                    return Err(error);
                }
            };
            match self.apply_new_stream_append_command(
                pg_id,
                bucket,
                &command,
                segment_record,
                shard_batch,
            )? {
                StreamAppendCommandApplyOutcome::Applied => return Ok(()),
                StreamAppendCommandApplyOutcome::RetryFromFreshSnapshot => continue,
            }
        }
    }

    fn register_payload_shard_acks(
        &self,
        data_pg_id: u32,
        shard_batch: &[(&ShardKey, WriteAck)],
    ) -> Result<(), ObjectPgActionError> {
        let pg_id = PgId::new(data_pg_id);
        let shard_ack_client = self.metadata_pg_primary_shard_ack_client(pg_id)?;
        shard_ack_client.register_written_shard_acks(pg_id, shard_batch)?;
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_register_payload_shard_acks(
        &self,
        data_pg_id: u32,
        written_shards: &[WrittenShardAck],
    ) -> Result<(), ObjectPgActionError> {
        let shard_batch: Vec<_> = written_shards
            .iter()
            .map(|written| (&written.key, written.ack))
            .collect();
        self.register_payload_shard_acks(data_pg_id, &shard_batch)
    }

    fn validate_payload_shard_acks(
        &self,
        data_pg_id: u32,
        ec: EcShape,
        segment_okh: &[u8; 16],
        segment_vid: GenerationId,
        shard_batch: &[(&ShardKey, WriteAck)],
    ) -> Result<(), ObjectPgActionError> {
        let expected_keys = Self::payload_shard_set_keys(segment_okh, segment_vid, ec);
        if shard_batch.len() != expected_keys.len() {
            return Err(ObjectPgActionError::Store(
                StoreError::PayloadShardSetMismatch {
                    reason: format!(
                        "expected {} shards for EC {}+{}, got {}",
                        expected_keys.len(),
                        ec.k,
                        ec.m,
                        shard_batch.len()
                    ),
                },
            ));
        }
        for (expected_key, (actual_key, _)) in expected_keys.iter().zip(shard_batch.iter()) {
            if expected_key != *actual_key {
                return Err(ObjectPgActionError::Store(
                    StoreError::PayloadShardSetMismatch {
                        reason: format!(
                            "expected shard {} at index {}, got {}",
                            expected_key,
                            expected_key.shard_index().get(),
                            actual_key
                        ),
                    },
                ));
            }
        }

        let pg_id = PgId::new(data_pg_id);
        let shard_ack_client = self.metadata_pg_primary_shard_ack_client(pg_id)?;
        for (key, ack) in shard_batch {
            shard_ack_client.validate_written_shard_ack(pg_id, key, *ack)?;
        }

        let data_pg = DataPgId::new(PgId::new(data_pg_id));
        let placement_key = segment_payload_placement_key(segment_okh, segment_vid);
        let locations = self
            .place_payload_shards(data_pg, ec, &placement_key)
            .map_err(|error| ObjectPgActionError::Store(cluster_build_error_to_store(error)))?;
        for (key, ack) in shard_batch {
            let location = Self::placed_payload_shard_location(&locations, key)
                .map_err(ObjectPgActionError::Store)?;
            self.read_payload_shard(location, key, *ack)
                .map_err(|error| ObjectPgActionError::Store(shard_io_error_to_store(error)))?;
        }
        Ok(())
    }

    pub fn audit_shard_storage_for_scavenger(
        &self,
    ) -> Result<Vec<ShardScavengerObservation>, StoreError> {
        let referenced_scan = self.collect_shard_scavenger_referenced_shards();
        let mut reference_scan_errors = Vec::new();
        let referenced_scan = match referenced_scan {
            Ok(referenced_scan) => referenced_scan,
            Err(error) => {
                reference_scan_errors.push(format!("reference scan failed: {error}"));
                ShardScavengerReferenceScan::default()
            }
        };
        let mut expected_nodes_by_shard: HashMap<(u32, ShardKey), HashSet<u32>> = HashMap::new();
        for (node_id, data_pg_id, shard_key) in &referenced_scan.locations {
            expected_nodes_by_shard
                .entry((*data_pg_id, shard_key.clone()))
                .or_default()
                .insert(*node_id);
        }
        let mut observations = Vec::new();

        for route in self.local_pg_routes() {
            let primary_node = self
                .local_map
                .metadata_pg_primary_node(self.operation_epoch(), route.pg_id())?;
            let primary_node_id = primary_node.node_id().as_u32();
            let data_pg = DataPgId::new(route.pg_id());
            let data_pg_id = data_pg.get();
            let scavenger_client = primary_node.shard_scavenger_client();
            let shard_rows = scavenger_client.list_scavenger_shard_rows(route.pg_id())?;
            let rows_by_key: HashMap<ShardKey, WriteAck> = shard_rows
                .iter()
                .map(|row| (row.key.clone(), row.ack))
                .collect();

            let mut files_by_node = Vec::new();
            let mut scan_errors = Vec::new();
            for node_id in self.local_map.node_ids() {
                let Some(node) = self.local_map.node(node_id) else {
                    continue;
                };
                match node
                    .shard_scavenger_client()
                    .list_scavenger_shard_files(data_pg)
                {
                    Ok(scan) if scan.errors.is_empty() => {
                        files_by_node.push((node_id.as_u32(), scan.files));
                    }
                    Ok(scan) => {
                        scan_errors.extend(
                            scan.errors
                                .into_iter()
                                .map(|error| (node_id.as_u32(), error)),
                        );
                    }
                    Err(error) => {
                        scan_errors.push((node_id.as_u32(), error.to_string()));
                    }
                }
            }

            if !reference_scan_errors.is_empty() {
                scavenger_client.record_shard_scavenger_observation(
                    route.pg_id(),
                    &Self::shard_scavenger_scan_incomplete_observation(
                        primary_node_id,
                        data_pg_id,
                        &reference_scan_errors,
                    ),
                )?;
                observations
                    .extend(scavenger_client.list_shard_scavenger_observations(route.pg_id())?);
                continue;
            }

            if !scan_errors.is_empty() {
                let mut errors_by_node: BTreeMap<u32, Vec<String>> = BTreeMap::new();
                for (node_id, error) in scan_errors {
                    errors_by_node.entry(node_id).or_default().push(error);
                }
                for (node_id, errors) in errors_by_node {
                    scavenger_client.record_shard_scavenger_observation(
                        route.pg_id(),
                        &Self::shard_scavenger_scan_incomplete_observation(
                            node_id, data_pg_id, &errors,
                        ),
                    )?;
                }
                observations
                    .extend(scavenger_client.list_shard_scavenger_observations(route.pg_id())?);
                continue;
            }

            let mut active_observations = HashSet::new();
            let mut file_locations = HashSet::new();
            for (node_id, files) in files_by_node {
                for file in files {
                    file_locations.insert((node_id, data_pg_id, file.key.clone()));
                    let observation_key = crate::types::ShardScavengerObservationKey {
                        node_id,
                        data_pg_id,
                        shard_index: file.key.shard_index(),
                        shard_key: file.key.clone(),
                    };
                    let Some(row_ack) = rows_by_key.get(&file.key).copied() else {
                        active_observations.insert(observation_key.clone());
                        scavenger_client.record_shard_scavenger_observation(
                            route.pg_id(),
                            &ShardScavengerObservationRecord {
                                key: observation_key,
                                data_size: Some(file.size),
                                crc64: None,
                                file_exists: true,
                                shard_row_exists: false,
                                reason: ShardScavengerObservationReason::FileWithoutShardRow,
                                last_error: None,
                            },
                        )?;
                        continue;
                    };
                    let shard_identity = (node_id, data_pg_id, file.key.clone());
                    if referenced_scan.locations.contains(&shard_identity) {
                        continue;
                    }
                    active_observations.insert(observation_key.clone());
                    scavenger_client.record_shard_scavenger_observation(
                        route.pg_id(),
                        &ShardScavengerObservationRecord {
                            key: observation_key,
                            data_size: Some(row_ack.stored_size),
                            crc64: Some(row_ack.crc64),
                            file_exists: true,
                            shard_row_exists: true,
                            reason: ShardScavengerObservationReason::UnreferencedShardRowAndFile,
                            last_error: None,
                        },
                    )?;
                }
            }

            for row in shard_rows {
                let shard_identity = (data_pg_id, row.key.clone());
                if let Some(expected_nodes) = expected_nodes_by_shard.get(&shard_identity) {
                    for expected_node_id in expected_nodes {
                        if file_locations.contains(&(
                            *expected_node_id,
                            data_pg_id,
                            row.key.clone(),
                        )) {
                            continue;
                        }
                        let observation_key = crate::types::ShardScavengerObservationKey {
                            node_id: *expected_node_id,
                            data_pg_id,
                            shard_index: row.key.shard_index(),
                            shard_key: row.key.clone(),
                        };
                        active_observations.insert(observation_key.clone());
                        scavenger_client.record_shard_scavenger_observation(
                            route.pg_id(),
                            &ShardScavengerObservationRecord {
                                key: observation_key,
                                data_size: Some(row.ack.stored_size),
                                crc64: Some(row.ack.crc64),
                                file_exists: false,
                                shard_row_exists: true,
                                reason: ShardScavengerObservationReason::ShardRowWithoutFile,
                                last_error: None,
                            },
                        )?;
                        if let Some(work_item) = referenced_scan
                            .repair_work_by_shard
                            .get(&(data_pg_id, row.key.clone()))
                            .copied()
                        {
                            self.schedule_placed_segment_shard_repair(
                                work_item.request,
                                work_item.shard_index,
                            )?;
                        }
                    }
                    continue;
                }

                if file_locations.iter().any(|(_, file_data_pg_id, file_key)| {
                    *file_data_pg_id == data_pg_id && file_key == &row.key
                }) {
                    continue;
                }
                let observation_key = crate::types::ShardScavengerObservationKey {
                    node_id: primary_node_id,
                    data_pg_id,
                    shard_index: row.key.shard_index(),
                    shard_key: row.key,
                };
                active_observations.insert(observation_key.clone());
                scavenger_client.record_shard_scavenger_observation(
                    route.pg_id(),
                    &ShardScavengerObservationRecord {
                        key: observation_key,
                        data_size: Some(row.ack.stored_size),
                        crc64: Some(row.ack.crc64),
                        file_exists: false,
                        shard_row_exists: true,
                        reason: ShardScavengerObservationReason::ShardRowWithoutFile,
                        last_error: None,
                    },
                )?;
            }

            for observation in scavenger_client.list_shard_scavenger_observations(route.pg_id())? {
                if observation.key.data_pg_id != data_pg_id
                    || observation.resolved_at.is_some()
                    || !matches!(
                        observation.reason,
                        ShardScavengerObservationReason::FileWithoutShardRow
                            | ShardScavengerObservationReason::ShardRowWithoutFile
                            | ShardScavengerObservationReason::UnreferencedShardRowAndFile
                            | ShardScavengerObservationReason::ScanIncomplete
                    )
                {
                    continue;
                }
                if !active_observations.contains(&observation.key) {
                    scavenger_client
                        .resolve_shard_scavenger_observation(route.pg_id(), &observation.key)?;
                }
            }

            observations.extend(scavenger_client.list_shard_scavenger_observations(route.pg_id())?);
        }

        Ok(observations)
    }

    fn shard_scavenger_scan_incomplete_observation(
        node_id: u32,
        data_pg_id: u32,
        errors: &[String],
    ) -> ShardScavengerObservationRecord {
        let shard_key = ShardKey::new(&[0; 16], 0, 0);
        ShardScavengerObservationRecord {
            key: ShardScavengerObservationKey {
                node_id,
                data_pg_id,
                shard_index: shard_key.shard_index(),
                shard_key,
            },
            data_size: None,
            crc64: None,
            file_exists: false,
            shard_row_exists: false,
            reason: ShardScavengerObservationReason::ScanIncomplete,
            last_error: Some(errors.join("; ")),
        }
    }

    fn collect_shard_scavenger_referenced_shards(
        &self,
    ) -> Result<ShardScavengerReferenceScan, StoreError> {
        let mut scan = ShardScavengerReferenceScan::default();
        for route in self.local_pg_routes() {
            let node = self
                .local_map
                .metadata_pg_primary_node(self.operation_epoch(), route.pg_id())?;
            for reference in node
                .shard_scavenger_client()
                .list_shard_scavenger_payload_references(route.pg_id())?
            {
                match reference {
                    ShardScavengerPayloadReference::Placed(reference) => {
                        self.extend_referenced_shard_set(&mut scan, &reference)?;
                    }
                    ShardScavengerPayloadReference::ReclaimOnly(reference) => {
                        self.extend_reclaim_referenced_shard_set(&mut scan, &reference)?;
                    }
                    ShardScavengerPayloadReference::RoutedMultipartPart(reference) => {
                        let data_pg_id = self
                            .local_map
                            .object_generation_multipart_part_data_pg(
                                &reference.bucket,
                                &reference.key,
                                reference.object_generation_id,
                                reference.part_number,
                            )
                            .get();
                        let reference = ShardScavengerPlacedShardSetReference {
                            data_pg_id,
                            okh: reference.part_okh,
                            generation_id: reference.part_vid,
                            placement_cluster_epoch: reference.placement_cluster_epoch,
                            stored_size: reference.stored_size,
                            crc64: reference.crc64,
                            ec: reference.ec,
                        };
                        self.extend_referenced_shard_set(&mut scan, &reference)?;
                    }
                }
            }
        }

        Ok(scan)
    }

    fn extend_referenced_shard_set(
        &self,
        scan: &mut ShardScavengerReferenceScan,
        reference: &ShardScavengerPlacedShardSetReference,
    ) -> Result<(), StoreError> {
        self.extend_referenced_shard_locations(
            scan,
            reference.data_pg_id,
            reference.okh,
            reference.generation_id,
            reference.placement_cluster_epoch,
            reference.ec,
        )?;
        let request = SegmentStoredBytesRequest {
            data_pg_id: reference.data_pg_id,
            segment_okh: reference.okh,
            segment_vid: reference.generation_id,
            stored_size: reference.stored_size as usize,
            segment_crc64: reference.crc64,
            ec: reference.ec,
        };
        for key in
            Self::payload_shard_set_keys(&reference.okh, reference.generation_id, reference.ec)
        {
            scan.repair_work_by_shard.insert(
                (reference.data_pg_id, key.clone()),
                PlacedSegmentShardRepairWorkItem {
                    request,
                    shard_index: key.shard_index(),
                },
            );
        }
        Ok(())
    }

    fn extend_reclaim_referenced_shard_set(
        &self,
        scan: &mut ShardScavengerReferenceScan,
        reference: &crate::types::ShardScavengerReclaimShardSetReference,
    ) -> Result<(), StoreError> {
        self.extend_referenced_shard_locations(
            scan,
            reference.data_pg_id,
            reference.okh,
            reference.generation_id,
            self.operation_epoch(),
            reference.ec,
        )
    }

    fn extend_referenced_shard_locations(
        &self,
        scan: &mut ShardScavengerReferenceScan,
        data_pg_id: u32,
        okh: [u8; 16],
        generation_id: GenerationId,
        placement_cluster_epoch: ClusterEpoch,
        ec: EcShape,
    ) -> Result<(), StoreError> {
        let data_pg = DataPgId::new(PgId::new(data_pg_id));
        let placement_key = segment_payload_placement_key(&okh, generation_id);
        let route =
            self.reconstructed_pg_route_at_epoch(data_pg.pg_id(), placement_cluster_epoch)?;
        let locations = self
            .place_payload_shards_for_pg_route_snapshot(&route, data_pg, ec, &placement_key)
            .map_err(cluster_build_error_to_store)?;
        for key in Self::payload_shard_set_keys(&okh, generation_id, ec) {
            let location = Self::placed_payload_shard_location(&locations, &key)?;
            scan.locations
                .insert((location.node_id().as_u32(), data_pg_id, key.clone()));
        }
        Ok(())
    }

    pub fn abort_stream_upload_session(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<(), ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        let mutation_client = self.object_mutation_metadata_primary_client(bucket, key)?;
        let mut pending_completed_session =
            self.pending_command_completes_stream_session(pg_id, bucket, key, session_id)?;
        self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;

        let observed_stream_session =
            match mutation_client.load_stream_upload_segments(pg_id, bucket, key, session_id) {
                Ok(_) => true,
                Err(ObjectPgActionError::Metadata(MetadataError::StreamSessionNotFound {
                    ..
                })) if pending_completed_session => {
                    return Ok(());
                }
                Err(error) => return Err(error),
            };

        #[cfg(any(test, feature = "test-hooks"))]
        self.maybe_run_before_stream_abort_storage_hook();

        loop {
            pending_completed_session =
                self.pending_command_completes_stream_session(pg_id, bucket, key, session_id)?;
            self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;

            let stream_session =
                match mutation_client.load_stream_upload_session(pg_id, bucket, key, session_id) {
                    Ok(stream_session) => stream_session,
                    Err(ObjectPgActionError::Metadata(MetadataError::StreamSessionNotFound {
                        ..
                    })) if pending_completed_session || observed_stream_session => {
                        return Ok(());
                    }
                    Err(error) => return Err(error),
                };
            let staged_segments =
                match mutation_client.load_stream_upload_segments(pg_id, bucket, key, session_id) {
                    Ok(staged_segments) => staged_segments,
                    Err(ObjectPgActionError::Metadata(MetadataError::StreamSessionNotFound {
                        ..
                    })) if pending_completed_session || observed_stream_session => {
                        return Ok(());
                    }
                    Err(error) => return Err(error),
                };
            let command_id = match self.next_object_metadata_command_id(pg_id) {
                Ok(command_id) => command_id,
                Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                })) => {
                    continue;
                }
                Err(error) => return Err(error),
            };
            let command = MetadataCommandEnvelope::new(
                command_id,
                MetadataCommandPayload::AbortStreamUpload(Box::new(AbortStreamUploadCommand {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    session_id: session_id.clone(),
                    staged_segments,
                    stream_create_bucket_write_reservation: stream_session
                        .bucket_write_reservation
                        .clone(),
                })),
            );
            if !self.try_install_object_pg_pending_command_or_drain(pg_id, bucket, &command)? {
                continue;
            }
            self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
            return Ok(());
        }
    }

    pub fn list_stream_upload_sessions_best_effort(&self) -> Vec<StreamUploadRecord> {
        const STREAM_UPLOAD_SESSION_BEST_EFFORT_PAGE_LIMIT: u32 = 1024;
        let mut sessions = Vec::new();
        for &pg_id in self.local_map.pg_ids() {
            let pg_id = PgId::new(pg_id);
            let Ok(node) = self
                .local_map
                .metadata_pg_primary_node(self.operation_epoch(), pg_id)
            else {
                continue;
            };
            let mut marker = None;
            loop {
                let Ok(page) = node
                    .object_mutation_metadata_client()
                    .list_all_stream_uploads_page(
                        pg_id,
                        marker.as_ref(),
                        STREAM_UPLOAD_SESSION_BEST_EFFORT_PAGE_LIMIT,
                    )
                else {
                    break;
                };
                if page.uploads.iter().any(|upload| {
                    self.object_metadata_pg_id(&upload.bucket, &upload.key) != pg_id.get()
                }) {
                    break;
                }
                sessions.extend(page.uploads);
                let Some(next_marker) = page.next_session_id_marker else {
                    break;
                };
                marker = Some(next_marker);
            }
        }
        sessions
    }

    pub fn scavenge_abandoned_stream_sessions(&self, max_age_ms: u64) -> usize {
        let cutoff = crate::clock::current_time_millis().saturating_sub(max_age_ms);
        let mut count = 0;

        for session in self.list_stream_upload_sessions_best_effort() {
            if session.created_at >= cutoff {
                continue;
            }
            if session.target != StreamUploadTarget::PutObject {
                continue;
            }
            match self.stream_upload_has_live_bucket_write_reservation(&session) {
                Ok(true) => continue,
                Ok(false) => {}
                Err(_) => continue,
            }
            match self.abort_stream_upload_session(
                &session.bucket,
                &session.key,
                &session.session_id,
            ) {
                Ok(()) => {
                    let _ = self.release_object_generation_reservation(
                        &session.bucket,
                        &session.key,
                        &session.session_id,
                    );
                    count += 1;
                }
                Err(ObjectPgActionError::Metadata(MetadataError::StreamSessionNotFound {
                    ..
                })) => {
                    let _ = self.release_object_generation_reservation(
                        &session.bucket,
                        &session.key,
                        &session.session_id,
                    );
                    count += 1;
                }
                Err(_) => {}
            }
        }

        count
    }

    pub fn read_segment_payload_stored_bytes_into(
        &self,
        req: SegmentStoredBytesRequest,
        dst: &mut Vec<u8>,
    ) -> Result<(), StoreError> {
        self.read_segment_payload_stored_bytes_at_placement_epoch_into(
            self.operation_epoch(),
            req,
            dst,
        )
    }

    pub fn read_segment_payload_stored_bytes_at_placement_epoch_into(
        &self,
        placement_cluster_epoch: ClusterEpoch,
        req: SegmentStoredBytesRequest,
        dst: &mut Vec<u8>,
    ) -> Result<(), StoreError> {
        self.require_current_payload_operation_epoch(req.data_pg_id)?;
        let found = if placement_cluster_epoch == self.operation_epoch() {
            self.try_read_placed_segment_stored_bytes_into(req, dst, true)?
        } else {
            let data_pg = DataPgId::new(PgId::new(req.data_pg_id));
            let route =
                self.reconstructed_pg_route_at_epoch(data_pg.pg_id(), placement_cluster_epoch)?;
            self.try_read_placed_segment_stored_bytes_for_pg_route_snapshot_into(&route, req, dst)?
        };
        match found {
            true => Ok(()),
            false => {
                dst.clear();
                Err(StoreError::NotFound)
            }
        }
    }

    pub fn try_take_placed_segment_shard_repair_work(
        &self,
    ) -> Option<PlacedSegmentShardRepairWorkItem> {
        self.local_map
            .runtime_state()
            .try_take_placed_segment_shard_repair_work()
    }

    pub fn wait_for_placed_segment_shard_repair_work(
        &self,
        stop: &AtomicBool,
    ) -> Option<PlacedSegmentShardRepairWorkItem> {
        self.local_map
            .runtime_state()
            .wait_for_placed_segment_shard_repair_work_poll(stop)
    }

    pub fn wake_placed_segment_shard_repair_workers(&self) {
        self.local_map
            .runtime_state()
            .wake_placed_segment_shard_repair_workers();
    }

    pub fn enqueue_durable_placed_segment_shard_repair_work(
        &self,
    ) -> Result<DurablePlacedSegmentShardRepairEnqueueSummary, StoreError> {
        let mut summary = DurablePlacedSegmentShardRepairEnqueueSummary::default();
        for route in self.local_pg_routes() {
            if route.state() != PgState::Active {
                continue;
            }
            for repair in self.list_placed_segment_shard_repairs(route.pg_id().get())? {
                summary.scanned += 1;
                if self.enqueue_placed_segment_shard_repair(
                    repair.work_item.request,
                    repair.work_item.shard_index,
                ) {
                    summary.enqueued += 1;
                }
            }
        }
        Ok(summary)
    }

    pub fn list_placed_segment_shard_repairs(
        &self,
        data_pg_id: u32,
    ) -> Result<Vec<PlacedSegmentShardRepairRecord>, StoreError> {
        let pg_id = PgId::new(data_pg_id);
        self.metadata_pg_primary_shard_ack_client(pg_id)?
            .list_placed_segment_shard_repairs(pg_id)
    }

    pub fn acquire_placed_segment_shard_repair_claim(
        &self,
        data_pg_id: u32,
        params: &PlacedSegmentShardRepairClaimAcquireParams,
    ) -> Result<Option<PlacedSegmentShardRepairClaimRecord>, StoreError> {
        let pg_id = PgId::new(data_pg_id);
        let client = self.metadata_pg_primary_shard_ack_client(pg_id)?;
        let request = PlacedSegmentShardRepairClaimAcquire {
            claim_id: params.claim_id.clone(),
            owner_token: params.owner_token.clone(),
            cluster_epoch: self.cluster_epoch(),
            claimed_at: params.claimed_at,
            lease_deadline: params.lease_deadline,
            now: params.now,
        };
        client.acquire_placed_segment_shard_repair_claim(pg_id, &request)
    }

    pub fn complete_placed_segment_shard_repair_claim(
        &self,
        claim: &PlacedSegmentShardRepairClaimRecord,
    ) -> Result<bool, StoreError> {
        let pg_id = PgId::new(claim.work_item.request.data_pg_id);
        if claim.cluster_epoch != self.cluster_epoch() {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: claim.cluster_epoch,
                current_epoch: self.cluster_epoch(),
            });
        }
        self.metadata_pg_primary_shard_ack_client(pg_id)?
            .complete_placed_segment_shard_repair_claim(pg_id, self.cluster_epoch(), claim)
    }

    pub fn record_placed_segment_shard_repair_claim_error(
        &self,
        claim: &PlacedSegmentShardRepairClaimRecord,
        last_error: &str,
        next_attempt_after: u64,
    ) -> Result<bool, StoreError> {
        let pg_id = PgId::new(claim.work_item.request.data_pg_id);
        if claim.cluster_epoch != self.cluster_epoch() {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: claim.cluster_epoch,
                current_epoch: self.cluster_epoch(),
            });
        }
        self.metadata_pg_primary_shard_ack_client(pg_id)?
            .record_placed_segment_shard_repair_claim_error(
                pg_id,
                self.cluster_epoch(),
                claim,
                last_error,
                next_attempt_after,
            )
    }

    pub fn record_placed_segment_shard_backfill(
        &self,
        work_item: &PlacedSegmentShardBackfillWorkItem,
        last_error: Option<&str>,
    ) -> Result<(), StoreError> {
        self.record_placed_segment_shard_backfill_with_remaining_tolerance(
            work_item,
            work_item.request.ec.m,
            last_error,
        )
    }

    pub fn record_placed_segment_shard_backfill_with_remaining_tolerance(
        &self,
        work_item: &PlacedSegmentShardBackfillWorkItem,
        remaining_tolerance: u8,
        last_error: Option<&str>,
    ) -> Result<(), StoreError> {
        let pg_id = PgId::new(work_item.request.data_pg_id);
        self.metadata_pg_primary_shard_ack_client(pg_id)?
            .record_placed_segment_shard_backfill(pg_id, work_item, remaining_tolerance, last_error)
    }

    pub fn list_placed_segment_shard_backfills(
        &self,
        data_pg_id: u32,
    ) -> Result<Vec<PlacedSegmentShardBackfillRecord>, StoreError> {
        let pg_id = PgId::new(data_pg_id);
        self.metadata_pg_primary_shard_ack_client(pg_id)?
            .list_placed_segment_shard_backfills(pg_id)
    }

    pub fn placed_segment_shard_backfill_exists(
        &self,
        work_item: &PlacedSegmentShardBackfillWorkItem,
    ) -> Result<bool, StoreError> {
        let pg_id = PgId::new(work_item.request.data_pg_id);
        self.metadata_pg_primary_shard_ack_client(pg_id)?
            .placed_segment_shard_backfill_exists(pg_id, work_item)
    }

    pub fn placed_segment_shard_backfill_backlog_depth(&self) -> Result<usize, StoreError> {
        let mut depth = 0usize;
        for route in self.local_pg_routes() {
            if route.state() != PgState::Active {
                continue;
            }
            let pg_id = route.pg_id();
            depth = depth.saturating_add(
                self.metadata_pg_primary_shard_ack_client(pg_id)?
                    .count_placed_segment_shard_backfills(pg_id)?,
            );
        }
        Ok(depth)
    }

    pub fn enqueue_placed_segment_shard_backfills_from_scavenger_references(
        &self,
    ) -> Result<PlacedSegmentShardBackfillCandidateEnqueueSummary, StoreError> {
        self.enqueue_placed_segment_shard_backfills_from_scavenger_references_with_limit(
            PLACED_SEGMENT_SHARD_BACKFILL_CANDIDATE_SCAN_LIMIT,
        )
    }

    fn enqueue_placed_segment_shard_backfills_from_scavenger_references_with_limit(
        &self,
        scan_limit: usize,
    ) -> Result<PlacedSegmentShardBackfillCandidateEnqueueSummary, StoreError> {
        let mut summary = PlacedSegmentShardBackfillCandidateEnqueueSummary::default();
        let desired_epoch = self.operation_epoch();
        let mut seen_candidates = HashSet::new();
        let mut verified_candidates = 0usize;
        for route in self.local_pg_routes() {
            if route.state() != PgState::Active {
                continue;
            }
            let node = match self
                .local_map
                .metadata_pg_primary_node(self.operation_epoch(), route.pg_id())
            {
                Ok(node) => node,
                Err(error) if shard_backfill_candidate_error_is_deferred(&error) => {
                    summary.deferred += 1;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let references = match node
                .shard_scavenger_client()
                .list_shard_scavenger_payload_references(route.pg_id())
            {
                Ok(references) => references,
                Err(error) if shard_backfill_candidate_error_is_deferred(&error) => {
                    summary.deferred += 1;
                    continue;
                }
                Err(error) => return Err(error),
            };
            for reference in references {
                let Some((request, source_epoch)) =
                    self.backfill_candidate_from_scavenger_reference(reference)
                else {
                    continue;
                };
                if !seen_candidates.insert((request, source_epoch)) {
                    continue;
                }
                summary.scanned += 1;
                if source_epoch == desired_epoch {
                    summary.current_epoch += 1;
                    continue;
                }
                let work_item = PlacedSegmentShardBackfillWorkItem {
                    request,
                    source_cluster_epoch: source_epoch,
                    desired_cluster_epoch: desired_epoch,
                };
                match self.placed_segment_shard_backfill_exists(&work_item) {
                    Ok(true) => {
                        summary.already_queued += 1;
                        continue;
                    }
                    Ok(false) => {}
                    Err(error) => {
                        note_shard_backfill_candidate_error(&mut summary, &error);
                        continue;
                    }
                }
                if verified_candidates >= scan_limit {
                    summary.limit_reached = true;
                    return Ok(summary);
                }
                verified_candidates += 1;
                let pg_id = PgId::new(request.data_pg_id);
                let source_route = match self.reconstructed_pg_route_at_epoch(pg_id, source_epoch) {
                    Ok(route) => route,
                    Err(error) => {
                        note_shard_backfill_candidate_error(&mut summary, &error);
                        continue;
                    }
                };
                let desired_route = match self.reconstructed_pg_route_at_epoch(pg_id, desired_epoch)
                {
                    Ok(route) => route,
                    Err(error) => {
                        note_shard_backfill_candidate_error(&mut summary, &error);
                        continue;
                    }
                };
                match self.placed_segment_payload_shard_locations_are_equal(
                    &source_route,
                    &desired_route,
                    request,
                ) {
                    Ok(true) => {
                        summary.already_complete += 1;
                        continue;
                    }
                    Ok(false) => {}
                    Err(error) => {
                        note_shard_backfill_candidate_error(&mut summary, &error);
                        continue;
                    }
                }
                let plan = match self.placed_segment_payload_shard_backfill_plan(
                    &source_route,
                    &desired_route,
                    request,
                ) {
                    Ok(plan) => plan,
                    Err(error) => {
                        note_shard_backfill_candidate_error(&mut summary, &error);
                        continue;
                    }
                };
                if !plan.unrecoverable_targets.is_empty() {
                    summary.unrecoverable += 1;
                    continue;
                }
                if plan.is_complete() {
                    summary.already_complete += 1;
                    continue;
                }
                if self
                    .record_placed_segment_shard_backfill_with_remaining_tolerance(
                        &work_item,
                        plan.source_remaining_tolerance(),
                        None,
                    )
                    .map_err(|error| note_shard_backfill_candidate_error(&mut summary, &error))
                    .is_err()
                {
                    continue;
                }
                summary.enqueued += 1;
            }
        }
        Ok(summary)
    }

    fn placed_segment_payload_shard_locations_are_equal(
        &self,
        source_route: &PgRouteSnapshot,
        desired_route: &PgRouteSnapshot,
        req: SegmentStoredBytesRequest,
    ) -> Result<bool, StoreError> {
        let data_pg = DataPgId::new(PgId::new(req.data_pg_id));
        let placement_key = segment_payload_placement_key(&req.segment_okh, req.segment_vid);
        let source_locations = self
            .place_payload_shards_for_pg_route_snapshot(
                source_route,
                data_pg,
                req.ec,
                &placement_key,
            )
            .map_err(cluster_build_error_to_store)?;
        let desired_locations = self
            .place_payload_shards_for_pg_route_snapshot(
                desired_route,
                data_pg,
                req.ec,
                &placement_key,
            )
            .map_err(cluster_build_error_to_store)?;
        Ok(source_locations.len() == desired_locations.len()
            && source_locations
                .iter()
                .zip(desired_locations.iter())
                .all(|(source, desired)| {
                    source.data_pg_id() == desired.data_pg_id()
                        && source.shard_index() == desired.shard_index()
                        && source.node_id() == desired.node_id()
                }))
    }

    fn backfill_candidate_from_scavenger_reference(
        &self,
        reference: ShardScavengerPayloadReference,
    ) -> Option<(SegmentStoredBytesRequest, ClusterEpoch)> {
        match reference {
            ShardScavengerPayloadReference::Placed(reference) => Some((
                SegmentStoredBytesRequest {
                    data_pg_id: reference.data_pg_id,
                    segment_okh: reference.okh,
                    segment_vid: reference.generation_id,
                    stored_size: reference.stored_size as usize,
                    segment_crc64: reference.crc64,
                    ec: reference.ec,
                },
                reference.placement_cluster_epoch,
            )),
            ShardScavengerPayloadReference::RoutedMultipartPart(reference) => {
                let data_pg_id = self
                    .local_map
                    .object_generation_multipart_part_data_pg(
                        &reference.bucket,
                        &reference.key,
                        reference.object_generation_id,
                        reference.part_number,
                    )
                    .get();
                Some((
                    SegmentStoredBytesRequest {
                        data_pg_id,
                        segment_okh: reference.part_okh,
                        segment_vid: reference.part_vid,
                        stored_size: reference.stored_size as usize,
                        segment_crc64: reference.crc64,
                        ec: reference.ec,
                    },
                    reference.placement_cluster_epoch,
                ))
            }
            ShardScavengerPayloadReference::ReclaimOnly(_) => None,
        }
    }

    pub fn acquire_placed_segment_shard_backfill_claim(
        &self,
        data_pg_id: u32,
        params: &PlacedSegmentShardBackfillClaimAcquireParams,
    ) -> Result<Option<PlacedSegmentShardBackfillClaimRecord>, StoreError> {
        let pg_id = PgId::new(data_pg_id);
        let client = self.metadata_pg_primary_shard_ack_client(pg_id)?;
        let request = PlacedSegmentShardBackfillClaimAcquire {
            claim_id: params.claim_id.clone(),
            owner_token: params.owner_token.clone(),
            cluster_epoch: self.cluster_epoch(),
            claimed_at: params.claimed_at,
            lease_deadline: params.lease_deadline,
            now: params.now,
        };
        client.acquire_placed_segment_shard_backfill_claim(pg_id, &request)
    }

    pub fn acquire_next_placed_segment_shard_backfill_claim(
        &self,
        params: &PlacedSegmentShardBackfillClaimAcquireParams,
    ) -> Result<Option<PlacedSegmentShardBackfillClaimRecord>, StoreError> {
        for route in self.local_pg_routes() {
            if route.state() != PgState::Active {
                continue;
            }
            if let Some(claim) =
                self.acquire_placed_segment_shard_backfill_claim(route.pg_id().get(), params)?
            {
                return Ok(Some(claim));
            }
        }
        Ok(None)
    }

    pub fn complete_placed_segment_shard_backfill_claim(
        &self,
        claim: &PlacedSegmentShardBackfillClaimRecord,
    ) -> Result<bool, StoreError> {
        let pg_id = PgId::new(claim.work_item.request.data_pg_id);
        if claim.cluster_epoch != self.cluster_epoch() {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: claim.cluster_epoch,
                current_epoch: self.cluster_epoch(),
            });
        }
        self.metadata_pg_primary_shard_ack_client(pg_id)?
            .complete_placed_segment_shard_backfill_claim(pg_id, self.cluster_epoch(), claim)
    }

    pub fn record_placed_segment_shard_backfill_claim_error(
        &self,
        claim: &PlacedSegmentShardBackfillClaimRecord,
        last_error: &str,
        next_attempt_after: u64,
    ) -> Result<bool, StoreError> {
        let pg_id = PgId::new(claim.work_item.request.data_pg_id);
        if claim.cluster_epoch != self.cluster_epoch() {
            return Err(StoreError::StalePayloadOperation {
                pg_id: pg_id.get(),
                operation_epoch: claim.cluster_epoch,
                current_epoch: self.cluster_epoch(),
            });
        }
        self.metadata_pg_primary_shard_ack_client(pg_id)?
            .record_placed_segment_shard_backfill_claim_error(
                pg_id,
                self.cluster_epoch(),
                claim,
                last_error,
                next_attempt_after,
            )
    }

    pub fn resolve_placed_segment_shard_backfill(
        &self,
        work_item: &PlacedSegmentShardBackfillWorkItem,
    ) -> Result<(), StoreError> {
        let pg_id = PgId::new(work_item.request.data_pg_id);
        self.metadata_pg_primary_shard_ack_client(pg_id)?
            .resolve_placed_segment_shard_backfill(pg_id, work_item)
    }

    pub fn repair_placed_segment_payload_shard(
        &self,
        req: SegmentStoredBytesRequest,
        shard_index: ShardIndex,
    ) -> Result<WrittenShardAck, StoreError> {
        let mut repaired = self.repair_placed_segment_payload_shards(req, &[shard_index])?;
        repaired
            .pop()
            .ok_or_else(|| StoreError::PayloadShardSetMismatch {
                reason: "single-shard repair produced no shard ack".to_string(),
            })
    }

    pub fn placed_segment_payload_shard_repair_targets(
        &self,
        req: SegmentStoredBytesRequest,
    ) -> Result<Vec<ShardIndex>, StoreError> {
        let health = self.placed_segment_payload_shard_health(req)?;
        if matches!(health.risk, PlacedSegmentShardSetRisk::Unrecoverable) {
            return Err(StoreError::NotFound);
        }
        Ok(health.repair_targets())
    }

    pub fn placed_segment_payload_shard_health(
        &self,
        req: SegmentStoredBytesRequest,
    ) -> Result<PlacedSegmentShardSetHealth, StoreError> {
        self.require_current_payload_operation_epoch(req.data_pg_id)?;
        validate_placed_segment_repair_ec_shape(req.ec)?;
        let data_pg = DataPgId::new(PgId::new(req.data_pg_id));
        let placement_key = segment_payload_placement_key(&req.segment_okh, req.segment_vid);
        let locations = self
            .place_payload_shards(data_pg, req.ec, &placement_key)
            .map_err(cluster_build_error_to_store)?;
        self.placed_segment_payload_shard_health_at_locations(
            req,
            &locations,
            PlacedSegmentShardHealthReadMode::CurrentRoute,
            None,
        )
    }

    pub fn placed_segment_payload_shard_health_for_pg_route_snapshot(
        &self,
        route: &PgRouteSnapshot,
        req: SegmentStoredBytesRequest,
    ) -> Result<PlacedSegmentShardSetHealth, StoreError> {
        validate_placed_segment_repair_ec_shape(req.ec)?;
        let data_pg = DataPgId::new(PgId::new(req.data_pg_id));
        let placement_key = segment_payload_placement_key(&req.segment_okh, req.segment_vid);
        let locations = self
            .place_payload_shards_for_pg_route_snapshot(route, data_pg, req.ec, &placement_key)
            .map_err(cluster_build_error_to_store)?;
        self.placed_segment_payload_shard_health_at_locations(
            req,
            &locations,
            PlacedSegmentShardHealthReadMode::HistoricalInspection,
            Some(route),
        )
    }

    pub fn placed_segment_payload_shard_backfill_plan(
        &self,
        source_route: &PgRouteSnapshot,
        desired_route: &PgRouteSnapshot,
        req: SegmentStoredBytesRequest,
    ) -> Result<PlacedSegmentShardBackfillPlan, StoreError> {
        let source_health =
            self.placed_segment_payload_shard_health_for_pg_route_snapshot(source_route, req)?;
        let desired_health =
            self.placed_segment_payload_shard_health_for_pg_route_snapshot(desired_route, req)?;
        build_placed_segment_shard_backfill_plan(source_health, desired_health)
    }

    pub fn record_placed_segment_shard_backfill_for_plan(
        &self,
        source_route: &PgRouteSnapshot,
        desired_route: &PgRouteSnapshot,
        req: SegmentStoredBytesRequest,
        last_error: Option<&str>,
    ) -> Result<PlacedSegmentShardBackfillPlan, StoreError> {
        let plan =
            self.placed_segment_payload_shard_backfill_plan(source_route, desired_route, req)?;
        if !plan.unrecoverable_targets.is_empty() {
            return Err(StoreError::PayloadShardSetMismatch {
                reason: format!(
                    "placed segment backfill plan cannot enqueue unrecoverable targets {:?}",
                    plan.unrecoverable_targets
                ),
            });
        }
        if plan.is_complete() {
            return Ok(plan);
        }
        let work_item = PlacedSegmentShardBackfillWorkItem {
            request: req,
            source_cluster_epoch: source_route.cluster_epoch(),
            desired_cluster_epoch: desired_route.cluster_epoch(),
        };
        self.record_placed_segment_shard_backfill_with_remaining_tolerance(
            &work_item,
            plan.source_remaining_tolerance(),
            last_error,
        )?;
        Ok(plan)
    }

    pub fn backfill_placed_segment_payload_shard_direct_copies(
        &self,
        source_route: &PgRouteSnapshot,
        desired_route: &PgRouteSnapshot,
        req: SegmentStoredBytesRequest,
    ) -> Result<Vec<WrittenShardAck>, StoreError> {
        let plan =
            self.placed_segment_payload_shard_backfill_plan(source_route, desired_route, req)?;
        if !plan.unrecoverable_targets.is_empty() {
            return Err(StoreError::PayloadShardSetMismatch {
                reason: format!(
                    "placed segment direct-copy backfill cannot satisfy unrecoverable targets {:?}",
                    plan.unrecoverable_targets
                ),
            });
        }
        if !plan.reconstruction_targets.is_empty() {
            return Err(StoreError::PayloadShardSetMismatch {
                reason:
                    "placed segment direct-copy backfill cannot satisfy EC reconstruction targets"
                        .to_string(),
            });
        }
        if plan.copy_targets.is_empty() {
            return Ok(Vec::new());
        }

        let mut copied = Vec::with_capacity(plan.copy_targets.len());
        for target in &plan.copy_targets {
            let ack = self.load_payload_shard_ack_for_pg_route_snapshot(
                source_route,
                req.data_pg_id,
                &target.shard_key,
            )?;
            let payload = self
                .read_payload_shard_for_historical_inspection(target.source, &target.shard_key, ack)
                .map_err(shard_io_error_to_store)?;
            let copied_ack = self
                .repair_payload_shard(target.destination, &target.shard_key, &payload)
                .map_err(shard_io_error_to_store)?;
            copied.push(WrittenShardAck {
                key: target.shard_key.clone(),
                ack: copied_ack,
            });
        }

        let copied_acks: Vec<_> = copied
            .iter()
            .map(|written| (&written.key, written.ack))
            .collect();
        self.register_payload_shard_acks(req.data_pg_id, &copied_acks)
            .map_err(|error| match error {
                ObjectPgActionError::Store(error) => error,
                other => StoreError::Io {
                    context: "register backfilled payload shard acks",
                    source: io::Error::other(other.to_string()),
                },
            })?;
        let desired_health =
            self.placed_segment_payload_shard_health_for_pg_route_snapshot(desired_route, req)?;
        for target in &plan.copy_targets {
            let Some(shard) = desired_health
                .shards
                .iter()
                .find(|shard| shard.shard_index == target.shard_index)
            else {
                return Err(StoreError::PayloadShardSetMismatch {
                    reason: format!(
                        "backfilled shard index {} missing from desired health",
                        target.shard_index.get()
                    ),
                });
            };
            if !shard.validation.is_valid() {
                return Err(StoreError::PayloadShardSetMismatch {
                    reason: format!(
                        "backfilled shard index {} still fails desired-route verification",
                        target.shard_index.get()
                    ),
                });
            }
        }
        Ok(copied)
    }

    pub fn backfill_placed_segment_payload_shards(
        &self,
        source_route: &PgRouteSnapshot,
        desired_route: &PgRouteSnapshot,
        req: SegmentStoredBytesRequest,
    ) -> Result<Vec<WrittenShardAck>, StoreError> {
        let plan =
            self.placed_segment_payload_shard_backfill_plan(source_route, desired_route, req)?;
        if !plan.unrecoverable_targets.is_empty() {
            return Err(StoreError::PayloadShardSetMismatch {
                reason: format!(
                    "placed segment backfill cannot satisfy unrecoverable targets {:?}",
                    plan.unrecoverable_targets
                ),
            });
        }
        if plan.copy_targets.is_empty() && plan.reconstruction_targets.is_empty() {
            return Ok(Vec::new());
        }

        let mut reconstructed_segment = None;
        if !plan.reconstruction_targets.is_empty() {
            let mut segment = Vec::new();
            if !self.try_read_placed_segment_stored_bytes_for_pg_route_snapshot_into(
                source_route,
                req,
                &mut segment,
            )? {
                return Err(StoreError::NotFound);
            }
            reconstructed_segment = Some(segment);
        }

        let mut backfilled =
            Vec::with_capacity(plan.copy_targets.len() + plan.reconstruction_targets.len());
        for target in &plan.copy_targets {
            let ack = self.load_payload_shard_ack_for_pg_route_snapshot(
                source_route,
                req.data_pg_id,
                &target.shard_key,
            )?;
            let payload = self
                .read_payload_shard_for_historical_inspection(target.source, &target.shard_key, ack)
                .map_err(shard_io_error_to_store)?;
            let copied_ack = self
                .repair_payload_shard(target.destination, &target.shard_key, &payload)
                .map_err(shard_io_error_to_store)?;
            backfilled.push(WrittenShardAck {
                key: target.shard_key.clone(),
                ack: copied_ack,
            });
        }

        if !plan.reconstruction_targets.is_empty() {
            let target_slots: Vec<_> = plan
                .reconstruction_targets
                .iter()
                .map(|shard_index| {
                    let desired = plan
                        .desired_health
                        .shards
                        .iter()
                        .find(|shard| shard.shard_index == *shard_index)
                        .ok_or_else(|| StoreError::PayloadShardSetMismatch {
                            reason: format!(
                                "backfill reconstruction shard index {} missing from desired health",
                                shard_index.get()
                            ),
                        })?;
                    Ok((usize::from(shard_index.get()), desired.location))
                })
                .collect::<Result<Vec<_>, StoreError>>()?;
            let reconstructed =
                self.local_map.write_erasure_coded_segment_shards_with(
                    &req.segment_okh,
                    req.segment_vid,
                    reconstructed_segment.as_ref().ok_or_else(|| {
                        StoreError::PayloadShardSetMismatch {
                            reason:
                                "backfill reconstruction missing reconstructed source segment"
                                    .to_string(),
                        }
                    })?,
                    req.ec,
                    |shard_batch| {
                        let mut written = Vec::with_capacity(target_slots.len());
                        for (slot, target_location) in &target_slots {
                            let (shard_key, shard_payload) =
                                shard_batch.get(*slot).ok_or_else(|| {
                                    StoreError::PayloadShardSetMismatch {
                                        reason: format!(
                                            "backfill reconstruction shard index {} outside encoded shard batch of {}",
                                            slot,
                                            shard_batch.len()
                                        ),
                                    }
                                })?;
                            let ack = self
                                .repair_payload_shard(*target_location, shard_key, shard_payload)
                                .map_err(shard_io_error_to_store)?;
                            written.push((shard_key.clone(), ack));
                        }
                        Ok(written)
                    },
                )?;
            backfilled.extend(reconstructed);
        }

        let backfilled_acks: Vec<_> = backfilled
            .iter()
            .map(|written| (&written.key, written.ack))
            .collect();
        self.register_payload_shard_acks(req.data_pg_id, &backfilled_acks)
            .map_err(|error| match error {
                ObjectPgActionError::Store(error) => error,
                other => StoreError::Io {
                    context: "register backfilled payload shard acks",
                    source: io::Error::other(other.to_string()),
                },
            })?;
        let target_indices: Vec<_> = plan
            .copy_targets
            .iter()
            .map(|target| target.shard_index)
            .chain(plan.reconstruction_targets.iter().copied())
            .collect();
        self.verify_backfilled_placed_segment_payload_shards(desired_route, req, &target_indices)?;
        Ok(backfilled)
    }

    pub fn backfill_placed_segment_payload_shards_for_work_item(
        &self,
        work_item: &PlacedSegmentShardBackfillWorkItem,
    ) -> Result<Vec<WrittenShardAck>, StoreError> {
        let pg_id = PgId::new(work_item.request.data_pg_id);
        let source_route =
            self.reconstructed_pg_route_at_epoch(pg_id, work_item.source_cluster_epoch)?;
        // Backfill rows capture the desired global epoch observed by the scanner. Later
        // unrelated PG changes can supersede that epoch while this PG's target route is
        // still the current desired placement, so execute toward the current route once
        // this handle has caught up to the recorded desired epoch.
        let desired_epoch = if self.operation_epoch() >= work_item.desired_cluster_epoch {
            self.operation_epoch()
        } else {
            work_item.desired_cluster_epoch
        };
        let desired_route = self.reconstructed_pg_route_at_epoch(pg_id, desired_epoch)?;
        self.backfill_placed_segment_payload_shards(
            &source_route,
            &desired_route,
            work_item.request,
        )
    }

    fn verify_backfilled_placed_segment_payload_shards(
        &self,
        desired_route: &PgRouteSnapshot,
        req: SegmentStoredBytesRequest,
        backfilled_shard_indices: &[ShardIndex],
    ) -> Result<(), StoreError> {
        let desired_health =
            self.placed_segment_payload_shard_health_for_pg_route_snapshot(desired_route, req)?;
        for target in backfilled_shard_indices {
            let Some(shard) = desired_health
                .shards
                .iter()
                .find(|shard| shard.shard_index == *target)
            else {
                return Err(StoreError::PayloadShardSetMismatch {
                    reason: format!(
                        "backfilled shard index {} missing from desired health",
                        target.get()
                    ),
                });
            };
            if !shard.validation.is_valid() {
                return Err(StoreError::PayloadShardSetMismatch {
                    reason: format!(
                        "backfilled shard index {} still fails desired-route verification",
                        target.get()
                    ),
                });
            }
        }
        Ok(())
    }

    fn placed_segment_payload_shard_health_at_locations(
        &self,
        req: SegmentStoredBytesRequest,
        locations: &[ShardLocation],
        read_mode: PlacedSegmentShardHealthReadMode,
        historical_route: Option<&PgRouteSnapshot>,
    ) -> Result<PlacedSegmentShardSetHealth, StoreError> {
        let ec_config = validate_placed_segment_repair_ec_shape(req.ec)?;
        let k = usize::from(req.ec.k);
        let padded = req.stored_size.div_ceil(k) * k;
        let shard_size = padded / k;
        let mut shards = Vec::with_capacity(ec_config.total_shards());
        let mut valid_shards = 0usize;

        for shard_index in 0..ec_config.total_shards() as u8 {
            let shard_key = ShardKey::new(&req.segment_okh, req.segment_vid.get(), shard_index);
            let location = locations
                .get(usize::from(shard_index))
                .copied()
                .ok_or_else(|| StoreError::PayloadShardSetMismatch {
                    reason: format!(
                        "inspect shard index {} outside {} placed shards",
                        shard_index,
                        locations.len()
                    ),
                })?;
            let ack_result = match historical_route {
                Some(route) => self.load_payload_shard_ack_for_pg_route_snapshot(
                    route,
                    req.data_pg_id,
                    &shard_key,
                ),
                None => self.load_payload_shard_ack(req.data_pg_id, &shard_key),
            };
            let validation = match ack_result {
                Ok(ack) if ack.stored_size == shard_size as u64 => {
                    let read_result = match read_mode {
                        PlacedSegmentShardHealthReadMode::CurrentRoute => {
                            self.read_payload_shard(location, &shard_key, ack)
                        }
                        PlacedSegmentShardHealthReadMode::HistoricalInspection => self
                            .read_payload_shard_for_historical_inspection(
                                location, &shard_key, ack,
                            ),
                    };
                    match read_result {
                        Ok(_) => {
                            valid_shards += 1;
                            PlacedSegmentShardValidation::Valid
                        }
                        Err(error) => {
                            let reason = error.to_string();
                            placed_segment_recoverable_shard_error(error)?;
                            PlacedSegmentShardValidation::Unreadable { reason }
                        }
                    }
                }
                Ok(ack) => PlacedSegmentShardValidation::WrongSize {
                    expected: shard_size as u64,
                    actual: ack.stored_size,
                },
                Err(StoreError::NotFound) => PlacedSegmentShardValidation::MissingAck,
                Err(error) => return Err(error),
            };
            shards.push(PlacedSegmentShardHealth {
                shard_index: ShardIndex::new(shard_index),
                shard_key,
                location,
                validation,
            });
        }

        let risk = if valid_shards == ec_config.total_shards() {
            PlacedSegmentShardSetRisk::Healthy
        } else if valid_shards >= k {
            PlacedSegmentShardSetRisk::Degraded {
                tolerance_remaining: valid_shards - k,
            }
        } else {
            PlacedSegmentShardSetRisk::Unrecoverable
        };

        Ok(PlacedSegmentShardSetHealth {
            total_shards: ec_config.total_shards(),
            required_shards: k,
            valid_shards,
            risk,
            shards,
        })
    }

    pub fn repair_placed_segment_payload_shards_if_needed(
        &self,
        req: SegmentStoredBytesRequest,
    ) -> Result<Vec<WrittenShardAck>, StoreError> {
        let repair_targets = self.placed_segment_payload_shard_repair_targets(req)?;
        self.repair_placed_segment_payload_shards_inner(req, &repair_targets, true)
    }

    pub fn repair_placed_segment_payload_shards_if_needed_preserving_repair_rows(
        &self,
        req: SegmentStoredBytesRequest,
    ) -> Result<Vec<WrittenShardAck>, StoreError> {
        let repair_targets = self.placed_segment_payload_shard_repair_targets(req)?;
        self.repair_placed_segment_payload_shards_inner(req, &repair_targets, false)
    }

    pub fn repair_placed_segment_payload_shards(
        &self,
        req: SegmentStoredBytesRequest,
        shard_indices: &[ShardIndex],
    ) -> Result<Vec<WrittenShardAck>, StoreError> {
        self.repair_placed_segment_payload_shards_inner(req, shard_indices, true)
    }

    fn repair_placed_segment_payload_shards_inner(
        &self,
        req: SegmentStoredBytesRequest,
        shard_indices: &[ShardIndex],
        resolve_repair_rows: bool,
    ) -> Result<Vec<WrittenShardAck>, StoreError> {
        let total_shards =
            req.ec
                .k
                .checked_add(req.ec.m)
                .ok_or_else(|| StoreError::PayloadShardSetMismatch {
                    reason: format!("EC shard count overflow for {}+{}", req.ec.k, req.ec.m),
                })?;
        if shard_indices.len() > usize::from(req.ec.m) {
            return Err(StoreError::PayloadShardSetMismatch {
                reason: format!(
                    "repair requested {} shards, but EC {}+{} can tolerate at most {}",
                    shard_indices.len(),
                    req.ec.k,
                    req.ec.m,
                    req.ec.m
                ),
            });
        }
        let mut seen = HashSet::with_capacity(shard_indices.len());
        for shard_index in shard_indices {
            if shard_index.get() >= total_shards {
                return Err(StoreError::PayloadShardSetMismatch {
                    reason: format!(
                        "repair shard index {} outside EC {}+{}",
                        shard_index.get(),
                        req.ec.k,
                        req.ec.m
                    ),
                });
            }
            if !seen.insert(shard_index.get()) {
                return Err(StoreError::PayloadShardSetMismatch {
                    reason: format!("duplicate repair shard index {}", shard_index.get()),
                });
            }
        }
        if shard_indices.is_empty() {
            return Ok(Vec::new());
        }

        let mut recovered_segment = Vec::new();
        match self.try_read_placed_segment_stored_bytes_into(req, &mut recovered_segment, false)? {
            true => {}
            false => return Err(StoreError::NotFound),
        }

        let data_pg = DataPgId::new(PgId::new(req.data_pg_id));
        let placement_key = segment_payload_placement_key(&req.segment_okh, req.segment_vid);
        let locations = self
            .place_payload_shards(data_pg, req.ec, &placement_key)
            .map_err(cluster_build_error_to_store)?;
        let target_slots: Vec<(usize, ShardLocation)> = shard_indices
            .iter()
            .map(|shard_index| {
                let slot = usize::from(shard_index.get());
                let location = locations.get(slot).copied().ok_or_else(|| {
                    StoreError::PayloadShardSetMismatch {
                        reason: format!(
                            "repair shard index {} outside {} placed shards",
                            shard_index.get(),
                            locations.len()
                        ),
                    }
                })?;
                Ok((slot, location))
            })
            .collect::<Result<_, StoreError>>()?;

        let repaired = self.local_map.write_erasure_coded_segment_shards_with(
            &req.segment_okh,
            req.segment_vid,
            &recovered_segment,
            req.ec,
            |shard_batch| {
                let mut repaired = Vec::with_capacity(target_slots.len());
                for (slot, target_location) in &target_slots {
                    let (shard_key, shard_payload) = shard_batch.get(*slot).ok_or_else(|| {
                        StoreError::PayloadShardSetMismatch {
                            reason: format!(
                                "repair shard index {} outside encoded shard batch of {}",
                                slot,
                                shard_batch.len()
                            ),
                        }
                    })?;
                    let ack = self
                        .repair_payload_shard(*target_location, shard_key, shard_payload)
                        .map_err(shard_io_error_to_store)?;
                    repaired.push((shard_key.clone(), ack));
                }
                Ok(repaired)
            },
        )?;
        let repaired_acks: Vec<_> = repaired
            .iter()
            .map(|repaired| (&repaired.key, repaired.ack))
            .collect();
        self.register_payload_shard_acks(req.data_pg_id, &repaired_acks)
            .map_err(|error| match error {
                ObjectPgActionError::Store(error) => error,
                other => StoreError::Io {
                    context: "register repaired payload shard acks",
                    source: io::Error::other(other.to_string()),
                },
            })?;
        self.verify_repaired_placed_segment_payload_shards(req, shard_indices)?;
        if resolve_repair_rows {
            for shard_index in shard_indices {
                self.resolve_placed_segment_shard_repair(req, *shard_index)?;
            }
        }
        Ok(repaired)
    }

    fn verify_repaired_placed_segment_payload_shards(
        &self,
        req: SegmentStoredBytesRequest,
        repaired_shard_indices: &[ShardIndex],
    ) -> Result<(), StoreError> {
        let remaining_targets = self.placed_segment_payload_shard_repair_targets(req)?;
        let repaired: HashSet<_> = repaired_shard_indices
            .iter()
            .map(|shard_index| shard_index.get())
            .collect();
        for shard_index in &remaining_targets {
            if repaired.contains(&shard_index.get()) {
                return Err(StoreError::PayloadShardSetMismatch {
                    reason: format!(
                        "repaired shard index {} still fails full-set verification",
                        shard_index.get()
                    ),
                });
            }
        }
        for shard_index in remaining_targets {
            self.schedule_placed_segment_shard_repair(req, shard_index)?;
        }
        Ok(())
    }

    fn try_read_placed_segment_stored_bytes_into(
        &self,
        req: SegmentStoredBytesRequest,
        dst: &mut Vec<u8>,
        schedule_repair_on_recovery: bool,
    ) -> Result<bool, StoreError> {
        let k = req.ec.k as usize;
        let padded = req.stored_size.div_ceil(k) * k;
        let shard_size = padded / k;

        if shard_size == 0 {
            dst.clear();
            return Ok(true);
        }

        if self.try_read_placed_segment_direct_into(req, dst)? {
            return Ok(true);
        }

        self.try_read_placed_segment_recovery_into(req, dst, schedule_repair_on_recovery)
    }

    fn try_read_placed_segment_stored_bytes_for_pg_route_snapshot_into(
        &self,
        route: &PgRouteSnapshot,
        req: SegmentStoredBytesRequest,
        dst: &mut Vec<u8>,
    ) -> Result<bool, StoreError> {
        let ec_config = validate_placed_segment_repair_ec_shape(req.ec)?;
        let k = usize::from(req.ec.k);
        let total_shards = ec_config.total_shards();
        let padded = req.stored_size.div_ceil(k) * k;
        let shard_size = padded / k;

        if shard_size == 0 {
            dst.clear();
            return Ok(true);
        }

        let data_pg = DataPgId::new(PgId::new(req.data_pg_id));
        let placement_key = segment_payload_placement_key(&req.segment_okh, req.segment_vid);
        let locations = self
            .place_payload_shards_for_pg_route_snapshot(route, data_pg, req.ec, &placement_key)
            .map_err(cluster_build_error_to_store)?;
        let mut all_shards = vec![None; total_shards];
        let mut present_count = 0usize;

        for shard_index in 0..k {
            self.try_load_placed_segment_shard_for_historical_inspection(
                route,
                req.data_pg_id,
                &req.segment_okh,
                req.segment_vid,
                &locations,
                shard_index,
                shard_size,
                &mut all_shards,
                &mut present_count,
            )?;
        }

        if present_count < k {
            for shard_index in k..total_shards {
                if present_count >= k {
                    break;
                }
                self.try_load_placed_segment_shard_for_historical_inspection(
                    route,
                    req.data_pg_id,
                    &req.segment_okh,
                    req.segment_vid,
                    &locations,
                    shard_index,
                    shard_size,
                    &mut all_shards,
                    &mut present_count,
                )?;
            }
        }

        if present_count < k {
            return Ok(false);
        }

        let mut recovered = None;
        let mut recovered_ranges = vec![None; k];

        if !(0..k).all(|i| all_shards[i].is_some()) {
            let missing_needed: Vec<usize> = (0..k).filter(|&i| all_shards[i].is_none()).collect();
            let present_indices: Vec<usize> = (0..total_shards)
                .filter(|&i| all_shards[i].is_some())
                .collect();
            let mut present_refs = Vec::with_capacity(present_indices.len());
            for &shard_index in &present_indices {
                let Some(shard) = all_shards.get(shard_index).and_then(Option::as_ref) else {
                    return Err(StoreError::PayloadShardSetMismatch {
                        reason: format!(
                            "historical segment recovery present shard index {} missing payload",
                            shard_index
                        ),
                    });
                };
                present_refs.push(shard.as_slice());
            }
            let codec = erasure_codec_for_shape(req.ec, "build historical segment recovery codec")?;
            let recovered_len = missing_needed.len() * shard_size;
            let mut recovered_buf = vec![0; recovered_len];
            let mut output_refs: Vec<&mut [u8]> = recovered_buf
                .chunks_exact_mut(shard_size)
                .take(missing_needed.len())
                .collect();

            codec
                .reconstruct(
                    &present_indices,
                    &present_refs,
                    &missing_needed,
                    &mut output_refs,
                )
                .map_err(|error| StoreError::ErasureCoding {
                    context: "reconstruct placed segment shards from historical route",
                    reason: error.to_string(),
                })?;

            for (slot, &missing_idx) in missing_needed.iter().enumerate() {
                let start = slot * shard_size;
                recovered_ranges[missing_idx] = Some((start, start + shard_size));
            }
            recovered = Some(recovered_buf);
        }

        dst.clear();
        dst.reserve(padded);
        for (idx, shard) in all_shards.iter().take(k).enumerate() {
            if let Some(shard) = shard.as_ref() {
                dst.extend_from_slice(shard);
            } else if let Some((start, end)) = recovered_ranges[idx] {
                let Some(recovered_buf) = recovered.as_ref() else {
                    return Err(StoreError::PayloadShardSetMismatch {
                        reason: format!(
                            "historical segment recovery missing reconstructed payload for data index {idx}"
                        ),
                    });
                };
                let Some(recovered_shard) = recovered_buf.get(start..end) else {
                    return Err(StoreError::PayloadShardSetMismatch {
                        reason: format!(
                            "historical segment recovery range {start}..{end} outside reconstructed payload length {}",
                            recovered_buf.len()
                        ),
                    });
                };
                dst.extend_from_slice(recovered_shard);
            } else {
                return Err(StoreError::PayloadShardSetMismatch {
                    reason: format!(
                        "historical segment recovery missing reconstructed shard for data index {idx}"
                    ),
                });
            }
        }
        dst.truncate(req.stored_size);
        let actual_crc64 = checksum::crc64::checksum(dst);
        if actual_crc64 != req.segment_crc64 {
            return Err(StoreError::IntegrityError {
                expected: req.segment_crc64,
                actual: actual_crc64,
            });
        }
        Ok(true)
    }

    fn try_read_placed_segment_direct_into(
        &self,
        req: SegmentStoredBytesRequest,
        dst: &mut Vec<u8>,
    ) -> Result<bool, StoreError> {
        let locations = self.segment_payload_locations(&req)?;
        let k = req.ec.k as usize;
        let padded = req.stored_size.div_ceil(k) * k;
        let shard_size = padded / k;
        dst.resize(padded, 0);
        let mut direct_shards = Vec::with_capacity(k);
        for (shard_index, location) in locations.iter().take(k).enumerate() {
            let shard_key =
                ShardKey::new(&req.segment_okh, req.segment_vid.get(), shard_index as u8);
            let ack = match self.load_payload_shard_ack(req.data_pg_id, &shard_key) {
                Ok(ack) => ack,
                Err(StoreError::NotFound) => return Ok(false),
                Err(error) => return Err(error),
            };
            if ack.stored_size != shard_size as u64 {
                return Ok(false);
            }
            direct_shards.push((*location, shard_key, ack));
        }
        let handle_entries: Vec<_> = direct_shards
            .iter()
            .map(|(location, shard_key, _)| (*location, shard_key.clone()))
            .collect();
        let mut read_handles = match self
            .local_map
            .acquire_payload_shard_read_handles(self.operation_epoch(), &handle_entries)
        {
            Ok(read_handles) => read_handles,
            Err(error) => {
                placed_segment_recoverable_shard_error(error)?;
                return Ok(false);
            }
        };

        for (shard_index, (location, shard_key, ack)) in direct_shards.iter().enumerate() {
            let start = shard_index * shard_size;
            let end = start + shard_size;
            if let Err(error) =
                self.maybe_run_before_placed_payload_shard_read_hook(*location, shard_key)
            {
                read_handles.release().map_err(shard_io_error_to_store)?;
                placed_segment_recoverable_shard_error(error)?;
                return Ok(false);
            }
            match self.local_map.read_payload_shard_into_without_handle(
                self.operation_epoch(),
                *location,
                shard_key,
                *ack,
                &mut dst[start..end],
            ) {
                Ok(()) => {}
                Err(error) => {
                    read_handles.release().map_err(shard_io_error_to_store)?;
                    placed_segment_recoverable_shard_error(error)?;
                    return Ok(false);
                }
            }
        }
        read_handles.release().map_err(shard_io_error_to_store)?;

        dst.truncate(req.stored_size);
        let actual_crc64 = checksum::crc64::checksum(dst);
        Ok(actual_crc64 == req.segment_crc64)
    }

    fn try_read_placed_segment_recovery_into(
        &self,
        req: SegmentStoredBytesRequest,
        dst: &mut Vec<u8>,
        schedule_repair_on_recovery: bool,
    ) -> Result<bool, StoreError> {
        let k = req.ec.k as usize;
        let m = req.ec.m as usize;
        let padded = req.stored_size.div_ceil(k) * k;
        let shard_size = padded / k;
        let locations = self.segment_payload_locations(&req)?;
        let mut all_shards = vec![None; k + m];
        let mut present_count = 0usize;
        let mut repair_targets = Vec::new();

        for shard_index in 0..k {
            self.try_load_placed_segment_shard(
                req.data_pg_id,
                &req.segment_okh,
                req.segment_vid,
                &locations,
                shard_index,
                shard_size,
                &mut all_shards,
                &mut present_count,
                Some(&mut repair_targets),
            )?;
        }

        if present_count < k {
            for shard_index in k..(k + m) {
                if present_count >= k {
                    break;
                }
                self.try_load_placed_segment_shard(
                    req.data_pg_id,
                    &req.segment_okh,
                    req.segment_vid,
                    &locations,
                    shard_index,
                    shard_size,
                    &mut all_shards,
                    &mut present_count,
                    Some(&mut repair_targets),
                )?;
            }
        }

        if present_count < k {
            return Ok(false);
        }

        let mut recovered = None;
        let mut recovered_ranges = vec![None; k];

        if !(0..k).all(|i| all_shards[i].is_some()) {
            let missing_needed: Vec<usize> = (0..k).filter(|&i| all_shards[i].is_none()).collect();
            let present_indices: Vec<usize> =
                (0..(k + m)).filter(|&i| all_shards[i].is_some()).collect();
            let mut present_refs = Vec::with_capacity(present_indices.len());
            for &shard_index in &present_indices {
                let Some(shard) = all_shards.get(shard_index).and_then(Option::as_ref) else {
                    return Err(StoreError::PayloadShardSetMismatch {
                        reason: format!(
                            "segment recovery present shard index {} missing payload",
                            shard_index
                        ),
                    });
                };
                present_refs.push(shard.as_slice());
            }
            let codec = erasure_codec_for_shape(req.ec, "build segment recovery codec")?;
            let recovered_len = missing_needed.len() * shard_size;
            let mut recovered_buf = vec![0; recovered_len];
            let mut output_refs: Vec<&mut [u8]> = recovered_buf
                .chunks_exact_mut(shard_size)
                .take(missing_needed.len())
                .collect();

            codec
                .reconstruct(
                    &present_indices,
                    &present_refs,
                    &missing_needed,
                    &mut output_refs,
                )
                .map_err(|error| StoreError::ErasureCoding {
                    context: "reconstruct placed segment shards",
                    reason: error.to_string(),
                })?;

            for (slot, &missing_idx) in missing_needed.iter().enumerate() {
                let start = slot * shard_size;
                recovered_ranges[missing_idx] = Some((start, start + shard_size));
            }
            recovered = Some(recovered_buf);
        }

        dst.clear();
        dst.reserve(padded);
        for (idx, shard) in all_shards.iter().take(k).enumerate() {
            if let Some(shard) = shard.as_ref() {
                dst.extend_from_slice(shard);
            } else if let Some((start, end)) = recovered_ranges[idx] {
                let Some(recovered_buf) = recovered.as_ref() else {
                    return Err(StoreError::PayloadShardSetMismatch {
                        reason: format!(
                            "segment recovery missing reconstructed payload for data index {idx}"
                        ),
                    });
                };
                let Some(recovered_shard) = recovered_buf.get(start..end) else {
                    return Err(StoreError::PayloadShardSetMismatch {
                        reason: format!(
                            "segment recovery range {start}..{end} outside reconstructed payload length {}",
                            recovered_buf.len()
                        ),
                    });
                };
                dst.extend_from_slice(recovered_shard);
            } else {
                return Err(StoreError::PayloadShardSetMismatch {
                    reason: format!(
                        "segment recovery missing reconstructed shard for data index {idx}"
                    ),
                });
            }
        }
        dst.truncate(req.stored_size);
        let actual_crc64 = checksum::crc64::checksum(dst);
        if actual_crc64 != req.segment_crc64 {
            return Err(StoreError::IntegrityError {
                expected: req.segment_crc64,
                actual: actual_crc64,
            });
        }
        if schedule_repair_on_recovery {
            for shard_index in repair_targets {
                self.schedule_placed_segment_shard_repair(req, shard_index)?;
            }
        }
        Ok(true)
    }

    #[allow(clippy::too_many_arguments)]
    fn try_load_placed_segment_shard(
        &self,
        data_pg_id: u32,
        segment_okh: &[u8; 16],
        segment_vid: GenerationId,
        locations: &[ShardLocation],
        shard_index: usize,
        shard_size: usize,
        all_shards: &mut [Option<Vec<u8>>],
        present_count: &mut usize,
        repair_targets: Option<&mut Vec<ShardIndex>>,
    ) -> Result<(), StoreError> {
        fn record_repair_target(targets: Option<&mut Vec<ShardIndex>>, shard_index: usize) {
            let Some(targets) = targets else {
                return;
            };
            let shard_index = ShardIndex::new(shard_index as u8);
            if !targets.contains(&shard_index) {
                targets.push(shard_index);
            }
        }

        let Some(location) = locations.get(shard_index).copied() else {
            return Ok(());
        };
        let shard_key = ShardKey::new(segment_okh, segment_vid.get(), shard_index as u8);
        let ack = match self.load_payload_shard_ack(data_pg_id, &shard_key) {
            Ok(ack) => ack,
            Err(StoreError::NotFound) => {
                record_repair_target(repair_targets, shard_index);
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        if ack.stored_size != shard_size as u64 {
            record_repair_target(repair_targets, shard_index);
            return Ok(());
        }
        if let Err(error) =
            self.maybe_run_before_placed_payload_shard_read_hook(location, &shard_key)
        {
            placed_segment_recoverable_shard_error(error)?;
            record_repair_target(repair_targets, shard_index);
            return Ok(());
        }
        match self.read_payload_shard(location, &shard_key, ack) {
            Ok(shard) => {
                all_shards[shard_index] = Some(shard);
                *present_count += 1;
            }
            Err(error) => {
                placed_segment_recoverable_shard_error(error)?;
                record_repair_target(repair_targets, shard_index);
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn try_load_placed_segment_shard_for_historical_inspection(
        &self,
        route: &PgRouteSnapshot,
        data_pg_id: u32,
        segment_okh: &[u8; 16],
        segment_vid: GenerationId,
        locations: &[ShardLocation],
        shard_index: usize,
        shard_size: usize,
        all_shards: &mut [Option<Vec<u8>>],
        present_count: &mut usize,
    ) -> Result<(), StoreError> {
        let Some(location) = locations.get(shard_index).copied() else {
            return Ok(());
        };
        let shard_key = ShardKey::new(segment_okh, segment_vid.get(), shard_index as u8);
        let ack = match self
            .load_payload_shard_ack_for_pg_route_snapshot(route, data_pg_id, &shard_key)
        {
            Ok(ack) => ack,
            Err(StoreError::NotFound) => return Ok(()),
            Err(error) => return Err(error),
        };
        if ack.stored_size != shard_size as u64 {
            return Ok(());
        }
        match self.read_payload_shard_for_historical_inspection(location, &shard_key, ack) {
            Ok(shard) => {
                all_shards[shard_index] = Some(shard);
                *present_count += 1;
            }
            Err(error) => {
                placed_segment_recoverable_shard_error(error)?;
            }
        }
        Ok(())
    }

    fn enqueue_placed_segment_shard_repair(
        &self,
        request: SegmentStoredBytesRequest,
        shard_index: ShardIndex,
    ) -> bool {
        self.local_map
            .runtime_state()
            .enqueue_placed_segment_shard_repair(PlacedSegmentShardRepairWorkItem {
                request,
                shard_index,
            })
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_enqueue_placed_segment_shard_repair(
        &self,
        work_item: PlacedSegmentShardRepairWorkItem,
    ) -> bool {
        self.local_map
            .runtime_state()
            .enqueue_placed_segment_shard_repair(work_item)
    }

    fn schedule_placed_segment_shard_repair(
        &self,
        request: SegmentStoredBytesRequest,
        shard_index: ShardIndex,
    ) -> Result<(), StoreError> {
        let pg_id = PgId::new(request.data_pg_id);
        let work_item = PlacedSegmentShardRepairWorkItem {
            request,
            shard_index,
        };
        self.metadata_pg_primary_shard_ack_client(pg_id)?
            .record_placed_segment_shard_repair(pg_id, &work_item, None)?;
        self.enqueue_placed_segment_shard_repair(request, shard_index);
        Ok(())
    }

    fn resolve_placed_segment_shard_repair(
        &self,
        request: SegmentStoredBytesRequest,
        shard_index: ShardIndex,
    ) -> Result<(), StoreError> {
        let pg_id = PgId::new(request.data_pg_id);
        let work_item = PlacedSegmentShardRepairWorkItem {
            request,
            shard_index,
        };
        self.metadata_pg_primary_shard_ack_client(pg_id)?
            .resolve_placed_segment_shard_repair(pg_id, &work_item)
    }

    fn load_payload_shard_ack(
        &self,
        data_pg_id: u32,
        shard_key: &ShardKey,
    ) -> Result<WriteAck, StoreError> {
        // Placed payload bytes are routed by LocalClusterMap; per-shard
        // CRC/size acks are metadata rows in the same PG and are read through
        // that PG's primary.
        let pg_id = PgId::new(data_pg_id);
        let shard_ack_client = self.metadata_pg_primary_shard_ack_client(pg_id)?;
        shard_ack_client.load_written_shard_ack(pg_id, shard_key)
    }

    fn load_payload_shard_ack_for_pg_route_snapshot(
        &self,
        route: &PgRouteSnapshot,
        data_pg_id: u32,
        shard_key: &ShardKey,
    ) -> Result<WriteAck, StoreError> {
        let pg_id = PgId::new(data_pg_id);
        if route.pg_id() != pg_id {
            return Err(StoreError::PayloadShardSetMismatch {
                reason: format!(
                    "historical shard ack route PG {} does not match data PG {}",
                    route.pg_id().get(),
                    pg_id.get()
                ),
            });
        }
        let primary = route.primary_node_id();
        if !route.acting_set().contains(&primary) {
            return Err(StoreError::NodeNotInActingSet {
                node_id: primary.as_u32(),
                pg_id: pg_id.get(),
                cluster_epoch: route.cluster_epoch(),
            });
        }
        let node = self
            .local_map
            .node(primary)
            .ok_or(StoreError::NodeNotFound {
                node_id: primary.as_u32(),
                pg_id: pg_id.get(),
                cluster_epoch: route.cluster_epoch(),
            })?;
        node.shard_ack_client()
            .load_written_shard_ack_for_historical_inspection(pg_id, shard_key)
    }

    fn segment_payload_locations(
        &self,
        req: &SegmentStoredBytesRequest,
    ) -> Result<Vec<ShardLocation>, StoreError> {
        self.segment_payload_shard_locations(
            req.data_pg_id,
            req.ec,
            &req.segment_okh,
            req.segment_vid,
        )
    }

    pub fn segment_payload_shard_locations(
        &self,
        data_pg_id: u32,
        ec: EcShape,
        segment_okh: &[u8; 16],
        segment_vid: GenerationId,
    ) -> Result<Vec<ShardLocation>, StoreError> {
        let data_pg_id = DataPgId::new(PgId::new(data_pg_id));
        let placement_key = segment_payload_placement_key(segment_okh, segment_vid);
        self.place_payload_shards(data_pg_id, ec, &placement_key)
            .map_err(cluster_build_error_to_store)
    }

    fn delete_payload_shard_set(
        &self,
        data_pg_id: u32,
        ec: EcShape,
        okh: &[u8; 16],
        generation_id: GenerationId,
    ) -> Result<(), ObjectPgActionError> {
        let shard_keys = Self::payload_shard_set_keys(okh, generation_id, ec);
        self.delete_placed_payload_shard_keys(
            DataPgId::new(PgId::new(data_pg_id)),
            ec,
            okh,
            generation_id,
            &shard_keys,
        )?;
        self.delete_metadata_primary_payload_shard_keys(data_pg_id, &shard_keys)
    }

    fn delete_payload_shard_set_best_effort_at_epoch(
        &self,
        operation_epoch: ClusterEpoch,
        data_pg_id: u32,
        ec: EcShape,
        okh: &[u8; 16],
        generation_id: GenerationId,
    ) {
        let shard_keys = Self::payload_shard_set_keys(okh, generation_id, ec);
        self.delete_payload_shard_keys_best_effort_at_epoch(
            operation_epoch,
            data_pg_id,
            ec,
            okh,
            generation_id,
            shard_keys,
        );
    }

    fn delete_payload_shard_keys_best_effort(
        &self,
        data_pg_id: u32,
        ec: EcShape,
        okh: &[u8; 16],
        generation_id: GenerationId,
        shard_keys: impl IntoIterator<Item = ShardKey>,
    ) {
        self.delete_payload_shard_keys_best_effort_at_epoch(
            self.operation_epoch(),
            data_pg_id,
            ec,
            okh,
            generation_id,
            shard_keys,
        );
    }

    fn delete_payload_shard_keys_best_effort_at_epoch(
        &self,
        operation_epoch: ClusterEpoch,
        data_pg_id: u32,
        ec: EcShape,
        okh: &[u8; 16],
        generation_id: GenerationId,
        shard_keys: impl IntoIterator<Item = ShardKey>,
    ) {
        let shard_keys: Vec<ShardKey> = shard_keys.into_iter().collect();
        self.delete_placed_payload_shard_keys_best_effort_at_epoch(
            operation_epoch,
            DataPgId::new(PgId::new(data_pg_id)),
            ec,
            okh,
            generation_id,
            &shard_keys,
        );
        self.delete_metadata_primary_payload_shard_keys_best_effort_at_epoch(
            operation_epoch,
            data_pg_id,
            &shard_keys,
        );
    }

    fn delete_placed_payload_shard_keys(
        &self,
        data_pg_id: DataPgId,
        ec: EcShape,
        okh: &[u8; 16],
        generation_id: GenerationId,
        shard_keys: &[ShardKey],
    ) -> Result<(), ObjectPgActionError> {
        let placement_key = segment_payload_placement_key(okh, generation_id);
        let locations = self
            .place_payload_shards(data_pg_id, ec, &placement_key)
            .map_err(|error| ObjectPgActionError::Store(cluster_build_error_to_store(error)))?;

        for shard_key in shard_keys {
            let location = Self::placed_payload_shard_location(&locations, shard_key)
                .map_err(ObjectPgActionError::Store)?;
            self.maybe_run_before_placed_payload_shard_delete_hook(shard_key)
                .map_err(ObjectPgActionError::Store)?;
            self.delete_payload_shard(location, shard_key)
                .map_err(|error| ObjectPgActionError::Store(shard_io_error_to_store(error)))?;
        }
        Ok(())
    }

    fn delete_placed_payload_shard_keys_best_effort_at_epoch(
        &self,
        operation_epoch: ClusterEpoch,
        data_pg_id: DataPgId,
        ec: EcShape,
        okh: &[u8; 16],
        generation_id: GenerationId,
        shard_keys: &[ShardKey],
    ) {
        let placement_key = segment_payload_placement_key(okh, generation_id);
        let route = match self
            .local_map
            .reconstructed_pg_route_at_epoch(data_pg_id.pg_id(), operation_epoch)
        {
            Some(route) => route,
            None => {
                self.emit_best_effort_payload_cleanup_error(
                    "resolve retained payload placement route",
                    &StoreError::PayloadShardSetMismatch {
                        reason: format!(
                            "PG {} route for cluster epoch {} is not retained",
                            data_pg_id.get(),
                            operation_epoch.get()
                        ),
                    },
                );
                return;
            }
        };
        if route.state() != PgState::Active {
            self.emit_best_effort_payload_cleanup_error(
                "resolve retained payload placement route",
                &StoreError::PgNotActive {
                    pg_id: data_pg_id.get(),
                    cluster_epoch: route.cluster_epoch(),
                    state: route.state(),
                },
            );
            return;
        }
        let locations = match LocalClusterMap::place_payload_shards_for_pg_route(
            operation_epoch,
            data_pg_id,
            ec,
            &placement_key,
            route.acting_set(),
        ) {
            Ok(locations) => locations,
            Err(error) => {
                let error = cluster_build_error_to_store(error);
                self.emit_best_effort_payload_cleanup_error("place payload shards", &error);
                return;
            }
        };

        for shard_key in shard_keys {
            match Self::placed_payload_shard_location(&locations, shard_key) {
                Ok(location) => {
                    if let Err(error) =
                        self.maybe_run_before_placed_payload_shard_delete_hook(shard_key)
                    {
                        self.emit_best_effort_payload_cleanup_error(
                            "delete placed payload shard",
                            &error,
                        );
                        continue;
                    }
                    if let Err(error) = self
                        .local_map
                        .delete_payload_shard_for_historical_cleanup(location, shard_key)
                    {
                        let error = shard_io_error_to_store(error);
                        self.emit_best_effort_payload_cleanup_error(
                            "delete placed payload shard",
                            &error,
                        );
                    }
                }
                Err(error) => {
                    self.emit_best_effort_payload_cleanup_error(
                        "resolve placed payload shard",
                        &error,
                    );
                }
            }
        }
    }

    fn delete_metadata_primary_payload_shard_keys(
        &self,
        data_pg_id: u32,
        shard_keys: &[ShardKey],
    ) -> Result<(), ObjectPgActionError> {
        let pg_id = PgId::new(data_pg_id);
        let shard_ack_client = self.metadata_pg_primary_shard_ack_client(pg_id)?;
        for shard_key in shard_keys {
            self.maybe_run_before_metadata_primary_payload_ack_delete_hook(shard_key)
                .map_err(ObjectPgActionError::Store)?;
            shard_ack_client.delete_written_shard_ack(pg_id, shard_key)?;
        }
        Ok(())
    }

    fn delete_metadata_primary_payload_shard_keys_best_effort_at_epoch(
        &self,
        operation_epoch: ClusterEpoch,
        data_pg_id: u32,
        shard_keys: &[ShardKey],
    ) {
        let pg_id = PgId::new(data_pg_id);
        let shard_ack_client = match self
            .metadata_pg_primary_shard_ack_client_at_retained_epoch(operation_epoch, pg_id)
        {
            Ok(shard_ack_client) => shard_ack_client,
            Err(error) => {
                self.emit_best_effort_payload_cleanup_error(
                    "resolve payload ack metadata PG primary",
                    &error,
                );
                return;
            }
        };
        for shard_key in shard_keys {
            if let Err(error) =
                self.maybe_run_before_metadata_primary_payload_ack_delete_hook(shard_key)
            {
                self.emit_best_effort_payload_cleanup_error("delete payload ack", &error);
                continue;
            }
            if let Err(error) = shard_ack_client.delete_written_shard_ack_at_retained_epoch(
                operation_epoch,
                pg_id,
                shard_key,
            ) {
                self.emit_best_effort_payload_cleanup_error("delete payload ack", &error);
            }
        }
    }

    fn placed_payload_shard_location(
        locations: &[ShardLocation],
        shard_key: &ShardKey,
    ) -> Result<ShardLocation, StoreError> {
        let shard_index = usize::from(shard_key.shard_index().get());
        locations
            .get(shard_index)
            .copied()
            .ok_or_else(|| StoreError::Io {
                context: "resolve placed payload shard index",
                source: std::io::Error::other(format!(
                    "shard index {shard_index} outside {} placed shards",
                    locations.len()
                )),
            })
    }

    fn payload_shard_set_keys(
        okh: &[u8; 16],
        generation_id: GenerationId,
        ec: EcShape,
    ) -> Vec<ShardKey> {
        (0..(ec.k + ec.m))
            .map(|shard_index| ShardKey::new(okh, generation_id.get(), shard_index))
            .collect()
    }

    fn delete_staged_stream_segment_payload_shards_best_effort(
        &self,
        segments: &[StreamUploadSegmentRecord],
    ) {
        for segment in segments {
            self.delete_stream_segment_payload_shards_best_effort(segment);
        }
    }

    fn delete_object_segment_payload_shards_best_effort(&self, segment: &ObjectSegmentRecord) {
        self.delete_payload_shard_set_best_effort_at_epoch(
            segment.placement_cluster_epoch,
            segment.data_pg_id,
            EcShape {
                k: segment.ec_k,
                m: segment.ec_m,
            },
            &segment.segment_okh,
            segment.segment_vid,
        );
    }

    fn delete_stream_segment_payload_shards_best_effort(
        &self,
        segment: &StreamUploadSegmentRecord,
    ) {
        self.delete_payload_shard_set_best_effort_at_epoch(
            segment.placement_cluster_epoch,
            segment.data_pg_id,
            EcShape {
                k: segment.ec_k,
                m: segment.ec_m,
            },
            &segment.segment_okh,
            segment.segment_vid,
        );
    }

    fn delete_stream_segment_payload_shard_keys_best_effort(
        &self,
        segment: &StreamUploadSegmentRecord,
        shard_keys: impl IntoIterator<Item = ShardKey>,
    ) {
        self.delete_payload_shard_keys_best_effort_at_epoch(
            segment.placement_cluster_epoch,
            segment.data_pg_id,
            EcShape {
                k: segment.ec_k,
                m: segment.ec_m,
            },
            &segment.segment_okh,
            segment.segment_vid,
            shard_keys,
        );
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_payload_shard_file_path(
        &self,
        data_pg_id: u32,
        ec: EcShape,
        segment_okh: &[u8; 16],
        segment_vid: GenerationId,
        shard_index: u8,
    ) -> Result<std::path::PathBuf, StoreError> {
        let data_pg = DataPgId::new(PgId::new(data_pg_id));
        let placement_key = segment_payload_placement_key(segment_okh, segment_vid);
        let locations = self
            .place_payload_shards(data_pg, ec, &placement_key)
            .map_err(cluster_build_error_to_store)?;
        let location = locations
            .get(usize::from(shard_index))
            .ok_or_else(|| StoreError::Io {
                context: "resolve placed payload shard index",
                source: std::io::Error::other(format!(
                    "shard index {shard_index} outside {} placed shards",
                    locations.len()
                )),
            })?;
        let shard_key = ShardKey::new(segment_okh, segment_vid.get(), shard_index);
        let node = self
            .local_map
            .node(location.node_id())
            .ok_or_else(|| StoreError::Io {
                context: "resolve placed payload shard node",
                source: std::io::Error::other(format!(
                    "unknown local node {}",
                    location.node_id().as_u32()
                )),
            })?;
        Ok(node
            .data_dir()
            .join(format!("pg-{data_pg_id:04}"))
            .join("shards")
            .join(shard_key.hex_prefix())
            .join(shard_key.hex()))
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_payload_shard_file_exists(
        &self,
        data_pg_id: u32,
        ec: EcShape,
        segment_okh: &[u8; 16],
        segment_vid: GenerationId,
        shard_index: u8,
    ) -> Result<bool, StoreError> {
        Ok(self
            .test_payload_shard_file_path(data_pg_id, ec, segment_okh, segment_vid, shard_index)?
            .exists())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_list_shard_scavenger_observations(
        &self,
        data_pg_id: u32,
    ) -> Result<Vec<ShardScavengerObservation>, StoreError> {
        let pg_id = PgId::new(data_pg_id);
        self.metadata_pg_primary_client(pg_id)?
            .list_shard_scavenger_observations(pg_id)
    }

    fn emit_best_effort_payload_cleanup_error(&self, operation: &'static str, error: &StoreError) {
        let Some(trace) = observability::current_context() else {
            return;
        };
        self.maybe_observe_best_effort_payload_cleanup_error(operation, error);
        let _ = observability::event_in_context(
            &trace,
            TRACE_TARGET,
            "payload_cleanup_best_effort_error",
            Some(format_args!("operation={operation:?} error={error}")),
        );
    }
}

fn segment_payload_placement_key(segment_okh: &[u8; 16], segment_vid: GenerationId) -> [u8; 24] {
    let mut key = [0u8; 24];
    key[..16].copy_from_slice(segment_okh);
    key[16..].copy_from_slice(&segment_vid.get().to_be_bytes());
    key
}

fn validate_placed_segment_repair_ec_shape(ec: EcShape) -> Result<EcConfig, StoreError> {
    EcConfig::new(ec.k, ec.m).map_err(|error| StoreError::ErasureCoding {
        context: "inspect placed segment repair targets EC shape",
        reason: error.to_string(),
    })
}

fn build_placed_segment_shard_backfill_plan(
    source_health: PlacedSegmentShardSetHealth,
    desired_health: PlacedSegmentShardSetHealth,
) -> Result<PlacedSegmentShardBackfillPlan, StoreError> {
    if source_health.total_shards != desired_health.total_shards
        || source_health.required_shards != desired_health.required_shards
    {
        return Err(StoreError::PayloadShardSetMismatch {
            reason: format!(
                "source shard set {}/{} does not match desired shard set {}/{}",
                source_health.required_shards,
                source_health.total_shards,
                desired_health.required_shards,
                desired_health.total_shards
            ),
        });
    }

    let mut already_present = Vec::new();
    let mut copy_targets = Vec::new();
    let mut reconstruction_targets = Vec::new();
    let mut unrecoverable_targets = Vec::new();
    let source_recoverable =
        !matches!(source_health.risk, PlacedSegmentShardSetRisk::Unrecoverable);

    for desired in &desired_health.shards {
        if desired.validation.is_valid() {
            already_present.push(desired.shard_index);
            continue;
        }
        let source = source_health
            .shards
            .iter()
            .find(|source| source.shard_index == desired.shard_index)
            .ok_or_else(|| StoreError::PayloadShardSetMismatch {
                reason: format!(
                    "desired shard index {} has no source shard",
                    desired.shard_index.get()
                ),
            })?;
        if source.validation.is_valid() {
            copy_targets.push(PlacedSegmentShardBackfillCopyTarget {
                shard_index: desired.shard_index,
                shard_key: desired.shard_key.clone(),
                source: source.location,
                destination: desired.location,
            });
        } else if source_recoverable {
            reconstruction_targets.push(desired.shard_index);
        } else {
            unrecoverable_targets.push(desired.shard_index);
        }
    }

    Ok(PlacedSegmentShardBackfillPlan {
        source_health,
        desired_health,
        already_present,
        copy_targets,
        reconstruction_targets,
        unrecoverable_targets,
    })
}

fn note_shard_backfill_candidate_error(
    summary: &mut PlacedSegmentShardBackfillCandidateEnqueueSummary,
    error: &StoreError,
) {
    if shard_backfill_candidate_error_is_deferred(error) {
        summary.deferred += 1;
    } else {
        summary.failed += 1;
    }
}

fn note_metadata_command_checkpoint_record_error(
    pg_id: PgId,
    outcome: &'static str,
    error: &StoreError,
) {
    let error_kind = metadata_command_checkpoint_record_error_kind(error);
    let _ = observability::emit_metadata_command_checkpoint_record_error(
        TRACE_TARGET,
        observability::MetadataCommandCheckpointRecordErrorSummary {
            pg_id: pg_id.get(),
            outcome,
            error_kind,
        },
    );
}

fn compact_metadata_command_log_for_checkpoint_record(
    metadata_client: &dyn MetadataCommandNodeClient,
    pg_id: PgId,
    cluster_epoch: ClusterEpoch,
    summary: &mut MetadataCommandCheckpointRecordSummary,
) {
    match metadata_client.compact_metadata_command_log(pg_id, cluster_epoch) {
        Ok(MetadataCommandLogCompactionStatus::NoCheckpoint { .. }) => {
            summary.compaction_no_checkpoint += 1;
        }
        Ok(MetadataCommandLogCompactionStatus::PendingCommand { .. }) => {
            summary.compaction_pending += 1;
        }
        Ok(MetadataCommandLogCompactionStatus::Compacted {
            deleted_entries: 0, ..
        }) => {
            summary.compaction_noop += 1;
        }
        Ok(MetadataCommandLogCompactionStatus::Compacted {
            deleted_entries, ..
        }) => {
            summary.compacted += 1;
            summary.compaction_deleted_entries = summary
                .compaction_deleted_entries
                .saturating_add(deleted_entries);
        }
        Err(error) => {
            if metadata_command_checkpoint_record_error_is_stale(&error) {
                summary.skipped_stale_epoch += 1;
                note_metadata_command_checkpoint_record_error(pg_id, "skipped_stale", &error);
            } else {
                note_metadata_command_checkpoint_record_error(pg_id, "compaction_failed", &error);
                summary.compaction_failed += 1;
            }
        }
    }
}

fn metadata_command_checkpoint_record_error_kind(error: &StoreError) -> &'static str {
    match error {
        StoreError::NotFound => "not_found",
        StoreError::IntegrityError { .. } => "integrity_error",
        StoreError::ShardAckMismatch { .. } => "shard_ack_mismatch",
        StoreError::PayloadShardSetMismatch { .. } => "payload_shard_set_mismatch",
        StoreError::PgNotFound { .. } => "pg_not_found",
        StoreError::InvalidPgTopology { .. } => "invalid_pg_topology",
        StoreError::ClusterPgNotFound { .. } => "cluster_pg_not_found",
        StoreError::ShardPgNotFound { .. } => "shard_pg_not_found",
        StoreError::PgNotActive { .. } => "pg_not_active",
        StoreError::ShardPgNotActive { .. } => "shard_pg_not_active",
        StoreError::ShardStore { source, .. } => {
            metadata_command_checkpoint_record_error_kind(source)
        }
        StoreError::StorageRpc { code, .. } => {
            metadata_command_checkpoint_record_storage_rpc_error_kind(*code)
        }
        StoreError::StorageRpcResourceExhausted { .. } => "storage_rpc_resource_exhausted",
        StoreError::StorageRpcShardDeleteInProgress { .. } => {
            "storage_rpc_shard_delete_in_progress"
        }
        StoreError::StalePayloadOperation { .. } => "stale_payload_operation",
        StoreError::StaleMetadataPrimaryBridge { .. } => "stale_metadata_primary_bridge",
        StoreError::StaleMetadataOperation { .. } => "stale_metadata_operation",
        StoreError::StaleMetadataRoute { .. } => "stale_metadata_route",
        StoreError::RouteMapExpired { .. } => "route_map_expired",
        StoreError::StaleMetadataCommand { .. } => "stale_metadata_command",
        StoreError::MetadataCommandWrongPg { .. } => "metadata_command_wrong_pg",
        StoreError::MetadataCommandFromNonPrimary { .. } => "metadata_command_from_non_primary",
        StoreError::MetadataCommandLogConflict { .. } => "metadata_command_log_conflict",
        StoreError::MetadataCommandLogGap { .. } => "metadata_command_log_gap",
        StoreError::MetadataCommandPendingConflict { .. } => "metadata_command_pending_conflict",
        StoreError::StaleShardOperation { .. } => "stale_shard_operation",
        StoreError::StaleShardLocation { .. } => "stale_shard_location",
        StoreError::MetadataCommandContention { .. } => "metadata_command_contention",
        StoreError::MetadataTransferEmpty { .. } => "metadata_transfer_empty",
        StoreError::MetadataCommandPendingOnNonPrimary { .. } => {
            "metadata_command_pending_on_non_primary"
        }
        StoreError::MetadataCommandLogChecksumMismatch { .. } => {
            "metadata_command_log_checksum_mismatch"
        }
        StoreError::MetadataCommandLogHashMismatch { .. } => "metadata_command_log_hash_mismatch",
        StoreError::MetadataCommandReplicaStateMissing { .. } => {
            "metadata_command_replica_state_missing"
        }
        StoreError::MetadataCommandReplicaStateDiverged { .. } => {
            "metadata_command_replica_state_diverged"
        }
        StoreError::MetadataStateDigestMismatch { .. } => "metadata_state_digest_mismatch",
        StoreError::MetadataTransferUnsupportedProof { .. } => {
            "metadata_transfer_unsupported_proof"
        }
        StoreError::MetadataCheckpointInvalid { .. } => "metadata_checkpoint_invalid",
        StoreError::NodeNotFound { .. } => "node_not_found",
        StoreError::NodeNotInActingSet { .. } => "node_not_in_acting_set",
        StoreError::ShardIndexMismatch { .. } => "shard_index_mismatch",
        StoreError::ShardScavengerObservationWrongPg { .. } => {
            "shard_scavenger_observation_wrong_pg"
        }
        StoreError::ShardScavengerObservationShardIndexMismatch { .. } => {
            "shard_scavenger_observation_shard_index_mismatch"
        }
        StoreError::ShardScavengerObservationInconsistentReason { .. } => {
            "shard_scavenger_observation_inconsistent_reason"
        }
        StoreError::InvalidKeyLength { .. } => "invalid_key_length",
        StoreError::InvalidShardKeyHex => "invalid_shard_key_hex",
        StoreError::ShardScavengerScanIncomplete { .. } => "shard_scavenger_scan_incomplete",
        StoreError::Io { context, .. } if *context == "connect storage-node RPC socket" => {
            "storage_rpc_socket_connect"
        }
        StoreError::Io { .. } => "io",
        StoreError::Db { .. } => "db",
        StoreError::ErasureCoding { .. } => "erasure_coding",
    }
}

fn metadata_command_checkpoint_record_storage_rpc_error_kind(
    code: StorageRpcErrorCode,
) -> &'static str {
    match code {
        StorageRpcErrorCode::FrameDecode => "storage_rpc_frame_decode",
        StorageRpcErrorCode::PayloadDecode => "storage_rpc_payload_decode",
        StorageRpcErrorCode::UnknownNode => "storage_rpc_unknown_node",
        StorageRpcErrorCode::UnknownPg => "storage_rpc_unknown_pg",
        StorageRpcErrorCode::WrongClusterEpoch => "storage_rpc_wrong_cluster_epoch",
        StorageRpcErrorCode::InactivePgRoute => "storage_rpc_inactive_pg_route",
        StorageRpcErrorCode::StaleShardLocation => "storage_rpc_stale_shard_location",
        StorageRpcErrorCode::NonActingSetAccess => "storage_rpc_non_acting_set_access",
        StorageRpcErrorCode::UnsupportedOperation => "storage_rpc_unsupported_operation",
        StorageRpcErrorCode::Internal => "storage_rpc_internal",
        StorageRpcErrorCode::ResourceExhausted => "storage_rpc_resource_exhausted",
        StorageRpcErrorCode::ReclaimClaimNotFound => "storage_rpc_reclaim_claim_not_found",
        StorageRpcErrorCode::ShardDeleteInProgress => "storage_rpc_shard_delete_in_progress",
        StorageRpcErrorCode::BucketWriteDrainConflict => "storage_rpc_bucket_write_drain_conflict",
        StorageRpcErrorCode::BucketWriteDrainNotFound => "storage_rpc_bucket_write_drain_not_found",
        StorageRpcErrorCode::ReclaimClaimConflict => "storage_rpc_reclaim_claim_conflict",
        StorageRpcErrorCode::NotFound => "storage_rpc_not_found",
        StorageRpcErrorCode::BucketWriteReservationConflict => {
            "storage_rpc_bucket_write_reservation_conflict"
        }
        StorageRpcErrorCode::BucketWriteReservationNotFound => {
            "storage_rpc_bucket_write_reservation_not_found"
        }
        StorageRpcErrorCode::MetadataCommandContention => "storage_rpc_metadata_command_contention",
        StorageRpcErrorCode::MetadataTransferHistoricalRouteActive => {
            "storage_rpc_metadata_transfer_historical_route_active"
        }
        StorageRpcErrorCode::TransportTimeout => "storage_rpc_transport_timeout",
        StorageRpcErrorCode::TransportClosed => "storage_rpc_transport_closed",
        StorageRpcErrorCode::ShardIntegrity => "storage_rpc_shard_integrity",
    }
}

fn metadata_command_checkpoint_record_error_is_stale(error: &StoreError) -> bool {
    match error {
        StoreError::StalePayloadOperation { .. }
        | StoreError::StaleMetadataCommand { .. }
        | StoreError::StaleMetadataPrimaryBridge { .. }
        | StoreError::StaleMetadataOperation { .. }
        | StoreError::StaleMetadataRoute { .. }
        | StoreError::RouteMapExpired { .. }
        | StoreError::StaleShardOperation { .. }
        | StoreError::StaleShardLocation { .. }
        | StoreError::PgNotActive { .. }
        | StoreError::MetadataCommandContention { .. } => true,
        StoreError::Io {
            context: "connect storage-node RPC socket",
            ..
        } => true,
        StoreError::StorageRpc { code, .. } => {
            storage_rpc_code_is_retryable_pg_route_error(*code)
                // Background checkpoint scans can observe intermediate route-map states
                // while PG metadata transfer is moving between Peering and Active routes.
                // A later scan will retry from a refreshed map.
                || matches!(
                    *code,
                    StorageRpcErrorCode::UnknownPg
                        | StorageRpcErrorCode::MetadataTransferHistoricalRouteActive
                )
        }
        _ => false,
    }
}

#[cfg(test)]
mod metadata_command_checkpoint_record_error_tests {
    use super::*;

    #[test]
    fn metadata_checkpoint_record_treats_restart_and_contention_as_transient() {
        assert!(metadata_command_checkpoint_record_error_is_stale(
            &StoreError::Io {
                context: "connect storage-node RPC socket",
                source: std::io::Error::new(std::io::ErrorKind::NotFound, "node socket missing"),
            },
        ));
        assert!(metadata_command_checkpoint_record_error_is_stale(
            &StoreError::StorageRpc {
                node_id: 0,
                operation: "metadata command checkpoint record current",
                code: StorageRpcErrorCode::MetadataCommandContention,
                message: "metadata command contention during export metadata command checkpoint"
                    .to_string(),
            },
        ));
        assert!(metadata_command_checkpoint_record_error_is_stale(
            &StoreError::StorageRpc {
                node_id: 0,
                operation: "metadata command checkpoint record current",
                code: StorageRpcErrorCode::UnknownPg,
                message: "PG 7 is not configured on this storage node".to_string(),
            },
        ));
        assert!(metadata_command_checkpoint_record_error_is_stale(
            &StoreError::StorageRpc {
                node_id: 0,
                operation: "metadata command replica state",
                code: StorageRpcErrorCode::MetadataTransferHistoricalRouteActive,
                message: "historical peering inspection for PG 7 at epoch 42 requires Peering route, got active".to_string(),
            },
        ));
        assert!(metadata_command_checkpoint_record_error_is_stale(
            &StoreError::StorageRpc {
                node_id: 0,
                operation: "metadata command checkpoint record current",
                code: StorageRpcErrorCode::TransportClosed,
                message: "storage RPC stream I/O error: early eof".to_string(),
            },
        ));
        assert!(!metadata_command_checkpoint_record_error_is_stale(
            &StoreError::StorageRpc {
                node_id: 0,
                operation: "metadata command checkpoint record current",
                code: StorageRpcErrorCode::Internal,
                message: "metadata state digest mismatch".to_string(),
            },
        ));
    }

    #[test]
    fn metadata_checkpoint_record_error_kind_labels_checkpoint_integrity_failures() {
        let epoch = ClusterEpoch::new(3).unwrap();
        assert_eq!(
            metadata_command_checkpoint_record_error_kind(
                &StoreError::MetadataCommandLogHashMismatch {
                    node_id: 0,
                    pg_id: 7,
                    cluster_epoch: epoch,
                    log_index: 9,
                    expected_previous_log_hash: 11,
                    actual_previous_log_hash: 12,
                    expected_log_hash: 13,
                    actual_log_hash: 14,
                }
            ),
            "metadata_command_log_hash_mismatch"
        );
        assert_eq!(
            metadata_command_checkpoint_record_error_kind(
                &StoreError::MetadataCommandReplicaStateDiverged {
                    node_id: 0,
                    reference_node_id: 1,
                    pg_id: 7,
                    cluster_epoch: epoch,
                    reference_cluster_epoch: epoch,
                    applied_log_index: 9,
                    reference_applied_log_index: 8,
                    applied_log_hash: 10,
                    reference_applied_log_hash: 11,
                    state_digest: 12,
                    reference_state_digest: 13,
                }
            ),
            "metadata_command_replica_state_diverged"
        );
        assert_eq!(
            metadata_command_checkpoint_record_error_kind(
                &StoreError::MetadataStateDigestMismatch {
                    node_id: 0,
                    pg_id: 7,
                    cluster_epoch: epoch,
                    expected_digest: 12,
                    actual_digest: 13,
                }
            ),
            "metadata_state_digest_mismatch"
        );
        assert_eq!(
            metadata_command_checkpoint_record_error_kind(&StoreError::MetadataCheckpointInvalid {
                node_id: 0,
                pg_id: 7,
                cluster_epoch: epoch,
                reason: "frame hash mismatch".to_string(),
            }),
            "metadata_checkpoint_invalid"
        );
        assert_eq!(
            metadata_command_checkpoint_record_error_kind(
                &StoreError::MetadataTransferUnsupportedProof {
                    node_id: 0,
                    pg_id: 7,
                    cluster_epoch: epoch,
                    applied_log_index: 9,
                    applied_log_hash: 10,
                }
            ),
            "metadata_transfer_unsupported_proof"
        );
    }

    #[test]
    fn shard_backfill_candidate_treats_restart_connect_failure_as_deferred() {
        assert!(shard_backfill_candidate_error_is_deferred(
            &StoreError::Io {
                context: "connect storage-node RPC socket",
                source: std::io::Error::new(std::io::ErrorKind::NotFound, "node socket missing"),
            },
        ));
        assert!(shard_backfill_candidate_error_is_deferred(
            &StoreError::StorageRpc {
                node_id: 0,
                operation: "list shard scavenger payload references",
                code: StorageRpcErrorCode::TransportClosed,
                message: "storage RPC stream I/O error: early eof".to_string(),
            },
        ));
        assert!(!shard_backfill_candidate_error_is_deferred(
            &StoreError::StorageRpc {
                node_id: 0,
                operation: "list shard scavenger payload references",
                code: StorageRpcErrorCode::PayloadDecode,
                message: "storage RPC frame checksum mismatch".to_string(),
            },
        ));
    }
}

fn metadata_command_checkpoint_record_decision(
    state: &MetadataCommandReplicaState,
    latest_checkpoint: Option<&MetadataCommandCheckpoint>,
    min_log_distance: u64,
    frame_risk_bytes: usize,
) -> Result<MetadataCommandCheckpointRecordDecision, StoreError> {
    let Some(checkpoint) = latest_checkpoint else {
        return Ok(MetadataCommandCheckpointRecordDecision::Record);
    };
    if checkpoint.cluster_epoch == state.cluster_epoch
        && checkpoint.applied_log_index == state.applied_log_index
        && checkpoint.applied_log_hash == state.applied_log_hash
        && checkpoint.state_digest == state.state_digest
    {
        return Ok(MetadataCommandCheckpointRecordDecision::AlreadyCurrent);
    }

    let log_distance = state
        .applied_log_index
        .saturating_sub(checkpoint.applied_log_index);
    if log_distance >= min_log_distance {
        return Ok(MetadataCommandCheckpointRecordDecision::Record);
    }

    let checkpoint_bytes = crate::storage_rpc::encode_metadata_command_checkpoint_payload(
        checkpoint,
    )
    .map_err(|error| StoreError::MetadataCheckpointInvalid {
        node_id: 0,
        pg_id: checkpoint.pg_id.get(),
        cluster_epoch: checkpoint.cluster_epoch,
        reason: error.to_string(),
    })?;
    if checkpoint_bytes.len() >= frame_risk_bytes {
        return Ok(MetadataCommandCheckpointRecordDecision::Record);
    }

    Ok(MetadataCommandCheckpointRecordDecision::SkipCadence)
}

fn shard_backfill_candidate_error_is_deferred(error: &StoreError) -> bool {
    match error {
        StoreError::ShardStore { source, .. } => shard_backfill_candidate_error_is_deferred(source),
        StoreError::PgNotActive { .. }
        | StoreError::ShardPgNotActive { .. }
        | StoreError::StalePayloadOperation { .. }
        | StoreError::StaleMetadataPrimaryBridge { .. }
        | StoreError::StaleMetadataOperation { .. }
        | StoreError::StaleMetadataRoute { .. }
        | StoreError::RouteMapExpired { .. }
        | StoreError::StaleShardOperation { .. }
        | StoreError::StaleShardLocation { .. }
        | StoreError::StorageRpcResourceExhausted { .. } => true,
        StoreError::Io {
            context: "connect storage-node RPC socket",
            ..
        } => true,
        StoreError::StorageRpc { code, .. } => storage_rpc_code_is_retryable_pg_route_error(*code),
        _ => false,
    }
}

fn storage_rpc_code_is_retryable_pg_route_error(code: StorageRpcErrorCode) -> bool {
    matches!(
        code,
        StorageRpcErrorCode::StaleShardLocation
            | StorageRpcErrorCode::InactivePgRoute
            | StorageRpcErrorCode::NonActingSetAccess
            | StorageRpcErrorCode::WrongClusterEpoch
            | StorageRpcErrorCode::MetadataCommandContention
            | StorageRpcErrorCode::TransportTimeout
            | StorageRpcErrorCode::TransportClosed
    )
}

fn erasure_codec_for_shape(ec: EcShape, context: &'static str) -> Result<ErasureCodec, StoreError> {
    let config = EcConfig::new(ec.k, ec.m).map_err(|error| StoreError::ErasureCoding {
        context,
        reason: error.to_string(),
    })?;
    ErasureCodec::new(config).map_err(|error| StoreError::ErasureCoding {
        context,
        reason: error.to_string(),
    })
}

fn cluster_build_error_to_store(error: ClusterBuildError) -> StoreError {
    match error {
        ClusterBuildError::PgNotFound {
            pg_id,
            cluster_epoch,
        } => StoreError::ClusterPgNotFound {
            pg_id,
            cluster_epoch,
        },
        ClusterBuildError::PgNotActive {
            pg_id,
            cluster_epoch,
            state,
        } => StoreError::PgNotActive {
            pg_id,
            cluster_epoch,
            state,
        },
        ClusterBuildError::StalePayloadPlacement {
            pg_id,
            operation_epoch,
            current_epoch,
        } => StoreError::StalePayloadOperation {
            pg_id,
            operation_epoch,
            current_epoch,
        },
        ClusterBuildError::RouteMapExpired {
            pg_id: _,
            cluster_epoch,
            valid_until_ms,
            now_ms,
        } => StoreError::RouteMapExpired {
            cluster_epoch,
            valid_until_ms,
            now_ms,
        },
        other => StoreError::Io {
            context: "place payload shards",
            source: std::io::Error::other(other.to_string()),
        },
    }
}

fn shard_io_error_to_store(error: ShardIoError) -> StoreError {
    match error {
        ShardIoError::Store {
            node_id,
            pg_id,
            cluster_epoch,
            source,
        } => StoreError::ShardStore {
            node_id,
            pg_id,
            cluster_epoch,
            source: Box::new(source),
        },
        ShardIoError::StaleOperationEpoch {
            node_id,
            pg_id,
            operation_epoch,
            current_epoch,
        } => StoreError::StaleShardOperation {
            node_id,
            pg_id,
            operation_epoch,
            current_epoch,
        },
        ShardIoError::StaleLocation {
            node_id,
            pg_id,
            location_epoch,
            current_epoch,
        } => StoreError::StaleShardLocation {
            node_id,
            pg_id,
            location_epoch,
            current_epoch,
        },
        ShardIoError::RouteMapExpired {
            node_id: _,
            pg_id: _,
            cluster_epoch,
            valid_until_ms,
            now_ms,
        } => StoreError::RouteMapExpired {
            cluster_epoch,
            valid_until_ms,
            now_ms,
        },
        ShardIoError::NodeNotFound {
            node_id,
            pg_id,
            cluster_epoch,
        } => StoreError::NodeNotFound {
            node_id,
            pg_id,
            cluster_epoch,
        },
        ShardIoError::PgNotFound {
            node_id,
            pg_id,
            cluster_epoch,
        } => StoreError::ShardPgNotFound {
            node_id,
            pg_id,
            cluster_epoch,
        },
        ShardIoError::PgNotActive {
            node_id,
            pg_id,
            cluster_epoch,
            state,
        } => StoreError::ShardPgNotActive {
            node_id,
            pg_id,
            cluster_epoch,
            state,
        },
        ShardIoError::NodeNotInActingSet {
            node_id,
            pg_id,
            cluster_epoch,
        } => StoreError::NodeNotInActingSet {
            node_id,
            pg_id,
            cluster_epoch,
        },
        ShardIoError::ShardIndexMismatch {
            node_id,
            pg_id,
            cluster_epoch,
            location_shard_index,
            key_shard_index,
        } => StoreError::ShardIndexMismatch {
            node_id,
            pg_id,
            cluster_epoch,
            location_shard_index,
            key_shard_index,
        },
    }
}

fn placed_segment_recoverable_shard_error(error: ShardIoError) -> Result<(), StoreError> {
    match error {
        ShardIoError::Store {
            source: StoreError::NotFound,
            ..
        }
        | ShardIoError::Store {
            source: StoreError::IntegrityError { .. },
            ..
        }
        | ShardIoError::Store {
            source:
                StoreError::StorageRpcShardDeleteInProgress {
                    operation: "read handles acquire",
                    ..
                },
            ..
        } => Ok(()),
        ShardIoError::Store {
            source: StoreError::StorageRpc {
                operation, code, ..
            },
            ..
        } if is_recoverable_remote_shard_read_error(operation, code) => Ok(()),
        ShardIoError::Store {
            source: StoreError::Io { context, source },
            ..
        } if is_recoverable_physical_shard_io_error(context, source.kind()) => Ok(()),
        other => Err(shard_io_error_to_store(other)),
    }
}

fn is_recoverable_remote_shard_read_error(
    operation: &'static str,
    code: StorageRpcErrorCode,
) -> bool {
    matches!(operation, "shard read" | "shard read range")
        && matches!(
            code,
            StorageRpcErrorCode::NotFound | StorageRpcErrorCode::ShardIntegrity
        )
}

fn is_recoverable_physical_shard_io_error(context: &'static str, kind: std::io::ErrorKind) -> bool {
    matches!(
        (context, kind),
        (
            "read payload shard size mismatch",
            std::io::ErrorKind::InvalidData
        ) | (
            "read shard file length mismatch",
            std::io::ErrorKind::InvalidData
        ) | ("read shard file", std::io::ErrorKind::UnexpectedEof)
    )
}

#[cfg(test)]
mod backfill_plan_tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::HashSet;

    fn backfill_plan_test_health(
        risk: PlacedSegmentShardSetRisk,
        valid_indexes: &[u8],
    ) -> PlacedSegmentShardSetHealth {
        let valid_indexes: HashSet<u8> = valid_indexes.iter().copied().collect();
        let shards = (0..6)
            .map(|shard_index| {
                let shard_index = ShardIndex::new(shard_index);
                PlacedSegmentShardHealth {
                    shard_index,
                    shard_key: ShardKey::new(&[9; 16], 1, shard_index.get()),
                    location: ShardLocation::new(
                        ClusterEpoch::INITIAL,
                        DataPgId::new(PgId::new(0)),
                        shard_index,
                        NodeId::new(u32::from(shard_index.get())),
                    ),
                    validation: if valid_indexes.contains(&shard_index.get()) {
                        PlacedSegmentShardValidation::Valid
                    } else {
                        PlacedSegmentShardValidation::MissingAck
                    },
                }
            })
            .collect();
        PlacedSegmentShardSetHealth {
            total_shards: 6,
            required_shards: 4,
            valid_shards: valid_indexes.len(),
            risk,
            shards,
        }
    }

    fn generated_backfill_plan_test_health(
        required_shards: usize,
        total_shards: usize,
        valid_mask: u16,
        data_pg_id: DataPgId,
        node_offset: u32,
    ) -> PlacedSegmentShardSetHealth {
        let valid_shards = (0..total_shards)
            .filter(|index| (valid_mask & (1_u16 << index)) != 0)
            .count();
        let risk = match valid_shards {
            valid if valid == total_shards => PlacedSegmentShardSetRisk::Healthy,
            valid if valid >= required_shards => PlacedSegmentShardSetRisk::Degraded {
                tolerance_remaining: valid - required_shards,
            },
            _ => PlacedSegmentShardSetRisk::Unrecoverable,
        };
        let shards = (0..total_shards)
            .map(|index| {
                let shard_index = ShardIndex::new(u8::try_from(index).unwrap());
                PlacedSegmentShardHealth {
                    shard_index,
                    shard_key: ShardKey::new(&[7; 16], 1, shard_index.get()),
                    location: ShardLocation::new(
                        ClusterEpoch::INITIAL,
                        data_pg_id,
                        shard_index,
                        NodeId::new(node_offset + u32::from(shard_index.get())),
                    ),
                    validation: if (valid_mask & (1_u16 << index)) != 0 {
                        PlacedSegmentShardValidation::Valid
                    } else {
                        PlacedSegmentShardValidation::MissingAck
                    },
                }
            })
            .collect();
        PlacedSegmentShardSetHealth {
            total_shards,
            required_shards,
            valid_shards,
            risk,
            shards,
        }
    }

    #[test]
    fn backfill_plan_marks_reconstruction_targets_for_recoverable_source_gaps() {
        let source_health = backfill_plan_test_health(
            PlacedSegmentShardSetRisk::Degraded {
                tolerance_remaining: 0,
            },
            &[0, 1, 2, 3],
        );
        let desired_health = backfill_plan_test_health(
            PlacedSegmentShardSetRisk::Degraded {
                tolerance_remaining: 1,
            },
            &[0, 1, 2, 3, 4],
        );

        let plan = build_placed_segment_shard_backfill_plan(source_health, desired_health).unwrap();

        assert_eq!(
            plan.already_present,
            vec![
                ShardIndex::new(0),
                ShardIndex::new(1),
                ShardIndex::new(2),
                ShardIndex::new(3),
                ShardIndex::new(4)
            ]
        );
        assert_eq!(plan.copy_targets, Vec::new());
        assert_eq!(plan.reconstruction_targets, vec![ShardIndex::new(5)]);
        assert_eq!(plan.unrecoverable_targets, Vec::new());
    }

    #[test]
    fn backfill_plan_marks_unrecoverable_targets_for_unrecoverable_source_gaps() {
        let source_health =
            backfill_plan_test_health(PlacedSegmentShardSetRisk::Unrecoverable, &[0, 1, 2]);
        let desired_health = backfill_plan_test_health(
            PlacedSegmentShardSetRisk::Degraded {
                tolerance_remaining: 1,
            },
            &[0, 1, 2, 3, 4],
        );

        let plan = build_placed_segment_shard_backfill_plan(source_health, desired_health).unwrap();

        assert_eq!(
            plan.already_present,
            vec![
                ShardIndex::new(0),
                ShardIndex::new(1),
                ShardIndex::new(2),
                ShardIndex::new(3),
                ShardIndex::new(4)
            ]
        );
        assert_eq!(plan.copy_targets, Vec::new());
        assert_eq!(plan.reconstruction_targets, Vec::new());
        assert_eq!(plan.unrecoverable_targets, vec![ShardIndex::new(5)]);
    }

    proptest! {
        #[test]
        fn prop_backfill_plan_classifies_targets_and_priority(
            required_shards in 1_usize..=6,
            parity_shards in 0_usize..=4,
            source_mask in any::<u16>(),
            desired_mask in any::<u16>(),
        ) {
            let total_shards = required_shards + parity_shards;
            prop_assume!(total_shards <= 10);
            let shard_mask = (1_u16 << total_shards) - 1;
            let source_mask = source_mask & shard_mask;
            let desired_mask = desired_mask & shard_mask;
            let source_valid_count = usize::try_from(source_mask.count_ones()).unwrap();
            let source_recoverable = source_valid_count >= required_shards;

            let source_health = generated_backfill_plan_test_health(
                required_shards,
                total_shards,
                source_mask,
                DataPgId::new(PgId::new(0)),
                10,
            );
            let desired_health = generated_backfill_plan_test_health(
                required_shards,
                total_shards,
                desired_mask,
                DataPgId::new(PgId::new(1)),
                100,
            );

            let plan = build_placed_segment_shard_backfill_plan(
                source_health.clone(),
                desired_health.clone(),
            )
            .unwrap();

            let expected_tolerance = source_valid_count.saturating_sub(required_shards);
            prop_assert_eq!(
                usize::from(plan.source_remaining_tolerance()),
                expected_tolerance
            );
            prop_assert_eq!(plan.source_health.valid_shards, source_valid_count);
            prop_assert_eq!(
                plan.source_health.risk,
                if source_valid_count == total_shards {
                    PlacedSegmentShardSetRisk::Healthy
                } else if source_recoverable {
                    PlacedSegmentShardSetRisk::Degraded {
                        tolerance_remaining: expected_tolerance,
                    }
                } else {
                    PlacedSegmentShardSetRisk::Unrecoverable
                }
            );

            for index in 0..total_shards {
                let shard_index = ShardIndex::new(u8::try_from(index).unwrap());
                let desired_valid = (desired_mask & (1_u16 << index)) != 0;
                let source_valid = (source_mask & (1_u16 << index)) != 0;
                let is_already_present = plan.already_present.contains(&shard_index);
                let copy_target = plan
                    .copy_targets
                    .iter()
                    .find(|target| target.shard_index == shard_index);
                let is_reconstruction_target =
                    plan.reconstruction_targets.contains(&shard_index);
                let is_unrecoverable_target =
                    plan.unrecoverable_targets.contains(&shard_index);
                let target_count = usize::from(is_already_present)
                    + usize::from(copy_target.is_some())
                    + usize::from(is_reconstruction_target)
                    + usize::from(is_unrecoverable_target);

                prop_assert_eq!(
                    target_count,
                    1,
                    "shard {} must be classified exactly once",
                    index
                );

                if desired_valid {
                    prop_assert!(is_already_present);
                    prop_assert!(copy_target.is_none());
                    prop_assert!(!is_reconstruction_target);
                    prop_assert!(!is_unrecoverable_target);
                } else if source_valid {
                    let target = copy_target.expect("valid source shard must produce a copy target");
                    prop_assert_eq!(target.source.shard_index(), shard_index);
                    prop_assert_eq!(target.destination.shard_index(), shard_index);
                    prop_assert!(!is_already_present);
                    prop_assert!(!is_reconstruction_target);
                    prop_assert!(!is_unrecoverable_target);
                } else if source_recoverable {
                    prop_assert!(is_reconstruction_target);
                    prop_assert!(!is_already_present);
                    prop_assert!(copy_target.is_none());
                    prop_assert!(!is_unrecoverable_target);
                } else {
                    prop_assert!(is_unrecoverable_target);
                    prop_assert!(!is_already_present);
                    prop_assert!(copy_target.is_none());
                    prop_assert!(!is_reconstruction_target);
                }
            }
        }
    }
}

#[cfg(test)]
mod reissue_decision_tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn metadata_contention_backoff_cap_grows_and_clamps() {
        assert_eq!(
            metadata_contention_backoff_cap(1),
            METADATA_CONTENTION_BACKOFF_INITIAL
        );
        assert_eq!(
            metadata_contention_backoff_cap(2),
            METADATA_CONTENTION_BACKOFF_INITIAL * 2
        );
        assert_eq!(
            metadata_contention_backoff_cap(3),
            METADATA_CONTENTION_BACKOFF_INITIAL * 4
        );
        assert_eq!(
            metadata_contention_backoff_cap(64),
            METADATA_CONTENTION_BACKOFF_MAX
        );
    }

    #[test]
    fn placed_segment_direct_read_recovers_when_read_handle_acquire_hits_delete_fence() {
        let error = ShardIoError::Store {
            node_id: 5,
            pg_id: 13,
            cluster_epoch: ClusterEpoch::INITIAL,
            source: StoreError::StorageRpcShardDeleteInProgress {
                node_id: 5,
                operation: "read handles acquire",
                message: "shard is being deleted".to_string(),
            },
        };

        placed_segment_recoverable_shard_error(error).unwrap();
    }

    #[test]
    fn placed_segment_direct_read_does_not_recover_unrelated_delete_fence_errors() {
        let error = ShardIoError::Store {
            node_id: 5,
            pg_id: 13,
            cluster_epoch: ClusterEpoch::INITIAL,
            source: StoreError::StorageRpcShardDeleteInProgress {
                node_id: 5,
                operation: "shard delete",
                message: "shard is being deleted".to_string(),
            },
        };

        assert!(matches!(
            placed_segment_recoverable_shard_error(error),
            Err(StoreError::ShardStore { .. })
        ));
    }

    #[test]
    fn placed_segment_read_recovers_remote_shard_read_damage_errors() {
        for (code, message) in [
            (StorageRpcErrorCode::NotFound, "not found".to_string()),
            (
                StorageRpcErrorCode::ShardIntegrity,
                "remote shard integrity failure".to_string(),
            ),
        ] {
            let error = ShardIoError::Store {
                node_id: 5,
                pg_id: 13,
                cluster_epoch: ClusterEpoch::INITIAL,
                source: StoreError::StorageRpc {
                    node_id: 5,
                    operation: "shard read",
                    code,
                    message,
                },
            };

            placed_segment_recoverable_shard_error(error).unwrap();
        }
    }

    #[test]
    fn placed_segment_read_does_not_recover_unrelated_remote_rpc_errors() {
        for (operation, code, message) in [
            (
                "shard read",
                StorageRpcErrorCode::Internal,
                "shard not found",
            ),
            (
                "shard read",
                StorageRpcErrorCode::Internal,
                "shard 00000000000000000000000000000000000000000000000000 ack mismatch: expected size 4 CRC 0x0000000000000001, got size 4 CRC 0x0000000000000002",
            ),
            (
                "shard delete",
                StorageRpcErrorCode::ShardIntegrity,
                "remote shard integrity failure",
            ),
        ] {
            let error = ShardIoError::Store {
                node_id: 5,
                pg_id: 13,
                cluster_epoch: ClusterEpoch::INITIAL,
                source: StoreError::StorageRpc {
                    node_id: 5,
                    operation,
                    code,
                    message: message.to_string(),
                },
            };

            assert!(matches!(
                placed_segment_recoverable_shard_error(error),
                Err(StoreError::ShardStore { .. })
            ));
        }
    }

    fn replica_match(code: u8) -> ReissuedPendingCommandReplicaMatch {
        match code % 3 {
            0 => ReissuedPendingCommandReplicaMatch::BelowReplacement,
            1 => ReissuedPendingCommandReplicaMatch::MatchesHashChain,
            _ => ReissuedPendingCommandReplicaMatch::MissingOrMismatched,
        }
    }

    proptest! {
        #[test]
        fn prop_reissued_pending_command_decision_is_fail_closed(
            payload_matches in any::<bool>(),
            primary_max_log_index in 0_u64..64,
            primary_applied_log_index in 0_u64..64,
            primary_applied_log_hash in any::<u64>(),
            acting_set_max_log_index in 0_u64..66,
            current_log_index in 0_u64..66,
            replica_inputs in proptest::collection::vec(
                (0_u32..6, 0_u64..66, 0_u64..66, any::<u64>(), 0_u8..3),
                0..8,
            ),
        ) {
            let primary_node_id = NodeId::new(1);
            let replicas = replica_inputs
                .into_iter()
                .map(|(node_id, max_log_index, applied_log_index, applied_log_hash, match_code)| {
                    ReissuedPendingCommandReplicaSummary {
                        node_id: NodeId::new(node_id),
                        max_log_index,
                        applied_log_index,
                        applied_log_hash,
                        replacement_match: replica_match(match_code),
                    }
                })
                .collect::<Vec<_>>();

            let decision = decide_reissued_pending_command(
                ReissuedPendingCommandPrimarySummary {
                    node_id: primary_node_id,
                    max_log_index: primary_max_log_index,
                    applied_log_index: primary_applied_log_index,
                    applied_log_hash: primary_applied_log_hash,
                },
                acting_set_max_log_index,
                current_log_index,
                payload_matches,
                &replicas,
            );

            if !payload_matches {
                prop_assert_eq!(decision, ReissuedPendingCommandDecision::StaleCommandDisplaced);
                return Ok(());
            }
            if primary_applied_log_index != primary_max_log_index {
                prop_assert_eq!(
                    decision,
                    ReissuedPendingCommandDecision::Conflict {
                        node_id: primary_node_id,
                        log_index: primary_max_log_index,
                    }
                );
                return Ok(());
            }
            let expected_log_index = primary_max_log_index + 1;
            if current_log_index != expected_log_index
                || acting_set_max_log_index > current_log_index
            {
                prop_assert_eq!(
                    decision,
                    ReissuedPendingCommandDecision::Conflict {
                        node_id: primary_node_id,
                        log_index: acting_set_max_log_index.max(current_log_index),
                    }
                );
                return Ok(());
            }
            for replica in &replicas {
                if replica.max_log_index < current_log_index {
                    if replica.max_log_index != primary_applied_log_index
                        || replica.applied_log_index != primary_applied_log_index
                        || replica.applied_log_hash != primary_applied_log_hash
                    {
                        prop_assert_eq!(
                            decision,
                            ReissuedPendingCommandDecision::Conflict {
                                node_id: replica.node_id,
                                log_index: primary_applied_log_index.max(replica.max_log_index),
                            }
                        );
                        return Ok(());
                    }
                    continue;
                }
                if replica.replacement_match != ReissuedPendingCommandReplicaMatch::MatchesHashChain
                {
                    prop_assert_eq!(
                        decision,
                        ReissuedPendingCommandDecision::Conflict {
                            node_id: replica.node_id,
                            log_index: current_log_index,
                        }
                    );
                    return Ok(());
                }
            }
            prop_assert_eq!(decision, ReissuedPendingCommandDecision::ReloadCurrent);
        }
    }

    #[test]
    fn reissued_pending_command_decision_allows_primary_last_window() {
        let decision = decide_reissued_pending_command(
            ReissuedPendingCommandPrimarySummary {
                node_id: NodeId::new(1),
                max_log_index: 1,
                applied_log_index: 1,
                applied_log_hash: 100,
            },
            2,
            2,
            true,
            &[
                ReissuedPendingCommandReplicaSummary {
                    node_id: NodeId::new(1),
                    max_log_index: 1,
                    applied_log_index: 1,
                    applied_log_hash: 100,
                    replacement_match: ReissuedPendingCommandReplicaMatch::BelowReplacement,
                },
                ReissuedPendingCommandReplicaSummary {
                    node_id: NodeId::new(0),
                    max_log_index: 2,
                    applied_log_index: 2,
                    applied_log_hash: 200,
                    replacement_match: ReissuedPendingCommandReplicaMatch::MatchesHashChain,
                },
            ],
        );
        assert_eq!(decision, ReissuedPendingCommandDecision::ReloadCurrent);
    }

    #[test]
    fn reissued_pending_command_decision_rejects_divergent_prefix() {
        let decision = decide_reissued_pending_command(
            ReissuedPendingCommandPrimarySummary {
                node_id: NodeId::new(1),
                max_log_index: 1,
                applied_log_index: 1,
                applied_log_hash: 100,
            },
            2,
            2,
            true,
            &[ReissuedPendingCommandReplicaSummary {
                node_id: NodeId::new(0),
                max_log_index: 2,
                applied_log_index: 2,
                applied_log_hash: 200,
                replacement_match: ReissuedPendingCommandReplicaMatch::MissingOrMismatched,
            }],
        );
        assert_eq!(
            decision,
            ReissuedPendingCommandDecision::Conflict {
                node_id: NodeId::new(0),
                log_index: 2,
            }
        );
    }

    #[test]
    fn reissued_pending_command_decision_rejects_below_replacement_divergent_prefix() {
        let decision = decide_reissued_pending_command(
            ReissuedPendingCommandPrimarySummary {
                node_id: NodeId::new(1),
                max_log_index: 1,
                applied_log_index: 1,
                applied_log_hash: 100,
            },
            1,
            2,
            true,
            &[ReissuedPendingCommandReplicaSummary {
                node_id: NodeId::new(0),
                max_log_index: 1,
                applied_log_index: 1,
                applied_log_hash: 999,
                replacement_match: ReissuedPendingCommandReplicaMatch::BelowReplacement,
            }],
        );
        assert_eq!(
            decision,
            ReissuedPendingCommandDecision::Conflict {
                node_id: NodeId::new(0),
                log_index: 1,
            }
        );
    }
}
