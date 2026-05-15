#[cfg(any(test, feature = "test-hooks"))]
use std::sync::Mutex;
use std::sync::{Arc, Weak};

use ec::{EcConfig, ErasureCodec};
use placement::NodeId;
use ring::rand::SecureRandom;

use local::LocalClusterRuntimeState;
pub use local::{LocalClusterMap, LocalNodeStore, LocalNodeStoreConfig, LocalPgRoute};

use crate::error::{ClusterBuildError, ShardIoError, StoreError};
use crate::metadata_command::{
    AbortStreamUploadCommand, AppendStreamSegmentCommand, BucketWriteReservationProof,
    CommitDirectPutObjectCommand, CreateMultipartUploadCommand, CreateStreamUploadCommand,
    DeleteObjectVersionTarget, MetadataCommandEnvelope, MetadataCommandId, MetadataCommandLogIndex,
    MetadataCommandPayload, MetadataCommandReplicaState, ObjectPayloadReclaimCommand,
    ReleaseObjectGenerationCommand, ReserveObjectGenerationCommand, ReserveObjectVersionCommand,
};
use crate::node::SharedStorageNode;
use crate::traits::{PgMetadataStore, ShardStore};
use crate::types::{
    BucketName, BucketWriteReservationRecord, ClusterEpoch, CommitDirectPutObjectReq,
    CreateStreamUploadReq, DataPgId, DirectPutCommitSnapshot, DirectPutWrittenSegment, EcShape,
    FinalizeDirectPutObjectOutcome, GenerationId, MultipartReclaimPartRecord,
    MultipartReclaimPartSegmentRecord, MultipartReclaimRecord, MultipartUploadRecord,
    ObjectEncryption, ObjectKey, ObjectLayout, ObjectPartRecord, ObjectSegmentRecord,
    ObjectSegmentsReclaimRecord, ObjectSegmentsReclaimSegmentRecord, PgId,
    PrepareStreamUploadSegmentAppendReq, PutLiveObjectReq, SegmentStoredBytesRequest, SessionId,
    ShardIndex, ShardKey, StreamUploadCommandRecord, StreamUploadRecord, StreamUploadSegmentRecord,
    StreamUploadState, StreamUploadTarget, VersionId, WriteAck, WrittenShardAck,
};
use crate::{BucketSnapshotLoadError, MetadataError, ObjectEtag, ObjectPgActionError};

mod local;
mod request_ops;

#[cfg(any(test, feature = "test-hooks"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataCommandApplyTestKind {
    CreateBucket,
    PutBucketVersioning,
    PutBucketAcl,
    PutBucketProperty,
    PutBucketSubresource,
    MarkBucketDeleting,
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

const TRACE_TARGET: &str = "storage";

fn conflicting_pending_object_metadata_command(context: &'static str) -> ObjectPgActionError {
    ObjectPgActionError::Store(StoreError::Io {
        context,
        source: std::io::Error::other("conflicting pending metadata command"),
    })
}

#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PendingMetadataCommandOutcome {
    Applied,
    Abandoned,
    RetryPartialExactConflict,
}

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
        DeleteObjectVersionTarget::DeleteMarker => None,
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

fn stream_upload_matches_command(
    existing: &StreamUploadRecord,
    create: &CreateStreamUploadCommand,
) -> bool {
    StreamUploadCommandRecord::from(existing) == create.session
        && existing.next_segment_vid == create.initial_next_segment_vid
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

fn multipart_upload_matches_command(
    existing: &MultipartUploadRecord,
    create: &CreateMultipartUploadCommand,
) -> bool {
    *existing == create.upload
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
            commit.matches_request(bucket, key, session_id, commit.object.generation_id)
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
type StreamAppendCommandIdHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
pub type PayloadShardCleanupTestHook =
    Arc<dyn Fn(&ShardKey) -> Result<(), StoreError> + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
pub type PayloadCleanupErrorTestHook = Arc<dyn Fn(&'static str, &StoreError) + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
#[derive(Default)]
struct StorageClusterTestHooks {
    before_stream_abort_storage: Option<StreamAbortHook>,
    before_metadata_command_pending_install: Option<MetadataCommandPendingInstallHook>,
    before_direct_put_command_id: Option<DirectPutCommandIdHook>,
    before_stream_append_command_id: Option<StreamAppendCommandIdHook>,
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
pub struct StreamAppendCommandIdHookGuard {
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
impl Drop for StreamAppendCommandIdHookGuard {
    fn drop(&mut self) {
        self.hooks.lock().unwrap().before_stream_append_command_id = None;
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

pub struct ObjectPayloadLease {
    cluster: Weak<StorageCluster>,
    runtime_state: Arc<LocalClusterRuntimeState>,
    bucket: BucketName,
    key: ObjectKey,
    generation_id: GenerationId,
    released: bool,
}

impl ObjectPayloadLease {
    fn new(
        cluster: Weak<StorageCluster>,
        runtime_state: Arc<LocalClusterRuntimeState>,
        bucket: BucketName,
        key: ObjectKey,
        generation_id: GenerationId,
    ) -> Self {
        Self {
            cluster,
            runtime_state,
            bucket,
            key,
            generation_id,
            released: false,
        }
    }

    pub fn release(mut self) -> ReleasedObjectPayloadLease {
        let remaining = self.runtime_state.release_object_payload_lease(
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
            remaining,
        }
    }
}

impl Drop for ObjectPayloadLease {
    fn drop(&mut self) {
        if !self.released {
            let _ = self.runtime_state.release_object_payload_lease(
                &self.bucket,
                &self.key,
                self.generation_id,
            );
        }
    }
}

pub struct ReleasedObjectPayloadLease {
    cluster: Weak<StorageCluster>,
    runtime_state: Arc<LocalClusterRuntimeState>,
    bucket: BucketName,
    key: ObjectKey,
    generation_id: GenerationId,
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
        self.runtime_state.enqueue_object_payload_reclaim(
            &self.bucket,
            &self.key,
            self.generation_id,
        );
    }
}

/// Cluster-shaped storage handle.
#[derive(Clone)]
pub struct StorageCluster {
    single_node: Arc<SharedStorageNode>,
    local_map: Arc<LocalClusterMap>,
    operation_epoch: ClusterEpoch,
    #[cfg(any(test, feature = "test-hooks"))]
    test_hooks: Arc<Mutex<StorageClusterTestHooks>>,
}

pub(super) struct DurableBucketWriteReservation {
    node: Arc<SharedStorageNode>,
    pg_id: u32,
    record: BucketWriteReservationRecord,
    legacy_counter_acquired: bool,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BucketWriteReservationDisposition {
    TransferredToCommand,
    ReleaseByCaller,
    PreserveForOwnershipCheckFailure,
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

impl StorageCluster {
    fn metadata_command_conflict(
        &self,
        node_id: NodeId,
        pg_id: PgId,
        log_index: u64,
    ) -> StoreError {
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

    fn metadata_command_is_bucket_pg_command(command: &MetadataCommandEnvelope) -> bool {
        matches!(
            command.payload(),
            MetadataCommandPayload::CreateBucket(_)
                | MetadataCommandPayload::PutBucketVersioning(_)
                | MetadataCommandPayload::PutBucketAcl(_)
                | MetadataCommandPayload::PutBucketProperty(_)
                | MetadataCommandPayload::PutBucketSubresource(_)
                | MetadataCommandPayload::MarkBucketDeleting(_)
                | MetadataCommandPayload::DeleteCompletedMultipartUpload(_)
                | MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(_)
        )
    }

    fn partial_exact_metadata_command_conflict_is_retryable(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        applied_nodes: usize,
    ) -> Result<bool, BucketSnapshotLoadError> {
        if applied_nodes == 0 {
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
        nodes.sort_by_key(|node| node.node_id() == primary_node_id);

        let mut expected_hashes = None;
        for (index, node) in nodes.into_iter().enumerate() {
            let pg = node.storage_node().get_pg(pg_id.get())?;
            let hashes =
                pg.applied_metadata_command_log_entry_hashes(node.node_id().as_u32(), command)?;
            match (index < applied_nodes, hashes, expected_hashes) {
                (true, Some(hashes), None) => expected_hashes = Some(hashes),
                (true, Some(hashes), Some(expected)) if hashes == expected => {}
                (true, _, _) => return Ok(false),
                (false, Some(_), None) => return Ok(false),
                (false, Some(hashes), Some(expected)) if hashes == expected => {}
                (false, Some(_), _) => return Ok(false),
                (false, None, _) => {}
            }
        }
        Ok(expected_hashes.is_some())
    }

    fn matching_reissued_pending_command_if_safe(
        &self,
        pg_id: PgId,
        primary_node_id: NodeId,
        primary_max_log_index: u64,
        acting_set_max_log_index: u64,
        stale_command: &MetadataCommandEnvelope,
        current: MetadataCommandEnvelope,
    ) -> Result<Option<MetadataCommandEnvelope>, StoreError> {
        let payload_matches = current.payload() == stale_command.payload();
        let current_log_index = current.id().log_index().get();
        let primary = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let primary_pg = primary.storage_node().get_pg(pg_id.get())?;
        let primary_state = primary_pg.metadata_command_replica_state()?;
        if current_log_index == primary_state.applied_log_index {
            let Some((_, log_hash)) = primary_pg
                .applied_metadata_command_log_entry_hashes(primary_node_id.as_u32(), &current)?
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
            drop(primary_pg);
            return self.matching_terminal_pending_command_if_safe(
                pg_id,
                primary_node_id,
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
        drop(primary_pg);
        let mut replicas = Vec::new();
        for node in self
            .local_map
            .metadata_pg_acting_nodes(self.operation_epoch(), pg_id)?
        {
            let pg = node.storage_node().get_pg(pg_id.get())?;
            let node_max_log_index = pg.max_metadata_command_log_index(self.operation_epoch())?;
            let node_state = pg.metadata_command_replica_state()?;
            let replacement_match = if node_max_log_index < current_log_index {
                ReissuedPendingCommandReplicaMatch::BelowReplacement
            } else if pg.has_matching_applied_metadata_command_log_entry(
                node.node_id().as_u32(),
                &current,
                primary_state.applied_log_hash,
            )? {
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
        let primary = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let primary_pg = primary.storage_node().get_pg(pg_id.get())?;
        let Some((previous_log_hash, terminal_log_hash)) = primary_pg
            .applied_metadata_command_log_entry_hashes(primary_node_id.as_u32(), &current)?
        else {
            return Err(self.metadata_command_conflict(primary_node_id, pg_id, current_log_index));
        };
        if terminal_log_hash != primary_state.applied_log_hash {
            return Err(self.metadata_command_conflict(primary_node_id, pg_id, current_log_index));
        }
        drop(primary_pg);

        for node in self
            .local_map
            .metadata_pg_acting_nodes(self.operation_epoch(), pg_id)?
        {
            let pg = node.storage_node().get_pg(pg_id.get())?;
            let node_max_log_index = pg.max_metadata_command_log_index(self.operation_epoch())?;
            let node_state = pg.metadata_command_replica_state()?;
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
            if !pg.has_matching_applied_metadata_command_log_entry(
                node.node_id().as_u32(),
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
        let pg = primary.storage_node().get_pg(pg_id.get())?;
        let primary_max_log_index = pg.max_metadata_command_log_index(self.operation_epoch())?;
        drop(pg);
        let acting_set_max_log_index = self.max_metadata_command_log_index_on_acting_set(pg_id)?;
        let pg = primary.storage_node().get_pg(pg_id.get())?;
        if let Some(current) = pg
            .pending_metadata_command_envelope(primary.node_id().as_u32(), self.operation_epoch())?
        {
            if current != *command {
                drop(pg);
                return self
                    .matching_reissued_pending_command_if_safe(
                        pg_id,
                        primary.node_id(),
                        primary_max_log_index,
                        acting_set_max_log_index,
                        command,
                        current,
                    )
                    .map_err(BucketSnapshotLoadError::from);
            }
        } else {
            return Ok(None);
        }
        drop(pg);
        if acting_set_max_log_index > primary_max_log_index {
            let pg = primary.storage_node().get_pg(pg_id.get())?;
            if let Some(current) = pg.pending_metadata_command_envelope(
                primary.node_id().as_u32(),
                self.operation_epoch(),
            )? {
                drop(pg);
                if current != *command {
                    return self
                        .matching_reissued_pending_command_if_safe(
                            pg_id,
                            primary.node_id(),
                            primary_max_log_index,
                            acting_set_max_log_index,
                            command,
                            current,
                        )
                        .map_err(BucketSnapshotLoadError::from);
                }
                return self
                    .matching_reissued_pending_command_if_safe(
                        pg_id,
                        primary.node_id(),
                        primary_max_log_index,
                        acting_set_max_log_index,
                        command,
                        current,
                    )
                    .map_err(BucketSnapshotLoadError::from);
            } else {
                return Ok(None);
            }
        }
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
        {
            let pg = primary.storage_node().get_pg(pg_id.get())?;
            if !pg.replace_pending_metadata_command_slot_for_reissue(
                primary.node_id().as_u32(),
                command,
                &replacement,
                Some(&bucket),
            )? {
                let current = pg.pending_metadata_command_envelope(
                    primary.node_id().as_u32(),
                    self.operation_epoch(),
                )?;
                let primary_max_log_index =
                    pg.max_metadata_command_log_index(self.operation_epoch())?;
                drop(pg);
                let Some(current) = current else {
                    return Ok(None);
                };
                let acting_set_max_log_index =
                    self.max_metadata_command_log_index_on_acting_set(pg_id)?;
                return self
                    .matching_reissued_pending_command_if_safe(
                        pg_id,
                        primary.node_id(),
                        primary_max_log_index,
                        acting_set_max_log_index,
                        command,
                        current,
                    )
                    .map_err(BucketSnapshotLoadError::from);
            }
        }
        Ok(Some(replacement))
    }

    #[cfg(test)]
    pub(crate) fn test_reissue_pending_metadata_command(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<Option<MetadataCommandEnvelope>, BucketSnapshotLoadError> {
        self.reissue_pending_metadata_command(pg_id, command)
    }

    fn max_metadata_command_log_index_on_acting_set(&self, pg_id: PgId) -> Result<u64, StoreError> {
        let mut max_log_index = 0;
        for node in self
            .local_map
            .metadata_pg_acting_nodes(self.operation_epoch(), pg_id)?
        {
            let pg = node.storage_node().get_pg(pg_id.get())?;
            max_log_index =
                max_log_index.max(pg.max_metadata_command_log_index(self.operation_epoch())?);
        }
        Ok(max_log_index)
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
        let single_node = Arc::clone(local_map.metadata_primary().storage_node());
        Ok(Arc::new(Self {
            single_node,
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
        Ok(self.single_node.as_ref())
    }

    // Read-only topology/config helpers still use the metadata-primary node
    // while PgTopology lives on SharedStorageNode.
    fn metadata_primary_topology_node(&self) -> &SharedStorageNode {
        self.single_node.as_ref()
    }

    fn set_pending_metadata_command_for_bucket(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        _context: &'static str,
    ) -> Result<(), ObjectPgActionError> {
        loop {
            if self.try_install_pending_metadata_command_for_bucket(pg_id, bucket, command)? {
                return Ok(());
            }
            self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
        }
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
        let primary = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), pg_id)?;
        let pg = primary.storage_node().get_pg(pg_id.get())?;
        match pg.try_insert_pending_metadata_command_slot(
            primary.node_id().as_u32(),
            command,
            Some(bucket),
        ) {
            Ok(()) => Ok(Some(())),
            Err(StoreError::MetadataCommandPendingConflict { .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn install_snapshot_sensitive_metadata_command_or_drain(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
    ) -> Result<SnapshotSensitiveCommandInstall, ObjectPgActionError> {
        if self.try_install_pending_metadata_command_for_bucket(pg_id, bucket, command)? {
            Ok(SnapshotSensitiveCommandInstall::Installed)
        } else {
            self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
            Ok(SnapshotSensitiveCommandInstall::ContenderDrained)
        }
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
        let pg = primary.storage_node().get_pg(pg_id.get())?;
        pg.pending_metadata_command_envelope(primary.node_id().as_u32(), self.operation_epoch())
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
        let pg = primary.storage_node().get_pg(pg_id.get())?;
        let _ = bucket;
        pg.remove_pending_metadata_command_slot(primary.node_id().as_u32(), command)
    }

    fn next_metadata_command_id(&self, pg_id: PgId) -> Result<MetadataCommandId, StoreError> {
        self.next_metadata_command_id_at_least(
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
        let pg = primary.storage_node().get_pg(pg_id.get())?;
        self.next_metadata_command_id_from_locked_pg_at_least(pg_id, &pg, min_log_index)
    }

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

    pub fn local_pg_route(&self, pg_id: PgId) -> Option<&LocalPgRoute> {
        self.local_map.pg_route(pg_id)
    }

    pub fn local_pg_routes(&self) -> impl Iterator<Item = &LocalPgRoute> + '_ {
        self.local_map.pg_routes()
    }

    /// Temporary process-local registry key for shared coordinator workers.
    ///
    /// Multiple `StorageCluster` handles backed by the same local node keep
    /// sharing process-local workers until a real cluster identity exists.
    pub fn process_local_registry_key(&self) -> usize {
        self.local_map.process_local_registry_key()
    }

    fn metadata_pg_primary_node(&self, pg_id: u32) -> Result<&SharedStorageNode, StoreError> {
        let node = self
            .local_map
            .metadata_pg_primary_node(self.operation_epoch(), PgId::new(pg_id))?;
        Ok(node.storage_node().as_ref())
    }

    fn bucket_metadata_pg_id(&self, bucket: &BucketName) -> u32 {
        self.metadata_primary_topology_node()
            .pg_topology()
            .bucket_pg_for(bucket)
    }

    fn object_metadata_pg_id(&self, bucket: &BucketName, key: &ObjectKey) -> u32 {
        self.metadata_primary_topology_node()
            .pg_topology()
            .object_pg_for(bucket, key)
    }

    fn bucket_metadata_primary_node(
        &self,
        bucket: &BucketName,
    ) -> Result<&SharedStorageNode, StoreError> {
        self.metadata_pg_primary_node(self.bucket_metadata_pg_id(bucket))
    }

    fn bucket_metadata_primary_node_arc(
        &self,
        bucket: &BucketName,
    ) -> Result<Arc<SharedStorageNode>, StoreError> {
        let node = self.local_map.metadata_pg_primary_node(
            self.operation_epoch(),
            PgId::new(self.bucket_metadata_pg_id(bucket)),
        )?;
        Ok(Arc::clone(node.storage_node()))
    }

    fn metadata_command_bucket_write_reservation_proof(
        command: &MetadataCommandEnvelope,
    ) -> Option<&BucketWriteReservationProof> {
        match command.payload() {
            MetadataCommandPayload::CreateStreamUpload(create) => {
                create.bucket_write_reservation.as_ref()
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
        let node = self.bucket_metadata_primary_node_arc(&proof.bucket)?;
        let bucket_pg = node.get_pg(pg_id)?;
        let Some(record) = PgMetadataStore::durable_bucket_write_reservation(
            &*bucket_pg,
            &proof.bucket,
            &proof.reservation_id,
        )?
        else {
            return Err(MetadataError::BucketWriteReservationNotFound {
                reservation_id: proof.reservation_id.clone(),
            }
            .into());
        };
        if proof.matches_record(&record) {
            Ok(())
        } else {
            Err(MetadataError::BucketWriteReservationConflict {
                reservation_id: proof.reservation_id.clone(),
            }
            .into())
        }
    }

    fn release_metadata_command_bucket_write_reservation(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), BucketSnapshotLoadError> {
        let Some(proof) = Self::metadata_command_bucket_write_reservation_proof(command) else {
            return Ok(());
        };
        let node = self.bucket_metadata_primary_node_arc(&proof.bucket)?;
        let pg_id = self.bucket_metadata_pg_id(&proof.bucket);
        let bucket_pg = node.get_pg(pg_id)?;
        if let Some(record) = PgMetadataStore::durable_bucket_write_reservation(
            &*bucket_pg,
            &proof.bucket,
            &proof.reservation_id,
        )? {
            if !proof.matches_record(&record) {
                return Err(MetadataError::BucketWriteReservationConflict {
                    reservation_id: proof.reservation_id.clone(),
                }
                .into());
            }
        }
        PgMetadataStore::release_metadata_command_bucket_write_reservation(
            &*bucket_pg,
            &proof.bucket,
            &proof.reservation_id,
            &proof.owner_token,
            proof.cluster_epoch,
            proof.bucket_execution_generation,
        )?;
        node.notify_bucket_coordination_change(&proof.bucket);
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
        const HEX: &[u8; 16] = b"0123456789abcdef";

        let rng = ring::rand::SystemRandom::new();
        let mut id_bytes = [0u8; 16];
        rng.fill(&mut id_bytes).map_err(|_| StoreError::Io {
            context: "generate bucket write reservation id",
            source: std::io::Error::other("failed to generate random reservation id"),
        })?;

        let mut encoded = String::with_capacity("bucket-write-".len() + id_bytes.len() * 2);
        encoded.push_str("bucket-write-");
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

    fn object_metadata_primary_node(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<&SharedStorageNode, StoreError> {
        self.metadata_pg_primary_node(self.object_metadata_pg_id(bucket, key))
    }

    pub fn default_payload_ec_shape(&self) -> EcShape {
        self.metadata_primary_topology_node().default_ec_shape()
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

    pub fn read_payload_shard(
        &self,
        location: ShardLocation,
        key: &ShardKey,
        expected: WriteAck,
    ) -> Result<Vec<u8>, ShardIoError> {
        self.local_map
            .read_payload_shard(self.operation_epoch(), location, key, expected)
    }

    pub fn read_payload_shard_into(
        &self,
        location: ShardLocation,
        key: &ShardKey,
        expected: WriteAck,
        dst: &mut [u8],
    ) -> Result<(), ShardIoError> {
        self.local_map
            .read_payload_shard_into(self.operation_epoch(), location, key, expected, dst)
    }

    pub fn delete_payload_shard(
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
            .metadata_primary_topology_node()
            .pg_topology()
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
        self.metadata_primary_topology_node()
            .write_erasure_coded_segment_shards_with(
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
        let primary_node = self.object_metadata_primary_node(bucket, key)?;
        let _bucket_guard = primary_node.lock_bucket(bucket);
        loop {
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
                            PendingMetadataCommandOutcome::Abandoned
                            | PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                                continue;
                            }
                        }
                    }
                    _ => {}
                }
                self.drain_pending_object_metadata_command(pg_id, &command)?;
                continue;
            }

            let object_pg = primary_node.get_pg(pg_id.get())?;
            match PgMetadataStore::get_object_generation_reservation(
                &*object_pg,
                bucket,
                key,
                reservation_id,
            ) {
                Ok(generation_id) => return Ok(generation_id),
                Err(MetadataError::ObjectGenerationReservationNotFound { .. }) => {}
                Err(error) => return Err(error.into()),
            }
            let generation_id = object_pg.next_generation_id(bucket, key)?;
            drop(object_pg);
            let Some(command_id) = self.next_object_metadata_command_id_or_drain(pg_id, bucket)?
            else {
                continue;
            };
            let command = MetadataCommandEnvelope::new(
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
            );
            if self
                .try_set_pending_metadata_command_for_bucket(pg_id, bucket, &command)?
                .is_none()
            {
                continue;
            }
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
        _primary_node: &SharedStorageNode,
    ) -> Result<VersionId, ObjectPgActionError> {
        loop {
            if let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
                if let MetadataCommandPayload::ReserveObjectVersion(reservation) = command.payload()
                {
                    let reserved_version_id = reservation.version_id;
                    let matches_request = reservation.matches_request(bucket, key);
                    let outcome = if matches_request {
                        let exact =
                            ExactPendingObjectMetadataCommand::for_checked_request(&command);
                        self.finish_exact_pending_object_metadata_command(pg_id, exact)?
                    } else {
                        self.drain_pending_object_metadata_command(pg_id, &command)?;
                        continue;
                    };
                    match outcome {
                        PendingMetadataCommandOutcome::Applied if matches_request => {
                            return Ok(reserved_version_id);
                        }
                        PendingMetadataCommandOutcome::Applied
                        | PendingMetadataCommandOutcome::Abandoned
                        | PendingMetadataCommandOutcome::RetryPartialExactConflict => continue,
                    }
                }
                self.drain_pending_object_metadata_command(pg_id, &command)?;
                continue;
            }

            let version_id = self.max_next_object_version_id_on_acting_set(pg_id, bucket, key)?;
            let Some(command_id) = self.next_object_metadata_command_id_or_drain(pg_id, bucket)?
            else {
                continue;
            };
            let command = MetadataCommandEnvelope::new(
                command_id,
                MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
                    bucket.clone(),
                    key.clone(),
                    version_id,
                )),
            );
            if self
                .try_set_pending_metadata_command_for_bucket(pg_id, bucket, &command)?
                .is_none()
            {
                continue;
            }
            self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
            return Ok(version_id);
        }
    }

    fn max_next_object_version_id_on_acting_set(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<VersionId, ObjectPgActionError> {
        let mut version_id = VersionId::from_u64(1);
        for node in self
            .local_map
            .metadata_pg_acting_nodes(self.operation_epoch(), pg_id)?
        {
            let pg = node.storage_node().get_pg(pg_id.get())?;
            let candidate = PgMetadataStore::next_version_id(&*pg, bucket, key)?;
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
            PendingMetadataCommandOutcome::Abandoned
            | PendingMetadataCommandOutcome::RetryPartialExactConflict => {
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
        match self.finish_object_pg_pending_slot(pg_id, command)? {
            PendingMetadataCommandOutcome::Applied
            | PendingMetadataCommandOutcome::Abandoned
            | PendingMetadataCommandOutcome::RetryPartialExactConflict => Ok(()),
        }
    }

    fn finish_object_pg_pending_slot(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<PendingMetadataCommandOutcome, ObjectPgActionError> {
        let mut command = command.clone();
        loop {
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
                    self.release_metadata_command_bucket_write_reservation(&command)
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
                            )
                            .map_err(bucket_snapshot_error_to_object_pg_action_error)? =>
                {
                    return Ok(PendingMetadataCommandOutcome::RetryPartialExactConflict);
                }
                Err(error)
                    if error.applied_nodes == 0
                        && Self::metadata_command_log_conflict_matches(&command, &error.source) =>
                {
                    let Some(reissued) = self
                        .reissue_pending_metadata_command(pg_id, &command)
                        .map_err(bucket_snapshot_error_to_object_pg_action_error)?
                    else {
                        return Ok(PendingMetadataCommandOutcome::Abandoned);
                    };
                    command = reissued;
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
                    self.delete_payload_shard_set_best_effort(
                        segment.data_pg_id,
                        EcShape {
                            k: segment.ec_k,
                            m: segment.ec_m,
                        },
                        &segment.segment_okh,
                        segment.segment_vid,
                    );
                }
                release_result?;
            }
            MetadataCommandPayload::AppendStreamSegment(append) => {
                self.delete_payload_shard_set_best_effort(
                    append.segment.data_pg_id,
                    EcShape {
                        k: append.segment.ec_k,
                        m: append.segment.ec_m,
                    },
                    &append.segment.segment_okh,
                    append.segment.segment_vid,
                );
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
                            PendingMetadataCommandOutcome::Abandoned
                            | PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                                continue;
                            }
                        }
                    }
                    _ => {
                        self.drain_pending_object_metadata_command(pg_id, &command)?;
                        continue;
                    }
                }
            }

            let Some(command_id) = self.next_object_metadata_command_id_or_drain(pg_id, bucket)?
            else {
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
            if self
                .try_set_pending_metadata_command_for_bucket(pg_id, bucket, &command)?
                .is_none()
            {
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

    fn drain_pending_object_metadata_commands_for_bucket_collect(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<Vec<MetadataCommandEnvelope>, ObjectPgActionError> {
        let mut applied = Vec::new();
        while let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
            if Self::metadata_command_is_bucket_pg_command(&command) {
                let outcome = self
                    .finish_pending_metadata_command_to_acting_set_allow_partial_exact_conflict_retry(
                        pg_id,
                        &command,
                        false,
                    )
                    .map_err(bucket_snapshot_error_to_object_pg_action_error)?;
                match outcome {
                    request_ops::FinishPendingMetadataCommandResult::Applied
                    | request_ops::FinishPendingMetadataCommandResult::Abandoned
                    | request_ops::FinishPendingMetadataCommandResult::RetryPartialExactConflict => {
                    }
                }
                continue;
            }
            let outcome = self.finish_object_pg_pending_slot(pg_id, &command)?;
            match outcome {
                PendingMetadataCommandOutcome::Applied => applied.push(command),
                PendingMetadataCommandOutcome::Abandoned
                | PendingMetadataCommandOutcome::RetryPartialExactConflict => {}
            }
        }
        Ok(applied)
    }

    fn drain_pending_object_metadata_commands_for_exact_bucket(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<(), ObjectPgActionError> {
        while let Some(command) = self.pending_metadata_command_for_bucket(pg_id, bucket)? {
            if command.bucket_name() != bucket {
                return Ok(());
            }
            if Self::metadata_command_is_bucket_pg_command(&command) {
                let outcome = self
                    .finish_pending_metadata_command_to_acting_set_allow_partial_exact_conflict_retry(
                        pg_id,
                        &command,
                        false,
                    )
                    .map_err(bucket_snapshot_error_to_object_pg_action_error)?;
                if let request_ops::FinishPendingMetadataCommandResult::RetryPartialExactConflict =
                    outcome
                {
                    continue;
                }
                continue;
            }
            let outcome = self.finish_object_pg_pending_slot(pg_id, &command)?;
            match outcome {
                PendingMetadataCommandOutcome::Applied
                | PendingMetadataCommandOutcome::Abandoned
                | PendingMetadataCommandOutcome::RetryPartialExactConflict => {}
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
        self.next_metadata_command_id(pg_id)
            .map_err(ObjectPgActionError::from)
    }

    fn next_object_metadata_command_id_or_drain(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Result<Option<MetadataCommandId>, ObjectPgActionError> {
        match self.next_object_metadata_command_id(pg_id) {
            Ok(command_id) => Ok(Some(command_id)),
            Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict { .. })) => {
                self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

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

    fn matching_stream_upload_exists(
        &self,
        pg_id: PgId,
        create: &CreateStreamUploadReq,
        expected_command: Option<&CreateStreamUploadCommand>,
    ) -> Result<bool, ObjectPgActionError> {
        let object_node = self.object_metadata_primary_node(&create.bucket, &create.key)?;
        let object_pg = object_node.get_pg(pg_id.get())?;
        match object_pg.get_stream_upload(&create.session_id) {
            Ok(existing)
                if expected_command
                    .is_some_and(|command| stream_upload_matches_command(&existing, command)) =>
            {
                Ok(true)
            }
            Ok(_) => Err(MetadataError::Db {
                context: "create stream upload existing session mismatch",
                source: rusqlite::Error::InvalidQuery,
            }
            .into()),
            Err(MetadataError::StreamSessionNotFound { .. }) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    pub(super) fn matching_multipart_upload_initiated_at(
        &self,
        pg_id: PgId,
        create: &crate::CreateMultipartUploadReq,
        expected_command: Option<&CreateMultipartUploadCommand>,
    ) -> Result<Option<u64>, ObjectPgActionError> {
        let object_node = self.object_metadata_primary_node(&create.bucket, &create.key)?;
        let object_pg = object_node.get_pg(pg_id.get())?;
        match object_pg.get_multipart_upload(&create.upload_id) {
            Ok(existing)
                if expected_command.is_some_and(|command| {
                    multipart_upload_matches_command(&existing, command)
                }) =>
            {
                Ok(Some(existing.initiated_at))
            }
            Ok(_) => Err(MetadataError::Db {
                context: "create multipart upload existing upload mismatch",
                source: rusqlite::Error::InvalidQuery,
            }
            .into()),
            Err(MetadataError::NoSuchUpload { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn apply_new_stream_append_command(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: &MetadataCommandEnvelope,
        segment_record: &StreamUploadSegmentRecord,
        shard_batch: &[(&ShardKey, WriteAck)],
    ) -> Result<(), ObjectPgActionError> {
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
                    return Ok(());
                }
                Err(error)
                    if error.applied_nodes == 0
                        && Self::metadata_command_log_conflict_matches(&command, &error.source) =>
                {
                    let Some(reissued) = self
                        .reissue_pending_metadata_command(pg_id, &command)
                        .map_err(bucket_snapshot_error_to_object_pg_action_error)?
                    else {
                        self.delete_payload_shard_keys_best_effort(
                            segment_record.data_pg_id,
                            EcShape {
                                k: segment_record.ec_k,
                                m: segment_record.ec_m,
                            },
                            &segment_record.segment_okh,
                            segment_record.segment_vid,
                            shard_batch.iter().map(|(key, _)| (*key).clone()),
                        );
                        return Err(conflicting_pending_object_metadata_command(
                            "pending stream append command was displaced during reissue",
                        ));
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
                        self.delete_payload_shard_keys_best_effort(
                            segment_record.data_pg_id,
                            EcShape {
                                k: segment_record.ec_k,
                                m: segment_record.ec_m,
                            },
                            &segment_record.segment_okh,
                            segment_record.segment_vid,
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
        let primary_node = self.object_metadata_primary_node(bucket, key)?;
        let _bucket_guard = primary_node.lock_bucket(bucket);
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
                            PendingMetadataCommandOutcome::Abandoned
                            | PendingMetadataCommandOutcome::RetryPartialExactConflict => {
                                continue;
                            }
                        }
                    }
                    _ => {
                        self.drain_pending_object_metadata_command(pg_id, &command)?;
                        continue;
                    }
                }
            }
            let Some(command_id) = self.next_object_metadata_command_id_or_drain(pg_id, bucket)?
            else {
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
            if self
                .try_set_pending_metadata_command_for_bucket(pg_id, bucket, &command)?
                .is_none()
            {
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

    pub fn commit_direct_put_object_from_payload_shards<E>(
        &self,
        req: &CommitDirectPutObjectReq,
        written_shards: &[WrittenShardAck],
        mut action: impl FnMut(DirectPutCommitSnapshot) -> Result<(), E>,
    ) -> Result<Result<FinalizeDirectPutObjectOutcome, E>, ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(&req.bucket, &req.key));
        let object_node = match self.object_metadata_primary_node(&req.bucket, &req.key) {
            Ok(node) => node,
            Err(error) => {
                self.delete_direct_put_segment_payload_shards(
                    req.data_pg_id,
                    req.ec,
                    &req.segment_okh,
                    req.segment_vid,
                    written_shards,
                );
                return Err(error.into());
            }
        };
        let shard_batch: Vec<(&ShardKey, WriteAck)> = written_shards
            .iter()
            .map(|written| (&written.key, written.ack))
            .collect();

        let _bucket_guard = object_node.lock_bucket(&req.bucket);
        let (command, new_pending_command) = loop {
            let (command, new_pending_command) = loop {
                let Some(command) = self.pending_metadata_command_for_bucket(pg_id, &req.bucket)?
                else {
                    let object_pg = object_node.get_pg(pg_id.get())?;
                    match self.validate_direct_put_commit_preconditions(
                        &object_pg,
                        req,
                        &mut action,
                    ) {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => {
                            drop(object_pg);
                            drop(_bucket_guard);
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
                            return Ok(Err(error));
                        }
                        Err(error) => {
                            drop(object_pg);
                            drop(_bucket_guard);
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
                            return Err(error);
                        }
                    }
                    drop(object_pg);

                    let version_id = if req.versioning == crate::BucketVersioningState::Enabled {
                        match self.reserve_next_object_version(
                            pg_id,
                            &req.bucket,
                            &req.key,
                            object_node,
                        ) {
                            Ok(version_id) => version_id,
                            Err(error) => {
                                drop(_bucket_guard);
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
                                return Err(error);
                            }
                        }
                    } else {
                        VersionId::Null
                    };
                    let object_pg = object_node.get_pg(pg_id.get())?;
                    self.maybe_run_before_direct_put_command_id_hook();
                    let command = match self.prepare_commit_direct_put_object_command(
                        pg_id, &object_pg, req, version_id,
                    ) {
                        Ok(command) => command,
                        Err(ObjectPgActionError::Store(
                            StoreError::MetadataCommandLogConflict { .. },
                        )) => {
                            drop(object_pg);
                            if let Err(error) = self
                                .drain_pending_object_metadata_commands_for_bucket(
                                    pg_id,
                                    &req.bucket,
                                )
                            {
                                drop(_bucket_guard);
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
                                return Err(error);
                            }
                            continue;
                        }
                        Err(error) => {
                            drop(object_pg);
                            drop(_bucket_guard);
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
                            return Err(error);
                        }
                    };
                    drop(object_pg);
                    break (command, true);
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
                );
                let has_abandoned_log = match self
                    .metadata_command_has_abandoned_log_on_acting_set(&command)
                {
                    Ok(has_abandoned_log) => has_abandoned_log,
                    Err(error) => {
                        let error = bucket_snapshot_error_to_object_pg_action_error(error.source);
                        drop(_bucket_guard);
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
                        drop(_bucket_guard);
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
                        drop(_bucket_guard);
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
                        return Err(error);
                    }
                    continue;
                }
                if is_matching_direct_put {
                    break (command, false);
                }
                if let Err(error) = self.drain_pending_object_metadata_command(pg_id, &command) {
                    drop(_bucket_guard);
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
                    return Err(error);
                }
            };

            if let Err(error) = self.register_payload_shard_acks(req.data_pg_id, &shard_batch) {
                if new_pending_command {
                    drop(_bucket_guard);
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
                return Err(error);
            }
            if new_pending_command
                && !self.try_install_pending_metadata_command_for_bucket(
                    pg_id,
                    &req.bucket,
                    &command,
                )?
            {
                self.drain_pending_object_metadata_commands_for_bucket(pg_id, &req.bucket)?;
                continue;
            }
            break (command, new_pending_command);
        };

        let mut command = command;
        loop {
            match self.apply_metadata_command_to_acting_set(&command) {
                Ok(()) => break,
                Err(error)
                    if error.applied_nodes == 0
                        && Self::metadata_command_log_conflict_matches(&command, &error.source) =>
                {
                    let Some(reissued) = self
                        .reissue_pending_metadata_command(pg_id, &command)
                        .map_err(bucket_snapshot_error_to_object_pg_action_error)?
                    else {
                        drop(_bucket_guard);
                        if new_pending_command {
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
                    command = reissued;
                }
                Err(error) => {
                    if new_pending_command && error.applied_nodes == 0 {
                        self.record_abandoned_metadata_command_to_acting_set(&command)
                            .map_err(|error| {
                                bucket_snapshot_error_to_object_pg_action_error(error.source)
                            })?;
                        self.remove_pending_metadata_command_for_bucket(
                            pg_id,
                            &req.bucket,
                            &command,
                        )
                        .map_err(ObjectPgActionError::from)?;
                        drop(_bucket_guard);
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

        self.remove_pending_metadata_command_for_bucket(pg_id, command.bucket_name(), &command)
            .map_err(ObjectPgActionError::from)?;

        #[cfg(any(test, feature = "test-hooks"))]
        crate::node::maybe_run_after_direct_put_metadata_publish_hook(
            self.metadata_primary_test_hook_node().test_hook_scope_id(),
        )?;

        let object_pg = object_node.get_pg(pg_id.get())?;
        let stored = PgMetadataStore::get_object_meta(&*object_pg, &req.bucket, &req.key)?;
        let live_record = stored.as_live().ok_or_else(|| MetadataError::Db {
            context: "stored object missing live record after direct put",
            source: rusqlite::Error::QueryReturnedNoRows,
        })?;

        match command.payload() {
            MetadataCommandPayload::CommitDirectPutObject(commit) => {
                Ok(Ok(FinalizeDirectPutObjectOutcome {
                    version_id: commit.object.version_id,
                    encryption: commit.object.encryption.clone(),
                    live_tags: live_record.tags.clone(),
                    live_size: live_record.size,
                    live_last_modified: live_record.last_modified,
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

    fn validate_direct_put_commit_preconditions<E>(
        &self,
        object_pg: &crate::PgStore,
        req: &CommitDirectPutObjectReq,
        action: &mut impl FnMut(DirectPutCommitSnapshot) -> Result<(), E>,
    ) -> Result<Result<(), E>, ObjectPgActionError> {
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

        let existing_etag = match PgMetadataStore::get_object_meta(object_pg, &req.bucket, &req.key)
        {
            Ok(stored) => stored.as_live().map(|record| record.etag.format()),
            Err(MetadataError::ObjectNotFound) => None,
            Err(other) => return Err(other.into()),
        };
        if let Err(error) = action(DirectPutCommitSnapshot { existing_etag }) {
            return Ok(Err(error));
        }
        Ok(Ok(()))
    }

    fn prepare_commit_direct_put_object_command(
        &self,
        pg_id: PgId,
        object_pg: &crate::PgStore,
        req: &CommitDirectPutObjectReq,
        version_id: VersionId,
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
        let command = CommitDirectPutObjectCommand {
            object,
            segments: vec![segment_record],
            generation_reservation_id: req.generation_reservation_id.clone(),
            write_sequence,
            last_modified_millis,
            stale_payload,
        };
        Ok(MetadataCommandEnvelope::new(
            self.next_object_metadata_command_id_from_locked_pg(pg_id, object_pg)?,
            MetadataCommandPayload::CommitDirectPutObject(Box::new(command)),
        ))
    }

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
                    Self::multipart_reclaim_from_parts(
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

    fn multipart_reclaim_from_parts(
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        created_at: u64,
        parts: &[ObjectPartRecord],
        streaming_segments: &[crate::MultipartPartSegmentRecord],
    ) -> MultipartReclaimRecord {
        use std::collections::BTreeMap;

        let mut segments_by_part: BTreeMap<u32, Vec<MultipartReclaimPartSegmentRecord>> =
            BTreeMap::new();
        for segment in streaming_segments {
            segments_by_part
                .entry(segment.part_number)
                .or_default()
                .push(MultipartReclaimPartSegmentRecord {
                    part_number: segment.part_number,
                    segment_index: segment.segment_index,
                    segment_okh: segment.segment_okh,
                    segment_vid: segment.segment_vid,
                    data_pg_id: segment.data_pg_id,
                    ec: EcShape {
                        k: segment.ec_k,
                        m: segment.ec_m,
                    },
                });
        }

        MultipartReclaimRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id,
            created_at,
            parts: parts
                .iter()
                .map(|part| {
                    if part.part_okh == [0u8; 16] {
                        MultipartReclaimPartRecord::Segments {
                            part_number: part.part_number,
                            segments: segments_by_part
                                .remove(&part.part_number)
                                .unwrap_or_default(),
                        }
                    } else {
                        MultipartReclaimPartRecord::ShardSet {
                            part_number: part.part_number,
                            part_okh: part.part_okh,
                            part_vid: part.part_vid,
                            data_pg_id: part.data_pg_id,
                            ec: EcShape {
                                k: part.ec_k,
                                m: part.ec_m,
                            },
                        }
                    }
                })
                .collect(),
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

    pub fn create_put_object_stream_session_record(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        encryption: ObjectEncryption,
    ) -> Result<(), ObjectPgActionError> {
        loop {
            let reservation = match self.acquire_durable_bucket_write_reservation(
                bucket,
                "put-object-stream-create",
                Some(key.as_str()),
            ) {
                Ok(reservation) => reservation,
                Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteDraining)) => {
                    self.wait_for_durable_bucket_write_drain(bucket)
                        .map_err(bucket_snapshot_error_to_object_pg_action_error)?;
                    continue;
                }
                Err(error) => return Err(bucket_snapshot_error_to_object_pg_action_error(error)),
            };
            let proof = BucketWriteReservationProof::from(&reservation.record);
            let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
            let result = self.create_put_object_stream_session_record_under_reservation(
                bucket,
                key,
                session_id,
                encryption,
                proof.clone(),
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
            let applied_commands =
                self.drain_pending_object_metadata_commands_for_bucket_collect(pg_id, bucket)?;
            let expected_command = applied_stream_create_command(&applied_commands, &request);
            if self.matching_stream_upload_exists(pg_id, &request, expected_command)? {
                return Ok(BucketWriteReservationDisposition::ReleaseByCaller);
            }
            self.reserve_put_object_generation(bucket, key, session_id)?;
            let command_id = match self.next_object_metadata_command_id(pg_id) {
                Ok(command_id) => command_id,
                Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                })) => {
                    self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
                    self.release_object_generation_reservation(bucket, key, session_id)?;
                    continue;
                }
                Err(error) => {
                    let _ = self.release_object_generation_reservation(bucket, key, session_id);
                    return Err(error);
                }
            };
            let command = MetadataCommandEnvelope::new(
                command_id,
                MetadataCommandPayload::CreateStreamUpload(Box::new(
                    CreateStreamUploadCommand::from_request_with_bucket_write_reservation(
                        request.clone(),
                        crate::clock::current_time_millis(),
                        Some(bucket_write_reservation.clone()),
                    ),
                )),
            );
            if !self.try_install_pending_metadata_command_for_bucket(pg_id, bucket, &command)? {
                let cleanup = self
                    .drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)
                    .and_then(|_| {
                        self.release_object_generation_reservation(bucket, key, session_id)
                    });
                cleanup?;
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
        self.object_metadata_primary_node(bucket, key)?
            .load_stream_upload_session(bucket, key, session_id)
    }

    pub fn prepare_stream_segment_append(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        request: &PrepareStreamUploadSegmentAppendReq,
    ) -> Result<(StreamUploadTarget, StreamUploadSegmentRecord), ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        let object_node = self.object_metadata_primary_node(bucket, key)?;
        let _bucket_guard = object_node.lock_bucket(bucket);
        self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;
        object_node.prepare_stream_segment_append(bucket, key, request)
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
        let object_node = match self.object_metadata_primary_node(bucket, key) {
            Ok(node) => node,
            Err(error) => {
                self.delete_payload_shard_keys_best_effort(
                    segment_record.data_pg_id,
                    EcShape {
                        k: segment_record.ec_k,
                        m: segment_record.ec_m,
                    },
                    &segment_record.segment_okh,
                    segment_record.segment_vid,
                    shard_batch.iter().map(|(key, _)| (*key).clone()),
                );
                return Err(error.into());
            }
        };
        let _bucket_guard = object_node.lock_bucket(bucket);
        loop {
            if let Err(error) =
                self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)
            {
                self.delete_payload_shard_keys_best_effort(
                    segment_record.data_pg_id,
                    EcShape {
                        k: segment_record.ec_k,
                        m: segment_record.ec_m,
                    },
                    &segment_record.segment_okh,
                    segment_record.segment_vid,
                    shard_batch.iter().map(|(key, _)| (*key).clone()),
                );
                return Err(error);
            }
            let object_pg = match object_node.get_pg(pg_id.get()) {
                Ok(pg) => pg,
                Err(error) => {
                    self.delete_payload_shard_keys_best_effort(
                        segment_record.data_pg_id,
                        EcShape {
                            k: segment_record.ec_k,
                            m: segment_record.ec_m,
                        },
                        &segment_record.segment_okh,
                        segment_record.segment_vid,
                        shard_batch.iter().map(|(key, _)| (*key).clone()),
                    );
                    return Err(error.into());
                }
            };
            let session = match object_pg.get_stream_upload(session_id) {
                Ok(session) => session,
                Err(error) => {
                    drop(object_pg);
                    self.delete_payload_shard_keys_best_effort(
                        segment_record.data_pg_id,
                        EcShape {
                            k: segment_record.ec_k,
                            m: segment_record.ec_m,
                        },
                        &segment_record.segment_okh,
                        segment_record.segment_vid,
                        shard_batch.iter().map(|(key, _)| (*key).clone()),
                    );
                    return Err(error.into());
                }
            };
            let existing_stream_segment = match object_pg.list_stream_segments(session_id) {
                Ok(segments) => segments
                    .into_iter()
                    .find(|segment| segment.segment_index == segment_index),
                Err(error) => {
                    drop(object_pg);
                    self.delete_payload_shard_keys_best_effort(
                        segment_record.data_pg_id,
                        EcShape {
                            k: segment_record.ec_k,
                            m: segment_record.ec_m,
                        },
                        &segment_record.segment_okh,
                        segment_record.segment_vid,
                        shard_batch.iter().map(|(key, _)| (*key).clone()),
                    );
                    return Err(error.into());
                }
            };
            drop(object_pg);
            let _ = session;
            match existing_stream_segment {
                Some(existing) if existing == *segment_record => return Ok(()),
                Some(_) => {
                    self.delete_payload_shard_keys_best_effort(
                        segment_record.data_pg_id,
                        EcShape {
                            k: segment_record.ec_k,
                            m: segment_record.ec_m,
                        },
                        &segment_record.segment_okh,
                        segment_record.segment_vid,
                        shard_batch.iter().map(|(key, _)| (*key).clone()),
                    );
                    return Err(ObjectPgActionError::InvalidRequest {
                        reason: format!("duplicate segment_index {segment_index}"),
                    });
                }
                None => {}
            }

            self.maybe_run_before_stream_append_command_id_hook();
            let command_id = match self.next_object_metadata_command_id(pg_id) {
                Ok(command_id) => command_id,
                Err(ObjectPgActionError::Store(StoreError::MetadataCommandLogConflict {
                    ..
                })) => {
                    continue;
                }
                Err(error) => {
                    self.delete_payload_shard_keys_best_effort(
                        segment_record.data_pg_id,
                        EcShape {
                            k: segment_record.ec_k,
                            m: segment_record.ec_m,
                        },
                        &segment_record.segment_okh,
                        segment_record.segment_vid,
                        shard_batch.iter().map(|(key, _)| (*key).clone()),
                    );
                    return Err(error);
                }
            };

            if let Err(error) =
                self.register_payload_shard_acks(segment_record.data_pg_id, shard_batch)
            {
                self.delete_payload_shard_keys_best_effort(
                    segment_record.data_pg_id,
                    EcShape {
                        k: segment_record.ec_k,
                        m: segment_record.ec_m,
                    },
                    &segment_record.segment_okh,
                    segment_record.segment_vid,
                    shard_batch.iter().map(|(key, _)| (*key).clone()),
                );
                return Err(error);
            }

            let command = MetadataCommandEnvelope::new(
                command_id,
                MetadataCommandPayload::AppendStreamSegment(Box::new(AppendStreamSegmentCommand {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    segment: segment_record.clone(),
                })),
            );
            if let Err(error) = self.set_pending_metadata_command_for_bucket(
                pg_id,
                bucket,
                &command,
                "conflicting pending command for stream segment append",
            ) {
                self.delete_payload_shard_keys_best_effort(
                    segment_record.data_pg_id,
                    EcShape {
                        k: segment_record.ec_k,
                        m: segment_record.ec_m,
                    },
                    &segment_record.segment_okh,
                    segment_record.segment_vid,
                    shard_batch.iter().map(|(key, _)| (*key).clone()),
                );
                return Err(error);
            }
            return self.apply_new_stream_append_command(
                pg_id,
                bucket,
                &command,
                segment_record,
                shard_batch,
            );
        }
    }

    fn register_payload_shard_acks(
        &self,
        data_pg_id: u32,
        shard_batch: &[(&ShardKey, WriteAck)],
    ) -> Result<(), ObjectPgActionError> {
        let data_pg = self
            .metadata_pg_primary_node(data_pg_id)?
            .get_pg(data_pg_id)?;
        data_pg.register_written_shards_batch(shard_batch)?;
        Ok(())
    }

    pub fn abort_stream_upload_session(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<(), ObjectPgActionError> {
        let pg_id = PgId::new(self.object_metadata_pg_id(bucket, key));
        let node = self.object_metadata_primary_node(bucket, key)?;
        let mut pending_completed_session =
            self.pending_command_completes_stream_session(pg_id, bucket, key, session_id)?;
        self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;

        {
            let object_pg = node.get_pg(pg_id.get())?;
            match object_pg.get_stream_upload(session_id) {
                Ok(_) => {}
                Err(MetadataError::StreamSessionNotFound { .. }) if pending_completed_session => {
                    return Ok(());
                }
                Err(error) => return Err(error.into()),
            }
        }

        #[cfg(any(test, feature = "test-hooks"))]
        self.maybe_run_before_stream_abort_storage_hook();

        let _bucket_guard = node.lock_bucket(bucket);
        loop {
            pending_completed_session =
                self.pending_command_completes_stream_session(pg_id, bucket, key, session_id)?;
            self.drain_pending_object_metadata_commands_for_bucket(pg_id, bucket)?;

            let object_pg = node.get_pg(pg_id.get())?;
            let session = match object_pg.get_stream_upload(session_id) {
                Ok(session) => session,
                Err(MetadataError::StreamSessionNotFound { .. }) if pending_completed_session => {
                    return Ok(());
                }
                Err(error) => return Err(error.into()),
            };
            let _ = session;
            let staged_segments = object_pg.list_stream_segments(session_id)?;
            drop(object_pg);
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
                })),
            );
            if !self.try_install_pending_metadata_command_for_bucket(pg_id, bucket, &command)? {
                continue;
            }
            self.apply_new_object_metadata_command_for_bucket(pg_id, bucket, &command)?;
            return Ok(());
        }
    }

    pub fn list_stream_upload_sessions_best_effort(&self) -> Vec<StreamUploadRecord> {
        let mut sessions = Vec::new();
        for &pg_id in self.metadata_primary_topology_node().pg_ids() {
            let Ok(node) = self.metadata_pg_primary_node(pg_id) else {
                continue;
            };
            let Ok(pg) = node.get_pg(pg_id) else {
                continue;
            };
            let Ok(mut local) = pg.list_all_stream_uploads() else {
                continue;
            };
            sessions.append(&mut local);
        }
        sessions
    }

    pub fn read_segment_payload_stored_bytes_into(
        &self,
        req: SegmentStoredBytesRequest,
        dst: &mut Vec<u8>,
    ) -> Result<(), StoreError> {
        self.require_current_payload_operation_epoch(req.data_pg_id)?;
        match self.try_read_placed_segment_stored_bytes_into(req, dst)? {
            true => Ok(()),
            false => Err(StoreError::NotFound),
        }
    }

    fn try_read_placed_segment_stored_bytes_into(
        &self,
        req: SegmentStoredBytesRequest,
        dst: &mut Vec<u8>,
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

        self.try_read_placed_segment_recovery_into(req, dst)
    }

    fn try_read_placed_segment_direct_into(
        &self,
        req: SegmentStoredBytesRequest,
        dst: &mut Vec<u8>,
    ) -> Result<bool, StoreError> {
        let Some(expected_crc64) = req.segment_crc64 else {
            return Ok(false);
        };

        let locations = self.segment_payload_locations(&req)?;
        let k = req.ec.k as usize;
        let padded = req.stored_size.div_ceil(k) * k;
        let shard_size = padded / k;
        dst.resize(padded, 0);
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
            let start = shard_index * shard_size;
            let end = start + shard_size;
            match self.read_payload_shard_into(*location, &shard_key, ack, &mut dst[start..end]) {
                Ok(()) => {}
                Err(error) => {
                    placed_segment_recoverable_shard_error(error)?;
                    return Ok(false);
                }
            }
        }

        dst.truncate(req.stored_size);
        let actual_crc64 = checksum::crc64::checksum(dst);
        Ok(actual_crc64 == expected_crc64)
    }

    fn try_read_placed_segment_recovery_into(
        &self,
        req: SegmentStoredBytesRequest,
        dst: &mut Vec<u8>,
    ) -> Result<bool, StoreError> {
        let k = req.ec.k as usize;
        let m = req.ec.m as usize;
        let padded = req.stored_size.div_ceil(k) * k;
        let shard_size = padded / k;
        let locations = self.segment_payload_locations(&req)?;
        let mut all_shards = vec![None; k + m];
        let mut present_count = 0usize;

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
            let present_refs: Vec<&[u8]> = present_indices
                .iter()
                .map(|&i| all_shards[i].as_ref().unwrap().as_slice())
                .collect();
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
                let recovered_buf = recovered.as_ref().unwrap();
                dst.extend_from_slice(&recovered_buf[start..end]);
            } else {
                unreachable!("missing reconstructed shard for data index {idx}");
            }
        }
        dst.truncate(req.stored_size);
        if let Some(expected_crc64) = req.segment_crc64 {
            let actual_crc64 = checksum::crc64::checksum(dst);
            if actual_crc64 != expected_crc64 {
                return Err(StoreError::IntegrityError {
                    expected: expected_crc64,
                    actual: actual_crc64,
                });
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
    ) -> Result<(), StoreError> {
        let Some(location) = locations.get(shard_index).copied() else {
            return Ok(());
        };
        let shard_key = ShardKey::new(segment_okh, segment_vid.get(), shard_index as u8);
        let ack = match self.load_payload_shard_ack(data_pg_id, &shard_key) {
            Ok(ack) => ack,
            Err(StoreError::NotFound) => return Ok(()),
            Err(error) => return Err(error),
        };
        if ack.stored_size != shard_size as u64 {
            return Ok(());
        }
        match self.read_payload_shard(location, &shard_key, ack) {
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

    fn load_payload_shard_ack(
        &self,
        data_pg_id: u32,
        shard_key: &ShardKey,
    ) -> Result<WriteAck, StoreError> {
        // Placed payload bytes are routed by LocalClusterMap; per-shard
        // CRC/size acks are metadata rows in the same PG and are read through
        // that PG's primary.
        let pg = self
            .metadata_pg_primary_node(data_pg_id)?
            .get_pg(data_pg_id)?;
        let stat = pg.stat_shard(shard_key)?;
        Ok(WriteAck {
            crc64: stat.crc64,
            stored_size: stat.size,
        })
    }

    fn segment_payload_locations(
        &self,
        req: &SegmentStoredBytesRequest,
    ) -> Result<Vec<ShardLocation>, StoreError> {
        let data_pg_id = DataPgId::new(PgId::new(req.data_pg_id));
        let placement_key = segment_payload_placement_key(&req.segment_okh, req.segment_vid);
        self.place_payload_shards(data_pg_id, req.ec, &placement_key)
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

    fn delete_payload_shard_set_best_effort(
        &self,
        data_pg_id: u32,
        ec: EcShape,
        okh: &[u8; 16],
        generation_id: GenerationId,
    ) {
        let shard_keys = Self::payload_shard_set_keys(okh, generation_id, ec);
        self.delete_payload_shard_keys_best_effort(data_pg_id, ec, okh, generation_id, shard_keys);
    }

    fn delete_payload_shard_keys_best_effort(
        &self,
        data_pg_id: u32,
        ec: EcShape,
        okh: &[u8; 16],
        generation_id: GenerationId,
        shard_keys: impl IntoIterator<Item = ShardKey>,
    ) {
        let shard_keys: Vec<ShardKey> = shard_keys.into_iter().collect();
        self.delete_placed_payload_shard_keys_best_effort(
            DataPgId::new(PgId::new(data_pg_id)),
            ec,
            okh,
            generation_id,
            &shard_keys,
        );
        self.delete_metadata_primary_payload_shard_keys_best_effort(data_pg_id, &shard_keys);
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

    fn delete_placed_payload_shard_keys_best_effort(
        &self,
        data_pg_id: DataPgId,
        ec: EcShape,
        okh: &[u8; 16],
        generation_id: GenerationId,
        shard_keys: &[ShardKey],
    ) {
        let placement_key = segment_payload_placement_key(okh, generation_id);
        let locations = match self.place_payload_shards(data_pg_id, ec, &placement_key) {
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
                    if let Err(error) = self.delete_payload_shard(location, shard_key) {
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
        let data_pg = self
            .metadata_pg_primary_node(data_pg_id)?
            .get_pg(data_pg_id)?;
        for shard_key in shard_keys {
            self.maybe_run_before_metadata_primary_payload_ack_delete_hook(shard_key)
                .map_err(ObjectPgActionError::Store)?;
            data_pg.delete_shard_record(shard_key)?;
        }
        Ok(())
    }

    fn delete_metadata_primary_payload_shard_keys_best_effort(
        &self,
        data_pg_id: u32,
        shard_keys: &[ShardKey],
    ) {
        let node = match self.metadata_pg_primary_node(data_pg_id) {
            Ok(node) => node,
            Err(error) => {
                self.emit_best_effort_payload_cleanup_error(
                    "resolve payload ack metadata PG primary",
                    &error,
                );
                return;
            }
        };
        let data_pg = match node.get_pg(data_pg_id) {
            Ok(data_pg) => data_pg,
            Err(error) => {
                self.emit_best_effort_payload_cleanup_error("resolve payload ack PG", &error);
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
            if let Err(error) = data_pg.delete_shard_record(shard_key) {
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
            let ec = EcShape {
                k: segment.ec_k,
                m: segment.ec_m,
            };
            self.delete_payload_shard_set_best_effort(
                segment.data_pg_id,
                ec,
                &segment.segment_okh,
                segment.segment_vid,
            );
        }
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
        } => Ok(()),
        ShardIoError::Store {
            source: StoreError::Io { context, source },
            ..
        } if is_recoverable_physical_shard_io_error(context, source.kind()) => Ok(()),
        other => Err(shard_io_error_to_store(other)),
    }
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
mod reissue_decision_tests {
    use super::*;
    use proptest::prelude::*;

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
